use bytes::Bytes;

use super::{
    CmdError, CommandHandler, IntoResp, Mutating, bulk_to_bytes, bulk_to_string, ensure_no_trailing,
};
use crate::db::Db;
use crate::resp::frame::Frame;
use crate::resp::writer::frame_to_bytes;

// ---------------------------------------------------------------------------
// Read commands: LRANGE, LLEN
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum ListRead {
    LRange(String, i64, i64),
    LLen(String),
}

impl ListRead {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        match name {
            "LRANGE" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LRANGE"))?)?;
                let start =
                    parse_i64(args.next().ok_or(CmdError::WrongArity("LRANGE"))?, "LRANGE")?;
                let stop = parse_i64(args.next().ok_or(CmdError::WrongArity("LRANGE"))?, "LRANGE")?;
                ensure_no_trailing(&mut args, "LRANGE")?;
                Ok(ListRead::LRange(key, start, stop))
            }
            "LLEN" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LLEN"))?)?;
                ensure_no_trailing(&mut args, "LLEN")?;
                Ok(ListRead::LLen(key))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for ListRead {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            ListRead::LRange(key, start, stop) => match db.get_list(&key) {
                Ok(Some(l)) => {
                    let items = l.range(start, stop);
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
                Ok(None) => Frame::Array(vec![]),
                Err(e) => e,
            },
            ListRead::LLen(key) => match db.get_list(&key) {
                Ok(Some(l)) => Frame::Integer(l.len() as i64),
                Ok(None) => Frame::Integer(0),
                Err(e) => e,
            },
        }
    }
}

impl IntoResp for ListRead {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            ListRead::LRange(key, start, stop) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"LRANGE")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(start.to_string().as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(stop.to_string().as_bytes())),
            ]),
            ListRead::LLen(key) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"LLEN")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]),
        };
        frame_to_bytes(&frame)
    }
}

// ---------------------------------------------------------------------------
// Write commands: LPUSH, RPUSH, LPOP, RPOP
// ---------------------------------------------------------------------------

// REDIS: LPOP/RPOP are writes because they mutate the list by removing elements.
// Logging them in the AOF ensures the pop is replayed on recovery, maintaining
// correct list state rather than silently re-inserting popped values.
#[derive(Debug, PartialEq)]
pub(crate) enum ListWrite {
    LPush(String, Vec<Bytes>),
    RPush(String, Vec<Bytes>),
    /// (key, optional count) — no count means pop exactly one element.
    LPop(String, Option<u64>),
    RPop(String, Option<u64>),
}

impl ListWrite {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        match name {
            "LPUSH" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LPUSH"))?)?;
                let values: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let values = values?;
                if values.is_empty() {
                    return Err(CmdError::WrongArity("LPUSH"));
                }
                Ok(ListWrite::LPush(key, values))
            }
            "RPUSH" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("RPUSH"))?)?;
                let values: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let values = values?;
                if values.is_empty() {
                    return Err(CmdError::WrongArity("RPUSH"));
                }
                Ok(ListWrite::RPush(key, values))
            }
            "LPOP" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LPOP"))?)?;
                let count = parse_optional_count(args.next(), "LPOP")?;
                ensure_no_trailing(&mut args, "LPOP")?;
                Ok(ListWrite::LPop(key, count))
            }
            "RPOP" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("RPOP"))?)?;
                let count = parse_optional_count(args.next(), "RPOP")?;
                ensure_no_trailing(&mut args, "RPOP")?;
                Ok(ListWrite::RPop(key, count))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for ListWrite {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            ListWrite::LPush(key, values) => match db.list_push_many(key, values, true) {
                Ok(len) => Frame::Integer(len as i64),
                Err(e) => e,
            },
            ListWrite::RPush(key, values) => match db.list_push_many(key, values, false) {
                Ok(len) => Frame::Integer(len as i64),
                Err(e) => e,
            },
            ListWrite::LPop(key, count) => match db.list_pop_many(key, count, true) {
                Ok(Some(values)) => list_pop_response(values, count),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
            ListWrite::RPop(key, count) => match db.list_pop_many(key, count, false) {
                Ok(Some(values)) => list_pop_response(values, count),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
        }
    }
}

impl IntoResp for ListWrite {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            ListWrite::LPush(key, values) => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from_static(b"LPUSH")),
                    Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                ];
                parts.extend(values.iter().map(|v| Frame::Bulk(v.clone())));
                Frame::Array(parts)
            }
            ListWrite::RPush(key, values) => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from_static(b"RPUSH")),
                    Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                ];
                parts.extend(values.iter().map(|v| Frame::Bulk(v.clone())));
                Frame::Array(parts)
            }
            ListWrite::LPop(key, count) => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from_static(b"LPOP")),
                    Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                ];
                if let Some(n) = count {
                    parts.push(Frame::Bulk(Bytes::copy_from_slice(
                        n.to_string().as_bytes(),
                    )));
                }
                Frame::Array(parts)
            }
            ListWrite::RPop(key, count) => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from_static(b"RPOP")),
                    Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                ];
                if let Some(n) = count {
                    parts.push(Frame::Bulk(Bytes::copy_from_slice(
                        n.to_string().as_bytes(),
                    )));
                }
                Frame::Array(parts)
            }
        };
        frame_to_bytes(&frame)
    }
}

// REDIS: Only write commands mutate state and must be appended to the AOF.
impl Mutating for ListWrite {}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build the LPOP/RPOP response.
///
/// - No count: return a single Bulk (or Null if empty).
/// - With count: return an Array of up to `count` elements (or empty Array if list is empty).
fn list_pop_response(values: Vec<Bytes>, count: Option<u64>) -> Frame {
    match count {
        None => values.into_iter().next().map_or(Frame::Null, Frame::Bulk),
        Some(_) => Frame::Array(values.into_iter().map(Frame::Bulk).collect()),
    }
}

fn parse_optional_count(frame: Option<Frame>, cmd: &'static str) -> Result<Option<u64>, CmdError> {
    match frame {
        None => Ok(None),
        Some(f) => {
            let s = bulk_to_string(f)?;
            let n = s
                .parse::<u64>()
                .map_err(|_| CmdError::InvalidArg(cmd, "count must be a non-negative integer"))?;
            Ok(Some(n))
        }
    }
}

fn parse_i64(frame: Frame, cmd: &'static str) -> Result<i64, CmdError> {
    let s = bulk_to_string(frame)?;
    s.parse::<i64>()
        .map_err(|_| CmdError::InvalidArg(cmd, "index must be an integer"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::WRONGTYPE;
    use crate::object::RedisObject;
    use crate::types::hash::Hash;

    fn cmd(items: &[&[u8]]) -> (String, impl Iterator<Item = Frame>) {
        let mut f: Vec<Frame> = items
            .iter()
            .map(|b| Frame::Bulk(Bytes::copy_from_slice(b)))
            .collect();
        let name = if let Frame::Bulk(b) = f.remove(0) {
            String::from_utf8(b.to_vec()).unwrap().to_uppercase()
        } else {
            panic!()
        };
        (name, f.into_iter())
    }

    fn b(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    // --- parse ---

    #[test]
    fn lpush_single_value() {
        let (name, args) = cmd(&[b"LPUSH", b"k", b"v"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::LPush("k".into(), vec![Bytes::from_static(b"v")])
        );
    }

    #[test]
    fn lpush_multiple_values() {
        let (name, args) = cmd(&[b"LPUSH", b"k", b"a", b"b", b"c"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::LPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ]
            )
        );
    }

    #[test]
    fn lpush_no_values_errors() {
        let (name, args) = cmd(&[b"LPUSH", b"k"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("LPUSH")
        );
    }

    #[test]
    fn rpush_single_value() {
        let (name, args) = cmd(&[b"RPUSH", b"k", b"v"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::RPush("k".into(), vec![Bytes::from_static(b"v")])
        );
    }

    #[test]
    fn lpop_no_count() {
        let (name, args) = cmd(&[b"LPOP", b"k"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::LPop("k".into(), None)
        );
    }

    #[test]
    fn lpop_with_count() {
        let (name, args) = cmd(&[b"LPOP", b"k", b"3"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::LPop("k".into(), Some(3))
        );
    }

    #[test]
    fn rpop_no_count() {
        let (name, args) = cmd(&[b"RPOP", b"k"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap(),
            ListWrite::RPop("k".into(), None)
        );
    }

    #[test]
    fn lrange_positive_indices() {
        let (name, args) = cmd(&[b"LRANGE", b"k", b"0", b"5"]);
        assert_eq!(
            ListRead::parse(&name, args).unwrap(),
            ListRead::LRange("k".into(), 0, 5)
        );
    }

    #[test]
    fn lrange_negative_index() {
        let (name, args) = cmd(&[b"LRANGE", b"k", b"0", b"-1"]);
        assert_eq!(
            ListRead::parse(&name, args).unwrap(),
            ListRead::LRange("k".into(), 0, -1)
        );
    }

    #[test]
    fn llen_parses_key() {
        let (name, args) = cmd(&[b"LLEN", b"k"]);
        assert_eq!(
            ListRead::parse(&name, args).unwrap(),
            ListRead::LLen("k".into())
        );
    }

    #[test]
    fn llen_with_extra_arg_errors() {
        let (name, args) = cmd(&[b"LLEN", b"k", b"extra"]);
        assert_eq!(
            ListRead::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("LLEN")
        );
    }

    #[test]
    fn lpop_with_extra_arg_errors() {
        let (name, args) = cmd(&[b"LPOP", b"k", b"1", b"extra"]);
        assert_eq!(
            ListWrite::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("LPOP")
        );
    }

    // --- execute ---

    #[test]
    fn lpush_ordering() {
        let mut db = Db::new(crate::eviction::EvictionPolicy::NoEviction);
        ListWrite::LPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListRead::LRange("k".into(), 0, -1).execute(&mut db),
            Frame::Array(vec![
                Frame::Bulk(b(b"c")),
                Frame::Bulk(b(b"b")),
                Frame::Bulk(b(b"a")),
            ])
        );
    }

    #[test]
    fn rpush_ordering() {
        let mut db = Db::new(crate::eviction::EvictionPolicy::NoEviction);
        ListWrite::RPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListRead::LRange("k".into(), 0, -1).execute(&mut db),
            Frame::Array(vec![
                Frame::Bulk(b(b"a")),
                Frame::Bulk(b(b"b")),
                Frame::Bulk(b(b"c")),
            ])
        );
    }

    #[test]
    fn lpop_no_count_returns_bulk() {
        let mut db = Db::new(crate::eviction::EvictionPolicy::NoEviction);
        ListWrite::LPush("k".into(), vec![b(b"v")]).execute(&mut db);
        assert_eq!(
            ListWrite::LPop("k".into(), None).execute(&mut db),
            Frame::Bulk(b(b"v"))
        );
    }

    #[test]
    fn rpop_with_count_returns_array() {
        let mut db = Db::new(crate::eviction::EvictionPolicy::NoEviction);
        ListWrite::RPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListWrite::RPop("k".into(), Some(2)).execute(&mut db),
            Frame::Array(vec![Frame::Bulk(b(b"c")), Frame::Bulk(b(b"b"))])
        );
    }

    #[test]
    fn lpush_on_hash_returns_wrongtype() {
        let mut db = Db::new(crate::eviction::EvictionPolicy::NoEviction);
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert_eq!(
            ListWrite::LPush("k".into(), vec![b(b"v")]).execute(&mut db),
            Frame::Error(WRONGTYPE.into())
        );
    }

    // --- IntoResp round-trip ---

    #[test]
    fn lpush_serializes_to_resp() {
        let cmd = ListWrite::LPush(
            "k".into(),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        );
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*4\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n$1\r\na\r\n$1\r\nb\r\n"
        );
    }

    #[test]
    fn lpop_with_count_serializes_to_resp() {
        let cmd = ListWrite::LPop("mylist".into(), Some(3));
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n3\r\n"
        );
    }
}
