use bytes::Bytes;

use super::{CmdError, CommandHandler, bulk_to_bytes, bulk_to_string};
use crate::db::Db;
use crate::resp::frame::Frame;
use crate::types::list::List;

/// List-family commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN.
#[derive(Debug, PartialEq)]
pub(crate) enum ListCmd {
    LPush(String, Vec<Bytes>),
    RPush(String, Vec<Bytes>),
    /// (key, optional count) — no count means pop exactly one element.
    LPop(String, Option<u64>),
    RPop(String, Option<u64>),
    LRange(String, i64, i64),
    LLen(String),
}

impl ListCmd {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<ListCmd, CmdError> {
        match name {
            "LPUSH" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LPUSH"))?)?;
                let values: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let values = values?;
                if values.is_empty() {
                    return Err(CmdError::WrongArity("LPUSH"));
                }
                Ok(ListCmd::LPush(key, values))
            }
            "RPUSH" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("RPUSH"))?)?;
                let values: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let values = values?;
                if values.is_empty() {
                    return Err(CmdError::WrongArity("RPUSH"));
                }
                Ok(ListCmd::RPush(key, values))
            }
            "LPOP" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LPOP"))?)?;
                let count = parse_optional_count(args.next(), "LPOP")?;
                Ok(ListCmd::LPop(key, count))
            }
            "RPOP" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("RPOP"))?)?;
                let count = parse_optional_count(args.next(), "RPOP")?;
                Ok(ListCmd::RPop(key, count))
            }
            "LRANGE" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LRANGE"))?)?;
                let start =
                    parse_i64(args.next().ok_or(CmdError::WrongArity("LRANGE"))?, "LRANGE")?;
                let stop = parse_i64(args.next().ok_or(CmdError::WrongArity("LRANGE"))?, "LRANGE")?;
                Ok(ListCmd::LRange(key, start, stop))
            }
            "LLEN" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("LLEN"))?)?;
                Ok(ListCmd::LLen(key))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for ListCmd {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            ListCmd::LPush(key, values) => match db.get_or_insert_list(key) {
                Ok(l) => {
                    // REDIS: LPUSH pushes values one by one to the head in the
                    // order given, so `LPUSH k a b c` → [c, b, a].
                    for v in values {
                        l.push_front(v);
                    }
                    Frame::Integer(l.len() as i64)
                }
                Err(e) => e,
            },
            ListCmd::RPush(key, values) => match db.get_or_insert_list(key) {
                Ok(l) => {
                    for v in values {
                        l.push_back(v);
                    }
                    Frame::Integer(l.len() as i64)
                }
                Err(e) => e,
            },
            ListCmd::LPop(key, count) => match db.get_list_mut(&key) {
                Ok(Some(l)) => list_pop_response(l, count, true),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
            ListCmd::RPop(key, count) => match db.get_list_mut(&key) {
                Ok(Some(l)) => list_pop_response(l, count, false),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
            ListCmd::LRange(key, start, stop) => match db.get_list(&key) {
                Ok(Some(l)) => {
                    let items = l.range(start, stop);
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
                Ok(None) => Frame::Array(vec![]),
                Err(e) => e,
            },
            ListCmd::LLen(key) => match db.get_list(&key) {
                Ok(Some(l)) => Frame::Integer(l.len() as i64),
                Ok(None) => Frame::Integer(0),
                Err(e) => e,
            },
        }
    }
}

/// Build the LPOP/RPOP response.
///
/// - No count: return a single Bulk (or Null if empty).
/// - With count: return an Array of up to `count` elements (or empty Array if list is empty).
fn list_pop_response(list: &mut List, count: Option<u64>, from_front: bool) -> Frame {
    match count {
        None => {
            let val = if from_front {
                list.pop_front()
            } else {
                list.pop_back()
            };
            val.map_or(Frame::Null, Frame::Bulk)
        }
        Some(n) => {
            let mut out = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let val = if from_front {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                match val {
                    Some(v) => out.push(Frame::Bulk(v)),
                    None => break,
                }
            }
            Frame::Array(out)
        }
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
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LPush("k".into(), vec![Bytes::from_static(b"v")])
        );
    }

    #[test]
    fn lpush_multiple_values() {
        let (name, args) = cmd(&[b"LPUSH", b"k", b"a", b"b", b"c"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LPush(
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
            ListCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("LPUSH")
        );
    }

    #[test]
    fn rpush_single_value() {
        let (name, args) = cmd(&[b"RPUSH", b"k", b"v"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::RPush("k".into(), vec![Bytes::from_static(b"v")])
        );
    }

    #[test]
    fn lpop_no_count() {
        let (name, args) = cmd(&[b"LPOP", b"k"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LPop("k".into(), None)
        );
    }

    #[test]
    fn lpop_with_count() {
        let (name, args) = cmd(&[b"LPOP", b"k", b"3"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LPop("k".into(), Some(3))
        );
    }

    #[test]
    fn rpop_no_count() {
        let (name, args) = cmd(&[b"RPOP", b"k"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::RPop("k".into(), None)
        );
    }

    #[test]
    fn lrange_positive_indices() {
        let (name, args) = cmd(&[b"LRANGE", b"k", b"0", b"5"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LRange("k".into(), 0, 5)
        );
    }

    #[test]
    fn lrange_negative_index() {
        let (name, args) = cmd(&[b"LRANGE", b"k", b"0", b"-1"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LRange("k".into(), 0, -1)
        );
    }

    #[test]
    fn llen_parses_key() {
        let (name, args) = cmd(&[b"LLEN", b"k"]);
        assert_eq!(
            ListCmd::parse(&name, args).unwrap(),
            ListCmd::LLen("k".into())
        );
    }

    // --- execute ---

    #[test]
    fn lpush_ordering() {
        let mut db = Db::new();
        ListCmd::LPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListCmd::LRange("k".into(), 0, -1).execute(&mut db),
            Frame::Array(vec![
                Frame::Bulk(b(b"c")),
                Frame::Bulk(b(b"b")),
                Frame::Bulk(b(b"a")),
            ])
        );
    }

    #[test]
    fn rpush_ordering() {
        let mut db = Db::new();
        ListCmd::RPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListCmd::LRange("k".into(), 0, -1).execute(&mut db),
            Frame::Array(vec![
                Frame::Bulk(b(b"a")),
                Frame::Bulk(b(b"b")),
                Frame::Bulk(b(b"c")),
            ])
        );
    }

    #[test]
    fn lpop_no_count_returns_bulk() {
        let mut db = Db::new();
        ListCmd::LPush("k".into(), vec![b(b"v")]).execute(&mut db);
        assert_eq!(
            ListCmd::LPop("k".into(), None).execute(&mut db),
            Frame::Bulk(b(b"v"))
        );
    }

    #[test]
    fn rpop_with_count_returns_array() {
        let mut db = Db::new();
        ListCmd::RPush("k".into(), vec![b(b"a"), b(b"b"), b(b"c")]).execute(&mut db);
        assert_eq!(
            ListCmd::RPop("k".into(), Some(2)).execute(&mut db),
            Frame::Array(vec![Frame::Bulk(b(b"c")), Frame::Bulk(b(b"b"))])
        );
    }

    #[test]
    fn lpush_on_hash_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert_eq!(
            ListCmd::LPush("k".into(), vec![b(b"v")]).execute(&mut db),
            Frame::Error(WRONGTYPE.into())
        );
    }
}
