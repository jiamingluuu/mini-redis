use bytes::Bytes;

use super::{CmdError, CommandHandler, IntoResp, Mutating, bulk_to_bytes, bulk_to_string};
use crate::db::Db;
use crate::entry;
use crate::object::RedisObject;
use crate::resp::frame::Frame;
use crate::resp::writer::frame_to_bytes;

/// Optional expiry for the SET command.
///
/// REDIS: SET supports EX (seconds) and PX (milliseconds) options.
/// setGenericCommand() in t_string.c handles both.
#[derive(Debug, PartialEq)]
pub(crate) enum SetExpiry {
    Ex(u64), // seconds
    Px(u64), // milliseconds
}

// ---------------------------------------------------------------------------
// Read commands: PING, GET
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum StringRead {
    Ping(Option<String>),
    Get(String),
}

impl StringRead {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        match name {
            "PING" => {
                let msg = args.next().map(bulk_to_string).transpose()?;
                Ok(StringRead::Ping(msg))
            }
            "GET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("GET"))?)?;
                Ok(StringRead::Get(key))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for StringRead {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            StringRead::Ping(msg) => match msg {
                None => Frame::Simple("PONG".into()),
                Some(m) => Frame::Bulk(Bytes::from(m)),
            },
            StringRead::Get(key) => match db.get_str(&key) {
                Ok(Some(b)) => Frame::Bulk(b.clone()),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
        }
    }
}

impl IntoResp for StringRead {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            StringRead::Ping(None) => Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]),
            StringRead::Ping(Some(msg)) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"PING")),
                Frame::Bulk(Bytes::copy_from_slice(msg.as_bytes())),
            ]),
            StringRead::Get(key) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"GET")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]),
        };
        frame_to_bytes(&frame)
    }
}

// ---------------------------------------------------------------------------
// Write commands: SET, DEL
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum StringWrite {
    Set(String, Bytes, Option<SetExpiry>),
    Del(Vec<String>),
}

impl StringWrite {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        match name {
            "SET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("SET"))?)?;
                let val = bulk_to_bytes(args.next().ok_or(CmdError::WrongArity("SET"))?)?;

                // Parse optional EX/PX arguments
                // REDIS: SET key value [EX seconds] [PX milliseconds]
                let expiry = if let Some(opt_frame) = args.next() {
                    let opt = bulk_to_string(opt_frame)?.to_uppercase();
                    let dur_frame = args.next().ok_or(CmdError::WrongArity("SET"))?;
                    let dur_str = bulk_to_string(dur_frame)?;
                    let dur: u64 = dur_str.parse().map_err(|_| {
                        CmdError::InvalidArg("SET", "value is not an integer or out of range")
                    })?;
                    if dur == 0 {
                        return Err(CmdError::InvalidArg(
                            "SET",
                            "invalid expire time in 'set' command",
                        ));
                    }
                    match opt.as_str() {
                        "EX" => Some(SetExpiry::Ex(dur)),
                        "PX" => Some(SetExpiry::Px(dur)),
                        _ => return Err(CmdError::InvalidArg("SET", "syntax error")),
                    }
                } else {
                    None
                };
                Ok(StringWrite::Set(key, val, expiry))
            }
            "DEL" => {
                let keys: Result<Vec<String>, CmdError> = args.map(bulk_to_string).collect();
                let keys = keys?;
                if keys.is_empty() {
                    return Err(CmdError::WrongArity("DEL"));
                }
                Ok(StringWrite::Del(keys))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for StringWrite {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            StringWrite::Set(key, value, expiry) => {
                // REDIS: SET always overwrites regardless of existing type.
                // setGenericCommand() in t_string.c calls dbAdd/dbOverwrite unconditionally.
                let expires_at = expiry.map(|e| {
                    let now = entry::now_ms();
                    match e {
                        SetExpiry::Ex(secs) => now + secs * 1000,
                        SetExpiry::Px(ms) => now + ms,
                    }
                });
                db.set_with_expiry(key, RedisObject::Str(value), expires_at);
                Frame::Simple("OK".into())
            }
            StringWrite::Del(keys) => {
                let count = keys.iter().filter(|k| db.remove(k)).count();
                Frame::Integer(count as i64)
            }
        }
    }
}

impl IntoResp for StringWrite {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            StringWrite::Set(key, value, expiry) => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from_static(b"SET")),
                    Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                    Frame::Bulk(value.clone()),
                ];
                // REDIS: AOF must serialize the EX/PX option so replay restores TTLs.
                match expiry {
                    Some(SetExpiry::Ex(secs)) => {
                        parts.push(Frame::Bulk(Bytes::from_static(b"EX")));
                        parts.push(Frame::Bulk(Bytes::from(secs.to_string())));
                    }
                    Some(SetExpiry::Px(ms)) => {
                        parts.push(Frame::Bulk(Bytes::from_static(b"PX")));
                        parts.push(Frame::Bulk(Bytes::from(ms.to_string())));
                    }
                    None => {}
                }
                Frame::Array(parts)
            }
            StringWrite::Del(keys) => {
                let mut parts = vec![Frame::Bulk(Bytes::from_static(b"DEL"))];
                parts.extend(
                    keys.iter()
                        .map(|k| Frame::Bulk(Bytes::copy_from_slice(k.as_bytes()))),
                );
                Frame::Array(parts)
            }
        };
        frame_to_bytes(&frame)
    }
}

// REDIS: Only write commands mutate state and must be appended to the AOF.
impl Mutating for StringWrite {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn array_cmd(items: &[&[u8]]) -> (String, impl Iterator<Item = Frame>) {
        let mut frames: Vec<Frame> = items
            .iter()
            .map(|b| Frame::Bulk(Bytes::copy_from_slice(b)))
            .collect();
        let name = if let Frame::Bulk(b) = frames.remove(0) {
            String::from_utf8(b.to_vec()).unwrap().to_uppercase()
        } else {
            panic!("expected bulk")
        };
        (name, frames.into_iter())
    }

    // --- parse ---

    #[test]
    fn ping_no_args() {
        let (name, args) = array_cmd(&[b"PING"]);
        assert_eq!(
            StringRead::parse(&name, args).unwrap(),
            StringRead::Ping(None)
        );
    }

    #[test]
    fn ping_with_message() {
        let (name, args) = array_cmd(&[b"PING", b"hello"]);
        assert_eq!(
            StringRead::parse(&name, args).unwrap(),
            StringRead::Ping(Some("hello".into()))
        );
    }

    #[test]
    fn get_with_key() {
        let (name, args) = array_cmd(&[b"GET", b"mykey"]);
        assert_eq!(
            StringRead::parse(&name, args).unwrap(),
            StringRead::Get("mykey".into())
        );
    }

    #[test]
    fn get_missing_key_errors() {
        let (name, args) = array_cmd(&[b"GET"]);
        assert_eq!(
            StringRead::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("GET")
        );
    }

    #[test]
    fn set_key_value() {
        let (name, args) = array_cmd(&[b"SET", b"k", b"v"]);
        assert_eq!(
            StringWrite::parse(&name, args).unwrap(),
            StringWrite::Set("k".into(), Bytes::from_static(b"v"), None)
        );
    }

    #[test]
    fn del_multiple_keys() {
        let (name, args) = array_cmd(&[b"DEL", b"k1", b"k2"]);
        assert_eq!(
            StringWrite::parse(&name, args).unwrap(),
            StringWrite::Del(vec!["k1".into(), "k2".into()])
        );
    }

    #[test]
    fn del_no_keys_errors() {
        let (name, args) = array_cmd(&[b"DEL"]);
        assert_eq!(
            StringWrite::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("DEL")
        );
    }

    // --- execute ---

    fn new_db() -> Db {
        Db::new(crate::eviction::EvictionPolicy::NoEviction)
    }

    #[test]
    fn ping_returns_pong() {
        let mut db = new_db();
        assert_eq!(
            StringRead::Ping(None).execute(&mut db),
            Frame::Simple("PONG".into())
        );
    }

    #[test]
    fn ping_with_msg_returns_bulk() {
        let mut db = new_db();
        assert_eq!(
            StringRead::Ping(Some("hi".into())).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"hi"))
        );
    }

    #[test]
    fn set_then_get() {
        let mut db = new_db();
        StringWrite::Set("k".into(), Bytes::from_static(b"v"), None).execute(&mut db);
        assert_eq!(
            StringRead::Get("k".into()).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[test]
    fn get_missing_returns_null() {
        let mut db = new_db();
        assert_eq!(StringRead::Get("k".into()).execute(&mut db), Frame::Null);
    }

    #[test]
    fn del_counts_removed_keys() {
        let mut db = new_db();
        StringWrite::Set("a".into(), Bytes::from_static(b"1"), None).execute(&mut db);
        StringWrite::Set("b".into(), Bytes::from_static(b"2"), None).execute(&mut db);
        assert_eq!(
            StringWrite::Del(vec!["a".into(), "b".into(), "c".into()]).execute(&mut db),
            Frame::Integer(2)
        );
    }

    // --- IntoResp round-trip ---

    #[test]
    fn set_serializes_to_resp() {
        let cmd = StringWrite::Set("foo".into(), Bytes::from_static(b"bar"), None);
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }

    #[test]
    fn del_serializes_to_resp() {
        let cmd = StringWrite::Del(vec!["k1".into(), "k2".into()]);
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*3\r\n$3\r\nDEL\r\n$2\r\nk1\r\n$2\r\nk2\r\n"
        );
    }

    #[test]
    fn get_serializes_to_resp() {
        let cmd = StringRead::Get("mykey".into());
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
        );
    }
}
