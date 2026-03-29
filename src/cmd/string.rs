use bytes::Bytes;

use super::{CmdError, CommandHandler, IntoResp, Mutating, bulk_to_bytes, bulk_to_string};
use crate::db::Db;
use crate::object::RedisObject;
use crate::resp::frame::Frame;
use crate::resp::writer::frame_to_bytes;

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
    Set(String, Bytes),
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
                Ok(StringWrite::Set(key, val))
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
            StringWrite::Set(key, value) => {
                // REDIS: SET always overwrites regardless of existing type.
                // setGenericCommand() in t_string.c calls dbAdd/dbOverwrite unconditionally.
                db.set(key, RedisObject::Str(value));
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
            StringWrite::Set(key, value) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SET")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(value.clone()),
            ]),
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
            StringWrite::Set("k".into(), Bytes::from_static(b"v"))
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

    #[test]
    fn ping_returns_pong() {
        let mut db = Db::new();
        assert_eq!(
            StringRead::Ping(None).execute(&mut db),
            Frame::Simple("PONG".into())
        );
    }

    #[test]
    fn ping_with_msg_returns_bulk() {
        let mut db = Db::new();
        assert_eq!(
            StringRead::Ping(Some("hi".into())).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"hi"))
        );
    }

    #[test]
    fn set_then_get() {
        let mut db = Db::new();
        StringWrite::Set("k".into(), Bytes::from_static(b"v")).execute(&mut db);
        assert_eq!(
            StringRead::Get("k".into()).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[test]
    fn get_missing_returns_null() {
        let mut db = Db::new();
        assert_eq!(StringRead::Get("k".into()).execute(&mut db), Frame::Null);
    }

    #[test]
    fn del_counts_removed_keys() {
        let mut db = Db::new();
        StringWrite::Set("a".into(), Bytes::from_static(b"1")).execute(&mut db);
        StringWrite::Set("b".into(), Bytes::from_static(b"2")).execute(&mut db);
        assert_eq!(
            StringWrite::Del(vec!["a".into(), "b".into(), "c".into()]).execute(&mut db),
            Frame::Integer(2)
        );
    }

    // --- IntoResp round-trip ---

    #[test]
    fn set_serializes_to_resp() {
        let cmd = StringWrite::Set("foo".into(), Bytes::from_static(b"bar"));
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
