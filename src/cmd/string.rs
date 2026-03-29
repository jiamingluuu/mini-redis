use bytes::Bytes;

use super::{CmdError, CommandHandler, bulk_to_bytes, bulk_to_string};
use crate::db::Db;
use crate::object::RedisObject;
use crate::resp::frame::Frame;

/// String-family commands: PING, GET, SET, DEL.
#[derive(Debug, PartialEq)]
pub(crate) enum StringCmd {
    Ping(Option<String>),
    Get(String),
    Set(String, Bytes),
    Del(Vec<String>),
}

impl StringCmd {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<StringCmd, CmdError> {
        match name {
            "PING" => {
                let msg = args.next().map(bulk_to_string).transpose()?;
                Ok(StringCmd::Ping(msg))
            }
            "GET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("GET"))?)?;
                Ok(StringCmd::Get(key))
            }
            "SET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("SET"))?)?;
                let val = bulk_to_bytes(args.next().ok_or(CmdError::WrongArity("SET"))?)?;
                Ok(StringCmd::Set(key, val))
            }
            "DEL" => {
                let keys: Result<Vec<String>, CmdError> = args.map(bulk_to_string).collect();
                let keys = keys?;
                if keys.is_empty() {
                    return Err(CmdError::WrongArity("DEL"));
                }
                Ok(StringCmd::Del(keys))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for StringCmd {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            StringCmd::Ping(msg) => match msg {
                None => Frame::Simple("PONG".into()),
                Some(m) => Frame::Bulk(Bytes::from(m)),
            },
            StringCmd::Get(key) => match db.get_str(&key) {
                Ok(Some(b)) => Frame::Bulk(b.clone()),
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
            StringCmd::Set(key, value) => {
                // REDIS: SET always overwrites regardless of existing type.
                // setGenericCommand() in t_string.c calls dbAdd/dbOverwrite unconditionally.
                db.set(key, RedisObject::Str(value));
                Frame::Simple("OK".into())
            }
            StringCmd::Del(keys) => {
                let count = keys.iter().filter(|k| db.remove(k)).count();
                Frame::Integer(count as i64)
            }
        }
    }
}

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
            StringCmd::parse(&name, args).unwrap(),
            StringCmd::Ping(None)
        );
    }

    #[test]
    fn ping_with_message() {
        let (name, args) = array_cmd(&[b"PING", b"hello"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap(),
            StringCmd::Ping(Some("hello".into()))
        );
    }

    #[test]
    fn get_with_key() {
        let (name, args) = array_cmd(&[b"GET", b"mykey"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap(),
            StringCmd::Get("mykey".into())
        );
    }

    #[test]
    fn get_missing_key_errors() {
        let (name, args) = array_cmd(&[b"GET"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("GET")
        );
    }

    #[test]
    fn set_key_value() {
        let (name, args) = array_cmd(&[b"SET", b"k", b"v"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap(),
            StringCmd::Set("k".into(), Bytes::from_static(b"v"))
        );
    }

    #[test]
    fn del_multiple_keys() {
        let (name, args) = array_cmd(&[b"DEL", b"k1", b"k2"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap(),
            StringCmd::Del(vec!["k1".into(), "k2".into()])
        );
    }

    #[test]
    fn del_no_keys_errors() {
        let (name, args) = array_cmd(&[b"DEL"]);
        assert_eq!(
            StringCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("DEL")
        );
    }

    // --- execute ---

    #[test]
    fn ping_returns_pong() {
        let mut db = Db::new();
        assert_eq!(
            StringCmd::Ping(None).execute(&mut db),
            Frame::Simple("PONG".into())
        );
    }

    #[test]
    fn ping_with_msg_returns_bulk() {
        let mut db = Db::new();
        assert_eq!(
            StringCmd::Ping(Some("hi".into())).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"hi"))
        );
    }

    #[test]
    fn set_then_get() {
        let mut db = Db::new();
        StringCmd::Set("k".into(), Bytes::from_static(b"v")).execute(&mut db);
        assert_eq!(
            StringCmd::Get("k".into()).execute(&mut db),
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[test]
    fn get_missing_returns_null() {
        let mut db = Db::new();
        assert_eq!(StringCmd::Get("k".into()).execute(&mut db), Frame::Null);
    }

    #[test]
    fn del_counts_removed_keys() {
        let mut db = Db::new();
        StringCmd::Set("a".into(), Bytes::from_static(b"1")).execute(&mut db);
        StringCmd::Set("b".into(), Bytes::from_static(b"2")).execute(&mut db);
        assert_eq!(
            StringCmd::Del(vec!["a".into(), "b".into(), "c".into()]).execute(&mut db),
            Frame::Integer(2)
        );
    }
}
