use bytes::Bytes;

use super::{CmdError, CommandHandler, bulk_to_bytes, bulk_to_string};
use crate::db::Db;
use crate::resp::frame::Frame;

/// Hash-family commands: HSET, HGET, HDEL, HGETALL, HLEN, HEXISTS.
// The H prefix is part of the Redis command names — suppress the lint.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, PartialEq)]
pub(crate) enum HashCmd {
    HSet(String, Vec<(Bytes, Bytes)>),
    HGet(String, Bytes),
    HDel(String, Vec<Bytes>),
    HGetAll(String),
    HLen(String),
    HExists(String, Bytes),
}

impl HashCmd {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<HashCmd, CmdError> {
        match name {
            "HSET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HSET"))?)?;
                let rest: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let rest = rest?;
                if rest.is_empty() || rest.len() % 2 != 0 {
                    return Err(CmdError::WrongArity("HSET"));
                }
                let pairs = rest
                    .chunks_exact(2)
                    .map(|c| (c[0].clone(), c[1].clone()))
                    .collect();
                Ok(HashCmd::HSet(key, pairs))
            }
            "HGET" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HGET"))?)?;
                let field = bulk_to_bytes(args.next().ok_or(CmdError::WrongArity("HGET"))?)?;
                Ok(HashCmd::HGet(key, field))
            }
            "HDEL" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HDEL"))?)?;
                let fields: Result<Vec<Bytes>, CmdError> = args.map(bulk_to_bytes).collect();
                let fields = fields?;
                if fields.is_empty() {
                    return Err(CmdError::WrongArity("HDEL"));
                }
                Ok(HashCmd::HDel(key, fields))
            }
            "HGETALL" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HGETALL"))?)?;
                Ok(HashCmd::HGetAll(key))
            }
            "HLEN" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HLEN"))?)?;
                Ok(HashCmd::HLen(key))
            }
            "HEXISTS" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("HEXISTS"))?)?;
                let field = bulk_to_bytes(args.next().ok_or(CmdError::WrongArity("HEXISTS"))?)?;
                Ok(HashCmd::HExists(key, field))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for HashCmd {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            HashCmd::HSet(key, pairs) => match db.get_or_insert_hash(key) {
                Ok(h) => {
                    let added: i64 = pairs.into_iter().map(|(f, v)| h.insert(f, v) as i64).sum();
                    Frame::Integer(added)
                }
                Err(e) => e,
            },
            HashCmd::HGet(key, field) => match db.get_hash(&key) {
                Ok(Some(h)) => match h.get(&field) {
                    Some(v) => Frame::Bulk(v.clone()),
                    None => Frame::Null,
                },
                Ok(None) => Frame::Null,
                Err(e) => e,
            },
            HashCmd::HDel(key, fields) => match db.get_hash_mut(&key) {
                Ok(Some(h)) => {
                    let removed: i64 = fields.iter().map(|f| h.remove(f) as i64).sum();
                    Frame::Integer(removed)
                }
                Ok(None) => Frame::Integer(0),
                Err(e) => e,
            },
            HashCmd::HGetAll(key) => match db.get_hash(&key) {
                Ok(Some(h)) => {
                    // REDIS: HGETALL returns an interleaved array: field, value, field, value…
                    let mut out = Vec::with_capacity(h.len() * 2);
                    for (f, v) in h.get_all() {
                        out.push(Frame::Bulk(f));
                        out.push(Frame::Bulk(v));
                    }
                    Frame::Array(out)
                }
                Ok(None) => Frame::Array(vec![]),
                Err(e) => e,
            },
            HashCmd::HLen(key) => match db.get_hash(&key) {
                Ok(Some(h)) => Frame::Integer(h.len() as i64),
                Ok(None) => Frame::Integer(0),
                Err(e) => e,
            },
            HashCmd::HExists(key, field) => match db.get_hash(&key) {
                Ok(Some(h)) => Frame::Integer(h.contains(&field) as i64),
                Ok(None) => Frame::Integer(0),
                Err(e) => e,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::WRONGTYPE;

    fn frames(items: &[&[u8]]) -> Vec<Frame> {
        items
            .iter()
            .map(|b| Frame::Bulk(Bytes::copy_from_slice(b)))
            .collect()
    }

    fn cmd(items: &[&[u8]]) -> (String, impl Iterator<Item = Frame>) {
        let mut f = frames(items);
        let name = if let Frame::Bulk(b) = f.remove(0) {
            String::from_utf8(b.to_vec()).unwrap().to_uppercase()
        } else {
            panic!()
        };
        (name, f.into_iter())
    }

    // --- parse ---

    #[test]
    fn hset_single_pair() {
        let (name, args) = cmd(&[b"HSET", b"myhash", b"field", b"value"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HSet(
                "myhash".into(),
                vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))]
            )
        );
    }

    #[test]
    fn hset_multiple_pairs() {
        let (name, args) = cmd(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HSet(
                "h".into(),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ]
            )
        );
    }

    #[test]
    fn hset_odd_field_value_count_errors() {
        let (name, args) = cmd(&[b"HSET", b"h", b"f1"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("HSET")
        );
    }

    #[test]
    fn hset_no_pairs_errors() {
        let (name, args) = cmd(&[b"HSET", b"h"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("HSET")
        );
    }

    #[test]
    fn hget_parses_key_and_field() {
        let (name, args) = cmd(&[b"HGET", b"h", b"f"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HGet("h".into(), Bytes::from_static(b"f"))
        );
    }

    #[test]
    fn hdel_single_field() {
        let (name, args) = cmd(&[b"HDEL", b"h", b"f"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HDel("h".into(), vec![Bytes::from_static(b"f")])
        );
    }

    #[test]
    fn hdel_no_fields_errors() {
        let (name, args) = cmd(&[b"HDEL", b"h"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("HDEL")
        );
    }

    #[test]
    fn hgetall_parses_key() {
        let (name, args) = cmd(&[b"HGETALL", b"h"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HGetAll("h".into())
        );
    }

    #[test]
    fn hlen_parses_key() {
        let (name, args) = cmd(&[b"HLEN", b"h"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HLen("h".into())
        );
    }

    #[test]
    fn hexists_parses_key_and_field() {
        let (name, args) = cmd(&[b"HEXISTS", b"h", b"f"]);
        assert_eq!(
            HashCmd::parse(&name, args).unwrap(),
            HashCmd::HExists("h".into(), Bytes::from_static(b"f"))
        );
    }

    // --- execute ---

    fn b(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    #[test]
    fn hset_creates_and_returns_new_count() {
        let mut db = Db::new();
        assert_eq!(
            HashCmd::HSet("h".into(), vec![(b(b"f1"), b(b"v1")), (b(b"f2"), b(b"v2"))])
                .execute(&mut db),
            Frame::Integer(2)
        );
    }

    #[test]
    fn hset_update_counts_zero_new_fields() {
        let mut db = Db::new();
        HashCmd::HSet("h".into(), vec![(b(b"f"), b(b"v1"))]).execute(&mut db);
        assert_eq!(
            HashCmd::HSet("h".into(), vec![(b(b"f"), b(b"v2"))]).execute(&mut db),
            Frame::Integer(0)
        );
    }

    #[test]
    fn hget_returns_value_or_null() {
        let mut db = Db::new();
        HashCmd::HSet("h".into(), vec![(b(b"f"), b(b"v"))]).execute(&mut db);
        assert_eq!(
            HashCmd::HGet("h".into(), b(b"f")).execute(&mut db),
            Frame::Bulk(b(b"v"))
        );
        assert_eq!(
            HashCmd::HGet("h".into(), b(b"missing")).execute(&mut db),
            Frame::Null
        );
    }

    #[test]
    fn hset_on_string_key_returns_wrongtype() {
        let mut db = Db::new();
        use crate::object::RedisObject;
        db.set("k".into(), RedisObject::Str(b(b"v")));
        assert_eq!(
            HashCmd::HSet("k".into(), vec![(b(b"f"), b(b"v"))]).execute(&mut db),
            Frame::Error(WRONGTYPE.into())
        );
    }
}
