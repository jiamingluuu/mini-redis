//! TTL-related commands: TTL, PTTL, EXPIRE, PEXPIRE, PERSIST.
//!
//! REDIS: Expiry is managed per-key via the `expires` dict in each Redis DB.
//! Commands like EXPIRE and PEXPIRE set a deadline, TTL/PTTL query remaining
//! time, and PERSIST removes the deadline. See expire.c.

use bytes::Bytes;

use super::{CmdError, CommandHandler, IntoResp, Mutating, bulk_to_string, ensure_no_trailing};
use crate::db::Db;
use crate::entry;
use crate::resp::frame::Frame;
use crate::resp::writer::frame_to_bytes;

// ---------------------------------------------------------------------------
// Read commands: TTL, PTTL
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum ExpireRead {
    /// TTL key — remaining time in seconds
    Ttl(String),
    /// PTTL key — remaining time in milliseconds
    Pttl(String),
}

impl ExpireRead {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        let cmd_name: &'static str = match name {
            "TTL" => "TTL",
            "PTTL" => "PTTL",
            _ => return Err(CmdError::UnknownCommand(name.to_string())),
        };
        let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity(cmd_name))?)?;
        ensure_no_trailing(&mut args, cmd_name)?;
        match cmd_name {
            "TTL" => Ok(ExpireRead::Ttl(key)),
            _ => Ok(ExpireRead::Pttl(key)),
        }
    }
}

impl CommandHandler for ExpireRead {
    fn execute(self, db: &mut Db) -> Frame {
        // REDIS: TTL/PTTL return:
        //   -2 if the key does not exist
        //   -1 if the key exists but has no expiry
        //   otherwise the remaining time
        let (key, in_ms) = match &self {
            ExpireRead::Ttl(k) => (k.as_str(), false),
            ExpireRead::Pttl(k) => (k.as_str(), true),
        };

        match db.get_expiry(key) {
            None => Frame::Integer(-2),
            Some(None) => Frame::Integer(-1),
            Some(Some(expires_at)) => {
                let now = entry::now_ms();
                if now >= expires_at {
                    // Key is expired — lazy expiry hasn't cleaned it yet,
                    // but we should report it as missing.
                    db.remove(key);
                    Frame::Integer(-2)
                } else {
                    let remaining_ms = expires_at - now;
                    if in_ms {
                        Frame::Integer(remaining_ms as i64)
                    } else {
                        // Ceiling division to match Redis: 1ms remaining → 1s
                        Frame::Integer(remaining_ms.div_ceil(1000) as i64)
                    }
                }
            }
        }
    }
}

impl IntoResp for ExpireRead {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            ExpireRead::Ttl(key) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"TTL")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]),
            ExpireRead::Pttl(key) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"PTTL")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]),
        };
        frame_to_bytes(&frame)
    }
}

// ---------------------------------------------------------------------------
// Write commands: EXPIRE, PEXPIRE, PERSIST
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum ExpireWrite {
    /// EXPIRE key seconds
    Expire(String, u64),
    /// PEXPIRE key milliseconds
    Pexpire(String, u64),
    /// PERSIST key — remove expiry
    Persist(String),
}

impl ExpireWrite {
    pub(crate) fn parse(
        name: &str,
        mut args: impl Iterator<Item = Frame>,
    ) -> Result<Self, CmdError> {
        match name {
            "EXPIRE" | "PEXPIRE" => {
                let cmd_name = if name == "EXPIRE" {
                    "EXPIRE"
                } else {
                    "PEXPIRE"
                };
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity(cmd_name))?)?;
                let dur_str = bulk_to_string(args.next().ok_or(CmdError::WrongArity(cmd_name))?)?;
                ensure_no_trailing(&mut args, cmd_name)?;
                let dur: u64 = dur_str.parse().map_err(|_| {
                    CmdError::InvalidArg(cmd_name, "value is not an integer or out of range")
                })?;
                if name == "EXPIRE" {
                    Ok(ExpireWrite::Expire(key, dur))
                } else {
                    Ok(ExpireWrite::Pexpire(key, dur))
                }
            }
            "PERSIST" => {
                let key = bulk_to_string(args.next().ok_or(CmdError::WrongArity("PERSIST"))?)?;
                ensure_no_trailing(&mut args, "PERSIST")?;
                Ok(ExpireWrite::Persist(key))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

impl CommandHandler for ExpireWrite {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            ExpireWrite::Expire(key, secs) => {
                // REDIS: EXPIRE sets a timeout in seconds from now.
                // Returns 1 if set, 0 if key doesn't exist.
                let expires_at = entry::now_ms() + secs * 1000;
                if db.set_expiry(&key, Some(expires_at)) {
                    Frame::Integer(1)
                } else {
                    Frame::Integer(0)
                }
            }
            ExpireWrite::Pexpire(key, ms) => {
                let expires_at = entry::now_ms() + ms;
                if db.set_expiry(&key, Some(expires_at)) {
                    Frame::Integer(1)
                } else {
                    Frame::Integer(0)
                }
            }
            ExpireWrite::Persist(key) => {
                // REDIS: PERSIST removes the expiry. Returns 1 if removed, 0 if
                // key doesn't exist or had no expiry.
                match db.get_expiry(&key) {
                    Some(Some(_)) => {
                        db.set_expiry(&key, None);
                        Frame::Integer(1)
                    }
                    _ => Frame::Integer(0),
                }
            }
        }
    }
}

impl IntoResp for ExpireWrite {
    fn to_resp_bytes(&self) -> Bytes {
        let frame = match self {
            ExpireWrite::Expire(key, secs) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"EXPIRE")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::from(secs.to_string())),
            ]),
            ExpireWrite::Pexpire(key, ms) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"PEXPIRE")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::from(ms.to_string())),
            ]),
            ExpireWrite::Persist(key) => Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"PERSIST")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            ]),
        };
        frame_to_bytes(&frame)
    }
}

impl Mutating for ExpireWrite {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::RedisObject;

    fn new_db() -> Db {
        Db::new(crate::eviction::EvictionPolicy::NoEviction)
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

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
    fn parse_ttl() {
        let (name, args) = array_cmd(&[b"TTL", b"mykey"]);
        assert_eq!(
            ExpireRead::parse(&name, args).unwrap(),
            ExpireRead::Ttl("mykey".into())
        );
    }

    #[test]
    fn parse_expire() {
        let (name, args) = array_cmd(&[b"EXPIRE", b"k", b"60"]);
        assert_eq!(
            ExpireWrite::parse(&name, args).unwrap(),
            ExpireWrite::Expire("k".into(), 60)
        );
    }

    #[test]
    fn parse_pexpire() {
        let (name, args) = array_cmd(&[b"PEXPIRE", b"k", b"5000"]);
        assert_eq!(
            ExpireWrite::parse(&name, args).unwrap(),
            ExpireWrite::Pexpire("k".into(), 5000)
        );
    }

    #[test]
    fn parse_persist() {
        let (name, args) = array_cmd(&[b"PERSIST", b"k"]);
        assert_eq!(
            ExpireWrite::parse(&name, args).unwrap(),
            ExpireWrite::Persist("k".into())
        );
    }

    #[test]
    fn ttl_with_extra_arg_errors() {
        let (name, args) = array_cmd(&[b"TTL", b"k", b"extra"]);
        assert_eq!(
            ExpireRead::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("TTL")
        );
    }

    #[test]
    fn expire_with_extra_arg_errors() {
        let (name, args) = array_cmd(&[b"EXPIRE", b"k", b"60", b"extra"]);
        assert_eq!(
            ExpireWrite::parse(&name, args).unwrap_err(),
            CmdError::WrongArity("EXPIRE")
        );
    }

    // --- TTL/PTTL execute ---

    #[test]
    fn ttl_missing_key_returns_minus_2() {
        let mut db = new_db();
        assert_eq!(
            ExpireRead::Ttl("k".into()).execute(&mut db),
            Frame::Integer(-2)
        );
    }

    #[test]
    fn ttl_no_expiry_returns_minus_1() {
        let mut db = new_db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(
            ExpireRead::Ttl("k".into()).execute(&mut db),
            Frame::Integer(-1)
        );
    }

    #[test]
    fn ttl_returns_positive_remaining() {
        let mut db = new_db();
        let far_future = entry::now_ms() + 10_000; // 10 seconds from now
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(far_future));
        let result = ExpireRead::Ttl("k".into()).execute(&mut db);
        if let Frame::Integer(remaining) = result {
            assert!(remaining > 0 && remaining <= 10, "got {remaining}");
        } else {
            panic!("expected Integer, got {result:?}");
        }
    }

    #[test]
    fn pttl_returns_milliseconds() {
        let mut db = new_db();
        let far_future = entry::now_ms() + 10_000;
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(far_future));
        let result = ExpireRead::Pttl("k".into()).execute(&mut db);
        if let Frame::Integer(remaining) = result {
            assert!(remaining > 5000 && remaining <= 10_000, "got {remaining}");
        } else {
            panic!("expected Integer, got {result:?}");
        }
    }

    // --- EXPIRE/PEXPIRE/PERSIST execute ---

    #[test]
    fn expire_on_missing_key_returns_0() {
        let mut db = new_db();
        assert_eq!(
            ExpireWrite::Expire("k".into(), 60).execute(&mut db),
            Frame::Integer(0)
        );
    }

    #[test]
    fn expire_on_existing_key_returns_1() {
        let mut db = new_db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(
            ExpireWrite::Expire("k".into(), 60).execute(&mut db),
            Frame::Integer(1)
        );
        // TTL should now be positive
        let ttl = ExpireRead::Ttl("k".into()).execute(&mut db);
        if let Frame::Integer(remaining) = ttl {
            assert!(remaining > 0 && remaining <= 60);
        } else {
            panic!("expected positive TTL");
        }
    }

    #[test]
    fn persist_removes_expiry() {
        let mut db = new_db();
        let future = entry::now_ms() + 60_000;
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(future));

        assert_eq!(
            ExpireWrite::Persist("k".into()).execute(&mut db),
            Frame::Integer(1)
        );
        assert_eq!(
            ExpireRead::Ttl("k".into()).execute(&mut db),
            Frame::Integer(-1)
        );
    }

    #[test]
    fn persist_on_key_without_expiry_returns_0() {
        let mut db = new_db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(
            ExpireWrite::Persist("k".into()).execute(&mut db),
            Frame::Integer(0)
        );
    }

    #[test]
    fn persist_on_missing_key_returns_0() {
        let mut db = new_db();
        assert_eq!(
            ExpireWrite::Persist("k".into()).execute(&mut db),
            Frame::Integer(0)
        );
    }

    // --- SET with EX ---

    #[test]
    fn set_with_ex_stores_expiry() {
        use crate::cmd::string::StringWrite;

        let mut db = new_db();
        StringWrite::Set(
            "k".into(),
            b("v"),
            Some(super::super::string::SetExpiry::Ex(10)),
        )
        .execute(&mut db);

        let ttl = ExpireRead::Ttl("k".into()).execute(&mut db);
        if let Frame::Integer(remaining) = ttl {
            assert!(remaining > 0 && remaining <= 10);
        } else {
            panic!("expected positive TTL");
        }
    }

    // --- Lazy expiry ---

    #[test]
    fn get_on_expired_key_returns_null() {
        use crate::cmd::string::StringRead;

        let mut db = new_db();
        // Set with expiry in the past
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(1));

        // GET should return null (lazy expiry)
        assert_eq!(StringRead::Get("k".into()).execute(&mut db), Frame::Null);
    }

    // --- IntoResp ---

    #[test]
    fn expire_serializes_to_resp() {
        let cmd = ExpireWrite::Expire("k".into(), 60);
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n60\r\n"
        );
    }

    #[test]
    fn persist_serializes_to_resp() {
        let cmd = ExpireWrite::Persist("k".into());
        assert_eq!(
            cmd.to_resp_bytes().as_ref(),
            b"*2\r\n$7\r\nPERSIST\r\n$1\r\nk\r\n"
        );
    }
}
