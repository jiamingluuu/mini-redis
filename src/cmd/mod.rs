pub(crate) mod hash;
pub(crate) mod list;
pub(crate) mod string;

use bytes::Bytes;

use crate::db::Db;
use crate::resp::frame::Frame;

pub(crate) use hash::{HashRead, HashWrite};
pub(crate) use list::{ListRead, ListWrite};
pub(crate) use string::{StringRead, StringWrite};

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// Convert a command back to its canonical RESP wire representation.
///
/// REDIS: AOF stores commands verbatim in RESP format. We reconstruct from the
/// parsed type rather than threading raw bytes through the pipeline — this
/// normalizes encoding (e.g. always uppercase command names) and gives each
/// command type full ownership of its serialization.
pub(crate) trait IntoResp {
    fn to_resp_bytes(&self) -> Bytes;
}

/// Marker trait for commands that mutate database state and must be written to
/// the AOF log. The `IntoResp` supertrait ensures every mutation can be
/// serialized — the compiler rejects a `Mutating` impl that lacks `IntoResp`.
///
/// REDIS: rioWrite() in aof.c appends only commands that change state. Read
/// commands (GET, HGET, LRANGE…) are never logged.
#[allow(dead_code)]
pub(crate) trait Mutating: IntoResp {}

/// Execute a command against the database, returning a response frame.
///
/// Implemented by each command type so that adding a new family only requires a
/// new `cmd/X.rs` file + one variant in `ReadCmd`/`WriteCmd`.
pub(crate) trait CommandHandler {
    fn execute(self, db: &mut Db) -> Frame;
}

// ---------------------------------------------------------------------------
// Top-level enums
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub(crate) enum ReadCmd {
    String(StringRead),
    Hash(HashRead),
    List(ListRead),
}

/// All write commands implement `Mutating` so they can be passed to AOF
/// logging functions with an `impl Mutating` bound.
#[derive(Debug, PartialEq)]
pub(crate) enum WriteCmd {
    String(StringWrite),
    Hash(HashWrite),
    List(ListWrite),
}

/// Top-level command — `Read` vs `Write` is explicit at the type level.
///
/// REDIS: Redis dispatches on the command name string via a dict of command
/// structs (server.c commandTable). We use a nested enum so the compiler
/// enforces exhaustive handling and makes the read/write distinction visible
/// to the Store actor without inspecting the command name at runtime.
#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Read(ReadCmd),
    Write(WriteCmd),
}

// ---------------------------------------------------------------------------
// CommandHandler impls
// ---------------------------------------------------------------------------

impl CommandHandler for ReadCmd {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            ReadCmd::String(cmd) => cmd.execute(db),
            ReadCmd::Hash(cmd) => cmd.execute(db),
            ReadCmd::List(cmd) => cmd.execute(db),
        }
    }
}

impl CommandHandler for WriteCmd {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            WriteCmd::String(cmd) => cmd.execute(db),
            WriteCmd::Hash(cmd) => cmd.execute(db),
            WriteCmd::List(cmd) => cmd.execute(db),
        }
    }
}

impl CommandHandler for Command {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            Command::Read(cmd) => cmd.execute(db),
            Command::Write(cmd) => cmd.execute(db),
        }
    }
}

// ---------------------------------------------------------------------------
// IntoResp impls
// ---------------------------------------------------------------------------

impl IntoResp for ReadCmd {
    fn to_resp_bytes(&self) -> Bytes {
        match self {
            ReadCmd::String(cmd) => cmd.to_resp_bytes(),
            ReadCmd::Hash(cmd) => cmd.to_resp_bytes(),
            ReadCmd::List(cmd) => cmd.to_resp_bytes(),
        }
    }
}

impl IntoResp for WriteCmd {
    fn to_resp_bytes(&self) -> Bytes {
        match self {
            WriteCmd::String(cmd) => cmd.to_resp_bytes(),
            WriteCmd::Hash(cmd) => cmd.to_resp_bytes(),
            WriteCmd::List(cmd) => cmd.to_resp_bytes(),
        }
    }
}

impl IntoResp for Command {
    fn to_resp_bytes(&self) -> Bytes {
        match self {
            Command::Read(cmd) => cmd.to_resp_bytes(),
            Command::Write(cmd) => cmd.to_resp_bytes(),
        }
    }
}

// `WriteCmd` is always a mutation — mark it so Store can call `append_aof(&write_cmd)`.
// Leaf types (StringWrite, HashWrite, ListWrite) also implement Mutating directly
// for call sites that hold a specific write type rather than the top-level enum.
impl Mutating for WriteCmd {}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

impl Command {
    /// Parse a `Command` from a raw RESP frame.
    ///
    /// The command name determines whether we route to a `ReadCmd` or `WriteCmd`
    /// variant — the read/write split is enforced at parse time, not at dispatch.
    pub(crate) fn from_frame(frame: Frame) -> Result<Command, CmdError> {
        let parts = match frame {
            Frame::Array(p) if !p.is_empty() => p,
            _ => return Err(CmdError::NotAnArray),
        };

        let mut iter = parts.into_iter();
        let name = bulk_to_string(iter.next().ok_or(CmdError::NotAnArray)?)?.to_uppercase();

        match name.as_str() {
            // ----- reads -----
            "PING" | "GET" => {
                StringRead::parse(&name, iter).map(|r| Command::Read(ReadCmd::String(r)))
            }
            "HGET" | "HGETALL" | "HLEN" | "HEXISTS" => {
                HashRead::parse(&name, iter).map(|r| Command::Read(ReadCmd::Hash(r)))
            }
            "LRANGE" | "LLEN" => {
                ListRead::parse(&name, iter).map(|r| Command::Read(ReadCmd::List(r)))
            }
            // ----- writes -----
            "SET" | "DEL" => {
                StringWrite::parse(&name, iter).map(|w| Command::Write(WriteCmd::String(w)))
            }
            "HSET" | "HDEL" => {
                HashWrite::parse(&name, iter).map(|w| Command::Write(WriteCmd::Hash(w)))
            }
            "LPUSH" | "RPUSH" | "LPOP" | "RPOP" => {
                ListWrite::parse(&name, iter).map(|w| Command::Write(WriteCmd::List(w)))
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced when converting a raw RESP frame into a `Command`.
#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum CmdError {
    #[error("ERR command must be a RESP array")]
    NotAnArray,
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),
    #[error("ERR wrong number of arguments for '{0}'")]
    WrongArity(&'static str),
    #[error("ERR invalid argument for '{0}': {1}")]
    InvalidArg(&'static str, &'static str),
    #[error("ERR command contains invalid UTF-8")]
    InvalidUtf8,
    #[error("ERR expected a bulk string frame")]
    NotBulk,
}

// ---------------------------------------------------------------------------
// Shared parser helpers
// ---------------------------------------------------------------------------

pub(crate) fn bulk_to_string(frame: Frame) -> Result<String, CmdError> {
    match frame {
        Frame::Bulk(b) => String::from_utf8(b.to_vec()).map_err(|_| CmdError::InvalidUtf8),
        _ => Err(CmdError::NotBulk),
    }
}

pub(crate) fn bulk_to_bytes(frame: Frame) -> Result<Bytes, CmdError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        _ => Err(CmdError::NotBulk),
    }
}

// ---------------------------------------------------------------------------
// Tests — dispatch-level: verify from_frame routes to the right variant
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn array(items: &[&[u8]]) -> Frame {
        Frame::Array(
            items
                .iter()
                .map(|b| Frame::Bulk(Bytes::copy_from_slice(b)))
                .collect(),
        )
    }

    #[test]
    fn routes_ping_to_read() {
        assert_eq!(
            Command::from_frame(array(&[b"PING"])).unwrap(),
            Command::Read(ReadCmd::String(StringRead::Ping(None)))
        );
    }

    #[test]
    fn routes_set_to_write() {
        let cmd = Command::from_frame(array(&[b"SET", b"k", b"v"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Write(WriteCmd::String(StringWrite::Set(..)))
        ));
    }

    #[test]
    fn routes_hset_to_write() {
        let cmd = Command::from_frame(array(&[b"HSET", b"h", b"f", b"v"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(..)))
        ));
    }

    #[test]
    fn routes_hget_to_read() {
        let cmd = Command::from_frame(array(&[b"HGET", b"h", b"f"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Read(ReadCmd::Hash(HashRead::HGet(..)))
        ));
    }

    #[test]
    fn routes_lpush_to_write() {
        let cmd = Command::from_frame(array(&[b"LPUSH", b"k", b"v"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Write(WriteCmd::List(ListWrite::LPush(..)))
        ));
    }

    #[test]
    fn routes_lrange_to_read() {
        let cmd = Command::from_frame(array(&[b"LRANGE", b"k", b"0", b"-1"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Read(ReadCmd::List(ListRead::LRange(..)))
        ));
    }

    #[test]
    fn routes_lpop_to_write() {
        let cmd = Command::from_frame(array(&[b"LPOP", b"k"])).unwrap();
        assert!(matches!(
            cmd,
            Command::Write(WriteCmd::List(ListWrite::LPop(..)))
        ));
    }

    #[test]
    fn unknown_command_returns_error() {
        let err = Command::from_frame(array(&[b"ZADD"])).unwrap_err();
        assert_eq!(err, CmdError::UnknownCommand("ZADD".into()));
    }

    #[test]
    fn non_array_frame_returns_error() {
        let err = Command::from_frame(Frame::Simple("PING".into())).unwrap_err();
        assert_eq!(err, CmdError::NotAnArray);
    }

    #[test]
    fn case_insensitive_routing() {
        assert_eq!(
            Command::from_frame(array(&[b"hset", b"h", b"f", b"v"])).unwrap(),
            Command::from_frame(array(&[b"HSET", b"h", b"f", b"v"])).unwrap()
        );
    }
}
