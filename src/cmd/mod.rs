pub(crate) mod hash;
pub(crate) mod list;
pub(crate) mod string;

use bytes::Bytes;

use crate::db::Db;
use crate::resp::frame::Frame;

pub(crate) use hash::HashCmd;
pub(crate) use list::ListCmd;
pub(crate) use string::StringCmd;

/// Execute a command against the database, returning a response frame.
///
/// Implemented by each command family (StringCmd, HashCmd, ListCmd) so that
/// adding a new family only requires a new `cmd/X.rs` file + one variant here.
pub(crate) trait CommandHandler {
    fn execute(self, db: &mut Db) -> Frame;
}

/// Top-level command — one variant per command family.
///
/// REDIS: Redis dispatches on the command name string looked up in a dict of
/// command structs (server.c commandTable). We use a nested enum so the
/// compiler enforces exhaustive handling at every call site.
#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    String(StringCmd),
    Hash(HashCmd),
    List(ListCmd),
}

impl CommandHandler for Command {
    fn execute(self, db: &mut Db) -> Frame {
        match self {
            Command::String(cmd) => cmd.execute(db),
            Command::Hash(cmd) => cmd.execute(db),
            Command::List(cmd) => cmd.execute(db),
        }
    }
}

impl Command {
    /// Parse a `Command` from a raw RESP frame.
    ///
    /// REDIS: processCommand() in server.c resolves the command name from
    /// the first bulk string, then dispatches to the command handler.
    pub(crate) fn from_frame(frame: Frame) -> Result<Command, CmdError> {
        let parts = match frame {
            Frame::Array(p) if !p.is_empty() => p,
            _ => return Err(CmdError::NotAnArray),
        };

        let mut iter = parts.into_iter();
        let name = bulk_to_string(iter.next().ok_or(CmdError::NotAnArray)?)?.to_uppercase();

        match name.as_str() {
            "PING" | "GET" | "SET" | "DEL" => StringCmd::parse(&name, iter).map(Command::String),
            "HSET" | "HGET" | "HDEL" | "HGETALL" | "HLEN" | "HEXISTS" => {
                HashCmd::parse(&name, iter).map(Command::Hash)
            }
            "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" | "LLEN" => {
                ListCmd::parse(&name, iter).map(Command::List)
            }
            other => Err(CmdError::UnknownCommand(other.to_string())),
        }
    }
}

/// Errors produced when converting a raw RESP frame into a Command.
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
// Shared helpers used by the sub-module parsers
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
    fn routes_ping() {
        assert_eq!(
            Command::from_frame(array(&[b"PING"])).unwrap(),
            Command::String(StringCmd::Ping(None))
        );
    }

    #[test]
    fn routes_hset() {
        let cmd = Command::from_frame(array(&[b"HSET", b"h", b"f", b"v"])).unwrap();
        assert!(matches!(cmd, Command::Hash(HashCmd::HSet(..))));
    }

    #[test]
    fn routes_lpush() {
        let cmd = Command::from_frame(array(&[b"LPUSH", b"k", b"v"])).unwrap();
        assert!(matches!(cmd, Command::List(ListCmd::LPush(..))));
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
