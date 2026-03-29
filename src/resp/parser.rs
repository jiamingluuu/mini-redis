use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;

use super::frame::Frame;

/// Errors produced during RESP parsing.
///
/// Uses `thiserror` per the Rust coding-style rule: typed errors for library code.
#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum ParseError {
    /// Not enough bytes yet — caller should buffer more data and retry.
    #[error("incomplete frame")]
    Incomplete,
    /// A byte that is not a valid RESP type prefix.
    #[error("invalid type byte: 0x{0:02x}")]
    InvalidByte(u8),
    /// A length or integer field could not be parsed as i64.
    #[error("invalid integer field")]
    InvalidInteger,
    /// A byte sequence is not valid UTF-8 where a string was expected.
    #[error("invalid UTF-8 in frame")]
    InvalidUtf8,
    /// The framing is structurally invalid (e.g. missing CRLF after bulk data).
    #[error("invalid frame format")]
    InvalidFormat,
}

/// Try to parse one complete RESP frame from `buf`.
///
/// REDIS: Redis reads from a per-client input buffer and calls processInlineBuffer
/// or processMultibulkBuffer depending on the first byte. We unify both paths
/// here. See src/networking.c processInputBuffer().
///
/// Returns:
/// - `Ok(Some(frame))` — a complete frame was parsed; `buf` is advanced past it.
/// - `Ok(None)`        — not enough data yet; `buf` is left unchanged.
/// - `Err(e)`          — the stream is malformed; the connection should be closed.
pub(crate) fn parse(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    // Use a cursor so we only advance `buf` on a successful full parse.
    // If parsing fails with Incomplete the original buffer stays intact and
    // the caller will append more bytes before retrying.
    let mut cursor = Cursor::new(&buf[..]);

    match parse_frame(&mut cursor) {
        Ok(frame) => {
            let consumed = cursor.position() as usize;
            buf.advance(consumed);
            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Internal recursive parser
// ---------------------------------------------------------------------------

fn parse_frame(cur: &mut Cursor<&[u8]>) -> Result<Frame, ParseError> {
    let type_byte = next_byte(cur)?;

    match type_byte {
        b'+' => read_line(cur).and_then(to_string).map(Frame::Simple),
        b'-' => read_line(cur).and_then(to_string).map(Frame::Error),
        b':' => read_integer(cur).map(Frame::Integer),
        b'$' => {
            let len = read_integer(cur)?;
            if len == -1 {
                return Ok(Frame::Null);
            }
            if len < 0 {
                return Err(ParseError::InvalidFormat);
            }
            let data = read_exact(cur, len as usize)?;
            expect_crlf(cur)?;
            Ok(Frame::Bulk(Bytes::from(data)))
        }
        b'*' => {
            let count = read_integer(cur)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < 0 {
                return Err(ParseError::InvalidFormat);
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(parse_frame(cur)?);
            }
            Ok(Frame::Array(items))
        }
        b => Err(ParseError::InvalidByte(b)),
    }
}

// ---------------------------------------------------------------------------
// Cursor helpers
// ---------------------------------------------------------------------------

/// Read one byte and advance the cursor.
fn next_byte(cur: &mut Cursor<&[u8]>) -> Result<u8, ParseError> {
    let pos = cur.position() as usize;
    // Scope the immutable borrow so it ends before we call set_position.
    let byte = {
        let slice = cur.get_ref();
        if pos >= slice.len() {
            return Err(ParseError::Incomplete);
        }
        slice[pos]
    };
    cur.set_position((pos + 1) as u64);
    Ok(byte)
}

/// Read bytes up to and including the next `\r\n`, returning the bytes before it.
fn read_line(cur: &mut Cursor<&[u8]>) -> Result<Vec<u8>, ParseError> {
    let start = cur.position() as usize;
    // Compute line contents and new cursor position inside a block so the
    // immutable slice borrow ends before we call set_position.
    let (line, new_pos) = {
        let slice = cur.get_ref();
        let mut found: Option<(Vec<u8>, usize)> = None;
        for i in start..slice.len() {
            if i + 1 < slice.len() && slice[i] == b'\r' && slice[i + 1] == b'\n' {
                found = Some((slice[start..i].to_vec(), i + 2));
                break;
            }
        }
        found.ok_or(ParseError::Incomplete)?
    };
    cur.set_position(new_pos as u64);
    Ok(line)
}

/// Read a CRLF-terminated decimal integer (may be negative, e.g. -1 for null).
fn read_integer(cur: &mut Cursor<&[u8]>) -> Result<i64, ParseError> {
    let line = read_line(cur)?;
    let s = std::str::from_utf8(&line).map_err(|_| ParseError::InvalidUtf8)?;
    s.parse::<i64>().map_err(|_| ParseError::InvalidInteger)
}

/// Read exactly `n` bytes.
fn read_exact(cur: &mut Cursor<&[u8]>, n: usize) -> Result<Vec<u8>, ParseError> {
    let pos = cur.position() as usize;
    let (data, new_pos) = {
        let slice = cur.get_ref();
        if pos + n > slice.len() {
            return Err(ParseError::Incomplete);
        }
        (slice[pos..pos + n].to_vec(), pos + n)
    };
    cur.set_position(new_pos as u64);
    Ok(data)
}

/// Consume a `\r\n` or return an error.
fn expect_crlf(cur: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
    let pos = cur.position() as usize;
    let ok = {
        let slice = cur.get_ref();
        if pos + 2 > slice.len() {
            return Err(ParseError::Incomplete);
        }
        slice[pos] == b'\r' && slice[pos + 1] == b'\n'
    };
    if !ok {
        return Err(ParseError::InvalidFormat);
    }
    cur.set_position((pos + 2) as u64);
    Ok(())
}

fn to_string(bytes: Vec<u8>) -> Result<String, ParseError> {
    String::from_utf8(bytes).map_err(|_| ParseError::InvalidUtf8)
}

// ---------------------------------------------------------------------------
// Tests — these encode RESP's specified wire behaviour, not just "it works"
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn buf(s: &str) -> BytesMut {
        BytesMut::from(s.as_bytes())
    }

    // --- simple string ---

    #[test]
    fn parse_simple_string() {
        let mut b = buf("+OK\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Simple("OK".into())));
        assert!(b.is_empty(), "buffer must be fully consumed");
    }

    // --- error ---

    #[test]
    fn parse_error_frame() {
        let mut b = buf("-ERR unknown command\r\n");
        assert_eq!(
            parse(&mut b).unwrap(),
            Some(Frame::Error("ERR unknown command".into()))
        );
    }

    // --- integer ---

    #[test]
    fn parse_integer_positive() {
        let mut b = buf(":1000\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Integer(1000)));
    }

    #[test]
    fn parse_integer_negative() {
        let mut b = buf(":-42\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Integer(-42)));
    }

    // --- bulk string ---

    #[test]
    fn parse_bulk_string() {
        let mut b = buf("$6\r\nfoobar\r\n");
        assert_eq!(
            parse(&mut b).unwrap(),
            Some(Frame::Bulk(Bytes::from_static(b"foobar")))
        );
        assert!(b.is_empty());
    }

    #[test]
    fn parse_null_bulk_string() {
        // REDIS: $-1\r\n is the canonical nil bulk reply — e.g. GET on a missing key.
        let mut b = buf("$-1\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Null));
    }

    #[test]
    fn parse_empty_bulk_string() {
        let mut b = buf("$0\r\n\r\n");
        assert_eq!(
            parse(&mut b).unwrap(),
            Some(Frame::Bulk(Bytes::from_static(b"")))
        );
    }

    // --- array ---

    #[test]
    fn parse_array_set_command() {
        // REDIS: redis-cli always sends commands as *N arrays of $M bulk strings.
        let mut b = buf("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let frame = parse(&mut b).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SET")),
                Frame::Bulk(Bytes::from_static(b"foo")),
                Frame::Bulk(Bytes::from_static(b"bar")),
            ])
        );
        assert!(b.is_empty());
    }

    #[test]
    fn parse_null_array() {
        let mut b = buf("*-1\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Null));
    }

    // --- partial / incomplete ---

    #[test]
    fn incomplete_returns_none_and_leaves_buffer() {
        // Packet 1: we get the array header and the first bulk string header,
        // but the payload bytes have not arrived yet.
        let mut b = buf("*2\r\n$3\r\nGET");
        let result = parse(&mut b).unwrap();
        assert_eq!(result, None);
        // Buffer must be untouched so the caller can append more bytes.
        assert_eq!(&b[..], b"*2\r\n$3\r\nGET");
    }

    #[test]
    fn two_frames_in_one_buffer() {
        // Pipelined commands arrive in a single TCP segment.
        let mut b = buf("+PONG\r\n:42\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Simple("PONG".into())));
        assert_eq!(parse(&mut b).unwrap(), Some(Frame::Integer(42)));
        assert!(b.is_empty());
    }

    // --- error cases ---

    #[test]
    fn invalid_type_byte_returns_error() {
        let mut b = buf("!invalid\r\n");
        assert!(matches!(parse(&mut b), Err(ParseError::InvalidByte(b'!'))));
    }

    #[test]
    fn bulk_string_missing_crlf_terminator() {
        // Length says 3, data is "foo", but no trailing \r\n — protocol error.
        let mut b = buf("$3\r\nfooXX");
        assert_eq!(parse(&mut b), Err(ParseError::InvalidFormat));
    }
}
