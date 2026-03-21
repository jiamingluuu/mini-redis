//! RESP2 (REdis Serialization Protocol v2) — streaming state-machine parser.
//!
//! Wire format summary:
//!   `+OK\r\n`              — Simple string   (text, NOT binary safe)
//!   `-ERR msg\r\n`         — Error           (text)
//!   `:1000\r\n`            — Integer
//!   `$6\r\nfoobar\r\n`     — Bulk string     (binary safe: length-prefixed)
//!   `$-1\r\n`              — Null bulk string
//!   `*3\r\n...`            — Array of N RESP values
//!
//! Why length-prefix for bulk strings? A delimiter-only protocol (like HTTP/1.0
//! headers) breaks if the payload contains the delimiter bytes. Length-prefixing
//! lets bulk strings carry arbitrary binary data — including `\r\n` sequences.
//! That's the property that makes RESP "binary safe."

use std::fmt;

// ─── Value type ──────────────────────────────────────────────────────────────

/// A fully-parsed RESP2 value.
///
/// REDIS: On the wire these are the only types. The server's internal
/// representation (`redisObject` in `object.c`) is richer — it tracks
/// encoding (e.g., OBJ_ENCODING_EMBSTR vs OBJ_ENCODING_RAW) and an LRU
/// clock for eviction. Over the network we only see these seven shapes.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// `+OK\r\n` — status replies from the server. Not binary safe (value
    /// must not contain `\r` or `\n`). Never sent by clients.
    SimpleString(Vec<u8>),

    /// `-ERR message\r\n` — error replies. The convention is `ERR_TYPE reason`.
    Error(Vec<u8>),

    /// `:1000\r\n` — signed 64-bit integer (Redis integers are always i64).
    Integer(i64),

    /// `$6\r\nfoobar\r\n` — binary-safe, length-prefixed bytes. This is how
    /// clients send every command argument. The length lets us memcpy the body
    /// without scanning for delimiters.
    BulkString(Vec<u8>),

    /// `$-1\r\n` — null bulk string. Signals "key not found" etc.
    /// Distinct from `BulkString(vec![])` (empty string vs absent key).
    Null,

    /// `*3\r\n...` — ordered sequence of N RESP values. Commands always arrive
    /// as arrays of bulk strings. Nested arrays appear in EXEC replies, cluster
    /// responses, and pub/sub messages.
    Array(Vec<RespValue>),
}

// ─── Error type ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ParseError {
    /// First byte wasn't a valid RESP type prefix.
    InvalidPrefix(u8),
    /// Length or integer field wasn't a valid i64.
    InvalidNumber,
    /// A length < -1 was given (only -1 is legal as a null sentinel).
    NegativeLength,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidPrefix(b) => write!(f, "invalid RESP prefix byte 0x{b:02x}"),
            ParseError::InvalidNumber => write!(f, "invalid integer in RESP header"),
            ParseError::NegativeLength => write!(f, "illegal negative RESP length"),
        }
    }
}

impl std::error::Error for ParseError {}

// ─── State machine ───────────────────────────────────────────────────────────
//
// The central challenge: TCP is a byte stream. A single `read()` call might
// return 1 byte, half a command, or 10 commands. We cannot assume messages
// arrive atomically.
//
// A state machine handles this by recording *what we know so far* in the
// current state variant. We can pause at any byte boundary and resume
// correctly when more data arrives.
//
// Contrast with recursive descent: a recursive parser needs the full input
// on the call stack. To support streaming you'd have to async-await at every
// read site, which the compiler transforms into — a state machine. So the
// explicit state machine is simply the manual version of what async/await
// would generate.
//
// REDIS: Production Redis accumulates client input in `c->querybuf` (an SDS
// string) and tracks parse progress in fields like `c->multibulklen` and
// `c->bulklen` on the client struct (networking.c). Our `Parser` struct models
// the same idea: the state *is* the parse progress.

#[derive(Debug)]
enum State {
    /// Waiting for the first byte of a new RESP value (the type prefix byte).
    Start,

    /// Collecting bytes for a `\r\n`-terminated header line.
    ///
    /// `prefix` is the type byte already consumed. `buf` accumulates the line
    /// body. `saw_cr` is true when the previous byte was `\r`, letting us
    /// correctly handle the case where `\r` and `\n` arrive in different chunks.
    ReadLine {
        prefix: u8,
        buf: Vec<u8>,
        saw_cr: bool,
    },

    /// Reading exactly `remaining` more bytes of a bulk string body (after `$N\r\n`).
    ///
    /// We process this state with a bulk memcpy rather than byte-by-byte, so
    /// we handle large values efficiently without special-casing in the hot loop.
    ReadBulkBody { remaining: usize, buf: Vec<u8> },

    /// Consuming the mandatory `\r\n` that follows a bulk string body.
    ReadBulkCrlf { data: Vec<u8>, saw_cr: bool },
}

/// A partially-assembled array.
struct ArrayFrame {
    expected: usize,
    items: Vec<RespValue>,
}

// ─── Parser ──────────────────────────────────────────────────────────────────

/// Streaming RESP2 parser.
///
/// Feed bytes incrementally via [`Parser::feed`]. Returns complete [`RespValue`]s
/// as they are fully parsed. Partial messages are held in internal state between
/// calls.
///
/// # Example
/// ```
/// let mut p = Parser::new();
/// // Pretend these two chunks arrived in separate TCP reads:
/// let _ = p.feed(b"*1\r\n$4\r\n").unwrap(); // nothing complete yet
/// let cmds = p.feed(b"PING\r\n").unwrap();   // now we have a full command
/// ```
pub struct Parser {
    state: State,

    /// Stack of partially-assembled arrays.
    ///
    /// RESP arrays are potentially nested (EXEC replies, etc.). When a value
    /// completes, we push it into the top frame. If that fills the frame, the
    /// finished array itself becomes a value and may fill its parent — and so
    /// on up the stack. For Redis commands the depth is always exactly 1.
    ///
    /// REDIS: Redis tracks this inline on the client struct:
    ///   `c->multibulklen` — how many more args to expect
    ///   `c->argc` / `c->argv` — collected args so far
    /// Our stack generalizes this to handle arbitrary nesting.
    array_stack: Vec<ArrayFrame>,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            state: State::Start,
            array_stack: Vec::new(),
        }
    }

    /// Feed a chunk of bytes into the parser.
    ///
    /// Returns all [`RespValue`]s that were fully parsed from the accumulated
    /// input. May return 0 values (partial message), 1 value (typical), or
    /// many values (pipelined commands arriving in one chunk).
    pub fn feed(&mut self, input: &[u8]) -> Result<Vec<RespValue>, ParseError> {
        let mut output = Vec::new();
        let mut i = 0;

        while i < input.len() {
            // We take ownership of the current state by replacing it with `Start`.
            // This is the idiomatic Rust pattern for state machines: it lets us
            // destructure the old state *and* write a new state to `self.state`
            // in the same match arm, without borrow-checker conflicts.
            let state = std::mem::replace(&mut self.state, State::Start);

            match state {
                // ── Waiting for a type prefix byte ───────────────────────────
                State::Start => {
                    let byte = input[i];
                    i += 1;
                    match byte {
                        b'+' | b'-' | b':' | b'*' | b'$' => {
                            self.state = State::ReadLine {
                                prefix: byte,
                                buf: Vec::new(),
                                saw_cr: false,
                            };
                        }
                        _ => return Err(ParseError::InvalidPrefix(byte)),
                    }
                }

                // ── Reading a \r\n-terminated header line ─────────────────────
                State::ReadLine {
                    prefix,
                    mut buf,
                    saw_cr,
                } => {
                    let byte = input[i];
                    i += 1;

                    if saw_cr {
                        if byte == b'\n' {
                            // Line complete — interpret it.
                            // self.state is already Start; interpret_line may overwrite it
                            // (e.g., to ReadBulkBody for '$' prefix).
                            if let Some(value) = self.interpret_line(prefix, buf)? {
                                self.deliver(value, &mut output);
                            }
                        } else {
                            // \r not followed by \n — protocol violation.
                            // We're lenient: treat the \r as a literal byte.
                            buf.push(b'\r');
                            buf.push(byte);
                            self.state = State::ReadLine {
                                prefix,
                                buf,
                                saw_cr: false,
                            };
                        }
                    } else if byte == b'\r' {
                        self.state = State::ReadLine {
                            prefix,
                            buf,
                            saw_cr: true,
                        };
                    } else {
                        buf.push(byte);
                        self.state = State::ReadLine {
                            prefix,
                            buf,
                            saw_cr: false,
                        };
                    }
                }

                // ── Reading bulk string body bytes ────────────────────────────
                State::ReadBulkBody { remaining, mut buf } => {
                    // Bulk-copy as many bytes as possible in one operation.
                    // This is the efficiency win over per-byte processing: a 1MB
                    // value is handled in one memcpy, not a million loop iterations.
                    let available = input.len() - i;
                    let to_read = available.min(remaining);
                    buf.extend_from_slice(&input[i..i + to_read]);
                    i += to_read;

                    let new_remaining = remaining - to_read;
                    if new_remaining == 0 {
                        self.state = State::ReadBulkCrlf {
                            data: buf,
                            saw_cr: false,
                        };
                    } else {
                        self.state = State::ReadBulkBody {
                            remaining: new_remaining,
                            buf,
                        };
                    }
                    // Note: when remaining==0, i may not advance (to_read==0). That's
                    // intentional — the next iteration will process the current byte
                    // in the new ReadBulkCrlf state.
                }

                // ── Consuming the \r\n after a bulk string body ───────────────
                State::ReadBulkCrlf { data, saw_cr } => {
                    let byte = input[i];
                    i += 1;

                    if saw_cr && byte == b'\n' {
                        // Bulk string complete.
                        self.deliver(RespValue::BulkString(data), &mut output);
                        // self.state stays Start
                    } else if byte == b'\r' {
                        self.state = State::ReadBulkCrlf { data, saw_cr: true };
                    } else {
                        // Unexpected byte — stay in CrLf state (lenient).
                        self.state = State::ReadBulkCrlf {
                            data,
                            saw_cr: false,
                        };
                    }
                }
            }
        }

        Ok(output)
    }

    /// Interpret a completed header line given its type prefix byte.
    ///
    /// Returns `Some(value)` for immediately-complete types: `+`, `-`, `:`, `$-1`.
    /// Returns `None` for types requiring more data: `$N` (sets state to ReadBulkBody)
    /// and `*N` (pushes an ArrayFrame onto the stack).
    fn interpret_line(
        &mut self,
        prefix: u8,
        line: Vec<u8>,
    ) -> Result<Option<RespValue>, ParseError> {
        match prefix {
            b'+' => Ok(Some(RespValue::SimpleString(line))),
            b'-' => Ok(Some(RespValue::Error(line))),
            b':' => Ok(Some(RespValue::Integer(parse_i64(&line)?))),

            b'$' => {
                let len = parse_i64(&line)?;
                match len {
                    -1 => Ok(Some(RespValue::Null)),
                    n if n < -1 => Err(ParseError::NegativeLength),
                    0 => {
                        // $0\r\n\r\n — empty bulk string. Skip body; just consume \r\n.
                        self.state = State::ReadBulkCrlf {
                            data: Vec::new(),
                            saw_cr: false,
                        };
                        Ok(None)
                    }
                    n => {
                        // REDIS: Redis validates n against proto-max-bulk-len (512 MB by
                        // default, configured via `proto-max-bulk-len`). We skip that.
                        self.state = State::ReadBulkBody {
                            remaining: n as usize,
                            buf: Vec::with_capacity(n as usize),
                        };
                        Ok(None)
                    }
                }
            }

            b'*' => {
                let count = parse_i64(&line)?;
                if count <= 0 {
                    // *0\r\n = empty array; *-1\r\n = null array (we treat as empty).
                    Ok(Some(RespValue::Array(Vec::new())))
                } else {
                    // Push a new frame; subsequent values will fill it.
                    // REDIS: `c->multibulklen = count` (networking.c)
                    self.array_stack.push(ArrayFrame {
                        expected: count as usize,
                        items: Vec::with_capacity(count as usize),
                    });
                    Ok(None)
                }
            }

            _ => unreachable!("prefix byte was validated before entering ReadLine"),
        }
    }

    /// Deliver a completed value: push it into the innermost in-progress array,
    /// or emit it to `output` if no array is active.
    ///
    /// Handles cascading completions: if delivering a value fills an array, that
    /// array itself becomes a value and is delivered to its parent (and so on).
    fn deliver(&mut self, value: RespValue, output: &mut Vec<RespValue>) {
        // We use a Vec as a mini-queue to avoid recursion. In practice the
        // queue never holds more than one element for typical Redis commands
        // (max nesting depth = 1).
        let mut pending = vec![value];

        while let Some(v) = pending.pop() {
            if self.array_stack.is_empty() {
                output.push(v);
                continue;
            }

            // Use index-based access to avoid holding a &mut borrow on
            // array_stack while we might also need to call pop().
            let idx = self.array_stack.len() - 1;
            self.array_stack[idx].items.push(v);

            if self.array_stack[idx].items.len() >= self.array_stack[idx].expected {
                // This array is full — pop it and re-deliver it as a completed value.
                let frame = self.array_stack.pop().unwrap();
                pending.push(RespValue::Array(frame.items));
            }
            // Otherwise: value stored, array still needs more elements — done.
        }
    }
}

// ─── Serializer ──────────────────────────────────────────────────────────────

/// Serialize a [`RespValue`] to its RESP2 wire representation.
///
/// REDIS: The server writes responses via `addReply*()` family of functions
/// (networking.c), which write directly into a per-client output buffer.
/// We serialize to a `Vec<u8>` for simplicity; the caller writes it to the socket.
pub fn serialize(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(value, &mut out);
    out
}

fn write_value(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(s) => {
            out.push(b'+');
            out.extend_from_slice(s);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            out.push(b'-');
            out.extend_from_slice(e);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            // Format directly into the output buffer.
            out.extend_from_slice(format!(":{n}\r\n").as_bytes());
        }
        RespValue::BulkString(data) => {
            // Length prefix, then body, then CRLF terminator.
            out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Null => {
            out.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Array(items) => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            for item in items {
                write_value(item, out);
            }
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn parse_i64(buf: &[u8]) -> Result<i64, ParseError> {
    std::str::from_utf8(buf)
        .map_err(|_| ParseError::InvalidNumber)?
        .trim()
        .parse::<i64>()
        .map_err(|_| ParseError::InvalidNumber)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Vec<RespValue> {
        Parser::new().feed(input).unwrap()
    }

    // ── Individual types ──────────────────────────────────────────────────────

    #[test]
    fn simple_string() {
        assert_eq!(
            parse_all(b"+OK\r\n"),
            vec![RespValue::SimpleString(b"OK".to_vec())]
        );
    }

    #[test]
    fn error() {
        assert_eq!(
            parse_all(b"-ERR unknown command\r\n"),
            vec![RespValue::Error(b"ERR unknown command".to_vec())]
        );
    }

    #[test]
    fn integer() {
        assert_eq!(parse_all(b":42\r\n"), vec![RespValue::Integer(42)]);
        assert_eq!(parse_all(b":-1\r\n"), vec![RespValue::Integer(-1)]);
    }

    #[test]
    fn bulk_string() {
        assert_eq!(
            parse_all(b"$5\r\nhello\r\n"),
            vec![RespValue::BulkString(b"hello".to_vec())]
        );
    }

    #[test]
    fn null_bulk_string() {
        assert_eq!(parse_all(b"$-1\r\n"), vec![RespValue::Null]);
    }

    #[test]
    fn empty_bulk_string() {
        assert_eq!(
            parse_all(b"$0\r\n\r\n"),
            vec![RespValue::BulkString(b"".to_vec())]
        );
    }

    // ── Binary safety ─────────────────────────────────────────────────────────
    //
    // This is THE key property of bulk strings: the length prefix means the
    // parser reads exactly N bytes without scanning for delimiters. A bulk
    // string can contain \r\n (or any bytes) without confusing the parser.

    #[test]
    fn bulk_string_containing_crlf() {
        // "hello\r\nworld" — the \r\n is INSIDE the value, not a terminator.
        let input = b"$12\r\nhello\r\nworld\r\n";
        assert_eq!(
            parse_all(input),
            vec![RespValue::BulkString(b"hello\r\nworld".to_vec())]
        );
    }

    #[test]
    fn bulk_string_containing_null_bytes() {
        let input = b"$5\r\nhe\x00lo\r\n";
        assert_eq!(
            parse_all(input),
            vec![RespValue::BulkString(b"he\x00lo".to_vec())]
        );
    }

    // ── Commands (arrays of bulk strings) ─────────────────────────────────────

    #[test]
    fn ping_command() {
        // redis-cli sends: *1\r\n$4\r\nPING\r\n
        let result = parse_all(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(
            result,
            vec![RespValue::Array(vec![RespValue::BulkString(
                b"PING".to_vec()
            )])]
        );
    }

    #[test]
    fn set_command() {
        // SET foo bar → *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let result = parse_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(
            result,
            vec![RespValue::Array(vec![
                RespValue::BulkString(b"SET".to_vec()),
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])]
        );
    }

    // ── Streaming (byte-by-byte) ──────────────────────────────────────────────
    //
    // The critical correctness test: feed the parser one byte at a time.
    // This exercises every possible TCP fragmentation point.

    #[test]
    fn streaming_byte_by_byte() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut p = Parser::new();
        let mut all = Vec::new();
        for byte in input {
            all.extend(p.feed(std::slice::from_ref(byte)).unwrap());
        }
        assert_eq!(
            all,
            vec![RespValue::Array(vec![
                RespValue::BulkString(b"SET".to_vec()),
                RespValue::BulkString(b"foo".to_vec()),
                RespValue::BulkString(b"bar".to_vec()),
            ])]
        );
    }

    #[test]
    fn streaming_split_at_crlf() {
        // Split so the \r and \n of a terminator arrive in separate chunks.
        let mut p = Parser::new();
        let part1 = b"*1\r\n$4\r\nPING\r"; // ends mid-terminator
        let part2 = b"\n";
        let mut all = p.feed(part1).unwrap();
        all.extend(p.feed(part2).unwrap());
        assert_eq!(
            all,
            vec![RespValue::Array(vec![RespValue::BulkString(
                b"PING".to_vec()
            )])]
        );
    }

    // ── Pipelining ───────────────────────────────────────────────────────────
    //
    // Redis clients may send multiple commands without waiting for replies
    // ("pipelining"). They'll arrive in a single read() call and we must
    // parse all of them.

    #[test]
    fn pipelining_two_commands() {
        let input = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let result = parse_all(input);
        let ping = RespValue::Array(vec![RespValue::BulkString(b"PING".to_vec())]);
        assert_eq!(result, vec![ping.clone(), ping]);
    }

    // ── Serializer roundtrip ──────────────────────────────────────────────────

    #[test]
    fn serialize_roundtrip() {
        let values = vec![
            RespValue::SimpleString(b"OK".to_vec()),
            RespValue::Error(b"ERR test".to_vec()),
            RespValue::Integer(-42),
            RespValue::BulkString(b"binary\r\nsafe".to_vec()),
            RespValue::Null,
            RespValue::Array(vec![
                RespValue::BulkString(b"SET".to_vec()),
                RespValue::BulkString(b"key".to_vec()),
                RespValue::BulkString(b"value".to_vec()),
            ]),
        ];
        for v in &values {
            let wire = serialize(v);
            let parsed = parse_all(&wire);
            assert_eq!(parsed, vec![v.clone()], "roundtrip failed for {v:?}");
        }
    }
}
