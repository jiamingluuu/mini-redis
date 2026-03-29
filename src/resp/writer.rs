use bytes::{BufMut, BytesMut};

use super::frame::Frame;

/// Encode `frame` into `buf` using the RESP wire format.
///
/// REDIS: Redis encodes replies in addReply* functions in src/networking.c.
/// We keep encoding and decoding symmetric: the same Frame type goes in and
/// out, which makes round-trip tests straightforward.
/// Encode `frame` to a fresh `Bytes` buffer — convenience wrapper around `encode`.
pub(crate) fn frame_to_bytes(frame: &Frame) -> bytes::Bytes {
    let mut buf = BytesMut::new();
    encode(frame, &mut buf);
    buf.freeze()
}

pub(crate) fn encode(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::Simple(s) => {
            buf.put_u8(b'+');
            buf.put(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.put(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            buf.put(n.to_string().as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::Bulk(data) => {
            buf.put_u8(b'$');
            buf.put(data.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Array(items) => {
            buf.put_u8(b'*');
            buf.put(items.len().to_string().as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                encode(item, buf);
            }
        }
        Frame::Null => {
            // REDIS: null bulk string is the standard nil representation.
            // Some clients also accept *-1\r\n (null array) but $-1\r\n is
            // the canonical form Redis uses for scalar nil replies.
            buf.put_slice(b"$-1\r\n");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests — verify round-trip correctness for every variant
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn encoded(frame: &Frame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode(frame, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn encode_simple() {
        assert_eq!(encoded(&Frame::Simple("OK".into())), b"+OK\r\n");
    }

    #[test]
    fn encode_error() {
        assert_eq!(encoded(&Frame::Error("ERR bad".into())), b"-ERR bad\r\n");
    }

    #[test]
    fn encode_integer() {
        assert_eq!(encoded(&Frame::Integer(42)), b":42\r\n");
        assert_eq!(encoded(&Frame::Integer(-1)), b":-1\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        assert_eq!(
            encoded(&Frame::Bulk(Bytes::from_static(b"hello"))),
            b"$5\r\nhello\r\n"
        );
    }

    #[test]
    fn encode_empty_bulk_string() {
        assert_eq!(
            encoded(&Frame::Bulk(Bytes::from_static(b""))),
            b"$0\r\n\r\n"
        );
    }

    #[test]
    fn encode_null() {
        assert_eq!(encoded(&Frame::Null), b"$-1\r\n");
    }

    #[test]
    fn encode_array() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from_static(b"key")),
            Frame::Bulk(Bytes::from_static(b"val")),
        ]);
        assert_eq!(
            encoded(&frame),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n"
        );
    }

    #[test]
    fn round_trip_through_parser() {
        use crate::resp::parser;

        let frames = vec![
            Frame::Simple("PONG".into()),
            Frame::Error("ERR oops".into()),
            Frame::Integer(100),
            Frame::Bulk(Bytes::from_static(b"binary\x00data")),
            Frame::Null,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"GET")),
                Frame::Bulk(Bytes::from_static(b"k")),
            ]),
        ];

        for original in frames {
            let mut buf = BytesMut::new();
            encode(&original, &mut buf);
            let parsed = parser::parse(&mut buf).unwrap().unwrap();
            assert_eq!(parsed, original);
        }
    }
}
