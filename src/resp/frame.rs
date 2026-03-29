use bytes::Bytes;

/// A single RESP (REdis Serialization Protocol) value.
///
/// REDIS: Every value on the wire starts with a type byte that allows O(1)
/// dispatch. Redis uses the same five types in resp.c. We add Null as a
/// first-class variant rather than encoding it as Option<Frame> — this keeps
/// call sites explicit about intent (e.g. a missing GET key vs. a real error).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Frame {
    /// `+OK\r\n` — short, non-binary status replies
    Simple(String),
    /// `-ERR message\r\n` — error replies
    Error(String),
    /// `:1000\r\n` — signed 64-bit integer
    Integer(i64),
    /// `$6\r\nfoobar\r\n` — binary-safe byte string
    Bulk(Bytes),
    /// `*2\r\n...` — ordered list of frames
    Array(Vec<Frame>),
    /// `$-1\r\n` or `*-1\r\n` — Redis null (absent value / null array)
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_frame_equality() {
        assert_eq!(Frame::Simple("OK".into()), Frame::Simple("OK".into()));
        assert_ne!(Frame::Simple("OK".into()), Frame::Simple("PONG".into()));
    }

    #[test]
    fn bulk_frame_equality() {
        let a = Frame::Bulk(Bytes::from_static(b"hello"));
        let b = Frame::Bulk(Bytes::from_static(b"hello"));
        assert_eq!(a, b);
    }

    #[test]
    fn null_frame_equality() {
        assert_eq!(Frame::Null, Frame::Null);
        assert_ne!(Frame::Null, Frame::Simple("".into()));
    }

    #[test]
    fn array_frame_contains_children() {
        let arr = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"GET")),
            Frame::Bulk(Bytes::from_static(b"foo")),
        ]);
        if let Frame::Array(items) = arr {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected Array");
        }
    }
}
