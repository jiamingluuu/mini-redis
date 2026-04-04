//! RDB snapshot encoder and decoder.
//!
//! REDIS: RDB (rdb.c / rdb.h) is Redis's point-in-time binary snapshot format.
//! It trades recovery granularity (you lose writes since last snapshot) for fast
//! startup — binary load vs replaying every command in AOF. Production Redis
//! uses both together: AOF for durability, RDB for fast restarts and replication
//! seeding.
//!
//! We implement the simplest valid subset of RDB v9: uncompressed strings, plain
//! hash encoding (type 4), and plain list encoding (type 1). These are the
//! oldest encodings and remain valid — `redis-cli --rdb` can read them.

use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::crc64::crc64;
use crate::db::{Db, DbSnapshot};
use crate::entry;
use crate::eviction::EvictionPolicy;
use crate::object::RedisObject;
use crate::types::hash::Hash;
use crate::types::list::List;

// ---------------------------------------------------------------------------
// RDB opcodes and type tags
// ---------------------------------------------------------------------------

const RDB_MAGIC: &[u8] = b"REDIS";
const RDB_VERSION: &[u8] = b"0009";

const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
#[allow(dead_code)]
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EOF: u8 = 0xFF;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_HASH: u8 = 4;

// ---------------------------------------------------------------------------
// RDB config
// ---------------------------------------------------------------------------

/// Configuration for the RDB subsystem.
#[derive(Debug, Clone)]
pub(crate) struct RdbConfig {
    pub(crate) path: std::path::PathBuf,
    pub(crate) enabled: bool,
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Encode the entire database as an RDB file.
///
/// REDIS: `rdbSave()` in rdb.c — iterates every DB (we only have DB 0),
/// writes the header, AUX fields, DB selector, resize hint, all key-value
/// pairs, EOF marker, and CRC64 checksum.
pub(crate) fn encode(db: &Db, path: &Path) -> io::Result<()> {
    let snapshot_ms = entry::now_ms();
    let snapshot = db.snapshot();
    encode_snapshot(&snapshot, path, snapshot_ms)
}

/// Encode a point-in-time snapshot to an RDB file.
///
/// `snapshot_ms` is the linearization timestamp captured by the caller at
/// snapshot start (Store actor when handling BGSAVE). Keys whose TTL is at or
/// before `snapshot_ms` are omitted from the dump.
pub(crate) fn encode_snapshot(
    snapshot: &DbSnapshot,
    path: &Path,
    snapshot_ms: u64,
) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();

    // -- Header --
    buf.extend_from_slice(RDB_MAGIC);
    buf.extend_from_slice(RDB_VERSION);

    // -- AUX fields --
    write_aux(&mut buf, b"redis-ver", b"7.0.0");
    let ctime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    write_aux(&mut buf, b"ctime", ctime.to_string().as_bytes());

    // -- DB 0 --
    buf.push(RDB_OPCODE_SELECTDB);
    write_length(&mut buf, 0);

    let mut live_keys = 0u64;
    let mut live_expiry_keys = 0u64;
    for (_, entry) in snapshot.iter() {
        if !include_in_snapshot(entry, snapshot_ms) {
            continue;
        }
        live_keys += 1;
        if entry.expires_at().is_some() {
            live_expiry_keys += 1;
        }
    }

    buf.push(RDB_OPCODE_RESIZEDB);
    write_length(&mut buf, live_keys);
    write_length(&mut buf, live_expiry_keys);

    // -- Key-value pairs --
    // REDIS: rdbSaveKeyValuePair() in rdb.c emits the expiry opcode before
    // the type+key+value triple when the key has a TTL.
    for (key, entry) in snapshot.iter() {
        if !include_in_snapshot(entry, snapshot_ms) {
            continue;
        }

        if let Some(expires_at) = entry.expires_at() {
            buf.push(RDB_OPCODE_EXPIRETIME_MS);
            buf.extend_from_slice(&expires_at.to_le_bytes());
        }
        write_key_value(&mut buf, key, entry.obj());
    }

    // -- EOF + checksum --
    buf.push(RDB_OPCODE_EOF);
    let checksum = crc64(0, &buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    // Atomic write: write to temp file then rename.
    // REDIS: rdbSave() writes to a temp file and renames for crash safety.
    let tmp_path = path.with_extension("rdb.tmp");
    let mut file = std::fs::File::create(&tmp_path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    std::fs::rename(&tmp_path, path)?;

    Ok(())
}

fn include_in_snapshot(entry: &crate::entry::Entry, snapshot_ms: u64) -> bool {
    match entry.expires_at() {
        Some(expires_at) => expires_at > snapshot_ms,
        None => true,
    }
}

fn write_aux(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.push(RDB_OPCODE_AUX);
    write_string(buf, key);
    write_string(buf, value);
}

fn write_key_value(buf: &mut Vec<u8>, key: &str, obj: &RedisObject) {
    match obj {
        RedisObject::Str(val) => {
            buf.push(RDB_TYPE_STRING);
            write_string(buf, key.as_bytes());
            write_string(buf, val);
        }
        RedisObject::List(list) => {
            buf.push(RDB_TYPE_LIST);
            write_string(buf, key.as_bytes());
            write_list(buf, list);
        }
        RedisObject::Hash(hash) => {
            buf.push(RDB_TYPE_HASH);
            write_string(buf, key.as_bytes());
            write_hash(buf, hash);
        }
    }
}

fn write_list(buf: &mut Vec<u8>, list: &List) {
    write_length(buf, list.len() as u64);
    for elem in list.iter() {
        write_string(buf, elem);
    }
}

fn write_hash(buf: &mut Vec<u8>, hash: &Hash) {
    write_length(buf, hash.len() as u64);
    for (field, value) in hash.iter() {
        write_string(buf, field);
        write_string(buf, value);
    }
}

/// Write a length-prefixed raw string (no LZF compression, no integer encoding).
fn write_string(buf: &mut Vec<u8>, data: &[u8]) {
    write_length(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Encode a length using Redis's variable-length encoding.
///
/// REDIS: rdb.h `rdbSaveLen()`:
/// - 0–63:       1 byte  `00xxxxxx`
/// - 64–16383:   2 bytes `01xxxxxx xxxxxxxx`
/// - 16384+:     5 bytes `10000000` + 4 bytes big-endian
fn write_length(buf: &mut Vec<u8>, len: u64) {
    if len < 64 {
        buf.push(len as u8);
    } else if len < 16384 {
        buf.push(0x40 | ((len >> 8) as u8));
        buf.push((len & 0xFF) as u8);
    } else {
        buf.push(0x80);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// Decode an RDB file into a `Db`.
///
/// REDIS: `rdbLoad()` in rdb.c — reads header, processes opcodes, loads
/// key-value pairs, verifies CRC64 checksum.
pub(crate) fn decode(path: &Path) -> anyhow::Result<Db> {
    let data = std::fs::read(path)?;
    if data.len() < 9 {
        anyhow::bail!("RDB file too short");
    }

    let mut cursor = io::Cursor::new(data.as_slice());

    // -- Header --
    let mut magic = [0u8; 5];
    cursor.read_exact(&mut magic)?;
    if magic != RDB_MAGIC {
        anyhow::bail!("invalid RDB magic: expected 'REDIS'");
    }

    let mut version = [0u8; 4];
    cursor.read_exact(&mut version)?;
    // We accept any version — we only use opcodes that exist in all versions.

    // -- Verify CRC64 checksum --
    // The last 8 bytes are the checksum over everything before them.
    if data.len() >= 17 {
        let payload = &data[..data.len() - 8];
        let stored_crc = u64::from_le_bytes(
            data[data.len() - 8..]
                .try_into()
                .map_err(|_| anyhow::anyhow!("RDB checksum read failed"))?,
        );
        let computed_crc = crc64(0, payload);
        if stored_crc != computed_crc {
            anyhow::bail!(
                "RDB checksum mismatch: stored {stored_crc:#x}, computed {computed_crc:#x}"
            );
        }
    }

    let mut db = Db::new(EvictionPolicy::NoEviction);

    // -- Process opcodes --
    // REDIS: rdbLoadRio() in rdb.c reads opcodes in a loop. Expiry opcodes
    // (0xFC for ms, 0xFD for seconds) precede the type+key+value triple.
    let mut pending_expiry: Option<u64> = None;

    loop {
        let opcode = read_byte(&mut cursor)?;
        match opcode {
            RDB_OPCODE_EXPIRETIME_MS => {
                // 8-byte little-endian epoch milliseconds
                let mut ts_buf = [0u8; 8];
                cursor.read_exact(&mut ts_buf)?;
                pending_expiry = Some(u64::from_le_bytes(ts_buf));
            }
            RDB_OPCODE_EXPIRETIME => {
                // 4-byte little-endian epoch seconds → convert to ms
                let mut ts_buf = [0u8; 4];
                cursor.read_exact(&mut ts_buf)?;
                pending_expiry = Some(u32::from_le_bytes(ts_buf) as u64 * 1000);
            }
            RDB_OPCODE_AUX => {
                let _key = read_string(&mut cursor)?;
                let _val = read_string(&mut cursor)?;
            }
            RDB_OPCODE_SELECTDB => {
                let _db_index = read_length(&mut cursor)?;
            }
            RDB_OPCODE_RESIZEDB => {
                let _db_size = read_length(&mut cursor)?;
                let _expires_size = read_length(&mut cursor)?;
            }
            RDB_OPCODE_EOF => {
                break;
            }
            // Type tags: key-value pair
            type_byte => {
                let key = read_string(&mut cursor)?;
                let key_str = String::from_utf8(key.to_vec())
                    .map_err(|e| anyhow::anyhow!("non-UTF8 key: {e}"))?;
                let obj = read_object(type_byte, &mut cursor)?;
                let expiry = pending_expiry.take();
                db.set_with_expiry(key_str, obj, expiry);
            }
        }
    }

    Ok(db)
}

fn read_byte(cursor: &mut io::Cursor<&[u8]>) -> anyhow::Result<u8> {
    let mut b = [0u8; 1];
    cursor
        .read_exact(&mut b)
        .map_err(|e| anyhow::anyhow!("unexpected end of RDB: {e}"))?;
    Ok(b[0])
}

/// Decode a Redis length-encoded integer.
fn read_length(cursor: &mut io::Cursor<&[u8]>) -> anyhow::Result<u64> {
    let first = read_byte(cursor)?;
    let enc_type = (first & 0xC0) >> 6;
    match enc_type {
        0 => Ok((first & 0x3F) as u64),
        1 => {
            let second = read_byte(cursor)?;
            Ok((((first & 0x3F) as u64) << 8) | second as u64)
        }
        2 => {
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            Ok(u32::from_be_bytes(buf) as u64)
        }
        3 => {
            // Special encoding (integer as string, LZF compressed, etc.)
            // REDIS: enc_type 3 means the length is actually a "special format"
            // indicator. The lower 6 bits tell us the sub-type:
            //   0 = 8-bit integer, 1 = 16-bit, 2 = 32-bit, 3 = LZF compressed
            let sub = first & 0x3F;
            match sub {
                0 => {
                    // 8-bit integer stored as string
                    let val = read_byte(cursor)?;
                    Ok(val as u64)
                }
                1 => {
                    // 16-bit integer stored as string (little-endian)
                    let mut buf = [0u8; 2];
                    cursor.read_exact(&mut buf)?;
                    Ok(u16::from_le_bytes(buf) as u64)
                }
                2 => {
                    // 32-bit integer stored as string (little-endian)
                    let mut buf = [0u8; 4];
                    cursor.read_exact(&mut buf)?;
                    Ok(u32::from_le_bytes(buf) as u64)
                }
                _ => anyhow::bail!("unsupported RDB special encoding: sub={sub}"),
            }
        }
        _ => unreachable!(),
    }
}

/// Read a length-prefixed string. Handles both plain strings and integer-encoded strings.
fn read_string(cursor: &mut io::Cursor<&[u8]>) -> anyhow::Result<Bytes> {
    let first = read_byte(cursor)?;
    let enc_type = (first & 0xC0) >> 6;

    if enc_type == 3 {
        // Special encoding: integer stored as string
        let sub = first & 0x3F;
        let int_str = match sub {
            0 => {
                let val = read_byte(cursor)? as i8;
                val.to_string()
            }
            1 => {
                let mut buf = [0u8; 2];
                cursor.read_exact(&mut buf)?;
                (i16::from_le_bytes(buf)).to_string()
            }
            2 => {
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                (i32::from_le_bytes(buf)).to_string()
            }
            _ => anyhow::bail!("unsupported RDB string encoding: sub={sub}"),
        };
        return Ok(Bytes::from(int_str));
    }

    // Not a special encoding — put the first byte back in context and read length normally.
    // We need to re-decode the length from the first byte.
    let len = match enc_type {
        0 => (first & 0x3F) as u64,
        1 => {
            let second = read_byte(cursor)?;
            (((first & 0x3F) as u64) << 8) | second as u64
        }
        2 => {
            let mut buf = [0u8; 4];
            cursor.read_exact(&mut buf)?;
            u32::from_be_bytes(buf) as u64
        }
        _ => unreachable!(),
    };

    let mut buf = vec![0u8; len as usize];
    cursor.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

fn read_object(type_byte: u8, cursor: &mut io::Cursor<&[u8]>) -> anyhow::Result<RedisObject> {
    match type_byte {
        RDB_TYPE_STRING => {
            let val = read_string(cursor)?;
            Ok(RedisObject::Str(val))
        }
        RDB_TYPE_LIST => {
            let len = read_length(cursor)?;
            let mut list = List::new();
            for _ in 0..len {
                let elem = read_string(cursor)?;
                list.push_back(elem);
            }
            Ok(RedisObject::List(list))
        }
        RDB_TYPE_HASH => {
            let len = read_length(cursor)?;
            let mut hash = Hash::new();
            for _ in 0..len {
                let field = read_string(cursor)?;
                let value = read_string(cursor)?;
                hash.insert(field, value);
            }
            Ok(RedisObject::Hash(hash))
        }
        _ => anyhow::bail!("unsupported RDB type: {type_byte}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::hash::Hash;
    use crate::types::list::List;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    // --- Length encoding ---

    #[test]
    fn length_encoding_small() {
        let mut buf = Vec::new();
        write_length(&mut buf, 42);
        assert_eq!(buf, vec![42]);
    }

    #[test]
    fn length_encoding_medium() {
        let mut buf = Vec::new();
        write_length(&mut buf, 256);
        assert_eq!(buf, vec![0x41, 0x00]);
    }

    #[test]
    fn length_encoding_large() {
        let mut buf = Vec::new();
        write_length(&mut buf, 100_000);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], 0x80);
        assert_eq!(
            u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
            100_000
        );
    }

    #[test]
    fn length_round_trip() {
        for len in [0, 1, 63, 64, 16383, 16384, 100_000, u32::MAX as u64] {
            let mut buf = Vec::new();
            write_length(&mut buf, len);
            let mut cursor = io::Cursor::new(buf.as_slice());
            let decoded = read_length(&mut cursor).unwrap();
            assert_eq!(decoded, len, "round-trip failed for {len}");
        }
    }

    // --- String encoding ---

    #[test]
    fn string_round_trip() {
        let cases = [b"" as &[u8], b"hello", &vec![0xFFu8; 1000]];
        for data in cases {
            let mut buf = Vec::new();
            write_string(&mut buf, data);
            let mut cursor = io::Cursor::new(buf.as_slice());
            let decoded = read_string(&mut cursor).unwrap();
            assert_eq!(decoded.as_ref(), data);
        }
    }

    // --- Empty Db round-trip ---

    #[test]
    fn encode_decode_empty_db() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");
        let db = Db::new(EvictionPolicy::NoEviction);
        encode(&db, &path).unwrap();
        let loaded = decode(&path).unwrap();
        assert_eq!(loaded.len(), 0);
    }

    // --- String round-trip ---

    #[test]
    fn encode_decode_strings() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("key1".into(), RedisObject::Str(b("value1")));
        db.set("key2".into(), RedisObject::Str(b("value2")));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.get_str("key1").unwrap().unwrap(), &b("value1"));
        assert_eq!(loaded.get_str("key2").unwrap().unwrap(), &b("value2"));
    }

    // --- Hash round-trip ---

    #[test]
    fn encode_decode_hash() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let mut hash = Hash::new();
        hash.insert(b("field1"), b("val1"));
        hash.insert(b("field2"), b("val2"));
        db.set("myhash".into(), RedisObject::Hash(hash));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();

        let h = loaded.get_hash("myhash").unwrap().unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h.get(&b("field1")).unwrap(), &b("val1"));
        assert_eq!(h.get(&b("field2")).unwrap(), &b("val2"));
    }

    // --- List round-trip ---

    #[test]
    fn encode_decode_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let mut list = List::new();
        list.push_back(b("a"));
        list.push_back(b("b"));
        list.push_back(b("c"));
        db.set("mylist".into(), RedisObject::List(list));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();

        let l = loaded.get_list("mylist").unwrap().unwrap();
        assert_eq!(l.len(), 3);
        assert_eq!(l.range(0, -1), vec![b("a"), b("b"), b("c")]);
    }

    // --- Mixed types round-trip ---

    #[test]
    fn encode_decode_mixed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("s".into(), RedisObject::Str(b("hello")));

        let mut hash = Hash::new();
        hash.insert(b("f"), b("v"));
        db.set("h".into(), RedisObject::Hash(hash));

        let mut list = List::new();
        list.push_back(b("x"));
        db.set("l".into(), RedisObject::List(list));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get_str("s").unwrap().unwrap(), &b("hello"));
        assert_eq!(loaded.get_hash("h").unwrap().unwrap().len(), 1);
        assert_eq!(loaded.get_list("l").unwrap().unwrap().len(), 1);
    }

    // --- CRC64 checksum verification ---

    #[test]
    fn decode_detects_corrupted_checksum() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let db = Db::new(EvictionPolicy::NoEviction);
        encode(&db, &path).unwrap();

        // Corrupt the last byte (part of the CRC64 checksum)
        let mut data = std::fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let result = decode(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    // --- Edge cases ---

    #[test]
    fn encode_decode_empty_hash() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("empty_hash".into(), RedisObject::Hash(Hash::new()));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();
        assert_eq!(loaded.get_hash("empty_hash").unwrap().unwrap().len(), 0);
    }

    #[test]
    fn encode_decode_empty_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("empty_list".into(), RedisObject::List(List::new()));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();
        assert_eq!(loaded.get_list("empty_list").unwrap().unwrap().len(), 0);
    }

    #[test]
    fn encode_decode_large_value() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let big = Bytes::from(vec![b'x'; 100_000]);
        db.set("big".into(), RedisObject::Str(big.clone()));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();
        assert_eq!(loaded.get_str("big").unwrap().unwrap(), &big);
    }

    // --- Expiry round-trip ---

    #[test]
    fn encode_decode_preserves_expiry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let far_future = crate::entry::now_ms() + 3_600_000; // 1 hour from now
        db.set_with_expiry(
            "exp_key".into(),
            RedisObject::Str(b("val")),
            Some(far_future),
        );
        db.set("no_exp".into(), RedisObject::Str(b("val2")));

        encode(&db, &path).unwrap();
        let loaded = decode(&path).unwrap();

        // Key with expiry: timestamp preserved
        let loaded_exp = loaded.get_expiry("exp_key");
        assert_eq!(loaded_exp, Some(Some(far_future)));

        // Key without expiry: no timestamp
        let loaded_no_exp = loaded.get_expiry("no_exp");
        assert_eq!(loaded_no_exp, Some(None));
    }

    #[test]
    fn encode_decode_expiry_count_in_resizedb() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let future = crate::entry::now_ms() + 60_000;
        db.set_with_expiry("a".into(), RedisObject::Str(b("v")), Some(future));
        db.set_with_expiry("b".into(), RedisObject::Str(b("v")), Some(future));
        db.set("c".into(), RedisObject::Str(b("v")));

        assert_eq!(db.expiry_count(), 2);

        encode(&db, &path).unwrap();
        let loaded = decode(&path).unwrap();
        assert_eq!(loaded.expiry_count(), 2);
        assert_eq!(loaded.len(), 3);
    }

    #[test]
    fn expired_key_from_rdb_behaves_correctly() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        // Set expiry in the past
        db.set_with_expiry("stale".into(), RedisObject::Str(b("old")), Some(1));
        db.set("fresh".into(), RedisObject::Str(b("new")));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();

        // Stale key should be lazily expired on access
        assert_eq!(loaded.get_str("stale"), Ok(None));
        // Fresh key should be accessible
        assert_eq!(loaded.get_str("fresh").unwrap().unwrap(), &b("new"));
    }

    #[test]
    fn encode_snapshot_excludes_key_expired_at_boundary() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set_with_expiry("expired".into(), RedisObject::Str(b("v")), Some(1000));
        db.set_with_expiry("live".into(), RedisObject::Str(b("v")), Some(1001));

        let snapshot = db.snapshot();
        encode_snapshot(&snapshot, &path, 1000).unwrap();
        let loaded = decode(&path).unwrap();

        assert_eq!(loaded.get_expiry("expired"), None);
        assert_eq!(loaded.get_expiry("live"), Some(Some(1001)));
    }

    #[test]
    fn encode_snapshot_includes_future_expiry_even_if_wall_clock_moves() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        let snapshot_ms = crate::entry::now_ms();
        let expires_at = snapshot_ms + 50;
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(expires_at));

        let snapshot = db.snapshot();
        std::thread::sleep(std::time::Duration::from_millis(60));

        encode_snapshot(&snapshot, &path, snapshot_ms).unwrap();
        let loaded = decode(&path).unwrap();
        assert_eq!(loaded.get_expiry("k"), Some(Some(expires_at)));
    }

    #[test]
    fn decoded_db_can_adopt_runtime_eviction_policy() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.rdb");

        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("k".into(), RedisObject::Str(b("v")));

        encode(&db, &path).unwrap();
        let mut loaded = decode(&path).unwrap();
        loaded.set_eviction_policy(crate::eviction::EvictionPolicy::AllKeysLru);

        let before = loaded.get_entry("k").unwrap().lru_clock();
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(loaded.get_str("k"), Ok(Some(&b("v"))));
        let after = loaded.get_entry("k").unwrap().lru_clock();
        assert!(after > before, "expected LRU clock to update after access");
    }
}
