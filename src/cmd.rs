//! Command dispatch and handlers.
//!
//! REDIS: Production Redis uses a hash table of `redisCommand` structs
//! (commands.c, generated from commands/*.json). Each entry has a proc pointer,
//! arity check, flags, and ACL categories. We use a match for simplicity;
//! the structure is the same — name → handler function.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::object::{
    HashEnc, ListEnc, RedisObject, SetEnc, StringEnc, INTSET_MAX_ENTRIES,
    LISTPACK_MAX_ELEMENT_BYTES, LISTPACK_MAX_ENTRIES,
};
use crate::resp::RespValue;
use crate::store::Db;

// ── Dispatch ──────────────────────────────────────────────────────────────────

/// Route a command to its handler. Returns the RESP response.
///
/// `args[0]` is the command name (caller must uppercase it).
pub fn dispatch(db: &mut Db, args: Vec<Vec<u8>>) -> RespValue {
    if args.is_empty() {
        return err("ERR empty command");
    }

    let cmd = args[0].to_ascii_uppercase();
    match cmd.as_slice() {
        // ── General ──────────────────────────────────────────────────────────
        b"DEL" => cmd_del(db, &args[1..]),
        b"EXISTS" => cmd_exists(db, &args[1..]),
        b"TYPE" => cmd_type(db, &args[1..]),
        b"KEYS" => cmd_keys(db, &args[1..]),

        // ── String ───────────────────────────────────────────────────────────
        b"SET" => cmd_set(db, &args[1..]),
        b"GET" => cmd_get(db, &args[1..]),
        b"MSET" => cmd_mset(db, &args[1..]),
        b"MGET" => cmd_mget(db, &args[1..]),
        b"APPEND" => cmd_append(db, &args[1..]),
        b"INCR" => cmd_incr(db, &args[1..], 1),
        b"DECR" => cmd_incr(db, &args[1..], -1),
        b"INCRBY" => cmd_incrby(db, &args[1..]),
        b"DECRBY" => cmd_decrby(db, &args[1..]),
        b"STRLEN" => cmd_strlen(db, &args[1..]),

        // ── List ─────────────────────────────────────────────────────────────
        b"LPUSH" => cmd_push(db, &args[1..], Side::Left),
        b"RPUSH" => cmd_push(db, &args[1..], Side::Right),
        b"LPOP" => cmd_pop(db, &args[1..], Side::Left),
        b"RPOP" => cmd_pop(db, &args[1..], Side::Right),
        b"LRANGE" => cmd_lrange(db, &args[1..]),
        b"LLEN" => cmd_llen(db, &args[1..]),
        b"LINDEX" => cmd_lindex(db, &args[1..]),

        // ── Hash ─────────────────────────────────────────────────────────────
        b"HSET" => cmd_hset(db, &args[1..]),
        b"HGET" => cmd_hget(db, &args[1..]),
        b"HMGET" => cmd_hmget(db, &args[1..]),
        b"HGETALL" => cmd_hgetall(db, &args[1..]),
        b"HDEL" => cmd_hdel(db, &args[1..]),
        b"HLEN" => cmd_hlen(db, &args[1..]),
        b"HKEYS" => cmd_hkeys(db, &args[1..]),
        b"HVALS" => cmd_hvals(db, &args[1..]),
        b"HEXISTS" => cmd_hexists(db, &args[1..]),

        // ── Set ──────────────────────────────────────────────────────────────
        b"SADD" => cmd_sadd(db, &args[1..]),
        b"SREM" => cmd_srem(db, &args[1..]),
        b"SMEMBERS" => cmd_smembers(db, &args[1..]),
        b"SCARD" => cmd_scard(db, &args[1..]),
        b"SISMEMBER" => cmd_sismember(db, &args[1..]),
        b"SUNION" => cmd_sunion(db, &args[1..]),
        b"SINTER" => cmd_sinter(db, &args[1..]),
        b"SDIFF" => cmd_sdiff(db, &args[1..]),

        _ => {
            let name = String::from_utf8_lossy(&cmd);
            RespValue::Error(format!("ERR unknown command `{name}`").into_bytes())
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn err(msg: &str) -> RespValue {
    RespValue::Error(msg.as_bytes().to_vec())
}

fn ok() -> RespValue {
    RespValue::SimpleString(b"OK".to_vec())
}

fn wrong_type() -> RespValue {
    err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Parse bytes as a UTF-8 key (Redis keys are bytes, but we store as String).
fn to_key(b: &[u8]) -> Option<String> {
    std::str::from_utf8(b).ok().map(|s| s.to_owned())
}

/// Try to parse bytes as an i64. Returns None if not a valid decimal integer.
/// REDIS: string2ll() in util.c — strict decimal, no leading zeros, no whitespace.
fn parse_int(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

/// Encode a string value with the most compact encoding.
/// REDIS: tryObjectEncoding() in object.c
fn encode_string(bytes: Vec<u8>) -> StringEnc {
    if let Some(n) = parse_int(&bytes) {
        StringEnc::Int(n)
    } else {
        StringEnc::Raw(bytes)
    }
}

/// Serialize a StringEnc to bytes (for APPEND, STRLEN, GET).
fn string_bytes(enc: &StringEnc) -> Vec<u8> {
    match enc {
        StringEnc::Int(n) => n.to_string().into_bytes(),
        StringEnc::Raw(b) => b.clone(),
    }
}

// ── General commands ──────────────────────────────────────────────────────────

/// DEL key [key ...]
/// Returns the number of keys deleted.
fn cmd_del(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'del' command");
    }
    let count = args
        .iter()
        .filter_map(|k| to_key(k))
        .filter(|k| db.data.remove(k).is_some())
        .count();
    RespValue::Integer(count as i64)
}

/// EXISTS key [key ...]
/// Counts how many of the given keys exist (a key listed N times counts N times).
fn cmd_exists(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'exists' command");
    }
    let count = args
        .iter()
        .filter_map(|k| to_key(k))
        .filter(|k| db.data.contains_key(k))
        .count();
    RespValue::Integer(count as i64)
}

/// TYPE key
/// Returns a status string: "string", "list", "hash", "set", or "none".
fn cmd_type(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'type' command");
    };
    let name = db
        .data
        .get(&key)
        .map(|o| o.type_name())
        .unwrap_or("none");
    RespValue::SimpleString(name.as_bytes().to_vec())
}

/// KEYS pattern
/// Returns all keys matching the glob pattern.
/// REDIS: O(N) scan — production Redis warns against KEYS in production.
/// We support only "*" for now (returns all keys).
fn cmd_keys(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(pattern) = args.first() else {
        return err("ERR wrong number of arguments for 'keys' command");
    };
    let keys: Vec<RespValue> = if pattern == b"*" {
        db.data
            .keys()
            .map(|k| RespValue::BulkString(k.as_bytes().to_vec()))
            .collect()
    } else {
        // Full glob matching is out of scope; return an error for non-* patterns.
        return err("ERR KEYS supports only '*' pattern in this implementation");
    };
    RespValue::Array(keys)
}

// ── String commands ───────────────────────────────────────────────────────────

/// SET key value
/// REDIS: setCommand() in t_string.c. Full Redis SET has NX/XX/GET/EX/PX/EXAT
/// options. We implement the basic form; options come in Phase 5 (expiry).
fn cmd_set(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'set' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let enc = encode_string(args[1].clone());
    db.data.insert(key, RedisObject::Str(enc));
    ok()
}

/// GET key
fn cmd_get(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'get' command");
    };
    match db.data.get(&key) {
        None => RespValue::Null,
        Some(RedisObject::Str(enc)) => RespValue::BulkString(string_bytes(enc)),
        Some(_) => wrong_type(),
    }
}

/// MSET key value [key value ...]
fn cmd_mset(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() || args.len() % 2 != 0 {
        return err("ERR wrong number of arguments for 'mset' command");
    }
    for pair in args.chunks_exact(2) {
        let Some(key) = to_key(&pair[0]) else { continue };
        db.data
            .insert(key, RedisObject::Str(encode_string(pair[1].clone())));
    }
    ok()
}

/// MGET key [key ...]
fn cmd_mget(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'mget' command");
    }
    let results = args
        .iter()
        .map(|k| match to_key(k).and_then(|k| db.data.get(&k)) {
            Some(RedisObject::Str(enc)) => RespValue::BulkString(string_bytes(enc)),
            _ => RespValue::Null, // missing key OR wrong type both return nil
        })
        .collect();
    RespValue::Array(results)
}

/// APPEND key value
/// REDIS: appendCommand() in t_string.c.
/// Appending to an Int key first stringifies it, then concatenates.
fn cmd_append(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'append' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    match db.data.get_mut(&key) {
        None => {
            // Key doesn't exist — create it.
            let enc = encode_string(args[1].clone());
            let len = string_bytes(&enc).len();
            db.data.insert(key, RedisObject::Str(enc));
            RespValue::Integer(len as i64)
        }
        Some(RedisObject::Str(enc)) => {
            // Stringify if Int, then concatenate. Result is always Raw.
            let mut existing = string_bytes(enc);
            existing.extend_from_slice(&args[1]);
            let new_len = existing.len();
            *enc = StringEnc::Raw(existing);
            RespValue::Integer(new_len as i64)
        }
        Some(_) => wrong_type(),
    }
}

/// Shared implementation for INCR / DECR (delta = +1 or -1).
fn cmd_incr(db: &mut Db, args: &[Vec<u8>], delta: i64) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'incr'/'decr' command");
    };
    apply_incr(db, key, delta)
}

/// INCRBY key increment
fn cmd_incrby(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'incrby' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let Some(delta) = parse_int(&args[1]) else {
        return err("ERR value is not an integer or out of range");
    };
    apply_incr(db, key, delta)
}

/// DECRBY key decrement
fn cmd_decrby(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'decrby' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let Some(delta) = parse_int(&args[1]) else {
        return err("ERR value is not an integer or out of range");
    };
    apply_incr(db, key, -delta)
}

/// Core increment logic shared by INCR/DECR/INCRBY/DECRBY.
fn apply_incr(db: &mut Db, key: String, delta: i64) -> RespValue {
    let current: i64 = match db.data.get(&key) {
        None => 0,
        Some(RedisObject::Str(StringEnc::Int(n))) => *n,
        Some(RedisObject::Str(StringEnc::Raw(b))) => {
            // REDIS: On a Raw string, try to parse as decimal.
            // Binary arithmetic is NOT performed (see encoding discussion).
            match parse_int(b) {
                Some(n) => n,
                None => return err("ERR value is not an integer or out of range"),
            }
        }
        Some(_) => return wrong_type(),
    };
    let result = match current.checked_add(delta) {
        Some(n) => n,
        None => return err("ERR increment or decrement would overflow"),
    };
    db.data
        .insert(key, RedisObject::Str(StringEnc::Int(result)));
    RespValue::Integer(result)
}

/// STRLEN key
fn cmd_strlen(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'strlen' command");
    };
    match db.data.get(&key) {
        None => RespValue::Integer(0),
        Some(RedisObject::Str(enc)) => RespValue::Integer(string_bytes(enc).len() as i64),
        Some(_) => wrong_type(),
    }
}

// ── List commands ─────────────────────────────────────────────────────────────

enum Side {
    Left,
    Right,
}

/// LPUSH / RPUSH key element [element ...]
/// Returns the list length after push.
/// REDIS: Redis pushes multiple elements left-to-right, so
/// LPUSH k a b c results in [c, b, a] (last arg ends up at head).
fn cmd_push(db: &mut Db, args: &[Vec<u8>], side: Side) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'push' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let elements = &args[1..];

    let obj = db
        .data
        .entry(key)
        .or_insert_with(|| RedisObject::List(ListEnc::Listpack(Vec::new())));

    let list_enc = match obj {
        RedisObject::List(enc) => enc,
        _ => return wrong_type(),
    };

    for elem in elements {
        match list_enc {
            ListEnc::Listpack(v) => {
                match side {
                    Side::Left => v.insert(0, elem.clone()),
                    Side::Right => v.push(elem.clone()),
                }
                // Promote if either threshold exceeded.
                // REDIS: listTypeTryConversionAppend() in t_list.c
                let needs_promotion = v.len() > LISTPACK_MAX_ENTRIES
                    || elem.len() > LISTPACK_MAX_ELEMENT_BYTES;
                if needs_promotion {
                    let promoted: VecDeque<Vec<u8>> = v.drain(..).collect();
                    *list_enc = ListEnc::Quicklist(promoted);
                }
            }
            ListEnc::Quicklist(vd) => match side {
                Side::Left => vd.push_front(elem.clone()),
                Side::Right => vd.push_back(elem.clone()),
            },
        }
    }

    let len = match list_enc {
        ListEnc::Listpack(v) => v.len(),
        ListEnc::Quicklist(vd) => vd.len(),
    };
    RespValue::Integer(len as i64)
}

/// LPOP / RPOP key [count]
fn cmd_pop(db: &mut Db, args: &[Vec<u8>], side: Side) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'pop' command");
    };
    let count: usize = match args.get(1) {
        Some(b) => match parse_int(b) {
            Some(n) if n >= 0 => n as usize,
            _ => return err("ERR value is not an integer or out of range"),
        },
        None => 1, // default: pop one element
    };

    let Some(obj) = db.data.get_mut(&key) else {
        return RespValue::Null;
    };
    let list_enc = match obj {
        RedisObject::List(enc) => enc,
        _ => return wrong_type(),
    };

    let mut popped: Vec<Vec<u8>> = Vec::with_capacity(count);
    for _ in 0..count {
        let elem = match list_enc {
            ListEnc::Listpack(v) => match side {
                Side::Left => {
                    if v.is_empty() {
                        break;
                    }
                    Some(v.remove(0))
                }
                Side::Right => v.pop(),
            },
            ListEnc::Quicklist(vd) => match side {
                Side::Left => vd.pop_front(),
                Side::Right => vd.pop_back(),
            },
        };
        match elem {
            Some(e) => popped.push(e),
            None => break,
        }
    }

    // Remove the key if the list is now empty.
    let is_empty = match list_enc {
        ListEnc::Listpack(v) => v.is_empty(),
        ListEnc::Quicklist(vd) => vd.is_empty(),
    };
    if is_empty {
        db.data.remove(&key);
    }

    if args.get(1).is_none() {
        // No count argument — return single bulk string or nil.
        popped
            .into_iter()
            .next()
            .map(RespValue::BulkString)
            .unwrap_or(RespValue::Null)
    } else {
        RespValue::Array(popped.into_iter().map(RespValue::BulkString).collect())
    }
}

/// LRANGE key start stop
/// Negative indices count from the tail: -1 is the last element.
fn cmd_lrange(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 {
        return err("ERR wrong number of arguments for 'lrange' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let (Some(start), Some(stop)) = (parse_int(&args[1]), parse_int(&args[2])) else {
        return err("ERR value is not an integer or out of range");
    };

    let Some(obj) = db.data.get(&key) else {
        return RespValue::Array(vec![]);
    };

    let items: Vec<&Vec<u8>> = match obj {
        RedisObject::List(ListEnc::Listpack(v)) => v.iter().collect(),
        RedisObject::List(ListEnc::Quicklist(vd)) => vd.iter().collect(),
        _ => return wrong_type(),
    };

    let len = items.len() as i64;
    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
    let stop = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) } as usize;

    if start > stop || items.is_empty() {
        return RespValue::Array(vec![]);
    }
    let result = items[start..=stop]
        .iter()
        .map(|e| RespValue::BulkString((*e).clone()))
        .collect();
    RespValue::Array(result)
}

/// LLEN key
fn cmd_llen(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'llen' command");
    };
    match db.data.get(&key) {
        None => RespValue::Integer(0),
        Some(RedisObject::List(ListEnc::Listpack(v))) => RespValue::Integer(v.len() as i64),
        Some(RedisObject::List(ListEnc::Quicklist(vd))) => {
            RespValue::Integer(vd.len() as i64)
        }
        Some(_) => wrong_type(),
    }
}

/// LINDEX key index
fn cmd_lindex(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'lindex' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let Some(idx) = parse_int(&args[1]) else {
        return err("ERR value is not an integer or out of range");
    };

    let Some(obj) = db.data.get(&key) else {
        return RespValue::Null;
    };

    let items: Vec<&Vec<u8>> = match obj {
        RedisObject::List(ListEnc::Listpack(v)) => v.iter().collect(),
        RedisObject::List(ListEnc::Quicklist(vd)) => vd.iter().collect(),
        _ => return wrong_type(),
    };

    let len = items.len() as i64;
    let i = if idx < 0 { len + idx } else { idx };
    if i < 0 || i >= len {
        return RespValue::Null;
    }
    RespValue::BulkString(items[i as usize].clone())
}

// ── Hash commands ─────────────────────────────────────────────────────────────

/// HSET key field value [field value ...]
/// Returns the number of NEW fields added.
/// REDIS: hsetCommand() in t_hash.c. Redis 4.0+ allows multiple field-value pairs.
fn cmd_hset(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || args[1..].len() % 2 != 0 {
        return err("ERR wrong number of arguments for 'hset' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };

    let obj = db
        .data
        .entry(key)
        .or_insert_with(|| RedisObject::Hash(HashEnc::Listpack(Vec::new())));

    let hash_enc = match obj {
        RedisObject::Hash(enc) => enc,
        _ => return wrong_type(),
    };

    let mut added = 0i64;
    for pair in args[1..].chunks_exact(2) {
        let field = pair[0].clone();
        let value = pair[1].clone();

        match hash_enc {
            HashEnc::Listpack(v) => {
                if let Some(pos) = v.iter().position(|(k, _)| k == &field) {
                    v[pos].1 = value.clone();
                } else {
                    v.push((field.clone(), value.clone()));
                    added += 1;
                }
                // Promote if thresholds exceeded.
                // REDIS: hashTypeConvert() in t_hash.c
                let needs_promotion = v.len() > LISTPACK_MAX_ENTRIES
                    || field.len() > LISTPACK_MAX_ELEMENT_BYTES
                    || value.len() > LISTPACK_MAX_ELEMENT_BYTES;
                if needs_promotion {
                    let promoted: HashMap<Vec<u8>, Vec<u8>> =
                        v.drain(..).collect();
                    *hash_enc = HashEnc::HashMap(promoted);
                }
            }
            HashEnc::HashMap(m) => {
                if m.insert(field, value).is_none() {
                    added += 1;
                }
            }
        }
    }
    RespValue::Integer(added)
}

/// HGET key field
fn cmd_hget(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'hget' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let field = &args[1];

    match db.data.get(&key) {
        None => RespValue::Null,
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => v
            .iter()
            .find(|(k, _)| k == field)
            .map(|(_, val)| RespValue::BulkString(val.clone()))
            .unwrap_or(RespValue::Null),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => m
            .get(field.as_slice())
            .map(|val| RespValue::BulkString(val.clone()))
            .unwrap_or(RespValue::Null),
        Some(_) => wrong_type(),
    }
}

/// HMGET key field [field ...]
fn cmd_hmget(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'hmget' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let fields = &args[1..];

    let results = match db.data.get(&key) {
        None => fields.iter().map(|_| RespValue::Null).collect(),
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => fields
            .iter()
            .map(|f| {
                v.iter()
                    .find(|(k, _)| k == f)
                    .map(|(_, val)| RespValue::BulkString(val.clone()))
                    .unwrap_or(RespValue::Null)
            })
            .collect(),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => fields
            .iter()
            .map(|f| {
                m.get(f.as_slice())
                    .map(|val| RespValue::BulkString(val.clone()))
                    .unwrap_or(RespValue::Null)
            })
            .collect(),
        Some(_) => return wrong_type(),
    };
    RespValue::Array(results)
}

/// HGETALL key
/// Returns alternating field-value pairs (same as the listpack wire layout).
fn cmd_hgetall(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'hgetall' command");
    };
    match db.data.get(&key) {
        None => RespValue::Array(vec![]),
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => {
            let mut out = Vec::with_capacity(v.len() * 2);
            for (k, val) in v {
                out.push(RespValue::BulkString(k.clone()));
                out.push(RespValue::BulkString(val.clone()));
            }
            RespValue::Array(out)
        }
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => {
            let mut out = Vec::with_capacity(m.len() * 2);
            for (k, val) in m {
                out.push(RespValue::BulkString(k.clone()));
                out.push(RespValue::BulkString(val.clone()));
            }
            RespValue::Array(out)
        }
        Some(_) => wrong_type(),
    }
}

/// HDEL key field [field ...]
fn cmd_hdel(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'hdel' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let fields = &args[1..];

    let Some(obj) = db.data.get_mut(&key) else {
        return RespValue::Integer(0);
    };
    let removed = match obj {
        RedisObject::Hash(HashEnc::Listpack(v)) => {
            let before = v.len();
            v.retain(|(k, _)| !fields.contains(k));
            (before - v.len()) as i64
        }
        RedisObject::Hash(HashEnc::HashMap(m)) => fields
            .iter()
            .filter(|f| m.remove(f.as_slice()).is_some())
            .count() as i64,
        _ => return wrong_type(),
    };
    // Remove key if hash is now empty.
    let is_empty = match db.data.get(&key) {
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => v.is_empty(),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => m.is_empty(),
        _ => false,
    };
    if is_empty {
        db.data.remove(&key);
    }
    RespValue::Integer(removed)
}

/// HLEN key
fn cmd_hlen(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'hlen' command");
    };
    match db.data.get(&key) {
        None => RespValue::Integer(0),
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => RespValue::Integer(v.len() as i64),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => RespValue::Integer(m.len() as i64),
        Some(_) => wrong_type(),
    }
}

/// HKEYS key
fn cmd_hkeys(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'hkeys' command");
    };
    match db.data.get(&key) {
        None => RespValue::Array(vec![]),
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => RespValue::Array(
            v.iter()
                .map(|(k, _)| RespValue::BulkString(k.clone()))
                .collect(),
        ),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => RespValue::Array(
            m.keys()
                .map(|k| RespValue::BulkString(k.clone()))
                .collect(),
        ),
        Some(_) => wrong_type(),
    }
}

/// HVALS key
fn cmd_hvals(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'hvals' command");
    };
    match db.data.get(&key) {
        None => RespValue::Array(vec![]),
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => RespValue::Array(
            v.iter()
                .map(|(_, val)| RespValue::BulkString(val.clone()))
                .collect(),
        ),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => RespValue::Array(
            m.values()
                .map(|val| RespValue::BulkString(val.clone()))
                .collect(),
        ),
        Some(_) => wrong_type(),
    }
}

/// HEXISTS key field
fn cmd_hexists(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'hexists' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let field = &args[1];
    let found = match db.data.get(&key) {
        None => false,
        Some(RedisObject::Hash(HashEnc::Listpack(v))) => v.iter().any(|(k, _)| k == field),
        Some(RedisObject::Hash(HashEnc::HashMap(m))) => m.contains_key(field.as_slice()),
        Some(_) => return wrong_type(),
    };
    RespValue::Integer(found as i64)
}

// ── Set commands ──────────────────────────────────────────────────────────────

/// Try to parse bytes as i64 for intset candidacy.
fn try_as_int(b: &[u8]) -> Option<i64> {
    parse_int(b)
}

/// SADD key member [member ...]
/// Returns the number of NEW members added.
fn cmd_sadd(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'sadd' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let members = &args[1..];

    let obj = db
        .data
        .entry(key)
        .or_insert_with(|| RedisObject::Set(SetEnc::Intset(Vec::new())));

    let set_enc = match obj {
        RedisObject::Set(enc) => enc,
        _ => return wrong_type(),
    };

    let mut added = 0i64;
    for member in members {
        added += sadd_one(set_enc, member) as i64;
    }
    RespValue::Integer(added)
}

/// Add one member to the set encoding, handling promotions.
/// Returns true if the member was newly added.
fn sadd_one(enc: &mut SetEnc, member: &[u8]) -> bool {
    match enc {
        SetEnc::Intset(v) => {
            if let Some(n) = try_as_int(member) {
                // Binary search insert to keep sorted.
                match v.binary_search(&n) {
                    Ok(_) => false, // already present
                    Err(pos) => {
                        v.insert(pos, n);
                        // Promote intset → listpack if over limit.
                        if v.len() > INTSET_MAX_ENTRIES {
                            let promoted: Vec<Vec<u8>> = v
                                .drain(..)
                                .map(|i| i.to_string().into_bytes())
                                .collect();
                            *enc = SetEnc::Listpack(promoted);
                        }
                        true
                    }
                }
            } else {
                // Non-integer: promote intset → listpack, then insert.
                let mut promoted: Vec<Vec<u8>> = v
                    .drain(..)
                    .map(|i| i.to_string().into_bytes())
                    .collect();
                promoted.push(member.to_vec());
                let new_enc = if promoted.len() > LISTPACK_MAX_ENTRIES
                    || member.len() > LISTPACK_MAX_ELEMENT_BYTES
                {
                    let ht: HashSet<Vec<u8>> = promoted.into_iter().collect();
                    SetEnc::Hashtable(ht)
                } else {
                    SetEnc::Listpack(promoted)
                };
                *enc = new_enc;
                true
            }
        }
        SetEnc::Listpack(v) => {
            if v.contains(&member.to_vec()) {
                return false;
            }
            v.push(member.to_vec());
            // Promote if thresholds exceeded.
            if v.len() > LISTPACK_MAX_ENTRIES || member.len() > LISTPACK_MAX_ELEMENT_BYTES {
                let ht: HashSet<Vec<u8>> = v.drain(..).collect();
                *enc = SetEnc::Hashtable(ht);
            }
            true
        }
        SetEnc::Hashtable(ht) => ht.insert(member.to_vec()),
    }
}

/// SREM key member [member ...]
fn cmd_srem(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'srem' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let members = &args[1..];

    let Some(obj) = db.data.get_mut(&key) else {
        return RespValue::Integer(0);
    };
    let removed = match obj {
        RedisObject::Set(SetEnc::Intset(v)) => members
            .iter()
            .filter_map(|m| try_as_int(m))
            .filter(|n| {
                if let Ok(pos) = v.binary_search(n) {
                    v.remove(pos);
                    true
                } else {
                    false
                }
            })
            .count() as i64,
        RedisObject::Set(SetEnc::Listpack(v)) => {
            let before = v.len();
            v.retain(|m| !members.contains(m));
            (before - v.len()) as i64
        }
        RedisObject::Set(SetEnc::Hashtable(ht)) => members
            .iter()
            .filter(|m| ht.remove(m.as_slice()))
            .count() as i64,
        _ => return wrong_type(),
    };
    // Remove key if set is now empty.
    let is_empty = match db.data.get(&key) {
        Some(RedisObject::Set(SetEnc::Intset(v))) => v.is_empty(),
        Some(RedisObject::Set(SetEnc::Listpack(v))) => v.is_empty(),
        Some(RedisObject::Set(SetEnc::Hashtable(ht))) => ht.is_empty(),
        _ => false,
    };
    if is_empty {
        db.data.remove(&key);
    }
    RespValue::Integer(removed)
}

/// SMEMBERS key
fn cmd_smembers(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'smembers' command");
    };
    match db.data.get(&key) {
        None => RespValue::Array(vec![]),
        Some(RedisObject::Set(enc)) => RespValue::Array(set_members_resp(enc)),
        Some(_) => wrong_type(),
    }
}

/// SCARD key
fn cmd_scard(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    let Some(key) = args.first().and_then(|k| to_key(k)) else {
        return err("ERR wrong number of arguments for 'scard' command");
    };
    match db.data.get(&key) {
        None => RespValue::Integer(0),
        Some(RedisObject::Set(enc)) => RespValue::Integer(set_len(enc) as i64),
        Some(_) => wrong_type(),
    }
}

/// SISMEMBER key member
fn cmd_sismember(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return err("ERR wrong number of arguments for 'sismember' command");
    }
    let Some(key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let member = &args[1];
    let found = match db.data.get(&key) {
        None => false,
        Some(RedisObject::Set(enc)) => set_contains(enc, member),
        Some(_) => return wrong_type(),
    };
    RespValue::Integer(found as i64)
}

/// SUNION key [key ...]
fn cmd_sunion(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'sunion' command");
    }
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    for key_bytes in args {
        let Some(key) = to_key(key_bytes) else { continue };
        match db.data.get(&key) {
            None => {}
            Some(RedisObject::Set(enc)) => {
                for m in set_iter(enc) {
                    result.insert(m);
                }
            }
            Some(_) => return wrong_type(),
        }
    }
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

/// SINTER key [key ...]
fn cmd_sinter(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'sinter' command");
    }
    // Build the intersection iteratively, starting from the first set.
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for key_bytes in args {
        let Some(key) = to_key(key_bytes) else {
            return RespValue::Array(vec![]); // missing key → empty intersection
        };
        match db.data.get(&key) {
            None => return RespValue::Array(vec![]),
            Some(RedisObject::Set(enc)) => sets.push(set_iter(enc).collect()),
            Some(_) => return wrong_type(),
        }
    }
    let mut iter = sets.into_iter();
    let first = iter.next().unwrap_or_default();
    let result: HashSet<Vec<u8>> = iter.fold(first, |acc, s| {
        acc.into_iter().filter(|m| s.contains(m)).collect()
    });
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

/// SDIFF key [key ...]
/// Returns members in the first set not present in any subsequent set.
fn cmd_sdiff(db: &mut Db, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return err("ERR wrong number of arguments for 'sdiff' command");
    }
    let Some(first_key) = to_key(&args[0]) else {
        return err("ERR invalid key");
    };
    let mut result: HashSet<Vec<u8>> = match db.data.get(&first_key) {
        None => HashSet::new(),
        Some(RedisObject::Set(enc)) => set_iter(enc).collect(),
        Some(_) => return wrong_type(),
    };
    for key_bytes in &args[1..] {
        let Some(key) = to_key(key_bytes) else { continue };
        match db.data.get(&key) {
            None => {}
            Some(RedisObject::Set(enc)) => {
                for m in set_iter(enc) {
                    result.remove(&m);
                }
            }
            Some(_) => return wrong_type(),
        }
    }
    RespValue::Array(result.into_iter().map(RespValue::BulkString).collect())
}

// ── Set encoding helpers ──────────────────────────────────────────────────────

fn set_len(enc: &SetEnc) -> usize {
    match enc {
        SetEnc::Intset(v) => v.len(),
        SetEnc::Listpack(v) => v.len(),
        SetEnc::Hashtable(ht) => ht.len(),
    }
}

fn set_contains(enc: &SetEnc, member: &[u8]) -> bool {
    match enc {
        SetEnc::Intset(v) => try_as_int(member)
            .map(|n| v.binary_search(&n).is_ok())
            .unwrap_or(false),
        SetEnc::Listpack(v) => v.iter().any(|m| m == member),
        SetEnc::Hashtable(ht) => ht.contains(member),
    }
}

fn set_iter(enc: &SetEnc) -> impl Iterator<Item = Vec<u8>> + '_ {
    // Unify all three encodings into one iterator.
    let (ints, lp, ht): (
        Box<dyn Iterator<Item = Vec<u8>>>,
        Box<dyn Iterator<Item = Vec<u8>>>,
        Box<dyn Iterator<Item = Vec<u8>>>,
    ) = match enc {
        SetEnc::Intset(v) => (
            Box::new(v.iter().map(|n| n.to_string().into_bytes())),
            Box::new(std::iter::empty()),
            Box::new(std::iter::empty()),
        ),
        SetEnc::Listpack(v) => (
            Box::new(std::iter::empty()),
            Box::new(v.iter().cloned()),
            Box::new(std::iter::empty()),
        ),
        SetEnc::Hashtable(ht) => (
            Box::new(std::iter::empty()),
            Box::new(std::iter::empty()),
            Box::new(ht.iter().cloned()),
        ),
    };
    ints.chain(lp).chain(ht)
}

fn set_members_resp(enc: &SetEnc) -> Vec<RespValue> {
    set_iter(enc).map(RespValue::BulkString).collect()
}
