//! Redis object model — internal value representation.
//!
//! REDIS: Production Redis uses a single `redisObject` struct (server.h):
//!   { type:4, encoding:4, lru:24, refcount:32, ptr:64 }
//! The `encoding` field is a runtime tag over a `void *ptr`.
//! Rust's enum is a *compile-time* tagged union — the same concept, but the
//! compiler enforces correct dispatch and eliminates a whole class of bugs.
//! Our nested enums map cleanly:
//!   outer variant  ≡  robj.type
//!   inner variant  ≡  robj.encoding
//!   inner payload  ≡  *robj.ptr (cast to the correct type)

use std::collections::{HashMap, HashSet, VecDeque};

// ── Promotion thresholds ──────────────────────────────────────────────────────
//
// REDIS: Defaults from server.h / config.c. Tunable at runtime via CONFIG SET.
// These numbers are the result of benchmarking: below these sizes a linear scan
// over a compact array beats pointer-chasing through a hashtable because the
// entire structure fits in a handful of cache lines.

pub const LISTPACK_MAX_ENTRIES: usize = 128;
pub const LISTPACK_MAX_ELEMENT_BYTES: usize = 64;
pub const INTSET_MAX_ENTRIES: usize = 512;

// ── String encoding ───────────────────────────────────────────────────────────

/// REDIS: OBJ_ENCODING_INT / OBJ_ENCODING_EMBSTR / OBJ_ENCODING_RAW
///
/// Production Redis has three string encodings:
///   INT     — value fits in a long; stored directly in the ptr field (no heap).
///   EMBSTR  — ≤44 bytes; redisObject + SDS header in one contiguous allocation.
///   RAW     — >44 bytes; separate SDS heap allocation.
///
/// We collapse EMBSTR and RAW into a single `Raw(Vec<u8>)` because the
/// difference is a memory-allocator optimization with no behavioral effect.
/// The interesting threshold for us is whether the value can be interpreted as
/// an i64 — that's the boundary that changes which operations are O(1).
#[derive(Debug, Clone)]
pub enum StringEnc {
    /// Value is a valid i64. INCR/DECR operate without any parsing.
    /// REDIS: stored as `(void *)(long)value` — pointer-sized slot holds the int.
    Int(i64),
    /// All other string values. Arbitrary bytes.
    Raw(Vec<u8>),
}

// ── List encoding ─────────────────────────────────────────────────────────────

/// REDIS: OBJ_ENCODING_LISTPACK / OBJ_ENCODING_QUICKLIST
///
/// Redis 7.0 replaced ziplist with listpack everywhere. A quicklist (t_list.c)
/// is a doubly-linked list of listpack nodes; for small lists a single node
/// suffices. We model this semantically:
///   Listpack  → Vec       (contiguous allocation, cache-friendly linear access)
///   Quicklist → VecDeque  (O(1) push/pop from both ends after promotion)
#[derive(Debug, Clone)]
pub enum ListEnc {
    /// ≤128 elements, each ≤64 bytes.
    Listpack(Vec<Vec<u8>>),
    /// Promoted when either threshold is exceeded.
    Quicklist(VecDeque<Vec<u8>>),
}

// ── Hash encoding ─────────────────────────────────────────────────────────────

/// REDIS: OBJ_ENCODING_LISTPACK / OBJ_ENCODING_HT
///
/// In listpack encoding, fields are stored as alternating key-value pairs
/// in a flat array. We model this as Vec<(key, value)> — same iteration order,
/// same O(n) lookup. Insertion order is preserved (Redis makes no ordering
/// guarantee, but this matches listpack's behavior).
#[derive(Debug, Clone)]
pub enum HashEnc {
    /// ≤128 field-value pairs, each key and value individually ≤64 bytes.
    Listpack(Vec<(Vec<u8>, Vec<u8>)>),
    HashMap(HashMap<Vec<u8>, Vec<u8>>),
}

// ── Set encoding ──────────────────────────────────────────────────────────────

/// REDIS: OBJ_ENCODING_INTSET / OBJ_ENCODING_LISTPACK / OBJ_ENCODING_HT
///
/// Intset (intset.c) is a sorted array of integers with binary-search lookup.
/// It promotes to listpack when a non-integer member is added, or to hashtable
/// when size exceeds LISTPACK_MAX_ENTRIES.
#[derive(Debug, Clone)]
pub enum SetEnc {
    /// All members are integers, ≤512 members. Sorted for binary search.
    /// REDIS: intset stores elements as int16/int32/int64 depending on range,
    /// upgrading the encoding when a larger integer arrives. We always use i64.
    Intset(Vec<i64>),
    /// Mixed types or post-intset, ≤128 members, each ≤64 bytes.
    Listpack(Vec<Vec<u8>>),
    /// Promoted once either listpack threshold is exceeded.
    Hashtable(HashSet<Vec<u8>>),
}

// ── Top-level object ──────────────────────────────────────────────────────────

/// The value stored for every key in the database.
#[derive(Debug, Clone)]
pub enum RedisObject {
    Str(StringEnc),
    List(ListEnc),
    Hash(HashEnc),
    Set(SetEnc),
}

impl RedisObject {
    /// The string Redis returns for the TYPE command.
    /// REDIS: typeCommand() in object.c
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisObject::Str(_) => "string",
            RedisObject::List(_) => "list",
            RedisObject::Hash(_) => "hash",
            RedisObject::Set(_) => "set",
        }
    }
}
