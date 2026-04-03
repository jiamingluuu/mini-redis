use std::collections::HashMap;
use std::mem;

use bytes::Bytes;

use crate::encoding::{HASH_MAX_LISTPACK_ENTRIES, HASH_MAX_LISTPACK_VALUE};

/// Internal encoding for a Hash value.
///
/// REDIS: Redis stores small hashes as a listpack — a single contiguous byte
/// array that packs all field/value pairs sequentially. Every lookup is O(n)
/// but the entire structure fits in a handful of cache lines, making it faster
/// in practice than a hashtable for small N.
///
/// We model the listpack as Vec<(Bytes, Bytes)> which preserves the key
/// property (one allocation, linear scan) without reimplementing the
/// byte-packing of the actual listpack format (lp.c).
///
/// Above the threshold Redis promotes to a real hashtable for O(1) access.
#[derive(Debug, Clone)]
enum Encoding {
    Listpack(Vec<(Bytes, Bytes)>),
    Table(HashMap<Bytes, Bytes>),
}

#[derive(Debug, Clone)]
pub(crate) struct Hash(Encoding);

impl Hash {
    pub(crate) fn new() -> Self {
        Self(Encoding::Listpack(Vec::new()))
    }

    /// Insert or update a field. Returns `true` if the field is new.
    ///
    /// Promotion is checked on every insert:
    /// - If the new field or value exceeds `HASH_MAX_LISTPACK_VALUE` bytes,
    ///   the encoding is promoted *before* the insert (avoids storing an
    ///   oversized entry in the listpack even transiently).
    /// - If the entry count exceeds `HASH_MAX_LISTPACK_ENTRIES` after the
    ///   insert, the encoding is promoted.
    pub(crate) fn insert(&mut self, field: Bytes, value: Bytes) -> bool {
        if self.is_listpack()
            && (field.len() > HASH_MAX_LISTPACK_VALUE || value.len() > HASH_MAX_LISTPACK_VALUE)
        {
            self.promote();
        }

        let is_new = match &mut self.0 {
            Encoding::Listpack(pairs) => {
                if let Some(pos) = pairs.iter().position(|(k, _)| k == &field) {
                    pairs[pos].1 = value;
                    false
                } else {
                    pairs.push((field, value));
                    true
                }
            }
            Encoding::Table(map) => map.insert(field, value).is_none(),
        };

        if let Encoding::Listpack(pairs) = &self.0
            && pairs.len() > HASH_MAX_LISTPACK_ENTRIES
        {
            self.promote();
        }

        is_new
    }

    pub(crate) fn get(&self, field: &Bytes) -> Option<&Bytes> {
        match &self.0 {
            Encoding::Listpack(pairs) => pairs.iter().find(|(k, _)| k == field).map(|(_, v)| v),
            Encoding::Table(map) => map.get(field),
        }
    }

    /// Remove a field. Returns `true` if the field existed.
    pub(crate) fn remove(&mut self, field: &Bytes) -> bool {
        match &mut self.0 {
            Encoding::Listpack(pairs) => {
                if let Some(pos) = pairs.iter().position(|(k, _)| k == field) {
                    pairs.swap_remove(pos);
                    true
                } else {
                    false
                }
            }
            Encoding::Table(map) => map.remove(field).is_some(),
        }
    }

    pub(crate) fn contains(&self, field: &Bytes) -> bool {
        self.get(field).is_some()
    }

    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            Encoding::Listpack(pairs) => pairs.len(),
            Encoding::Table(map) => map.len(),
        }
    }

    /// Rough size estimate used for maxmemory enforcement.
    ///
    /// REDIS: listpack hashes are compact contiguous blobs, while hashtables pay
    /// substantially more pointer/bucket overhead per entry. We model that
    /// difference coarsely so encoding promotion is visible to eviction logic.
    pub(crate) fn estimated_size(&self) -> usize {
        match &self.0 {
            Encoding::Listpack(pairs) => {
                16 + pairs
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 8)
                    .sum::<usize>()
            }
            Encoding::Table(map) => {
                64 + map
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 64)
                    .sum::<usize>()
            }
        }
    }

    /// Returns all field-value pairs in an unspecified order.
    pub(crate) fn get_all(&self) -> Vec<(Bytes, Bytes)> {
        match &self.0 {
            Encoding::Listpack(pairs) => pairs.clone(),
            Encoding::Table(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    /// Iterate over field-value pairs regardless of encoding.
    ///
    /// REDIS: Used by RDB serialization to dump all hash entries without caring
    /// whether the hash is a listpack or hashtable internally.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        match &self.0 {
            Encoding::Listpack(pairs) => Box::new(pairs.iter().map(|(k, v)| (k, v)))
                as Box<dyn Iterator<Item = (&Bytes, &Bytes)>>,
            Encoding::Table(map) => Box::new(map.iter()),
        }
    }

    // Used in tests and will serve OBJECT ENCODING when that command is added.
    #[allow(dead_code)]
    pub(crate) fn encoding_name(&self) -> &'static str {
        match &self.0 {
            Encoding::Listpack(_) => "listpack",
            Encoding::Table(_) => "hashtable",
        }
    }

    fn is_listpack(&self) -> bool {
        matches!(&self.0, Encoding::Listpack(_))
    }

    /// Convert Listpack → Table. Uses mem::replace to satisfy the borrow
    /// checker: we take ownership of the old encoding before building the new one.
    fn promote(&mut self) {
        // REDIS: hashTypeConvert() in t_hash.c — once promoted, a hash never
        // downgrades back to listpack even if entries are removed.
        let old = mem::replace(&mut self.0, Encoding::Table(HashMap::new()));
        if let Encoding::Listpack(pairs) = old {
            self.0 = Encoding::Table(pairs.into_iter().collect());
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    // --- basic operations ---

    #[test]
    fn new_hash_uses_listpack() {
        let h = Hash::new();
        assert_eq!(h.encoding_name(), "listpack");
    }

    #[test]
    fn insert_new_field_returns_true() {
        let mut h = Hash::new();
        assert!(h.insert(b("f"), b("v")));
    }

    #[test]
    fn insert_existing_field_returns_false() {
        let mut h = Hash::new();
        h.insert(b("f"), b("v1"));
        assert!(!h.insert(b("f"), b("v2")));
        assert_eq!(h.get(&b("f")).unwrap(), &b("v2"));
    }

    #[test]
    fn get_missing_field_returns_none() {
        let h = Hash::new();
        assert_eq!(h.get(&b("missing")), None);
    }

    #[test]
    fn remove_existing_field_returns_true() {
        let mut h = Hash::new();
        h.insert(b("f"), b("v"));
        assert!(h.remove(&b("f")));
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn remove_missing_field_returns_false() {
        let mut h = Hash::new();
        assert!(!h.remove(&b("nope")));
    }

    #[test]
    fn contains_is_correct() {
        let mut h = Hash::new();
        h.insert(b("f"), b("v"));
        assert!(h.contains(&b("f")));
        assert!(!h.contains(&b("other")));
    }

    #[test]
    fn get_all_returns_all_pairs() {
        let mut h = Hash::new();
        h.insert(b("a"), b("1"));
        h.insert(b("b"), b("2"));
        let mut all = h.get_all();
        all.sort_by_key(|(k, _)| k.clone());
        assert_eq!(all, vec![(b("a"), b("1")), (b("b"), b("2"))]);
    }

    // --- promotion: entry count ---

    #[test]
    fn promotes_to_hashtable_when_entry_count_exceeds_threshold() {
        let mut h = Hash::new();
        for i in 0..=HASH_MAX_LISTPACK_ENTRIES {
            h.insert(Bytes::from(format!("field{i}")), b("v"));
        }
        assert_eq!(h.encoding_name(), "hashtable");
    }

    #[test]
    fn stays_listpack_at_exactly_threshold() {
        let mut h = Hash::new();
        for i in 0..HASH_MAX_LISTPACK_ENTRIES {
            h.insert(Bytes::from(format!("field{i}")), b("v"));
        }
        assert_eq!(h.encoding_name(), "listpack");
    }

    // --- promotion: value size ---

    #[test]
    fn promotes_when_field_exceeds_size_threshold() {
        let mut h = Hash::new();
        let big_field = Bytes::from(vec![b'x'; HASH_MAX_LISTPACK_VALUE + 1]);
        h.insert(big_field, b("v"));
        assert_eq!(h.encoding_name(), "hashtable");
    }

    #[test]
    fn promotes_when_value_exceeds_size_threshold() {
        let mut h = Hash::new();
        let big_value = Bytes::from(vec![b'x'; HASH_MAX_LISTPACK_VALUE + 1]);
        h.insert(b("f"), big_value);
        assert_eq!(h.encoding_name(), "hashtable");
    }

    // --- operations work correctly after promotion ---

    #[test]
    fn insert_get_del_work_after_promotion() {
        let mut h = Hash::new();
        // Force promotion via size
        h.insert(b("f"), Bytes::from(vec![b'x'; HASH_MAX_LISTPACK_VALUE + 1]));
        assert_eq!(h.encoding_name(), "hashtable");

        h.insert(b("g"), b("hello"));
        assert_eq!(h.get(&b("g")).unwrap(), &b("hello"));
        assert!(h.remove(&b("g")));
        assert_eq!(h.get(&b("g")), None);
    }

    #[test]
    fn promotion_changes_estimated_size() {
        let mut h = Hash::new();
        h.insert(b("f"), b("v"));
        let before = h.estimated_size();

        h.insert(
            Bytes::from(vec![b'x'; HASH_MAX_LISTPACK_VALUE + 1]),
            Bytes::from_static(b"v"),
        );
        assert_eq!(h.encoding_name(), "hashtable");
        assert!(h.estimated_size() > before);
    }
}
