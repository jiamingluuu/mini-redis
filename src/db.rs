use std::collections::HashMap;

use bytes::Bytes;
use rand::seq::IteratorRandom;

use crate::entry::{self, Entry};
use crate::eviction::EvictionPolicy;
use crate::object::RedisObject;
use crate::resp::frame::Frame;
use crate::types::{hash::Hash, list::List};

/// Redis's canonical WRONGTYPE error message.
///
/// REDIS: src/t_string.c, t_hash.c, t_list.c all call checkType() which
/// returns this exact string when the stored type doesn't match the command.
pub(crate) const WRONGTYPE: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

/// The in-memory key-value store.
///
/// All WRONGTYPE checking is centralised here — command handlers never build
/// `Frame::Error(WRONGTYPE…)` directly; they call an accessor and propagate
/// the `Err(Frame)` it returns.
///
/// Internally wraps each `RedisObject` in an `Entry` carrying per-key metadata
/// (expiry, LRU clock, LFU counter). The public accessor API remains unchanged
/// — lazy expiry and access tracking are handled transparently.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    data: HashMap<String, Entry>,
    eviction_policy: EvictionPolicy,
    used_memory: usize,
}

impl Db {
    pub(crate) fn new(eviction_policy: EvictionPolicy) -> Self {
        Self {
            data: HashMap::new(),
            eviction_policy,
            used_memory: 0,
        }
    }

    pub(crate) fn set_eviction_policy(&mut self, eviction_policy: EvictionPolicy) {
        self.eviction_policy = eviction_policy;
    }

    // -----------------------------------------------------------------------
    // Metadata accessors
    // -----------------------------------------------------------------------

    /// Iterate over all key-entry pairs.
    ///
    /// REDIS: Used by RDB serialization to snapshot the entire keyspace.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &Entry)> {
        self.data.iter()
    }

    /// Number of keys in the database.
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Estimated memory usage in bytes.
    pub(crate) fn used_memory(&self) -> usize {
        self.used_memory
    }

    /// Number of keys that have an expiry set.
    pub(crate) fn expiry_count(&self) -> usize {
        self.data
            .values()
            .filter(|e| e.expires_at().is_some())
            .count()
    }

    /// Read-only access to an Entry (for eviction metadata inspection).
    pub(crate) fn get_entry(&self, key: &str) -> Option<&Entry> {
        self.data.get(key)
    }

    /// Mutable access to an Entry (for eviction/expiry tests and LRU/LFU manipulation).
    #[cfg(test)]
    pub(crate) fn get_entry_mut(&mut self, key: &str) -> Option<&mut Entry> {
        self.data.get_mut(key)
    }

    /// Sample `count` random keys from the database.
    ///
    /// REDIS: `dictGetRandomKey()` in dict.c — used by active expiry and
    /// eviction to sample without iterating the full keyspace.
    pub(crate) fn random_keys(&self, count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        self.data
            .keys()
            .choose_multiple(&mut rng, count)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Sample `count` random keys that have an expiry set.
    pub(crate) fn random_keys_with_expiry(&self, count: usize) -> Vec<String> {
        let mut rng = rand::thread_rng();
        self.data
            .iter()
            .filter(|(_, e)| e.expires_at().is_some())
            .map(|(k, _)| k)
            .choose_multiple(&mut rng, count)
            .into_iter()
            .cloned()
            .collect()
    }

    // -----------------------------------------------------------------------
    // Lazy expiry helper
    // -----------------------------------------------------------------------

    /// Check if a key is expired and remove it if so. Returns `true` if the
    /// key was expired (and removed).
    ///
    /// REDIS: `expireIfNeeded()` in db.c — called before every key lookup.
    /// This is the "lazy" half of Redis's dual expiry strategy.
    fn expire_if_needed(&mut self, key: &str) -> bool {
        let expired = self
            .data
            .get(key)
            .is_some_and(|e| e.is_expired(entry::now_ms()));
        if expired {
            self.remove(key);
        }
        expired
    }

    /// Touch access-tracking metadata for a key.
    fn touch(&mut self, key: &str) {
        if let Some(entry) = self.data.get_mut(key) {
            match self.eviction_policy {
                EvictionPolicy::AllKeysLru => {
                    // Use seconds since epoch modulo u32::MAX as a simple clock
                    let clock = (entry::now_ms() / 1000) as u32;
                    entry.touch_lru(clock);
                }
                EvictionPolicy::AllKeysLfu => {
                    let now_min = entry::now_minutes();
                    // Use defaults matching Redis (log_factor=10, decay_time=1)
                    entry.touch_lfu(now_min, 10, 1);
                }
                EvictionPolicy::NoEviction => {}
            }
        }
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Remove a key, returning `true` if it existed.
    pub(crate) fn remove(&mut self, key: &str) -> bool {
        if let Some(entry) = self.data.remove(key) {
            self.used_memory = self
                .used_memory
                .saturating_sub(Self::key_overhead(key) + entry.estimated_size());
            true
        } else {
            false
        }
    }

    /// Unconditionally store an object (no expiry).
    ///
    /// REDIS: SET always overwrites regardless of the existing type — no WRONGTYPE.
    /// setGenericCommand() in t_string.c calls dbAdd/dbOverwrite unconditionally.
    pub(crate) fn set(&mut self, key: String, obj: RedisObject) {
        self.set_with_expiry(key, obj, None);
    }

    /// Store an object with an optional expiry timestamp (epoch ms).
    pub(crate) fn set_with_expiry(
        &mut self,
        key: String,
        obj: RedisObject,
        expires_at: Option<u64>,
    ) {
        // Remove old entry's memory contribution
        if let Some(old) = self.data.get(&key) {
            self.used_memory = self
                .used_memory
                .saturating_sub(Self::key_overhead(&key) + old.estimated_size());
        }

        let clock = (entry::now_ms() / 1000) as u32;
        let mut new_entry = Entry::new(obj, clock);
        new_entry.set_expires_at(expires_at);

        self.used_memory += Self::key_overhead(&key) + new_entry.estimated_size();
        self.data.insert(key, new_entry);
    }

    /// Get the expiry timestamp for a key (if any).
    pub(crate) fn get_expiry(&self, key: &str) -> Option<Option<u64>> {
        self.data.get(key).map(|e| e.expires_at())
    }

    /// Set or remove expiry on an existing key. Returns `true` if the key exists.
    pub(crate) fn set_expiry(&mut self, key: &str, expires_at: Option<u64>) -> bool {
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_expires_at(expires_at);
            true
        } else {
            false
        }
    }

    /// Return a string value by key.
    ///
    /// - `Ok(Some(&Bytes))` — key exists and is a String
    /// - `Ok(None)`         — key does not exist
    /// - `Err(Frame)`       — key exists with the wrong type (WRONGTYPE error frame)
    pub(crate) fn get_str(&mut self, key: &str) -> Result<Option<&Bytes>, Frame> {
        self.expire_if_needed(key);
        self.touch(key);
        match self.data.get(key).map(|e| e.obj()) {
            Some(RedisObject::Str(b)) => Ok(Some(b)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return a shared reference to a hash.
    pub(crate) fn get_hash(&mut self, key: &str) -> Result<Option<&Hash>, Frame> {
        self.expire_if_needed(key);
        self.touch(key);
        match self.data.get(key).map(|e| e.obj()) {
            Some(RedisObject::Hash(h)) => Ok(Some(h)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return an exclusive reference to a hash (for mutations like HDEL).
    #[cfg(test)]
    pub(crate) fn get_hash_mut(&mut self, key: &str) -> Result<Option<&mut Hash>, Frame> {
        self.expire_if_needed(key);
        self.touch(key);
        match self.data.get_mut(key).map(|e| e.obj_mut()) {
            Some(RedisObject::Hash(h)) => Ok(Some(h)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return a shared reference to a list.
    pub(crate) fn get_list(&mut self, key: &str) -> Result<Option<&List>, Frame> {
        self.expire_if_needed(key);
        self.touch(key);
        match self.data.get(key).map(|e| e.obj()) {
            Some(RedisObject::List(l)) => Ok(Some(l)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return (or create) a mutable hash for the given key.
    ///
    /// Returns `Err(Frame)` if the key already holds a non-Hash type.
    #[cfg(test)]
    pub(crate) fn get_or_insert_hash(&mut self, key: String) -> Result<&mut Hash, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);
        if !self.data.contains_key(&key) {
            self.set(key.clone(), RedisObject::Hash(Hash::new()));
        }
        let entry = self.data.get_mut(&key).expect("just inserted");
        match entry.obj_mut() {
            RedisObject::Hash(h) => Ok(h),
            _ => Err(Frame::Error(WRONGTYPE.into())),
        }
    }

    /// Return (or create) a mutable list for the given key.
    ///
    /// Returns `Err(Frame)` if the key already holds a non-List type.
    #[cfg(test)]
    pub(crate) fn get_or_insert_list(&mut self, key: String) -> Result<&mut List, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);
        if !self.data.contains_key(&key) {
            self.set(key.clone(), RedisObject::List(List::new()));
        }
        let entry = self.data.get_mut(&key).expect("just inserted");
        match entry.obj_mut() {
            RedisObject::List(l) => Ok(l),
            _ => Err(Frame::Error(WRONGTYPE.into())),
        }
    }

    pub(crate) fn hash_upsert_many(
        &mut self,
        key: String,
        pairs: Vec<(Bytes, Bytes)>,
    ) -> Result<i64, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);
        if !self.data.contains_key(&key) {
            self.set(key.clone(), RedisObject::Hash(Hash::new()));
        }

        let (added, before_size, after_size) = {
            let entry = self.data.get_mut(&key).expect("key exists after insertion");
            match entry.obj_mut() {
                RedisObject::Hash(hash) => {
                    let before_size = hash.estimated_size();
                    let added = pairs
                        .into_iter()
                        .map(|(field, value)| hash.insert(field, value) as i64)
                        .sum();
                    let after_size = hash.estimated_size();
                    (added, before_size, after_size)
                }
                _ => return Err(Frame::Error(WRONGTYPE.into())),
            }
        };

        self.apply_size_delta(before_size, after_size);
        Ok(added)
    }

    pub(crate) fn hash_remove_many(
        &mut self,
        key: String,
        fields: Vec<Bytes>,
    ) -> Result<Option<i64>, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);

        if !self.data.contains_key(&key) {
            return Ok(None);
        }

        let (removed, before_size, after_size) = {
            let entry = self.data.get_mut(&key).expect("key existence checked");
            match entry.obj_mut() {
                RedisObject::Hash(hash) => {
                    let before_size = hash.estimated_size();
                    let removed = fields.iter().map(|field| hash.remove(field) as i64).sum();
                    let after_size = hash.estimated_size();
                    (removed, before_size, after_size)
                }
                _ => return Err(Frame::Error(WRONGTYPE.into())),
            }
        };

        self.apply_size_delta(before_size, after_size);
        Ok(Some(removed))
    }

    pub(crate) fn list_push_many(
        &mut self,
        key: String,
        values: Vec<Bytes>,
        front: bool,
    ) -> Result<usize, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);
        if !self.data.contains_key(&key) {
            self.set(key.clone(), RedisObject::List(List::new()));
        }

        let (len, before_size, after_size) = {
            let entry = self.data.get_mut(&key).expect("key exists after insertion");
            match entry.obj_mut() {
                RedisObject::List(list) => {
                    let before_size = list.estimated_size();
                    for value in values {
                        if front {
                            list.push_front(value);
                        } else {
                            list.push_back(value);
                        }
                    }
                    let after_size = list.estimated_size();
                    (list.len(), before_size, after_size)
                }
                _ => return Err(Frame::Error(WRONGTYPE.into())),
            }
        };

        self.apply_size_delta(before_size, after_size);
        Ok(len)
    }

    pub(crate) fn list_pop_many(
        &mut self,
        key: String,
        count: Option<u64>,
        front: bool,
    ) -> Result<Option<Vec<Bytes>>, Frame> {
        self.expire_if_needed(&key);
        self.touch(&key);

        if !self.data.contains_key(&key) {
            return Ok(None);
        }

        let (values, before_size, after_size) = {
            let entry = self.data.get_mut(&key).expect("key existence checked");
            match entry.obj_mut() {
                RedisObject::List(list) => {
                    let before_size = list.estimated_size();
                    let mut values = Vec::new();

                    match count {
                        None => {
                            let value = if front {
                                list.pop_front()
                            } else {
                                list.pop_back()
                            };
                            if let Some(value) = value {
                                values.push(value);
                            }
                        }
                        Some(n) => {
                            values.reserve(n as usize);
                            for _ in 0..n {
                                let value = if front {
                                    list.pop_front()
                                } else {
                                    list.pop_back()
                                };
                                match value {
                                    Some(value) => values.push(value),
                                    None => break,
                                }
                            }
                        }
                    }

                    let after_size = list.estimated_size();
                    (values, before_size, after_size)
                }
                _ => return Err(Frame::Error(WRONGTYPE.into())),
            }
        };

        self.apply_size_delta(before_size, after_size);
        Ok(Some(values))
    }

    /// Estimate memory overhead for a key string.
    fn key_overhead(key: &str) -> usize {
        // String allocation + HashMap entry overhead
        key.len() + 64
    }

    fn apply_size_delta(&mut self, before: usize, after: usize) {
        if after >= before {
            self.used_memory += after - before;
        } else {
            self.used_memory = self.used_memory.saturating_sub(before - after);
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

    fn wrongtype() -> Frame {
        Frame::Error(WRONGTYPE.into())
    }

    fn db() -> Db {
        Db::new(EvictionPolicy::NoEviction)
    }

    // --- get_str ---

    #[test]
    fn get_str_missing_returns_none() {
        let mut db = db();
        assert_eq!(db.get_str("k"), Ok(None));
    }

    #[test]
    fn get_str_correct_type_returns_value() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_str("k"), Ok(Some(&b("v"))));
    }

    #[test]
    fn get_str_wrong_type_returns_wrongtype() {
        let mut db = db();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert_eq!(db.get_str("k"), Err(wrongtype()));
    }

    // --- get_hash / get_hash_mut ---

    #[test]
    fn get_hash_missing_returns_none() {
        let mut db = db();
        assert!(db.get_hash("k").unwrap().is_none());
    }

    #[test]
    fn get_hash_correct_type_returns_ref() {
        let mut db = db();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert!(db.get_hash("k").unwrap().is_some());
    }

    #[test]
    fn get_hash_wrong_type_returns_wrongtype() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_hash("k").unwrap_err(), wrongtype());
    }

    #[test]
    fn get_hash_mut_allows_mutation() {
        let mut db = db();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        let h = db.get_hash_mut("k").unwrap().unwrap();
        h.insert(b("f"), b("v"));
        assert_eq!(db.get_hash("k").unwrap().unwrap().len(), 1);
    }

    // --- get_list / get_list_mut ---

    #[test]
    fn get_list_missing_returns_none() {
        let mut db = db();
        assert!(db.get_list("k").unwrap().is_none());
    }

    #[test]
    fn get_list_wrong_type_returns_wrongtype() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_list("k").unwrap_err(), wrongtype());
    }

    // --- get_or_insert_hash ---

    #[test]
    fn get_or_insert_hash_creates_new() {
        let mut db = db();
        let h = db.get_or_insert_hash("k".into()).unwrap();
        h.insert(b("f"), b("v"));
        assert_eq!(db.get_hash("k").unwrap().unwrap().len(), 1);
    }

    #[test]
    fn get_or_insert_hash_wrong_type_returns_wrongtype() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_or_insert_hash("k".into()).unwrap_err(), wrongtype());
    }

    // --- get_or_insert_list ---

    #[test]
    fn get_or_insert_list_creates_new() {
        let mut db = db();
        let l = db.get_or_insert_list("k".into()).unwrap();
        l.push_front(b("v"));
        assert_eq!(db.get_list("k").unwrap().unwrap().len(), 1);
    }

    #[test]
    fn get_or_insert_list_wrong_type_returns_wrongtype() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_or_insert_list("k".into()).unwrap_err(), wrongtype());
    }

    // --- remove ---

    #[test]
    fn remove_existing_returns_true() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert!(db.remove("k"));
        assert_eq!(db.get_str("k"), Ok(None));
    }

    #[test]
    fn remove_missing_returns_false() {
        let mut db = db();
        assert!(!db.remove("k"));
    }

    // --- expiry ---

    #[test]
    fn set_with_expiry_and_get_expiry() {
        let mut db = db();
        db.set_with_expiry("k".into(), RedisObject::Str(b("v")), Some(99999));
        assert_eq!(db.get_expiry("k"), Some(Some(99999)));
    }

    #[test]
    fn set_expiry_on_existing_key() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert!(db.set_expiry("k", Some(5000)));
        assert_eq!(db.get_expiry("k"), Some(Some(5000)));
    }

    #[test]
    fn set_expiry_on_missing_key_returns_false() {
        let mut db = db();
        assert!(!db.set_expiry("k", Some(5000)));
    }

    // --- memory tracking ---

    #[test]
    fn used_memory_increases_on_set() {
        let mut db = db();
        let before = db.used_memory();
        db.set("k".into(), RedisObject::Str(b("value")));
        assert!(db.used_memory() > before);
    }

    #[test]
    fn used_memory_decreases_on_remove() {
        let mut db = db();
        db.set("k".into(), RedisObject::Str(b("value")));
        let after_set = db.used_memory();
        db.remove("k");
        assert!(db.used_memory() < after_set);
    }

    #[test]
    fn used_memory_tracks_hash_mutations() {
        let mut db = db();
        db.set("h".into(), RedisObject::Hash(Hash::new()));
        let before = db.used_memory();

        let added = db
            .hash_upsert_many("h".into(), vec![(b("field"), b("value"))])
            .unwrap();
        assert_eq!(added, 1);
        assert!(db.used_memory() > before);

        let after_insert = db.used_memory();
        let removed = db.hash_remove_many("h".into(), vec![b("field")]).unwrap();
        assert_eq!(removed, Some(1));
        assert!(db.used_memory() < after_insert);
    }

    #[test]
    fn used_memory_tracks_hash_promotion() {
        let mut db = db();
        db.set("h".into(), RedisObject::Hash(Hash::new()));
        db.hash_upsert_many("h".into(), vec![(b("f"), b("v"))])
            .unwrap();
        let before = db.used_memory();

        let big = Bytes::from(vec![b'x'; crate::encoding::HASH_MAX_LISTPACK_VALUE + 1]);
        db.hash_upsert_many("h".into(), vec![(b("big"), big)])
            .unwrap();

        assert!(db.used_memory() > before);
        assert_eq!(
            db.get_hash("h").unwrap().unwrap().encoding_name(),
            "hashtable"
        );
    }

    #[test]
    fn used_memory_tracks_list_mutations() {
        let mut db = db();
        db.set("l".into(), RedisObject::List(List::new()));
        let before = db.used_memory();

        let len = db
            .list_push_many("l".into(), vec![b("a"), b("b")], false)
            .unwrap();
        assert_eq!(len, 2);
        assert!(db.used_memory() > before);

        let after_push = db.used_memory();
        let popped = db
            .list_pop_many("l".into(), Some(1), false)
            .unwrap()
            .unwrap();
        assert_eq!(popped, vec![b("b")]);
        assert!(db.used_memory() < after_push);
    }

    // --- random_keys ---

    #[test]
    fn random_keys_returns_subset() {
        let mut db = db();
        for i in 0..10 {
            db.set(format!("k{i}"), RedisObject::Str(b("v")));
        }
        let keys = db.random_keys(3);
        assert_eq!(keys.len(), 3);
        for k in &keys {
            assert!(db.get_entry(k).is_some());
        }
    }

    #[test]
    fn random_keys_with_expiry_filters_correctly() {
        let mut db = db();
        db.set("no_exp".into(), RedisObject::Str(b("v")));
        db.set_with_expiry("has_exp".into(), RedisObject::Str(b("v")), Some(u64::MAX));
        let keys = db.random_keys_with_expiry(10);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "has_exp");
    }
}
