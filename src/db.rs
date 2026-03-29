use std::collections::HashMap;

use bytes::Bytes;

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
#[derive(Debug, Clone)]
pub(crate) struct Db {
    data: HashMap<String, RedisObject>,
}

impl Db {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Iterate over all key-value pairs.
    ///
    /// REDIS: Used by RDB serialization to snapshot the entire keyspace.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &RedisObject)> {
        self.data.iter()
    }

    /// Number of keys in the database.
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Remove a key, returning `true` if it existed.
    pub(crate) fn remove(&mut self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    /// Unconditionally store an object.
    ///
    /// REDIS: SET always overwrites regardless of the existing type — no WRONGTYPE.
    /// setGenericCommand() in t_string.c calls dbAdd/dbOverwrite unconditionally.
    pub(crate) fn set(&mut self, key: String, obj: RedisObject) {
        self.data.insert(key, obj);
    }

    /// Return a string value by key.
    ///
    /// - `Ok(Some(&Bytes))` — key exists and is a String
    /// - `Ok(None)`         — key does not exist
    /// - `Err(Frame)`       — key exists with the wrong type (WRONGTYPE error frame)
    pub(crate) fn get_str(&self, key: &str) -> Result<Option<&Bytes>, Frame> {
        match self.data.get(key) {
            Some(RedisObject::Str(b)) => Ok(Some(b)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return a shared reference to a hash.
    pub(crate) fn get_hash(&self, key: &str) -> Result<Option<&Hash>, Frame> {
        match self.data.get(key) {
            Some(RedisObject::Hash(h)) => Ok(Some(h)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return an exclusive reference to a hash (for mutations like HDEL).
    pub(crate) fn get_hash_mut(&mut self, key: &str) -> Result<Option<&mut Hash>, Frame> {
        match self.data.get_mut(key) {
            Some(RedisObject::Hash(h)) => Ok(Some(h)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return a shared reference to a list.
    pub(crate) fn get_list(&self, key: &str) -> Result<Option<&List>, Frame> {
        match self.data.get(key) {
            Some(RedisObject::List(l)) => Ok(Some(l)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return an exclusive reference to a list (for mutations like LPOP).
    pub(crate) fn get_list_mut(&mut self, key: &str) -> Result<Option<&mut List>, Frame> {
        match self.data.get_mut(key) {
            Some(RedisObject::List(l)) => Ok(Some(l)),
            Some(_) => Err(Frame::Error(WRONGTYPE.into())),
            None => Ok(None),
        }
    }

    /// Return (or create) a mutable hash for the given key.
    ///
    /// Returns `Err(Frame)` if the key already holds a non-Hash type.
    pub(crate) fn get_or_insert_hash(&mut self, key: String) -> Result<&mut Hash, Frame> {
        let obj = self
            .data
            .entry(key)
            .or_insert_with(|| RedisObject::Hash(Hash::new()));
        match obj {
            RedisObject::Hash(h) => Ok(h),
            _ => Err(Frame::Error(WRONGTYPE.into())),
        }
    }

    /// Return (or create) a mutable list for the given key.
    ///
    /// Returns `Err(Frame)` if the key already holds a non-List type.
    pub(crate) fn get_or_insert_list(&mut self, key: String) -> Result<&mut List, Frame> {
        let obj = self
            .data
            .entry(key)
            .or_insert_with(|| RedisObject::List(List::new()));
        match obj {
            RedisObject::List(l) => Ok(l),
            _ => Err(Frame::Error(WRONGTYPE.into())),
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

    // --- get_str ---

    #[test]
    fn get_str_missing_returns_none() {
        let db = Db::new();
        assert_eq!(db.get_str("k"), Ok(None));
    }

    #[test]
    fn get_str_correct_type_returns_value() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_str("k"), Ok(Some(&b("v"))));
    }

    #[test]
    fn get_str_wrong_type_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert_eq!(db.get_str("k"), Err(wrongtype()));
    }

    // --- get_hash / get_hash_mut ---

    #[test]
    fn get_hash_missing_returns_none() {
        let db = Db::new();
        assert!(db.get_hash("k").unwrap().is_none());
    }

    #[test]
    fn get_hash_correct_type_returns_ref() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        assert!(db.get_hash("k").unwrap().is_some());
    }

    #[test]
    fn get_hash_wrong_type_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_hash("k").unwrap_err(), wrongtype());
    }

    #[test]
    fn get_hash_mut_allows_mutation() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Hash(Hash::new()));
        let h = db.get_hash_mut("k").unwrap().unwrap();
        h.insert(b("f"), b("v"));
        assert_eq!(db.get_hash("k").unwrap().unwrap().len(), 1);
    }

    // --- get_list / get_list_mut ---

    #[test]
    fn get_list_missing_returns_none() {
        let db = Db::new();
        assert!(db.get_list("k").unwrap().is_none());
    }

    #[test]
    fn get_list_wrong_type_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_list("k").unwrap_err(), wrongtype());
    }

    // --- get_or_insert_hash ---

    #[test]
    fn get_or_insert_hash_creates_new() {
        let mut db = Db::new();
        let h = db.get_or_insert_hash("k".into()).unwrap();
        h.insert(b("f"), b("v"));
        assert_eq!(db.get_hash("k").unwrap().unwrap().len(), 1);
    }

    #[test]
    fn get_or_insert_hash_wrong_type_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_or_insert_hash("k".into()).unwrap_err(), wrongtype());
    }

    // --- get_or_insert_list ---

    #[test]
    fn get_or_insert_list_creates_new() {
        let mut db = Db::new();
        let l = db.get_or_insert_list("k".into()).unwrap();
        l.push_front(b("v"));
        assert_eq!(db.get_list("k").unwrap().unwrap().len(), 1);
    }

    #[test]
    fn get_or_insert_list_wrong_type_returns_wrongtype() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert_eq!(db.get_or_insert_list("k".into()).unwrap_err(), wrongtype());
    }

    // --- remove ---

    #[test]
    fn remove_existing_returns_true() {
        let mut db = Db::new();
        db.set("k".into(), RedisObject::Str(b("v")));
        assert!(db.remove("k"));
        assert_eq!(db.get_str("k"), Ok(None));
    }

    #[test]
    fn remove_missing_returns_false() {
        let mut db = Db::new();
        assert!(!db.remove("k"));
    }
}
