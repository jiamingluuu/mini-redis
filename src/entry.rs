//! Per-key metadata wrapper.
//!
//! REDIS: Every key in the Redis keyspace is stored as a `robj` (redis object)
//! inside a `dictEntry`. The entry also carries metadata: an LRU clock or LFU
//! counter (packed into `robj.lru`, 24 bits in server.h), and the expiry time
//! (stored in a separate `expires` dict). We combine all of this into a single
//! `Entry` struct for simplicity.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::object::RedisObject;

/// Initial LFU counter value for new keys.
///
/// REDIS: `LFU_INIT_VAL` in server.h — new keys start at 5 so they aren't
/// immediately evicted before they've had a chance to prove their frequency.
const LFU_INIT_VAL: u8 = 5;

/// Per-key metadata: the value object, optional expiry, and access-tracking
/// data for LRU/LFU eviction.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    obj: Arc<RedisObject>,
    /// Expiry timestamp in epoch milliseconds, or `None` for no expiry.
    /// REDIS: Stored in a separate `expires` dict mapping key → ms timestamp.
    expires_at: Option<u64>,
    /// Seconds since server start (wrapping). Updated on every access when
    /// using LRU eviction policy.
    /// REDIS: `robj.lru` stores `LRU_CLOCK()` — server.lruclock updated by
    /// serverCron every 100ms.
    lru_clock: u32,
    /// LFU data packed into 32 bits:
    /// - bits 16..31: last-decrement time in minutes (wrapping u16)
    /// - bits 0..7:   logarithmic frequency counter (0..255)
    ///
    /// REDIS: Packed into the same 24-bit `robj.lru` field with 16 bits for
    /// decrement time and 8 bits for counter. We use a full u32 for clarity.
    lfu_data: u32,
}

impl Entry {
    pub(crate) fn new(obj: RedisObject, lru_clock: u32) -> Self {
        let now_min = now_minutes();
        Self {
            obj: Arc::new(obj),
            expires_at: None,
            lru_clock,
            lfu_data: ((now_min as u32) << 8) | LFU_INIT_VAL as u32,
        }
    }

    /// Check whether this key has expired.
    pub(crate) fn is_expired(&self, now_ms: u64) -> bool {
        match self.expires_at {
            Some(exp) => now_ms >= exp,
            None => false,
        }
    }

    /// Update the LRU clock (called on every access under LRU policy).
    pub(crate) fn touch_lru(&mut self, clock: u32) {
        self.lru_clock = clock;
    }

    /// Update the LFU counter (called on every access under LFU policy).
    ///
    /// REDIS: `updateLFU()` in db.c — first decays the counter based on
    /// elapsed time, then probabilistically increments it. The probability
    /// of increment decreases as the counter grows, creating a logarithmic
    /// frequency distribution.
    pub(crate) fn touch_lfu(&mut self, now_minutes: u16, log_factor: u8, decay_time: u8) {
        // Step 1: Decay
        let counter = self.lfu_decay(now_minutes, decay_time);

        // Step 2: Probabilistic increment
        // REDIS: LFULogIncr() in evict.c
        let new_counter = if counter == 255 {
            255
        } else {
            let r: f64 = rand::random();
            let base_val = (counter as f64 - LFU_INIT_VAL as f64).max(0.0);
            let p = 1.0 / (base_val * log_factor as f64 + 1.0);
            if r < p { counter + 1 } else { counter }
        };

        // Store updated counter and new decrement timestamp
        self.lfu_data = ((now_minutes as u32) << 8) | new_counter as u32;
    }

    /// Decay the LFU counter based on elapsed time.
    ///
    /// REDIS: `LFUDecrAndReturn()` in evict.c — the counter is decremented by
    /// `elapsed_minutes / decay_time`. This ensures that keys which haven't
    /// been accessed recently have their counters reduced.
    fn lfu_decay(&self, now_minutes: u16, decay_time: u8) -> u8 {
        if decay_time == 0 {
            return self.lfu_counter();
        }
        let last_decr = self.lfu_last_decrement();
        // Wrapping subtraction handles the u16 wrap-around
        let elapsed = now_minutes.wrapping_sub(last_decr);
        let decrement = elapsed as u32 / decay_time as u32;
        let counter = self.lfu_counter() as u32;
        counter.saturating_sub(decrement) as u8
    }

    /// Get the LFU counter after applying time-based decay (for eviction comparison).
    pub(crate) fn lfu_decayed_counter(&self, now_minutes: u16, decay_time: u8) -> u8 {
        self.lfu_decay(now_minutes, decay_time)
    }

    pub(crate) fn lfu_counter(&self) -> u8 {
        (self.lfu_data & 0xFF) as u8
    }

    pub(crate) fn lfu_last_decrement(&self) -> u16 {
        ((self.lfu_data >> 8) & 0xFFFF) as u16
    }

    pub(crate) fn lru_clock(&self) -> u32 {
        self.lru_clock
    }

    pub(crate) fn expires_at(&self) -> Option<u64> {
        self.expires_at
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: Option<u64>) {
        self.expires_at = expires_at;
    }

    pub(crate) fn obj(&self) -> &RedisObject {
        self.obj.as_ref()
    }

    /// Return a mutable object reference using copy-on-write semantics.
    ///
    /// REDIS: BGSAVE snapshots rely on COW page tables after `fork()`. We model
    /// that at the object level: if a snapshot still shares this object, the
    /// first mutation clones the object and preserves snapshot isolation.
    pub(crate) fn obj_mut_cow(&mut self) -> &mut RedisObject {
        Arc::make_mut(&mut self.obj)
    }

    #[allow(dead_code)]
    pub(crate) fn into_obj(self) -> RedisObject {
        Arc::unwrap_or_clone(self.obj)
    }

    /// Rough size estimate for memory tracking.
    ///
    /// REDIS: `objectComputeSize()` in object.c gives precise sizes. We use a
    /// simple approximation: fixed overhead + estimated value size. Good enough
    /// for eviction decisions — the exact number doesn't matter, only the
    /// relative ordering.
    pub(crate) fn estimated_size(&self) -> usize {
        // Entry struct overhead + object size estimate
        let base = std::mem::size_of::<Self>();
        base + self.obj.estimated_size()
    }
}

/// Current epoch time in milliseconds.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Current time in minutes since epoch (wrapping to u16).
///
/// REDIS: LFU uses minutes for its decay timestamp, stored in 16 bits.
pub(crate) fn now_minutes() -> u16 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (secs / 60) as u16
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn str_entry(val: &str) -> Entry {
        Entry::new(RedisObject::Str(Bytes::copy_from_slice(val.as_bytes())), 0)
    }

    #[test]
    fn not_expired_without_ttl() {
        let entry = str_entry("v");
        assert!(!entry.is_expired(u64::MAX));
    }

    #[test]
    fn expired_when_past_deadline() {
        let mut entry = str_entry("v");
        entry.set_expires_at(Some(1000));
        assert!(!entry.is_expired(999));
        assert!(entry.is_expired(1000));
        assert!(entry.is_expired(1001));
    }

    #[test]
    fn lru_touch_updates_clock() {
        let mut entry = str_entry("v");
        assert_eq!(entry.lru_clock(), 0);
        entry.touch_lru(42);
        assert_eq!(entry.lru_clock(), 42);
    }

    #[test]
    fn lfu_initial_counter_is_init_val() {
        let entry = str_entry("v");
        assert_eq!(entry.lfu_counter(), LFU_INIT_VAL);
    }

    #[test]
    fn lfu_counter_increments_probabilistically() {
        let mut entry = str_entry("v");
        let now = now_minutes();
        // With log_factor=0 and low counter, increment probability is ~1.0
        // so after many touches the counter should grow
        for _ in 0..100 {
            entry.touch_lfu(now, 0, 1);
        }
        assert!(entry.lfu_counter() > LFU_INIT_VAL);
    }

    #[test]
    fn lfu_counter_capped_at_255() {
        let mut entry = str_entry("v");
        let now = now_minutes();
        // With log_factor=0, increment is almost guaranteed each touch
        for _ in 0..500 {
            entry.touch_lfu(now, 0, 1);
        }
        assert!(entry.lfu_counter() <= 255);
    }

    #[test]
    fn lfu_decay_reduces_counter() {
        let mut entry = str_entry("v");
        // Manually set counter to 100, last_decrement to 0
        entry.lfu_data = (0u32 << 8) | 100;
        // 10 minutes later with decay_time=1, should decay by 10
        let decayed = entry.lfu_decayed_counter(10, 1);
        assert_eq!(decayed, 90);
    }

    #[test]
    fn lfu_decay_saturates_at_zero() {
        let mut entry = str_entry("v");
        entry.lfu_data = (0u32 << 8) | 5;
        let decayed = entry.lfu_decayed_counter(100, 1);
        assert_eq!(decayed, 0);
    }

    #[test]
    fn persist_removes_expiry() {
        let mut entry = str_entry("v");
        entry.set_expires_at(Some(1000));
        assert!(entry.expires_at().is_some());
        entry.set_expires_at(None);
        assert!(entry.expires_at().is_none());
    }

    #[test]
    fn estimated_size_is_positive() {
        let entry = str_entry("hello");
        assert!(entry.estimated_size() > 0);
    }
}
