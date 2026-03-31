//! Memory eviction policies and cycle logic.
//!
//! REDIS: When `maxmemory` is reached, Redis evicts keys according to a
//! configurable policy. Rather than maintaining a global LRU/LFU structure
//! (expensive for millions of keys), Redis *approximates* by sampling N random
//! keys and evicting the best candidate from the sample. This gives O(1)
//! per-eviction cost with surprisingly good hit-rate — see Antirez's blog post
//! "Random notes on improving the Redis LRU algorithm."

/// Which eviction strategy to use when `maxmemory` is exceeded.
///
/// REDIS: `maxmemory-policy` in redis.conf. We implement the two "allkeys"
/// variants (sample from all keys, not just those with expiry) plus NoEviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionPolicy {
    /// Return OOM errors on writes when over the memory limit.
    NoEviction,
    /// Evict the least-recently-used key from a random sample.
    AllKeysLru,
    /// Evict the least-frequently-used key from a random sample.
    AllKeysLfu,
}

/// Tuning knobs for the eviction subsystem.
#[derive(Debug, Clone)]
pub(crate) struct EvictionConfig {
    pub(crate) policy: EvictionPolicy,
    /// Maximum memory in bytes. 0 means no limit.
    pub(crate) maxmemory: usize,
    /// Number of random keys to sample per eviction round (Redis default: 5).
    pub(crate) maxmemory_samples: usize,
    /// LFU logarithmic counter factor (Redis default: 10).
    #[allow(dead_code)]
    pub(crate) lfu_log_factor: u8,
    /// LFU decay time in minutes (Redis default: 1).
    pub(crate) lfu_decay_time: u8,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            policy: EvictionPolicy::NoEviction,
            maxmemory: 0,
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
        }
    }
}

/// Run one eviction cycle: keep evicting until `used_memory` drops below
/// `maxmemory`, or until we can't evict anything (NoEviction / empty db).
///
/// REDIS: `freeMemoryIfNeeded()` in evict.c — called before every write
/// command when maxmemory is set. It loops, sampling `maxmemory_samples`
/// keys each iteration, picking the best victim by policy, and deleting it.
///
/// Returns `true` if memory is now under the limit (or no limit is set),
/// `false` if we're still over (NoEviction policy, or db is empty).
pub(crate) fn eviction_cycle(db: &mut crate::db::Db, config: &EvictionConfig) -> bool {
    if config.maxmemory == 0 || db.used_memory() <= config.maxmemory {
        return true;
    }

    if config.policy == EvictionPolicy::NoEviction {
        return false;
    }

    // REDIS: evict.c loops until memory is under limit or no more keys.
    // Each iteration samples N keys and evicts the best candidate.
    let mut iterations = 0;
    while db.used_memory() > config.maxmemory && db.len() > 0 {
        let keys = db.random_keys(config.maxmemory_samples);
        if keys.is_empty() {
            break;
        }

        let victim = match config.policy {
            EvictionPolicy::AllKeysLru => find_lru_victim(db, &keys),
            EvictionPolicy::AllKeysLfu => find_lfu_victim(db, &keys, config),
            EvictionPolicy::NoEviction => unreachable!(),
        };

        if let Some(key) = victim {
            db.remove(&key);
        } else {
            break;
        }

        iterations += 1;
        if iterations > 128 {
            // Safety valve — don't spin forever.
            break;
        }
    }

    db.used_memory() <= config.maxmemory
}

/// Pick the key with the oldest LRU clock from the sample.
fn find_lru_victim(db: &crate::db::Db, keys: &[String]) -> Option<String> {
    let mut best: Option<(String, u32)> = None;
    for key in keys {
        if let Some(entry) = db.get_entry(key) {
            let clock = entry.lru_clock();
            match &best {
                None => best = Some((key.clone(), clock)),
                Some((_, best_clock)) => {
                    // Lower clock = older access = better victim
                    if clock < *best_clock {
                        best = Some((key.clone(), clock));
                    }
                }
            }
        }
    }
    best.map(|(k, _)| k)
}

/// Pick the key with the lowest LFU counter from the sample.
///
/// REDIS: Before comparing counters, Redis decays them based on elapsed time
/// since last access. We do the same via `Entry::lfu_decayed_counter()`.
fn find_lfu_victim(db: &crate::db::Db, keys: &[String], config: &EvictionConfig) -> Option<String> {
    let now_minutes = crate::entry::now_minutes();
    let mut best: Option<(String, u8)> = None;
    for key in keys {
        if let Some(entry) = db.get_entry(key) {
            let counter = entry.lfu_decayed_counter(now_minutes, config.lfu_decay_time);
            match &best {
                None => best = Some((key.clone(), counter)),
                Some((_, best_counter)) => {
                    if counter < *best_counter {
                        best = Some((key.clone(), counter));
                    }
                }
            }
        }
    }
    best.map(|(k, _)| k)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::entry::Entry;
    use crate::object::RedisObject;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn no_eviction_returns_false_when_over_limit() {
        let config = EvictionConfig {
            policy: EvictionPolicy::NoEviction,
            maxmemory: 1, // impossibly small
            ..Default::default()
        };
        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("k".into(), RedisObject::Str(b("value")));

        assert!(!eviction_cycle(&mut db, &config));
    }

    #[test]
    fn no_limit_always_returns_true() {
        let config = EvictionConfig::default(); // maxmemory = 0
        let mut db = Db::new(EvictionPolicy::NoEviction);
        db.set("k".into(), RedisObject::Str(b("value")));

        assert!(eviction_cycle(&mut db, &config));
    }

    #[test]
    fn lru_evicts_oldest_key() {
        let mut db = Db::new(EvictionPolicy::AllKeysLru);

        // Insert keys with increasing LRU clocks (via touch)
        db.set("old".into(), RedisObject::Str(b("v")));
        // Touch "old" at clock=1
        if let Some(e) = db.get_entry_mut("old") {
            e.touch_lru(1);
        }

        db.set("new".into(), RedisObject::Str(b("v")));
        if let Some(e) = db.get_entry_mut("new") {
            e.touch_lru(100);
        }

        let config = EvictionConfig {
            policy: EvictionPolicy::AllKeysLru,
            maxmemory: 1,
            maxmemory_samples: 10,
            ..Default::default()
        };

        // Should evict "old" first (lower LRU clock)
        let keys = db.random_keys(10);
        let victim = find_lru_victim(&db, &keys);
        assert_eq!(victim, Some("old".into()));
    }

    #[test]
    fn lfu_evicts_lowest_counter() {
        let mut db = Db::new(EvictionPolicy::AllKeysLfu);

        db.set("cold".into(), RedisObject::Str(b("v")));
        // LFU counter starts at 5 (LFU_INIT_VAL). Don't touch it.

        db.set("hot".into(), RedisObject::Str(b("v")));
        // Bump "hot" counter by touching it many times
        let now_min = crate::entry::now_minutes();
        for _ in 0..50 {
            if let Some(e) = db.get_entry_mut("hot") {
                e.touch_lfu(now_min, 10, 1);
            }
        }

        let config = EvictionConfig {
            policy: EvictionPolicy::AllKeysLfu,
            maxmemory: 1,
            maxmemory_samples: 10,
            ..Default::default()
        };

        let keys = db.random_keys(10);
        let victim = find_lfu_victim(&db, &keys, &config);
        assert_eq!(victim, Some("cold".into()));
    }

    #[test]
    fn eviction_cycle_removes_keys_until_under_limit() {
        let mut db = Db::new(EvictionPolicy::AllKeysLru);

        // Insert several keys
        for i in 0..10 {
            let key = format!("key{i}");
            db.set(key, RedisObject::Str(b("value")));
        }

        let target = db.used_memory() / 2;
        let config = EvictionConfig {
            policy: EvictionPolicy::AllKeysLru,
            maxmemory: target,
            maxmemory_samples: 5,
            ..Default::default()
        };

        let result = eviction_cycle(&mut db, &config);
        assert!(result);
        assert!(db.used_memory() <= target);
        assert!(db.len() < 10);
    }
}
