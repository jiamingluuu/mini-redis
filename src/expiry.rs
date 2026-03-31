//! Active expiry cycle.
//!
//! REDIS: `activeExpireCycle()` in expire.c runs periodically from serverCron
//! (every 100ms). It samples random keys from the `expires` dict and deletes
//! those that have passed their TTL. If more than 25% of the sample was
//! expired, it repeats immediately — this adaptive loop ensures that under
//! heavy TTL churn, expired keys are cleaned up quickly without blocking the
//! event loop for too long.
//!
//! The "lazy" half (checking TTL on every key access) lives in `db.rs`.

use crate::db::Db;
use crate::entry;

/// Number of keys to sample per iteration.
///
/// REDIS: `ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP` — defaults to 20.
const SAMPLE_SIZE: usize = 20;

/// Maximum iterations before yielding back to the event loop.
///
/// REDIS: `activeExpireCycle` uses a time budget (typically 25% of 100ms).
/// We use an iteration cap for simplicity — same effect in practice.
const MAX_ITERATIONS: usize = 16;

/// Fraction of expired keys in the sample that triggers another round.
/// If >25% of the sample was expired, keep going.
const THRESHOLD_PERCENT: usize = 25;

/// Run one active-expiry cycle.
///
/// Samples random keys with expiry, deletes expired ones, and repeats if
/// the expired fraction exceeds 25%. Returns the total number of keys deleted.
pub(crate) fn active_expire_cycle(db: &mut Db) -> usize {
    let now = entry::now_ms();
    let mut total_deleted = 0;

    for _ in 0..MAX_ITERATIONS {
        let keys = db.random_keys_with_expiry(SAMPLE_SIZE);
        if keys.is_empty() {
            break;
        }

        let sampled = keys.len();
        let mut deleted = 0;

        for key in keys {
            if let Some(Some(expires_at)) = db.get_expiry(&key)
                && now >= expires_at
            {
                db.remove(&key);
                deleted += 1;
            }
        }

        total_deleted += deleted;

        // REDIS: If fewer than 25% were expired, the keyspace is mostly clean
        // — stop early to avoid wasting CPU.
        if sampled == 0 || deleted * 100 / sampled < THRESHOLD_PERCENT {
            break;
        }
    }

    total_deleted
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eviction::EvictionPolicy;
    use crate::object::RedisObject;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn removes_expired_keys() {
        let mut db = Db::new(EvictionPolicy::NoEviction);

        // Insert keys with expiry in the past
        for i in 0..10 {
            db.set_with_expiry(
                format!("expired_{i}"),
                RedisObject::Str(b("v")),
                Some(1), // epoch ms 1 — already expired
            );
        }
        // Insert keys without expiry
        for i in 0..5 {
            db.set(format!("permanent_{i}"), RedisObject::Str(b("v")));
        }

        let deleted = active_expire_cycle(&mut db);

        assert_eq!(deleted, 10);
        assert_eq!(db.len(), 5); // only permanent keys remain
    }

    #[test]
    fn leaves_non_expired_keys() {
        let mut db = Db::new(EvictionPolicy::NoEviction);

        let far_future = entry::now_ms() + 3_600_000;
        for i in 0..10 {
            db.set_with_expiry(
                format!("key_{i}"),
                RedisObject::Str(b("v")),
                Some(far_future),
            );
        }

        let deleted = active_expire_cycle(&mut db);
        assert_eq!(deleted, 0);
        assert_eq!(db.len(), 10);
    }

    #[test]
    fn repeats_when_high_expiry_ratio() {
        let mut db = Db::new(EvictionPolicy::NoEviction);

        // Insert 100 expired keys — should trigger multiple iterations
        for i in 0..100 {
            db.set_with_expiry(format!("exp_{i}"), RedisObject::Str(b("v")), Some(1));
        }

        let deleted = active_expire_cycle(&mut db);

        // Should have cleaned up most or all expired keys across iterations
        assert!(
            deleted > 20,
            "deleted {deleted}, expected >20 (multi-iteration)"
        );
    }

    #[test]
    fn empty_db_is_noop() {
        let mut db = Db::new(EvictionPolicy::NoEviction);
        assert_eq!(active_expire_cycle(&mut db), 0);
    }

    #[test]
    fn no_expiry_keys_is_noop() {
        let mut db = Db::new(EvictionPolicy::NoEviction);
        for i in 0..10 {
            db.set(format!("k{i}"), RedisObject::Str(b("v")));
        }
        assert_eq!(active_expire_cycle(&mut db), 0);
    }
}
