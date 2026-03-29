/// Threshold constants that govern encoding promotions.
///
/// REDIS: These mirror the default values from redis.conf.
/// hash-max-listpack-entries 128 / hash-max-listpack-value 64
/// list-max-listpack-size    128 / list-max-listpack-value 64
///
/// Naming them here (rather than scattering literals through the code) makes
/// the intent greppable and the thresholds easy to tune.
pub(crate) const HASH_MAX_LISTPACK_ENTRIES: usize = 128;
pub(crate) const HASH_MAX_LISTPACK_VALUE: usize = 64;

pub(crate) const LIST_MAX_LISTPACK_ENTRIES: usize = 128;
pub(crate) const LIST_MAX_LISTPACK_VALUE: usize = 64;
