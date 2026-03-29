pub(crate) mod aof;
pub(crate) mod crc64;
pub(crate) mod rdb;

pub(crate) use rdb::RdbConfig;

use std::path::PathBuf;

/// How often the AOF file is fsynced to disk.
///
/// REDIS: Redis offers three appendfsync policies in redis.conf. The trade-off
/// is durability vs latency:
/// - `Always` — fsync after every write; at most one command lost on crash, but
///   highest latency (the `fdatasync` syscall blocks the event loop).
/// - `EverySecond` — fsync once per second via a background timer; up to ~1s of
///   writes can be lost. This is Redis's recommended default.
/// - `No` — let the OS decide when to flush; best throughput but unbounded data
///   loss window (typically 30s on Linux).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FsyncPolicy {
    Always,
    EverySecond,
    No,
}

/// Configuration for the AOF subsystem.
#[derive(Debug, Clone)]
pub(crate) struct AofConfig {
    pub(crate) path: PathBuf,
    pub(crate) fsync: FsyncPolicy,
    pub(crate) enabled: bool,
}
