mod cmd;
mod connection;
mod db;
mod encoding;
mod entry;
mod eviction;
mod expiry;
mod object;
mod persist;
mod resp;
mod server;
mod store;
mod types;

use std::path::PathBuf;

use clap::Parser;

use eviction::{EvictionConfig, EvictionPolicy};
use persist::aof::Aof;
use persist::{AofConfig, FsyncPolicy, RdbConfig};

/// A Redis-compatible server implemented in Rust for learning purposes.
#[derive(Parser)]
#[command(name = "redis")]
struct Cli {
    /// Enable AOF persistence
    #[arg(long, default_value_t = false)]
    aof_enabled: bool,

    /// Path to the AOF file
    #[arg(long, default_value = "./appendonly.aof")]
    aof_path: PathBuf,

    /// AOF fsync policy: always, everysec, or no
    #[arg(long, default_value = "everysec")]
    appendfsync: String,

    /// Enable RDB snapshot persistence
    #[arg(long, default_value_t = false)]
    rdb_enabled: bool,

    /// Path to the RDB snapshot file
    #[arg(long, default_value = "./dump.rdb")]
    rdb_path: PathBuf,

    /// Maximum memory in bytes (0 = no limit)
    #[arg(long, default_value_t = 0)]
    maxmemory: usize,

    /// Eviction policy: noeviction, allkeys-lru, allkeys-lfu
    #[arg(long, default_value = "noeviction")]
    maxmemory_policy: String,

    /// Number of random keys to sample per eviction round
    #[arg(long, default_value_t = 5)]
    maxmemory_samples: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let fsync = match cli.appendfsync.as_str() {
        "always" => FsyncPolicy::Always,
        "everysec" => FsyncPolicy::EverySecond,
        "no" => FsyncPolicy::No,
        other => anyhow::bail!(
            "invalid appendfsync value '{other}': must be 'always', 'everysec', or 'no'"
        ),
    };

    let eviction_policy = match cli.maxmemory_policy.as_str() {
        "noeviction" => EvictionPolicy::NoEviction,
        "allkeys-lru" => EvictionPolicy::AllKeysLru,
        "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
        other => anyhow::bail!(
            "invalid maxmemory-policy '{other}': must be 'noeviction', 'allkeys-lru', or 'allkeys-lfu'"
        ),
    };

    let eviction_config = EvictionConfig {
        policy: eviction_policy,
        maxmemory: cli.maxmemory,
        maxmemory_samples: cli.maxmemory_samples,
        ..Default::default()
    };

    let aof_config = AofConfig {
        path: cli.aof_path,
        fsync,
        enabled: cli.aof_enabled,
    };

    let rdb_config = RdbConfig {
        path: cli.rdb_path,
        enabled: cli.rdb_enabled,
    };

    // REDIS: On startup, loading priority matches Redis:
    // 1. If AOF is enabled and the file exists → replay AOF (most recent data)
    // 2. Else if RDB is enabled and file exists → load RDB (fast binary load)
    // 3. Else → empty Db
    let mut db = if aof_config.enabled && aof_config.path.exists() {
        eprintln!("Replaying AOF from {:?}…", aof_config.path);
        let db = Aof::replay(&aof_config.path)?;
        eprintln!("AOF replay complete");
        db
    } else if rdb_config.enabled && rdb_config.path.exists() {
        eprintln!("Loading RDB from {:?}…", rdb_config.path);
        let db = persist::rdb::decode(&rdb_config.path)?;
        eprintln!("RDB load complete");
        db
    } else {
        if aof_config.enabled {
            eprintln!("AOF enabled, creating new file at {:?}", aof_config.path);
        }
        if rdb_config.enabled {
            eprintln!("RDB enabled, snapshot path {:?}", rdb_config.path);
        }
        db::Db::new(eviction_policy)
    };

    db.set_eviction_policy(eviction_policy);

    let aof = if aof_config.enabled {
        Some(Aof::open(&aof_config)?)
    } else {
        None
    };

    let addr = "127.0.0.1:6379";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("Listening on {addr}");

    if cli.maxmemory > 0 {
        eprintln!(
            "maxmemory: {} bytes, policy: {:?}",
            cli.maxmemory, eviction_policy
        );
    }

    let (store_handle, store) = store::StoreHandle::new(db, aof, eviction_config);
    tokio::spawn(store.run());

    server::run(listener, store_handle, rdb_config.path).await;

    Ok(())
}
