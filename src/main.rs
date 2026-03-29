mod cmd;
mod connection;
mod db;
mod encoding;
mod object;
mod persist;
mod resp;
mod server;
mod store;
mod types;

use std::path::PathBuf;

use clap::Parser;

use persist::aof::Aof;
use persist::{AofConfig, FsyncPolicy};

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

    let aof_config = AofConfig {
        path: cli.aof_path,
        fsync,
        enabled: cli.aof_enabled,
    };

    // REDIS: On startup, if AOF is enabled and the file exists, Redis replays
    // it through a fake client to rebuild the in-memory state (aof.c
    // loadAppendOnlyFile). If the file doesn't exist, start with an empty Db.
    let (db, aof) = if aof_config.enabled {
        let db = if aof_config.path.exists() {
            eprintln!("Replaying AOF from {:?}…", aof_config.path);
            let db = Aof::replay(&aof_config.path)?;
            eprintln!("AOF replay complete");
            db
        } else {
            eprintln!("AOF enabled, creating new file at {:?}", aof_config.path);
            db::Db::new()
        };
        let aof = Aof::open(&aof_config)?;
        (db, Some(aof))
    } else {
        (db::Db::new(), None)
    };

    let addr = "127.0.0.1:6379";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("Listening on {addr}");

    let (store_handle, store) = store::StoreHandle::new(db, aof);
    tokio::spawn(store.run());

    server::run(listener, store_handle).await;

    Ok(())
}
