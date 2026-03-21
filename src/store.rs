//! Store actor — owns the database, processes commands sequentially.
//!
//! REDIS: All command processing runs on Redis's single main thread, so the
//! main data structures need no locks. We achieve the same guarantee via the
//! actor pattern: one Tokio task owns `Db` exclusively, and all connection
//! tasks communicate with it through a channel.
//!
//! The channel serializes commands in arrival order — exactly what Redis's
//! event loop does. Rust's ownership system enforces the invariant at compile
//! time: `Db` cannot be shared across tasks without this indirection.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::cmd;
use crate::object::RedisObject;
use crate::resp::RespValue;

// ── Handle ────────────────────────────────────────────────────────────────────

/// Cloneable sender side of the store channel.
/// Each connection task holds one clone; all share the same actor.
pub type DbHandle = mpsc::Sender<Request>;

// ── Wire types ────────────────────────────────────────────────────────────────

/// A command sent from a connection task to the store actor.
pub struct Request {
    /// Parsed command tokens. args[0] is the command name (already uppercased),
    /// args[1..] are the arguments as raw bytes.
    pub args: Vec<Vec<u8>>,
    /// The actor sends its response back through this one-shot channel.
    pub reply_tx: oneshot::Sender<RespValue>,
}

// ── Database ──────────────────────────────────────────────────────────────────

/// The key-value store.
///
/// REDIS: `server.db` is an array of `redisDb` structs (server.h). Each has
/// a `dict *dict` (keyspace) and a `dict *expires` (TTL map). We implement a
/// single database; expiry is a Phase 5 concern.
pub struct Db {
    pub data: HashMap<String, RedisObject>,
}

impl Db {
    fn new() -> Self {
        Db {
            data: HashMap::new(),
        }
    }
}

// ── Actor ─────────────────────────────────────────────────────────────────────

/// Start the store actor and return a handle to it.
///
/// The actor runs until all `DbHandle` clones are dropped (server shutdown).
pub fn spawn() -> DbHandle {
    // Back-pressure: if 256 commands are queued and unprocessed, senders block.
    // REDIS: Redis doesn't back-pressure this way — it buffers in client output
    // buffers and the OS TCP send buffer. For our purposes 256 is fine.
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(run(rx));
    tx
}

async fn run(mut rx: mpsc::Receiver<Request>) {
    let mut db = Db::new();
    while let Some(req) = rx.recv().await {
        let response = cmd::dispatch(&mut db, req.args);
        // Silently drop if the connection closed before receiving the reply.
        let _ = req.reply_tx.send(response);
    }
}
