//! TCP server, connection handling, and actor wiring.
//!
//! REDIS: Production Redis uses a single-threaded event loop (ae.c) so the
//! main data structures need no locks. We approximate this with an actor:
//! one Tokio task owns the Db exclusively; all connection tasks send commands
//! to it via a channel and receive responses on one-shot channels.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

mod cmd;
mod object;
mod resp;
mod store;
use resp::{Parser, RespValue, serialize};
use store::DbHandle;

// REDIS: Default port is 6379. Salvatore chose it from a now-defunct Italian
// mobile app — "MERZ" on a phone keypad roughly maps to 6379.
const BIND_ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(BIND_ADDR).await?;
    eprintln!("redis-rs listening on {BIND_ADDR}");

    // Start the store actor. Every connection task gets a clone of this handle.
    // The actor owns the Db exclusively — no Arc<Mutex<...>> needed.
    let db = store::spawn();

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        eprintln!("[+] connection from {peer_addr}");

        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer_addr, db).await {
                eprintln!("[!] {peer_addr}: {e}");
            }
            eprintln!("[-] disconnected: {peer_addr}");
        });
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    peer: SocketAddr,
    db: DbHandle,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];
    let mut parser = Parser::new();

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let commands = parser
            .feed(&buf[..n])
            .map_err(|e| anyhow::anyhow!("parse error from {peer}: {e}"))?;

        for cmd in commands {
            let response = dispatch_to_actor(&db, cmd).await;
            socket.write_all(&serialize(&response)).await?;
        }
    }

    Ok(())
}

/// Extract args from a RESP command and send them to the store actor.
async fn dispatch_to_actor(db: &DbHandle, value: RespValue) -> RespValue {
    // REDIS: Clients always send commands as RESP arrays of bulk strings.
    let RespValue::Array(args) = value else {
        return RespValue::Error(b"ERR protocol error: expected array".to_vec());
    };

    // Handle PING locally — it doesn't touch the store.
    if let Some(RespValue::BulkString(cmd)) = args.first() {
        if cmd.eq_ignore_ascii_case(b"PING") {
            return match args.get(1) {
                Some(RespValue::BulkString(msg)) => RespValue::BulkString(msg.clone()),
                _ => RespValue::SimpleString(b"PONG".to_vec()),
            };
        }
        if cmd.eq_ignore_ascii_case(b"COMMAND") {
            return RespValue::Array(vec![]);
        }
    }

    let raw_args: Vec<Vec<u8>> = args
        .into_iter()
        .filter_map(|v| match v {
            RespValue::BulkString(b) => Some(b),
            _ => None,
        })
        .collect();

    let (reply_tx, reply_rx) = oneshot::channel();
    if db.send(store::Request { args: raw_args, reply_tx }).await.is_err() {
        return RespValue::Error(b"ERR server shutting down".to_vec());
    }
    reply_rx
        .await
        .unwrap_or_else(|_| RespValue::Error(b"ERR internal error".to_vec()))
}
