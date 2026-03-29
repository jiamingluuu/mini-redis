use tokio::net::TcpListener;

use crate::connection::Connection;
use crate::store::StoreHandle;

/// Accept TCP connections in a loop, spawning one task per client.
///
/// REDIS: Redis uses a single-threaded event loop (ae.c) to multiplex all
/// clients. We use tokio tasks instead, which gives us OS-level preemption
/// and multi-core utilisation for free while keeping the data ownership model
/// identical — all data access still flows through the single store actor.
pub async fn run(listener: TcpListener, store: StoreHandle) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };

        let store = store.clone();
        tokio::spawn(async move {
            eprintln!("client connected: {peer}");
            Connection::new(stream, store).run().await;
            eprintln!("client disconnected: {peer}");
        });
    }
}
