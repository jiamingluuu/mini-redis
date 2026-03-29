mod cmd;
mod connection;
mod db;
mod encoding;
mod object;
mod resp;
mod server;
mod store;
mod types;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("Listening on {addr}");

    let (store_handle, store) = store::StoreHandle::new();
    tokio::spawn(store.run());

    server::run(listener, store_handle).await;

    Ok(())
}
