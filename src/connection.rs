use std::path::PathBuf;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::cmd::{CmdError, Command};
use crate::resp::{frame::Frame, parser, writer};
use crate::store::StoreHandle;

/// Per-connection state: an I/O buffer and a handle to the store actor.
///
/// REDIS: Redis keeps one querybuf per client (networking.c). We use a single
/// BytesMut for input, matching that design. Responses are written directly to
/// the socket rather than queued in an output buffer, which is a simplification
/// relative to Redis's reply list.
pub(crate) struct Connection {
    stream: TcpStream,
    buf: BytesMut,
    store: StoreHandle,
    rdb_path: PathBuf,
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, store: StoreHandle, rdb_path: PathBuf) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            store,
            rdb_path,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            match parser::parse(&mut self.buf) {
                Ok(Some(frame)) => {
                    let response = self.handle(frame).await;
                    if self.write_frame(&response).await.is_err() {
                        return;
                    }
                    // Loop back immediately — pipelined frames may still be
                    // waiting in the buffer without needing another read.
                }
                Ok(None) => match self.stream.read_buf(&mut self.buf).await {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(_) => return,
                },
                Err(e) => {
                    let err = Frame::Error(format!("ERR protocol error: {e}"));
                    let _ = self.write_frame(&err).await;
                    return;
                }
            }
        }
    }

    async fn handle(&self, frame: Frame) -> Frame {
        match Command::from_frame(frame) {
            Ok(cmd) => match self.store.execute(cmd).await {
                Ok(f) => f,
                Err(_) => Frame::Error("ERR internal error".into()),
            },
            Err(CmdError::UnknownCommand(ref name)) if is_admin(name) => {
                self.handle_admin(name).await
            }
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    /// Handle admin commands that don't go through the normal Command pipeline.
    ///
    /// REDIS: Commands like SAVE, BGSAVE, CONFIG, etc. are administrative —
    /// they don't operate on the keyspace via CommandHandler. We route them
    /// separately to keep Command focused on data operations.
    async fn handle_admin(&self, name: &str) -> Frame {
        match name {
            "SAVE" => match self.store.save().await {
                Ok(f) => f,
                Err(_) => Frame::Error("ERR internal error".into()),
            },
            "BGSAVE" => match self.store.bgsave(self.rdb_path.clone()).await {
                Ok(f) => f,
                Err(_) => Frame::Error("ERR internal error".into()),
            },
            _ => Frame::Error(format!("ERR unknown command '{name}'")),
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
        let mut out = BytesMut::new();
        writer::encode(frame, &mut out);
        self.stream.write_all(&out).await
    }
}

/// Check if a command name is an admin command handled outside the normal pipeline.
fn is_admin(name: &str) -> bool {
    matches!(name, "SAVE" | "BGSAVE")
}
