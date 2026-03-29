use tokio::sync::{mpsc, oneshot};

use crate::cmd::{Command, CommandHandler};
use crate::db::Db;
use crate::resp::frame::Frame;

// ---------------------------------------------------------------------------
// Actor message
// ---------------------------------------------------------------------------

pub(crate) enum StoreCmd {
    Execute {
        cmd: Command,
        reply: oneshot::Sender<Frame>,
    },
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// Owns the key-value store and processes commands serially.
///
/// REDIS: The single-threaded event loop in ae.c guarantees that only one
/// command touches the data at a time. We achieve the same guarantee via Rust
/// ownership: no reference to `db` ever escapes this task.
pub(crate) struct Store {
    db: Db,
    rx: mpsc::Receiver<StoreCmd>,
}

impl Store {
    fn new(rx: mpsc::Receiver<StoreCmd>) -> Self {
        Self { db: Db::new(), rx }
    }

    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                StoreCmd::Execute { cmd, reply } => {
                    let frame = self.execute(cmd);
                    let _ = reply.send(frame);
                }
            }
        }
    }

    fn execute(&mut self, cmd: Command) -> Frame {
        cmd.execute(&mut self.db)
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct StoreHandle {
    tx: mpsc::Sender<StoreCmd>,
}

impl StoreHandle {
    pub(crate) fn new() -> (Self, Store) {
        let (tx, rx) = mpsc::channel(256);
        (Self { tx }, Store::new(rx))
    }

    pub(crate) async fn execute(&self, cmd: Command) -> anyhow::Result<Frame> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCmd::Execute {
                cmd,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("store actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("store actor dropped reply"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::cmd::{HashCmd, ListCmd, StringCmd};
    use crate::db::WRONGTYPE;

    async fn spawn() -> StoreHandle {
        let (h, store) = StoreHandle::new();
        tokio::spawn(store.run());
        h
    }

    async fn exec(h: &StoreHandle, cmd: Command) -> Frame {
        h.execute(cmd).await.unwrap()
    }

    // --- String ---

    #[tokio::test]
    async fn get_missing_returns_null() {
        let h = spawn().await;
        assert_eq!(
            exec(&h, Command::String(StringCmd::Get("k".into()))).await,
            Frame::Null
        );
    }

    #[tokio::test]
    async fn set_then_get() {
        let h = spawn().await;
        exec(
            &h,
            Command::String(StringCmd::Set("k".into(), Bytes::from_static(b"v"))),
        )
        .await;
        assert_eq!(
            exec(&h, Command::String(StringCmd::Get("k".into()))).await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[tokio::test]
    async fn del_returns_count() {
        let h = spawn().await;
        exec(
            &h,
            Command::String(StringCmd::Set("a".into(), Bytes::from_static(b"1"))),
        )
        .await;
        exec(
            &h,
            Command::String(StringCmd::Set("b".into(), Bytes::from_static(b"2"))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::String(StringCmd::Del(vec!["a".into(), "b".into(), "c".into()]))
            )
            .await,
            Frame::Integer(2)
        );
    }

    #[tokio::test]
    async fn get_on_wrong_type_returns_wrongtype() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "k".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::String(StringCmd::Get("k".into()))).await,
            Frame::Error(WRONGTYPE.into())
        );
    }

    // --- Hash ---

    #[tokio::test]
    async fn hset_creates_hash_and_returns_new_count() {
        let h = spawn().await;
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HSet(
                    "myhash".into(),
                    vec![
                        (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                        (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                    ]
                ))
            )
            .await,
            Frame::Integer(2)
        );
    }

    #[tokio::test]
    async fn hset_update_existing_field_counts_zero() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v1"))],
            )),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HSet(
                    "h".into(),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v2"))],
                ))
            )
            .await,
            Frame::Integer(0)
        );
    }

    #[tokio::test]
    async fn hget_returns_value_or_null() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HGet("h".into(), Bytes::from_static(b"f")))
            )
            .await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HGet("h".into(), Bytes::from_static(b"missing")))
            )
            .await,
            Frame::Null
        );
    }

    #[tokio::test]
    async fn hdel_returns_removed_count() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "h".into(),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            )),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HDel(
                    "h".into(),
                    vec![Bytes::from_static(b"f1"), Bytes::from_static(b"nope")]
                ))
            )
            .await,
            Frame::Integer(1)
        );
    }

    #[tokio::test]
    async fn hgetall_returns_interleaved_pairs() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )),
        )
        .await;
        let frame = exec(&h, Command::Hash(HashCmd::HGetAll("h".into()))).await;
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"f")),
                Frame::Bulk(Bytes::from_static(b"v")),
            ])
        );
    }

    #[tokio::test]
    async fn hlen_and_hexists() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::Hash(HashCmd::HLen("h".into()))).await,
            Frame::Integer(1)
        );
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HExists("h".into(), Bytes::from_static(b"f")))
            )
            .await,
            Frame::Integer(1)
        );
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HExists("h".into(), Bytes::from_static(b"nope")))
            )
            .await,
            Frame::Integer(0)
        );
    }

    #[tokio::test]
    async fn hset_on_string_key_returns_wrongtype() {
        let h = spawn().await;
        exec(
            &h,
            Command::String(StringCmd::Set("k".into(), Bytes::from_static(b"v"))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Hash(HashCmd::HSet(
                    "k".into(),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                ))
            )
            .await,
            Frame::Error(WRONGTYPE.into())
        );
    }

    // --- List ---

    #[tokio::test]
    async fn lpush_creates_list_and_returns_length() {
        let h = spawn().await;
        assert_eq!(
            exec(
                &h,
                Command::List(ListCmd::LPush("k".into(), vec![Bytes::from_static(b"a")]))
            )
            .await,
            Frame::Integer(1)
        );
    }

    #[tokio::test]
    async fn lpush_ordering() {
        let h = spawn().await;
        exec(
            &h,
            Command::List(ListCmd::LPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::List(ListCmd::LRange("k".into(), 0, -1))).await,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"c")),
                Frame::Bulk(Bytes::from_static(b"b")),
                Frame::Bulk(Bytes::from_static(b"a")),
            ])
        );
    }

    #[tokio::test]
    async fn rpush_ordering() {
        let h = spawn().await;
        exec(
            &h,
            Command::List(ListCmd::RPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::List(ListCmd::LRange("k".into(), 0, -1))).await,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"a")),
                Frame::Bulk(Bytes::from_static(b"b")),
                Frame::Bulk(Bytes::from_static(b"c")),
            ])
        );
    }

    #[tokio::test]
    async fn lpop_no_count_returns_bulk() {
        let h = spawn().await;
        exec(
            &h,
            Command::List(ListCmd::LPush("k".into(), vec![Bytes::from_static(b"v")])),
        )
        .await;
        assert_eq!(
            exec(&h, Command::List(ListCmd::LPop("k".into(), None))).await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[tokio::test]
    async fn rpop_with_count_returns_array() {
        let h = spawn().await;
        exec(
            &h,
            Command::List(ListCmd::RPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::List(ListCmd::RPop("k".into(), Some(2)))).await,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"c")),
                Frame::Bulk(Bytes::from_static(b"b")),
            ])
        );
    }

    #[tokio::test]
    async fn llen_returns_length() {
        let h = spawn().await;
        exec(
            &h,
            Command::List(ListCmd::RPush(
                "k".into(),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )),
        )
        .await;
        assert_eq!(
            exec(&h, Command::List(ListCmd::LLen("k".into()))).await,
            Frame::Integer(2)
        );
    }

    #[tokio::test]
    async fn lpush_on_hash_returns_wrongtype() {
        let h = spawn().await;
        exec(
            &h,
            Command::Hash(HashCmd::HSet(
                "k".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::List(ListCmd::LPush("k".into(), vec![Bytes::from_static(b"v")]))
            )
            .await,
            Frame::Error(WRONGTYPE.into())
        );
    }
}
