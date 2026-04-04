use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::cmd::{Command, CommandHandler, IntoResp};
use crate::db::Db;
use crate::eviction::{self, EvictionConfig};
use crate::persist::FsyncPolicy;
use crate::persist::aof::Aof;
use crate::resp::frame::Frame;

// ---------------------------------------------------------------------------
// Actor message
// ---------------------------------------------------------------------------

pub(crate) enum StoreCmd {
    Execute {
        cmd: Command,
        reply: oneshot::Sender<Frame>,
    },
    /// Trigger an explicit AOF fsync and reply with +OK or an error.
    Save { reply: oneshot::Sender<Frame> },
    /// Snapshot the Db to an RDB file in a background task.
    ///
    /// REDIS: BGSAVE in rdb.c — Redis forks and the child writes the snapshot.
    /// We model this with app-level COW: capture a Db snapshot made of Arc
    /// handles at command dispatch time, then encode it in a background task.
    /// The reply is sent immediately with +OK.
    BgSave {
        path: PathBuf,
        reply: oneshot::Sender<Frame>,
    },
    /// Internal completion event from the background snapshot worker.
    BgSaveDone {
        result: Result<(), String>,
        duration: Duration,
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
    tx: mpsc::Sender<StoreCmd>,
    aof: Option<Aof>,
    aof_error: Option<String>,
    eviction_config: EvictionConfig,
    bgsave_running: bool,
    last_bgsave_error: Option<String>,
    last_bgsave_duration: Option<Duration>,
}

impl Store {
    pub(crate) fn new(
        db: Db,
        tx: mpsc::Sender<StoreCmd>,
        rx: mpsc::Receiver<StoreCmd>,
        aof: Option<Aof>,
        eviction_config: EvictionConfig,
    ) -> Self {
        Self {
            db,
            rx,
            tx,
            aof,
            aof_error: None,
            eviction_config,
            bgsave_running: false,
            last_bgsave_error: None,
            last_bgsave_duration: None,
        }
    }

    /// Run the store actor loop.
    ///
    /// Uses `tokio::select!` to multiplex three concerns:
    /// 1. Client commands (always)
    /// 2. serverCron tick at 100ms (always — drives active expiry + eviction)
    /// 3. AOF fsync tick at 1s (only when appendfsync=everysec)
    ///
    /// REDIS: Redis's event loop (ae.c) similarly multiplexes file events
    /// (client commands) and time events (serverCron for background tasks
    /// like active expiry, AOF fsync, replication heartbeats).
    pub(crate) async fn run(mut self) {
        let needs_aof_tick = self.aof.is_some()
            && self
                .aof
                .as_ref()
                .is_some_and(|a| a.fsync_policy() == FsyncPolicy::EverySecond);

        // REDIS: serverCron runs at server.hz (default 10 = every 100ms).
        let mut cron_interval = tokio::time::interval(std::time::Duration::from_millis(100));
        cron_interval.tick().await; // skip first immediate tick

        let mut aof_interval = tokio::time::interval(std::time::Duration::from_secs(1));
        aof_interval.tick().await;

        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(cmd) => self.handle(cmd),
                        None => return,
                    }
                }
                _ = cron_interval.tick() => {
                    self.server_cron();
                }
                _ = aof_interval.tick(), if needs_aof_tick => {
                    if let Some(aof) = &mut self.aof
                        && let Err(e) = aof.fsync()
                    {
                        self.record_aof_error(format!("AOF fsync failed: {e}"));
                    }
                }
            }
        }
    }

    /// Periodic background work, analogous to Redis's `serverCron()`.
    ///
    /// REDIS: serverCron in server.c runs every 100ms and handles:
    /// - Active expiry sampling (expire.c)
    /// - Memory eviction checks (evict.c)
    /// - Replication heartbeats, lazy-free, etc. (not yet implemented)
    fn server_cron(&mut self) {
        crate::expiry::active_expire_cycle(&mut self.db);
        eviction::eviction_cycle(&mut self.db, &self.eviction_config);
    }

    fn handle(&mut self, msg: StoreCmd) {
        match msg {
            StoreCmd::Execute { cmd, reply } => {
                let frame = self.execute(cmd);
                let _ = reply.send(frame);
            }
            StoreCmd::Save { reply } => {
                let frame = if let Some(frame) = self.aof_misconf_frame() {
                    frame
                } else {
                    match &mut self.aof {
                        Some(aof) => match aof.fsync() {
                            Ok(()) => Frame::Simple("OK".into()),
                            Err(e) => {
                                self.record_aof_error(format!("AOF fsync failed: {e}"));
                                self.aof_misconf_frame()
                                    .expect("latched AOF error should produce a reply")
                            }
                        },
                        None => Frame::Error("ERR AOF is not enabled".into()),
                    }
                };
                let _ = reply.send(frame);
            }
            StoreCmd::BgSave { path, reply } => {
                // REDIS: Return BUSY if a background save is already in progress.
                if self.bgsave_running {
                    let _ = reply.send(Frame::Error(
                        "ERR Background save already in progress".into(),
                    ));
                    return;
                }

                // REDIS: Snapshot boundary is when BGSAVE is handled by the main
                // loop. We capture both snapshot data and snapshot time here.
                let snapshot_ms = crate::entry::now_ms();
                let snapshot = self.db.snapshot();
                let tx = self.tx.clone();

                self.bgsave_running = true;
                let _ = reply.send(Frame::Simple("Background saving started".into()));

                tokio::spawn(async move {
                    let started = std::time::Instant::now();
                    let result = match tokio::task::spawn_blocking(move || {
                        crate::persist::rdb::encode_snapshot(&snapshot, &path, snapshot_ms)
                    })
                    .await
                    {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(e) => Err(format!("BGSAVE task failed: {e}")),
                    };

                    let _ = tx
                        .send(StoreCmd::BgSaveDone {
                            result,
                            duration: started.elapsed(),
                        })
                        .await;
                });
            }
            StoreCmd::BgSaveDone { result, duration } => {
                self.bgsave_running = false;
                self.last_bgsave_duration = Some(duration);
                match result {
                    Ok(()) => {
                        self.last_bgsave_error = None;
                    }
                    Err(err) => {
                        eprintln!("BGSAVE error: {err}");
                        self.last_bgsave_error = Some(err);
                    }
                }
            }
        }
    }

    fn execute(&mut self, cmd: Command) -> Frame {
        match cmd {
            Command::Write(w) => {
                if let Some(frame) = self.aof_misconf_frame() {
                    return frame;
                }

                // REDIS: processCommand() in server.c calls freeMemoryIfNeeded()
                // before executing any write command. If we're over maxmemory with
                // NoEviction policy, reject the write with an OOM error.
                if !eviction::eviction_cycle(&mut self.db, &self.eviction_config) {
                    return Frame::Error(
                        "OOM command not allowed when used memory > 'maxmemory'".into(),
                    );
                }

                // REDIS: AOF logs the command *after* successful execution.
                // We serialize the RESP bytes *before* execute() consumes `w`,
                // since IntoResp::to_resp_bytes takes &self (non-consuming).
                let resp_bytes = w.to_resp_bytes();
                let frame = w.execute(&mut self.db);

                // Only log successful mutations — WRONGTYPE errors mean the
                // command had no effect and should not be replayed.
                if !matches!(frame, Frame::Error(_))
                    && let Some(aof) = &mut self.aof
                    && let Err(e) = aof.append_bytes(&resp_bytes)
                {
                    self.record_aof_error(format!("AOF append failed: {e}"));
                    return self
                        .aof_misconf_frame()
                        .expect("latched AOF error should produce a reply");
                }

                frame
            }
            Command::Read(r) => r.execute(&mut self.db),
        }
    }

    fn record_aof_error(&mut self, message: String) {
        if self.aof_error.is_none() {
            self.aof_error = Some(message);
        }
    }

    fn aof_misconf_frame(&self) -> Option<Frame> {
        self.aof_error.as_ref().map(|message| {
            Frame::Error(format!(
                "MISCONF AOF writes are disabled because persistence failed: {message}"
            ))
        })
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
    pub(crate) fn new(db: Db, aof: Option<Aof>, eviction_config: EvictionConfig) -> (Self, Store) {
        let (tx, rx) = mpsc::channel(256);
        (
            Self { tx: tx.clone() },
            Store::new(db, tx, rx, aof, eviction_config),
        )
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

    /// Send a BGSAVE command to snapshot the Db to an RDB file.
    pub(crate) async fn bgsave(&self, path: PathBuf) -> anyhow::Result<Frame> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCmd::BgSave {
                path,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("store actor is gone"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("store actor dropped reply"))
    }

    /// Send a SAVE command to trigger AOF fsync.
    pub(crate) async fn save(&self) -> anyhow::Result<Frame> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(StoreCmd::Save { reply: reply_tx })
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
    use std::io;
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::cmd::{
        HashRead, HashWrite, ListRead, ListWrite, ReadCmd, StringRead, StringWrite, WriteCmd,
    };
    use crate::db::WRONGTYPE;
    use crate::persist::FsyncPolicy;
    use crate::persist::aof::AofWriter;

    struct FailingWriter {
        fail_writes: bool,
        fail_sync: bool,
    }

    impl AofWriter for FailingWriter {
        fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
            if self.fail_writes {
                Err(io::Error::other("simulated append failure"))
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn sync_all(&mut self) -> io::Result<()> {
            if self.fail_sync {
                Err(io::Error::other("simulated fsync failure"))
            } else {
                Ok(())
            }
        }
    }

    async fn spawn() -> StoreHandle {
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());
        h
    }

    async fn spawn_with_aof(aof: Aof) -> StoreHandle {
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            Some(aof),
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());
        h
    }

    async fn exec(h: &StoreHandle, cmd: Command) -> Frame {
        h.execute(cmd).await.unwrap()
    }

    async fn wait_for_rdb(path: &Path) {
        for _ in 0..200 {
            if path.exists() && crate::persist::rdb::decode(path).is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for RDB at {}", path.display());
    }

    // --- String ---

    #[tokio::test]
    async fn get_missing_returns_null() {
        let h = spawn().await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::String(StringRead::Get("k".into())))
            )
            .await,
            Frame::Null
        );
    }

    #[tokio::test]
    async fn set_then_get() {
        let h = spawn().await;
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::String(StringRead::Get("k".into())))
            )
            .await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[tokio::test]
    async fn del_returns_count() {
        let h = spawn().await;
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "a".into(),
                Bytes::from_static(b"1"),
                None,
            ))),
        )
        .await;
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "b".into(),
                Bytes::from_static(b"2"),
                None,
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::String(StringWrite::Del(vec![
                    "a".into(),
                    "b".into(),
                    "c".into()
                ])))
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "k".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::String(StringRead::Get("k".into())))
            )
            .await,
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
                Command::Write(WriteCmd::Hash(HashWrite::HSet(
                    "myhash".into(),
                    vec![
                        (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                        (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                    ]
                )))
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v1"))],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::Hash(HashWrite::HSet(
                    "h".into(),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v2"))],
                )))
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::Hash(HashRead::HGet(
                    "h".into(),
                    Bytes::from_static(b"f")
                )))
            )
            .await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::Hash(HashRead::HGet(
                    "h".into(),
                    Bytes::from_static(b"missing")
                )))
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "h".into(),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::Hash(HashWrite::HDel(
                    "h".into(),
                    vec![Bytes::from_static(b"f1"), Bytes::from_static(b"nope")]
                )))
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        let frame = exec(
            &h,
            Command::Read(ReadCmd::Hash(HashRead::HGetAll("h".into()))),
        )
        .await;
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
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "h".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        assert_eq!(
            exec(&h, Command::Read(ReadCmd::Hash(HashRead::HLen("h".into())))).await,
            Frame::Integer(1)
        );
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::Hash(HashRead::HExists(
                    "h".into(),
                    Bytes::from_static(b"f")
                )))
            )
            .await,
            Frame::Integer(1)
        );
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::Hash(HashRead::HExists(
                    "h".into(),
                    Bytes::from_static(b"nope")
                )))
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
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::Hash(HashWrite::HSet(
                    "k".into(),
                    vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                )))
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
                Command::Write(WriteCmd::List(ListWrite::LPush(
                    "k".into(),
                    vec![Bytes::from_static(b"a")]
                )))
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
            Command::Write(WriteCmd::List(ListWrite::LPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::List(ListRead::LRange("k".into(), 0, -1)))
            )
            .await,
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
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Read(ReadCmd::List(ListRead::LRange("k".into(), 0, -1)))
            )
            .await,
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
            Command::Write(WriteCmd::List(ListWrite::LPush(
                "k".into(),
                vec![Bytes::from_static(b"v")],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::List(ListWrite::LPop("k".into(), None)))
            )
            .await,
            Frame::Bulk(Bytes::from_static(b"v"))
        );
    }

    #[tokio::test]
    async fn rpop_with_count_returns_array() {
        let h = spawn().await;
        exec(
            &h,
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "k".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::List(ListWrite::RPop("k".into(), Some(2))))
            )
            .await,
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
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "k".into(),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            ))),
        )
        .await;
        assert_eq!(
            exec(&h, Command::Read(ReadCmd::List(ListRead::LLen("k".into())))).await,
            Frame::Integer(2)
        );
    }

    #[tokio::test]
    async fn lpush_on_hash_returns_wrongtype() {
        let h = spawn().await;
        exec(
            &h,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "k".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        assert_eq!(
            exec(
                &h,
                Command::Write(WriteCmd::List(ListWrite::LPush(
                    "k".into(),
                    vec![Bytes::from_static(b"v")]
                )))
            )
            .await,
            Frame::Error(WRONGTYPE.into())
        );
    }

    // --- AOF integration ---

    #[tokio::test]
    async fn write_commands_are_logged_to_aof() {
        use crate::persist::{AofConfig, FsyncPolicy};
        use std::io::Read as _;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let config = AofConfig {
            path: path.clone(),
            fsync: FsyncPolicy::Always,
            enabled: true,
        };
        let aof = Aof::open(&config).unwrap();
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            Some(aof),
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;

        // Read back the AOF file contents
        let mut contents = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[tokio::test]
    async fn wrongtype_errors_not_logged() {
        use crate::persist::{AofConfig, FsyncPolicy};
        use std::io::Read as _;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let config = AofConfig {
            path: path.clone(),
            fsync: FsyncPolicy::Always,
            enabled: true,
        };
        let aof = Aof::open(&config).unwrap();
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            Some(aof),
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        // SET a string key
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;

        // Try HSET on string key — should fail with WRONGTYPE
        let result = exec(
            &h,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "k".into(),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            ))),
        )
        .await;
        assert!(matches!(result, Frame::Error(_)));

        // AOF should only contain the SET, not the failed HSET
        let mut contents = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[tokio::test]
    async fn save_triggers_fsync() {
        use crate::persist::{AofConfig, FsyncPolicy};

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let config = AofConfig {
            path: path.clone(),
            fsync: FsyncPolicy::No, // No auto-fsync — only explicit SAVE
            enabled: true,
        };
        let aof = Aof::open(&config).unwrap();
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            Some(aof),
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;

        let save_result = h.save().await.unwrap();
        assert_eq!(save_result, Frame::Simple("OK".into()));
    }

    #[tokio::test]
    async fn save_without_aof_returns_error() {
        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        let result = h.save().await.unwrap();
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- Restart integration ---

    /// Simulate: populate store → SAVE → drop store → replay AOF → verify data.
    ///
    /// This is the core AOF durability guarantee: after a clean shutdown (or
    /// crash with fsync=always), replaying the AOF produces the same state.
    #[tokio::test]
    async fn aof_restart_preserves_all_data_types() {
        use crate::persist::{AofConfig, FsyncPolicy};

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let config = AofConfig {
            path: path.clone(),
            fsync: FsyncPolicy::Always,
            enabled: true,
        };

        // Phase 1: populate
        {
            let aof = Aof::open(&config).unwrap();
            let (h, store) = StoreHandle::new(
                Db::new(crate::eviction::EvictionPolicy::NoEviction),
                Some(aof),
                EvictionConfig::default(),
            );
            tokio::spawn(store.run());

            // String
            exec(
                &h,
                Command::Write(WriteCmd::String(StringWrite::Set(
                    "str_key".into(),
                    Bytes::from_static(b"str_val"),
                    None,
                ))),
            )
            .await;

            // Hash
            exec(
                &h,
                Command::Write(WriteCmd::Hash(HashWrite::HSet(
                    "hash_key".into(),
                    vec![
                        (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                        (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                    ],
                ))),
            )
            .await;

            // List
            exec(
                &h,
                Command::Write(WriteCmd::List(ListWrite::RPush(
                    "list_key".into(),
                    vec![
                        Bytes::from_static(b"a"),
                        Bytes::from_static(b"b"),
                        Bytes::from_static(b"c"),
                    ],
                ))),
            )
            .await;

            // SAVE to flush
            let r = h.save().await.unwrap();
            assert_eq!(r, Frame::Simple("OK".into()));
        }
        // Store is dropped here — simulating shutdown.

        // Phase 2: replay and verify
        let mut db = Aof::replay(&path).unwrap();

        // String
        assert_eq!(
            db.get_str("str_key").unwrap().unwrap(),
            &Bytes::from_static(b"str_val")
        );

        // Hash
        let hash = db.get_hash("hash_key").unwrap().unwrap();
        assert_eq!(hash.len(), 2);
        assert_eq!(
            hash.get(&Bytes::from_static(b"f1")),
            Some(&Bytes::from_static(b"v1"))
        );

        // List
        let list = db.get_list("list_key").unwrap().unwrap();
        assert_eq!(list.len(), 3);
    }

    /// Simulate crash: truncated AOF file still recovers prior commands.
    #[tokio::test]
    async fn aof_restart_with_truncated_tail() {
        use crate::persist::{AofConfig, FsyncPolicy};
        use std::io::Write as _;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let config = AofConfig {
            path: path.clone(),
            fsync: FsyncPolicy::Always,
            enabled: true,
        };

        // Populate
        {
            let aof = Aof::open(&config).unwrap();
            let (h, store) = StoreHandle::new(
                Db::new(crate::eviction::EvictionPolicy::NoEviction),
                Some(aof),
                EvictionConfig::default(),
            );
            tokio::spawn(store.run());

            exec(
                &h,
                Command::Write(WriteCmd::String(StringWrite::Set(
                    "good_key".into(),
                    Bytes::from_static(b"good_val"),
                    None,
                ))),
            )
            .await;

            h.save().await.unwrap();
        }

        // Simulate crash: append partial command
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"*3\r\n$3\r\nSET\r\n$5\r\ncra").unwrap();
        }

        // Replay — should recover good_key, ignore truncated tail
        let mut db = Aof::replay(&path).unwrap();
        assert_eq!(
            db.get_str("good_key").unwrap().unwrap(),
            &Bytes::from_static(b"good_val")
        );
        assert_eq!(db.get_str("cra"), Ok(None));
    }

    // --- BGSAVE integration ---

    /// BGSAVE: populate store → bgsave → wait → decode RDB → verify data.
    #[tokio::test]
    async fn bgsave_produces_valid_rdb() {
        let dir = tempfile::TempDir::new().unwrap();
        let rdb_path = dir.path().join("dump.rdb");

        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        // String
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "str_key".into(),
                Bytes::from_static(b"str_val"),
                None,
            ))),
        )
        .await;

        // Hash
        exec(
            &h,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "hash_key".into(),
                vec![
                    (Bytes::from_static(b"f1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"f2"), Bytes::from_static(b"v2")),
                ],
            ))),
        )
        .await;

        // List
        exec(
            &h,
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "list_key".into(),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            ))),
        )
        .await;

        // Trigger BGSAVE
        let result = h.bgsave(rdb_path.clone()).await.unwrap();
        assert_eq!(result, Frame::Simple("Background saving started".into()));

        // Wait for background task to finish writing
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Decode and verify
        let mut db = crate::persist::rdb::decode(&rdb_path).unwrap();
        assert_eq!(
            db.get_str("str_key").unwrap().unwrap(),
            &Bytes::from_static(b"str_val")
        );
        let hash = db.get_hash("hash_key").unwrap().unwrap();
        assert_eq!(hash.len(), 2);
        assert_eq!(
            hash.get(&Bytes::from_static(b"f1")),
            Some(&Bytes::from_static(b"v1"))
        );
        let list = db.get_list("list_key").unwrap().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(
            list.range(0, -1),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]
        );
    }

    #[tokio::test]
    async fn bgsave_snapshot_excludes_late_writes() {
        let dir = tempfile::TempDir::new().unwrap();
        let rdb_path = dir.path().join("dump.rdb");

        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "a".into(),
                Bytes::from_static(b"1"),
                None,
            ))),
        )
        .await;

        let result = h.bgsave(rdb_path.clone()).await.unwrap();
        assert_eq!(result, Frame::Simple("Background saving started".into()));

        // Happens after snapshot boundary; must not appear in the RDB.
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "b".into(),
                Bytes::from_static(b"2"),
                None,
            ))),
        )
        .await;

        wait_for_rdb(&rdb_path).await;

        let mut db = crate::persist::rdb::decode(&rdb_path).unwrap();
        assert_eq!(db.get_str("a").unwrap().unwrap(), &Bytes::from_static(b"1"));
        assert_eq!(db.get_str("b"), Ok(None));
    }

    #[tokio::test]
    async fn bgsave_returns_busy_while_running() {
        let dir = tempfile::TempDir::new().unwrap();
        let rdb_path = dir.path().join("dump.rdb");

        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        // Large value makes the background encode take long enough that a
        // second BGSAVE call deterministically overlaps.
        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "big".into(),
                Bytes::from(vec![b'x'; 20_000_000]),
                None,
            ))),
        )
        .await;

        let first = h.bgsave(rdb_path.clone()).await.unwrap();
        assert_eq!(first, Frame::Simple("Background saving started".into()));

        let second = h.bgsave(rdb_path.clone()).await.unwrap();
        assert_eq!(
            second,
            Frame::Error("ERR Background save already in progress".into())
        );

        wait_for_rdb(&rdb_path).await;
    }

    #[tokio::test]
    async fn bgsave_snapshot_isolated_from_hash_and_list_mutations() {
        let dir = tempfile::TempDir::new().unwrap();
        let rdb_path = dir.path().join("dump.rdb");

        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        exec(
            &h,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "hash".into(),
                vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))],
            ))),
        )
        .await;
        exec(
            &h,
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "list".into(),
                vec![Bytes::from_static(b"a")],
            ))),
        )
        .await;

        let result = h.bgsave(rdb_path.clone()).await.unwrap();
        assert_eq!(result, Frame::Simple("Background saving started".into()));

        // Mutations after snapshot start should not appear in the dump.
        exec(
            &h,
            Command::Write(WriteCmd::Hash(HashWrite::HSet(
                "hash".into(),
                vec![(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"))],
            ))),
        )
        .await;
        exec(
            &h,
            Command::Write(WriteCmd::List(ListWrite::RPush(
                "list".into(),
                vec![Bytes::from_static(b"b")],
            ))),
        )
        .await;

        wait_for_rdb(&rdb_path).await;

        let mut snapshot_db = crate::persist::rdb::decode(&rdb_path).unwrap();
        let hash = snapshot_db.get_hash("hash").unwrap().unwrap();
        assert_eq!(hash.len(), 1);
        assert_eq!(
            hash.get(&Bytes::from_static(b"f1")),
            Some(&Bytes::from_static(b"v1"))
        );
        assert_eq!(hash.get(&Bytes::from_static(b"f2")), None);

        let list = snapshot_db.get_list("list").unwrap().unwrap();
        assert_eq!(list.range(0, -1), vec![Bytes::from_static(b"a")]);
    }

    #[tokio::test]
    async fn bgsave_failure_resets_running_state() {
        let dir = tempfile::TempDir::new().unwrap();
        let bad_path = dir.path().join("missing").join("dump.rdb");
        let good_path = dir.path().join("dump.rdb");

        let (h, store) = StoreHandle::new(
            Db::new(crate::eviction::EvictionPolicy::NoEviction),
            None,
            EvictionConfig::default(),
        );
        tokio::spawn(store.run());

        exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;

        let first = h.bgsave(bad_path).await.unwrap();
        assert_eq!(first, Frame::Simple("Background saving started".into()));

        // Wait for the failed save to clear running state, then ensure a new
        // BGSAVE can start and complete.
        let mut started = false;
        for _ in 0..50 {
            let result = h.bgsave(good_path.clone()).await.unwrap();
            match result {
                Frame::Simple(ref msg) if msg == "Background saving started" => {
                    started = true;
                    break;
                }
                Frame::Error(ref msg) if msg == "ERR Background save already in progress" => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                other => panic!("unexpected BGSAVE response: {other:?}"),
            }
        }
        assert!(started, "expected BGSAVE to recover after failed run");
        wait_for_rdb(&good_path).await;
    }

    #[tokio::test]
    async fn aof_append_failure_returns_misconf_and_latches() {
        let aof = Aof::from_writer(
            Box::new(FailingWriter {
                fail_writes: true,
                fail_sync: false,
            }),
            FsyncPolicy::No,
        );
        let h = spawn_with_aof(aof).await;

        let first = exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;
        assert!(matches!(first, Frame::Error(ref msg) if msg.contains("MISCONF")));

        let second = exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "other".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;
        assert!(matches!(second, Frame::Error(ref msg) if msg.contains("MISCONF")));
    }

    #[tokio::test]
    async fn periodic_aof_fsync_failure_latches_misconf() {
        let aof = Aof::from_writer(
            Box::new(FailingWriter {
                fail_writes: false,
                fail_sync: true,
            }),
            FsyncPolicy::EverySecond,
        );
        let h = spawn_with_aof(aof).await;

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let result = exec(
            &h,
            Command::Write(WriteCmd::String(StringWrite::Set(
                "k".into(),
                Bytes::from_static(b"v"),
                None,
            ))),
        )
        .await;
        assert!(matches!(result, Frame::Error(ref msg) if msg.contains("MISCONF")));
    }
}
