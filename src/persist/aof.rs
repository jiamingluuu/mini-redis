use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use bytes::BytesMut;

use super::{AofConfig, FsyncPolicy};
use crate::cmd::{Command, CommandHandler};
use crate::db::Db;
use crate::resp::parser;

/// Append-Only File writer.
///
/// REDIS: The AOF (appendonly.c) logs every mutation as the RESP command that
/// produced it. On recovery, the server simply replays the file through its
/// normal command pipeline — no special deserialization needed.
///
/// We use `std::fs::File` (synchronous) deliberately: AOF append is a small
/// sequential write inside the Store actor's single-threaded loop. Async I/O
/// would add task-switching overhead for no benefit here — the same design
/// choice Redis makes in appendonly.c.
pub(crate) struct Aof {
    writer: BufWriter<File>,
    fsync_policy: FsyncPolicy,
}

impl Aof {
    pub(crate) fn fsync_policy(&self) -> FsyncPolicy {
        self.fsync_policy
    }

    /// Open (or create) the AOF file in append mode.
    pub(crate) fn open(config: &AofConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            fsync_policy: config.fsync,
        })
    }

    /// Append raw RESP bytes to the AOF file.
    ///
    /// If the fsync policy is `Always`, the data is flushed and fsynced
    /// immediately. Otherwise it stays in the BufWriter until the next
    /// explicit `fsync()` call (or OS flush).
    pub(crate) fn append_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(bytes)?;
        if self.fsync_policy == FsyncPolicy::Always {
            self.fsync()?;
        }
        Ok(())
    }

    /// Flush the BufWriter and fsync the underlying file descriptor.
    ///
    /// REDIS: Redis calls `aof_fsync()` which maps to `fdatasync` on Linux.
    /// We use `sync_all()` which calls `fsync` — slightly more conservative
    /// (also syncs metadata) but correct and portable.
    pub(crate) fn fsync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Replay an AOF file to reconstruct the database state.
    ///
    /// REDIS: On startup, Redis opens the AOF and feeds commands through a
    /// "fake client" (aof.c `loadAppendOnlyFile`). We do the same: parse RESP
    /// frames, convert to Commands, filter to writes only, and execute.
    ///
    /// If the file ends with an incomplete frame (simulating a crash mid-write),
    /// we log a warning and return the database built so far — matching Redis's
    /// `aof-load-truncated yes` default behavior.
    pub(crate) fn replay(path: &Path) -> anyhow::Result<Db> {
        let data = std::fs::read(path)?;
        let mut buf = BytesMut::from(data.as_slice());
        let mut db = Db::new();

        loop {
            if buf.is_empty() {
                break;
            }

            match parser::parse(&mut buf) {
                Ok(Some(frame)) => {
                    // Only replay write commands — reads are never logged,
                    // but be defensive in case the file contains garbage.
                    match Command::from_frame(frame) {
                        Ok(Command::Write(w)) => {
                            w.execute(&mut db);
                        }
                        Ok(Command::Read(_)) => {
                            // Skip — should never appear in an AOF but harmless.
                        }
                        Err(e) => {
                            // REDIS: A parse error mid-file means corruption.
                            anyhow::bail!("AOF replay: invalid command: {e}");
                        }
                    }
                }
                Ok(None) => {
                    // Incomplete frame at end of file — truncated tail.
                    // REDIS: with aof-load-truncated=yes, Redis truncates the
                    // file and continues. We log a warning and return what we
                    // have.
                    eprintln!(
                        "AOF warning: truncated command at end of file ({} bytes remaining), \
                         ignoring tail",
                        buf.len()
                    );
                    break;
                }
                Err(e) => {
                    anyhow::bail!("AOF replay: parse error: {e}");
                }
            }
        }

        Ok(db)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::IntoResp;
    use crate::cmd::{HashWrite, ListWrite, StringWrite};
    use bytes::Bytes;
    use std::io::Read as _;
    use tempfile::NamedTempFile;

    fn test_config(path: &Path, policy: FsyncPolicy) -> AofConfig {
        AofConfig {
            path: path.to_path_buf(),
            fsync: policy,
            enabled: true,
        }
    }

    #[test]
    fn append_and_replay_round_trip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write commands to AOF
        {
            let config = test_config(&path, FsyncPolicy::Always);
            let mut aof = Aof::open(&config).unwrap();

            let set_cmd = StringWrite::Set("foo".into(), Bytes::from_static(b"bar"));
            aof.append_bytes(&set_cmd.to_resp_bytes()).unwrap();

            let hset_cmd = HashWrite::HSet(
                "myhash".into(),
                vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))],
            );
            aof.append_bytes(&hset_cmd.to_resp_bytes()).unwrap();

            let lpush_cmd = ListWrite::LPush(
                "mylist".into(),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            );
            aof.append_bytes(&lpush_cmd.to_resp_bytes()).unwrap();
        }

        // Replay and verify
        let db = Aof::replay(&path).unwrap();
        assert_eq!(
            db.get_str("foo").unwrap().unwrap(),
            &Bytes::from_static(b"bar")
        );
        assert_eq!(db.get_hash("myhash").unwrap().unwrap().len(), 1);
        assert_eq!(db.get_list("mylist").unwrap().unwrap().len(), 2);
    }

    #[test]
    fn replay_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let db = Aof::replay(tmp.path()).unwrap();
        // Empty file → empty Db
        assert_eq!(db.get_str("anything"), Ok(None));
    }

    #[test]
    fn replay_truncated_tail() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write a complete command followed by an incomplete one
        {
            let config = test_config(&path, FsyncPolicy::Always);
            let mut aof = Aof::open(&config).unwrap();

            let set_cmd = StringWrite::Set("key".into(), Bytes::from_static(b"val"));
            aof.append_bytes(&set_cmd.to_resp_bytes()).unwrap();
            aof.fsync().unwrap();
        }

        // Append truncated bytes directly
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            // Partial RESP: array header but no body
            file.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nk").unwrap();
            file.sync_all().unwrap();
        }

        // Replay should recover the first command and warn about the tail
        let db = Aof::replay(&path).unwrap();
        assert_eq!(
            db.get_str("key").unwrap().unwrap(),
            &Bytes::from_static(b"val")
        );
        // The truncated SET for "k" should NOT be present
        assert_eq!(db.get_str("k"), Ok(None));
    }

    #[test]
    fn replay_corrupted_middle() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write a valid command, then corrupt bytes, then another valid command
        {
            let config = test_config(&path, FsyncPolicy::Always);
            let mut aof = Aof::open(&config).unwrap();

            let set_cmd = StringWrite::Set("ok".into(), Bytes::from_static(b"fine"));
            aof.append_bytes(&set_cmd.to_resp_bytes()).unwrap();
            // Invalid RESP type byte — not +, -, :, $, or *
            aof.append_bytes(b"!garbage\r\n").unwrap();
            aof.fsync().unwrap();
        }

        let result = Aof::replay(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("parse error"));
    }

    #[test]
    fn append_respects_resp_format() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let config = test_config(&path, FsyncPolicy::Always);
            let mut aof = Aof::open(&config).unwrap();
            let cmd = StringWrite::Set("k".into(), Bytes::from_static(b"v"));
            aof.append_bytes(&cmd.to_resp_bytes()).unwrap();
        }

        let mut contents = Vec::new();
        File::open(&path)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    }

    #[test]
    fn fsync_policy_no_does_not_auto_sync() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let config = test_config(&path, FsyncPolicy::No);
        let mut aof = Aof::open(&config).unwrap();
        let cmd = StringWrite::Set("k".into(), Bytes::from_static(b"v"));
        // Should succeed without error even with No policy
        aof.append_bytes(&cmd.to_resp_bytes()).unwrap();
        // Explicit fsync should also work
        aof.fsync().unwrap();
    }
}
