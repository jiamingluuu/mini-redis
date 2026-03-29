# TODO

## Mitigate BGSAVE latency spike from Db::clone()

### Background

Our BGSAVE implementation clones the entire `Db` synchronously inside the store
actor before spawning the background RDB encode task. While `Bytes` values are
cheap to clone (Arc-backed), the clone still copies every key (`String`) and
every container (`Hash`, `List`) upfront, creating a latency spike proportional
to the number of keys. During this clone, the store actor is blocked and cannot
process commands.

Production Redis avoids this by using `fork()`, which leverages OS-level
copy-on-write (COW) page tables — the child gets a snapshot instantly, and the
OS only copies pages the parent modifies afterward. The memory overhead is
proportional to writes during the snapshot, not total DB size.

### Possible solutions

1. **Incremental cloning** — Clone in batches (e.g., 1000 keys per tick),
   interleaved with normal command processing. Track which keys have been
   copied; freeze-on-read any uncovered key before allowing mutation.
   Simplest conceptually but adds complexity to the store actor loop.

2. **Persistent/immutable data structures** — Use structurally-shared
   collections (e.g., `im::HashMap`) where cloning is O(1) via reference
   counting on tree nodes. Mutations create new nodes along the path; old
   snapshots see the old tree. Trade-off: slightly slower per-operation due
   to tree overhead vs flat `HashMap`.

3. **Arc-wrapped values** — Use `HashMap<String, Arc<RedisObject>>`. Cloning
   the map copies keys + Arc pointers (no deep copy of values). Mutations
   use `Arc::make_mut` or replace the entry. Middle ground: cheaper than
   full clone, simpler than persistent data structures. Demonstrates the
   same COW principle Redis relies on, at the application level.

4. **Actual `fork()` on Unix** — Use `libc::fork()` and write the RDB in
   the child process (what Redis does). Must drop the tokio runtime in the
   child and use pure synchronous I/O — which `rdb::encode()` already does.
   Not portable to Windows, but neither is Redis.
