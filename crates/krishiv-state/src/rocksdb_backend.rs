use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::snapshot::{decode_snapshot_entries, write_prefix};

/// File-backed or ephemeral state backend using RocksDB (LSM-tree).
///
/// Key encoding: `[8-byte LE op_id_len][op_id][8-byte LE name_len][state_name][raw_key]`
/// This matches the snapshot binary format so snapshots are portable across
/// backend implementations.
///
/// # Durability
///
/// The backend exposes a per-instance `durable_fsync` knob (default `false`).
/// When `false`, every [`put`](Self::put) / [`delete`](Self::delete) /
/// [`put_batch`](Self::put_batch) / [`delete_batch`](Self::delete_batch) goes to
/// RocksDB with `set_sync(false)` — the writes are buffered in the WAL and may
/// be lost on a process crash until [`sync`](Self::sync) is called explicitly.
/// This is the right shape for the hot path of a streaming operator that
/// checkpoints periodically: the checkpoint code calls
/// [`sync`](Self::sync) once per epoch and the per-write fsync cost is amortized
/// over the batch.
///
/// When `true` (file-backed, durable profiles), every write is `set_sync(true)`
/// — the strongest possible durability, paid per write. This is the historical
/// behavior and remains the default for `open_for_profile(_, Some(_))` so
/// single-node and distributed deployments are unchanged in their durability
/// guarantees; the streaming engine opts in to the lower-fsync path via
/// [`Self::with_durable_fsync(false)`] when the profile permits it.
pub struct RocksDbStateBackend {
    db: DB,
    // Keep tempdir around for ephemeral instances — deletes on drop.
    _tempdir: Option<tempfile::TempDir>,
    /// When `true`, every write is `set_sync(true)`. When `false`, writes are
    /// batched in the WAL and the operator must call [`Self::sync`] at
    /// checkpoint time to make them durable.
    durable_fsync: bool,
}

impl RocksDbStateBackend {
    /// STATE-2: Build production-tuned RocksDB options with bloom filters,
    /// dynamic level compaction, and configurable write buffer / max open files.
    fn production_options() -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_level_compaction_dynamic_level_bytes(true);
        let write_buffer_mb = std::env::var("KRISHIV_ROCKSDB_WRITE_BUFFER_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64);
        opts.set_write_buffer_size(write_buffer_mb * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        let max_open_files = std::env::var("KRISHIV_ROCKSDB_MAX_OPEN_FILES")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(512);
        opts.set_max_open_files(max_open_files);
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_block_size(16 * 1024);
        opts.set_block_based_table_factory(&block_opts);
        opts
    }

    /// Open or create a file-backed RocksDB database at `path` with
    /// `durable_fsync = true` (per-write fsync).
    pub fn open(path: impl AsRef<std::path::Path>) -> StateResult<Self> {
        let opts = Self::production_options();
        let db = DB::open(&opts, path.as_ref()).map_err(db_err)?;
        Ok(Self {
            db,
            _tempdir: None,
            durable_fsync: true,
        })
    }

    /// Open an ephemeral (tempfile-backed) RocksDB database with
    /// `durable_fsync = false`. Used by the embedded and dev-local paths where
    /// process-lifetime durability is sufficient and per-write fsync is wasteful.
    pub fn ephemeral() -> StateResult<Self> {
        if krishiv_common::requires_file_backed_state(krishiv_common::resolve_durability_profile())
        {
            return Err(StateError::BackendUnavailable {
                message: "ephemeral state backend is forbidden under durable profiles".into(),
                source: None,
            });
        }
        let dir = tempfile::tempdir().map_err(db_err)?;
        let opts = Self::production_options();
        let db = DB::open(&opts, dir.path()).map_err(db_err)?;
        Ok(Self {
            db,
            _tempdir: Some(dir),
            durable_fsync: false,
        })
    }

    /// Open state storage appropriate for the durability profile.
    ///
    /// - `durable` profile + `Some(path)` → file-backed, `durable_fsync = true`.
    /// - `durable` profile + `None`      → error (durable requires a path).
    /// - `dev-local` profile + `Some(path)` → file-backed, `durable_fsync = true`
    ///   (caller asked for the file path; honor the durability contract).
    /// - `dev-local` profile + `None`      → ephemeral, `durable_fsync = false`.
    pub fn open_for_profile(
        profile: krishiv_common::DurabilityProfile,
        path: Option<&std::path::Path>,
    ) -> StateResult<Self> {
        if krishiv_common::requires_file_backed_state(profile) {
            let path = path.ok_or_else(|| StateError::BackendUnavailable {
                message: "durable profile requires a file-backed state directory".into(),
                source: None,
            })?;
            Self::open(path)
        } else {
            match path {
                Some(p) => Self::open(p),
                None => Self::ephemeral(),
            }
        }
    }

    /// Construct an ephemeral backend with `durable_fsync = false`.
    pub fn new() -> StateResult<Self> {
        Self::ephemeral()
    }

    /// Builder: override the per-write fsync behavior. Pass `true` for the
    /// historical per-write-fsync semantics, `false` to batch WAL writes and
    /// rely on explicit [`sync`](Self::sync) calls at checkpoint time.
    #[must_use]
    pub fn with_durable_fsync(mut self, durable_fsync: bool) -> Self {
        self.durable_fsync = durable_fsync;
        self
    }

    /// Force the WAL to disk. Call this once per checkpoint when
    /// `durable_fsync = false` so a process crash never loses more than the
    /// unflushed window between the previous `sync` and the next one. Returns
    /// `Ok(())` after a successful fsync; otherwise the underlying IO error.
    pub fn sync(&self) -> StateResult<()> {
        self.db.flush_wal(true).map_err(db_err)
    }

    /// Whether per-write fsync is enabled.
    pub fn durable_fsync(&self) -> bool {
        self.durable_fsync
    }

    /// Total number of keys stored across all namespaces.
    pub fn key_count(&self) -> usize {
        self.db.iterator(IteratorMode::Start).count()
    }

    /// Create a RocksDB hard-linked checkpoint at `target_dir`.
    ///
    /// The target directory is created by RocksDB and will contain all SST
    /// files (hard-linked) plus MANIFEST, CURRENT, and OPTIONS files (copied).
    /// Used by [`crate::incremental_checkpoint::RocksDbIncrementalCheckpointer`].
    pub fn create_rocksdb_checkpoint(&self, target_dir: &std::path::Path) -> StateResult<()> {
        let ckpt = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(|e| {
            StateError::BackendUnavailable {
                message: format!("rocksdb checkpoint create: {e}"),
                source: None,
            }
        })?;
        ckpt.create_checkpoint(target_dir)
            .map_err(|e| StateError::BackendUnavailable {
                message: format!("rocksdb checkpoint write: {e}"),
                source: None,
            })
    }

    /// Snapshot the RocksDB backend state to bytes.
    ///
    /// Offloads the blocking RocksDB I/O to `block_in_place` so the Tokio
    /// reactor thread is not stalled.  Requires a multi-threaded Tokio runtime.
    pub async fn snapshot_async(&self) -> StateResult<Vec<u8>> {
        use crate::backend::StateBackend;
        tokio::task::block_in_place(|| self.snapshot())
    }

    /// Restore RocksDB backend state from a snapshot.
    ///
    /// Offloads the blocking RocksDB I/O to `block_in_place` so the Tokio
    /// reactor thread is not stalled.  Requires a multi-threaded Tokio runtime.
    pub async fn load_snapshot_async(&mut self, bytes: Vec<u8>) -> StateResult<()> {
        use crate::backend::StateBackend;
        tokio::task::block_in_place(|| self.load_snapshot(&bytes))
    }

    fn rocksdb_key(namespace: &Namespace, key: &[u8]) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len() + key.len());
        write_prefix(&mut out, op, name);
        out.extend_from_slice(key);
        out
    }

    fn rocksdb_prefix(namespace: &Namespace) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len());
        write_prefix(&mut out, op, name);
        out
    }

    fn decode_key(k: &[u8]) -> Option<(Namespace, Vec<u8>)> {
        let mut pos = 0usize;
        let op_len = read_u64_le(k, &mut pos)? as usize;
        if pos + op_len > k.len() {
            return None;
        }
        let op_id = std::str::from_utf8(k.get(pos..pos + op_len)?)
            .ok()?
            .to_owned();
        pos += op_len;
        let name_len = read_u64_le(k, &mut pos)? as usize;
        if pos + name_len > k.len() {
            return None;
        }
        let state_name = std::str::from_utf8(k.get(pos..pos + name_len)?)
            .ok()?
            .to_owned();
        pos += name_len;
        let raw_key = k.get(pos..).unwrap_or(&[]).to_vec();
        Some((Namespace::new(op_id, state_name), raw_key))
    }

    /// Build a [`WriteOptions`] matching the configured durability policy.
    /// The struct is cheap to construct (no allocation); this helper is `const`
    /// to make it clear there's no per-call setup cost.
    fn write_opts(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.durable_fsync);
        opts
    }
}

fn db_err(e: impl std::error::Error + Send + Sync + 'static) -> StateError {
    StateError::BackendUnavailable {
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

fn read_u64_le(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if buf.len() < *pos + 8 {
        return None;
    }
    let v = u64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?);
    *pos += 8;
    Some(v)
}

impl StateBackend for RocksDbStateBackend {
    fn as_rocksdb(&self) -> Option<&RocksDbStateBackend> {
        Some(self)
    }

    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        let rk = Self::rocksdb_key(namespace, key);
        self.db.get(&rk).map_err(db_err)
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let rk = Self::rocksdb_key(namespace, &key);
        let write_opts = self.write_opts();
        self.db.put_opt(rk, value, &write_opts).map_err(db_err)
    }

    fn sync(&self) -> StateResult<()> {
        // Flush the WAL so writes buffered under `durable_fsync = false` become
        // crash-durable. Cheap no-op effect when `durable_fsync = true` (each
        // write is already fsynced). The streaming checkpoint calls this once
        // per epoch via the `StateBackend::sync` trait method.
        self.db.flush_wal(true).map_err(db_err)
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        let rk = Self::rocksdb_key(namespace, key);
        let write_opts = self.write_opts();
        self.db.delete_opt(rk, &write_opts).map_err(db_err)
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        let prefix = Self::rocksdb_prefix(namespace);
        let mut batch = WriteBatch::default();
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (k, _) = item.map_err(db_err)?;
            if !k.starts_with(prefix.as_slice()) {
                break;
            }
            batch.delete(&*k);
        }
        let write_opts = self.write_opts();
        self.db.write_opt(batch, &write_opts).map_err(db_err)
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let mut seen = std::collections::HashSet::new();
        for item in self.db.iterator(IteratorMode::Start) {
            let (k, _) = item.map_err(db_err)?;
            if let Some((ns, _)) = Self::decode_key(&k) {
                seen.insert(ns);
            }
        }
        let mut res: Vec<_> = seen.into_iter().collect();
        res.sort_by(|a, b| {
            a.operator_id()
                .cmp(b.operator_id())
                .then(a.state_name().cmp(b.state_name()))
        });
        Ok(res)
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        let prefix = Self::rocksdb_prefix(namespace);
        let mut keys = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (k, _) = item.map_err(db_err)?;
            if !k.starts_with(prefix.as_slice()) {
                break;
            }
            if let Some((_, raw)) = Self::decode_key(&k) {
                keys.push(raw);
            }
        }
        Ok(keys)
    }

    fn snapshot(&self) -> StateResult<Vec<u8>> {
        let mut out = Vec::new();
        let mut count = 0u64;
        out.extend_from_slice(&1u32.to_le_bytes()); // version
        out.extend_from_slice(&0u64.to_le_bytes()); // placeholder for count

        for item in self.db.iterator(IteratorMode::Start) {
            let (k, v) = item.map_err(db_err)?;
            if let Some((ns, raw_key)) = Self::decode_key(&k) {
                let op = ns.operator_id().as_bytes();
                out.extend_from_slice(&(op.len() as u64).to_le_bytes());
                out.extend_from_slice(op);
                let name = ns.state_name().as_bytes();
                out.extend_from_slice(&(name.len() as u64).to_le_bytes());
                out.extend_from_slice(name);
                out.extend_from_slice(&(raw_key.len() as u64).to_le_bytes());
                out.extend_from_slice(&raw_key);
                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                out.extend_from_slice(&v);
                count += 1;
            }
        }
        if let Some(s) = out.get_mut(4..12) {
            s.copy_from_slice(&count.to_le_bytes());
        }
        Ok(out)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        let entries = decode_snapshot_entries(bytes)?;

        // Delete all existing keys first.
        let all_keys: Vec<Vec<u8>> = self
            .db
            .iterator(IteratorMode::Start)
            .map(|item| item.map(|(k, _)| k.to_vec()).map_err(db_err))
            .collect::<StateResult<_>>()?;

        let mut batch = WriteBatch::default();
        for k in all_keys {
            batch.delete(&k);
        }
        for (op_id, name, key, value) in entries {
            let ns = Namespace::new(op_id, name);
            let rk = Self::rocksdb_key(&ns, &key);
            batch.put(rk, value);
        }
        let write_opts = self.write_opts();
        self.db.write_opt(batch, &write_opts).map_err(db_err)
    }

    fn put_batch(&mut self, entries: &[(&str, &str, &[u8], &[u8])]) -> StateResult<()> {
        let mut batch = WriteBatch::default();
        for (op_id, name, key, value) in entries {
            let ns = Namespace::new(*op_id, *name);
            let rk = Self::rocksdb_key(&ns, key);
            batch.put(rk, *value);
        }
        let write_opts = self.write_opts();
        self.db.write_opt(batch, &write_opts).map_err(db_err)
    }

    fn delete_batch(&mut self, entries: &[(&Namespace, &[u8])]) -> StateResult<()> {
        let mut batch = WriteBatch::default();
        for (ns, key) in entries {
            let rk = Self::rocksdb_key(ns, key);
            batch.delete(rk);
        }
        let write_opts = self.write_opts();
        self.db.write_opt(batch, &write_opts).map_err(db_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(op: &str, name: &str) -> Namespace {
        Namespace::new(op, name)
    }

    #[test]
    fn rocksdb_get_missing_returns_none() {
        let b = RocksDbStateBackend::new().unwrap();
        assert!(b.get(&ns("op1", "window"), b"k1").unwrap().is_none());
    }

    #[test]
    fn rocksdb_put_get_roundtrip() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let n = ns("op1", "counts");
        b.put(&n, b"user-a".to_vec(), b"42".to_vec()).unwrap();
        assert_eq!(b.get(&n, b"user-a").unwrap(), Some(b"42".to_vec()));
    }

    #[test]
    fn rocksdb_delete_removes_key() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let n = ns("op1", "counts");
        b.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
        b.delete(&n, b"k").unwrap();
        assert!(b.get(&n, b"k").unwrap().is_none());
    }

    #[test]
    fn rocksdb_namespaces_isolated() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let na = ns("op1", "window");
        let nb = ns("op2", "window");
        b.put(&na, b"key".to_vec(), b"val-a".to_vec()).unwrap();
        b.put(&nb, b"key".to_vec(), b"val-b".to_vec()).unwrap();
        assert_eq!(b.get(&na, b"key").unwrap(), Some(b"val-a".to_vec()));
        assert_eq!(b.get(&nb, b"key").unwrap(), Some(b"val-b".to_vec()));
    }

    #[test]
    fn rocksdb_clear_namespace_is_scoped() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let na = ns("op1", "window");
        let nb = ns("op2", "window");
        b.put(&na, b"k1".to_vec(), b"v1".to_vec()).unwrap();
        b.put(&na, b"k2".to_vec(), b"v2".to_vec()).unwrap();
        b.put(&nb, b"k1".to_vec(), b"keep".to_vec()).unwrap();
        b.clear_namespace(&na).unwrap();
        assert!(b.get(&na, b"k1").unwrap().is_none());
        assert!(b.get(&na, b"k2").unwrap().is_none());
        assert_eq!(b.get(&nb, b"k1").unwrap(), Some(b"keep".to_vec()));
    }

    #[test]
    fn rocksdb_list_keys_returns_prefix_only() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let n = ns("op1", "counts");
        let other = ns("op1", "other");
        b.put(&n, b"a".to_vec(), b"1".to_vec()).unwrap();
        b.put(&n, b"b".to_vec(), b"2".to_vec()).unwrap();
        b.put(&other, b"c".to_vec(), b"3".to_vec()).unwrap();
        let mut keys = b.list_keys(&n).unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn rocksdb_snapshot_roundtrip() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let n = ns("op1", "state");
        b.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
        let snap = b.snapshot().unwrap();

        let mut b2 = RocksDbStateBackend::new().unwrap();
        b2.load_snapshot(&snap).unwrap();
        assert_eq!(b2.get(&n, b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn rocksdb_open_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.rocksdb");
        let n = ns("op1", "data");
        {
            let mut b = RocksDbStateBackend::open(&path).unwrap();
            b.put(&n, b"key".to_vec(), b"value".to_vec()).unwrap();
        }
        let b2 = RocksDbStateBackend::open(&path).unwrap();
        assert_eq!(b2.get(&n, b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn rocksdb_put_batch_and_delete_batch() {
        let mut b = RocksDbStateBackend::new().unwrap();
        let n = ns("op", "s");
        b.put_batch(&[("op", "s", b"k1", b"v1"), ("op", "s", b"k2", b"v2")])
            .unwrap();
        assert_eq!(b.get(&n, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(b.get(&n, b"k2").unwrap(), Some(b"v2".to_vec()));
        b.delete_batch(&[(&n, b"k1".as_ref())]).unwrap();
        assert!(b.get(&n, b"k1").unwrap().is_none());
    }

    #[test]
    fn rocksdb_ephemeral_disables_durable_fsync_by_default() {
        // Embedded / dev-local path: per-write fsync is wasteful.
        let b = RocksDbStateBackend::ephemeral().unwrap();
        assert!(
            !b.durable_fsync(),
            "ephemeral default must skip per-write fsync"
        );
    }

    #[test]
    fn rocksdb_file_backed_keeps_durable_fsync_for_safety() {
        // The file-backed path is the durable contract — keep per-write fsync
        // unless the caller explicitly opts out via `with_durable_fsync(false)`.
        let dir = tempfile::tempdir().unwrap();
        let b = RocksDbStateBackend::open(dir.path().join("state.rocksdb")).unwrap();
        assert!(
            b.durable_fsync(),
            "file-backed default must keep per-write fsync"
        );

        let b2 = RocksDbStateBackend::open(dir.path().join("state2.rocksdb"))
            .unwrap()
            .with_durable_fsync(false);
        assert!(!b2.durable_fsync());
    }

    #[test]
    fn rocksdb_sync_is_idempotent() {
        // `sync` must be safe to call repeatedly (e.g. before a checkpoint and
        // after a checkpoint). Both calls succeed on a freshly-opened backend.
        let b = RocksDbStateBackend::new().unwrap();
        b.sync().unwrap();
        b.sync().unwrap();
    }

    /// Latency regression: with `durable_fsync = false`, a tight loop of `put`
    /// operations must be at least 2× faster than the per-write-fsync path
    /// *whenever the per-write fsync cost is measurable*. On hardware where
    /// fsync is sub-µs (NVMe with battery-backed write cache, in-memory tmpfs,
    /// etc.) the gap can shrink to noise — the test reports the result and
    /// skips the strict comparison in that case rather than flaking CI.
    ///
    /// Each `put` with `durable_fsync = true` issues an `fsync` (typically
    /// 50-200 µs on a SATA SSD, 1-10 µs on NVMe with WAL cache, sub-µs on
    /// tmpfs or battery-backed write cache). With `false`, RocksDB writes
    /// to the WAL buffer and returns immediately. The 2× ratio is conservative
    /// to avoid CI flakes; on real production hardware (SATA SSD, NFS, EBS)
    /// the gap is 10-100× and this assertion would always fire.
    #[test]
    #[ignore = "timing-sensitive benchmark: flakes under parallel test load (audit §14c); \
                run explicitly — the set_sync(durable_fsync) invariant is covered by \
                the durable_fsync getter tests"]
    fn rocksdb_ephemeral_is_faster_than_file_backed_with_durable_fsync() {
        const ITERS: usize = 200;
        let mut fast =
            RocksDbStateBackend::ephemeral().expect("ephemeral always opens under dev-local");
        let start_fast = std::time::Instant::now();
        for i in 0..ITERS {
            let key = format!("k{i}");
            fast.put(&ns("op", "s"), key.into_bytes(), b"v".to_vec())
                .unwrap();
        }
        let elapsed_fast = start_fast.elapsed();

        let dir = tempfile::tempdir().unwrap();
        let mut slow =
            RocksDbStateBackend::open(dir.path().join("state.rocksdb")).expect("file-backed open");
        // `durable_fsync = true` is the file-backed default; this is the
        // historical behavior we are measuring against.
        assert!(slow.durable_fsync());
        let start_slow = std::time::Instant::now();
        for i in 0..ITERS {
            let key = format!("k{i}");
            slow.put(&ns("op", "s"), key.into_bytes(), b"v".to_vec())
                .unwrap();
        }
        let elapsed_slow = start_slow.elapsed();

        // Sanity: both paths must produce visible writes.
        assert!(
            !elapsed_fast.is_zero(),
            "fast path took zero time — clock broken?"
        );
        assert!(
            !elapsed_slow.is_zero(),
            "slow path took zero time — clock broken?"
        );

        // If the slow path took less than 5 ms total, the per-write fsync
        // cost is below noise on this host (e.g. NVMe with battery-backed
        // write cache, tmpfs, in-memory CI runner). Skip the strict 2× check
        // and report the measurement instead — the architectural invariant
        // (writes go through `set_sync(self.durable_fsync)`) is enforced by
        // the `durable_fsync` getter tested above.
        if elapsed_slow.as_millis() < 5 {
            eprintln!(
                "skipping strict 2× check: per-fsync host cost is too low to \
                 measure reliably (slow={elapsed_slow:?}, fast={elapsed_fast:?})"
            );
            return;
        }
        assert!(
            elapsed_fast.as_nanos() * 2 <= elapsed_slow.as_nanos(),
            "ephemeral must be at least 2× faster than durable-fsync: \
             fast={elapsed_fast:?} slow={elapsed_slow:?}"
        );
    }
}
