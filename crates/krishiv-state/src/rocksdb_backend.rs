use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::snapshot::{decode_snapshot_entries, write_prefix};

/// File-backed or ephemeral state backend using RocksDB (LSM-tree).
///
/// Key encoding: `[8-byte LE op_id_len][op_id][8-byte LE name_len][state_name][raw_key]`
/// This matches the snapshot binary format so snapshots are portable across
/// backend implementations.
pub struct RocksDbStateBackend {
    db: DB,
    // Keep tempdir around for ephemeral instances — deletes on drop.
    _tempdir: Option<tempfile::TempDir>,
}

impl RocksDbStateBackend {
    /// Open or create a file-backed RocksDB database at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> StateResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path.as_ref()).map_err(db_err)?;
        Ok(Self { db, _tempdir: None })
    }

    /// Create an ephemeral database backed by a temp directory.
    pub fn in_memory() -> StateResult<Self> {
        let dir = tempfile::tempdir().map_err(db_err)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, dir.path()).map_err(db_err)?;
        Ok(Self {
            db,
            _tempdir: Some(dir),
        })
    }

    pub fn ephemeral() -> StateResult<Self> {
        if krishiv_common::requires_file_backed_state(krishiv_common::resolve_durability_profile())
        {
            return Err(StateError::BackendUnavailable {
                message: "ephemeral state backend is forbidden under durable profiles".into(),
                source: None,
            });
        }
        Self::in_memory()
    }

    /// Open state storage appropriate for the durability profile.
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
            Self::ephemeral()
        }
    }

    /// Ergonomic alias for `ephemeral()` — suitable for unit tests and embedded use.
    pub fn new() -> StateResult<Self> {
        Self::ephemeral()
    }

    /// Full key range owned by this backend (single-node always owns all key groups).
    pub fn key_group_range(&self) -> std::ops::RangeInclusive<u16> {
        0..=(crate::key_group::NUM_KEY_GROUPS - 1)
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

    pub async fn snapshot_async(&self) -> StateResult<Vec<u8>> {
        use crate::backend::StateBackend;
        self.snapshot()
    }

    pub async fn load_snapshot_async(&mut self, bytes: Vec<u8>) -> StateResult<()> {
        use crate::backend::StateBackend;
        self.load_snapshot(&bytes)
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
        let op_id = std::str::from_utf8(&k[pos..pos + op_len]).ok()?.to_owned();
        pos += op_len;
        let name_len = read_u64_le(k, &mut pos)? as usize;
        if pos + name_len > k.len() {
            return None;
        }
        let state_name = std::str::from_utf8(&k[pos..pos + name_len])
            .ok()?
            .to_owned();
        pos += name_len;
        let raw_key = k[pos..].to_vec();
        Some((Namespace::new(op_id, state_name), raw_key))
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
    let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(v)
}

impl StateBackend for RocksDbStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        let rk = Self::rocksdb_key(namespace, key);
        self.db.get(&rk).map_err(db_err)
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let rk = Self::rocksdb_key(namespace, &key);
        self.db.put(rk, value).map_err(db_err)
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        let rk = Self::rocksdb_key(namespace, key);
        self.db.delete(rk).map_err(db_err)
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
        self.db.write(batch).map_err(db_err)
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
        out[4..12].copy_from_slice(&count.to_le_bytes());
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
        self.db.write(batch).map_err(db_err)
    }

    fn put_batch(&mut self, entries: &[(&str, &str, &[u8], &[u8])]) -> StateResult<()> {
        let mut batch = WriteBatch::default();
        for (op_id, name, key, value) in entries {
            let ns = Namespace::new(*op_id, *name);
            let rk = Self::rocksdb_key(&ns, key);
            batch.put(rk, *value);
        }
        self.db.write(batch).map_err(db_err)
    }

    fn delete_batch(&mut self, entries: &[(&Namespace, &[u8])]) -> StateResult<()> {
        let mut batch = WriteBatch::default();
        for (ns, key) in entries {
            let rk = Self::rocksdb_key(ns, key);
            batch.delete(rk);
        }
        self.db.write(batch).map_err(db_err)
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
}
