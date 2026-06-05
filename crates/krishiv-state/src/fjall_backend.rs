use fjall::{Config, Database, Keyspace};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::snapshot::{decode_snapshot_entries, write_prefix};

/// Ephemeral or file-backed database for keyed state storage using Fjall (LSM-tree).
pub struct FjallStateBackend {
    _db: Database,
    keyspace: Keyspace,
    // Keep tempdir around if in_memory so it deletes on drop
    _tempdir: Option<tempfile::TempDir>,
}

impl FjallStateBackend {
    /// Open or create a file-backed fjall database at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> StateResult<Self> {
        let path = path.as_ref();
        let db = fjall::Database::open(Config::new(path)).map_err(db_err)?;
        let keyspace = db
            .keyspace("state", fjall::KeyspaceCreateOptions::default)
            .map_err(db_err)?;
        Ok(Self {
            _db: db,
            keyspace,
            _tempdir: None,
        })
    }

    /// Create an ephemeral in-memory fjall database using a temp directory.
    pub fn in_memory() -> StateResult<Self> {
        let tempdir = tempfile::tempdir().map_err(db_err)?;
        let db = fjall::Database::open(Config::new(tempdir.path())).map_err(db_err)?;
        let keyspace = db
            .keyspace("state", fjall::KeyspaceCreateOptions::default)
            .map_err(db_err)?;
        Ok(Self {
            _db: db,
            keyspace,
            _tempdir: Some(tempdir),
        })
    }

    pub fn ephemeral() -> StateResult<Self> {
        Self::in_memory()
    }

    /// Ergonomic alias for [`ephemeral`] — creates a temp-dir-backed instance
    /// suitable for unit tests and single-call embedded execution.
    pub fn new() -> StateResult<Self> {
        Self::ephemeral()
    }

    /// Return the full key-group range owned by this backend instance.
    ///
    /// A single-node backend always owns all key groups `0..=(NUM_KEY_GROUPS - 1)`.
    /// In a distributed rescale scenario, construct the backend with a narrower range
    /// and override this method.
    pub fn key_group_range(&self) -> std::ops::RangeInclusive<u16> {
        0..=(crate::key_group::NUM_KEY_GROUPS - 1)
    }

    /// Total number of keys stored across all namespaces. Useful for assertions
    /// in tests and diagnostics.
    pub fn key_count(&self) -> usize {
        self.keyspace.iter().count()
    }

    /// Async wrapper for [`StateBackend::snapshot`].
    ///
    /// Fjall's LSM-tree snapshot is an in-memory iteration and is fast enough
    /// to run directly. For very large state, callers can wrap in
    /// `spawn_blocking` themselves if needed.
    pub async fn snapshot_async(&self) -> crate::StateResult<Vec<u8>> {
        use crate::backend::StateBackend;
        self.snapshot()
    }

    /// Async wrapper for [`StateBackend::load_snapshot`].
    pub async fn load_snapshot_async(&mut self, bytes: Vec<u8>) -> crate::StateResult<()> {
        use crate::backend::StateBackend;
        self.load_snapshot(&bytes)
    }

    fn fjall_key(namespace: &Namespace, key: &[u8]) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len() + key.len());
        write_prefix(&mut out, op, name);
        out.extend_from_slice(key);
        out
    }

    fn fjall_prefix(namespace: &Namespace) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len());
        write_prefix(&mut out, op, name);
        out
    }

    fn decode_fjall_key(k: &[u8]) -> Option<(Namespace, Vec<u8>)> {
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

impl StateBackend for FjallStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        let fk = Self::fjall_key(namespace, key);
        match self.keyspace.get(fk).map_err(db_err)? {
            None => Ok(None),
            Some(v) => Ok(Some(v.to_vec())),
        }
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let fk = Self::fjall_key(namespace, &key);
        self.keyspace.insert(fk, value).map_err(db_err)?;
        Ok(())
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        let fk = Self::fjall_key(namespace, key);
        self.keyspace.remove(fk).map_err(db_err)?;
        Ok(())
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        let prefix = Self::fjall_prefix(namespace);
        let mut keys = Vec::new();
        for kv in self.keyspace.prefix(prefix) {
            let (k, _) = kv.into_inner().map_err(db_err)?;
            keys.push(k);
        }
        for k in keys {
            self.keyspace.remove(k).map_err(db_err)?;
        }
        Ok(())
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let mut seen = std::collections::HashSet::new();
        for kv in self.keyspace.iter() {
            let (k, _) = kv.into_inner().map_err(db_err)?;
            if let Some((ns, _)) = Self::decode_fjall_key(&k) {
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
        let prefix = Self::fjall_prefix(namespace);
        let mut keys = Vec::new();
        for kv in self.keyspace.prefix(prefix) {
            let (k, _) = kv.into_inner().map_err(db_err)?;
            if let Some((_, raw)) = Self::decode_fjall_key(&k) {
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

        for kv in self.keyspace.iter() {
            let (k, v) = kv.into_inner().map_err(db_err)?;
            if let Some((ns, raw_key)) = Self::decode_fjall_key(&k) {
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
        let all_keys: Vec<Vec<u8>> = self
            .keyspace
            .iter()
            .map(|g| g.into_inner().map(|(k, _)| k.to_vec()).map_err(db_err))
            .collect::<StateResult<_>>()?;

        let mut batch = self._db.batch();
        for k in all_keys {
            batch.remove(&self.keyspace, k);
        }
        for e in entries {
            let ns = Namespace::new(e.0, e.1);
            let fk = Self::fjall_key(&ns, &e.2);
            batch.insert(&self.keyspace, fk, e.3);
        }
        batch.commit().map_err(db_err)?;
        Ok(())
    }
}
