use krishiv_async_util::unix_now_ms;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::snapshot::{decode_snapshot_entries, write_prefix};

/// Single redb table used by `RedbStateBackend`.
///
/// Composite key layout: `{u64_op_id_len}{op_id}{u64_name_len}{name}{raw_key}`
/// (all lengths are 8-byte little-endian).  This allows namespace prefix scans
/// using `range()` on the ordered B-tree.
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

/// Ephemeral or file-backed database for keyed state storage.
pub struct RedbStateBackend {
    db: Database,
}

impl RedbStateBackend {
    /// Open or create a file-backed redb database at `path`.
    ///
    /// The database is created if it does not yet exist.  Suitable for
    /// production use; state persists across executor restarts.
    ///
    /// P1-9: If the database file is corrupt (e.g. truncated after an OS crash),
    /// the corrupt file is renamed to `<path>.corrupt.<unix_ms>` and a fresh
    /// empty database is started so the process can continue rather than
    /// panicking on startup.
    pub fn open(path: impl AsRef<std::path::Path>) -> StateResult<Self> {
        let path = path.as_ref();
        match Database::create(path) {
            Ok(db) => {
                let this = Self { db };
                this.ensure_table()?;
                Ok(this)
            }
            Err(e) => {
                let ts = unix_now_ms();
                let corrupt_path = format!("{}.corrupt.{ts}", path.display());
                tracing::error!(
                    path = %path.display(),
                    corrupt_path = %corrupt_path,
                    error = %e,
                    "redb open failed; renaming corrupt file and starting fresh"
                );
                if let Err(rename_err) = std::fs::rename(path, &corrupt_path) {
                    tracing::warn!(error = %rename_err, "failed to rename corrupt redb file");
                }
                let db = Database::create(path).map_err(db_err)?;
                let this = Self { db };
                this.ensure_table()?;
                Ok(this)
            }
        }
    }

    /// Create an ephemeral in-memory redb database.
    ///
    /// Data is lost when the backend is dropped.  Suitable for tests and
    /// single-run jobs that do not require recovery.
    pub fn in_memory() -> StateResult<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(db_err)?;
        let this = Self { db };
        this.ensure_table()?;
        Ok(this)
    }

    /// Backwards-compatible alias for `in_memory()`.
    pub fn ephemeral() -> StateResult<Self> {
        Self::in_memory()
    }

    /// Ensure the state table exists (idempotent).
    fn ensure_table(&self) -> StateResult<()> {
        let wtxn = self.db.begin_write().map_err(db_err)?;
        wtxn.open_table(STATE_TABLE).map_err(db_err)?;
        wtxn.commit().map_err(db_err)?;
        Ok(())
    }

    /// Build the composite redb key for `(namespace, key)`.
    fn redb_key(namespace: &Namespace, key: &[u8]) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len() + key.len());
        write_prefix(&mut out, op, name);
        out.extend_from_slice(key);
        out
    }

    /// Build the namespace prefix (without the trailing raw key).
    fn redb_prefix(namespace: &Namespace) -> Vec<u8> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        let mut out = Vec::with_capacity(16 + op.len() + name.len());
        write_prefix(&mut out, op, name);
        out
    }

    /// Decode a stored redb key back to `(Namespace, raw_key)`.
    fn decode_redb_key(k: &[u8]) -> Option<(Namespace, Vec<u8>)> {
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

fn db_err(
    e: impl std::fmt::Display + std::fmt::Debug + Send + Sync + std::error::Error + 'static,
) -> StateError {
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

impl StateBackend for RedbStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        let rk = Self::redb_key(namespace, key);
        let rtxn = self.db.begin_read().map_err(db_err)?;
        let table = rtxn.open_table(STATE_TABLE).map_err(db_err)?;
        match table.get(rk.as_slice()).map_err(db_err)? {
            None => Ok(None),
            Some(v) => Ok(Some(v.value().to_vec())),
        }
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let rk = Self::redb_key(namespace, &key);
        let wtxn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = wtxn.open_table(STATE_TABLE).map_err(db_err)?;
            table
                .insert(rk.as_slice(), value.as_slice())
                .map_err(db_err)?;
        }
        wtxn.commit().map_err(db_err)?;
        Ok(())
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        let rk = Self::redb_key(namespace, key);
        let wtxn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = wtxn.open_table(STATE_TABLE).map_err(db_err)?;
            table.remove(rk.as_slice()).map_err(db_err)?;
        }
        wtxn.commit().map_err(db_err)?;
        Ok(())
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        let prefix = Self::redb_prefix(namespace);
        let wtxn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = wtxn.open_table(STATE_TABLE).map_err(db_err)?;
            let keys_to_delete: Vec<Vec<u8>> = table
                .range(prefix.as_slice()..)
                .map_err(db_err)?
                .map_while(|entry| {
                    let (k, _) = entry.ok()?;
                    let kb = k.value();
                    if kb.starts_with(prefix.as_slice()) {
                        Some(kb.to_vec())
                    } else {
                        None
                    }
                })
                .collect();
            for k in keys_to_delete {
                table.remove(k.as_slice()).map_err(db_err)?;
            }
        }
        wtxn.commit().map_err(db_err)?;
        Ok(())
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let rtxn = self.db.begin_read().map_err(db_err)?;
        let table = rtxn.open_table(STATE_TABLE).map_err(db_err)?;
        let mut seen = std::collections::BTreeSet::new();
        for entry in table.iter().map_err(db_err)? {
            let (k, _) = entry.map_err(db_err)?;
            if let Some((ns, _)) = Self::decode_redb_key(k.value()) {
                seen.insert(ns);
            }
        }
        Ok(seen.into_iter().collect())
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        let prefix = Self::redb_prefix(namespace);
        let rtxn = self.db.begin_read().map_err(db_err)?;
        let table = rtxn.open_table(STATE_TABLE).map_err(db_err)?;
        let mut keys = Vec::new();
        for entry in table.range(prefix.as_slice()..).map_err(db_err)? {
            let (k, _) = entry.map_err(db_err)?;
            let kb = k.value();
            if !kb.starts_with(prefix.as_slice()) {
                break;
            }
            let raw_key = kb[prefix.len()..].to_vec();
            keys.push(raw_key);
        }
        Ok(keys)
    }

    fn snapshot(&self) -> StateResult<Vec<u8>> {
        let rtxn = self.db.begin_read().map_err(db_err)?;
        let table = rtxn.open_table(STATE_TABLE).map_err(db_err)?;
        let count = table.len().map_err(db_err)?;

        let mut out = Vec::new();
        out.extend_from_slice(&1u32.to_le_bytes()); // version = 1
        out.extend_from_slice(&count.to_le_bytes());

        for entry in table.iter().map_err(db_err)? {
            let (k, v) = entry.map_err(db_err)?;
            if let Some((ns, raw_key)) = Self::decode_redb_key(k.value()) {
                let op_b = ns.operator_id().as_bytes();
                let name_b = ns.state_name().as_bytes();
                let val = v.value();
                out.extend_from_slice(&(op_b.len() as u64).to_le_bytes());
                out.extend_from_slice(op_b);
                out.extend_from_slice(&(name_b.len() as u64).to_le_bytes());
                out.extend_from_slice(name_b);
                out.extend_from_slice(&(raw_key.len() as u64).to_le_bytes());
                out.extend_from_slice(&raw_key);
                out.extend_from_slice(&(val.len() as u64).to_le_bytes());
                out.extend_from_slice(val);
            } else {
                tracing::warn!(
                    key_len = k.value().len(),
                    "snapshot: skipping entry with undecodable redb key"
                );
            }
        }
        Ok(out)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        let entries =
            decode_snapshot_entries(bytes).map_err(|e| StateError::SnapshotIncomplete {
                message: format!("failed to decode snapshot entries: {e}"),
            })?;

        let wtxn = self.db.begin_write().map_err(db_err)?;
        let result = (|| -> StateResult<()> {
            let mut table = wtxn.open_table(STATE_TABLE).map_err(db_err)?;

            let keys_to_delete: Vec<Vec<u8>> = table
                .iter()
                .map_err(db_err)?
                .map(|e| e.map(|(k, _)| k.value().to_vec()).map_err(db_err))
                .collect::<StateResult<Vec<_>>>()?;
            for k in keys_to_delete {
                table.remove(k.as_slice()).map_err(db_err)?;
            }

            for (op_id, state_name, raw_key, value) in entries {
                let ns = Namespace::new(&op_id, &state_name);
                let rk = Self::redb_key(&ns, &raw_key);
                table.insert(rk.as_slice(), value.as_slice()).map_err(|e| {
                    StateError::SnapshotIncomplete {
                        message: format!("mid-scan insert failed: {e}"),
                    }
                })?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                wtxn.commit().map_err(db_err)?;
                Ok(())
            }
            Err(e) => {
                if let Err(abort_err) = wtxn.abort() {
                    return Err(StateError::SnapshotIncomplete {
                        message: format!(
                            "load_snapshot failed (original: {e}, abort: {abort_err})"
                        ),
                    });
                }
                Err(e)
            }
        }
    }

    fn put_batch(&mut self, entries: &[(&str, &str, &[u8], &[u8])]) -> StateResult<()> {
        let write_txn = self.db.begin_write().map_err(db_err)?;
        {
            let mut table = write_txn.open_table(STATE_TABLE).map_err(db_err)?;
            for (op_id, name, key, value) in entries {
                let ns = Namespace::new(*op_id, *name);
                let rk = Self::redb_key(&ns, key);
                table.insert(rk.as_slice(), *value).map_err(db_err)?;
            }
        }
        write_txn.commit().map_err(db_err)?;
        Ok(())
    }

    fn get_batch(&self, keys: &[(&str, &str, &[u8])]) -> StateResult<Vec<Option<Vec<u8>>>> {
        let rtxn = self.db.begin_read().map_err(db_err)?;
        let table = rtxn.open_table(STATE_TABLE).map_err(db_err)?;
        let mut results = Vec::with_capacity(keys.len());
        for (op_id, name, key) in keys {
            let ns = Namespace::new(*op_id, *name);
            let rk = Self::redb_key(&ns, key);
            match table.get(rk.as_slice()).map_err(db_err)? {
                None => results.push(None),
                Some(v) => results.push(Some(v.value().to_vec())),
            }
        }
        Ok(results)
    }
}

impl RedbStateBackend {
    pub async fn snapshot_async(&self) -> StateResult<Vec<u8>> {
        tokio::task::block_in_place(|| StateBackend::snapshot(self))
    }

    pub async fn load_snapshot_async(&mut self, bytes: Vec<u8>) -> StateResult<()> {
        tokio::task::block_in_place(|| StateBackend::load_snapshot(self, &bytes))
    }
}

pub type RocksDbStateBackend = RedbStateBackend;
