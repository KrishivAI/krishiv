use std::collections::HashMap;

use crate::error::StateResult;
use crate::namespace::Namespace;

/// Keyed state backend contract for streaming operators.
///
/// All methods are synchronous so the caller controls async dispatch
/// (e.g. `spawn_blocking` for the redb backend in R5.2).
pub trait StateBackend: Send + Sync {
    /// Return the value stored for `key` in `namespace`, or `None` if absent.
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>>;
    /// Store `value` under `key` in `namespace`.
    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()>;
    /// Remove `key` from `namespace`.  No-op if absent.
    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()>;
    /// Remove all keys in `namespace`.
    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()>;
    /// List all namespaces present in this backend (R5.2 inspection API).
    fn list_namespaces(&self) -> StateResult<Vec<Namespace>>;
    /// List all keys stored in `namespace` (R5.2 inspection API).
    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>>;

    /// Serialize all state to a portable byte snapshot.
    ///
    /// Format: `[4-byte LE version=1][8-byte LE entry_count][entries...]`
    /// where each entry is: `[8-byte LE op_id_len][op_id][8-byte LE name_len][name][8-byte LE key_len][key][8-byte LE val_len][val]`
    fn snapshot(&self) -> StateResult<Vec<u8>>;

    /// Replace current state with the contents of a snapshot produced by `snapshot()`.
    ///
    /// On failure the backend aborts the write transaction, preserving its prior
    /// data (not leaving the backend empty).
    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()>;

    /// Store multiple `(namespace_op_id, namespace_state_name, key, value)` entries.
    ///
    /// The default implementation calls `put` for each entry individually.
    /// Backends that support batch writes should override this for efficiency —
    /// `RocksDbStateBackend` overrides this to open a single write transaction for all entries.
    fn put_batch(&mut self, entries: &[(&str, &str, &[u8], &[u8])]) -> StateResult<()> {
        for (op_id, name, key, value) in entries {
            let ns = Namespace::new(*op_id, *name);
            self.put(&ns, key.to_vec(), value.to_vec())?;
        }
        Ok(())
    }

    /// Delete multiple `(namespace, key)` entries at once.
    ///
    /// The default implementation calls `delete` for each entry individually.
    /// Backends that support batch deletes should override this for efficiency —
    /// `RocksDbStateBackend` overrides this to open a single write transaction for all entries.
    fn delete_batch(&mut self, entries: &[(&Namespace, &[u8])]) -> StateResult<()> {
        for (ns, key) in entries {
            self.delete(ns, key)?;
        }
        Ok(())
    }

    /// Retrieve multiple values for `(namespace_op_id, namespace_state_name, key)` triples.
    ///
    /// The default implementation calls `get` for each entry individually.
    /// Backends that support batch reads should override this for efficiency —
    /// `RocksDbStateBackend` overrides this to open a single read transaction for all keys.
    fn get_batch(&self, keys: &[(&str, &str, &[u8])]) -> StateResult<Vec<Option<Vec<u8>>>> {
        keys.iter()
            .map(|(op_id, name, key)| {
                let ns = Namespace::new(*op_id, *name);
                self.get(&ns, key)
            })
            .collect()
    }

    /// Remove all entries whose TTL has expired (GAP-15).
    ///
    /// The default implementation is a no-op — non-TTL backends do not expire
    /// entries.  `TtlStateBackend` overrides this to perform an eager scan-and-
    /// delete pass, preventing unbounded memory growth from entries that were
    /// written but never read again after they expired (lazy-delete only removes
    /// entries on reads, so cold keys accumulate otherwise).
    ///
    /// Returns the number of entries evicted.
    fn purge_expired(&mut self) -> StateResult<usize> {
        Ok(0)
    }

    /// Inform the backend of the current event-time watermark in milliseconds.
    ///
    /// When called on a [`TtlStateBackend`], subsequent `purge_expired` and
    /// read-time expiry checks will use `watermark_ms` as "current time" instead
    /// of the wall clock, enabling deterministic event-time-based eviction driven
    /// by the streaming executor's watermark.
    ///
    /// The default implementation is a no-op — backends that do not implement
    /// TTL expiry ignore the watermark.
    fn set_watermark(&mut self, _watermark_ms: i64) {}

    /// Downcast hook to the concrete RocksDB backend, when this backend is
    /// (or wraps) one (Phase 56: the incremental SST checkpointer needs the
    /// native `create_rocksdb_checkpoint`; portable snapshots stay the
    /// fallback for every other backend). Default: `None`.
    fn as_rocksdb(&self) -> Option<&crate::rocksdb_backend::RocksDbStateBackend> {
        None
    }

    /// Force any buffered writes to durable storage.
    ///
    /// Backends that batch writes for throughput (e.g.
    /// [`RocksDbStateBackend`](crate::rocksdb_backend::RocksDbStateBackend)
    /// opened with `durable_fsync = false`) buffer puts/deletes in a
    /// write-ahead log and only make them crash-durable when this is called.
    /// The streaming checkpoint path calls `sync()` exactly once per epoch,
    /// after writing all accumulators, so the per-write fsync cost is amortized
    /// into a single flush. In-memory and per-write-fsync backends treat this
    /// as a no-op (the default).
    fn sync(&self) -> StateResult<()> {
        Ok(())
    }
}

// ── InMemoryStateBackend ──────────────────────────────────────────────────────

/// Lightweight in-memory [`StateBackend`] for tests and embedded mode.
///
/// Data is stored in a `HashMap<(Namespace, Vec<u8>), Vec<u8>>` — it is
/// **not** durable and will be lost on process exit.  Use
/// [`crate::rocksdb_backend::RocksDbStateBackend`] for production deployments.
#[derive(Debug, Default)]
pub struct InMemoryStateBackend {
    data: HashMap<(Namespace, Vec<u8>), Vec<u8>>,
}

impl StateBackend for InMemoryStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        Ok(self.data.get(&(namespace.clone(), key.to_vec())).cloned())
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        self.data.insert((namespace.clone(), key), value);
        Ok(())
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        self.data.remove(&(namespace.clone(), key.to_vec()));
        Ok(())
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        self.data.retain(|(ns, _), _| ns != namespace);
        Ok(())
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let mut out: Vec<Namespace> = self
            .data
            .keys()
            .map(|(ns, _)| ns.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        Ok(out)
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        let mut keys: Vec<Vec<u8>> = self
            .data
            .keys()
            .filter_map(|(ns, key)| {
                if ns == namespace {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn snapshot(&self) -> StateResult<Vec<u8>> {
        use std::io::Write;
        let snap_err = |e: std::io::Error| crate::error::StateError::SnapshotCorrupt {
            message: e.to_string(),
        };
        let entry_count = self.data.len() as u64;
        let mut buf = Vec::new();
        buf.write_all(&1u32.to_le_bytes()).map_err(snap_err)?;
        buf.write_all(&entry_count.to_le_bytes())
            .map_err(snap_err)?;
        for ((ns, key), value) in &self.data {
            let op_id = ns.operator_id().as_bytes();
            let name = ns.state_name().as_bytes();
            buf.write_all(&(op_id.len() as u64).to_le_bytes())
                .map_err(snap_err)?;
            buf.write_all(op_id).map_err(snap_err)?;
            buf.write_all(&(name.len() as u64).to_le_bytes())
                .map_err(snap_err)?;
            buf.write_all(name).map_err(snap_err)?;
            buf.write_all(&(key.len() as u64).to_le_bytes())
                .map_err(snap_err)?;
            buf.write_all(key).map_err(snap_err)?;
            buf.write_all(&(value.len() as u64).to_le_bytes())
                .map_err(snap_err)?;
            buf.write_all(value).map_err(snap_err)?;
        }
        Ok(buf)
    }

    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()> {
        use crate::snapshot::decode_snapshot_entries;
        let entries = decode_snapshot_entries(bytes)?;
        self.data.clear();
        for (op_id, state_name, key, value) in entries {
            let ns = Namespace::new(&op_id, &state_name);
            self.data.insert((ns, key), value);
        }
        Ok(())
    }
}
