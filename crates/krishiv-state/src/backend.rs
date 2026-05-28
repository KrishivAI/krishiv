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
    /// The backend is cleared before loading; partial failures leave the backend empty.
    fn load_snapshot(&mut self, bytes: &[u8]) -> StateResult<()>;

    /// Store multiple `(namespace_op_id, namespace_state_name, key, value)` entries.
    ///
    /// The default implementation calls `put` for each entry individually.
    /// Backends that support batch writes should override this for efficiency —
    /// `RedbStateBackend` overrides this to open a single write transaction for all entries.
    fn put_batch(&mut self, entries: &[(&str, &str, &[u8], &[u8])]) -> StateResult<()> {
        for (op_id, name, key, value) in entries {
            let ns = Namespace::new(*op_id, *name);
            self.put(&ns, key.to_vec(), value.to_vec())?;
        }
        Ok(())
    }

    /// Retrieve multiple values for `(namespace_op_id, namespace_state_name, key)` triples.
    ///
    /// The default implementation calls `get` for each entry individually.
    /// Backends that support batch reads should override this for efficiency —
    /// `RedbStateBackend` overrides this to open a single read transaction for all keys.
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
}
