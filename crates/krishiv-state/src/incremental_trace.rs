#![forbid(unsafe_code)]

//! Durable `TraceStateNamespace` — RocksDB-backed persistence for incremental
//! operator Traces.
//!
//! Each incremental operator (join, aggregate) maintains a `Trace` in memory
//! for fast probing. On checkpoint, the Trace is serialized as Arrow IPC and
//! written to RocksDB under a key derived from `(operator_uid, behavior_version,
//! partition_id)`. On restore, the Trace is deserialized and replayed.
//!
//! When `behavior_version` changes, the old key is absent; the operator starts
//! with an empty Trace and recomputes from scratch.

use std::sync::{Arc, Mutex};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;
use crate::rocksdb_backend::RocksDbStateBackend;

/// Operator-id used in the RocksDB namespace for all Trace entries.
pub const TRACE_NAMESPACE: &str = "__incremental_trace__";
/// State-name component of the RocksDB namespace for all Trace entries.
const TRACE_STATE_NAME: &str = "trace";

/// Key encoding: `{operator_uid}\x00{behavior_version:016x}\x00{partition_id:08x}`
fn trace_key(uid: &str, behavior_version: u64, partition_id: u32) -> Vec<u8> {
    format!("{uid}\x00{behavior_version:016x}\x00{partition_id:08x}").into_bytes()
}

fn lock_err() -> StateError {
    StateError::LockPoisoned {
        message: "incremental trace backend lock poisoned".into(),
    }
}

/// Persistent store for incremental operator Trace data.
///
/// Wraps `RocksDbStateBackend` with namespace isolation so that Trace entries
/// never collide with regular operator keyed state.
pub struct TraceStateNamespace {
    backend: Arc<Mutex<RocksDbStateBackend>>,
    namespace: Namespace,
}

impl TraceStateNamespace {
    pub fn new(backend: Arc<Mutex<RocksDbStateBackend>>) -> Self {
        Self {
            backend,
            namespace: Namespace::new(TRACE_NAMESPACE, TRACE_STATE_NAME),
        }
    }

    /// Persist Arrow IPC bytes for an operator's Trace.
    ///
    /// Overwrites any existing entry for the same `(uid, behavior_version, partition_id)`.
    pub fn put_trace(
        &self,
        uid: &str,
        behavior_version: u64,
        partition_id: u32,
        ipc_bytes: &[u8],
    ) -> StateResult<()> {
        let key = trace_key(uid, behavior_version, partition_id);
        let mut guard = self.backend.lock().map_err(|_| lock_err())?;
        guard.put(&self.namespace, key, ipc_bytes.to_vec())
    }

    /// Retrieve Arrow IPC bytes for an operator's Trace.
    ///
    /// Returns `None` if no entry exists (e.g., first run or after a
    /// `behavior_version` bump that changed the key).
    pub fn get_trace(
        &self,
        uid: &str,
        behavior_version: u64,
        partition_id: u32,
    ) -> StateResult<Option<Vec<u8>>> {
        let key = trace_key(uid, behavior_version, partition_id);
        let guard = self.backend.lock().map_err(|_| lock_err())?;
        guard.get(&self.namespace, &key)
    }

    /// Delete all Trace entries for an operator UID across all versions and partitions.
    /// Used when an operator is removed from the flow.
    pub fn delete_operator_traces(&self, uid: &str) -> StateResult<usize> {
        let prefix = format!("{uid}\x00").into_bytes();
        let mut guard = self.backend.lock().map_err(|_| lock_err())?;

        let matching: Vec<Vec<u8>> = guard
            .list_keys(&self.namespace)?
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect();

        let count = matching.len();
        if count > 0 {
            let entries: Vec<(&Namespace, &[u8])> =
                matching.iter().map(|k| (&self.namespace, k.as_slice())).collect();
            guard.delete_batch(&entries)?;
        }
        Ok(count)
    }

    /// Delete a specific Trace entry (e.g., when a behavior_version is retired).
    pub fn delete_trace(
        &self,
        uid: &str,
        behavior_version: u64,
        partition_id: u32,
    ) -> StateResult<()> {
        let key = trace_key(uid, behavior_version, partition_id);
        let mut guard = self.backend.lock().map_err(|_| lock_err())?;
        guard.delete(&self.namespace, &key)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend() -> Arc<Mutex<RocksDbStateBackend>> {
        Arc::new(Mutex::new(RocksDbStateBackend::new().unwrap()))
    }

    #[test]
    fn put_and_get_roundtrip() {
        let ns = TraceStateNamespace::new(test_backend());
        let data = b"fake-ipc-bytes";
        ns.put_trace("op-join-1", 0, 0, data).unwrap();
        let got = ns.get_trace("op-join-1", 0, 0).unwrap();
        assert_eq!(got.as_deref(), Some(data.as_slice()));
    }

    #[test]
    fn behavior_version_change_returns_none() {
        let ns = TraceStateNamespace::new(test_backend());
        ns.put_trace("op-1", 0, 0, b"old").unwrap();
        let got = ns.get_trace("op-1", 1, 0).unwrap(); // version 1 doesn't exist
        assert_eq!(got, None);
    }

    #[test]
    fn delete_operator_removes_all_versions() {
        let ns = TraceStateNamespace::new(test_backend());
        ns.put_trace("op-A", 0, 0, b"v0").unwrap();
        ns.put_trace("op-A", 1, 0, b"v1").unwrap();
        let removed = ns.delete_operator_traces("op-A").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(ns.get_trace("op-A", 0, 0).unwrap(), None);
        assert_eq!(ns.get_trace("op-A", 1, 0).unwrap(), None);
    }

    #[test]
    fn delete_operator_does_not_affect_other_operators() {
        let ns = TraceStateNamespace::new(test_backend());
        ns.put_trace("op-A", 0, 0, b"a").unwrap();
        ns.put_trace("op-B", 0, 0, b"b").unwrap();
        ns.delete_operator_traces("op-A").unwrap();
        assert_eq!(ns.get_trace("op-A", 0, 0).unwrap(), None);
        assert_eq!(ns.get_trace("op-B", 0, 0).unwrap(), Some(b"b".to_vec()));
    }

    #[test]
    fn delete_specific_trace() {
        let ns = TraceStateNamespace::new(test_backend());
        ns.put_trace("op-1", 0, 0, b"data").unwrap();
        ns.delete_trace("op-1", 0, 0).unwrap();
        assert_eq!(ns.get_trace("op-1", 0, 0).unwrap(), None);
    }
}
