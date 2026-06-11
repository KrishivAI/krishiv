#![forbid(unsafe_code)]

//! E4.4 — QueryableState: runtime point-lookup on live operator state.
//!
//! [`QueryableStateStore`] is a thread-safe registry that maps
//! `(job_id, op_id)` pairs to their live [`StateBackend`] instances.
//! External callers (HTTP/gRPC) query individual keys without stopping the job.
//!
//! # Usage
//! ```ignore
//! let store = QueryableStateStore::new();
//! store.register("job-1", "agg-op", Arc::new(backend));
//!
//! let val = store.get("job-1", "agg-op", "window_counts", b"user-42")?;
//! store.deregister_job("job-1");
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::backend::StateBackend;
use crate::error::{StateError, StateResult};
use crate::namespace::Namespace;

// ── QueryableStateStore ───────────────────────────────────────────────────────

/// Thread-safe registry of live operator state backends.
///
/// Backends are registered by `(job_id, op_id)` and can be queried by
/// external callers without holding any per-operator lock.
#[derive(Clone, Default)]
pub struct QueryableStateStore {
    /// `"{job_id}::{op_id}"` → backend
    backends: Arc<RwLock<HashMap<String, Arc<dyn StateBackend + Send + Sync>>>>,
}

impl QueryableStateStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `backend` under `(job_id, op_id)`.
    ///
    /// Overwrites any previously registered backend for the same pair.
    /// Returns silently if the registry lock is poisoned (only possible after
    /// a panic in a concurrent writer, which leaves the system in an inconsistent state).
    pub fn register(
        &self,
        job_id: &str,
        op_id: &str,
        backend: Arc<dyn StateBackend + Send + Sync>,
    ) {
        let key = registry_key(job_id, op_id);
        if let Ok(mut map) = self.backends.write() {
            map.insert(key, backend);
        }
    }

    /// Look up `key` in namespace `(op_id, state_name)` for job `job_id`.
    ///
    /// Returns `None` if the job/operator is not registered or the key is absent.
    pub fn get(
        &self,
        job_id: &str,
        op_id: &str,
        state_name: &str,
        key: &[u8],
    ) -> StateResult<Option<Vec<u8>>> {
        let map = self.backends.read().map_err(|_| StateError::LockPoisoned {
            message: "queryable state registry lock poisoned".into(),
        })?;
        let k = registry_key(job_id, op_id);
        match map.get(&k) {
            None => Ok(None),
            Some(backend) => {
                let ns = Namespace::new(op_id, state_name);
                backend.get(&ns, key)
            }
        }
    }

    /// Return the set of registered `(job_id, op_id)` pairs.
    pub fn list_registered(&self) -> Vec<(String, String)> {
        let Ok(map) = self.backends.read() else {
            return Vec::new();
        };
        map.keys()
            .filter_map(|k| {
                let mut parts = k.splitn(2, "::");
                let job = parts.next()?.to_owned();
                let op = parts.next()?.to_owned();
                Some((job, op))
            })
            .collect()
    }

    /// List all state namespaces for `(job_id, op_id)`.
    ///
    /// Returns an empty vec if the operator is not registered.
    pub fn list_namespaces(
        &self,
        job_id: &str,
        op_id: &str,
    ) -> StateResult<Vec<crate::namespace::Namespace>> {
        let map = self.backends.read().map_err(|_| StateError::LockPoisoned {
            message: "queryable state registry lock poisoned".into(),
        })?;
        let k = registry_key(job_id, op_id);
        match map.get(&k) {
            None => Ok(vec![]),
            Some(backend) => backend.list_namespaces(),
        }
    }

    /// Remove all backends registered for `job_id`.
    pub fn deregister_job(&self, job_id: &str) {
        let prefix = format!("{job_id}::");
        if let Ok(mut map) = self.backends.write() {
            map.retain(|k, _| !k.starts_with(&prefix));
        }
    }

    /// Return a `QueryableStateHandle` for point lookups on a specific operator.
    ///
    /// Returns `Err` if the operator is not registered.
    pub fn handle(
        &self,
        job_id: &str,
        op_id: &str,
        state_name: &str,
    ) -> StateResult<QueryableStateHandle> {
        let k = registry_key(job_id, op_id);
        let backend = self
            .backends
            .read()
            .map_err(|_| StateError::LockPoisoned {
                message: "queryable state registry lock poisoned".into(),
            })?
            .get(&k)
            .cloned()
            .ok_or_else(|| StateError::BackendUnavailable {
                message: format!("no queryable backend for job={job_id} op={op_id}"),
                source: None,
            })?;
        Ok(QueryableStateHandle {
            backend,
            namespace: Namespace::new(op_id, state_name),
        })
    }
}

impl std::fmt::Debug for QueryableStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.backends.read().map(|m| m.len()).unwrap_or(0);
        write!(f, "QueryableStateStore({count} backends)")
    }
}

// ── QueryableStateHandle ──────────────────────────────────────────────────────

/// A scoped handle for point lookups on one operator's state name.
///
/// Created via [`QueryableStateStore::handle`].
pub struct QueryableStateHandle {
    backend: Arc<dyn StateBackend + Send + Sync>,
    namespace: Namespace,
}

impl QueryableStateHandle {
    /// Look up `key` in the operator's state namespace.
    pub fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        self.backend.get(&self.namespace, key)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn registry_key(job_id: &str, op_id: &str) -> String {
    format!("{job_id}::{op_id}")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StateBackend;
    use crate::rocksdb_backend::RocksDbStateBackend;

    fn make_backend() -> Arc<RocksDbStateBackend> {
        Arc::new(RocksDbStateBackend::new().unwrap())
    }

    fn prepopulated_backend(
        op_id: &str,
        state_name: &str,
        key: &[u8],
        val: &[u8],
    ) -> Arc<RocksDbStateBackend> {
        let mut b = RocksDbStateBackend::new().unwrap();
        let ns = Namespace::new(op_id, state_name);
        b.put(&ns, key.to_vec(), val.to_vec()).unwrap();
        Arc::new(b)
    }

    #[test]
    fn get_returns_value_from_registered_backend() {
        let store = QueryableStateStore::new();
        let backend = prepopulated_backend("op-1", "counts", b"user-a", b"42");
        store.register("job-1", "op-1", backend);

        let val = store.get("job-1", "op-1", "counts", b"user-a").unwrap();
        assert_eq!(val, Some(b"42".to_vec()));
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let store = QueryableStateStore::new();
        let backend = prepopulated_backend("op-1", "counts", b"user-a", b"42");
        store.register("job-1", "op-1", backend);

        let val = store.get("job-1", "op-1", "counts", b"no-such-key").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn get_returns_none_for_unregistered_operator() {
        let store = QueryableStateStore::new();
        let val = store.get("job-99", "op-1", "counts", b"key").unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn deregister_job_removes_all_operators() {
        let store = QueryableStateStore::new();
        store.register("job-1", "op-1", make_backend());
        store.register("job-1", "op-2", make_backend());
        store.register("job-2", "op-1", make_backend());

        store.deregister_job("job-1");
        let registered = store.list_registered();
        assert!(registered.iter().all(|(j, _)| j != "job-1"));
        assert!(registered.iter().any(|(j, _)| j == "job-2"));
    }

    #[test]
    fn list_registered_returns_all_pairs() {
        let store = QueryableStateStore::new();
        store.register("job-1", "op-1", make_backend());
        store.register("job-1", "op-2", make_backend());

        let mut list = store.list_registered();
        list.sort();
        assert_eq!(list.len(), 2);
        assert!(list.iter().all(|(j, _)| j == "job-1"));
    }

    #[test]
    fn handle_get_returns_value() {
        let store = QueryableStateStore::new();
        let backend = prepopulated_backend("op-1", "timers", b"key1", b"v1");
        store.register("job-1", "op-1", backend);

        let handle = store.handle("job-1", "op-1", "timers").unwrap();
        assert_eq!(handle.get(b"key1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn handle_errors_for_unregistered_operator() {
        let store = QueryableStateStore::new();
        assert!(store.handle("job-x", "op-x", "state").is_err());
    }

    #[test]
    fn register_overwrites_previous_backend() {
        let store = QueryableStateStore::new();
        let old = prepopulated_backend("op-1", "s", b"k", b"old");
        let new = prepopulated_backend("op-1", "s", b"k", b"new");
        store.register("job-1", "op-1", old);
        store.register("job-1", "op-1", new);

        let val = store.get("job-1", "op-1", "s", b"k").unwrap();
        assert_eq!(val, Some(b"new".to_vec()));
    }
}
