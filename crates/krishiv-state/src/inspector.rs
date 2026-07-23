use crate::backend::StateBackend;
use crate::error::StateResult;
use crate::namespace::Namespace;

/// Read-only metadata inspector for a keyed state backend (R5.2).
///
/// Exposes namespace and key metadata without exposing value bytes or
/// allowing any mutation.  The immutable borrow prevents concurrent writes.
pub struct StateInspector<'a, B: StateBackend> {
    backend: &'a B,
}

impl<'a, B: StateBackend> StateInspector<'a, B> {
    /// Create an inspector.  The backend borrow prevents mutation while
    /// the inspector is live.
    pub fn new(backend: &'a B) -> Self {
        Self { backend }
    }

    /// List all namespaces present in the backend.
    pub fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        self.backend.list_namespaces()
    }

    /// Count the number of keys in `namespace`.
    pub fn key_count(&self, namespace: &Namespace) -> StateResult<usize> {
        Ok(self.backend.list_keys(namespace)?.len())
    }

    /// Total bytes across all key vectors in `namespace`.  Value bytes are
    /// intentionally not surfaced; use key size as a proxy for namespace size.
    pub fn key_size_bytes(&self, namespace: &Namespace) -> StateResult<usize> {
        Ok(self
            .backend
            .list_keys(namespace)?
            .iter()
            .map(|k| k.len())
            .sum())
    }

    /// Always `true` — the inspector never mutates state.
    pub fn is_read_only(&self) -> bool {
        true
    }
}

/// The read half of the State Processor API: reads a keyed-state backend's full
/// contents — keys **and** values — as `(key, value)` pairs, for offline
/// inspection / debugging / migration of checkpointed state (Spark's State Data
/// Source / Flink's State Processor API). Unlike [`StateInspector`] (which
/// deliberately surfaces only metadata), this exposes value bytes, so use it on
/// a restored checkpoint/savepoint backend to materialise "state as a table".
pub struct StateReader<'a, B: StateBackend> {
    backend: &'a B,
}

impl<'a, B: StateBackend> StateReader<'a, B> {
    pub fn new(backend: &'a B) -> Self {
        Self { backend }
    }

    /// All `(key, value)` entries in `namespace`, in backend key order.
    pub fn entries(&self, namespace: &Namespace) -> StateResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let keys = self.backend.list_keys(namespace)?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.backend.get(namespace, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// List all namespaces present in the backend.
    pub fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        self.backend.list_namespaces()
    }
}
