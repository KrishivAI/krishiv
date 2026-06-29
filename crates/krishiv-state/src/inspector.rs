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
