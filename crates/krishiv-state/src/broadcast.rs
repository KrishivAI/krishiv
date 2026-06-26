//! T18: Flink-equivalent `BroadcastStream` state.
//!
//! `BroadcastState` models a *non-keyed*, replicated map that every parallel
//! instance of an operator can read and any single writer (typically the
//! broadcast input side) can update.  The semantics mirror Flink's
//! `BroadcastProcessFunction.Context::getBroadcastState`.
//!
//! # Relationship to keyed state
//!
//! Unlike keyed state (which is partitioned by input key and only visible to
//! the task owning that key group), broadcast state is stored under a fixed
//! namespace (`broadcast:<state_name>`) so all tasks that share the same
//! operator name see identical data.  The coordinator replicates checkpoint
//! snapshots of broadcast state to all successor tasks via the existing
//! `SnapshotEntry` broadcast path in `krishiv_state::checkpoint::rescaling`.
//!
//! # Storage layout
//!
//! Keys and values are opaque byte slices.  Higher-level typed descriptors
//! (`BroadcastStateDescriptor<K, V>`) plug in serde-friendly codecs so
//! callers work with native Rust types.
//!
//! # Example
//!
//! ```rust
//! use std::sync::{Arc, Mutex};
//! use krishiv_state::{BroadcastState, BroadcastStateDescriptor, InMemoryStateBackend};
//!
//! let descriptor = BroadcastStateDescriptor::new("rules");
//! let backend = Arc::new(Mutex::new(InMemoryStateBackend::default()));
//! let mut state = BroadcastState::open(descriptor, backend);
//!
//! state.put(b"rule-1".to_vec(), b"allow".to_vec()).unwrap();
//! let v = state.get(b"rule-1").unwrap();
//! assert_eq!(v.as_deref(), Some(b"allow".as_ref()));
//! ```

use std::sync::{Arc, Mutex};

use crate::backend::StateBackend;
use crate::error::StateResult;
use crate::namespace::Namespace;

// ── BroadcastBackend: interior-mutability wrapper ────────────────────────────

/// Object-safe trait over a locked state backend.
///
/// Implemented for `Mutex<B>` (not `Arc<Mutex<B>>`) so that `Arc<Mutex<B>>`
/// naturally coerces to `Arc<dyn BroadcastBackend>` — the stable-Rust way to
/// erase a concrete backend type.
pub trait BroadcastBackend: Send + Sync + 'static {
    fn get(&self, ns: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>>;
    fn put(&self, ns: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()>;
    fn delete(&self, ns: &Namespace, key: &[u8]) -> StateResult<()>;
    fn clear_namespace(&self, ns: &Namespace) -> StateResult<()>;
    fn list_keys(&self, ns: &Namespace) -> StateResult<Vec<Vec<u8>>>;
}

impl<B: StateBackend + Send + 'static> BroadcastBackend for Mutex<B> {
    fn get(&self, ns: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        self.lock().unwrap().get(ns, key)
    }
    fn put(&self, ns: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        self.lock().unwrap().put(ns, key, value)
    }
    fn delete(&self, ns: &Namespace, key: &[u8]) -> StateResult<()> {
        self.lock().unwrap().delete(ns, key)
    }
    fn clear_namespace(&self, ns: &Namespace) -> StateResult<()> {
        self.lock().unwrap().clear_namespace(ns)
    }
    fn list_keys(&self, ns: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        self.lock().unwrap().list_keys(ns)
    }
}

// ── Descriptor ────────────────────────────────────────────────────────────────

/// Identifies a broadcast state map by name.
///
/// The descriptor is cheap to clone and is used to open a [`BroadcastState`]
/// handle backed by any [`StateBackend`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BroadcastStateDescriptor {
    /// Logical name of this broadcast state (must be unique per operator).
    pub name: String,
}

impl BroadcastStateDescriptor {
    /// Create a descriptor with the given logical name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Namespace key used inside the [`StateBackend`].
    ///
    /// The prefix `broadcast:` distinguishes broadcast entries from keyed-state
    /// entries at the storage layer and matches the convention used by the
    /// checkpoint rescaling path.
    pub fn namespace(&self) -> Namespace {
        Namespace::new("broadcast", &self.name)
    }
}

// ── BroadcastState ────────────────────────────────────────────────────────────

/// A non-keyed, replicated map state backed by a [`StateBackend`].
///
/// Instances are obtained via [`BroadcastState::open`].  Multiple handles
/// over the same backend share state as long as they use the same descriptor.
///
/// The `backend` argument must be `Arc<Mutex<B>>` where `B: StateBackend`.
/// This matches the type returned by [`crate::RocksDbStateBackend`] construction
/// helpers and the [`crate::InMemoryStateBackend`] used in tests.
pub struct BroadcastState {
    ns: Namespace,
    backend: Arc<dyn BroadcastBackend>,
}

impl BroadcastState {
    /// Open a broadcast state handle.
    ///
    /// Pass an `Arc<Mutex<B>>` for any `B: StateBackend + Send + 'static`.
    pub fn open<B: StateBackend + Send + 'static>(
        descriptor: BroadcastStateDescriptor,
        backend: Arc<Mutex<B>>,
    ) -> Self {
        Self {
            ns: descriptor.namespace(),
            backend: backend as Arc<dyn BroadcastBackend>,
        }
    }

    /// Return the value associated with `key`, or `None` if absent.
    pub fn get(&self, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        self.backend.get(&self.ns, key)
    }

    /// Return `true` if `key` is present in the broadcast state.
    pub fn contains(&self, key: &[u8]) -> StateResult<bool> {
        Ok(self.backend.get(&self.ns, key)?.is_some())
    }

    /// Insert or replace the value for `key`.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> StateResult<()> {
        self.backend.put(&self.ns, key.into(), value.into())
    }

    /// Remove `key` from the broadcast state.  No-op if absent.
    pub fn remove(&mut self, key: &[u8]) -> StateResult<()> {
        self.backend.delete(&self.ns, key)
    }

    /// Return all `(key, value)` pairs in the broadcast state.
    ///
    /// The order of entries is backend-defined (typically insertion order for
    /// the in-memory backend, lexicographic for RocksDB).
    pub fn entries(&self) -> StateResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let keys = self.backend.list_keys(&self.ns)?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.backend.get(&self.ns, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Remove all entries from the broadcast state.
    pub fn clear(&mut self) -> StateResult<()> {
        self.backend.clear_namespace(&self.ns)
    }

    /// Return the number of entries currently stored.
    pub fn len(&self) -> StateResult<usize> {
        Ok(self.backend.list_keys(&self.ns)?.len())
    }

    /// Return `true` when the broadcast state is empty.
    pub fn is_empty(&self) -> StateResult<bool> {
        Ok(self.len()? == 0)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::InMemoryStateBackend;

    fn make_state(name: &str) -> BroadcastState {
        let descriptor = BroadcastStateDescriptor::new(name);
        let backend = Arc::new(Mutex::new(InMemoryStateBackend::default()));
        BroadcastState::open(descriptor, backend)
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut state = make_state("rules");
        state.put(b"rule-1".to_vec(), b"allow".to_vec()).unwrap();
        let v = state.get(b"rule-1").unwrap();
        assert_eq!(v.as_deref(), Some(b"allow".as_ref()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let state = make_state("rules");
        assert!(state.get(b"no-such-key").unwrap().is_none());
    }

    #[test]
    fn contains_reports_presence() {
        let mut state = make_state("cfg");
        assert!(!state.contains(b"k").unwrap());
        state.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert!(state.contains(b"k").unwrap());
    }

    #[test]
    fn remove_deletes_entry() {
        let mut state = make_state("model");
        state.put(b"w".to_vec(), b"0.5".to_vec()).unwrap();
        state.remove(b"w").unwrap();
        assert!(state.get(b"w").unwrap().is_none());
    }

    #[test]
    fn entries_returns_all_pairs() {
        let mut state = make_state("multi");
        state.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        state.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        state.put(b"c".to_vec(), b"3".to_vec()).unwrap();
        let mut entries = state.entries().unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn clear_removes_all_entries() {
        let mut state = make_state("cfg2");
        state.put(b"x".to_vec(), b"1".to_vec()).unwrap();
        state.put(b"y".to_vec(), b"2".to_vec()).unwrap();
        state.clear().unwrap();
        assert!(state.is_empty().unwrap());
    }

    #[test]
    fn len_tracks_entry_count() {
        let mut state = make_state("counter");
        assert_eq!(state.len().unwrap(), 0);
        state.put(b"a".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(state.len().unwrap(), 1);
        state.put(b"b".to_vec(), b"v".to_vec()).unwrap();
        assert_eq!(state.len().unwrap(), 2);
        state.remove(b"a").unwrap();
        assert_eq!(state.len().unwrap(), 1);
    }

    /// Two handles over the same backend with the same descriptor share state.
    #[test]
    fn two_handles_share_state() {
        let descriptor = BroadcastStateDescriptor::new("shared");
        let backend = Arc::new(Mutex::new(InMemoryStateBackend::default()));

        let mut writer = BroadcastState::open(descriptor.clone(), Arc::clone(&backend));
        let reader = BroadcastState::open(descriptor, backend);

        writer.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        assert_eq!(
            reader.get(b"key").unwrap().as_deref(),
            Some(b"value".as_ref())
        );
    }

    /// Two handles with different descriptor names have independent namespaces.
    #[test]
    fn different_descriptors_are_isolated() {
        let backend = Arc::new(Mutex::new(InMemoryStateBackend::default()));
        let mut rules =
            BroadcastState::open(BroadcastStateDescriptor::new("rules"), Arc::clone(&backend));
        let model =
            BroadcastState::open(BroadcastStateDescriptor::new("model"), Arc::clone(&backend));

        rules.put(b"r1".to_vec(), b"deny".to_vec()).unwrap();
        assert!(model.get(b"r1").unwrap().is_none());
        assert!(rules.get(b"r1").unwrap().is_some());
    }
}
