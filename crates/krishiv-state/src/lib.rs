#![forbid(unsafe_code)]

//! Keyed state API and in-memory backend for Krishiv R5.1 stateful streaming.
//!
//! State must be accessed only within `process_batch` or
//! `flush_triggered_windows` on the executor operator loop — never from
//! timer callbacks.  The `InMemoryStateBackend` is the R5.1 implementation;
//! RocksDB arrives in R5.2 behind the same `StateBackend` trait.

use std::collections::BTreeMap;

// ── Error / Result ────────────────────────────────────────────────────────────

/// Errors from keyed state operations.
#[derive(Debug)]
pub enum StateError {
    BackendUnavailable { message: String },
}

impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendUnavailable { message } => {
                write!(f, "state backend unavailable: {message}")
            }
        }
    }
}

impl std::error::Error for StateError {}

/// Convenience alias for state operation results.
pub type StateResult<T> = Result<T, StateError>;

// ── Namespace ─────────────────────────────────────────────────────────────────

/// A state namespace scoped to one operator and one logical state variable.
///
/// Maps 1:1 to a RocksDB column family in R5.2.  The compound name
/// `{operator_id}:{state_name}` is unique per job.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace {
    operator_id: String,
    state_name: String,
}

impl Namespace {
    /// Create a namespace.
    pub fn new(operator_id: impl Into<String>, state_name: impl Into<String>) -> Self {
        Self {
            operator_id: operator_id.into(),
            state_name: state_name.into(),
        }
    }

    /// Operator that owns this namespace.
    pub fn operator_id(&self) -> &str {
        &self.operator_id
    }

    /// Logical state variable name within the operator.
    pub fn state_name(&self) -> &str {
        &self.state_name
    }

    /// Composite name used for logging and RocksDB column-family mapping.
    pub fn column_family_name(&self) -> String {
        format!("{}:{}", self.operator_id, self.state_name)
    }
}

// ── StateBackend ──────────────────────────────────────────────────────────────

/// Keyed state backend contract for streaming operators.
///
/// All methods are synchronous so the caller controls async dispatch
/// (e.g. `spawn_blocking` for the RocksDB backend in R5.2).
pub trait StateBackend: Send + Sync {
    /// Return the value stored for `key` in `namespace`, or `None` if absent.
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>>;
    /// Store `value` under `key` in `namespace`.
    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()>;
    /// Remove `key` from `namespace`.  No-op if absent.
    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()>;
    /// Remove all keys in `namespace`.
    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()>;
}

// ── InMemoryStateBackend ──────────────────────────────────────────────────────

// Compound map key: (operator_id, state_name, record_key)
type InMemKey = (String, String, Vec<u8>);

/// In-memory keyed state backend for R5.1.
///
/// State survives for the job lifetime but is lost on executor restart.
/// R5.2 replaces this with a RocksDB backend that checkpoints to object store.
#[derive(Debug, Default, Clone)]
pub struct InMemoryStateBackend {
    store: BTreeMap<InMemKey, Vec<u8>>,
}

impl InMemoryStateBackend {
    /// Create an empty in-memory backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of keys stored across all namespaces.
    pub fn key_count(&self) -> usize {
        self.store.len()
    }

    fn make_key(namespace: &Namespace, key: &[u8]) -> InMemKey {
        (
            namespace.operator_id().to_owned(),
            namespace.state_name().to_owned(),
            key.to_vec(),
        )
    }
}

impl StateBackend for InMemoryStateBackend {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        Ok(self.store.get(&Self::make_key(namespace, key)).cloned())
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        self.store.insert(Self::make_key(namespace, &key), value);
        Ok(())
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        self.store.remove(&Self::make_key(namespace, key));
        Ok(())
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        let op = namespace.operator_id().to_owned();
        let name = namespace.state_name().to_owned();
        self.store.retain(|(o, n, _), _| o != &op || n != &name);
        Ok(())
    }
}

// ── TimerKey ──────────────────────────────────────────────────────────────────

/// A registered event-time timer for a `(namespace, record_key)` pair.
///
/// Ordered by `(deadline_ms, namespace, key)` so `BTreeMap` iterates fired
/// timers in deadline order — a prefix scan is sufficient.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimerKey {
    /// Fires when `watermark_ms >= deadline_ms`.
    pub deadline_ms: i64,
    /// Namespace of the operator that registered the timer.
    pub namespace: Namespace,
    /// Record key the timer is associated with.
    pub key: Vec<u8>,
}

impl TimerKey {
    /// Create a timer key.
    pub fn new(namespace: Namespace, key: Vec<u8>, deadline_ms: i64) -> Self {
        Self {
            deadline_ms,
            namespace,
            key,
        }
    }
}

// ── TimerService ──────────────────────────────────────────────────────────────

/// Event-time timer service contract.
///
/// R5.1 supports event-time timers only.  Processing-time timers arrive in R5.2.
pub trait TimerService: Send + Sync {
    /// Register a timer that fires when `watermark_ms >= timer.deadline_ms`.
    fn register_event_time_timer(&mut self, timer: TimerKey) -> StateResult<()>;
    /// Cancel a timer identified by `(namespace, key)`.  No-op if not found.
    fn cancel_timer(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()>;
    /// Drain all timers with `deadline_ms <= watermark_ms`, returning them in
    /// ascending deadline order.
    fn drain_fired_timers(&mut self, watermark_ms: i64) -> Vec<TimerKey>;
    /// Number of pending (not yet fired) timers.
    fn pending_count(&self) -> usize;
}

// ── InMemoryTimerService ──────────────────────────────────────────────────────

/// In-memory event-time timer service for R5.1.
///
/// Timers are stored in a `BTreeMap` ordered by `(deadline_ms, namespace, key)`
/// so that `drain_fired_timers` is an efficient prefix split.
#[derive(Debug, Default)]
pub struct InMemoryTimerService {
    timers: BTreeMap<TimerKey, ()>,
}

impl InMemoryTimerService {
    /// Create an empty timer service.
    pub fn new() -> Self {
        Self::default()
    }
}

impl TimerService for InMemoryTimerService {
    fn register_event_time_timer(&mut self, timer: TimerKey) -> StateResult<()> {
        self.timers.insert(timer, ());
        Ok(())
    }

    fn cancel_timer(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        // A timer is identified by (namespace, key) regardless of deadline_ms.
        // Scan is O(n) but timer counts per operator are small in practice.
        self.timers
            .retain(|t, _| !(t.namespace == *namespace && t.key == key));
        Ok(())
    }

    fn drain_fired_timers(&mut self, watermark_ms: i64) -> Vec<TimerKey> {
        // Sentinel: the smallest key with deadline_ms > watermark_ms.
        // BTreeMap::split_off returns [sentinel, ∞); self keeps [−∞, sentinel).
        // After the split: self.timers has fired timers, `pending` has the rest.
        let sentinel = TimerKey {
            deadline_ms: watermark_ms + 1,
            namespace: Namespace::new("", ""),
            key: vec![],
        };
        let pending = self.timers.split_off(&sentinel);
        std::mem::replace(&mut self.timers, pending)
            .into_keys()
            .collect()
    }

    fn pending_count(&self) -> usize {
        self.timers.len()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(op: &str, name: &str) -> Namespace {
        Namespace::new(op, name)
    }

    // ── StateBackend ──────────────────────────────────────────────────────────

    #[test]
    fn state_get_missing_returns_none() {
        let backend = InMemoryStateBackend::new();
        assert!(backend.get(&ns("op1", "window"), b"k1").unwrap().is_none());
    }

    #[test]
    fn state_put_and_get_roundtrip() {
        let mut backend = InMemoryStateBackend::new();
        let n = ns("op1", "counts");
        backend.put(&n, b"user-a".to_vec(), b"42".to_vec()).unwrap();
        assert_eq!(backend.get(&n, b"user-a").unwrap(), Some(b"42".to_vec()));
    }

    #[test]
    fn state_delete_removes_key() {
        let mut backend = InMemoryStateBackend::new();
        let n = ns("op1", "counts");
        backend.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
        backend.delete(&n, b"k").unwrap();
        assert!(backend.get(&n, b"k").unwrap().is_none());
    }

    #[test]
    fn state_delete_missing_key_is_noop() {
        let mut backend = InMemoryStateBackend::new();
        backend
            .delete(&ns("op1", "counts"), b"nonexistent")
            .unwrap();
    }

    #[test]
    fn state_clear_namespace_removes_only_matching_keys() {
        let mut backend = InMemoryStateBackend::new();
        let ns_a = ns("op1", "window");
        let ns_b = ns("op1", "other");
        let ns_c = ns("op2", "window");

        backend.put(&ns_a, b"k1".to_vec(), b"v1".to_vec()).unwrap();
        backend.put(&ns_a, b"k2".to_vec(), b"v2".to_vec()).unwrap();
        backend.put(&ns_b, b"k1".to_vec(), b"vb".to_vec()).unwrap();
        backend.put(&ns_c, b"k1".to_vec(), b"vc".to_vec()).unwrap();

        backend.clear_namespace(&ns_a).unwrap();

        assert!(backend.get(&ns_a, b"k1").unwrap().is_none());
        assert!(backend.get(&ns_a, b"k2").unwrap().is_none());
        assert_eq!(backend.get(&ns_b, b"k1").unwrap(), Some(b"vb".to_vec()));
        assert_eq!(backend.get(&ns_c, b"k1").unwrap(), Some(b"vc".to_vec()));
    }

    #[test]
    fn state_namespaces_are_isolated() {
        let mut backend = InMemoryStateBackend::new();
        let ns_a = ns("op1", "window");
        let ns_b = ns("op2", "window");
        backend
            .put(&ns_a, b"key".to_vec(), b"val-a".to_vec())
            .unwrap();
        backend
            .put(&ns_b, b"key".to_vec(), b"val-b".to_vec())
            .unwrap();
        assert_eq!(backend.get(&ns_a, b"key").unwrap(), Some(b"val-a".to_vec()));
        assert_eq!(backend.get(&ns_b, b"key").unwrap(), Some(b"val-b".to_vec()));
    }

    // ── Namespace ─────────────────────────────────────────────────────────────

    #[test]
    fn namespace_column_family_name_format() {
        let n = Namespace::new("window-op", "counts");
        assert_eq!(n.column_family_name(), "window-op:counts");
    }

    // ── TimerService ──────────────────────────────────────────────────────────

    #[test]
    fn timer_fires_at_correct_watermark() {
        let mut svc = InMemoryTimerService::new();
        let n = ns("tw", "timers");

        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
            .unwrap();
        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
            .unwrap();

        assert_eq!(svc.pending_count(), 2);

        // Nothing fires before deadline.
        assert!(svc.drain_fired_timers(999).is_empty());
        assert_eq!(svc.pending_count(), 2);

        // First fires at exact deadline.
        let fired = svc.drain_fired_timers(1000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].deadline_ms, 1000);
        assert_eq!(svc.pending_count(), 1);

        // Second fires.
        let fired = svc.drain_fired_timers(2000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].deadline_ms, 2000);
        assert_eq!(svc.pending_count(), 0);
    }

    #[test]
    fn timer_drain_order_is_ascending_deadline() {
        let mut svc = InMemoryTimerService::new();
        let n = ns("tw", "timers");

        // Register in reverse order.
        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k3".to_vec(), 3000))
            .unwrap();
        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
            .unwrap();
        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
            .unwrap();

        let fired = svc.drain_fired_timers(3000);
        assert_eq!(fired.len(), 3);
        assert_eq!(fired[0].deadline_ms, 1000);
        assert_eq!(fired[1].deadline_ms, 2000);
        assert_eq!(fired[2].deadline_ms, 3000);
    }

    #[test]
    fn timer_cancel_removes_correct_timer() {
        let mut svc = InMemoryTimerService::new();
        let n = ns("tw", "timers");

        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k1".to_vec(), 1000))
            .unwrap();
        svc.register_event_time_timer(TimerKey::new(n.clone(), b"k2".to_vec(), 2000))
            .unwrap();

        svc.cancel_timer(&n, b"k1").unwrap();
        assert_eq!(svc.pending_count(), 1);

        let fired = svc.drain_fired_timers(2000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].key, b"k2");
    }

    #[test]
    fn timer_cancel_missing_is_noop() {
        let mut svc = InMemoryTimerService::new();
        svc.cancel_timer(&ns("tw", "timers"), b"nonexistent")
            .unwrap();
        assert_eq!(svc.pending_count(), 0);
    }

    #[test]
    fn timer_drain_empty_returns_empty() {
        let mut svc = InMemoryTimerService::new();
        assert!(svc.drain_fired_timers(9999).is_empty());
    }
}
