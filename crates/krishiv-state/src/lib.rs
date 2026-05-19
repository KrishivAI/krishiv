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
    /// List all namespaces present in this backend (R5.2 inspection API).
    fn list_namespaces(&self) -> StateResult<Vec<Namespace>>;
    /// List all keys stored in `namespace` (R5.2 inspection API).
    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>>;
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

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        let mut seen = std::collections::BTreeSet::new();
        for (op_id, state_name, _) in self.store.keys() {
            seen.insert(Namespace::new(op_id, state_name));
        }
        Ok(seen.into_iter().collect())
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        let op = namespace.operator_id();
        let name = namespace.state_name();
        Ok(self
            .store
            .keys()
            .filter(|(o, n, _)| o == op && n == name)
            .map(|(_, _, k)| k.clone())
            .collect())
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

// ── ProcessingTimeTimerKey ────────────────────────────────────────────────────

/// A registered processing-time timer.
///
/// Ordered by `(fire_at_ms, namespace, key)` so a BTreeMap prefix split
/// efficiently drains all timers whose wall-clock deadline has passed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProcessingTimeTimerKey {
    /// Wall-clock time (ms since UNIX epoch) when the timer fires.
    pub fire_at_ms: i64,
    /// Namespace of the operator that registered the timer.
    pub namespace: Namespace,
    /// Record key the timer is associated with.
    pub key: Vec<u8>,
}

impl ProcessingTimeTimerKey {
    /// Create a processing-time timer key.
    pub fn new(namespace: Namespace, key: Vec<u8>, fire_at_ms: i64) -> Self {
        Self { fire_at_ms, namespace, key }
    }
}

// ── ProcessingTimeTimerService ────────────────────────────────────────────────

/// Processing-time timer service contract (R5.2).
///
/// Timers fire based on wall-clock time.  The caller passes `now_ms`
/// explicitly so the implementation is deterministic under test.
pub trait ProcessingTimeTimerService: Send + Sync {
    /// Register a timer that fires when `now_ms >= timer.fire_at_ms`.
    fn register_processing_time_timer(&mut self, timer: ProcessingTimeTimerKey) -> StateResult<()>;
    /// Cancel a timer identified by `(namespace, key)`.  No-op if not found.
    fn cancel_processing_time_timer(
        &mut self,
        namespace: &Namespace,
        key: &[u8],
    ) -> StateResult<()>;
    /// Drain all timers with `fire_at_ms <= now_ms` in ascending order.
    fn drain_fired_processing_time_timers(
        &mut self,
        now_ms: i64,
    ) -> Vec<ProcessingTimeTimerKey>;
    /// Number of pending timers.
    fn pending_count(&self) -> usize;
}

// ── InMemoryProcessingTimeTimerService ────────────────────────────────────────

/// In-memory processing-time timer service for R5.2.
#[derive(Debug, Default)]
pub struct InMemoryProcessingTimeTimerService {
    timers: BTreeMap<ProcessingTimeTimerKey, ()>,
}

impl InMemoryProcessingTimeTimerService {
    /// Create an empty service.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ProcessingTimeTimerService for InMemoryProcessingTimeTimerService {
    fn register_processing_time_timer(&mut self, timer: ProcessingTimeTimerKey) -> StateResult<()> {
        self.timers.insert(timer, ());
        Ok(())
    }

    fn cancel_processing_time_timer(
        &mut self,
        namespace: &Namespace,
        key: &[u8],
    ) -> StateResult<()> {
        self.timers.retain(|t, _| !(t.namespace == *namespace && t.key == key));
        Ok(())
    }

    fn drain_fired_processing_time_timers(&mut self, now_ms: i64) -> Vec<ProcessingTimeTimerKey> {
        let sentinel = ProcessingTimeTimerKey {
            fire_at_ms: now_ms + 1,
            namespace: Namespace::new("", ""),
            key: vec![],
        };
        let pending = self.timers.split_off(&sentinel);
        std::mem::replace(&mut self.timers, pending).into_keys().collect()
    }

    fn pending_count(&self) -> usize {
        self.timers.len()
    }
}

// ── TtlConfig ─────────────────────────────────────────────────────────────────

/// State TTL (time-to-live) configuration (R5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlConfig {
    /// Duration in milliseconds.  State expires this many ms after it is written.
    pub ttl_ms: u64,
}

impl TtlConfig {
    /// Create a TTL config with the given duration.
    pub fn new(ttl_ms: u64) -> Self {
        Self { ttl_ms }
    }
}

// ── TtlStateBackend ───────────────────────────────────────────────────────────

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A [`StateBackend`] wrapper that enforces TTL expiry on all stored values.
///
/// Values are encoded as `[8-byte LE expires_at_ms][raw value bytes]`.
/// Expired values are treated as absent (lazy deletion on read; the raw bytes
/// remain in the inner store until the next write or `clear_namespace`).
pub struct TtlStateBackend<B: StateBackend> {
    inner: B,
    config: TtlConfig,
}

impl<B: StateBackend> TtlStateBackend<B> {
    /// Wrap `inner` with the given TTL config.
    pub fn new(inner: B, config: TtlConfig) -> Self {
        Self { inner, config }
    }

    /// Access the underlying backend.
    pub fn inner(&self) -> &B {
        &self.inner
    }

    fn encode(value: Vec<u8>, expires_at_ms: i64) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(8 + value.len());
        encoded.extend_from_slice(&expires_at_ms.to_le_bytes());
        encoded.extend_from_slice(&value);
        encoded
    }

    fn decode_if_live(encoded: Vec<u8>, now_ms: i64) -> Option<Vec<u8>> {
        if encoded.len() < 8 {
            return None;
        }
        let expires_at_ms =
            i64::from_le_bytes(encoded[..8].try_into().expect("slice is exactly 8 bytes"));
        if now_ms >= expires_at_ms { None } else { Some(encoded[8..].to_vec()) }
    }
}

impl<B: StateBackend> StateBackend for TtlStateBackend<B> {
    fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        match self.inner.get(namespace, key)? {
            None => Ok(None),
            Some(encoded) => Ok(Self::decode_if_live(encoded, unix_now_ms())),
        }
    }

    fn put(&mut self, namespace: &Namespace, key: Vec<u8>, value: Vec<u8>) -> StateResult<()> {
        let expires_at_ms = unix_now_ms() + self.config.ttl_ms as i64;
        self.inner.put(namespace, key, Self::encode(value, expires_at_ms))
    }

    fn delete(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        self.inner.delete(namespace, key)
    }

    fn clear_namespace(&mut self, namespace: &Namespace) -> StateResult<()> {
        self.inner.clear_namespace(namespace)
    }

    fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
        self.inner.list_namespaces()
    }

    fn list_keys(&self, namespace: &Namespace) -> StateResult<Vec<Vec<u8>>> {
        self.inner.list_keys(namespace)
    }
}

// ── StateInspector ────────────────────────────────────────────────────────────

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
        Ok(self.backend.list_keys(namespace)?.iter().map(|k| k.len()).sum())
    }

    /// Always `true` — the inspector never mutates state.
    pub fn is_read_only(&self) -> bool {
        true
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

    // ── list_namespaces / list_keys ───────────────────────────────────────────

    #[test]
    fn list_namespaces_empty_backend() {
        let b = InMemoryStateBackend::new();
        assert!(b.list_namespaces().unwrap().is_empty());
    }

    #[test]
    fn list_namespaces_returns_unique_namespaces() {
        let mut b = InMemoryStateBackend::new();
        let n1 = ns("op1", "counts");
        let n2 = ns("op2", "counts");
        b.put(&n1, b"k1".to_vec(), b"v".to_vec()).unwrap();
        b.put(&n1, b"k2".to_vec(), b"v".to_vec()).unwrap();
        b.put(&n2, b"k1".to_vec(), b"v".to_vec()).unwrap();
        let mut namespaces = b.list_namespaces().unwrap();
        namespaces.sort();
        assert_eq!(namespaces, vec![n1, n2]);
    }

    #[test]
    fn list_keys_returns_keys_for_namespace() {
        let mut b = InMemoryStateBackend::new();
        let n = ns("op1", "window");
        b.put(&n, b"alpha".to_vec(), b"v".to_vec()).unwrap();
        b.put(&n, b"beta".to_vec(), b"v".to_vec()).unwrap();
        b.put(&ns("op1", "other"), b"alpha".to_vec(), b"v".to_vec()).unwrap();
        let mut keys = b.list_keys(&n).unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    }

    // ── ProcessingTimeTimerService ────────────────────────────────────────────

    #[test]
    fn processing_time_timer_fires_at_now_ms() {
        let mut svc = InMemoryProcessingTimeTimerService::new();
        let n = ns("op1", "pt");
        svc.register_processing_time_timer(ProcessingTimeTimerKey::new(
            n.clone(),
            b"k1".to_vec(),
            1000,
        ))
        .unwrap();
        svc.register_processing_time_timer(ProcessingTimeTimerKey::new(
            n.clone(),
            b"k2".to_vec(),
            2000,
        ))
        .unwrap();
        assert!(svc.drain_fired_processing_time_timers(999).is_empty());
        let fired = svc.drain_fired_processing_time_timers(1000);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].fire_at_ms, 1000);
        assert_eq!(svc.pending_count(), 1);
    }

    #[test]
    fn processing_time_timer_cancel_is_noop_for_missing() {
        let mut svc = InMemoryProcessingTimeTimerService::new();
        svc.cancel_processing_time_timer(&ns("op", "s"), b"nope").unwrap();
        assert_eq!(svc.pending_count(), 0);
    }

    // ── TtlStateBackend ───────────────────────────────────────────────────────

    #[test]
    fn ttl_backend_returns_value_before_expiry() {
        let inner = InMemoryStateBackend::new();
        let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
        let n = ns("op1", "session");
        ttl.put(&n, b"k".to_vec(), b"val".to_vec()).unwrap();
        // Immediately after write the value must be live.
        assert_eq!(ttl.get(&n, b"k").unwrap(), Some(b"val".to_vec()));
    }

    #[test]
    fn ttl_backend_expired_value_returns_none() {
        // Write with an expiry in the past by constructing a raw inner entry.
        let mut inner = InMemoryStateBackend::new();
        let n = ns("op1", "session");
        // Manually encode an already-expired entry (expires_at = 1 ms since epoch).
        let expires_at_ms: i64 = 1;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&expires_at_ms.to_le_bytes());
        encoded.extend_from_slice(b"stale");
        inner.put(&n, b"k".to_vec(), encoded).unwrap();

        let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
        // now_ms() >> 1, so this entry must be expired.
        assert!(ttl.get(&n, b"k").unwrap().is_none());
    }

    #[test]
    fn ttl_backend_delete_removes_entry() {
        let inner = InMemoryStateBackend::new();
        let mut ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000));
        let n = ns("op1", "s");
        ttl.put(&n, b"k".to_vec(), b"v".to_vec()).unwrap();
        ttl.delete(&n, b"k").unwrap();
        assert!(ttl.get(&n, b"k").unwrap().is_none());
    }

    // ── StateInspector ────────────────────────────────────────────────────────

    #[test]
    fn state_inspector_is_read_only() {
        let b = InMemoryStateBackend::new();
        let inspector = StateInspector::new(&b);
        assert!(inspector.is_read_only());
    }

    #[test]
    fn state_inspector_key_count_and_namespaces() {
        let mut b = InMemoryStateBackend::new();
        let n = ns("op1", "window");
        b.put(&n, b"a".to_vec(), b"1".to_vec()).unwrap();
        b.put(&n, b"b".to_vec(), b"2".to_vec()).unwrap();
        let inspector = StateInspector::new(&b);
        assert_eq!(inspector.list_namespaces().unwrap(), vec![n.clone()]);
        assert_eq!(inspector.key_count(&n).unwrap(), 2);
        assert_eq!(inspector.key_size_bytes(&n).unwrap(), 2); // "a" + "b"
    }
}
