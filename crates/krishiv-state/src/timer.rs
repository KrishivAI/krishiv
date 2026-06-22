use std::collections::{BTreeMap, HashMap};

use crate::error::StateResult;
use crate::namespace::Namespace;

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

/// In-memory event-time timer service for R5.1.
///
/// Timers are stored in a `BTreeMap` ordered by `(deadline_ms, namespace, key)`
/// so that `drain_fired_timers` is an efficient prefix split.
///
/// A secondary `HashMap<(namespace, key), deadline_ms>` index enables O(log N)
/// cancel by identity without scanning the full `BTreeMap`.  Both structures are
/// kept in sync by `register_event_time_timer`, `cancel_timer`, and
/// `drain_fired_timers`.
#[derive(Debug, Default)]
pub struct InMemoryTimerService {
    /// Primary ordered index: `TimerKey → ()`.  Drives deadline-ordered drain.
    timers: BTreeMap<TimerKey, ()>,
    /// Secondary identity index: `(namespace, key) → deadline_ms`.
    /// Enables O(1) lookup of the deadline when cancelling by identity.
    identity_index: HashMap<(Namespace, Vec<u8>), i64>,
}

impl InMemoryTimerService {
    /// Create an empty timer service.
    pub fn new() -> Self {
        Self::default()
    }
}

impl TimerService for InMemoryTimerService {
    fn register_event_time_timer(&mut self, timer: TimerKey) -> StateResult<()> {
        // If a timer for the same (namespace, key) already exists, remove the old
        // primary-index entry first so the two indexes stay in sync.
        let identity = (timer.namespace.clone(), timer.key.clone());
        if let Some(old_deadline) = self.identity_index.get(&identity).copied() {
            let old_key = TimerKey {
                deadline_ms: old_deadline,
                namespace: timer.namespace.clone(),
                key: timer.key.clone(),
            };
            self.timers.remove(&old_key);
        }
        self.identity_index.insert(identity, timer.deadline_ms);
        self.timers.insert(timer, ());
        Ok(())
    }

    fn cancel_timer(&mut self, namespace: &Namespace, key: &[u8]) -> StateResult<()> {
        // O(1) lookup via the secondary identity index instead of a full scan.
        let identity = (namespace.clone(), key.to_vec());
        if let Some(deadline_ms) = self.identity_index.remove(&identity) {
            let timer_key = TimerKey {
                deadline_ms,
                namespace: namespace.clone(),
                key: key.to_vec(),
            };
            self.timers.remove(&timer_key);
        }
        Ok(())
    }

    fn drain_fired_timers(&mut self, watermark_ms: i64) -> Vec<TimerKey> {
        // Sentinel: the smallest key with deadline_ms > watermark_ms.
        // BTreeMap::split_off returns [sentinel, ∞); self keeps [−∞, sentinel).
        // After the split: self.timers has fired timers, `pending` has the rest.
        let sentinel = TimerKey {
            deadline_ms: watermark_ms.saturating_add(1),
            namespace: Namespace::new("", ""),
            key: vec![],
        };
        let pending = self.timers.split_off(&sentinel);
        let fired: Vec<TimerKey> = std::mem::replace(&mut self.timers, pending)
            .into_keys()
            .collect();
        // Evict fired timers from the identity index.
        for t in &fired {
            self.identity_index
                .remove(&(t.namespace.clone(), t.key.clone()));
        }
        fired
    }

    fn pending_count(&self) -> usize {
        self.timers.len()
    }
}
