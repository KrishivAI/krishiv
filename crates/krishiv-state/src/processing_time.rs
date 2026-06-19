use std::collections::{BTreeMap, HashMap};

use crate::error::StateResult;
use crate::namespace::Namespace;

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
        Self {
            fire_at_ms,
            namespace,
            key,
        }
    }
}

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
    fn drain_fired_processing_time_timers(&mut self, now_ms: i64) -> Vec<ProcessingTimeTimerKey>;
    /// Number of pending timers.
    fn pending_count(&self) -> usize;
}

/// In-memory processing-time timer service for R5.2.
///
/// A secondary `HashMap<(namespace, key), fire_at_ms>` index enables O(1)
/// cancel by identity without scanning the full `BTreeMap`.  Both structures
/// are kept in sync by `register_processing_time_timer`,
/// `cancel_processing_time_timer`, and `drain_fired_processing_time_timers`.
#[derive(Debug, Default)]
pub struct InMemoryProcessingTimeTimerService {
    timers: BTreeMap<ProcessingTimeTimerKey, ()>,
    identity_index: HashMap<(Namespace, Vec<u8>), i64>,
}

impl InMemoryProcessingTimeTimerService {
    /// Create an empty service.
    pub fn new() -> Self {
        Self::default()
    }
}

impl ProcessingTimeTimerService for InMemoryProcessingTimeTimerService {
    fn register_processing_time_timer(&mut self, timer: ProcessingTimeTimerKey) -> StateResult<()> {
        let identity = (timer.namespace.clone(), timer.key.clone());
        if let Some(old_fire_at) = self.identity_index.get(&identity).copied() {
            let old_key = ProcessingTimeTimerKey {
                fire_at_ms: old_fire_at,
                namespace: timer.namespace.clone(),
                key: timer.key.clone(),
            };
            self.timers.remove(&old_key);
        }
        self.identity_index.insert(identity, timer.fire_at_ms);
        self.timers.insert(timer, ());
        Ok(())
    }

    fn cancel_processing_time_timer(
        &mut self,
        namespace: &Namespace,
        key: &[u8],
    ) -> StateResult<()> {
        let identity = (namespace.clone(), key.to_vec());
        if let Some(fire_at_ms) = self.identity_index.remove(&identity) {
            let timer_key = ProcessingTimeTimerKey {
                fire_at_ms,
                namespace: namespace.clone(),
                key: key.to_vec(),
            };
            self.timers.remove(&timer_key);
        }
        Ok(())
    }

    fn drain_fired_processing_time_timers(&mut self, now_ms: i64) -> Vec<ProcessingTimeTimerKey> {
        let sentinel = ProcessingTimeTimerKey {
            fire_at_ms: now_ms + 1,
            namespace: Namespace::new("", ""),
            key: vec![],
        };
        let pending = self.timers.split_off(&sentinel);
        let fired: Vec<ProcessingTimeTimerKey> = std::mem::replace(&mut self.timers, pending)
            .into_keys()
            .collect();
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
