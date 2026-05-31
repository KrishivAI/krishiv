//! gRPC checkpoint barrier injection (R16 S1.2).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use krishiv_proto::KeyGroupRange;
use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};

/// Receives barriers from the coordinator and dispatches to source injectors.
#[derive(Debug, Default)]
pub struct BarrierInjector {
    pending: VecDeque<CheckpointBarrier>,
    last_injected_epoch: u64,
}

impl BarrierInjector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a barrier from the coordinator stream.
    pub fn enqueue(&mut self, barrier: CheckpointBarrier) {
        self.pending.push_back(barrier);
    }

    /// Pop the next barrier if sources have finished emitting pre-barrier records.
    pub fn next_barrier(&mut self) -> Option<CheckpointBarrier> {
        let next = self.pending.pop_front()?;
        if next.epoch <= self.last_injected_epoch {
            return None;
        }
        self.last_injected_epoch = next.epoch;
        Some(next)
    }

    /// Convert a proto barrier to operator epoch for `OperatorMessage::Barrier`.
    pub fn operator_epoch(barrier: &CheckpointBarrier) -> u64 {
        barrier.epoch
    }
}

/// Thread-safe barrier inbox shared between gRPC task and source operators.
#[derive(Clone, Default)]
pub struct SharedBarrierInjector {
    inner: Arc<Mutex<BarrierInjector>>,
}

impl SharedBarrierInjector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, barrier: CheckpointBarrier) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .enqueue(barrier);
    }

    pub fn next_barrier(&self) -> Option<CheckpointBarrier> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .next_barrier()
    }
}

/// Task-id keyed state-handle key-group ranges used by the barrier service.
#[derive(Clone, Default)]
pub struct SharedKeyGroupRanges {
    inner: Arc<DashMap<String, KeyGroupRange>>,
}

impl SharedKeyGroupRanges {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, task_id: impl Into<String>, range: KeyGroupRange) {
        self.inner.insert(task_id.into(), range);
    }

    pub fn get(&self, task_id: &str) -> Option<KeyGroupRange> {
        self.inner.get(task_id).map(|r| *r)
    }

    pub fn remove(&self, task_id: &str) {
        self.inner.remove(task_id);
    }
}

/// Build a checkpoint barrier message.
pub fn make_checkpoint_barrier(job_id: &str, epoch: u64, checkpoint_id: &str) -> CheckpointBarrier {
    CheckpointBarrier {
        epoch,
        job_id: job_id.to_string(),
        checkpoint_id: checkpoint_id.to_string(),
        barrier_kind: BarrierKind::Checkpoint as i32,
        timestamp_ms: krishiv_common::async_util::unix_now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_injection_is_monotonic() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1-dup"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 1);
        assert!(inj.next_barrier().is_none());
        inj.enqueue(make_checkpoint_barrier("job", 2, "cp-2"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 2);
    }

    #[test]
    fn barrier_injector_new_is_empty() {
        let mut inj = BarrierInjector::new();
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn barrier_injector_enqueue_multiple_epochs() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        inj.enqueue(make_checkpoint_barrier("job", 2, "cp-2"));
        inj.enqueue(make_checkpoint_barrier("job", 3, "cp-3"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 1);
        assert_eq!(inj.next_barrier().unwrap().epoch, 2);
        assert_eq!(inj.next_barrier().unwrap().epoch, 3);
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn barrier_injector_rejects_stale_epoch() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 5, "cp-5"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 5);
        // Epoch 3 is before 5, should be rejected
        inj.enqueue(make_checkpoint_barrier("job", 3, "cp-3"));
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn barrier_injector_rejects_equal_epoch() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 5, "cp-5"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 5);
        // Same epoch 5 should be rejected (epoch <= last_injected_epoch)
        inj.enqueue(make_checkpoint_barrier("job", 5, "cp-5-dup"));
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn make_checkpoint_barrier_fields() {
        let barrier = make_checkpoint_barrier("job-1", 42, "cp-42");
        assert_eq!(barrier.epoch, 42);
        assert_eq!(barrier.job_id, "job-1");
        assert_eq!(barrier.checkpoint_id, "cp-42");
        assert_eq!(barrier.barrier_kind, BarrierKind::Checkpoint as i32);
        assert!(barrier.timestamp_ms > 0);
    }

    #[test]
    fn operator_epoch_returns_barrier_epoch() {
        let barrier = make_checkpoint_barrier("job", 7, "cp-7");
        assert_eq!(BarrierInjector::operator_epoch(&barrier), 7);
    }

    #[test]
    fn shared_barrier_injector_new_is_empty() {
        let inj = SharedBarrierInjector::new();
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn shared_barrier_injector_enqueue_and_dequeue() {
        let inj = SharedBarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        let barrier = inj.next_barrier().unwrap();
        assert_eq!(barrier.epoch, 1);
        assert!(inj.next_barrier().is_none());
    }

    #[test]
    fn shared_barrier_injector_is_clone() {
        let inj1 = SharedBarrierInjector::new();
        let inj2 = inj1.clone();
        inj1.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        let barrier = inj2.next_barrier().unwrap();
        assert_eq!(barrier.epoch, 1);
    }

    #[test]
    fn shared_barrier_injector_monotonic() {
        let inj = SharedBarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1-dup"));
        inj.enqueue(make_checkpoint_barrier("job", 2, "cp-2"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 1);
        assert!(inj.next_barrier().is_none());
        assert_eq!(inj.next_barrier().unwrap().epoch, 2);
    }
}
