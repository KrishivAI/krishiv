//! gRPC checkpoint barrier injection (R16 S1.2).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use krishiv_proto::KeyGroupRange;
use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};
use tokio::sync::oneshot;

/// Checkpoint metadata returned after a barrier-driven checkpoint completes.
#[derive(Debug, Clone)]
pub struct BarrierAckCompletion {
    pub checkpoint_uri: String,
    pub key_group_range_start: u32,
    pub key_group_range_end: u32,
}

type BarrierWaitersMap = DashMap<(String, u64), Vec<oneshot::Sender<BarrierAckCompletion>>>;

/// Waits for barrier checkpoint completion before gRPC acks are sent.
#[derive(Clone, Default)]
pub struct SharedBarrierAckRegistry {
    waiters: Arc<BarrierWaitersMap>,
}

impl SharedBarrierAckRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a waiter for `(job_id, epoch)` barrier completion.
    pub fn register_wait(
        &self,
        job_id: &str,
        epoch: u64,
    ) -> oneshot::Receiver<BarrierAckCompletion> {
        let (tx, rx) = oneshot::channel();
        self.waiters
            .entry((job_id.to_owned(), epoch))
            .or_default()
            .push(tx);
        rx
    }

    /// Signal completion for all waiters on a barrier epoch.
    pub fn complete(&self, job_id: &str, epoch: u64, completion: BarrierAckCompletion) {
        if let Some((_, senders)) = self.waiters.remove(&(job_id.to_owned(), epoch)) {
            for tx in senders {
                let _ = tx.send(completion.clone());
            }
        }
    }
}

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

    /// Pop the next injectable barrier, discarding any stale or duplicate
    /// epochs ahead of it.
    ///
    /// # Why this is a loop
    ///
    /// It used to pop one barrier and, if that one was stale, `return None` —
    /// having already removed it. `None` is the caller's signal for "no barrier
    /// is available", so a single duplicate at the head of the queue hid a
    /// perfectly good barrier sitting directly behind it until the caller's
    /// next poll, and a run of `n` duplicates cost `n` polls. The coordinator
    /// is meanwhile waiting on an ack with a 120 s deadline.
    ///
    /// Duplicates are not hypothetical: the coordinator re-sends a barrier when
    /// an ack is slow, which is exactly when the extra delay is least
    /// affordable. `None` now means the queue holds nothing injectable, which
    /// is what every caller already assumed it meant.
    pub fn next_barrier(&mut self) -> Option<CheckpointBarrier> {
        while let Some(next) = self.pending.pop_front() {
            if next.epoch > self.last_injected_epoch {
                self.last_injected_epoch = next.epoch;
                return Some(next);
            }
            // Stale or duplicate epoch: drop it and keep looking rather than
            // reporting the whole queue empty.
        }
        None
    }
}

/// Abstraction over any barrier source so downstream operators can consume barriers
/// without being coupled to `SharedBarrierInjector` directly.
pub trait BarrierSource: Send + Sync {
    fn next_barrier(&self) -> Option<CheckpointBarrier>;
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

impl BarrierSource for SharedBarrierInjector {
    fn next_barrier(&self) -> Option<CheckpointBarrier> {
        SharedBarrierInjector::next_barrier(self)
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

    /// A duplicate must not hide the barrier queued behind it.
    ///
    /// This asserted the opposite: after `[1, 1-dup, 2]` it expected epoch 1,
    /// then `None`, then epoch 2 — encoding the bug, because `None` is the
    /// caller's "nothing to inject" signal and epoch 2 was already sitting in
    /// the queue. A run of duplicates cost one poll each while the coordinator
    /// waited on the ack.
    #[test]
    fn a_duplicate_does_not_hide_the_barrier_behind_it() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1"));
        inj.enqueue(make_checkpoint_barrier("job", 1, "cp-1-dup"));
        inj.enqueue(make_checkpoint_barrier("job", 2, "cp-2"));

        assert_eq!(inj.next_barrier().unwrap().epoch, 1);
        assert_eq!(
            inj.next_barrier().unwrap().epoch,
            2,
            "the duplicate must be skipped, not reported as an empty queue"
        );
        assert!(inj.next_barrier().is_none(), "now it really is empty");
    }

    /// Several stale epochs in a row are all skipped in one call.
    #[test]
    fn a_run_of_stale_epochs_is_skipped_in_one_poll() {
        let mut inj = BarrierInjector::new();
        inj.enqueue(make_checkpoint_barrier("job", 9, "cp-9"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 9);
        for stale in [3, 4, 9, 2] {
            inj.enqueue(make_checkpoint_barrier("job", stale, "stale"));
        }
        inj.enqueue(make_checkpoint_barrier("job", 10, "cp-10"));
        assert_eq!(inj.next_barrier().unwrap().epoch, 10);
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
        assert_eq!(inj.next_barrier().unwrap().epoch, 2);
        assert!(inj.next_barrier().is_none());
    }
}
