//! gRPC checkpoint barrier injection (R16 S1.2).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

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
        self.inner.lock().unwrap().enqueue(barrier);
    }

    pub fn next_barrier(&self) -> Option<CheckpointBarrier> {
        self.inner.lock().unwrap().next_barrier()
    }
}

/// Build a checkpoint barrier message.
pub fn make_checkpoint_barrier(job_id: &str, epoch: u64, checkpoint_id: &str) -> CheckpointBarrier {
    CheckpointBarrier {
        epoch,
        job_id: job_id.to_string(),
        checkpoint_id: checkpoint_id.to_string(),
        barrier_kind: BarrierKind::Checkpoint as i32,
        timestamp_ms: krishiv_async_util::unix_now_ms(),
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
}
