//! Checkpoint barrier simulation (dev/test only): `BarrierSimulator` and `BarrierSnapshot`.

use std::collections::VecDeque;
use std::sync::Mutex;

use krishiv_proto::wire::v1::CheckpointBarrier;

use crate::barrier_transport::BarrierSource;
use crate::{ExecutorError, ExecutorResult};

const MAX_SIMULATED_SNAPSHOTS: usize = 1000;

/// Checkpoint-barrier simulation for dev/test use.
///
/// Production checkpointing uses [`crate::runner::TaskRunner::handle_initiate_checkpoint`]
/// together with [`crate::BarrierInjector`] and the gRPC barrier service.
pub(crate) struct BarrierSimulator {
    last_committed_epoch: u64,
    simulated_snapshots: Vec<BarrierSnapshot>,
    pending: Mutex<VecDeque<CheckpointBarrier>>,
}

/// Metadata logged for each simulated checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BarrierSnapshot {
    pub epoch: u64,
    /// Watermark at the time the barrier was processed.
    pub watermark_ms: i64,
    /// Number of open window buckets at snapshot time.
    pub open_windows: usize,
}

impl Default for BarrierSimulator {
    fn default() -> Self {
        Self {
            last_committed_epoch: 0,
            simulated_snapshots: Vec::new(),
            pending: Mutex::new(VecDeque::new()),
        }
    }
}

impl BarrierSource for BarrierSimulator {
    fn next_barrier(&self) -> Option<CheckpointBarrier> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front()
    }
}

impl BarrierSimulator {
    /// Create a new barrier simulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a barrier to be returned by [`BarrierSource::next_barrier`].
    pub fn queue_barrier(&self, barrier: CheckpointBarrier) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(barrier);
    }

    /// Process a checkpoint barrier for `epoch`.
    ///
    /// The caller must supply the current `watermark_ms` and `open_windows`
    /// count from the operator that is being snapshotted.  Returns `Ok(())`
    /// if the barrier is accepted and logged; `Err` if the epoch is stale.
    pub fn process_barrier(
        &mut self,
        epoch: u64,
        watermark_ms: i64,
        open_windows: usize,
    ) -> ExecutorResult<()> {
        if epoch <= self.last_committed_epoch {
            return Err(ExecutorError::InvalidAssignment {
                message: format!(
                    "stale barrier epoch {epoch}; last committed epoch is \
                     {}",
                    self.last_committed_epoch
                ),
            });
        }
        self.simulated_snapshots.push(BarrierSnapshot {
            epoch,
            watermark_ms,
            open_windows,
        });
        if self.simulated_snapshots.len() > MAX_SIMULATED_SNAPSHOTS {
            let excess = self.simulated_snapshots.len() - MAX_SIMULATED_SNAPSHOTS;
            self.simulated_snapshots.drain(0..excess);
        }
        self.last_committed_epoch = epoch;
        Ok(())
    }

    /// All snapshots logged so far, in epoch order.
    pub fn snapshots(&self) -> &[BarrierSnapshot] {
        &self.simulated_snapshots
    }

    /// Most recently committed epoch.
    pub fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_simulator_new_is_empty() {
        let sim = BarrierSimulator::new();
        assert_eq!(sim.last_committed_epoch(), 0);
        assert!(sim.snapshots().is_empty());
    }

    #[test]
    fn process_barrier_first_epoch() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 1000, 2).unwrap();
        assert_eq!(sim.last_committed_epoch(), 1);
        assert_eq!(sim.snapshots().len(), 1);
        assert_eq!(sim.snapshots()[0].epoch, 1);
        assert_eq!(sim.snapshots()[0].watermark_ms, 1000);
        assert_eq!(sim.snapshots()[0].open_windows, 2);
    }

    #[test]
    fn process_barrier_monotonic_epochs() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 100, 0).unwrap();
        sim.process_barrier(2, 200, 1).unwrap();
        sim.process_barrier(3, 300, 2).unwrap();
        assert_eq!(sim.last_committed_epoch(), 3);
        assert_eq!(sim.snapshots().len(), 3);
    }

    #[test]
    fn process_barrier_rejects_stale_epoch() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(5, 100, 0).unwrap();
        let err = sim.process_barrier(3, 200, 0).unwrap_err();
        assert!(err.to_string().contains("stale barrier epoch 3"));
        assert_eq!(sim.last_committed_epoch(), 5);
    }

    #[test]
    fn process_barrier_rejects_equal_epoch() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(5, 100, 0).unwrap();
        let err = sim.process_barrier(5, 200, 0).unwrap_err();
        assert!(err.to_string().contains("stale barrier epoch 5"));
    }

    #[test]
    fn process_barrier_with_zero_watermark() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 0, 0).unwrap();
        assert_eq!(sim.snapshots()[0].watermark_ms, 0);
    }

    #[test]
    fn process_barrier_with_zero_open_windows() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 100, 0).unwrap();
        assert_eq!(sim.snapshots()[0].open_windows, 0);
    }

    #[test]
    fn process_barrier_with_large_watermark() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, i64::MAX, 100).unwrap();
        assert_eq!(sim.snapshots()[0].watermark_ms, i64::MAX);
    }

    #[test]
    fn barrier_snapshot_equality() {
        let s1 = BarrierSnapshot {
            epoch: 1,
            watermark_ms: 100,
            open_windows: 2,
        };
        let s2 = BarrierSnapshot {
            epoch: 1,
            watermark_ms: 100,
            open_windows: 2,
        };
        assert_eq!(s1, s2);
    }

    #[test]
    fn barrier_snapshot_clone() {
        let s1 = BarrierSnapshot {
            epoch: 1,
            watermark_ms: 100,
            open_windows: 2,
        };
        let s2 = s1.clone();
        assert_eq!(s1, s2);
    }

    #[test]
    fn barrier_snapshot_debug() {
        let s = BarrierSnapshot {
            epoch: 1,
            watermark_ms: 100,
            open_windows: 2,
        };
        let debug = format!("{:?}", s);
        assert!(debug.contains("epoch: 1"));
        assert!(debug.contains("watermark_ms: 100"));
    }
}
