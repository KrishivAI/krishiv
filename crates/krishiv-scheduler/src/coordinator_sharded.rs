//! Inner locks (ExecutorInner, CheckpointInner) are the long-term primary source
//! of truth for executor registry and checkpoint coordinator state. The outer
//! Coordinator maintains a snapshot view for convenience. The dual sync dance
//! is transitional; hot paths should migrate to direct inner access + Notify
//! signaling to eliminate block_on and reduce lock contention.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checkpoint::CheckpointCoordinator;
use crate::heartbeat::ExecutorRegistry;
use krishiv_proto::{CheckpointAckRequest, CheckpointAckResponse, CoordinatorState, ExecutorId};
use tokio::sync::Notify;

/// Executor-facing state guarded by a dedicated `RwLock`.
#[derive(Clone, Debug)]
pub struct ExecutorInner {
    pub executors: ExecutorRegistry,
    pub state: CoordinatorState,
    pub ticks_since_restart: u64,
    pub recovering: bool,
    /// Notify used to wake waiters when executor or state changes occur.
    /// Enables future removal of periodic block_on-based sync.
    pub notify: Arc<Notify>,
}

impl ExecutorInner {
    /// Handle a heartbeat on the executor inner state — updates the registry
    /// and returns the new lease generation.
    pub fn handle_heartbeat(
        &mut self,
        heartbeat: krishiv_proto::ExecutorHeartbeat,
    ) -> Result<krishiv_proto::LeaseGeneration, crate::SchedulerError> {
        let executor_id = heartbeat.executor_id().clone();
        let fallback_lease = heartbeat.lease_generation();
        self.executors.heartbeat(heartbeat)?;
        let lease_generation = self
            .executors
            .find_executor(&executor_id)
            .map(|e| e.lease_generation())
            .unwrap_or(fallback_lease);
        self.notify.notify_waiters();
        Ok(lease_generation)
    }
}

/// Checkpoint-facing state guarded by a dedicated `RwLock`.
#[derive(Clone, Debug)]
pub struct CheckpointInner {
    pub coordinators: HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    pub notify_sent: HashSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    pub barrier_sent: HashSet<(krishiv_proto::JobId, u64)>,
    /// Notify for checkpoint-related state changes (acks, epoch advances).
    pub notify: Arc<Notify>,
}

impl CheckpointInner {
    pub fn new() -> Self {
        Self {
            coordinators: HashMap::new(),
            notify_sent: HashSet::new(),
            barrier_sent: HashSet::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Handle a checkpoint ack directly on the inner state, bypassing the full
    /// coordinator lock.
    pub fn handle_ack(&mut self, ack: CheckpointAckRequest) -> CheckpointAckResponse {
        let job_id = ack.job_id.clone();
        let res = match self.coordinators.get_mut(&job_id) {
            None => CheckpointAckResponse::JobNotFound,
            Some(coord) => {
                let coordinator_token = coord.fencing_token();
                if ack.fencing_token.as_u64() != coordinator_token.as_u64() {
                    return CheckpointAckResponse::StaleFencingToken {
                        current_token: coordinator_token.as_u64(),
                    };
                }
                let current_epoch = coord.current_epoch();
                match coord.receive_ack(ack.clone()) {
                    Ok(true) => {
                        self.clear_notify_for_epoch(&job_id, ack.epoch);
                        // Metrics: caller may track CHECKPOINT_EPOCHS_TOTAL externally
                        CheckpointAckResponse::Accepted
                    }
                    Ok(false) => CheckpointAckResponse::Accepted,
                    Err(_) => CheckpointAckResponse::StaleEpoch { current_epoch },
                }
            }
        };
        self.notify.notify_waiters();
        res
    }

    fn clear_notify_for_epoch(&mut self, job_id: &krishiv_proto::JobId, epoch: u64) {
        self.notify_sent
            .retain(|(jid, _, e)| jid != job_id || *e != epoch);
        self.barrier_sent
            .retain(|(jid, e)| jid != job_id || *e != epoch);
    }
}

/// The sync helper functions below are transitional. Hot paths should prefer
/// the bypass fast-path methods on SharedCoordinator that operate directly on
/// the inner locks. The long-term goal is for ExecutorInner/CheckpointInner
/// (plus Notify) to be the sole source of truth.

// ── Executor sync helpers (G3) ──────────────────────────────────────────────

/// Synchronise executor state FROM the Coordinator fields INTO the inner lock.
/// Call after any coordinator mutation that modifies the executor registry
/// (register, deregister, advance_heartbeat_clock) so the inner lock's hot-path
/// readers see consistent state.
pub(crate) fn sync_executor_to_inner(
    src_executors: &ExecutorRegistry,
    src_state: CoordinatorState,
    src_ticks: u64,
    src_recovering: bool,
    inner: &mut ExecutorInner,
) {
    inner.executors.clone_from(src_executors);
    inner.state = src_state;
    inner.ticks_since_restart = src_ticks;
    inner.recovering = src_recovering;
}

// ── Checkpoint sync helpers (G3) ───────────────────────────────────────────

/// Synchronise checkpoint state FROM the Coordinator fields INTO the inner lock.
pub(crate) fn sync_checkpoint_to_inner(
    src_coordinators: &HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    src_notify: &HashSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    src_barrier: &HashSet<(krishiv_proto::JobId, u64)>,
    inner: &mut CheckpointInner,
) {
    inner.coordinators.clone_from(src_coordinators);
    inner.notify_sent.clone_from(src_notify);
    inner.barrier_sent.clone_from(src_barrier);
}
