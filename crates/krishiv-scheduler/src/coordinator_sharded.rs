//! Inner locks (ExecutorInner, CheckpointInner) are the long-term primary source
//! of truth for executor registry and checkpoint coordinator state.
//!
//! The outer Coordinator maintains a snapshot for synchronous tick paths
//! (advance_heartbeat_tick). The sync dance copies state in both directions:
//!   outer → inner: after sync tick mutations
//!   inner → outer: after gRPC/in-process handler mutations
//!
//! Full elimination of the sync dance requires making all outer Coordinator
//! methods that access executor/checkpoint state read from the inner locks
//! directly — deferred to avoid a larger refactor.

use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checkpoint::{CheckpointCoordinator, CheckpointCoordinatorState, PendingCommit};
use crate::heartbeat::ExecutorRegistry;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorState, ExecutorId, JobId,
};
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
    pub fn register_executor(
        &mut self,
        descriptor: krishiv_proto::ExecutorDescriptor,
    ) -> Result<krishiv_proto::LeaseGeneration, crate::SchedulerError> {
        let res = self.executors.register(descriptor);
        if res.is_ok() {
            self.notify.notify_waiters();
        }
        res
    }

    pub fn deregister_executor(
        &mut self,
        executor_id: &ExecutorId,
        lease_generation: krishiv_proto::LeaseGeneration,
    ) -> Result<krishiv_proto::LeaseGeneration, crate::SchedulerError> {
        let res = self.executors.deregister(executor_id, lease_generation);
        if res.is_ok() {
            self.notify.notify_waiters();
        }
        res
    }

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
#[derive(Clone, Debug, Default)]
pub struct CheckpointInner {
    pub coordinators: HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    pub notify_sent: IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    pub barrier_sent: HashSet<(krishiv_proto::JobId, u64)>,
    /// Notify for checkpoint-related state changes (acks, epoch advances).
    pub notify: Arc<Notify>,
}

impl CheckpointInner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_parts(
        coordinators: HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
        notify_sent: IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
        barrier_sent: HashSet<(krishiv_proto::JobId, u64)>,
    ) -> Self {
        Self {
            coordinators,
            notify_sent,
            barrier_sent,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Handle a checkpoint ack with the async 3-phase protocol.
    ///
    /// Phase 1 (under lock): extract commit data in-memory.
    /// Phase 2 (outside lock): caller performs async storage I/O.
    /// Phase 3 (under lock): caller calls [`Self::finalize_ack`].
    ///
    /// Returns `(response, Option<PendingCommit>)` — if `Some(commit)`,
    /// the caller must write storage and then call `finalize_ack`.
    pub async fn handle_ack(
        &mut self,
        ack: CheckpointAckRequest,
    ) -> (CheckpointAckResponse, Option<PendingCommit>) {
        let job_id = ack.job_id.clone();
        let ack_epoch = ack.epoch;

        let result = match self.coordinators.get_mut(&job_id) {
            None => (CheckpointAckResponse::JobNotFound, None),
            Some(coord) => {
                if ack.fencing_token.as_u64() != coord.fencing_token().as_u64() {
                    return (
                        CheckpointAckResponse::StaleFencingToken {
                            current_token: coord.fencing_token().as_u64(),
                        },
                        None,
                    );
                }
                match coord.receive_ack_async(ack.clone()).await {
                    Ok(true) => {
                        let commit = coord.take_pending_commit();
                        (CheckpointAckResponse::Accepted, commit)
                    }
                    Ok(false) => (CheckpointAckResponse::Accepted, None),
                    Err(_) => {
                        let current_epoch = coord.current_epoch();
                        (CheckpointAckResponse::StaleEpoch { current_epoch }, None)
                    }
                }
            }
        };

        if result.1.is_some() {
            self.clear_notify_for_epoch(&job_id, ack_epoch);
        }
        self.notify.notify_waiters();

        (result.0, result.1)
    }

    /// Phase 3: finalize a commit after storage I/O completes.
    /// Must be called under the same lock as `handle_ack`.
    pub fn finalize_ack(
        &mut self,
        job_id: &JobId,
        epoch: u64,
    ) -> krishiv_state::checkpoint::CheckpointResult<()> {
        let coord = self.coordinators.get_mut(job_id).ok_or_else(|| {
            krishiv_state::checkpoint::CheckpointError::Storage {
                message: format!(
                    "cannot finalize checkpoint epoch {epoch}: job {job_id} is not registered"
                ),
            }
        })?;
        coord.finalize_commit(epoch)?;
        krishiv_metrics::global_metrics().inc_checkpoint_committed(job_id.as_str());
        self.notify.notify_waiters();
        Ok(())
    }

    fn clear_notify_for_epoch(&mut self, job_id: &krishiv_proto::JobId, epoch: u64) {
        self.notify_sent
            .retain(|(jid, _, e)| jid != job_id || *e != epoch);
        self.barrier_sent
            .retain(|(jid, e)| jid != job_id || *e != epoch);
    }
}

// The sync helper functions below are transitional. Hot paths should prefer
// the bypass fast-path methods on SharedCoordinator that operate directly on
// the inner locks. The long-term goal is for ExecutorInner/CheckpointInner
// (plus Notify) to be the sole source of truth.

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
///
/// This is the outer→inner direction and is intentionally a *full replace*: the
/// outer `Coordinator` is authoritative for membership (job submit/evict) and
/// for deliberate backward moves (checkpoint/savepoint restore lowers the
/// epoch). The inner→outer direction instead uses [`merge_checkpoint_coordinator`]
/// so a late gRPC ack can never roll the outer copy backwards.
///
/// NOTE (C1): a full replace here can still momentarily clobber an inner
/// coordinator that is mid-commit (`Committing`) in the narrow window between
/// the gRPC ack's storage I/O and its finalize. That is bounded — at worst the
/// epoch retries — and is fully resolved only by collapsing the two copies into
/// a single source of truth (see `docs/implementation/status.md`).
pub(crate) fn sync_checkpoint_to_inner(
    src_coordinators: &HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    src_notify: &IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    src_barrier: &HashSet<(krishiv_proto::JobId, u64)>,
    inner: &mut CheckpointInner,
) {
    inner.coordinators.clone_from(src_coordinators);
    inner.notify_sent.clone_from(src_notify);
    inner.barrier_sent.clone_from(src_barrier);
}

/// Membership-aware, monotonic outer→inner checkpoint sync (C1 residual 1).
///
/// Used by the *periodic* sync path (`advance_heartbeat_tick`), which fires on a
/// fixed cadence regardless of checkpoint progress and must NOT clobber an inner
/// coordinator that a concurrent ack has already advanced further (e.g. mid
/// `Committing`, in the window between a gRPC/barrier ack's storage I/O and its
/// finalize). Unlike the full-replace [`sync_checkpoint_to_inner`] (which the
/// restore path needs because restore deliberately lowers the epoch), this:
///
///   * adds jobs the outer has but the inner lacks (job submit while the inner
///     is the live quorum owner),
///   * removes jobs the outer no longer has (job evict/cancel),
///   * for jobs in both, overwrites the inner entry *only* when the outer copy
///     is at least as advanced by `(epoch, state_rank)` — never regressing an
///     in-flight inner epoch.
///
/// `notify_sent` / `barrier_sent` are intentionally NOT replaced here: in the
/// single-owner ack model the inner lock is authoritative for per-epoch
/// delivery tracking, and a full replace would resurrect entries the inner
/// cleared on quorum. Membership eviction is handled below.
pub(crate) fn sync_checkpoint_to_inner_monotonic(
    src_coordinators: &HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    inner: &mut CheckpointInner,
) {
    // Drop jobs the outer no longer tracks (cancel/evict). Reference only
    // `src_coordinators` in every closure so the borrows stay disjoint.
    inner
        .coordinators
        .retain(|job_id, _| src_coordinators.contains_key(job_id));
    inner
        .notify_sent
        .retain(|(jid, _, _)| src_coordinators.contains_key(jid));
    inner
        .barrier_sent
        .retain(|(jid, _)| src_coordinators.contains_key(jid));

    // Forward-merge each outer job into the inner without regressing in-flight
    // inner epochs.
    for (job_id, src) in src_coordinators {
        merge_checkpoint_coordinator(&mut inner.coordinators, job_id, src.clone());
    }
}

/// Lexicographic checkpoint progress key: `(current_epoch, state_rank)`.
///
/// `state_rank` orders the per-epoch lifecycle so a forward-merge never
/// regresses a coordinator: pre-quorum (`AwaitingAcks`) < `Committing` <
/// terminal. `Failed` and `Committed` share the top rank — both are terminal
/// for the epoch, so a same-epoch tie keeps the destination (the copy that
/// reached a terminal state first wins; we never flip a committed epoch to
/// failed or vice versa).
fn checkpoint_progress(coord: &CheckpointCoordinator) -> (u64, u8) {
    let rank = match coord.coordinator_state() {
        CheckpointCoordinatorState::Idle => 0,
        CheckpointCoordinatorState::AwaitingAcks { .. } => 1,
        CheckpointCoordinatorState::Committing { .. } => 2,
        CheckpointCoordinatorState::Failed { .. }
        | CheckpointCoordinatorState::Committed { .. } => 3,
    };
    (coord.current_epoch(), rank)
}

/// Forward-merge one job's checkpoint coordinator from one dual-state copy into
/// the other (C1, inner→outer direction).
///
/// Applies `src` only when it is strictly more advanced than the existing `dst`
/// entry (by `(epoch, state_rank)`), or when `dst` has no entry. This keeps the
/// inner→outer sync from (a) clobbering *other* jobs' in-flight epochs — it
/// touches only `job_id` — and (b) rolling *this* job back past a newer epoch
/// the outer copy already advanced to (e.g. a late `finalize_ack` for epoch N
/// arriving after the barrier path already initiated epoch N+1).
pub(crate) fn merge_checkpoint_coordinator(
    dst: &mut HashMap<JobId, CheckpointCoordinator>,
    job_id: &JobId,
    src: CheckpointCoordinator,
) {
    match dst.get(job_id) {
        Some(existing) if checkpoint_progress(existing) >= checkpoint_progress(&src) => {
            // dst is at least as advanced — keep it (never regress).
        }
        _ => {
            dst.insert(job_id.clone(), src);
        }
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;
    use std::sync::Arc;

    fn coord(job: &JobId, epoch: u64, state: CheckpointCoordinatorState) -> CheckpointCoordinator {
        let storage: Arc<dyn krishiv_state::checkpoint::CheckpointStorage> =
            Arc::new(krishiv_state::checkpoint::LocalFsCheckpointStorage::ephemeral().unwrap());
        let mut c = CheckpointCoordinator::new_for_test(job.clone(), storage, 1_000, 1);
        c.current_epoch = epoch;
        c.state = state;
        c
    }

    #[test]
    fn merge_inserts_when_absent_then_applies_when_ahead() {
        let job = JobId::try_new("m1").unwrap();
        let mut dst = HashMap::new();
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(&job, 1, CheckpointCoordinatorState::Committed { epoch: 1 }),
        );
        assert_eq!(dst[&job].current_epoch(), 1);
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(&job, 2, CheckpointCoordinatorState::Committed { epoch: 2 }),
        );
        assert_eq!(dst[&job].current_epoch(), 2);
    }

    #[test]
    fn merge_does_not_regress_to_older_epoch() {
        // A late finalize for epoch 5 must not clobber an outer copy that
        // already advanced to epoch 6.
        let job = JobId::try_new("m2").unwrap();
        let mut dst = HashMap::new();
        dst.insert(
            job.clone(),
            coord(
                &job,
                6,
                CheckpointCoordinatorState::AwaitingAcks {
                    epoch: 6,
                    initiated_at_ms: 0,
                },
            ),
        );
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(&job, 5, CheckpointCoordinatorState::Committed { epoch: 5 }),
        );
        assert_eq!(dst[&job].current_epoch(), 6);
        assert!(matches!(
            dst[&job].coordinator_state(),
            CheckpointCoordinatorState::AwaitingAcks { epoch: 6, .. }
        ));
    }

    #[test]
    fn merge_applies_commit_over_awaiting_same_epoch() {
        let job = JobId::try_new("m3").unwrap();
        let mut dst = HashMap::new();
        dst.insert(
            job.clone(),
            coord(
                &job,
                3,
                CheckpointCoordinatorState::AwaitingAcks {
                    epoch: 3,
                    initiated_at_ms: 0,
                },
            ),
        );
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(&job, 3, CheckpointCoordinatorState::Committed { epoch: 3 }),
        );
        assert!(matches!(
            dst[&job].coordinator_state(),
            CheckpointCoordinatorState::Committed { epoch: 3 }
        ));
    }

    #[test]
    fn merge_does_not_regress_committing_to_awaiting_same_epoch() {
        // R1 protection: an outer AwaitingAcks{N} must not clobber a more
        // advanced Committing{N} that another path is mid-finalizing.
        let job = JobId::try_new("m4").unwrap();
        let mut dst = HashMap::new();
        dst.insert(
            job.clone(),
            coord(&job, 4, CheckpointCoordinatorState::Committing { epoch: 4 }),
        );
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(
                &job,
                4,
                CheckpointCoordinatorState::AwaitingAcks {
                    epoch: 4,
                    initiated_at_ms: 0,
                },
            ),
        );
        assert!(matches!(
            dst[&job].coordinator_state(),
            CheckpointCoordinatorState::Committing { epoch: 4 }
        ));
    }

    #[test]
    fn merge_keeps_committed_on_terminal_tie() {
        // A committed epoch must never be flipped to Failed by the other copy.
        let job = JobId::try_new("m5").unwrap();
        let mut dst = HashMap::new();
        dst.insert(
            job.clone(),
            coord(&job, 2, CheckpointCoordinatorState::Committed { epoch: 2 }),
        );
        merge_checkpoint_coordinator(
            &mut dst,
            &job,
            coord(
                &job,
                2,
                CheckpointCoordinatorState::Failed {
                    epoch: 2,
                    reason: "storage".into(),
                },
            ),
        );
        assert!(matches!(
            dst[&job].coordinator_state(),
            CheckpointCoordinatorState::Committed { epoch: 2 }
        ));
    }
}
