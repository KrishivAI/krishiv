//! `ExecutorInner` and `CheckpointInner` are the single source of truth for
//! executor-registry and checkpoint-coordinator state respectively.
//!
//! `Coordinator` now embeds these structs directly as `exec: ExecutorInner`
//! and `ckpt: CheckpointInner`.  `SharedCoordinator` clones them into dedicated
//! `RwLock`s (`executor_inner`, `checkpoint_inner`) so hot-path callers
//! (heartbeat processing, checkpoint acks) can bypass the full coordinator lock.
//!
//! Sync direction (outer→inner): after mutating `Coordinator.exec` or `.ckpt`,
//! callers use `executor_inner.clone_from(&coord.exec)` (executor registry),
//! `ckpt_inner.apply_monotonic_from(&coord.ckpt)` (periodic/submit paths), or
//! `ckpt_inner.replace_data_from(&coord.ckpt)` (restore path, full replace).
//! Inner→outer syncing uses `Coordinator::apply_checkpoint_inner_sync`, called
//! from the in-process ack path. The gRPC ack path uses `merge_checkpoint_coordinator`
//! for per-job monotonic merges without touching other jobs.

use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checkpoint::{CheckpointCoordinator, CheckpointCoordinatorState, PendingCommit};
use crate::coordinator::RestoreDirective;
use crate::heartbeat::ExecutorRegistry;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorState, ExecutorId, JobId,
};
use tokio::sync::Notify;

/// Maximum entries in a checkpoint sent-tracking set before old entries are evicted.
const MAX_CHECKPOINT_NOTIFY_ENTRIES: usize = 10_000;

fn prune_sent_set(set: &mut IndexSet<(JobId, ExecutorId, u64)>) {
    while set.len() > MAX_CHECKPOINT_NOTIFY_ENTRIES {
        let Some(oldest) = set.get_index(0).cloned() else {
            break;
        };
        set.shift_remove(&oldest);
    }
}

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
///
/// This is the authoritative source for checkpoint-control state. All checkpoint
/// ack processing, restore directives, and completion notifications are managed here.
#[derive(Clone, Debug, Default)]
pub struct CheckpointInner {
    pub coordinators: HashMap<krishiv_proto::JobId, CheckpointCoordinator>,
    pub notify_sent: IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    pub barrier_sent: HashSet<(krishiv_proto::JobId, u64)>,
    /// (job_id, executor_id, epoch) triples for which a checkpoint-complete
    /// notification (transactional-sink commit signal) was already delivered.
    pub checkpoint_complete_sent: IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    /// Active restore directives per job: every executor with tasks in the job
    /// must reload state/offsets from the directive's epoch (global rollback).
    pub restore_directives: HashMap<krishiv_proto::JobId, RestoreDirective>,
    /// (job_id, executor_id, epoch) triples for which a restore directive was
    /// already delivered.
    pub restore_notify_sent: IndexSet<(krishiv_proto::JobId, ExecutorId, u64)>,
    /// Jobs that must be cancelled once their savepoint epoch commits.
    pub pending_stop_after_savepoint: HashMap<krishiv_proto::JobId, u64>,
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
            checkpoint_complete_sent: IndexSet::new(),
            restore_directives: HashMap::new(),
            restore_notify_sent: IndexSet::new(),
            pending_stop_after_savepoint: HashMap::new(),
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

    /// Record a restore directive for a job and reset delivery tracking.
    pub fn set_restore_directive(&mut self, job_id: &JobId, directive: RestoreDirective) {
        self.restore_directives.insert(job_id.clone(), directive);
        self.restore_notify_sent.retain(|(jid, _, _)| jid != job_id);
        // Re-deliver completion signal: an executor that restored may hold
        // prepared transactions that must be committed.
        self.checkpoint_complete_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.notify.notify_waiters();
    }

    /// Active restore directive for a job, if any.
    pub fn restore_directive(&self, job_id: &JobId) -> Option<RestoreDirective> {
        self.restore_directives.get(job_id).cloned()
    }

    /// Checkpoint-complete notifications to deliver to `executor_id`.
    ///
    /// `is_running` reports whether `executor_id` owns a running task in the job
    /// (supplied by the caller from `job_coordinators`).
    pub fn pending_checkpoint_complete_for_executor(
        &mut self,
        executor_id: &ExecutorId,
        mut is_running: impl FnMut(&JobId) -> bool,
    ) -> Vec<krishiv_proto::CheckpointCompleteCommand> {
        let mut out = Vec::new();
        for (job_id, coord) in &self.coordinators {
            let Some(epoch) = coord.committed_epoch() else {
                continue;
            };
            if epoch == 0 || !is_running(job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), epoch);
            if self.checkpoint_complete_sent.contains(&key) {
                continue;
            }
            out.push(krishiv_proto::CheckpointCompleteCommand {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token(),
            });
            self.checkpoint_complete_sent.insert(key);
            prune_sent_set(&mut self.checkpoint_complete_sent);
        }
        out
    }

    /// Restore directives to deliver to `executor_id`.
    ///
    /// `is_active` reports whether `executor_id` owns a running or assigned task
    /// in the job (supplied by the caller from `job_coordinators`).
    pub fn pending_restore_commands_for_executor(
        &mut self,
        executor_id: &ExecutorId,
        mut is_active: impl FnMut(&JobId) -> bool,
    ) -> Vec<krishiv_proto::RestoreFromCheckpointCommand> {
        let mut out = Vec::new();
        let directives: Vec<(JobId, RestoreDirective)> = self
            .restore_directives
            .iter()
            .map(|(job_id, directive)| (job_id.clone(), directive.clone()))
            .collect();
        for (job_id, directive) in directives {
            if !is_active(&job_id) {
                continue;
            }
            let key = (job_id.clone(), executor_id.clone(), directive.epoch);
            if self.restore_notify_sent.contains(&key) {
                continue;
            }
            let Ok(fencing_token) = krishiv_proto::FencingToken::try_new(directive.fencing_token)
            else {
                tracing::error!(
                    job_id = %job_id,
                    epoch = directive.epoch,
                    "restore directive carries a zero fencing token; dropping"
                );
                continue;
            };
            out.push(krishiv_proto::RestoreFromCheckpointCommand {
                job_id: job_id.clone(),
                epoch: directive.epoch,
                fencing_token,
                sink_commit: directive.sink_commit.clone(),
                sink_abort: directive.sink_abort.clone(),
            });
            self.restore_notify_sent.insert(key);
            prune_sent_set(&mut self.restore_notify_sent);
        }
        out
    }

    /// Full replace of all 7 data fields from `src`, preserving `self.notify`.
    ///
    /// Use on the restore path where a deliberate backward epoch move is correct.
    pub fn replace_data_from(&mut self, src: &CheckpointInner) {
        self.coordinators.clone_from(&src.coordinators);
        self.notify_sent.clone_from(&src.notify_sent);
        self.barrier_sent.clone_from(&src.barrier_sent);
        self.checkpoint_complete_sent
            .clone_from(&src.checkpoint_complete_sent);
        self.restore_directives.clone_from(&src.restore_directives);
        self.restore_notify_sent
            .clone_from(&src.restore_notify_sent);
        self.pending_stop_after_savepoint
            .clone_from(&src.pending_stop_after_savepoint);
    }

    /// Monotonic merge from `src` into `self`.
    ///
    /// - Drops jobs absent in `src` (cancel/evict propagates).
    /// - Forward-merges per-job coordinators without regressing in-flight epochs (C1).
    /// - Full-replaces the 4 delivery-tracking fields (no monotonic invariant there).
    ///
    /// Use on the periodic tick and submit_job paths.
    pub fn apply_monotonic_from(&mut self, src: &CheckpointInner) {
        self.coordinators
            .retain(|job_id, _| src.coordinators.contains_key(job_id));
        self.notify_sent
            .retain(|(jid, _, _)| src.coordinators.contains_key(jid));
        self.barrier_sent
            .retain(|(jid, _)| src.coordinators.contains_key(jid));
        for (job_id, src_coord) in &src.coordinators {
            merge_checkpoint_coordinator(&mut self.coordinators, job_id, src_coord.clone());
        }
        self.checkpoint_complete_sent
            .clone_from(&src.checkpoint_complete_sent);
        self.restore_directives.clone_from(&src.restore_directives);
        self.restore_notify_sent
            .clone_from(&src.restore_notify_sent);
        self.pending_stop_after_savepoint
            .clone_from(&src.pending_stop_after_savepoint);
    }

    /// Remove all checkpoint-control state for a completed/cancelled job.
    pub fn clear_job(&mut self, job_id: &JobId) {
        self.coordinators.remove(job_id);
        self.restore_directives.remove(job_id);
        self.pending_stop_after_savepoint.remove(job_id);
        self.restore_notify_sent.retain(|(jid, _, _)| jid != job_id);
        self.checkpoint_complete_sent
            .retain(|(jid, _, _)| jid != job_id);
        self.notify_sent.retain(|(jid, _, _)| jid != job_id);
        self.barrier_sent.retain(|(jid, _)| jid != job_id);
    }
}

// ── Checkpoint sync helpers (G3) ───────────────────────────────────────────
//
// Outer→inner sync uses CheckpointInner::replace_data_from (full replace,
// restore path) or CheckpointInner::apply_monotonic_from (periodic/submit paths).
// Inner→outer sync uses Coordinator::apply_checkpoint_inner_sync.
// Per-job monotonic merges use merge_checkpoint_coordinator (gRPC ack path).

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
        Some(existing) if checkpoint_progress(existing) > checkpoint_progress(&src) => {
            // dst is strictly more advanced — keep it (never regress).
        }
        Some(existing) if checkpoint_progress(existing) == checkpoint_progress(&src) => {
            // Same (epoch, state_rank). If src carries a newer fencing token
            // (e.g. after a leader election), take it; otherwise keep dst.
            if src.fencing_token.as_u64() > existing.fencing_token.as_u64() {
                dst.insert(job_id.clone(), src);
            }
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

#[cfg(test)]
mod checkpoint_inner_tests {
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

    fn job(id: &str) -> JobId {
        JobId::try_new(id).unwrap()
    }

    fn exec(id: &str) -> ExecutorId {
        ExecutorId::try_new(id).unwrap()
    }

    #[test]
    fn set_restore_directive_records_and_resets_tracking() {
        let j = job("ci-restore");
        let e = exec("ci-exec");
        let mut inner = CheckpointInner::new();
        inner.restore_notify_sent.insert((j.clone(), e.clone(), 1));
        inner
            .checkpoint_complete_sent
            .insert((j.clone(), e.clone(), 1));

        inner.set_restore_directive(
            &j,
            RestoreDirective {
                epoch: 7,
                fencing_token: 3,
                sink_commit: Vec::new(),
                sink_abort: Vec::new(),
            },
        );

        assert_eq!(inner.restore_directive(&j).map(|d| d.epoch), Some(7));
        assert!(inner.restore_notify_sent.is_empty());
        assert!(inner.checkpoint_complete_sent.is_empty());
    }

    #[test]
    fn pending_restore_commands_dedupes_and_respects_relevance() {
        let j = job("ci-restore2");
        let e = exec("ci-exec2");
        let mut inner = CheckpointInner::new();
        inner.set_restore_directive(
            &j,
            RestoreDirective {
                epoch: 5,
                fencing_token: 2,
                sink_commit: Vec::new(),
                sink_abort: Vec::new(),
            },
        );

        let none = inner.pending_restore_commands_for_executor(&e, |_| false);
        assert!(none.is_empty());

        let first = inner.pending_restore_commands_for_executor(&e, |_| true);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].epoch, 5);
        let second = inner.pending_restore_commands_for_executor(&e, |_| true);
        assert!(second.is_empty());
    }

    #[test]
    fn pending_restore_commands_drops_zero_fencing_token() {
        let j = job("ci-restore3");
        let e = exec("ci-exec3");
        let mut inner = CheckpointInner::new();
        inner.restore_directives.insert(
            j.clone(),
            RestoreDirective {
                epoch: 5,
                fencing_token: 0,
                sink_commit: Vec::new(),
                sink_abort: Vec::new(),
            },
        );
        let cmds = inner.pending_restore_commands_for_executor(&e, |_| true);
        assert!(cmds.is_empty(), "zero fencing token must be dropped");
    }

    #[test]
    fn pending_checkpoint_complete_dedupes_committed_epoch() {
        let j = job("ci-complete");
        let e = exec("ci-exec4");
        let mut inner = CheckpointInner::new();
        inner.coordinators.insert(
            j.clone(),
            coord(&j, 4, CheckpointCoordinatorState::Committed { epoch: 4 }),
        );

        let first = inner.pending_checkpoint_complete_for_executor(&e, |_| true);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].epoch, 4);
        let second = inner.pending_checkpoint_complete_for_executor(&e, |_| true);
        assert!(second.is_empty());
    }

    #[test]
    fn clear_job_removes_all_control_state() {
        let j = job("ci-clear");
        let e = exec("ci-exec5");
        let mut inner = CheckpointInner::new();
        inner.coordinators.insert(
            j.clone(),
            coord(&j, 1, CheckpointCoordinatorState::Committed { epoch: 1 }),
        );
        inner.restore_directives.insert(
            j.clone(),
            RestoreDirective {
                epoch: 1,
                fencing_token: 1,
                sink_commit: Vec::new(),
                sink_abort: Vec::new(),
            },
        );
        inner.pending_stop_after_savepoint.insert(j.clone(), 1);
        inner.restore_notify_sent.insert((j.clone(), e.clone(), 1));
        inner
            .checkpoint_complete_sent
            .insert((j.clone(), e.clone(), 1));
        inner.notify_sent.insert((j.clone(), e.clone(), 1));
        inner.barrier_sent.insert((j.clone(), 1));

        inner.clear_job(&j);

        assert!(inner.coordinators.is_empty());
        assert!(inner.restore_directives.is_empty());
        assert!(inner.pending_stop_after_savepoint.is_empty());
        assert!(inner.restore_notify_sent.is_empty());
        assert!(inner.checkpoint_complete_sent.is_empty());
        assert!(inner.notify_sent.is_empty());
        assert!(inner.barrier_sent.is_empty());
    }

    #[test]
    fn apply_monotonic_from_does_not_clobber_advanced_inner() {
        let j = job("ci-snap");
        // Inner is mid-Committing on epoch 3.
        let mut inner = CheckpointInner::new();
        inner.coordinators.insert(
            j.clone(),
            coord(&j, 3, CheckpointCoordinatorState::Committing { epoch: 3 }),
        );
        // Outer has the same job at AwaitingAcks epoch 3 (stale copy) plus a
        // restore directive that must propagate.
        let mut outer = CheckpointInner::new();
        outer.coordinators.insert(
            j.clone(),
            coord(
                &j,
                3,
                CheckpointCoordinatorState::AwaitingAcks {
                    epoch: 3,
                    initiated_at_ms: 0,
                },
            ),
        );
        outer.restore_directives.insert(
            j.clone(),
            RestoreDirective {
                epoch: 3,
                fencing_token: 1,
                sink_commit: Vec::new(),
                sink_abort: Vec::new(),
            },
        );

        inner.apply_monotonic_from(&outer);

        // The Committing epoch must NOT have been clobbered by AwaitingAcks.
        assert!(matches!(
            inner.coordinators[&j].coordinator_state(),
            CheckpointCoordinatorState::Committing { epoch: 3 }
        ));
        // Delivery-tracking fields ARE replaced from outer.
        assert!(inner.restore_directives.contains_key(&j));
    }
}
