//! Coordinator-side checkpoint barrier dispatch over gRPC (WS-4 / ADR-R16.1).

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

use dashmap::DashMap;
use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, ExecutorId, FencingToken, JobId, TaskId,
};

use crate::barrier_client::inject_barrier;
use crate::barrier_tracker::CheckpointBarrierTracker;
use crate::heartbeat::ExecutorRecord;
use crate::{Coordinator, SchedulerResult};

fn barrier_channels() -> &'static DashMap<String, tonic::transport::Channel> {
    static CHANNELS: OnceLock<DashMap<String, tonic::transport::Channel>> = OnceLock::new();
    CHANNELS.get_or_init(DashMap::new)
}

async fn get_or_connect_barrier_channel(
    endpoint: &str,
) -> Result<tonic::transport::Channel, String> {
    let channels = barrier_channels();
    if let Some(channel) = channels.get(endpoint) {
        return Ok(channel.clone());
    }

    let parsed = tonic::transport::Channel::from_shared(endpoint.to_owned())
        .map_err(|e| format!("invalid barrier endpoint {endpoint}: {e}"))?;
    let channel = parsed
        .connect()
        .await
        .map_err(|e| format!("barrier connect {endpoint}: {e}"))?;

    Ok(channels
        .entry(endpoint.to_owned())
        .or_insert(channel.clone())
        .clone())
}

/// One barrier round-trip target for a running task on an executor.
#[derive(Debug, Clone)]
pub struct BarrierDispatchTarget {
    pub executor_id: ExecutorId,
    pub barrier_endpoint: String,
    pub task_id: TaskId,
}

/// Plan for dispatching one checkpoint epoch via BarrierService.
#[derive(Debug, Clone)]
pub struct BarrierDispatchPlan {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    pub targets: Vec<BarrierDispatchTarget>,
}

impl Coordinator {
    /// Collect executors/tasks that should receive a barrier for in-flight checkpoint epochs.
    pub fn pending_barrier_dispatch_plans(&self) -> Vec<BarrierDispatchPlan> {
        let mut plans = Vec::new();
        for (job_id, coord) in &self.ckpt.coordinators {
            let epoch = match &coord.state {
                crate::checkpoint::CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
                _ => continue,
            };
            let key = (job_id.clone(), epoch);
            if self.ckpt.barrier_sent.contains(&key) {
                continue;
            }
            let mut targets = Vec::new();
            let Some(job) = self.job_coordinators.get(job_id).map(|jc| jc.read_record()) else {
                continue;
            };
            for stage in &job.stages {
                for task in stage.tasks() {
                    if task.state() != krishiv_proto::TaskState::Running {
                        continue;
                    }
                    let Some(executor_id) = task.assigned_executor() else {
                        continue;
                    };
                    let Ok(record) = self.exec.executors.find_executor(executor_id) else {
                        continue;
                    };
                    let Some(endpoint) = barrier_endpoint_for_record(record) else {
                        continue;
                    };
                    targets.push(BarrierDispatchTarget {
                        executor_id: executor_id.clone(),
                        barrier_endpoint: endpoint,
                        task_id: task.task_id().clone(),
                    });
                }
            }
            if targets.is_empty() {
                continue;
            }
            plans.push(BarrierDispatchPlan {
                job_id: job_id.clone(),
                epoch,
                fencing_token: coord.fencing_token,
                targets,
            });
        }
        plans
    }

    /// Mark `(job_id, epoch)` as dispatched so we do not re-send barriers.
    pub fn mark_barrier_dispatched(&mut self, job_id: &JobId, epoch: u64) {
        self.ckpt.barrier_sent.insert((job_id.clone(), epoch));
    }

    /// Apply barrier acks as checkpoint acks (one per task).
    pub fn apply_barrier_acks(
        &mut self,
        job_id: &JobId,
        epoch: u64,
        fencing_token: FencingToken,
        acks: &[(TaskId, krishiv_proto::wire::v1::BarrierAck)],
    ) {
        for (task_id, ack) in acks {
            let snapshot_path = ack.state_handle.as_ref().map(|h| h.checkpoint_uri.clone());
            let operator_id = match krishiv_proto::OperatorId::try_new(format!(
                "op-{}",
                task_id.as_str()
            )) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(task_id = %task_id, error = %e, "failed to derive operator_id from task_id; skipping ack");
                    continue;
                }
            };
            let request = CheckpointAckRequest {
                job_id: job_id.clone(),
                operator_id,
                task_id: task_id.clone(),
                epoch,
                fencing_token,
                source_offsets: Vec::new(),
                snapshot_path,
            };
            match self.handle_checkpoint_ack(request) {
                CheckpointAckResponse::Accepted => {}
                response => {
                    tracing::warn!(
                        job_id = job_id.as_str(),
                        task_id = task_id.as_str(),
                        epoch,
                        ?response,
                        "barrier ack rejected during fanout"
                    );
                }
            }
        }
    }

    /// Apply barrier acks with deferred post-commit FS I/O.
    ///
    /// Like [`Self::apply_barrier_acks`] but uses
    /// [`Self::handle_checkpoint_ack_deferred`] so the in-memory ack processing
    /// happens under the coordinator write lock while the post-commit work
    /// (savepoint preservation, stop-with-savepoint) is returned for the caller
    /// to execute **outside** the lock. This prevents filesystem I/O from
    /// blocking heartbeats and job submissions during barrier fanout.
    ///
    /// Returns a list of `(job_id, epoch)` pairs that need
    /// [`Coordinator::on_checkpoint_epoch_committed`] to be called outside the
    /// lock.
    pub fn apply_barrier_acks_deferred(
        &mut self,
        job_id: &JobId,
        epoch: u64,
        fencing_token: FencingToken,
        acks: &[(TaskId, krishiv_proto::wire::v1::BarrierAck)],
    ) -> Vec<(JobId, u64)> {
        let mut post_commit_jobs = Vec::new();
        for (task_id, ack) in acks {
            let snapshot_path = ack.state_handle.as_ref().map(|h| h.checkpoint_uri.clone());
            let operator_id = match krishiv_proto::OperatorId::try_new(format!(
                "op-{}",
                task_id.as_str()
            )) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(task_id = %task_id, error = %e, "failed to derive operator_id from task_id; skipping ack");
                    continue;
                }
            };
            let request = CheckpointAckRequest {
                job_id: job_id.clone(),
                operator_id,
                task_id: task_id.clone(),
                epoch,
                fencing_token,
                source_offsets: Vec::new(),
                snapshot_path,
            };
            let (response, post_commit) = self.handle_checkpoint_ack_deferred(request);
            if let Some((pc_job_id, pc_epoch)) = post_commit {
                post_commit_jobs.push((pc_job_id, pc_epoch));
            } else if !matches!(response, CheckpointAckResponse::Accepted) {
                tracing::warn!(
                    job_id = job_id.as_str(),
                    task_id = task_id.as_str(),
                    epoch,
                    ?response,
                    "barrier ack rejected during fanout"
                );
            }
        }
        post_commit_jobs
    }
}

fn barrier_endpoint_for_record(record: &ExecutorRecord) -> Option<String> {
    record
        .descriptor()
        .barrier_endpoint()
        .map(str::to_owned)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            record
                .descriptor()
                .task_endpoint()
                .map(str::to_owned)
                .filter(|s| !s.is_empty())
        })
}

/// Dispatch barriers for one plan; returns per-task acks on success.
pub async fn dispatch_barrier_plan(
    plan: &BarrierDispatchPlan,
    timeout: Duration,
) -> Result<Vec<(TaskId, krishiv_proto::wire::v1::BarrierAck)>, String> {
    use futures::stream::{FuturesUnordered, StreamExt};

    let expected: HashSet<String> = plan
        .targets
        .iter()
        .map(|t| t.task_id.as_str().to_owned())
        .collect();
    let mut tracker = CheckpointBarrierTracker::new(
        plan.job_id.as_str(),
        plan.epoch,
        expected.iter().cloned(),
        timeout,
    );

    let mut futures = FuturesUnordered::new();
    for target in &plan.targets {
        let checkpoint_id = format!("task:{}/cp-{}", target.task_id.as_str(), plan.epoch);
        let endpoint = target.barrier_endpoint.clone();
        let barrier = CheckpointBarrier {
            epoch: plan.epoch,
            job_id: plan.job_id.as_str().to_owned(),
            checkpoint_id,
            barrier_kind: BarrierKind::Checkpoint as i32,
            timestamp_ms: 0,
        };
        futures.push(async move {
            let channel = get_or_connect_barrier_channel(&endpoint).await?;
            let mut client =
                krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient::with_interceptor(
                    channel,
                    krishiv_metrics::grpc::inject_trace_context
                        as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                );
            let ack = inject_barrier(&mut client, barrier, timeout).await?;
            Ok::<_, String>(ack)
        });
    }

    let total_targets = plan.targets.len();
    let mut failure_count = 0usize;
    while let Some(result) = futures.next().await {
        match result {
            Ok(ack) => {
                // Duplicate acks (same task_id already received) are expected in
                // at-least-once delivery — just skip them. Wrong epoch/job acks
                // should not abort the entire barrier round.
                tracker.record_ack(&ack);
            }
            Err(e) => {
                failure_count = failure_count.saturating_add(1);
                // Early-abort: if remaining live targets can never reach quorum,
                // stop waiting for the full timeout.
                let completed = tracker.completed_count();
                let remaining_live = total_targets.saturating_sub(failure_count);
                if completed + remaining_live < total_targets {
                    tracing::error!(
                        job_id = %plan.job_id,
                        epoch = plan.epoch,
                        total = total_targets,
                        completed,
                        failures = failure_count,
                        "barrier quorum mathematically impossible; aborting early"
                    );
                    break;
                }
                // Log the connection error but continue processing remaining
                // executor acks. Only fail after the loop if quorum is unachievable.
                tracing::warn!(
                    job_id = %plan.job_id,
                    epoch = plan.epoch,
                    error = %e,
                    "barrier ack from executor failed; continuing with remaining acks"
                );
            }
        }
    }

    if !tracker.is_complete() {
        return Err(format!(
            "barrier quorum incomplete for job {} epoch {}: missing {:?}",
            plan.job_id,
            plan.epoch,
            tracker.missing_tasks()
        ));
    }
    let mut acks: Vec<(TaskId, krishiv_proto::wire::v1::BarrierAck)> = Vec::new();
    for ack in tracker.collected_acks() {
        acks.push((
            TaskId::try_new(ack.task_id.clone()).map_err(|e| e.to_string())?,
            ack.clone(),
        ));
    }
    Ok(acks)
}

/// Build a [`CheckpointAckRequest`] from one barrier ack so the barrier transport
/// and the `checkpoint_ack` RPC produce identical acks for the same task/epoch.
///
/// Returns `Err` when a valid `OperatorId` cannot be derived from `task_id`
/// (e.g. the task-id string is empty or all-whitespace). The caller should
/// log the error and skip the ack for that task.
fn barrier_ack_to_checkpoint_ack(
    job_id: &JobId,
    epoch: u64,
    fencing_token: FencingToken,
    task_id: &TaskId,
    ack: &krishiv_proto::wire::v1::BarrierAck,
) -> Result<CheckpointAckRequest, String> {
    let snapshot_path = ack.state_handle.as_ref().map(|h| h.checkpoint_uri.clone());
    let operator_id = krishiv_proto::OperatorId::try_new(format!("op-{}", task_id.as_str()))
        .map_err(|e| {
            format!(
                "failed to derive operator_id from task_id '{}': {e}",
                task_id.as_str()
            )
        })?;
    Ok(CheckpointAckRequest {
        job_id: job_id.clone(),
        operator_id,
        task_id: task_id.clone(),
        epoch,
        fencing_token,
        source_offsets: Vec::new(),
        snapshot_path,
    })
}

/// Run pending barrier dispatches against a shared coordinator (background loop).
///
/// C1 residual 2 (split-quorum): barrier acks are routed through
/// `checkpoint_inner.handle_ack` — the *same* quorum accumulator the
/// `checkpoint_ack` gRPC handler uses. Before this fix the barrier path acked
/// the outer `Coordinator` while the RPC path acked the inner lock, so when a
/// single epoch's tasks acked over different transports neither copy reached
/// quorum alone and the epoch timed out and retried. With both transports
/// feeding `checkpoint_inner`, an epoch commits exactly once regardless of how
/// each task's ack arrives.
pub async fn drive_barrier_dispatches(
    shared: &crate::SharedCoordinator,
    timeout: Duration,
) -> SchedulerResult<()> {
    use crate::checkpoint::CheckpointCoordinator;

    let plans = {
        let coord = shared.read().await;
        coord.pending_barrier_dispatch_plans()
    };
    for plan in plans {
        let job_id = plan.job_id.clone();
        let epoch = plan.epoch;
        let fencing = plan.fencing_token;
        let acks = match dispatch_barrier_plan(&plan, timeout).await {
            Ok(acks) => acks,
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id,
                    epoch,
                    error = %e,
                    "barrier dispatch failed; heartbeat checkpoint fallback remains"
                );
                continue;
            }
        };

        // Mark dispatched on the outer coordinator (delivery dedup tracking).
        shared.write().await.mark_barrier_dispatched(&job_id, epoch);

        for (task_id, ack) in &acks {
            let request = match barrier_ack_to_checkpoint_ack(&job_id, epoch, fencing, task_id, ack)
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        job_id = %job_id,
                        task_id = %task_id,
                        epoch,
                        error = %e,
                        "skipping barrier ack: could not build checkpoint ack request"
                    );
                    continue;
                }
            };

            // Phase 1: in-memory ack processing under the dedicated checkpoint
            // inner lock — the single quorum owner shared with the gRPC path.
            let (response, pending) = {
                let mut inner = shared.checkpoint_inner.write().await;
                inner.handle_ack(request).await
            };
            if !matches!(response, CheckpointAckResponse::Accepted) {
                tracing::warn!(
                    job_id = %job_id,
                    task_id = %task_id,
                    epoch,
                    ?response,
                    "barrier ack rejected during fanout"
                );
                continue;
            }

            let Some(commit) = pending else {
                // Ack accepted, quorum not yet reached — keep collecting.
                continue;
            };

            // Phase 2: async storage I/O with no coordinator lock held.
            if let Err(e) = CheckpointCoordinator::commit_storage(commit).await {
                let aborted = {
                    let mut inner = shared.checkpoint_inner.write().await;
                    if let Some(coord) = inner.coordinators.get_mut(&job_id) {
                        coord.abort_epoch(&format!("checkpoint storage write failed: {e}"));
                    }
                    inner.coordinators.get(&job_id).cloned()
                };
                krishiv_metrics::global_metrics().inc_checkpoint_failed(job_id.as_str());
                // Lock order: checkpoint_inner released above, take outer now.
                let mut coord = shared.write().await;
                if let Some(aborted) = aborted {
                    crate::coordinator_sharded::merge_checkpoint_coordinator(
                        &mut coord.ckpt.coordinators,
                        &job_id,
                        aborted,
                    );
                }
                tracing::warn!(
                    job_id = %job_id,
                    epoch,
                    error = %e,
                    "barrier-path checkpoint commit failed; epoch aborted"
                );
                continue;
            }

            // Phase 3: finalize under checkpoint_inner, snapshot the committed
            // coordinator, release, then monotonically merge into the outer copy
            // and run post-commit FS work (savepoint preserve, stop-after).
            let (finalize_result, committed) = {
                let mut inner = shared.checkpoint_inner.write().await;
                let result = inner.finalize_ack(&job_id, epoch);
                (result, inner.coordinators.get(&job_id).cloned())
            };
            {
                let mut coord = shared.write().await;
                if let Some(committed) = committed {
                    crate::coordinator_sharded::merge_checkpoint_coordinator(
                        &mut coord.ckpt.coordinators,
                        &job_id,
                        committed,
                    );
                }
                if let Err(error) = finalize_result {
                    tracing::error!(
                        job_id = %job_id,
                        epoch,
                        error = %error,
                        "barrier-path checkpoint finalize failed"
                    );
                    continue;
                }
                coord.on_checkpoint_epoch_committed(&job_id, epoch);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_proto::wire::v1::BarrierAck;
    use krishiv_proto::{CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorState};

    /// Regression (Wave 2 — Scheduler Hardening): `apply_barrier_acks` must
    /// drive every ack through `handle_checkpoint_ack` and handle every
    /// `CheckpointAckResponse` variant explicitly (it previously discarded
    /// the response with `let _ = ...`, hiding rejected acks from operators).
    /// A rejected ack (here, `JobNotFound` for an unknown job) must not
    /// short-circuit processing of the remaining acks in the batch.
    #[test]
    fn apply_barrier_acks_handles_rejected_and_accepted_acks_without_panicking() {
        let mut coord = Coordinator::active(CoordinatorId::try_new("barrier-acks").unwrap());
        let job_id = JobId::try_new("missing-job").unwrap();
        let acks = vec![
            (
                TaskId::try_new("t0").unwrap(),
                BarrierAck {
                    epoch: 1,
                    job_id: job_id.as_str().to_string(),
                    task_id: "t0".into(),
                    state_handle: None,
                },
            ),
            (
                TaskId::try_new("t1").unwrap(),
                BarrierAck {
                    epoch: 1,
                    job_id: job_id.as_str().to_string(),
                    task_id: "t1".into(),
                    state_handle: None,
                },
            ),
        ];

        // Job does not exist, so every ack resolves to `JobNotFound`. The
        // call must process both acks (not panic, not bail out early).
        coord.apply_barrier_acks(&job_id, 1, FencingToken::try_new(1).unwrap(), &acks);
    }

    #[test]
    fn pending_plan_skips_executor_without_barrier_endpoint() {
        let mut coord = Coordinator::active(CoordinatorId::try_new("barrier-plan").unwrap());
        let exec_id = krishiv_proto::ExecutorId::try_new("exec-1").unwrap();
        coord
            .register_executor(ExecutorDescriptor::new(exec_id.clone(), "host", 2))
            .unwrap();
        coord
            .executor_heartbeat(ExecutorHeartbeat::new(exec_id, ExecutorState::Healthy))
            .unwrap();
        assert!(coord.pending_barrier_dispatch_plans().is_empty());
    }
}
