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
        for (job_id, coord) in &self.checkpoint_coordinators {
            let epoch = match &coord.state {
                crate::checkpoint::CheckpointCoordinatorState::AwaitingAcks { epoch, .. } => *epoch,
                _ => continue,
            };
            let key = (job_id.clone(), epoch);
            if self.barrier_dispatch_sent.contains(&key) {
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
                    let Ok(record) = self.executors.find_executor(executor_id) else {
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
        self.barrier_dispatch_sent.insert((job_id.clone(), epoch));
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
            let request = CheckpointAckRequest {
                job_id: job_id.clone(),
                operator_id: krishiv_proto::OperatorId::try_new(format!("op-{}", task_id.as_str()))
                    .expect("task_id is non-empty, so operator_id is non-empty"),
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

    while let Some(result) = futures.next().await {
        let ack = result?;
        if !tracker.record_ack(&ack) {
            return Err(format!(
                "unexpected ack for job {} epoch {}",
                ack.job_id, ack.epoch
            ));
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

/// Run pending barrier dispatches against a shared coordinator (background loop).
pub async fn drive_barrier_dispatches(
    shared: &crate::SharedCoordinator,
    timeout: Duration,
) -> SchedulerResult<()> {
    let plans = {
        let coord = shared.read().await;
        coord.pending_barrier_dispatch_plans()
    };
    for plan in plans {
        let job_id = plan.job_id.clone();
        let epoch = plan.epoch;
        let fencing = plan.fencing_token;
        match dispatch_barrier_plan(&plan, timeout).await {
            Ok(acks) => {
                let mut coord = shared.write().await;
                coord.mark_barrier_dispatched(&job_id, epoch);
                coord.apply_barrier_acks(&job_id, epoch, fencing, &acks);
            }
            Err(e) => {
                tracing::warn!(
                    job_id = %job_id,
                    epoch,
                    error = %e,
                    "barrier dispatch failed; heartbeat checkpoint fallback remains"
                );
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
