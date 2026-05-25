//! Coordinator-side checkpoint barrier dispatch over gRPC (WS-4 / ADR-R16.1).

use std::collections::HashSet;
use std::time::Duration;

use krishiv_proto::wire::v1::{BarrierKind, CheckpointBarrier};
use krishiv_proto::{CheckpointAckRequest, ExecutorId, FencingToken, JobId, TaskId};

use crate::barrier_client::inject_barrier;
use crate::barrier_tracker::CheckpointBarrierTracker;
use crate::heartbeat::ExecutorRecord;
use crate::{Coordinator, SchedulerResult};

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
            let Some(job) = self.jobs.get(job_id) else {
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
                operator_id: format!("op-{}", task_id.as_str()),
                task_id: task_id.clone(),
                epoch,
                fencing_token,
                source_offsets: Vec::new(),
                snapshot_path,
            };
            let _ = self.handle_checkpoint_ack(request);
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

    let mut acks: Vec<(TaskId, krishiv_proto::wire::v1::BarrierAck)> = Vec::new();
    for target in &plan.targets {
        let checkpoint_id = format!("task:{}/cp-{}", target.task_id.as_str(), plan.epoch);
        let channel = tonic::transport::Channel::from_shared(target.barrier_endpoint.clone())
            .map_err(|e| format!("invalid barrier endpoint {}: {e}", target.barrier_endpoint))?
            .connect()
            .await
            .map_err(|e| format!("barrier connect {}: {e}", target.barrier_endpoint))?;
        let mut client =
            krishiv_proto::wire::v1::barrier_service_client::BarrierServiceClient::new(channel);
        let barrier = CheckpointBarrier {
            epoch: plan.epoch,
            job_id: plan.job_id.as_str().to_owned(),
            checkpoint_id: checkpoint_id.clone(),
            barrier_kind: BarrierKind::Checkpoint as i32,
            timestamp_ms: 0,
        };
        inject_barrier(&mut client, barrier, &mut tracker, timeout).await?;
    }

    if !tracker.is_complete() {
        return Err(format!(
            "barrier quorum incomplete for job {} epoch {}: missing {:?}",
            plan.job_id,
            plan.epoch,
            tracker.missing_tasks()
        ));
    }
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
        let coord = shared
            .read()
            .map_err(|_| crate::SchedulerError::Transport {
                message: "coordinator lock poisoned".to_string(),
            })?;
        coord.pending_barrier_dispatch_plans()
    };
    for plan in plans {
        let job_id = plan.job_id.clone();
        let epoch = plan.epoch;
        let fencing = plan.fencing_token;
        match dispatch_barrier_plan(&plan, timeout).await {
            Ok(acks) => {
                let mut coord = shared
                    .write()
                    .map_err(|_| crate::SchedulerError::Transport {
                        message: "coordinator lock poisoned".to_string(),
                    })?;
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
    use krishiv_proto::{CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorState};

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
