//! In-process coordinator ↔ executor transport (ADR-12.4).

use std::sync::{Arc, Mutex};

use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorHeartbeat,
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, RegisterExecutorRequest,
    RegisterExecutorResponse, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
    TransportVersion,
};
use tokio::sync::RwLock;

use crate::coordinator_sharded::{CheckpointInner, ExecutorInner};
use crate::{Coordinator, SchedulerError, TaskUpdateOutcome, status_from_scheduler_error};

/// Task endpoint marker: assignments are delivered via [`ExecutorAssignmentInbox`]
/// instead of gRPC (`push_assignments_in_process` in `krishiv-runtime`).
pub const IN_PROCESS_TASK_ENDPOINT: &str = "inprocess://local";

/// Returns true when `endpoint` should use inbox delivery.
pub fn is_in_process_task_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("inprocess://")
}

/// Bridges executor RPCs to an in-memory [`Coordinator`] (no tonic server required).
///
/// Lock sharding: dedicated inner locks for executor registry and checkpoint
/// state so that hot-path operations (heartbeat, checkpoint ack) do not contend
/// with full coordinator state access.
#[derive(Clone)]
pub struct InProcessCoordinatorBridge {
    coordinator: Arc<Mutex<Coordinator>>,
    /// Dedicated lock for executor registry state.
    pub(crate) executor_inner: Arc<RwLock<ExecutorInner>>,
    /// Dedicated lock for checkpoint coordinator state.
    pub(crate) checkpoint_inner: Arc<RwLock<CheckpointInner>>,
}

impl InProcessCoordinatorBridge {
    /// Wrap a coordinator with sharded inner locks for direct method calls
    /// from the executor runner.
    pub fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        executor_inner: Arc<RwLock<ExecutorInner>>,
        checkpoint_inner: Arc<RwLock<CheckpointInner>>,
    ) -> Self {
        Self {
            coordinator,
            executor_inner,
            checkpoint_inner,
        }
    }
}

impl Drop for InProcessCoordinatorBridge {
    fn drop(&mut self) {
        if let Ok(coord) = self.coordinator.lock() {
            let running: usize = coord
                .job_snapshots()
                .iter()
                .map(|j| j.assigned_task_count())
                .sum();
            if running > 0 {
                tracing::warn!(
                    running_tasks = running,
                    "InProcessCoordinatorBridge dropped with in-flight tasks; \
                     tasks will not receive further status updates"
                );
            }
        }
    }
}

fn lock_coord(
    coordinator: &Arc<Mutex<Coordinator>>,
) -> Result<std::sync::MutexGuard<'_, Coordinator>, tonic::Status> {
    coordinator
        .lock()
        .map_err(|_| tonic::Status::internal("coordinator lock poisoned"))
}

#[tonic::async_trait]
impl CoordinatorExecutorService for InProcessCoordinatorBridge {
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut coordinator = lock_coord(&self.coordinator)?;
        let lease = coordinator
            .register_executor(request.descriptor().clone())
            .map_err(status_from_scheduler_error)?;
        Ok(tonic::Response::new(RegisterExecutorResponse::new(
            request.descriptor().executor_id().clone(),
            lease,
            TransportDisposition::Accepted,
        )))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut coordinator = lock_coord(&self.coordinator)?;
        let lease = coordinator
            .deregister_executor(request.executor_id(), request.lease_generation())
            .map_err(status_from_scheduler_error)?;
        Ok(tonic::Response::new(DeregisterExecutorResponse::new(
            request.executor_id().clone(),
            lease,
            TransportDisposition::Accepted,
        )))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        let executor_id = request.executor_id().clone();
        let lease_generation = request.lease_generation();
        let running_tasks: Vec<_> = request
            .running_attempts()
            .iter()
            .map(|a| a.task_id().clone())
            .collect();
        let streaming_states: Vec<_> = request.streaming_task_states().to_vec();

        // Fast path: update the executor registry via the sharded inner lock.
        // This avoids serializing the heartbeat behind the full coordinator lock
        // when the heartbeat carries no streaming state updates.
        let mut heartbeat = ExecutorHeartbeat::new(executor_id.clone(), request.state())
            .with_lease_generation(lease_generation)
            .with_running_tasks(running_tasks.clone());
        if let Some(bytes) = request.memory_used_bytes() {
            heartbeat = heartbeat.with_memory_used_bytes(bytes);
        }
        if let Some(bytes) = request.memory_limit_bytes() {
            heartbeat = heartbeat.with_memory_limit_bytes(bytes);
        }
        if let Some(count) = request.active_task_count() {
            heartbeat = heartbeat.with_active_task_count(count);
        }

        let lease_generation = {
            let mut inner = self.executor_inner.write().await;
            let lg = inner
                .handle_heartbeat(heartbeat)
                .map_err(status_from_scheduler_error)?;
            // Sync inner → outer to prevent dual-state drift (G3).
            let mut coord = lock_coord(&self.coordinator)?;
            coord.executors.clone_from(&inner.executors);
            coord.state = inner.state;
            coord.recovering = inner.recovering;
            lg
        };

        // Complex heartbeat processing (streaming states, hot-keys, LLM quota,
        // checkpoint initiation) still needs the full coordinator lock.
        if !streaming_states.is_empty() {
            let mut coordinator = lock_coord(&self.coordinator)?;
            coordinator
                .executor_heartbeat(
                    ExecutorHeartbeat::new(executor_id, request.state())
                        .with_lease_generation(lease_generation)
                        .with_running_tasks(running_tasks)
                        .with_streaming_task_states(streaming_states),
                )
                .map_err(status_from_scheduler_error)?;
        }

        Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
            lease_generation,
            TransportDisposition::Accepted,
        )))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(request.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported coordinator/executor transport version {}; current version is {}",
                request.version(),
                TransportVersion::CURRENT
            )));
        }
        let mut update = krishiv_proto::TaskStatusUpdate::new(
            request.job_id().clone(),
            request.stage_id().clone(),
            request.task_id().clone(),
            request.executor_id().clone(),
            request.state(),
            request.attempt_id().as_u32(),
        )
        .with_lease_generation(request.lease_generation());
        if let Some(message) = request.message() {
            update = update.with_message(message);
        }
        if let Some(meta) = request.output_metadata() {
            update = update.with_output_metadata(meta.clone());
        }
        let mut coordinator = lock_coord(&self.coordinator)?;
        let response = match coordinator.apply_task_update(update) {
            Ok(TaskUpdateOutcome::Applied) | Ok(TaskUpdateOutcome::Duplicate) => {
                TaskStatusResponse::new(TransportDisposition::Accepted)
            }
            Err(SchedulerError::UnknownJob { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownJob)
            }
            Err(SchedulerError::UnknownTask { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownTask)
            }
            Err(SchedulerError::UnknownExecutor { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownExecutor)
            }
            Err(SchedulerError::StaleExecutorLease { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleLease)
            }
            Err(SchedulerError::StaleTaskAttempt { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleAttempt)
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };
        Ok(tonic::Response::new(response))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
        let request = request.into_inner();
        let job_id = request.job_id.clone();
        let ack_epoch = request.epoch;

        // Phase 1: extract commit data under the lock (in-memory only, no I/O).
        let (response, pending_commit, require_finalize) = {
            let mut inner = self.checkpoint_inner.write().await;
            let (response, commit) = inner.handle_ack(request).await;
            let require_finalize = commit.is_some();
            (response, commit, require_finalize)
        };

        // Phase 2: perform async storage I/O outside the lock.
        if let Some(commit) = pending_commit {
            crate::checkpoint::CheckpointCoordinator::commit_storage(commit)
                .await
                .map_err(|e| tonic::Status::internal(format!("checkpoint commit failed: {e}")))?;
        }

        // Phase 3: finalize and sync inner → outer coordinator.
        {
            let mut inner = self.checkpoint_inner.write().await;
            if require_finalize {
                inner.finalize_ack(&job_id, ack_epoch);
            }
            // Sync inner → outer coordinator to avoid dual-state drift (G3).
            let mut coord = lock_coord(&self.coordinator)?;
            coord
                .checkpoint_coordinators
                .clone_from(&inner.coordinators);
            coord.checkpoint_notify_sent.clone_from(&inner.notify_sent);
            coord.barrier_dispatch_sent.clone_from(&inner.barrier_sent);
        }
        Ok(tonic::Response::new(response))
    }
}
