//! In-process coordinator ↔ executor transport (ADR-12.4).

use std::sync::{Arc, Mutex};

use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorHeartbeat,
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, RegisterExecutorRequest,
    RegisterExecutorResponse, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
    TransportVersion,
};

use crate::{Coordinator, SchedulerError, TaskUpdateOutcome, status_from_scheduler_error};

/// Task endpoint marker: assignments are delivered via [`ExecutorAssignmentInbox`]
/// instead of gRPC (`push_assignments_in_process` in `krishiv-runtime`).
pub const IN_PROCESS_TASK_ENDPOINT: &str = "inprocess://local";

/// Returns true when `endpoint` should use inbox delivery.
pub fn is_in_process_task_endpoint(endpoint: &str) -> bool {
    endpoint.starts_with("inprocess://")
}

/// Bridges executor RPCs to an in-memory [`Coordinator`] (no tonic server required).
#[derive(Clone)]
pub struct InProcessCoordinatorBridge {
    coordinator: Arc<Mutex<Coordinator>>,
}

impl InProcessCoordinatorBridge {
    /// Wrap a coordinator for direct method calls from the executor runner.
    pub fn new(coordinator: Arc<Mutex<Coordinator>>) -> Self {
        Self { coordinator }
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
        let mut heartbeat = ExecutorHeartbeat::new(request.executor_id().clone(), request.state())
            .with_lease_generation(request.lease_generation())
            .with_running_tasks(
                request
                    .running_attempts()
                    .iter()
                    .map(|a| a.task_id().clone())
                    .collect(),
            );
        if let Some(bytes) = request.memory_used_bytes() {
            heartbeat = heartbeat.with_memory_used_bytes(bytes);
        }
        if let Some(bytes) = request.memory_limit_bytes() {
            heartbeat = heartbeat.with_memory_limit_bytes(bytes);
        }
        if let Some(count) = request.active_task_count() {
            heartbeat = heartbeat.with_active_task_count(count);
        }
        if !request.streaming_task_states().is_empty() {
            heartbeat =
                heartbeat.with_streaming_task_states(request.streaming_task_states().to_vec());
        }
        let mut coordinator = lock_coord(&self.coordinator)?;
        let effects = coordinator
            .executor_heartbeat(heartbeat)
            .map_err(status_from_scheduler_error)?;
        Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
            effects.lease_generation,
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
        let mut coordinator = lock_coord(&self.coordinator)?;
        let response = coordinator.handle_checkpoint_ack(request);
        Ok(tonic::Response::new(response))
    }
}
