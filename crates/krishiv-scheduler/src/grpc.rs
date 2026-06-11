//! Coordinator gRPC adapters.

use std::sync::Arc;

use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CheckpointEpochInfo, CoordinatorExecutorService,
    CoordinatorManagementService, DeregisterExecutorRequest, DeregisterExecutorResponse,
    ExecutorHeartbeat, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
    HeartbeatThrottleCommand, InspectStateRequest, InspectStateResponse, JobId, LeaseGeneration,
    ListCheckpointsRequest, ListCheckpointsResponse, RegisterExecutorRequest,
    RegisterExecutorResponse, RestoreJobRequest, RestoreJobResponse, StateSnapshotInfo,
    TaskStatusRequest, TaskStatusResponse, TaskStatusUpdate, TransportDisposition,
    TransportVersion, TriggerSavepointRequest, TriggerSavepointResponse, wire,
};
use krishiv_state::checkpoint::CheckpointStorage;

use crate::auth::{extract_auth_context, validate_grpc_auth, validate_grpc_writer};
use crate::checkpoint::CheckpointCoordinator;
use crate::coordinator::SharedCoordinator;
use crate::error::{SchedulerError, TaskUpdateOutcome};

#[derive(Debug, Clone)]
pub struct CoordinatorExecutorTonicService {
    coordinator: SharedCoordinator,
}

impl CoordinatorExecutorTonicService {
    /// Create a coordinator/executor service adapter.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self { coordinator }
    }

    /// Shared coordinator backing this adapter.
    pub fn coordinator(&self) -> &SharedCoordinator {
        &self.coordinator
    }
}

#[tonic::async_trait]
impl CoordinatorExecutorService for CoordinatorExecutorTonicService {
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        // GAP-CP-08: Extract auth context for every handler.
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "register_executor");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let descriptor = request.descriptor().clone();
        let executor_id = descriptor.executor_id().clone();

        let response = {
            let mut coordinator = self.coordinator.write().await;
            match coordinator.register_executor(descriptor) {
                Ok(lease_generation) => RegisterExecutorResponse::new(
                    executor_id.clone(),
                    lease_generation,
                    TransportDisposition::Accepted,
                ),
                Err(SchedulerError::DuplicateExecutor { executor_id }) => {
                    RegisterExecutorResponse::new(
                        executor_id,
                        LeaseGeneration::initial(),
                        TransportDisposition::Duplicate,
                    )
                    .with_message("executor is already registered")
                }
                Err(error) => return Err(status_from_scheduler_error(error)),
            }
        };

        // Keep the sharded executor snapshot consistent after the durable
        // coordinator mutation above.
        {
            let coordinator = self.coordinator.read().await;
            let mut executor_inner = self.coordinator.executor_inner.write().await;
            crate::coordinator_sharded::sync_executor_to_inner(
                &coordinator.executors,
                coordinator.state,
                coordinator.executors.current_tick,
                coordinator.recovering,
                &mut executor_inner,
            );
        }

        Ok(tonic::Response::new(response))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
        // defense-in-depth: redundant when server-level interceptor is active
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "deregister_executor");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut coordinator = self.coordinator.write().await;

        let response = match coordinator
            .deregister_executor(request.executor_id(), request.lease_generation())
        {
            Ok(lease_generation) => DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                lease_generation,
                TransportDisposition::Accepted,
            ),
            Err(SchedulerError::UnknownExecutor { .. }) => DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                request.lease_generation(),
                TransportDisposition::UnknownExecutor,
            )
            .with_message("executor is not registered"),
            Err(SchedulerError::StaleExecutorLease { expected, .. }) => {
                DeregisterExecutorResponse::new(
                    request.executor_id().clone(),
                    expected,
                    TransportDisposition::StaleLease,
                )
                .with_message("executor lease generation is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
        // defense-in-depth: redundant when server-level interceptor is active
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "executor_heartbeat");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut heartbeat = ExecutorHeartbeat::new(request.executor_id().clone(), request.state())
            .with_lease_generation(request.lease_generation())
            .with_running_tasks(
                request
                    .running_attempts()
                    .iter()
                    .map(|attempt| attempt.task_id().clone())
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
        if !request.hot_key_reports().is_empty() {
            heartbeat = heartbeat.with_hot_key_reports(request.hot_key_reports().to_vec());
        }
        if !request.streaming_progress().is_empty() {
            heartbeat = heartbeat.with_streaming_progress(request.streaming_progress().to_vec());
        }
        let mut coordinator = self.coordinator.write().await;

        let response = match coordinator.executor_heartbeat(heartbeat) {
            Ok(effects) => {
                let mut resp = ExecutorHeartbeatResponse::new(
                    effects.lease_generation,
                    TransportDisposition::Accepted,
                );
                if !effects.source_throttles.is_empty() {
                    let wire_cmds: Vec<HeartbeatThrottleCommand> = effects
                        .source_throttles
                        .into_iter()
                        .map(|c| HeartbeatThrottleCommand {
                            source_id: c.source_id,
                            rows_per_second: c.rows_per_second,
                        })
                        .collect();
                    resp = resp.with_throttle_commands(wire_cmds);
                }
                if !effects.checkpoint_commands.is_empty() {
                    resp = resp.with_checkpoint_commands(effects.checkpoint_commands);
                }
                resp
            }
            Err(SchedulerError::UnknownExecutor { .. }) => ExecutorHeartbeatResponse::new(
                request.lease_generation(),
                TransportDisposition::UnknownExecutor,
            )
            .with_message("executor is not registered"),
            Err(SchedulerError::StaleExecutorLease { expected, .. }) => {
                ExecutorHeartbeatResponse::new(expected, TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        // defense-in-depth: redundant when server-level interceptor is active
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "task_status");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let mut update = TaskStatusUpdate::new(
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
        if let Some(output_metadata) = request.output_metadata() {
            update = update.with_output_metadata(output_metadata.clone());
        }

        let mut coordinator = self.coordinator.write().await;

        let response = match coordinator.apply_task_update(update) {
            Ok(TaskUpdateOutcome::Applied) => {
                TaskStatusResponse::new(TransportDisposition::Accepted)
            }
            Ok(TaskUpdateOutcome::Duplicate) => {
                TaskStatusResponse::new(TransportDisposition::Duplicate)
                    .with_message("task status update was already applied")
            }
            Err(SchedulerError::UnknownJob { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownJob)
                    .with_message("job is not registered")
            }
            Err(SchedulerError::UnknownTask { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownTask)
                    .with_message("task is not registered")
            }
            Err(SchedulerError::UnknownExecutor { .. }) => {
                TaskStatusResponse::new(TransportDisposition::UnknownExecutor)
                    .with_message("executor is not registered")
            }
            Err(SchedulerError::StaleExecutorLease { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleLease)
                    .with_message("executor lease generation is stale")
            }
            Err(SchedulerError::StaleTaskAttempt { .. }) => {
                TaskStatusResponse::new(TransportDisposition::StaleAttempt)
                    .with_message("task attempt is stale")
            }
            Err(error) => return Err(status_from_scheduler_error(error)),
        };

        Ok(tonic::Response::new(response))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
        // defense-in-depth: redundant when server-level interceptor is active
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "checkpoint_ack");
        ensure_shared_coordinator_active(&self.coordinator).await?;
        let ack = request.into_inner();
        let job_id = ack.job_id.clone();
        let ack_epoch = ack.epoch;

        // Phase 1: in-memory ack processing under dedicated checkpoint inner lock (H2 sharding).
        let (response, pending) = {
            let mut inner = self.coordinator.checkpoint_inner.write().await;
            inner.handle_ack(ack).await
        };

        // Phase 2: async storage I/O without any coordinator lock.
        if let Some(commit) = pending {
            if let Err(e) = CheckpointCoordinator::commit_storage(commit).await {
                // Storage write failed — abort the in-flight epoch so the
                // coordinator does not hang on awaiting-acks timeout.
                let mut inner = self.coordinator.checkpoint_inner.write().await;
                if let Some(coord) = inner.coordinators.get_mut(&job_id) {
                    coord.abort_epoch(&format!("checkpoint storage write failed: {e}"));
                }
                krishiv_metrics::global_metrics().inc_checkpoint_failed(job_id.as_str());
                // Sync inner → outer coordinator after abort.
                let mut coordinator = self.coordinator.write().await;
                coordinator
                    .checkpoint_coordinators
                    .clone_from(&inner.coordinators);
                return Err(tonic::Status::internal(format!(
                    "checkpoint commit failed: {e}"
                )));
            }

            // Phase 3: finalize commit under checkpoint inner lock.
            {
                let mut inner = self.coordinator.checkpoint_inner.write().await;
                let finalize_result = inner.finalize_ack(&job_id, ack_epoch);
                // Sync inner → outer coordinator even if finalization failed so
                // callers observe the authoritative checkpoint-inner state.
                let mut coordinator = self.coordinator.write().await;
                coordinator
                    .checkpoint_coordinators
                    .clone_from(&inner.coordinators);
                finalize_result.map_err(|error| {
                    tonic::Status::internal(format!("checkpoint finalize failed: {error}"))
                })?;
            }
        }

        Ok(tonic::Response::new(response))
    }
}

/// Management service implementation: routes CLI→coordinator RPCs (GAP-RT-04).
#[tonic::async_trait]
impl CoordinatorManagementService for CoordinatorExecutorTonicService {
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<TriggerSavepointRequest>,
    ) -> Result<tonic::Response<TriggerSavepointResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "trigger_savepoint");
        let req = request.into_inner();
        let label = if req.label.is_empty() {
            None
        } else {
            Some(req.label)
        };
        let mut coordinator = self.coordinator.write().await;
        let epoch = coordinator
            .savepoint_job(&req.job_id, label)
            .map_err(status_from_scheduler_error)?;
        Ok(tonic::Response::new(TriggerSavepointResponse {
            epoch,
            message: String::new(),
        }))
    }

    async fn restore_job(
        &self,
        request: tonic::Request<RestoreJobRequest>,
    ) -> Result<tonic::Response<RestoreJobResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_writer(&auth)?;
        tracing::debug!(subject = %auth.subject(), "restore_job");
        let req = request.into_inner();
        let leader_token = {
            let token = self
                .coordinator
                .leader_fencing_token
                .load(std::sync::atomic::Ordering::SeqCst);
            if token > 0 { Some(token) } else { None }
        };
        let mut checkpoint_inner = self.coordinator.checkpoint_inner.write().await;
        let mut coordinator = self.coordinator.write().await;
        match coordinator.activate_job_restore_from_checkpoint_with_fencing(
            &req.job_id,
            req.epoch,
            &req.storage_path,
            leader_token,
        ) {
            Ok(_meta) => {
                crate::coordinator_sharded::sync_checkpoint_to_inner(
                    &coordinator.checkpoint_coordinators,
                    &coordinator.checkpoint_notify_sent,
                    &coordinator.barrier_dispatch_sent,
                    &mut checkpoint_inner,
                );
                Ok(tonic::Response::new(RestoreJobResponse {
                    accepted: true,
                    message: format!(
                        "restore activated for job {} epoch {}",
                        req.job_id, req.epoch
                    ),
                }))
            }
            Err(e) => Ok(tonic::Response::new(RestoreJobResponse {
                accepted: false,
                message: e.to_string(),
            })),
        }
    }

    async fn list_checkpoints(
        &self,
        request: tonic::Request<ListCheckpointsRequest>,
    ) -> Result<tonic::Response<ListCheckpointsResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "list_checkpoints");
        let req = request.into_inner();
        let (epoch_nums, storage): (Vec<u64>, Option<Arc<dyn CheckpointStorage>>) = {
            let checkpoint_inner = self.coordinator.checkpoint_inner.read().await;
            match checkpoint_inner.coordinators.get(&req.job_id) {
                None => (vec![], None),
                Some(coord) => {
                    let epochs = coord
                        .list_epochs()
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;
                    (epochs, Some(Arc::clone(&coord.storage)))
                }
            }
        };
        // Enrich each epoch with savepoint metadata.
        // I/O is done outside the coordinator lock.
        let epochs = epoch_nums
            .into_iter()
            .map(|epoch| {
                let (is_savepoint, savepoint_label) = storage
                    .as_ref()
                    .and_then(|s| {
                        krishiv_state::checkpoint::read_epoch_metadata(
                            s.as_ref(),
                            req.job_id.as_str(),
                            epoch,
                        )
                        .ok()
                        .flatten()
                        .map(|m| (m.is_savepoint, m.savepoint_label.unwrap_or_default()))
                    })
                    .unwrap_or((false, String::new()));
                CheckpointEpochInfo {
                    epoch,
                    is_savepoint,
                    savepoint_label: if savepoint_label.is_empty() {
                        None
                    } else {
                        Some(savepoint_label)
                    },
                }
            })
            .collect();
        Ok(tonic::Response::new(ListCheckpointsResponse { epochs }))
    }

    async fn inspect_state(
        &self,
        request: tonic::Request<InspectStateRequest>,
    ) -> Result<tonic::Response<InspectStateResponse>, tonic::Status> {
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "inspect_state");
        let req = request.into_inner();
        // Read historical snapshots from durable checkpoint storage so that
        // the response includes snapshots from completed (already-acked) epochs,
        // not just the in-flight pending_acks map.
        let storage_opt = {
            let checkpoint_inner = self.coordinator.checkpoint_inner.read().await;
            checkpoint_inner
                .coordinators
                .get(&req.job_id)
                .map(|coord| Arc::clone(&coord.storage))
        };
        let Some(storage) = storage_opt else {
            return Ok(tonic::Response::new(InspectStateResponse {
                snapshots: vec![],
            }));
        };
        let epochs = krishiv_state::checkpoint::list_valid_epochs_async(
            storage.as_ref(),
            req.job_id.as_str(),
        )
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let mut snapshots = Vec::new();
        for epoch in epochs.into_iter().rev().take(20) {
            let Some(meta) = krishiv_state::checkpoint::read_epoch_metadata_async(
                storage.as_ref(),
                req.job_id.as_str(),
                epoch,
            )
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            else {
                continue;
            };
            for snap in &meta.operator_snapshots {
                if req.operator_id.is_empty() || snap.operator_id == req.operator_id {
                    snapshots.push(StateSnapshotInfo {
                        task_id: snap.task_id.clone(),
                        snapshot_path: snap.snapshot_path.clone(),
                    });
                }
            }
        }
        Ok(tonic::Response::new(InspectStateResponse { snapshots }))
    }
}

/// Networked gRPC adapter for coordinator/executor transport calls.
#[derive(Debug, Clone)]
pub struct CoordinatorExecutorGrpcService {
    inner: CoordinatorExecutorTonicService,
}

impl CoordinatorExecutorGrpcService {
    /// Create a network service from a shared coordinator.
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self {
            inner: CoordinatorExecutorTonicService::new(coordinator),
        }
    }

    /// Shared coordinator backing this service.
    pub fn coordinator(&self) -> &SharedCoordinator {
        self.inner.coordinator()
    }
}

#[tonic::async_trait]
impl wire::v1::coordinator_executor_server::CoordinatorExecutor for CoordinatorExecutorGrpcService {
    async fn register_executor(
        &self,
        request: tonic::Request<wire::v1::RegisterExecutorRequest>,
    ) -> Result<tonic::Response<wire::v1::RegisterExecutorResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let request = wire::register_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .register_executor(request_with_metadata(request, metadata))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::register_executor_response_to_wire(response),
        ))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<wire::v1::DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<wire::v1::DeregisterExecutorResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let request = wire::deregister_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .deregister_executor(request_with_metadata(request, metadata))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::deregister_executor_response_to_wire(response),
        ))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<wire::v1::ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<wire::v1::ExecutorHeartbeatResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let request = wire::executor_heartbeat_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .executor_heartbeat(request_with_metadata(request, metadata))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::executor_heartbeat_response_to_wire(response),
        ))
    }

    async fn task_status(
        &self,
        request: tonic::Request<wire::v1::TaskStatusRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let request = wire::task_status_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .task_status(request_with_metadata(request, metadata))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<wire::v1::CheckpointAckRequest>,
    ) -> Result<tonic::Response<wire::v1::CheckpointAckResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let request = wire::checkpoint_ack_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .checkpoint_ack(request_with_metadata(request, metadata))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::checkpoint_ack_response_to_wire(
            response,
        )))
    }
}

fn request_with_metadata<T>(
    message: T,
    metadata: tonic::metadata::MetadataMap,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    *request.metadata_mut() = metadata;
    request
}

async fn ensure_shared_coordinator_active(
    coordinator: &SharedCoordinator,
) -> Result<(), tonic::Status> {
    coordinator
        .read()
        .await
        .ensure_active()
        .map_err(status_from_scheduler_error)
}

/// gRPC adapter exposing the coordinator management service (GAP-RT-04).
///
/// Converts wire proto types to domain types, then delegates to
/// `CoordinatorExecutorTonicService::CoordinatorManagementService`.
#[derive(Debug, Clone)]
pub struct CoordinatorManagementGrpcService {
    inner: CoordinatorExecutorTonicService,
}

impl CoordinatorManagementGrpcService {
    pub fn new(coordinator: SharedCoordinator) -> Self {
        Self {
            inner: CoordinatorExecutorTonicService::new(coordinator),
        }
    }
}

#[tonic::async_trait]
impl wire::v1::coordinator_management_server::CoordinatorManagement
    for CoordinatorManagementGrpcService
{
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<wire::v1::TriggerSavepointRequest>,
    ) -> Result<tonic::Response<wire::v1::TriggerSavepointResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = TriggerSavepointRequest {
            job_id,
            label: w.label,
        };
        let resp = CoordinatorManagementService::trigger_savepoint(
            &self.inner,
            request_with_metadata(domain, metadata),
        )
        .await?
        .into_inner();
        Ok(tonic::Response::new(wire::v1::TriggerSavepointResponse {
            epoch: resp.epoch,
            message: String::new(),
        }))
    }

    async fn restore_job(
        &self,
        request: tonic::Request<wire::v1::RestoreJobRequest>,
    ) -> Result<tonic::Response<wire::v1::RestoreJobResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = RestoreJobRequest {
            job_id,
            epoch: w.epoch,
            storage_path: w.storage_path,
        };
        let resp = CoordinatorManagementService::restore_job(
            &self.inner,
            request_with_metadata(domain, metadata),
        )
        .await?
        .into_inner();
        Ok(tonic::Response::new(wire::v1::RestoreJobResponse {
            accepted: resp.accepted,
            message: resp.message,
        }))
    }

    async fn list_checkpoints(
        &self,
        request: tonic::Request<wire::v1::ListCheckpointsRequest>,
    ) -> Result<tonic::Response<wire::v1::ListCheckpointsResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = ListCheckpointsRequest { job_id };
        let resp = CoordinatorManagementService::list_checkpoints(
            &self.inner,
            request_with_metadata(domain, metadata),
        )
        .await?
        .into_inner();
        let epochs = resp
            .epochs
            .into_iter()
            .map(|e| wire::v1::CheckpointEpochInfo {
                epoch: e.epoch,
                is_savepoint: e.is_savepoint,
                savepoint_label: e.savepoint_label.unwrap_or_default(),
            })
            .collect();
        Ok(tonic::Response::new(wire::v1::ListCheckpointsResponse {
            epochs,
        }))
    }

    async fn inspect_state(
        &self,
        request: tonic::Request<wire::v1::InspectStateRequest>,
    ) -> Result<tonic::Response<wire::v1::InspectStateResponse>, tonic::Status> {
        let metadata = request.metadata().clone();
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = InspectStateRequest {
            job_id,
            operator_id: w.operator_id,
        };
        let resp = CoordinatorManagementService::inspect_state(
            &self.inner,
            request_with_metadata(domain, metadata),
        )
        .await?
        .into_inner();
        let snapshots = resp
            .snapshots
            .into_iter()
            .map(|s| wire::v1::StateSnapshotInfo {
                task_id: s.task_id,
                snapshot_path: s.snapshot_path,
            })
            .collect();
        Ok(tonic::Response::new(wire::v1::InspectStateResponse {
            snapshots,
        }))
    }
}

/// Build the generated tonic server around the scheduler-backed gRPC adapter.
pub fn coordinator_executor_grpc_server(
    coordinator: SharedCoordinator,
) -> wire::v1::coordinator_executor_server::CoordinatorExecutorServer<CoordinatorExecutorGrpcService>
{
    wire::v1::coordinator_executor_server::CoordinatorExecutorServer::new(
        CoordinatorExecutorGrpcService::new(coordinator),
    )
}

/// Build the coordinator management gRPC service (GAP-RT-04).
pub fn coordinator_management_grpc_server(
    coordinator: SharedCoordinator,
) -> wire::v1::coordinator_management_server::CoordinatorManagementServer<
    CoordinatorManagementGrpcService,
> {
    wire::v1::coordinator_management_server::CoordinatorManagementServer::new(
        CoordinatorManagementGrpcService::new(coordinator),
    )
}

/// Chained gRPC interceptor that extracts distributed tracing context and
/// validates client authentication on every incoming request.
fn trace_and_auth_interceptor(
    req: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    let req = krishiv_metrics::grpc::extract_trace_context(req)?;
    crate::auth::auth_interceptor(req)
}

/// Read `KRISHIV_TLS_CERT`, `KRISHIV_TLS_KEY`, and optionally `KRISHIV_CA_CERT`
/// to produce a [`tonic::transport::ServerTlsConfig`].
///
/// Returns `Ok(None)` when the env vars are absent (plaintext mode).
/// Returns an error when only one of cert/key is set, or when a file cannot be read.
pub fn server_tls_config_from_env(
) -> Result<Option<tonic::transport::ServerTlsConfig>, Box<dyn std::error::Error + Send + Sync>>
{
    let cert_path = std::env::var("KRISHIV_TLS_CERT").ok();
    let key_path = std::env::var("KRISHIV_TLS_KEY").ok();
    match (cert_path, key_path) {
        (Some(cert), Some(key)) => {
            let cert_pem = std::fs::read(&cert)
                .map_err(|e| format!("KRISHIV_TLS_CERT: cannot read {cert}: {e}"))?;
            let key_pem = std::fs::read(&key)
                .map_err(|e| format!("KRISHIV_TLS_KEY: cannot read {key}: {e}"))?;
            let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
            let mut tls = tonic::transport::ServerTlsConfig::new().identity(identity);
            if let Ok(ca_path) = std::env::var("KRISHIV_CA_CERT") {
                let ca_pem = std::fs::read(&ca_path)
                    .map_err(|e| format!("KRISHIV_CA_CERT: cannot read {ca_path}: {e}"))?;
                tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca_pem));
            }
            Ok(Some(tls))
        }
        (None, None) => Ok(None),
        _ => Err("KRISHIV_TLS_CERT and KRISHIV_TLS_KEY must both be set or both unset".into()),
    }
}

/// Serve the coordinator/executor gRPC API on an already-bound listener.
///
/// Equivalent to [`serve_coordinator_executor_grpc_with_listener_and_tracker`]
/// with a fresh, unobserved [`crate::rpc_drain::InFlightTracker`] — use that
/// function instead when the caller needs to drain in-flight calls before
/// shutdown (e.g. coordinator demotion).
pub async fn serve_coordinator_executor_grpc_with_listener(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    serve_coordinator_executor_grpc_with_listener_and_tracker(
        listener,
        coordinator,
        crate::rpc_drain::InFlightTracker::new(),
        None,
    )
    .await
}

/// Serve the coordinator/executor gRPC API, reporting every in-flight call
/// into `tracker` so the caller can drain outstanding RPCs before demoting
/// the coordinator (R11) instead of relying on a fixed sleep.
///
/// Pass `tls_config = Some(...)` to enable mutual-TLS.  Use
/// [`server_tls_config_from_env`] to load TLS material from the standard
/// `KRISHIV_TLS_CERT` / `KRISHIV_TLS_KEY` / `KRISHIV_CA_CERT` env vars.
pub async fn serve_coordinator_executor_grpc_with_listener_and_tracker(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
    tracker: crate::rpc_drain::InFlightTracker,
    tls_config: Option<tonic::transport::ServerTlsConfig>,
) -> Result<(), tonic::transport::Error> {
    let coordinator_for_management = coordinator.clone();
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls_config {
        builder = builder.tls_config(tls)?;
    }
    builder
        .layer(krishiv_metrics::grpc::GrpcDurationLayer)
        .layer(crate::rpc_drain::InFlightLayer::new(tracker))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            coordinator_executor_grpc_server(coordinator),
            trace_and_auth_interceptor,
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            coordinator_management_grpc_server(coordinator_for_management),
            trace_and_auth_interceptor,
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

fn ensure_transport_version(version: TransportVersion) -> Result<(), tonic::Status> {
    if TransportVersion::CURRENT.is_compatible_with(version) {
        Ok(())
    } else {
        Err(tonic::Status::invalid_argument(format!(
            "unsupported coordinator/executor transport version {version}; current version is {}",
            TransportVersion::CURRENT
        )))
    }
}

fn status_from_wire_error(error: wire::WireError) -> tonic::Status {
    tonic::Status::invalid_argument(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::request_with_metadata;

    #[test]
    fn request_with_metadata_preserves_authorization_header() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("Bearer coord-secret"),
        );

        let request = request_with_metadata((), metadata);

        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        assert_eq!(auth, Some("Bearer coord-secret"));
    }
}

pub(crate) fn status_from_scheduler_error(error: SchedulerError) -> tonic::Status {
    match error {
        SchedulerError::InactiveCoordinator { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::StaleExecutorLease { .. } | SchedulerError::StaleTaskAttempt { .. } => {
            tonic::Status::failed_precondition(error.to_string())
        }
        SchedulerError::UnknownExecutor { .. }
        | SchedulerError::UnknownJob { .. }
        | SchedulerError::UnknownStage { .. }
        | SchedulerError::UnknownTask { .. } => tonic::Status::not_found(error.to_string()),
        SchedulerError::DuplicateExecutor { .. } | SchedulerError::DuplicateJob { .. } => {
            tonic::Status::already_exists(error.to_string())
        }
        SchedulerError::NoExecutors
        | SchedulerError::InvalidJob { .. }
        | SchedulerError::InvalidPlan { .. }
        | SchedulerError::Optimizer(_) => tonic::Status::invalid_argument(error.to_string()),
        SchedulerError::Transport { .. }
        | SchedulerError::ExecutorUnavailable { .. }
        | SchedulerError::Store { .. } => tonic::Status::unavailable(error.to_string()),
    }
}
