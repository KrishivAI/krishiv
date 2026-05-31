//! Coordinator gRPC adapters.

use std::net::SocketAddr;
use std::sync::Arc;

use krishiv_checkpoint::CheckpointStorage;
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

use crate::auth::{extract_auth_context, validate_grpc_auth};
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
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "register_executor");
        let request = request.into_inner();
        ensure_transport_version(request.version())?;

        let descriptor = request.descriptor().clone();
        let executor_id = descriptor.executor_id().clone();

        // Phase 1: register under dedicated executor inner lock (H2 sharding).
        let response = {
            let mut executor_inner = self.coordinator.executor_inner.write().await;
            match executor_inner.register_executor(descriptor) {
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

        // Phase 2: sync executor state from inner → outer coordinator.
        {
            let inner = self.coordinator.executor_inner.read().await;
            let mut coordinator = self.coordinator.write().await;
            coordinator.executors.clone_from(&inner.executors);
        }

        Ok(tonic::Response::new(response))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
        // defense-in-depth: redundant when server-level interceptor is active
        let auth = extract_auth_context(request.metadata());
        validate_grpc_auth(&auth)?;
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
        validate_grpc_auth(&auth)?;
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
        if !request.llm_quota_reports().is_empty() {
            heartbeat = heartbeat.with_llm_quota_reports(request.llm_quota_reports().to_vec());
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
                if !effects.llm_throttles.is_empty() {
                    resp = resp.with_llm_throttles(effects.llm_throttles);
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
        validate_grpc_auth(&auth)?;
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
        validate_grpc_auth(&auth)?;
        tracing::debug!(subject = %auth.subject(), "checkpoint_ack");
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
                inner.finalize_ack(&job_id, ack_epoch);
                // Sync inner → outer coordinator.
                let mut coordinator = self.coordinator.write().await;
                coordinator
                    .checkpoint_coordinators
                    .clone_from(&inner.coordinators);
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
        let req = request.into_inner();
        let label = if req.label.is_empty() {
            None
        } else {
            Some(req.label)
        };
        let mut coordinator = self.coordinator.write().await;
        let epoch = coordinator
            .savepoint_job(&req.job_id, label)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(TriggerSavepointResponse { epoch }))
    }

    async fn restore_job(
        &self,
        request: tonic::Request<RestoreJobRequest>,
    ) -> Result<tonic::Response<RestoreJobResponse>, tonic::Status> {
        let req = request.into_inner();
        let coordinator = self.coordinator.read().await;
        match coordinator.restore_job_from_checkpoint(&req.job_id, req.epoch, &req.storage_path) {
            Ok(_meta) => Ok(tonic::Response::new(RestoreJobResponse {
                accepted: true,
                message: format!(
                    "restore plan loaded for job {} epoch {}",
                    req.job_id, req.epoch
                ),
            })),
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
        let req = request.into_inner();
        let (epoch_nums, storage): (Vec<u64>, Option<Arc<dyn CheckpointStorage>>) = {
            let coordinator = self.coordinator.read().await;
            let epochs = coordinator
                .list_job_checkpoints(&req.job_id)
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            let storage = coordinator
                .checkpoint_coordinator(&req.job_id)
                .map(|c| Arc::clone(&c.storage));
            (epochs, storage)
        };
        // Enrich each epoch with savepoint metadata.
        // I/O is done outside the coordinator lock.
        let epochs = epoch_nums
            .into_iter()
            .map(|epoch| {
                let (is_savepoint, savepoint_label) = storage
                    .as_ref()
                    .and_then(|s| {
                        krishiv_checkpoint::read_epoch_metadata(
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
        let req = request.into_inner();
        let coordinator = self.coordinator.read().await;
        // Collect snapshot paths for the requested operator from the checkpoint coordinator.
        let snapshots = coordinator
            .checkpoint_coordinator(&req.job_id)
            .map(|coord| {
                coord
                    .pending_acks
                    .values()
                    .filter(|ack| req.operator_id.is_empty() || ack.operator_id == req.operator_id)
                    .filter_map(|ack| {
                        ack.snapshot_path.as_ref().map(|path| StateSnapshotInfo {
                            task_id: ack.task_id.as_str().to_owned(),
                            snapshot_path: path.clone(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
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
        let request = wire::register_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .register_executor(tonic::Request::new(request))
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
        let request = wire::deregister_executor_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .deregister_executor(tonic::Request::new(request))
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
        let request = wire::executor_heartbeat_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .executor_heartbeat(tonic::Request::new(request))
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
        let request = wire::task_status_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .task_status(tonic::Request::new(request))
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
        let request = wire::checkpoint_ack_request_from_wire(request.into_inner())
            .map_err(status_from_wire_error)?;
        let response = self
            .inner
            .checkpoint_ack(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::checkpoint_ack_response_to_wire(
            response,
        )))
    }
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
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = TriggerSavepointRequest {
            job_id,
            label: w.label,
        };
        let resp = CoordinatorManagementService::trigger_savepoint(
            &self.inner,
            tonic::Request::new(domain),
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
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = RestoreJobRequest {
            job_id,
            epoch: w.epoch,
            storage_path: w.storage_path,
        };
        let resp =
            CoordinatorManagementService::restore_job(&self.inner, tonic::Request::new(domain))
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
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = ListCheckpointsRequest { job_id };
        let resp = CoordinatorManagementService::list_checkpoints(
            &self.inner,
            tonic::Request::new(domain),
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
        let w = request.into_inner();
        let job_id = JobId::try_new(w.job_id)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid job_id: {e}")))?;
        let domain = InspectStateRequest {
            job_id,
            operator_id: w.operator_id,
        };
        let resp =
            CoordinatorManagementService::inspect_state(&self.inner, tonic::Request::new(domain))
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

/// Serve the coordinator/executor gRPC API on a socket address.
#[allow(dead_code)]
pub async fn serve_coordinator_executor_grpc(
    addr: SocketAddr,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    let coordinator_for_management = coordinator.clone();
    tonic::transport::Server::builder()
        .add_service(tonic::service::interceptor::InterceptedService::new(
            coordinator_executor_grpc_server(coordinator),
            trace_and_auth_interceptor,
        ))
        .add_service(tonic::service::interceptor::InterceptedService::new(
            coordinator_management_grpc_server(coordinator_for_management),
            trace_and_auth_interceptor,
        ))
        .serve(addr)
        .await
}

/// Serve the coordinator/executor gRPC API on an already-bound listener.
pub async fn serve_coordinator_executor_grpc_with_listener(
    listener: tokio::net::TcpListener,
    coordinator: SharedCoordinator,
) -> Result<(), tonic::transport::Error> {
    let coordinator_for_management = coordinator.clone();
    tonic::transport::Server::builder()
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
        | SchedulerError::InvalidPlan { .. } => tonic::Status::invalid_argument(error.to_string()),
        SchedulerError::Transport { .. } | SchedulerError::ExecutorUnavailable { .. } => {
            tonic::Status::unavailable(error.to_string())
        }
    }
}
