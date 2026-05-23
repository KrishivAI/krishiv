//! Executor transport types: `ExecutorTransportError`, `ExecutorConfig`, `ExecutorRuntime`.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;

use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
    LeaseGeneration, RegisterExecutorRequest, RegisterExecutorResponse, TaskStatusRequest,
    TaskStatusResponse, TransportVersion, wire,
};

use crate::{ExecutorAssignmentInbox, ExecutorError, ExecutorResult, ExecutorTransportResult};
use crate::grpc::executor_task_grpc_server;

/// Network transport error raised by the executor gRPC client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorTransportError {
    /// The gRPC channel could not be created or used.
    Transport { message: String },
    /// The coordinator returned a gRPC status error.
    Status { message: String },
    /// A protobuf response could not be converted to a Krishiv contract.
    Wire { message: String },
}

impl fmt::Display for ExecutorTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport { message } => write!(f, "executor transport failed: {message}"),
            Self::Status { message } => write!(f, "coordinator rejected transport call: {message}"),
            Self::Wire { message } => write!(f, "invalid coordinator wire response: {message}"),
        }
    }
}

impl Error for ExecutorTransportError {}

impl From<tonic::transport::Error> for ExecutorTransportError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::Transport {
            message: value.to_string(),
        }
    }
}

impl From<tonic::Status> for ExecutorTransportError {
    fn from(value: tonic::Status) -> Self {
        Self::Status {
            message: value.to_string(),
        }
    }
}

impl From<wire::WireError> for ExecutorTransportError {
    fn from(value: wire::WireError) -> Self {
        Self::Wire {
            message: value.to_string(),
        }
    }
}

/// R3.1 executor startup configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorConfig {
    executor_id: ExecutorId,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    lease_generation: LeaseGeneration,
}

impl ExecutorConfig {
    /// Create executor configuration.
    pub fn new(
        executor_id: impl Into<String>,
        host: impl Into<String>,
        slots: usize,
        coordinator_endpoint: impl Into<String>,
    ) -> ExecutorResult<Self> {
        if slots == 0 {
            return Err(ExecutorError::InvalidSlots);
        }

        let coordinator_endpoint = coordinator_endpoint.into();
        if coordinator_endpoint.trim().is_empty() {
            return Err(ExecutorError::EmptyCoordinatorEndpoint);
        }

        let executor_id =
            ExecutorId::try_new(executor_id).map_err(|error| ExecutorError::InvalidExecutorId {
                message: error.to_string(),
            })?;

        Ok(Self {
            executor_id,
            host: host.into(),
            slots,
            coordinator_endpoint,
            lease_generation: LeaseGeneration::initial(),
        })
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Host or pod name advertised by the executor.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Advertised task slots.
    pub fn slots(&self) -> usize {
        self.slots
    }

    /// Coordinator endpoint the executor will connect to in a later R3.1 slice.
    pub fn coordinator_endpoint(&self) -> &str {
        &self.coordinator_endpoint
    }

    /// Current executor lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Update lease generation after coordinator registration or heartbeat (P0-8).
    pub fn set_lease_generation(&mut self, lease_generation: LeaseGeneration) {
        self.lease_generation = lease_generation;
    }

    /// Build an executor descriptor for registration.
    pub fn descriptor(&self) -> ExecutorDescriptor {
        ExecutorDescriptor::new(self.executor_id.clone(), self.host.clone(), self.slots)
    }
}

/// Minimal executor runtime facade for the R3.1 bootstrap slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRuntime {
    config: ExecutorConfig,
}

impl ExecutorRuntime {
    /// Create an executor runtime.
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }

    /// Runtime configuration.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Mutable runtime configuration (lease generation updates).
    pub fn config_mut(&mut self) -> &mut ExecutorConfig {
        &mut self.config
    }

    /// Apply lease generation from a coordinator transport response (P0-8).
    pub fn apply_lease_generation(&mut self, lease_generation: LeaseGeneration) {
        self.config.set_lease_generation(lease_generation);
    }

    /// Build the versioned registration request this executor will send.
    pub fn registration_request(&self) -> RegisterExecutorRequest {
        RegisterExecutorRequest::new(self.config.descriptor())
    }

    /// Register this executor through a tonic-shaped coordinator service.
    pub async fn register_with<S>(
        &self,
        service: &S,
    ) -> Result<RegisterExecutorResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .register_executor(tonic::Request::new(self.registration_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Build a deregistration request for this executor.
    pub fn deregistration_request(&self) -> DeregisterExecutorRequest {
        DeregisterExecutorRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
        )
        .with_reason("executor graceful shutdown")
    }

    /// Deregister this executor through a tonic-shaped coordinator service.
    pub async fn deregister_with<S>(
        &self,
        service: &S,
    ) -> Result<DeregisterExecutorResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .deregister_executor(tonic::Request::new(self.deregistration_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Build an empty healthy heartbeat request for this executor.
    pub fn heartbeat_request(&self) -> ExecutorHeartbeatRequest {
        ExecutorHeartbeatRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
            ExecutorState::Healthy,
        )
    }

    /// Build a heartbeat including LLM quota reports (R17).
    pub fn heartbeat_request_with_llm_quota(
        &self,
        reports: Vec<krishiv_proto::LlmQuotaReport>,
    ) -> ExecutorHeartbeatRequest {
        self.heartbeat_request().with_llm_quota_reports(reports)
    }

    /// Apply LLM throttle commands from a heartbeat response (R17).
    pub fn apply_llm_throttles_from_response(response: &ExecutorHeartbeatResponse) {
        for cmd in response.llm_throttles() {
            crate::llm_throttle::apply_llm_throttle(
                &cmd.model,
                cmd.max_requests_per_minute,
                cmd.max_tokens_per_minute,
            );
        }
    }

    /// Send a heartbeat through a tonic-shaped coordinator service.
    pub async fn heartbeat_with<S>(
        &self,
        service: &S,
    ) -> Result<ExecutorHeartbeatResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        service
            .executor_heartbeat(tonic::Request::new(self.heartbeat_request()))
            .await
            .map(tonic::Response::into_inner)
    }

    /// Register this executor through a networked coordinator gRPC endpoint.
    pub async fn register_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<RegisterExecutorResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::register_executor_request_to_wire(self.registration_request());
        let response = client.register_executor(request).await?.into_inner();
        Ok(wire::register_executor_response_from_wire(response)?)
    }

    /// Deregister this executor through a networked coordinator gRPC endpoint.
    pub async fn deregister_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<DeregisterExecutorResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::deregister_executor_request_to_wire(self.deregistration_request());
        let response = client.deregister_executor(request).await?.into_inner();
        Ok(wire::deregister_executor_response_from_wire(response)?)
    }

    /// Send one healthy heartbeat through a networked coordinator gRPC endpoint.
    pub async fn heartbeat_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<ExecutorHeartbeatResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let request = wire::executor_heartbeat_request_to_wire(self.heartbeat_request());
        let response = client.executor_heartbeat(request).await?.into_inner();
        Ok(wire::executor_heartbeat_response_from_wire(response)?)
    }

    /// Send a checkpoint acknowledgement to the coordinator over gRPC.
    pub async fn checkpoint_ack_with_grpc_endpoint(
        &self,
        request: CheckpointAckRequest,
    ) -> ExecutorTransportResult<CheckpointAckResponse> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;
        let wire_req = wire::checkpoint_ack_request_to_wire(request);
        let response = client.checkpoint_ack(wire_req).await?.into_inner();
        Ok(wire::checkpoint_ack_response_from_wire(response)?)
    }

    /// Register once and immediately send one heartbeat over gRPC.
    pub async fn register_and_heartbeat_once(
        &mut self,
    ) -> ExecutorTransportResult<(RegisterExecutorResponse, ExecutorHeartbeatResponse)> {
        let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
            self.config.coordinator_endpoint.clone(),
        )
        .await?;

        let registration = client
            .register_executor(wire::register_executor_request_to_wire(
                self.registration_request(),
            ))
            .await?
            .into_inner();
        let registration = wire::register_executor_response_from_wire(registration)?;
        self.apply_lease_generation(registration.lease_generation());

        let heartbeat = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                self.heartbeat_request(),
            ))
            .await?
            .into_inner();
        let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat)?;
        self.apply_lease_generation(heartbeat.lease_generation());

        Ok((registration, heartbeat))
    }

    /// Human-readable startup summary for the binary.
    pub fn startup_summary(&self) -> String {
        format!(
            "Krishiv executor {} ready for transport {} at {} with {} slot(s)",
            self.config.executor_id(),
            TransportVersion::CURRENT,
            self.config.coordinator_endpoint(),
            self.config.slots()
        )
    }
}

/// gRPC-backed `CoordinatorExecutorService` for the executor task runner loop.
///
/// GAP-CP-09: The task runner in `--connect` mode needs a `CoordinatorExecutorService`
/// to report task status (Running / Succeeded / Failed) after each assignment is
/// executed.
#[derive(Debug, Clone)]
pub struct GrpcCoordinatorService {
    endpoint: String,
    client: std::sync::Arc<
        tokio::sync::Mutex<
            Option<wire::v1::coordinator_executor_client::CoordinatorExecutorClient<
                tonic::transport::Channel,
            >>,
        >,
    >,
}

impl GrpcCoordinatorService {
    /// Create a gRPC coordinator service backed by `endpoint`.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn client(
        &self,
    ) -> Result<
        wire::v1::coordinator_executor_client::CoordinatorExecutorClient<tonic::transport::Channel>,
        tonic::Status,
    > {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))?,
            );
        }
        Ok(guard.as_ref().expect("client initialized").clone())
    }
}

#[tonic::async_trait]
impl CoordinatorExecutorService for GrpcCoordinatorService {
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let response = client
            .register_executor(wire::register_executor_request_to_wire(request.into_inner()))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::register_executor_response_from_wire(response)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let response = client
            .deregister_executor(wire::deregister_executor_request_to_wire(
                request.into_inner(),
            ))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::deregister_executor_response_from_wire(response)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let response = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(request.into_inner()))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::executor_heartbeat_response_from_wire(response)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let response = client
            .task_status(wire::task_status_request_to_wire(request.into_inner()))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::task_status_response_from_wire(response)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }

    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let response = client
            .checkpoint_ack(wire::checkpoint_ack_request_to_wire(request.into_inner()))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::checkpoint_ack_response_from_wire(response)
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
        ))
    }
}

/// Serve the executor task-assignment gRPC API on a socket address.
pub async fn serve_executor_task_grpc(
    addr: SocketAddr,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(executor_task_grpc_server(inbox))
        .serve(addr)
        .await
}

/// Serve the executor task-assignment gRPC API on an already-bound listener.
pub async fn serve_executor_task_grpc_with_listener(
    listener: tokio::net::TcpListener,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(executor_task_grpc_server(inbox))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}
