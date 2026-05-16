#![forbid(unsafe_code)]

//! R3.1 executor process skeleton.
//!
//! This crate owns executor-side process configuration, versioned
//! coordinator/executor transport requests, and the first networked gRPC client
//! path. The task runner loop and DataFusion execution path land in later R3.1
//! slices.

use std::error::Error;
use std::fmt;

use krishiv_proto::{
    CoordinatorExecutorService, ExecutorDescriptor, ExecutorHeartbeatRequest,
    ExecutorHeartbeatResponse, ExecutorId, ExecutorState, LeaseGeneration, RegisterExecutorRequest,
    RegisterExecutorResponse, TransportVersion, wire,
};

/// Executor crate result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor transport result alias.
pub type ExecutorTransportResult<T> = Result<T, ExecutorTransportError>;

/// Executor configuration or startup error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    /// Executor id failed validation.
    InvalidExecutorId { message: String },
    /// Task slots must be greater than zero.
    InvalidSlots,
    /// Coordinator endpoint cannot be empty.
    EmptyCoordinatorEndpoint,
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutorId { message } => write!(f, "invalid executor id: {message}"),
            Self::InvalidSlots => f.write_str("task slots must be greater than zero"),
            Self::EmptyCoordinatorEndpoint => f.write_str("coordinator endpoint cannot be empty"),
        }
    }
}

impl Error for ExecutorError {}

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

    /// Build an empty healthy heartbeat request for this executor.
    pub fn heartbeat_request(&self) -> ExecutorHeartbeatRequest {
        ExecutorHeartbeatRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
            ExecutorState::Healthy,
        )
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

    /// Register once and immediately send one heartbeat over gRPC.
    pub async fn register_and_heartbeat_once(
        &self,
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

        let heartbeat = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                self.heartbeat_request(),
            ))
            .await?
            .into_inner();
        let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat)?;

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

#[cfg(test)]
mod tests {
    use krishiv_proto::{
        CoordinatorExecutorService, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
        ExecutorState, LeaseGeneration, RegisterExecutorRequest, RegisterExecutorResponse,
        TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion,
    };

    use super::{ExecutorConfig, ExecutorError, ExecutorRuntime};

    struct AcceptingCoordinatorService;

    #[tonic::async_trait]
    impl CoordinatorExecutorService for AcceptingCoordinatorService {
        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }
    }

    #[test]
    fn config_rejects_invalid_values() {
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 0, "http://coordinator"),
            Err(ExecutorError::InvalidSlots)
        ));
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 1, " "),
            Err(ExecutorError::EmptyCoordinatorEndpoint)
        ));
    }

    #[test]
    fn runtime_builds_versioned_registration_request() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 2, "http://coordinator").unwrap(),
        );
        let request = runtime.registration_request();

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().executor_id().as_str(), "exec-1");
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn runtime_builds_heartbeat_with_initial_lease() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let heartbeat = runtime.heartbeat_request();

        assert_eq!(heartbeat.state(), ExecutorState::Healthy);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert!(heartbeat.running_attempts().is_empty());
    }

    #[tokio::test]
    async fn runtime_registers_and_heartbeats_through_service_boundary() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let service = AcceptingCoordinatorService;

        let registration = runtime.register_with(&service).await.unwrap();
        let heartbeat = runtime.heartbeat_with(&service).await.unwrap();

        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
    }
}
