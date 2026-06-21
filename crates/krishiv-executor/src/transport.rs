//! Executor transport types: `ExecutorTransportError`, `ExecutorConfig`, `ExecutorRuntime`.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
    DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
    ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
    LeaseGeneration, RegisterExecutorRequest, RegisterExecutorResponse, TaskAttemptRef,
    TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion, wire,
};

use crate::grpc::{
    SharedContinuousInputs, SharedLoopExecutors, executor_task_grpc_server,
    executor_task_grpc_server_with_continuous,
};
use crate::{ExecutorAssignmentInbox, ExecutorError, ExecutorResult, ExecutorTransportResult};

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
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    executor_id: ExecutorId,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    lease_generation: LeaseGeneration,
    task_endpoint: Option<String>,
    barrier_endpoint: Option<String>,
    progress_buffer: Option<Arc<dashmap::DashMap<String, krishiv_proto::StreamingProgressReport>>>,
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
            task_endpoint: None,
            barrier_endpoint: None,
            progress_buffer: None,
        })
    }

    #[must_use]
    pub fn with_task_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        if !endpoint.trim().is_empty() {
            self.task_endpoint = Some(endpoint);
        }
        self
    }

    #[must_use]
    pub fn with_barrier_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        if !endpoint.trim().is_empty() {
            self.barrier_endpoint = Some(endpoint);
        }
        self
    }

    /// Attach a shared streaming progress buffer (builder form).
    #[must_use]
    pub fn with_progress_buffer(
        mut self,
        buffer: Arc<dashmap::DashMap<String, krishiv_proto::StreamingProgressReport>>,
    ) -> Self {
        self.progress_buffer = Some(buffer);
        self
    }

    /// Attach a shared streaming progress buffer (setter form, for post-construction wiring).
    pub fn set_progress_buffer(
        &mut self,
        buffer: Arc<dashmap::DashMap<String, krishiv_proto::StreamingProgressReport>>,
    ) {
        self.progress_buffer = Some(buffer);
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

    /// Update lease generation after coordinator registration or heartbeat (GAP-C4).
    pub fn set_lease_generation(&mut self, lease_generation: LeaseGeneration) {
        self.lease_generation = lease_generation;
    }

    /// Build an executor descriptor for registration.
    pub fn descriptor(&self) -> ExecutorDescriptor {
        let mut d =
            ExecutorDescriptor::new(self.executor_id.clone(), self.host.clone(), self.slots);
        if let Some(ep) = &self.task_endpoint {
            d = d.with_task_endpoint(ep);
        }
        if let Some(ep) = &self.barrier_endpoint {
            d = d.with_barrier_endpoint(ep);
        }
        d
    }
}

/// Minimal executor runtime facade for the R3.1 bootstrap slice.
#[derive(Debug, Clone)]
pub struct ExecutorRuntime {
    config: ExecutorConfig,
    running_attempts: Option<Arc<DashMap<String, TaskAttemptRef>>>,
    pool: Arc<tokio::sync::OnceCell<crate::grpc_client::CoordinatorGrpcPool>>,
}

impl ExecutorRuntime {
    /// Create an executor runtime.
    pub fn new(config: ExecutorConfig) -> Self {
        Self {
            config,
            running_attempts: None,
            pool: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// Wire a shared running-attempts map so heartbeats report live tasks (P1-19).
    pub fn set_running_attempts(&mut self, running_attempts: Arc<DashMap<String, TaskAttemptRef>>) {
        self.running_attempts = Some(running_attempts);
    }

    /// Runtime configuration.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Mutable access to runtime configuration.
    pub fn config_mut(&mut self) -> &mut ExecutorConfig {
        &mut self.config
    }

    /// Apply coordinator-issued lease generation from register/heartbeat responses.
    pub fn apply_lease_generation(&mut self, lease_generation: LeaseGeneration) {
        self.config.set_lease_generation(lease_generation);
    }

    /// Update advertised task/barrier gRPC endpoints after listeners bind.
    pub fn set_advertised_endpoints(
        &mut self,
        task_endpoint: Option<String>,
        barrier_endpoint: Option<String>,
    ) {
        if let Some(ep) = task_endpoint {
            self.config = self.config.clone().with_task_endpoint(ep);
        }
        if let Some(ep) = barrier_endpoint {
            self.config = self.config.clone().with_barrier_endpoint(ep);
        }
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
        let attempts: Vec<TaskAttemptRef> = self
            .running_attempts
            .as_ref()
            .map(|map| map.iter().map(|entry| entry.value().clone()).collect())
            .unwrap_or_default();
        let active_count = attempts.len() as u32;

        // Drain streaming progress snapshots (GAP-OB-04). Runner tasks write
        // into the buffer via ProgressBufferCallback; we drain here so each
        // heartbeat reports the latest progress for every actively-streaming
        // task.  The buffer is cleared after reading so stale entries from
        // completed tasks do not accumulate.
        let progress: Vec<krishiv_proto::StreamingProgressReport> =
            if let Some(buf) = &self.config.progress_buffer {
                let reports: Vec<_> = buf.iter().map(|e| e.value().clone()).collect();
                buf.clear();
                reports
            } else {
                Vec::new()
            };

        let mut req = ExecutorHeartbeatRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
            ExecutorState::Healthy,
        )
        .with_running_attempts(attempts)
        .with_streaming_progress(progress)
        .with_active_task_count(active_count);

        let rss = read_proc_rss_bytes();
        let total = read_proc_mem_total_bytes();
        if let Some(bytes) = rss {
            req = req.with_memory_used_bytes(bytes);
        }
        if let Some(bytes) = total {
            req = req.with_memory_limit_bytes(bytes);
        }

        req
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

    /// Build an intercepted coordinator gRPC client from the pooled client connection.
    async fn connect_coordinator_client(
        &self,
    ) -> ExecutorTransportResult<
        wire::v1::coordinator_executor_client::CoordinatorExecutorClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            >,
        >,
    > {
        let pool = self
            .pool
            .get_or_init(|| async {
                crate::grpc_client::CoordinatorGrpcPool::new(
                    self.config.coordinator_endpoint.clone(),
                    self.config.lease_generation,
                )
            })
            .await;

        // Propagate current lease generation to the pool
        pool.set_lease_generation(self.config.lease_generation);

        pool.client()
            .await
            .map_err(|e| ExecutorTransportError::Transport {
                message: e.to_string(),
            })
    }

    /// Register this executor through a networked coordinator gRPC endpoint.
    pub async fn register_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<RegisterExecutorResponse> {
        let mut client = self.connect_coordinator_client().await?;
        let request = wire::register_executor_request_to_wire(self.registration_request());
        let response = client.register_executor(request).await?.into_inner();
        Ok(wire::register_executor_response_from_wire(response)?)
    }

    /// Deregister this executor through a networked coordinator gRPC endpoint.
    pub async fn deregister_with_grpc_endpoint(
        &self,
    ) -> ExecutorTransportResult<DeregisterExecutorResponse> {
        let mut client = self.connect_coordinator_client().await?;
        let request = wire::deregister_executor_request_to_wire(self.deregistration_request());
        let response = client.deregister_executor(request).await?.into_inner();
        Ok(wire::deregister_executor_response_from_wire(response)?)
    }

    /// Send one healthy heartbeat through a networked coordinator gRPC endpoint.
    pub async fn heartbeat_with_grpc_endpoint(
        &mut self,
    ) -> ExecutorTransportResult<ExecutorHeartbeatResponse> {
        let mut client = self.connect_coordinator_client().await?;
        let request = wire::executor_heartbeat_request_to_wire(self.heartbeat_request());
        let response = client.executor_heartbeat(request).await?.into_inner();
        Ok(wire::executor_heartbeat_response_from_wire(response)?)
    }

    /// Send a checkpoint acknowledgement to the coordinator over gRPC.
    pub async fn checkpoint_ack_with_grpc_endpoint(
        &self,
        request: CheckpointAckRequest,
    ) -> ExecutorTransportResult<CheckpointAckResponse> {
        let mut client = self.connect_coordinator_client().await?;
        let wire_req = wire::checkpoint_ack_request_to_wire(request);
        let response = client.checkpoint_ack(wire_req).await?.into_inner();
        Ok(wire::checkpoint_ack_response_from_wire(response)?)
    }

    /// Register once and immediately send one heartbeat over gRPC.
    pub async fn register_and_heartbeat_once(
        &mut self,
    ) -> ExecutorTransportResult<(RegisterExecutorResponse, ExecutorHeartbeatResponse)> {
        let mut client = self.connect_coordinator_client().await?;

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
        if heartbeat.disposition() == TransportDisposition::Accepted {
            self.apply_lease_generation(heartbeat.lease_generation());
        }

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

/// gRPC-backed `CoordinatorExecutorService` with pooled client (GAP-C3, B7).
///
/// Stamps the live executor lease generation onto every outbound request so
/// that retries after a lease bump cannot ship a stale lease.  The lease is
/// shared via [`SharedLeaseGeneration`] with the executor heartbeat loop and
/// with re-registration paths.
#[derive(Clone)]
pub struct GrpcCoordinatorService {
    pool: crate::grpc_client::CoordinatorGrpcPool,
}

impl fmt::Debug for GrpcCoordinatorService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcCoordinatorService")
            .field("endpoint", &self.pool.endpoint())
            .finish_non_exhaustive()
    }
}

impl GrpcCoordinatorService {
    pub fn new(endpoint: impl Into<String>, lease_generation: LeaseGeneration) -> Self {
        Self {
            pool: crate::grpc_client::CoordinatorGrpcPool::new(endpoint, lease_generation),
        }
    }

    /// Build a service that shares its lease atomic with the caller (executor binary).
    pub fn with_shared_lease(
        endpoint: impl Into<String>,
        lease: crate::grpc_client::SharedLeaseGeneration,
    ) -> Self {
        Self {
            pool: crate::grpc_client::CoordinatorGrpcPool::with_shared_lease(endpoint, lease),
        }
    }

    /// Handle for the shared lease atomic.
    pub fn lease_handle(&self) -> crate::grpc_client::SharedLeaseGeneration {
        self.pool.lease_handle()
    }

    /// Invalidate the cached gRPC channel (e.g. after a stale-lease error).
    pub async fn invalidate_channel(&self) {
        self.pool.invalidate().await;
    }

    fn live_lease(&self) -> LeaseGeneration {
        self.pool.lease_generation()
    }

    async fn client(
        &self,
    ) -> Result<
        wire::v1::coordinator_executor_client::CoordinatorExecutorClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            >,
        >,
        tonic::Status,
    > {
        self.pool
            .client()
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))
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
            .register_executor(wire::register_executor_request_to_wire(
                request.into_inner(),
            ))
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
        // Stamp the live lease before forwarding so retries after a lease bump
        // do not ship a stale generation (B7).
        let mut hb = request.into_inner();
        hb = hb.with_lease_generation(self.live_lease());
        let response = client
            .executor_heartbeat(wire::executor_heartbeat_request_to_wire(hb))
            .await?
            .into_inner();
        let decoded = wire::executor_heartbeat_response_from_wire(response)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        // Coordinator's authoritative lease — propagate immediately.
        self.pool.set_lease_generation(decoded.lease_generation());
        Ok(tonic::Response::new(decoded))
    }

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let mut client = self.client().await?;
        let req = request
            .into_inner()
            .with_lease_generation(self.live_lease());
        let response = client
            .task_status(wire::task_status_request_to_wire(req))
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
        let req = request.into_inner();
        let response = client
            .checkpoint_ack(wire::checkpoint_ack_request_to_wire(req))
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
        .add_service(tonic::service::interceptor::InterceptedService::new(
            executor_task_grpc_server(inbox),
            krishiv_metrics::grpc::extract_trace_context,
        ))
        .serve(addr)
        .await
}

/// Serve the executor task-assignment gRPC API on an already-bound listener.
pub async fn serve_executor_task_grpc_with_listener(
    listener: tokio::net::TcpListener,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(tonic::service::interceptor::InterceptedService::new(
            executor_task_grpc_server(inbox),
            krishiv_metrics::grpc::extract_trace_context,
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Serve the executor task-assignment gRPC API sharing continuous state with a runner.
///
/// Use this variant in the executor CLI to share `loop_executors` and
/// `continuous_inputs` with the `ExecutorTaskRunner` so that distributed
/// `push_continuous_input` / `drain_continuous_output` RPCs operate on the
/// same window executor state as `execute_loop_fragment`.
pub async fn serve_executor_task_grpc_with_listener_and_continuous(
    listener: tokio::net::TcpListener,
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(tonic::service::interceptor::InterceptedService::new(
            executor_task_grpc_server_with_continuous(inbox, loop_executors, continuous_inputs),
            krishiv_metrics::grpc::extract_trace_context,
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Read process RSS (resident set size) in bytes from `/proc/self/status`.
/// Returns `None` if the file cannot be read or parsed (e.g. non-Linux).
fn read_proc_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().strip_suffix(" kB")?.trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Read total system memory in bytes from `/proc/meminfo`.
/// Returns `None` if the file cannot be read or parsed.
fn read_proc_mem_total_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.trim().strip_suffix(" kB")?.trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}
