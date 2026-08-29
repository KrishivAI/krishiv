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
    http_endpoint: Option<String>,
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
            http_endpoint: None,
            barrier_endpoint: None,
            progress_buffer: None,
        })
    }

    /// Incarnation id for this executor *process*.
    ///
    /// Process-global rather than per-config: a config rebuilt inside one
    /// running process must not look like a restart to the coordinator, which
    /// fences a changed incarnation's in-flight work. Only a genuinely new
    /// process may present a new id.
    fn incarnation_id() -> &'static str {
        static INCARNATION_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        INCARNATION_ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
    }

    #[must_use]
    /// Advertised task gRPC endpoint, when one has been bound.
    pub fn task_endpoint(&self) -> Option<&str> {
        self.task_endpoint.as_deref()
    }

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

    /// Advertise the executor's HTTP endpoint (probes, metrics, log ring).
    #[must_use]
    pub fn with_http_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.http_endpoint = Some(endpoint.into());
        self
    }

    /// Build an executor descriptor for registration.
    pub fn descriptor(&self) -> ExecutorDescriptor {
        let mut d =
            ExecutorDescriptor::new(self.executor_id.clone(), self.host.clone(), self.slots)
                .with_incarnation_id(Self::incarnation_id());
        if let Some(ep) = &self.task_endpoint {
            d = d.with_task_endpoint(ep);
        }
        if let Some(ep) = &self.barrier_endpoint {
            d = d.with_barrier_endpoint(ep);
        }
        if let Some(ep) = &self.http_endpoint {
            d = d.with_http_endpoint(ep);
        }
        // Phase 53: advertise the rack for RACK_LOCAL placement. Node
        // identity is `host`; the rack comes from deployment topology.
        if let Ok(rack) = std::env::var("KRISHIV_RACK_ID")
            && !rack.trim().is_empty()
        {
            d = d.with_rack_id(rack);
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

    /// Drop the runtime heartbeat/registration channel so the next RPC
    /// resolves the coordinator Service again after an active-leader change.
    pub async fn invalidate_coordinator_channel(&self) {
        if let Some(pool) = self.pool.get() {
            pool.invalidate().await;
        }
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
        if let Some(ep) = task_endpoint
            && !ep.trim().is_empty()
        {
            self.config.task_endpoint = Some(ep);
        }
        if let Some(ep) = barrier_endpoint
            && !ep.trim().is_empty()
        {
            self.config.barrier_endpoint = Some(ep);
        }
    }

    /// Build the versioned registration request this executor will send.
    pub fn registration_request(&self) -> RegisterExecutorRequest {
        RegisterExecutorRequest::new(self.config.descriptor())
    }

    /// Register this executor through a tonic-shaped coordinator service.
    pub async fn register_with(
        &self,
        service: &dyn CoordinatorExecutorService,
    ) -> Result<RegisterExecutorResponse, tonic::Status> {
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
    pub async fn deregister_with(
        &self,
        service: &dyn CoordinatorExecutorService,
    ) -> Result<DeregisterExecutorResponse, tonic::Status> {
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

        // Read streaming progress snapshots (GAP-OB-04). Runner tasks write
        // into the buffer via ProgressBufferCallback; each heartbeat reports the
        // latest progress for every actively-streaming task.
        //
        // Reading does NOT clear. This used to `buf.clear()` right here, which
        // made *building* a request destroy state: if the heartbeat that
        // followed then failed, those reports were gone — and a coordinator
        // outage is exactly when every heartbeat fails and exactly when someone
        // is watching the freshness numbers. It also meant any caller who built
        // a request to inspect it silently drained the buffer.
        //
        // The buffer is cleared by [`clear_reported_progress`] once a heartbeat
        // has actually been accepted, which is also what keeps entries from
        // completed tasks from accumulating.
        let progress: Vec<krishiv_proto::StreamingProgressReport> = self
            .config
            .progress_buffer
            .as_ref()
            .map(|buf| buf.iter().map(|e| e.value().clone()).collect())
            .unwrap_or_default();

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

        // CPU core count available to this executor (cgroup-aware).
        req = req.with_cpu_cores_used(available_cpu_cores() as f64);

        // Cumulative network bytes (sent, recv) across all interfaces.
        if let Some((sent, recv)) = read_proc_net_bytes() {
            req = req
                .with_network_bytes_sent(sent)
                .with_network_bytes_recv(recv);
        }

        req
    }

    /// Drop the streaming progress snapshots a delivered heartbeat carried.
    ///
    /// Called only after the coordinator has the reports, so a failed heartbeat
    /// leaves them for the next one. Reports written *during* the RPC are also
    /// cleared, which is a much smaller window than the previous behaviour of
    /// discarding on every failure — and the next heartbeat is moments away.
    fn clear_reported_progress(&self) {
        if let Some(buf) = &self.config.progress_buffer {
            buf.clear();
        }
    }

    /// Send a heartbeat through a tonic-shaped coordinator service.
    pub async fn heartbeat_with(
        &self,
        service: &dyn CoordinatorExecutorService,
    ) -> Result<ExecutorHeartbeatResponse, tonic::Status> {
        let response = service
            .executor_heartbeat(tonic::Request::new(self.heartbeat_request()))
            .await
            .map(tonic::Response::into_inner)?;
        self.clear_reported_progress();
        Ok(response)
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
        self.clear_reported_progress();
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
        self.clear_reported_progress();
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
            wire::register_executor_response_from_wire(response).map_err(|e| {
                krishiv_metrics::grpc::internal_status("decode register_executor response", &e)
            })?,
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
            wire::deregister_executor_response_from_wire(response).map_err(|e| {
                krishiv_metrics::grpc::internal_status("decode deregister_executor response", &e)
            })?,
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
        let decoded = wire::executor_heartbeat_response_from_wire(response).map_err(|e| {
            krishiv_metrics::grpc::internal_status("decode executor_heartbeat response", &e)
        })?;
        // Only an accepted heartbeat carries an authoritative lease. A stale
        // response is the signal to re-register; adopting its generation here
        // would race the caller's recovery path and could temporarily unfence
        // an old process. The accepted registration response replaces the
        // shared lease, including a legitimate generation reset after failover.
        if decoded.disposition() == krishiv_proto::TransportDisposition::Accepted {
            self.pool.set_lease_generation(decoded.lease_generation());
        }
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
            wire::task_status_response_from_wire(response).map_err(|e| {
                krishiv_metrics::grpc::internal_status("decode task_status response", &e)
            })?,
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
            wire::checkpoint_ack_response_from_wire(response).map_err(|e| {
                krishiv_metrics::grpc::internal_status("decode checkpoint_ack response", &e)
            })?,
        ))
    }

    async fn push_task_result(
        &self,
        request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
    ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
        use futures::StreamExt as _;

        let mut client = self.client().await?;
        let mut typed_stream = request.into_inner();
        // Forward typed chunks into an mpsc channel and hand the tonic client
        // a concrete ReceiverStream: a `dyn Stream` request type trips rustc's
        // higher-ranked Send inference inside the generated client. The small
        // channel bound keeps at most a few chunks in flight.
        let (tx, rx) = tokio::sync::mpsc::channel::<wire::v1::TaskResultChunk>(4);
        let forwarder = tokio::spawn(async move {
            while let Some(item) = typed_stream.next().await {
                match item {
                    Ok(chunk) => {
                        if tx
                            .send(wire::task_result_chunk_to_wire(chunk))
                            .await
                            .is_err()
                        {
                            break; // receiver dropped: call already failed
                        }
                    }
                    Err(status) => {
                        // Truncate the stream; the server rejects it as
                        // "ended without a final chunk".
                        tracing::error!(%status, "task result chunk stream error; aborting push");
                        break;
                    }
                }
            }
        });
        let result = client
            .push_task_result(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await;
        let _ = forwarder.await;
        let response = result?.into_inner();
        Ok(tonic::Response::new(
            wire::push_task_result_response_from_wire(response).map_err(|e| {
                krishiv_metrics::grpc::internal_status("decode push_task_result response", &e)
            })?,
        ))
    }
}

/// Serve the executor task-assignment gRPC API on a socket address.
pub async fn serve_executor_task_grpc(
    addr: SocketAddr,
    inbox: ExecutorAssignmentInbox,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .layer(krishiv_metrics::grpc::GrpcDurationLayer)
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
        .layer(krishiv_metrics::grpc::GrpcDurationLayer)
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
        .layer(krishiv_metrics::grpc::GrpcDurationLayer)
        .add_service(tonic::service::interceptor::InterceptedService::new(
            executor_task_grpc_server_with_continuous(
                inbox,
                loop_executors,
                continuous_inputs,
                None,
            ),
            krishiv_metrics::grpc::extract_trace_context,
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Serve the executor task gRPC endpoint sharing the FULL continuous state
/// with the runner, including Phase 55 run-loop egress buffers and input
/// notifies (push wakes run-loops; drain serves their egress buffers).
#[allow(clippy::too_many_arguments)]
pub async fn serve_executor_task_grpc_with_run_loop(
    listener: tokio::net::TcpListener,
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
    continuous_outputs: crate::runner::SharedContinuousOutputs,
    input_notify: crate::runner::SharedContinuousNotify,
    continuous_connector_sources: crate::runner::SharedContinuousConnectorSources,
    class_executors: crate::grpc::SharedClassExecutors,
    egress_notify: crate::runner::SharedContinuousNotify,
    continuous_busy: crate::grpc::SharedContinuousBusy,
    continuous_egress_dropped: std::sync::Arc<dashmap::DashMap<String, u64>>,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .layer(krishiv_metrics::grpc::GrpcDurationLayer)
        .add_service(tonic::service::interceptor::InterceptedService::new(
            crate::grpc::executor_task_grpc_server_with_run_loop(
                inbox,
                loop_executors,
                continuous_inputs,
                continuous_outputs,
                input_notify,
                continuous_connector_sources,
                class_executors,
                egress_notify,
                continuous_busy,
                continuous_egress_dropped,
                None,
            ),
            krishiv_metrics::grpc::extract_trace_context,
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
}

/// Pull a `kB`-suffixed value out of a `/proc` key/value file.
///
/// `/proc/self/status` and `/proc/meminfo` share this shape, and both parsers
/// had their own copy of it. Split out so the parsing is testable without a
/// `/proc` to read: these run on every heartbeat and their output is what an
/// operator sees as an executor's memory, but nothing exercised them, because
/// the file path was baked into the function.
fn proc_kb_value(contents: &str, key: &str) -> Option<u64> {
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            let rest = rest.trim();
            // The unit is conventionally `kB` but the kernel has used `KB`, and
            // a bare number is not worth discarding either.
            let digits = rest
                .strip_suffix("kB")
                .or_else(|| rest.strip_suffix("KB"))
                .unwrap_or(rest)
                .trim();
            return digits.parse::<u64>().ok().map(|kb| kb * 1024);
        }
    }
    None
}

/// Read process RSS (resident set size) in bytes from `/proc/self/status`.
/// Returns `None` if the file cannot be read or parsed (e.g. non-Linux).
fn read_proc_rss_bytes() -> Option<u64> {
    proc_kb_value(
        &std::fs::read_to_string("/proc/self/status").ok()?,
        "VmRSS:",
    )
}

/// Read total system memory in bytes from `/proc/meminfo`.
/// Returns `None` if the file cannot be read or parsed.
fn read_proc_mem_total_bytes() -> Option<u64> {
    proc_kb_value(&std::fs::read_to_string("/proc/meminfo").ok()?, "MemTotal:")
}

/// Read the number of available CPU cores.
fn available_cpu_cores() -> u32 {
    // available_parallelism respects cgroup CPU limits in recent kernels
    // (Linux 5.13+) and container CPU pinning. Falls back to 1 on error.
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Read total network bytes sent and received from `/proc/net/dev`.
/// Returns `(sent, recv)` or `None` on non-Linux or parse failure.
fn read_proc_net_bytes() -> Option<(u64, u64)> {
    Some(parse_proc_net_dev(
        &std::fs::read_to_string("/proc/net/dev").ok()?,
    ))
}

/// Sum `(sent, recv)` bytes across real interfaces in `/proc/net/dev`.
///
/// Loopback is excluded. Every loopback byte is counted once as sent and once
/// as received, so including `lo` both double-counts and reports traffic that
/// never touched a wire — and an executor pod talks to its own sidecars and
/// local services over loopback constantly. The number this feeds is the
/// heartbeat's network counter, which exists to say how much of the cluster
/// interconnect a node is using; loopback is exactly the traffic that does not.
///
/// Format after the two header lines:
///
/// ```text
///     lo: 1234 10 0 0 0 0 0 0  1234 10 0 0 0 0 0 0
///   eth0: <recv bytes> ... <8 recv fields> <sent bytes> ...
/// ```
///
/// The interface name carries its colon, so `split_whitespace` puts recv bytes
/// at index 1 and sent bytes at index 9.
fn parse_proc_net_dev(contents: &str) -> (u64, u64) {
    let mut total_sent: u64 = 0;
    let mut total_recv: u64 = 0;
    for line in contents.lines().skip(2) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 10 {
            continue;
        }
        let name = parts.first().copied().unwrap_or("").trim_end_matches(':');
        if name == "lo" {
            continue;
        }
        let recv: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let sent: u64 = parts.get(9).and_then(|s| s.parse().ok()).unwrap_or(0);
        total_recv = total_recv.saturating_add(recv);
        total_sent = total_sent.saturating_add(sent);
    }
    (total_sent, total_recv)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod proc_parsing_tests {
    use super::*;

    /// Real `/proc/self/status` shape: tab-indented values, `kB` unit,
    /// `VmRSS` sitting among many similarly-prefixed keys.
    #[test]
    fn rss_is_read_from_the_right_line_and_scaled_to_bytes() {
        let status = "Name:\tkrishiv\nVmPeak:\t 9999999 kB\nVmSize:\t 8888888 kB\n\
                      VmRSS:\t 4194304 kB\nVmData:\t 123 kB\n";
        assert_eq!(proc_kb_value(status, "VmRSS:"), Some(4_194_304 * 1024));
        assert_eq!(proc_kb_value(status, "VmPeak:"), Some(9_999_999 * 1024));
    }

    #[test]
    fn meminfo_total_is_read_and_scaled() {
        let meminfo = "MemTotal:       65805312 kB\nMemFree:         1234 kB\n";
        assert_eq!(proc_kb_value(meminfo, "MemTotal:"), Some(65_805_312 * 1024));
    }

    /// A missing key, or a value that is not a number, must read as "unknown"
    /// rather than as zero. The heartbeat omits the field when this is `None`;
    /// reporting 0 bytes of RSS would look like a healthy idle executor.
    #[test]
    fn an_absent_or_unparseable_value_is_unknown_not_zero() {
        assert_eq!(proc_kb_value("MemFree: 1 kB\n", "VmRSS:"), None);
        assert_eq!(proc_kb_value("VmRSS:\t not-a-number kB\n", "VmRSS:"), None);
    }

    /// Loopback must not be counted.
    ///
    /// Every loopback byte appears once as sent and once as received, and an
    /// executor pod uses loopback heavily, so including `lo` both double-counts
    /// and reports traffic that never crossed the interconnect this counter
    /// exists to measure.
    #[test]
    fn loopback_is_excluded_from_the_network_counters() {
        let dev = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 1000000000 10 0 0 0 0 0 0 1000000000 10 0 0 0 0 0 0
  eth0:       7000 20 0 0 0 0 0 0       3000 20 0 0 0 0 0 0
";
        assert_eq!(
            parse_proc_net_dev(dev),
            (3000, 7000),
            "only eth0 should be counted, and (sent, recv) must not be swapped"
        );
    }

    /// Several real interfaces sum; short or malformed lines are skipped
    /// rather than poisoning the total.
    #[test]
    fn real_interfaces_sum_and_malformed_lines_are_skipped() {
        let dev = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  eth0: 100 1 0 0 0 0 0 0 200 1 0 0 0 0 0 0
 truncated: 1 2 3
  eth1: 300 1 0 0 0 0 0 0 400 1 0 0 0 0 0 0
";
        assert_eq!(parse_proc_net_dev(dev), (600, 400));
    }

    /// An empty or non-Linux file yields zeros, not a panic on `skip(2)`.
    #[test]
    fn an_empty_file_is_zero_not_a_panic() {
        assert_eq!(parse_proc_net_dev(""), (0, 0));
        assert_eq!(parse_proc_net_dev("only one line\n"), (0, 0));
    }
}
