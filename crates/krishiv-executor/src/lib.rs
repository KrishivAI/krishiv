#![forbid(unsafe_code)]

//! R3.1 executor process skeleton.
//!
//! This crate owns executor-side process configuration, versioned
//! coordinator/executor transport requests, and the first networked gRPC client
//! path. The minimal task runner skeleton lands here; the DataFusion execution
//! path lands in a later R3.1 slice.

use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use krishiv_proto::{
    CoordinatorExecutorService, ExecutorDescriptor, ExecutorHeartbeatRequest,
    ExecutorHeartbeatResponse, ExecutorId, ExecutorState, ExecutorTaskAssignment,
    ExecutorTaskService, LeaseGeneration, RegisterExecutorRequest, RegisterExecutorResponse,
    TaskAttemptRef, TaskState, TaskStatusRequest, TaskStatusResponse, TransportDisposition,
    TransportVersion, wire,
};
use krishiv_sql::SqlEngine;

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
    /// The executor assignment inbox lock was poisoned.
    AssignmentInboxPoisoned,
    /// A received task assignment cannot be executed.
    InvalidAssignment { message: String },
    /// Local stage fragment execution failed.
    LocalExecution { message: String },
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutorId { message } => write!(f, "invalid executor id: {message}"),
            Self::InvalidSlots => f.write_str("task slots must be greater than zero"),
            Self::EmptyCoordinatorEndpoint => f.write_str("coordinator endpoint cannot be empty"),
            Self::AssignmentInboxPoisoned => f.write_str("executor assignment inbox is poisoned"),
            Self::InvalidAssignment { message } => write!(f, "invalid task assignment: {message}"),
            Self::LocalExecution { message } => {
                write!(f, "local stage fragment execution failed: {message}")
            }
        }
    }
}

impl Error for ExecutorError {}

/// In-memory receiver queue for task assignments delivered to an executor.
#[derive(Debug, Clone, Default)]
pub struct ExecutorAssignmentInbox {
    assignments: Arc<RwLock<Vec<ExecutorTaskAssignment>>>,
}

impl ExecutorAssignmentInbox {
    /// Create an empty assignment inbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store one received assignment.
    pub fn push(&self, assignment: ExecutorTaskAssignment) -> ExecutorResult<()> {
        self.assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .push(assignment);
        Ok(())
    }

    /// Remove the next received assignment in FIFO order.
    pub fn pop_next(&self) -> ExecutorResult<Option<ExecutorTaskAssignment>> {
        let mut assignments = self
            .assignments
            .write()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?;
        if assignments.is_empty() {
            Ok(None)
        } else {
            Ok(Some(assignments.remove(0)))
        }
    }

    /// Snapshot all received assignments.
    pub fn assignments(&self) -> ExecutorResult<Vec<ExecutorTaskAssignment>> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .clone())
    }

    /// Number of assignments received so far.
    pub fn len(&self) -> ExecutorResult<usize> {
        Ok(self
            .assignments
            .read()
            .map_err(|_| ExecutorError::AssignmentInboxPoisoned)?
            .len())
    }

    /// Whether the inbox is empty.
    pub fn is_empty(&self) -> ExecutorResult<bool> {
        Ok(self.len()? == 0)
    }
}

/// Result of one executor-side task runner pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskRunReport {
    assignment: ExecutorTaskAssignment,
    output: ExecutorTaskOutput,
    running_disposition: TransportDisposition,
    terminal_disposition: TransportDisposition,
}

impl ExecutorTaskRunReport {
    fn new(
        assignment: ExecutorTaskAssignment,
        output: ExecutorTaskOutput,
        running_disposition: TransportDisposition,
        terminal_disposition: TransportDisposition,
    ) -> Self {
        Self {
            assignment,
            output,
            running_disposition,
            terminal_disposition,
        }
    }

    /// Assignment processed by this runner pass.
    pub fn assignment(&self) -> &ExecutorTaskAssignment {
        &self.assignment
    }

    /// Local output metadata produced by this runner pass.
    pub fn output(&self) -> &ExecutorTaskOutput {
        &self.output
    }

    /// Coordinator response to the `Running` status update.
    pub fn running_disposition(&self) -> TransportDisposition {
        self.running_disposition
    }

    /// Coordinator response to the terminal status update.
    pub fn terminal_disposition(&self) -> TransportDisposition {
        self.terminal_disposition
    }
}

/// Local executor output metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskOutput {
    kind: ExecutorTaskOutputKind,
    row_count: usize,
    batch_count: usize,
    column_count: usize,
}

impl ExecutorTaskOutput {
    fn sql(row_count: usize, batch_count: usize, column_count: usize) -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Sql,
            row_count,
            batch_count,
            column_count,
        }
    }

    fn placeholder() -> Self {
        Self {
            kind: ExecutorTaskOutputKind::Placeholder,
            row_count: 0,
            batch_count: 0,
            column_count: 0,
        }
    }

    /// Output kind.
    pub fn kind(&self) -> ExecutorTaskOutputKind {
        self.kind
    }

    /// Number of rows produced locally.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Number of Arrow record batches produced locally.
    pub fn batch_count(&self) -> usize {
        self.batch_count
    }

    /// Number of columns in the local output schema.
    pub fn column_count(&self) -> usize {
        self.column_count
    }
}

/// Local executor output kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorTaskOutputKind {
    /// Real SQL fragment executed through the Krishiv SQL/DataFusion seam.
    Sql,
    /// Placeholder path for non-SQL fragments while R3.1 is still bootstrapping.
    Placeholder,
}

/// Minimal R3.1 stage-local task runner skeleton.
#[derive(Debug, Clone)]
pub struct ExecutorTaskRunner {
    inbox: ExecutorAssignmentInbox,
}

impl ExecutorTaskRunner {
    /// Create a runner over an executor assignment inbox.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self { inbox }
    }

    /// Assignment inbox consumed by this runner.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }

    /// Consume and run one queued assignment, if present.
    pub async fn run_next_with<S>(
        &self,
        coordinator: &S,
    ) -> Result<Option<ExecutorTaskRunReport>, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let Some(assignment) = self
            .inbox
            .pop_next()
            .map_err(|error| tonic::Status::internal(error.to_string()))?
        else {
            return Ok(None);
        };

        self.run_assignment_with(assignment, coordinator)
            .await
            .map(Some)
    }

    /// Run a specific assignment through the skeleton lifecycle.
    pub async fn run_assignment_with<S>(
        &self,
        assignment: ExecutorTaskAssignment,
        coordinator: &S,
    ) -> Result<ExecutorTaskRunReport, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let running = self
            .send_task_status(
                &assignment,
                TaskState::Running,
                "executor accepted assignment",
                coordinator,
            )
            .await?;
        ensure_status_accepted_or_duplicate(running.disposition(), TaskState::Running)?;

        let output = match self.execute_stage_fragment(&assignment).await {
            Ok(output) => output,
            Err(error) => {
            let failed = self
                .send_task_status(
                    &assignment,
                    TaskState::Failed,
                    "executor failed assignment before DataFusion execution",
                    coordinator,
                )
                .await?;
            ensure_status_accepted_or_duplicate(failed.disposition(), TaskState::Failed)?;
            return Err(tonic::Status::internal(error.to_string()));
            }
        };

        let terminal = self
            .send_task_status(
                &assignment,
                TaskState::Succeeded,
                "executor completed placeholder stage-local fragment",
                coordinator,
            )
            .await?;
        ensure_status_accepted_or_duplicate(terminal.disposition(), TaskState::Succeeded)?;

        Ok(ExecutorTaskRunReport::new(
            assignment,
            output,
            running.disposition(),
            terminal.disposition(),
        ))
    }

    async fn execute_stage_fragment(
        &self,
        assignment: &ExecutorTaskAssignment,
    ) -> ExecutorResult<ExecutorTaskOutput> {
        let fragment = assignment.plan_fragment().description().trim();
        if fragment.is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("plan fragment description cannot be empty"),
            });
        }
        if assignment.output_contract().description().trim().is_empty() {
            return Err(ExecutorError::InvalidAssignment {
                message: String::from("output contract description cannot be empty"),
            });
        }

        if let Some(query) = sql_query_from_fragment(fragment) {
            let dataframe = SqlEngine::new()
                .sql(query)
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
            let batches = dataframe
                .collect()
                .await
                .map_err(|error| ExecutorError::LocalExecution {
                    message: error.to_string(),
                })?;
            let row_count = batches.iter().map(|batch| batch.num_rows()).sum();
            let column_count = batches
                .first()
                .map_or(0, arrow_record_batch_column_count);
            return Ok(ExecutorTaskOutput::sql(
                row_count,
                batches.len(),
                column_count,
            ));
        }

        Ok(ExecutorTaskOutput::placeholder())
    }

    async fn send_task_status<S>(
        &self,
        assignment: &ExecutorTaskAssignment,
        state: TaskState,
        message: &'static str,
        coordinator: &S,
    ) -> Result<TaskStatusResponse, tonic::Status>
    where
        S: CoordinatorExecutorService,
    {
        let ids = TaskAttemptRef::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        );
        let request = TaskStatusRequest::new(
            ids,
            assignment.executor_id().clone(),
            assignment.lease_generation(),
            state,
        )
        .with_message(message);

        coordinator
            .task_status(tonic::Request::new(request))
            .await
            .map(tonic::Response::into_inner)
    }
}

fn sql_query_from_fragment(fragment: &str) -> Option<&str> {
    let (_, query) = fragment.split_once("sql:")?;
    let query = query.trim();
    (!query.is_empty()).then_some(query)
}

fn arrow_record_batch_column_count(batch: &arrow::record_batch::RecordBatch) -> usize {
    batch.num_columns()
}

fn ensure_status_accepted_or_duplicate(
    disposition: TransportDisposition,
    state: TaskState,
) -> Result<(), tonic::Status> {
    match disposition {
        TransportDisposition::Accepted | TransportDisposition::Duplicate => Ok(()),
        _ => Err(tonic::Status::failed_precondition(format!(
            "coordinator returned {disposition} for {state} status"
        ))),
    }
}

/// Executor-side task assignment service backed by an in-memory inbox.
#[derive(Debug, Clone)]
pub struct ExecutorTaskInboxService {
    inbox: ExecutorAssignmentInbox,
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self { inbox }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }
}

#[tonic::async_trait]
impl ExecutorTaskService for ExecutorTaskInboxService {
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let assignment = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(assignment.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                assignment.version(),
                TransportVersion::CURRENT
            )));
        }

        self.inbox
            .push(assignment)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(TaskStatusResponse::new(
            TransportDisposition::Accepted,
        )))
    }
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
        }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::executor_task_assignment_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .assign_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
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
        AttemptId, CoordinatorExecutorService, CoordinatorId, ExecutorHeartbeatRequest,
        ExecutorHeartbeatResponse, ExecutorId, ExecutorState, ExecutorTaskAssignment,
        ExecutorTaskService, InputPartition, JobId, JobKind, JobSpec, JobState, LeaseGeneration,
        OutputContract, OutputContractKind, PlanFragment, RegisterExecutorRequest,
        RegisterExecutorResponse, StageId, StageSpec, TaskAttemptRef, TaskId, TaskSpec,
        TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion, wire,
    };
    use krishiv_scheduler::{Coordinator, CoordinatorExecutorTonicService, SharedCoordinator};

    use super::{
        ExecutorAssignmentInbox, ExecutorConfig, ExecutorError, ExecutorRuntime,
        ExecutorTaskInboxService, ExecutorTaskRunner, serve_executor_task_grpc_with_listener,
    };

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

    #[tokio::test]
    async fn task_inbox_service_accepts_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox.clone());

        let response = service
            .assign_task(tonic::Request::new(demo_assignment("task-1")))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        let assignments = inbox.assignments().unwrap();
        assert_eq!(assignments[0].task_id().as_str(), "task-1");
        assert_eq!(
            assignments[0].lease_generation(),
            LeaseGeneration::initial()
        );
    }

    #[tokio::test]
    async fn task_assignment_flows_over_network_to_executor_inbox() {
        let inbox = ExecutorAssignmentInbox::new();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_inbox = inbox.clone();
        let server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(listener, server_inbox)
                .await
                .unwrap();
        });

        let mut client =
            wire::v1::executor_task_client::ExecutorTaskClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let response = client
            .assign_task(wire::executor_task_assignment_to_wire(demo_assignment(
                "task-network-1",
            )))
            .await
            .unwrap()
            .into_inner();
        let response = wire::task_status_response_from_wire(response).unwrap();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        assert_eq!(
            inbox.assignments().unwrap()[0].task_id().as_str(),
            "task-network-1"
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_runner_reports_running_and_success_to_scheduler() {
        let executor_id = ExecutorId::try_new("exec-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-runner-1").unwrap();

        {
            let mut coordinator = shared.write().unwrap();
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-runner",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            let mut assignments = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
            inbox.push(assignments.remove(0)).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert!(matches!(
            report.running_disposition(),
            TransportDisposition::Accepted | TransportDisposition::Duplicate
        ));
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().unwrap();
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    fn demo_assignment(task_id: &str) -> ExecutorTaskAssignment {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new(task_id).unwrap(),
            AttemptId::initial(),
        );

        ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet partition"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first split")])
    }

    fn single_task_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "runner smoke", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "single stage").with_task(
                TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "placeholder select 1 fragment",
                ),
            ),
        )
    }
}
