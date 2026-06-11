//! gRPC service types for the executor task assignment protocol.

pub const EXECUTOR_TASK_BEARER_TOKEN_ENV: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";
pub const REQUIRE_EXECUTOR_TASK_AUTH_ENV: &str = "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH";

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_dataflow::ContinuousWindowExecutor;
use krishiv_proto::{
    ExecutorTaskAssignment, ExecutorTaskService, TaskStatusResponse, TransportDisposition,
    TransportVersion, wire,
};

use crate::{AssignmentPushOutcome, ExecutorAssignmentInbox, ExecutorError};

/// Shared map of per-job stateful window executors for continuous streaming.
///
/// Keyed by job-id. Shared between `ExecutorTaskInboxService` (for
/// `push_continuous_input` / `drain_continuous_output`) and `ExecutorTaskRunner`
/// (for `stream:loop:` fragment execution).
pub type SharedLoopExecutors =
    Arc<DashMap<String, Arc<Mutex<ContinuousWindowExecutor>>>>;

/// Shared per-job input buffer for continuous streaming tasks.
///
/// `push_continuous_input` appends decoded batches here; `drain_continuous_output`
/// drains and processes them through the matching loop executor.
pub type SharedContinuousInputs = Arc<DashMap<String, Vec<RecordBatch>>>;

/// Executor-side task assignment service backed by an in-memory inbox.
#[derive(Debug, Clone)]
pub struct ExecutorTaskInboxService {
    inbox: ExecutorAssignmentInbox,
    /// Per-job stateful window executors — shared with the task runner.
    pub(crate) loop_executors: SharedLoopExecutors,
    /// Per-job pending input batches for distributed continuous push.
    pub(crate) continuous_inputs: SharedContinuousInputs,
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inbox,
            loop_executors: Arc::new(DashMap::new()),
            continuous_inputs: Arc::new(DashMap::new()),
        }
    }

    /// Create a task assignment service that shares state with an existing runner.
    pub fn new_with_continuous(
        inbox: ExecutorAssignmentInbox,
        loop_executors: SharedLoopExecutors,
        continuous_inputs: SharedContinuousInputs,
    ) -> Self {
        Self {
            inbox,
            loop_executors,
            continuous_inputs,
        }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }
}

/// Authentication settings for the executor task-control gRPC API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskAuthConfig {
    require_auth: bool,
    bearer_token: Option<String>,
}

impl ExecutorTaskAuthConfig {
    /// Build auth config from process environment.
    pub fn from_env() -> Self {
        Self {
            require_auth: parse_bool_env(REQUIRE_EXECUTOR_TASK_AUTH_ENV),
            bearer_token: configured_executor_task_bearer_token(),
        }
    }

    /// Build auth config directly for tests and embedders.
    pub fn new(require_auth: bool, bearer_token: Option<String>) -> Self {
        Self {
            require_auth,
            bearer_token: bearer_token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty()),
        }
    }

    /// Whether the process must fail closed if no bearer token is configured.
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    /// Whether a non-empty bearer token is configured.
    pub fn has_bearer_token(&self) -> bool {
        self.bearer_token.is_some()
    }

    /// The configured bearer token, if any.
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Validate the required-auth startup contract.
    pub fn validate_required(&self) -> crate::ExecutorResult<()> {
        if self.require_auth && self.bearer_token.is_none() {
            return Err(crate::ExecutorError::LocalExecution {
                message: format!(
                    "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
                ),
            });
        }
        Ok(())
    }
}

fn parse_bool_env(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

fn configured_executor_task_bearer_token() -> Option<String> {
    std::env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

pub fn bearer_token_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
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

        match self.inbox.push_with_outcome(assignment) {
            Ok(AssignmentPushOutcome::Enqueued) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )),
            Ok(AssignmentPushOutcome::Duplicate) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Duplicate),
            )),
            Err(ExecutorError::AssignmentQueueFull { current, max }) => {
                // Proper backpressure signal to the coordinator.
                Err(tonic::Status::resource_exhausted(format!(
                    "executor assignment queue full (current={current}, max={max})"
                )))
            }
            Err(other) => Err(tonic::Status::internal(other.to_string())),
        }
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<krishiv_proto::task::TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(request.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                request.version(),
                TransportVersion::CURRENT
            )));
        }
        let removed = self
            .inbox
            .cancel_task(request.task_id())
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let response = if removed {
            TaskStatusResponse::new(TransportDisposition::Accepted)
        } else {
            TaskStatusResponse::new(TransportDisposition::UnknownTask)
                .with_message("task is not queued on this executor")
        };
        Ok(tonic::Response::new(response))
    }

    async fn push_continuous_input(
        &self,
        request: tonic::Request<krishiv_proto::task::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let req = request.into_inner();
        let job_id = req.job_id.as_str().to_owned();

        // Decode Arrow IPC bytes into RecordBatches.
        let batches = decode_ipc_batches(&req.ipc_bytes)?;

        // Append to the per-job input buffer; drain_continuous_output will
        // process them through the window executor.
        self.continuous_inputs
            .entry(job_id)
            .or_insert_with(Vec::new)
            .extend(batches);

        Ok(tonic::Response::new(TaskStatusResponse::new(
            TransportDisposition::Accepted,
        )))
    }

    async fn drain_continuous_output(
        &self,
        request: tonic::Request<krishiv_proto::task::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<krishiv_proto::task::DrainContinuousOutputResponse>, tonic::Status>
    {
        use krishiv_proto::TransportDisposition;

        let req = request.into_inner();
        let job_id = req.job_id.as_str();

        // Take pending input batches for this job.
        let input_batches = self
            .continuous_inputs
            .remove(job_id)
            .map(|(_, v)| v)
            .unwrap_or_default();

        // If no loop executor exists yet, there's nothing to drain.
        let executor_entry = match self.loop_executors.get(job_id) {
            Some(e) => e,
            None => {
                return Ok(tonic::Response::new(
                    krishiv_proto::task::DrainContinuousOutputResponse {
                        version: krishiv_proto::TransportVersion::CURRENT,
                        disposition: TransportDisposition::UnknownTask,
                        ipc_bytes: vec![],
                    },
                ));
            }
        };
        let executor_arc = executor_entry.value().clone();
        drop(executor_entry);

        let output_batches = {
            let mut exec = executor_arc
                .lock()
                .map_err(|_| tonic::Status::internal("loop executor lock poisoned"))?;
            exec.drain(input_batches)
                .map_err(|e| tonic::Status::internal(e.to_string()))?
        };

        let ipc_bytes = encode_ipc_batches(&output_batches)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(
            krishiv_proto::task::DrainContinuousOutputResponse {
                version: krishiv_proto::TransportVersion::CURRENT,
                disposition: TransportDisposition::Accepted,
                ipc_bytes,
            },
        ))
    }
}

fn decode_ipc_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>, tonic::Status> {
    if ipc_bytes.is_empty() {
        return Ok(vec![]);
    }
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)
        .map_err(|e| tonic::Status::invalid_argument(format!("IPC decode: {e}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(
            batch.map_err(|e| tonic::Status::invalid_argument(format!("IPC batch: {e}")))?,
        );
    }
    Ok(batches)
}

fn encode_ipc_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, arrow::error::ArrowError> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    use arrow::ipc::writer::StreamWriter;
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
    required_bearer_token: Option<String>,
    auth_misconfiguration: Option<String>,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self::with_auth_config(inbox, ExecutorTaskAuthConfig::from_env())
    }

    /// Create a networked executor task service with explicit auth config.
    pub fn with_auth_config(inbox: ExecutorAssignmentInbox, auth: ExecutorTaskAuthConfig) -> Self {
        let auth_misconfiguration = (auth.require_auth() && !auth.has_bearer_token()).then(|| {
            format!(
                "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty \
                 {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
            )
        });
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
            required_bearer_token: auth.bearer_token().map(ToOwned::to_owned),
            auth_misconfiguration,
        }
    }

    /// Require a bearer token for network task-control RPCs.
    #[must_use]
    pub fn with_required_bearer_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.required_bearer_token = (!token.trim().is_empty()).then(|| token.trim().to_owned());
        self.auth_misconfiguration = None;
        self
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }

    fn validate_auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> {
        if let Some(message) = &self.auth_misconfiguration {
            return Err(tonic::Status::unauthenticated(message.clone()));
        }
        let Some(expected) = &self.required_bearer_token else {
            return Ok(());
        };
        match bearer_token_from_metadata(metadata) {
            Some(actual)
                if constant_time_eq::constant_time_eq(actual.as_bytes(), expected.as_bytes()) =>
            {
                Ok(())
            }
            Some(_) => Err(tonic::Status::unauthenticated(
                "invalid executor task bearer token",
            )),
            None => Err(tonic::Status::unauthenticated(
                "missing executor task bearer token",
            )),
        }
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
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

    async fn cancel_task(
        &self,
        request: tonic::Request<wire::v1::TaskCancellationRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::task_cancellation_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .cancel_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn push_continuous_input(
        &self,
        request: tonic::Request<wire::v1::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::push_continuous_input_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .push_continuous_input(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn drain_continuous_output(
        &self,
        request: tonic::Request<wire::v1::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<wire::v1::DrainContinuousOutputResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::drain_continuous_output_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .drain_continuous_output(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::drain_continuous_output_response_to_wire(response),
        ))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
}

/// Build the generated tonic server sharing continuous-streaming state with a runner.
///
/// The `loop_executors` and `continuous_inputs` maps from
/// `ExecutorTaskRunner::shared_loop_executors()` / `shared_continuous_inputs()`
/// are shared here so that distributed `push_continuous_input` / `drain_continuous_output`
/// RPCs operate on the same state as `execute_loop_fragment`.
pub fn executor_task_grpc_server_with_continuous(
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    let inner = ExecutorTaskInboxService::new_with_continuous(inbox, loop_executors, continuous_inputs);
    let auth = ExecutorTaskAuthConfig::from_env();
    let auth_misconfiguration = (auth.require_auth() && !auth.has_bearer_token()).then(|| {
        format!(
            "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty \
             {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
        )
    });
    let service = ExecutorTaskGrpcService {
        inner,
        required_bearer_token: auth.bearer_token().map(ToOwned::to_owned),
        auth_misconfiguration,
    };
    wire::v1::executor_task_server::ExecutorTaskServer::new(service)
}
