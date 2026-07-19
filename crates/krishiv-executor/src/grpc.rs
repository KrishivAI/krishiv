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
pub type SharedLoopExecutors = Arc<DashMap<String, Arc<Mutex<ContinuousWindowExecutor>>>>;

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
    /// Phase 55: per-job run-loop egress buffers — shared with the task runner.
    pub(crate) continuous_outputs: crate::runner::SharedContinuousOutputs,
    /// Phase 55: per-buffer-key input notifies — shared with the task runner
    /// so a push wakes a blocked run-loop within microseconds.
    pub(crate) input_notify: crate::runner::SharedContinuousNotify,
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inbox,
            loop_executors: Arc::new(DashMap::new()),
            continuous_inputs: Arc::new(DashMap::new()),
            continuous_outputs: Arc::new(DashMap::new()),
            input_notify: Arc::new(DashMap::new()),
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
            continuous_outputs: Arc::new(DashMap::new()),
            input_notify: Arc::new(DashMap::new()),
        }
    }

    /// Share the run-loop egress buffers and input notifies with the runner
    /// (Phase 55: push wakes the run-loop; drain serves its egress buffer).
    #[must_use]
    pub fn with_run_loop_state(
        mut self,
        continuous_outputs: crate::runner::SharedContinuousOutputs,
        input_notify: crate::runner::SharedContinuousNotify,
    ) -> Self {
        self.continuous_outputs = continuous_outputs;
        self.input_notify = input_notify;
        self
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
    krishiv_common::truthy_env(name)
}

fn configured_executor_task_bearer_token() -> Option<String> {
    std::env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

pub fn bearer_token_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    krishiv_common::bearer_token(metadata.get("authorization").and_then(|v| v.to_str().ok()))
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
            Err(other) => Err(krishiv_metrics::grpc::internal_status(
                "handle task assignment",
                &other,
            )),
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
            .map_err(|error| krishiv_metrics::grpc::internal_status("cancel task", &error))?;
        // A cancel for a job with a registered `stream:loop` executor is a
        // continuous-job teardown (the only producer of continuous cancels is
        // job-level deregister/cancel). Retire the whole job identity on this
        // process: drop the stateful window executor and buffered inputs,
        // purge the inbox's dedupe entries, and clear the task tombstone — a
        // later *recreated* job legitimately reuses the same deterministic
        // ids (`task-streaming`, attempts from 1) and must be treated as a
        // fresh incarnation, not swallowed as an at-least-once duplicate or
        // insta-cancelled by the stale tombstone.
        let job_id = request.job_id();
        // Run-loop subtasks key their state by `{job}#…`; a cancel for the
        // job (or any of its subtasks) retires the whole composite family.
        let rloop_prefix = format!("{}#", job_id.as_str());
        let composite_keys: Vec<String> = self
            .loop_executors
            .iter()
            .filter(|e| e.key().starts_with(&rloop_prefix))
            .map(|e| e.key().clone())
            .collect();
        let had_cycle_executor = self.loop_executors.remove(job_id.as_str()).is_some();
        let had_rloop = !composite_keys.is_empty();
        for key in &composite_keys {
            self.loop_executors.remove(key);
        }
        if had_cycle_executor || had_rloop {
            self.continuous_inputs.remove(job_id.as_str());
            self.continuous_inputs
                .retain(|k, _| !k.starts_with(&rloop_prefix));
            self.continuous_outputs.remove(job_id.as_str());
            // Wake any run-loop blocked in its idle wait so it observes the
            // cancellation immediately instead of on the fallback tick, then
            // drop the notify entries.
            for entry in self.input_notify.iter() {
                if entry.key() == job_id.as_str() || entry.key().starts_with(&rloop_prefix) {
                    entry.value().notify_waiters();
                }
            }
            self.input_notify
                .retain(|k, _| k != job_id.as_str() && !k.starts_with(&rloop_prefix));
            let purged = self.inbox.forget_job(job_id).map_err(|error| {
                krishiv_metrics::grpc::internal_status("forget cancelled job", &error)
            })?;
            // Run-loop tasks poll `is_task_cancelled` to exit — their
            // tombstone is cleared by the loop itself after it stops, so only
            // cycle-model tombstones are cleared eagerly here.
            if had_cycle_executor && !had_rloop {
                self.inbox
                    .clear_cancelled_task(request.task_id())
                    .map_err(|error| {
                        krishiv_metrics::grpc::internal_status(
                            "clear cancelled task tombstone",
                            &error,
                        )
                    })?;
            }
            tracing::debug!(
                job_id = %job_id,
                purged_dedupe_entries = purged,
                run_loop_subtasks = composite_keys.len(),
                "continuous job cancelled — stateful executors dropped and inbox identity retired"
            );
        }
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

        // Phase 55: a push addressed at a registered run-loop subtask buffer
        // (`{job}#{task}` — the keyed-exchange path) lands task-scoped;
        // everything else keeps the per-job buffer (cycle model + external
        // ingest, which any subtask may claim and re-route by key group).
        let task_key = format!("{job_id}#{}", req.task_id.as_str());
        let buffer_key = if self.input_notify.contains_key(&task_key) {
            task_key
        } else {
            job_id.clone()
        };

        // Enforce per-buffer capacity to prevent unbounded memory growth (M1).
        const MAX_PENDING_BATCHES: usize = 64;
        {
            let mut entry = self
                .continuous_inputs
                .entry(buffer_key.clone())
                .or_default();
            if entry.len() + batches.len() > MAX_PENDING_BATCHES {
                return Err(tonic::Status::resource_exhausted(format!(
                    "continuous input buffer for job {} exceeded capacity ({MAX_PENDING_BATCHES}); \
                     slow down the producer or increase the drain rate",
                    entry.len() + batches.len(),
                )));
            }
            entry.extend(batches);
        }
        // Wake a blocked run-loop within microseconds of arrival.
        if let Some(notify) = self.input_notify.get(&buffer_key) {
            notify.notify_waiters();
        }
        if buffer_key != job_id
            && let Some(notify) = self.input_notify.get(&job_id)
        {
            notify.notify_waiters();
        }

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

        // Phase 55: run-loop jobs emit into a per-job egress buffer as they
        // run — drain serves (and clears) it without driving any execution.
        if let Some(mut egress) = self.continuous_outputs.get_mut(job_id) {
            let batches: Vec<RecordBatch> = egress.drain(..).collect();
            drop(egress);
            let ipc_bytes = encode_ipc_batches(&batches).map_err(|e| {
                krishiv_metrics::grpc::internal_status("encode continuous output", &e)
            })?;
            return Ok(tonic::Response::new(
                krishiv_proto::task::DrainContinuousOutputResponse {
                    version: krishiv_proto::TransportVersion::CURRENT,
                    disposition: TransportDisposition::Accepted,
                    ipc_bytes,
                },
            ));
        }

        // Check executor FIRST to avoid losing input batches on early return.
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

        // Now safe to consume pending input batches.
        let input_batches = self
            .continuous_inputs
            .remove(job_id)
            .map(|(_, v)| v)
            .unwrap_or_default();

        let output_batches = {
            let mut exec = executor_arc
                .lock()
                .map_err(|_| tonic::Status::internal("loop executor lock poisoned"))?;
            exec.drain(input_batches).map_err(|e| {
                krishiv_metrics::grpc::internal_status("drain continuous executor", &e)
            })?
        };

        let ipc_bytes = encode_ipc_batches(&output_batches)
            .map_err(|e| krishiv_metrics::grpc::internal_status("encode continuous output", &e))?;

        Ok(tonic::Response::new(
            krishiv_proto::task::DrainContinuousOutputResponse {
                version: krishiv_proto::TransportVersion::CURRENT,
                disposition: TransportDisposition::Accepted,
                ipc_bytes,
            },
        ))
    }
}

/// Maximum IPC payload size accepted from the wire (256 MiB).
const MAX_IPC_BYTES: usize = 256 * 1024 * 1024;

fn decode_ipc_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>, tonic::Status> {
    if ipc_bytes.is_empty() {
        return Ok(vec![]);
    }
    if ipc_bytes.len() > MAX_IPC_BYTES {
        return Err(tonic::Status::resource_exhausted(format!(
            "IPC payload {} bytes exceeds max {} bytes",
            ipc_bytes.len(),
            MAX_IPC_BYTES
        )));
    }
    use arrow::ipc::reader::StreamReader;
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)
        .map_err(|e| tonic::Status::invalid_argument(format!("IPC decode: {e}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches
            .push(batch.map_err(|e| tonic::Status::invalid_argument(format!("IPC batch: {e}")))?);
    }
    Ok(batches)
}

fn encode_ipc_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, arrow::error::ArrowError> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    use arrow::ipc::writer::StreamWriter;
    let schema = batches
        .first()
        .ok_or_else(|| arrow::error::ArrowError::InvalidArgumentError("empty batches".to_string()))?
        .schema();
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
    let max = krishiv_proto::max_grpc_message_bytes();
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
        .max_decoding_message_size(max)
        .max_encoding_message_size(max)
}

/// Build the generated tonic server sharing continuous-streaming state with a runner.
///
/// The `loop_executors` and `continuous_inputs` maps from
/// `ExecutorTaskRunner::shared_loop_executors()` / `shared_continuous_inputs()`
/// are shared here so that distributed `push_continuous_input` / `drain_continuous_output`
/// RPCs operate on the same state as `execute_loop_fragment`.
///
/// H-19 (audit): callers that wired auth via the builder API had their
/// explicit token silently dropped because this constructor always
/// rebuilt auth from the process environment. The new `auth` parameter
/// takes precedence; pass `None` to keep the env-based default.
pub fn executor_task_grpc_server_with_continuous(
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
    auth: Option<ExecutorTaskAuthConfig>,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    executor_task_grpc_server_with_run_loop(
        inbox,
        loop_executors,
        continuous_inputs,
        Arc::new(DashMap::new()),
        Arc::new(DashMap::new()),
        auth,
    )
}

/// Build the generated tonic server sharing the FULL continuous-streaming
/// state with a runner, including the Phase 55 run-loop egress buffers and
/// input notifies (so pushes wake run-loops and drains serve their egress).
pub fn executor_task_grpc_server_with_run_loop(
    inbox: ExecutorAssignmentInbox,
    loop_executors: SharedLoopExecutors,
    continuous_inputs: SharedContinuousInputs,
    continuous_outputs: crate::runner::SharedContinuousOutputs,
    input_notify: crate::runner::SharedContinuousNotify,
    auth: Option<ExecutorTaskAuthConfig>,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    let inner =
        ExecutorTaskInboxService::new_with_continuous(inbox, loop_executors, continuous_inputs)
            .with_run_loop_state(continuous_outputs, input_notify);
    let auth = auth.unwrap_or_else(ExecutorTaskAuthConfig::from_env);
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
    let max = krishiv_proto::max_grpc_message_bytes();
    wire::v1::executor_task_server::ExecutorTaskServer::new(service)
        .max_decoding_message_size(max)
        .max_encoding_message_size(max)
}
