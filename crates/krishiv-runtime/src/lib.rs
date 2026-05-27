#![forbid(unsafe_code)]

//! Runtime traits and local backends for Krishiv.
//!
//! Embedded mode runs batch SQL through the session `SqlEngine` (in `krishiv-api`).
//! Streaming plans are accepted here and executed via [`local_streaming`] on
//! single-node backends (ADR-12.5).

use std::error::Error;
use std::fmt;

use krishiv_plan::{ExecutionKind, PhysicalPlan};

pub mod continuous_stream;
pub mod execution_runtime;
pub mod flight_action;
mod flight_client;
pub mod flight_protocol;
pub mod in_process;
pub mod in_process_cluster;
pub mod local_streaming;
mod plan;
pub mod stream_kafka;

pub use continuous_stream::ContinuousStreamRegistry;
pub use execution_runtime::{
    BatchTableRegistration, ClusterEndpoints, ExecutionRuntime, InProcessExecutionRuntime,
    RemoteExecutionRuntime, RuntimeMode, build_execution_runtime,
};
pub use flight_action::{
    BoundedWindowBody, ContinuousDrainBody, ContinuousPushBody, ContinuousRegisterBody,
    ExplainBody, KrishivFlightAction, RegisterParquetBody, action_type, decode_batches,
    encode_batches, strip_action_type, tags as action_tags,
};
pub use in_process::{BatchSqlTable, InProcessStreamingRuntime, execute_windowed_in_process};
pub use in_process_cluster::{InProcessCluster, fragment_from_local_spec, plan_spec_to_local};
pub use local_streaming::{LocalWindowExecutionSpec, LocalWindowKind, execute_windowed_stream};
pub use plan::is_streaming_plan;

// tracing is used for debug-level plan delegation logging.
use tracing::debug;

/// Runtime result alias.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Runtime errors shared by bootstrap backends and traits.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// A requested capability is not available in the current release slice.
    Unsupported { feature: String },
    /// Runtime state was invalid for the requested operation.
    InvalidState { message: String },
    /// A transport-level failure (connection refused, timeout, etc.).
    Transport { message: String },
    /// The submitted plan was rejected by the coordinator.
    PlanRejected { reason: String },
    /// The operation succeeded for some partitions but not all.
    PartialResult { succeeded: usize, failed: usize },
}

impl RuntimeError {
    /// Create an unsupported-feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
        }
    }

    /// Create a transport-level error.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
        }
    }

    /// Create a plan-rejected error.
    pub fn plan_rejected(reason: impl Into<String>) -> Self {
        Self::PlanRejected {
            reason: reason.into(),
        }
    }

    /// Create a partial-result error.
    pub fn partial_result(succeeded: usize, failed: usize) -> Self {
        Self::PartialResult { succeeded, failed }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { feature } => {
                write!(f, "unsupported runtime feature: {feature}")
            }
            Self::InvalidState { message } => write!(f, "invalid runtime state: {message}"),
            Self::Transport { message } => write!(f, "transport error: {message}"),
            Self::PlanRejected { reason } => write!(f, "plan rejected: {reason}"),
            Self::PartialResult { succeeded, failed } => write!(
                f,
                "partial result: {succeeded} partitions succeeded, {failed} failed"
            ),
        }
    }
}

impl Error for RuntimeError {}

/// Stable job identifier used by local and future distributed runtimes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(String);

impl JobId {
    /// Create a job id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Minimal job state for R1 local job listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Job is accepted but not running yet.
    Pending,
    /// Job is running.
    Running,
    /// Job completed successfully.
    Succeeded,
    /// Job failed.
    Failed,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

/// Minimal job status surfaced by `krishiv jobs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatus {
    id: JobId,
    name: String,
    state: JobState,
}

impl JobStatus {
    /// Create a job status.
    pub fn new(id: JobId, name: impl Into<String>, state: JobState) -> Self {
        Self {
            id,
            name: name.into(),
            state,
        }
    }

    /// Job id.
    pub fn id(&self) -> &JobId {
        &self.id
    }

    /// Job name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }
}

/// Local in-memory job registry for bootstrap CLI/status behavior.
#[derive(Debug, Default, Clone)]
pub struct LocalJobRegistry {
    jobs: Vec<JobStatus>,
}

impl LocalJobRegistry {
    /// Add or replace a job status.
    pub fn upsert(&mut self, status: JobStatus) {
        if let Some(existing) = self.jobs.iter_mut().find(|job| job.id == status.id) {
            *existing = status;
        } else {
            self.jobs.push(status);
        }
    }

    /// List known jobs.
    pub fn list(&self) -> &[JobStatus] {
        &self.jobs
    }

    /// Snapshot known jobs.
    pub fn snapshot(&self) -> Vec<JobStatus> {
        self.jobs.clone()
    }
}

/// A tiny task spec used by executor stubs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    id: String,
    description: String,
}

impl TaskSpec {
    /// Create a task spec.
    pub fn new(id: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            description: description.into(),
        }
    }

    /// Task id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Human-readable task description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// A tiny task report used by executor stubs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    task_id: String,
    state: JobState,
}

impl TaskReport {
    /// Create a task report.
    pub fn new(task_id: impl Into<String>, state: JobState) -> Self {
        Self {
            task_id: task_id.into(),
            state,
        }
    }

    /// Task id.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Task state.
    pub fn state(&self) -> JobState {
        self.state
    }
}

/// Minimal execution report for bootstrap backends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReport {
    backend: String,
    plan_name: String,
    kind: ExecutionKind,
    accepted: bool,
}

impl ExecutionReport {
    /// Create an execution report.
    pub fn new(
        backend: impl Into<String>,
        plan_name: impl Into<String>,
        kind: ExecutionKind,
        accepted: bool,
    ) -> Self {
        Self {
            backend: backend.into(),
            plan_name: plan_name.into(),
            kind,
            accepted,
        }
    }

    /// Backend name.
    pub fn backend(&self) -> &str {
        &self.backend
    }

    /// Plan name.
    pub fn plan_name(&self) -> &str {
        &self.plan_name
    }

    /// Execution kind.
    pub fn kind(&self) -> ExecutionKind {
        self.kind
    }

    /// Whether the backend accepted the plan.
    pub fn accepted(&self) -> bool {
        self.accepted
    }
}

/// Runtime backend contract shared by embedded, single-node, and distributed modes.
pub trait ExecutionBackend {
    /// Backend name.
    fn backend_name(&self) -> &str;

    /// Accept or execute a physical plan. Batch plans are accepted without
    /// re-running SQL; execution happens in the session `SqlEngine`.
    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport>;
}

/// Minimal task executor contract for future scheduler integration.
pub trait TaskExecutor {
    /// Execute one task.
    fn execute_task(&mut self, task: TaskSpec) -> RuntimeResult<TaskReport>;
}

fn accept_local_plan(backend: &str, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
    if plan.name().trim().is_empty() {
        return Err(RuntimeError::plan_rejected("plan name must not be empty"));
    }
    Ok(ExecutionReport::new(
        backend,
        plan.name(),
        plan.kind(),
        true,
    ))
}

/// Single-node in-process backend: accepts batch and streaming plans for local execution.
#[derive(Debug, Default)]
pub struct SingleNodeBackend;

impl ExecutionBackend for SingleNodeBackend {
    fn backend_name(&self) -> &str {
        "single-node"
    }

    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        debug!(
            backend = "single-node",
            plan = %plan.name(),
            kind = %plan.kind(),
            streaming = is_streaming_plan(plan),
            "SingleNodeBackend: accepted plan for in-process execution"
        );
        accept_local_plan(self.backend_name(), plan)
    }
}

/// Embedded in-process backend: batch via session `SqlEngine`, streaming delegated to
/// [`SingleNodeBackend`] (ADR-12.5).
#[derive(Debug, Default)]
pub struct EmbeddedBackend {
    single_node: SingleNodeBackend,
}

impl ExecutionBackend for EmbeddedBackend {
    fn backend_name(&self) -> &str {
        "embedded"
    }

    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        if is_streaming_plan(plan) {
            debug!(
                backend = "embedded",
                plan = %plan.name(),
                "EmbeddedBackend: redirecting streaming plan to SingleNodeBackend"
            );
            return self.single_node.execute(plan);
        }
        debug!(
            backend = "embedded",
            plan = %plan.name(),
            kind = %plan.kind(),
            "EmbeddedBackend: accepted batch plan (execution via session SqlEngine)"
        );
        accept_local_plan(self.backend_name(), plan)
    }
}

/// Distributed backend that routes plan execution to a remote coordinator
/// via Arrow Flight SQL (GAP-RT-01 / ADR-12.3).
#[derive(Debug, Clone)]
pub struct DistributedBackend {
    flight_url: String,
}

impl DistributedBackend {
    pub fn new(flight_url: impl Into<String>) -> Self {
        Self {
            flight_url: flight_url.into(),
        }
    }

    pub fn flight_url(&self) -> &str {
        &self.flight_url
    }
}

impl ExecutionBackend for DistributedBackend {
    fn backend_name(&self) -> &str {
        "distributed"
    }

    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        use krishiv_async_util::block_on;

        debug!(
            backend = "distributed",
            coordinator = %self.flight_url,
            plan = %plan.name(),
            kind = %plan.kind(),
            sql = %flight_client::plan_to_sql(plan),
            "DistributedBackend: submitting plan via Flight SQL"
        );
        block_on(flight_client::execute_remote_plan(&self.flight_url, plan))?;
        Ok(ExecutionReport::new(
            self.backend_name(),
            plan.name(),
            plan.kind(),
            true,
        ))
    }
}

#[cfg(test)]
mod distributed_flight_tests {
    use std::net::SocketAddr;

    use krishiv_flight_sql::make_flight_sql_server;
    use krishiv_plan::{ExecutionKind, PhysicalPlan};
    use tonic::transport::Server;

    use super::{DistributedBackend, ExecutionBackend};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distributed_backend_submits_plan_over_flight_sql() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);

        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_flight_sql_server())
                .serve_with_incoming(incoming)
                .await
                .expect("serve");
        });

        let url = format!("http://{addr}");
        let mut backend = DistributedBackend::new(url);
        let plan = PhysicalPlan::new("SELECT 1 AS n", ExecutionKind::Batch);
        let report = backend.execute(&plan).expect("execute");
        assert!(report.accepted());
        server.abort();
    }
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    use super::{EmbeddedBackend, ExecutionBackend, SingleNodeBackend, is_streaming_plan};

    #[test]
    fn embedded_backend_accepts_bootstrap_plan() {
        let plan = PhysicalPlan::new("bootstrap", ExecutionKind::Batch);
        let mut backend = EmbeddedBackend::default();

        let report = backend.execute(&plan).expect("execute");

        assert_eq!(report.backend(), "embedded");
        assert_eq!(report.plan_name(), "bootstrap");
        assert!(report.accepted());
    }

    #[test]
    fn embedded_redirects_streaming_kind_to_single_node() {
        let plan = PhysicalPlan::new("events", ExecutionKind::Streaming);
        assert!(is_streaming_plan(&plan));
        let mut backend = EmbeddedBackend::default();
        let report = backend.execute(&plan).expect("execute");
        assert_eq!(report.backend(), "single-node");
    }

    #[test]
    fn single_node_accepts_streaming_plan() {
        let plan = PhysicalPlan::new("stream:tw:key=u", ExecutionKind::Batch);
        let mut backend = SingleNodeBackend;
        let report = backend.execute(&plan).expect("execute");
        assert_eq!(report.backend(), "single-node");
        assert!(report.accepted());
    }
}
