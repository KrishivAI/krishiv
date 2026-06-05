#![forbid(unsafe_code)]

//! Runtime traits and local backends for Krishiv.
//!
//! Embedded mode runs batch SQL through the session `SqlEngine` (in `krishiv-api`).
//! Streaming plans are accepted here and executed via [`local_streaming`] on
//! single-node backends (ADR-12.5).

use std::fmt;

use krishiv_plan::{ExecutionKind, PhysicalPlan};

pub mod continuous_stream;
mod coordinator_http_client;
pub mod execution_runtime;
pub mod flight_action;
pub mod flight_client;
pub mod flight_protocol;
pub mod in_process;
pub mod in_process_cluster;
pub mod local_streaming;
mod plan;
pub mod stream_kafka;

pub use continuous_stream::ContinuousStreamRegistry;
pub use coordinator_http_client::{
    execute_coordinator_batch_sql, execute_coordinator_batch_sql_inline,
    execute_coordinator_bounded_window, execute_coordinator_continuous_drain,
    execute_coordinator_continuous_push, execute_coordinator_continuous_register,
    execute_coordinator_physical_plan,
};
pub use execution_runtime::{
    BatchTableRegistration, ClusterEndpoints, ExecutionPlacement, ExecutionRuntime,
    InProcessExecutionRuntime, RemoteExecutionRuntime, RuntimeMode, build_execution_runtime,
};
pub use flight_action::{
    BoundedWindowBody, ContinuousDrainBody, ContinuousPushBody, ContinuousRegisterBody,
    ExplainBody, KrishivFlightAction, RegisterParquetBody, action_type, decode_batches,
    encode_batches, strip_action_type, tags as action_tags,
};
pub use in_process::{BatchSqlTable, InProcessStreamingRuntime, execute_windowed_in_process};
pub use in_process_cluster::{InProcessCluster, fragment_from_local_spec, plan_spec_to_local};
pub use local_streaming::{
    LocalWindowExecutionSpec, LocalWindowKind, execute_streaming_window, execute_windowed_stream,
};
pub use plan::{is_streaming_plan, streaming_spec_from_plan};

// tracing is used for debug-level plan delegation logging.
use tracing::debug;

/// Runtime result alias.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Runtime errors shared by bootstrap backends and traits.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeError {
    /// A requested capability is not available in the current release slice.
    #[error("unsupported runtime feature: {feature}")]
    Unsupported { feature: String },
    /// Runtime state was invalid for the requested operation.
    #[error("invalid runtime state: {message}")]
    InvalidState { message: String },
    /// A transport-level failure (connection refused, timeout, etc.).
    #[error("transport error: {message}")]
    Transport { message: String },
    /// The submitted plan was rejected by the coordinator.
    #[error("plan rejected: {reason}")]
    PlanRejected { reason: String },
    /// The operation succeeded for some partitions but not all.
    #[error("partial result: {succeeded} partitions succeeded, {failed} failed")]
    PartialResult { succeeded: usize, failed: usize },
    /// The remote server responded with gRPC `Unimplemented` — the client should
    /// fall back to the legacy SQL-comment protocol.  Kept as a distinct variant
    /// so callers use a proper enum match instead of fragile string comparison.
    #[error("server unimplemented: {message}")]
    ServerUnimplemented { message: String },
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
///
/// NOTE: execute is deliberately *not* async. Embedded and SingleNode backends
/// are trivially synchronous (they only inspect plan metadata). DistributedBackend
/// uses `block_on` internally at its I/O boundary, which is the correct single
/// sync/async seam.  This eliminates nested `block_on` deadlocks (B1).
pub trait ExecutionBackend: Send + Sync {
    /// Backend name.
    fn backend_name(&self) -> &str;

    /// Accept or execute a physical plan. Batch plans are accepted without
    /// re-running SQL; execution happens in the session `SqlEngine`.
    fn execute(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport>;
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

    fn execute(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
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

    fn execute(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        if plan.kind() == krishiv_plan::ExecutionKind::Streaming {
            debug!(
                backend = "embedded",
                plan = %plan.name(),
                "EmbeddedBackend: delegating streaming plan to SingleNodeBackend"
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
///
/// This is the one backend that genuinely needs async I/O.  We use `block_on`
/// at its single sync/async seam — the `ExecutionBackend::execute` trait method
/// is sync to prevent nested `block_on` deadlocks in embedded/single-node callers.
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

    fn execute(&self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        debug!(
            backend = "distributed",
            coordinator = %self.flight_url,
            plan = %plan.name(),
            kind = %plan.kind(),
            sql = %flight_client::plan_to_sql(plan),
            "DistributedBackend: submitting plan via Flight SQL"
        );
        krishiv_common::async_util::block_on(flight_client::execute_remote_plan(
            &self.flight_url,
            plan,
        ))?;
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

    /// DistributedBackend::execute is now sync (block_on at I/O boundary).
    /// The tokio runtime is needed only for the Flight server background task.
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
        let backend = DistributedBackend::new(url);
        let plan = PhysicalPlan::new("SELECT 1 AS n", ExecutionKind::Batch);
        // execute is now sync — no .await needed, no nested block_on risk
        let report = backend.execute(&plan).expect("execute");
        assert!(report.accepted());
        server.abort();
    }
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    use super::{
        DistributedBackend, EmbeddedBackend, ExecutionBackend, ExecutionReport, JobId, JobState,
        JobStatus, LocalJobRegistry, RuntimeError, SingleNodeBackend, TaskReport, TaskSpec,
        accept_local_plan,
    };

    #[test]
    fn embedded_backend_accepts_bootstrap_plan() {
        let plan = PhysicalPlan::new("bootstrap", ExecutionKind::Batch);
        let backend = EmbeddedBackend::default();

        let report = backend.execute(&plan).expect("execute");

        assert_eq!(report.backend(), "embedded");
        assert_eq!(report.plan_name(), "bootstrap");
        assert!(report.accepted());
    }

    #[test]
    fn embedded_accepts_batch_plan_only() {
        // EmbeddedBackend::execute returns backend="embedded" for batch plans.
        let plan = PhysicalPlan::new("SELECT 1", ExecutionKind::Batch);
        let backend = EmbeddedBackend::default();
        let report = backend.execute(&plan).expect("execute");
        assert_eq!(report.backend(), "embedded");
        assert!(report.accepted());
    }

    #[test]
    fn single_node_accepts_streaming_plan() {
        let plan = PhysicalPlan::new("stream:tw:key=u", ExecutionKind::Batch);
        let backend = SingleNodeBackend;
        let report = backend.execute(&plan).expect("execute");
        assert_eq!(report.backend(), "single-node");
        assert!(report.accepted());
    }

    #[test]
    fn runtime_error_display_unsupported() {
        let err = RuntimeError::unsupported("shuffle");
        assert_eq!(err.to_string(), "unsupported runtime feature: shuffle");
    }

    #[test]
    fn runtime_error_display_transport() {
        let err = RuntimeError::transport("connection refused");
        assert_eq!(err.to_string(), "transport error: connection refused");
    }

    #[test]
    fn runtime_error_display_plan_rejected() {
        let err = RuntimeError::plan_rejected("missing output schema");
        assert_eq!(err.to_string(), "plan rejected: missing output schema");
    }

    #[test]
    fn runtime_error_display_partial_result() {
        let err = RuntimeError::partial_result(3, 1);
        assert_eq!(
            err.to_string(),
            "partial result: 3 partitions succeeded, 1 failed"
        );
    }

    #[test]
    fn runtime_error_display_invalid_state() {
        let err = RuntimeError::InvalidState {
            message: "job not found".into(),
        };
        assert_eq!(err.to_string(), "invalid runtime state: job not found");
    }

    #[test]
    fn runtime_error_is_std_error() {
        let err = RuntimeError::unsupported("test");
        let e: &dyn std::error::Error = &err;
        assert!(e.source().is_none());
    }

    #[test]
    fn runtime_error_clone_and_eq() {
        let err1 = RuntimeError::transport("fail");
        let err2 = err1.clone();
        assert_eq!(err1, err2);
    }

    #[test]
    fn job_id_new_and_as_str() {
        let id = JobId::new("job-42");
        assert_eq!(id.as_str(), "job-42");
    }

    #[test]
    fn job_id_empty_string() {
        let id = JobId::new("");
        assert_eq!(id.as_str(), "");
    }

    #[test]
    fn job_id_special_chars() {
        let id = JobId::new("j-1.2_3");
        assert_eq!(id.as_str(), "j-1.2_3");
    }

    #[test]
    fn job_id_clone_eq_hash() {
        let a = JobId::new("x");
        let b = a.clone();
        assert_eq!(a, b);
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(a, 1);
        assert_eq!(map.get(&b), Some(&1));
    }

    #[test]
    fn job_state_display_pending() {
        assert_eq!(JobState::Pending.to_string(), "pending");
    }

    #[test]
    fn job_state_display_running() {
        assert_eq!(JobState::Running.to_string(), "running");
    }

    #[test]
    fn job_state_display_succeeded() {
        assert_eq!(JobState::Succeeded.to_string(), "succeeded");
    }

    #[test]
    fn job_state_display_failed() {
        assert_eq!(JobState::Failed.to_string(), "failed");
    }

    #[test]
    fn job_status_constructors_and_accessors() {
        let id = JobId::new("j1");
        let status = JobStatus::new(id.clone(), "my-job", JobState::Running);
        assert_eq!(status.id(), &id);
        assert_eq!(status.name(), "my-job");
        assert_eq!(status.state(), JobState::Running);
    }

    #[test]
    fn job_status_clone_and_eq() {
        let s1 = JobStatus::new(JobId::new("j1"), "j", JobState::Pending);
        let s2 = s1.clone();
        assert_eq!(s1, s2);
    }

    #[test]
    fn local_job_registry_empty_list() {
        let reg = LocalJobRegistry::default();
        assert!(reg.list().is_empty());
    }

    #[test]
    fn local_job_registry_upsert_adds_new() {
        let mut reg = LocalJobRegistry::default();
        let s = JobStatus::new(JobId::new("j1"), "job1", JobState::Pending);
        reg.upsert(s);
        assert_eq!(reg.list().len(), 1);
        assert_eq!(reg.list()[0].id().as_str(), "j1");
    }

    #[test]
    fn local_job_registry_upsert_replaces_existing() {
        let mut reg = LocalJobRegistry::default();
        reg.upsert(JobStatus::new(JobId::new("j1"), "v1", JobState::Pending));
        reg.upsert(JobStatus::new(JobId::new("j1"), "v2", JobState::Running));
        assert_eq!(reg.list().len(), 1);
        assert_eq!(reg.list()[0].name(), "v2");
        assert_eq!(reg.list()[0].state(), JobState::Running);
    }

    #[test]
    fn local_job_registry_snapshot() {
        let mut reg = LocalJobRegistry::default();
        reg.upsert(JobStatus::new(JobId::new("j1"), "a", JobState::Pending));
        reg.upsert(JobStatus::new(JobId::new("j2"), "b", JobState::Running));
        let snap = reg.snapshot();
        assert_eq!(snap.len(), 2);
    }

    #[test]
    fn task_spec_accessors() {
        let t = TaskSpec::new("t-1", "do stuff");
        assert_eq!(t.id(), "t-1");
        assert_eq!(t.description(), "do stuff");
    }

    #[test]
    fn task_spec_clone_and_eq() {
        let t1 = TaskSpec::new("t", "desc");
        let t2 = t1.clone();
        assert_eq!(t1, t2);
    }

    #[test]
    fn task_report_accessors() {
        let r = TaskReport::new("t-1", JobState::Succeeded);
        assert_eq!(r.task_id(), "t-1");
        assert_eq!(r.state(), JobState::Succeeded);
    }

    #[test]
    fn task_report_clone_and_eq() {
        let r1 = TaskReport::new("t", JobState::Failed);
        let r2 = r1.clone();
        assert_eq!(r1, r2);
    }

    #[test]
    fn execution_report_accessors() {
        let r = ExecutionReport::new("single-node", "plan-1", ExecutionKind::Batch, true);
        assert_eq!(r.backend(), "single-node");
        assert_eq!(r.plan_name(), "plan-1");
        assert_eq!(r.kind(), ExecutionKind::Batch);
        assert!(r.accepted());
    }

    #[test]
    fn execution_report_not_accepted() {
        let r = ExecutionReport::new("embedded", "p", ExecutionKind::Streaming, false);
        assert!(!r.accepted());
    }

    #[test]
    fn execution_report_clone_and_eq() {
        let r1 = ExecutionReport::new("b", "p", ExecutionKind::Batch, true);
        let r2 = r1.clone();
        assert_eq!(r1, r2);
    }

    #[test]
    fn accept_local_plan_rejects_empty_name() {
        let plan = PhysicalPlan::new("  ", ExecutionKind::Batch);
        let err = accept_local_plan("test", &plan).unwrap_err();
        assert!(matches!(err, RuntimeError::PlanRejected { .. }));
    }

    #[test]
    fn accept_local_plan_accepts_valid_name() {
        let plan = PhysicalPlan::new("my-plan", ExecutionKind::Batch);
        let report = accept_local_plan("backend", &plan).unwrap();
        assert!(report.accepted());
        assert_eq!(report.backend(), "backend");
    }

    #[test]
    fn distributed_backend_new_and_flight_url() {
        let b = DistributedBackend::new("http://localhost:50051");
        assert_eq!(b.flight_url(), "http://localhost:50051");
        assert_eq!(b.backend_name(), "distributed");
    }

    #[test]
    fn distributed_backend_clone() {
        let b = DistributedBackend::new("http://x");
        let b2 = b.clone();
        assert_eq!(b2.flight_url(), "http://x");
    }

    #[test]
    fn single_node_backend_name() {
        let b = SingleNodeBackend;
        assert_eq!(b.backend_name(), "single-node");
    }

    #[test]
    fn embedded_backend_name() {
        let b = EmbeddedBackend::default();
        assert_eq!(b.backend_name(), "embedded");
    }

    #[test]
    fn single_node_accepts_batch_plan() {
        let plan = PhysicalPlan::new("SELECT 1", ExecutionKind::Batch);
        let b = SingleNodeBackend;
        let report = b.execute(&plan).expect("execute");
        assert!(report.accepted());
    }
}
