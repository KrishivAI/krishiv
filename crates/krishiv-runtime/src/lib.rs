#![forbid(unsafe_code)]

//! Runtime traits and local backend stubs for Krishiv.
//!
//! R1 bootstrap defines the runtime seams without implementing real query
//! execution. The first real local execution path will be added when
//! DataFusion integration lands.

use std::error::Error;
use std::fmt;

use krishiv_plan::{ExecutionKind, PhysicalPlan};

/// Runtime result alias.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Runtime errors shared by bootstrap backends and traits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// A requested capability is not available in the current release slice.
    Unsupported { feature: String },
    /// Runtime state was invalid for the requested operation.
    InvalidState { message: String },
}

impl RuntimeError {
    /// Create an unsupported-feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { feature } => {
                write!(f, "unsupported runtime feature: {feature}")
            }
            Self::InvalidState { message } => write!(f, "invalid runtime state: {message}"),
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
    /// Add a job status to the registry.
    pub fn record(&mut self, status: JobStatus) {
        self.upsert(status);
    }

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

    /// Execute a physical plan.
    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport>;
}

/// Minimal task executor contract for future scheduler integration.
pub trait TaskExecutor {
    /// Execute one task.
    fn execute_task(&mut self, task: TaskSpec) -> RuntimeResult<TaskReport>;
}

/// Embedded in-process backend stub.
#[derive(Debug, Default)]
pub struct EmbeddedBackend;

impl ExecutionBackend for EmbeddedBackend {
    fn backend_name(&self) -> &str {
        "embedded"
    }

    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        Ok(ExecutionReport::new(
            self.backend_name(),
            plan.name(),
            plan.kind(),
            true,
        ))
    }
}

/// Single-node backend stub.
#[derive(Debug, Default)]
pub struct SingleNodeBackend;

impl ExecutionBackend for SingleNodeBackend {
    fn backend_name(&self) -> &str {
        "single-node"
    }

    fn execute(&mut self, plan: &PhysicalPlan) -> RuntimeResult<ExecutionReport> {
        Ok(ExecutionReport::new(
            self.backend_name(),
            plan.name(),
            plan.kind(),
            true,
        ))
    }
}

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, PhysicalPlan};

    use super::{EmbeddedBackend, ExecutionBackend};

    #[test]
    fn embedded_backend_accepts_bootstrap_plan() {
        let plan = PhysicalPlan::new("bootstrap", ExecutionKind::Batch);
        let mut backend = EmbeddedBackend;

        let report = match backend.execute(&plan) {
            Ok(report) => report,
            Err(error) => panic!("unexpected runtime error: {error}"),
        };

        assert_eq!(report.backend(), "embedded");
        assert_eq!(report.plan_name(), "bootstrap");
        assert!(report.accepted());
    }
}
