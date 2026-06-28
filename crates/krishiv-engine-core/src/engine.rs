//! The engine contract and the job lifecycle handle.

use async_trait::async_trait;
use krishiv_proto::JobId;

use crate::error::{EngineError, EngineResult};
use crate::job::CompiledJob;
use crate::kind::EngineKind;
use crate::runtime::EngineRuntime;

/// Status of a submitted job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    /// Continuous engine still running, or batch job in progress.
    Running,
    /// Ran to completion (batch) or was stopped cleanly.
    Completed,
    /// Terminated with an error.
    Failed,
}

/// Handle to a running or completed job.
///
/// Phase 0 carries identity and status. Lifecycle control — await terminal
/// status, cancel, trigger a savepoint — is added when the placement providers
/// land (Phase 2), where the handle gains the cluster-aware channels.
#[derive(Debug, Clone)]
pub struct JobHandle {
    job_id: JobId,
    status: JobStatus,
}

impl JobHandle {
    /// Build a handle for `job_id` in `status`.
    pub fn new(job_id: JobId, status: JobStatus) -> Self {
        Self { job_id, status }
    }

    /// Build a handle from a job name, validating it into a [`JobId`].
    ///
    /// Lets engine adapters return a handle without depending on
    /// `krishiv-proto` directly.
    pub fn from_name(name: &str, status: JobStatus) -> EngineResult<Self> {
        let job_id = JobId::try_new(name)
            .map_err(|e| EngineError::InvalidJob(format!("invalid job name '{name}': {e}")))?;
        Ok(Self::new(job_id, status))
    }

    /// The job's identity.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// The job's current status.
    pub fn status(&self) -> JobStatus {
        self.status
    }
}

/// One compute engine.
///
/// The three implementations (batch, incremental, streaming) live in their own
/// crates and share this contract so neither the placement layer nor the API
/// surface forks per engine. An engine codes only against the trait objects in
/// [`EngineRuntime`], which is what lets the same engine run embedded or
/// distributed unchanged.
#[async_trait]
pub trait ComputeEngine: Send + Sync {
    /// Which compute model this engine implements.
    fn kind(&self) -> EngineKind;

    /// Plan-time validation: can this engine run `job`? No side effects, no
    /// execution. Returns an error describing the first incompatibility.
    fn validate(&self, job: &CompiledJob) -> EngineResult<()>;

    /// Execute `job` using the placement-provided services in `rt`. A batch
    /// engine returns once the job is complete; a continuous engine returns
    /// once the job is running.
    async fn run(&self, job: CompiledJob, rt: EngineRuntime) -> EngineResult<JobHandle>;
}
