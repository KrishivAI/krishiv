//! Scheduler errors and result aliases.

use std::error::Error;
use std::fmt;

use krishiv_proto::{
    CoordinatorId, CoordinatorState, ExecutorId, JobId, LeaseGeneration, StageId, TaskId,
};

/// Scheduler result alias.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

/// Result of applying a task status update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdateOutcome {
    /// The update changed scheduler state.
    Applied,
    /// The update was already reflected in scheduler state.
    Duplicate,
}
/// Scheduler and coordinator errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    /// The coordinator is not active.
    InactiveCoordinator {
        coordinator_id: CoordinatorId,
        state: CoordinatorState,
    },
    /// Executor already exists.
    DuplicateExecutor { executor_id: ExecutorId },
    /// Executor was not found.
    UnknownExecutor { executor_id: ExecutorId },
    /// Executor used an older or otherwise invalid lease generation.
    StaleExecutorLease {
        executor_id: ExecutorId,
        expected: LeaseGeneration,
        received: LeaseGeneration,
    },
    /// No healthy executors are available for placement.
    NoExecutors,
    /// Job already exists.
    DuplicateJob { job_id: JobId },
    /// Job was not found.
    UnknownJob { job_id: JobId },
    /// Stage was not found.
    UnknownStage { stage_id: StageId },
    /// Task was not found.
    UnknownTask { task_id: TaskId },
    /// Task status referenced an attempt that is no longer current.
    StaleTaskAttempt {
        task_id: TaskId,
        expected: u32,
        received: u32,
    },
    /// Job submission was invalid.
    InvalidJob { message: String },
    /// Distributed DAG conversion failed.
    InvalidPlan { message: String },
    /// Coordinator/executor transport failed.
    Transport { message: String },
    /// Executor endpoint is unavailable for task dispatch.
    ExecutorUnavailable { endpoint: String, reason: String },
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InactiveCoordinator {
                coordinator_id,
                state,
            } => write!(
                f,
                "coordinator {coordinator_id} is {state}; only the active coordinator may mutate state"
            ),
            Self::DuplicateExecutor { executor_id } => {
                write!(f, "executor already registered: {executor_id}")
            }
            Self::UnknownExecutor { executor_id } => write!(f, "unknown executor: {executor_id}"),
            Self::StaleExecutorLease {
                executor_id,
                expected,
                received,
            } => write!(
                f,
                "stale executor lease for {executor_id}: expected generation {expected}, received {received}"
            ),
            Self::NoExecutors => f.write_str("no healthy executors are available"),
            Self::DuplicateJob { job_id } => write!(f, "job already exists: {job_id}"),
            Self::UnknownJob { job_id } => write!(f, "unknown job: {job_id}"),
            Self::UnknownStage { stage_id } => write!(f, "unknown stage: {stage_id}"),
            Self::UnknownTask { task_id } => write!(f, "unknown task: {task_id}"),
            Self::StaleTaskAttempt {
                task_id,
                expected,
                received,
            } => write!(
                f,
                "stale task attempt for {task_id}: expected attempt {expected}, received {received}"
            ),
            Self::InvalidJob { message } => write!(f, "invalid job: {message}"),
            Self::InvalidPlan { message } => write!(f, "invalid plan: {message}"),
            Self::Transport { message } => write!(f, "transport error: {message}"),
            Self::ExecutorUnavailable { endpoint, reason } => {
                write!(f, "executor endpoint {endpoint} unavailable: {reason}")
            }
        }
    }
}

/// NOTE: `source()` returns `None` for all variants because no variant wraps an
/// inner `dyn Error`.  Variants that carry a `message: String` describe the
/// cause inline.  If future variants wrap boxed errors (e.g. `StoreError`,
/// `EtcdError`, `LeaseError`), their `source()` must delegate to the inner
/// error to preserve the error chain.
impl Error for SchedulerError {}
