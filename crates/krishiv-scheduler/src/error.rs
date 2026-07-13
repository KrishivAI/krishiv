//! Scheduler errors and result aliases.

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
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    /// The coordinator is not active.
    #[error(
        "coordinator {coordinator_id} is {state}; only the active coordinator may mutate state"
    )]
    InactiveCoordinator {
        coordinator_id: CoordinatorId,
        state: CoordinatorState,
    },
    /// Executor already exists.
    #[error("executor already registered: {executor_id}")]
    DuplicateExecutor { executor_id: ExecutorId },
    /// Executor was not found.
    #[error("unknown executor: {executor_id}")]
    UnknownExecutor { executor_id: ExecutorId },
    /// Executor used an older or otherwise invalid lease generation.
    #[error(
        "stale executor lease for {executor_id}: expected generation {expected}, received {received}"
    )]
    StaleExecutorLease {
        executor_id: ExecutorId,
        expected: LeaseGeneration,
        received: LeaseGeneration,
    },
    /// No healthy executors are available for placement.
    #[error("no healthy executors are available")]
    NoExecutors,
    /// Job already exists.
    #[error("job already exists: {job_id}")]
    DuplicateJob { job_id: JobId },
    /// Job was not found.
    #[error("unknown job: {job_id}")]
    UnknownJob { job_id: JobId },
    /// Stage was not found.
    #[error("unknown stage: {stage_id}")]
    UnknownStage { stage_id: StageId },
    /// Task was not found.
    #[error("unknown task: {task_id}")]
    UnknownTask { task_id: TaskId },
    /// Task status referenced an attempt that is no longer current.
    #[error("stale task attempt for {task_id}: expected attempt {expected}, received {received}")]
    StaleTaskAttempt {
        task_id: TaskId,
        expected: u32,
        received: u32,
    },
    /// Job submission was invalid.
    #[error("invalid job: {message}")]
    InvalidJob { message: String },
    /// Distributed DAG conversion failed.
    #[error("invalid plan: {message}")]
    InvalidPlan { message: String },
    /// Adaptive query optimization failed.
    #[error(transparent)]
    Optimizer(#[from] krishiv_plan::optimizer::OptimizerError),
    /// Coordinator/executor transport failed.
    #[error("transport error: {message}")]
    Transport { message: String },
    /// An executor permanently rejected a task assignment as invalid
    /// (a non-retryable gRPC status such as `InvalidArgument`). The task
    /// payload is malformed, so re-delivering it can never succeed — the
    /// launch loop fails the job terminally instead of retrying forever.
    #[error("executor {endpoint} rejected assignment: {message}")]
    AssignmentRejected { endpoint: String, message: String },
    /// Executor endpoint is unavailable for task dispatch.
    #[error("executor endpoint {endpoint} unavailable: {reason}")]
    ExecutorUnavailable { endpoint: String, reason: String },
    /// Storage/persistence backend failed.
    #[error("store error: {message}")]
    Store { message: String },
}
