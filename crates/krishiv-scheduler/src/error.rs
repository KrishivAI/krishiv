//! Scheduler errors and result aliases.

use krishiv_proto::{
    CoordinatorId, CoordinatorState, ExecutorId, JobId, LeaseGeneration, StageId, TaskId,
};

/// Scheduler result alias.
pub type SchedulerResult<T> = Result<T, SchedulerError>;

/// Stable operator-facing classification for scheduler failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// The operation may succeed after retry or failover.
    Retryable,
    /// The engine reached a non-retryable terminal condition.
    Terminal,
    /// The submitted request, plan, or identifier is invalid.
    UserError,
}

/// Result of applying a task status update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskUpdateOutcome {
    /// The update changed scheduler state.
    Applied,
    /// The update was already reflected in scheduler state.
    Duplicate,
}
/// Why a task status update was fenced off as stale.
///
/// All three are fenced identically; they are separated so an operator
/// reading a log line can tell which one happened without inspecting
/// coordinator state after the fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleTaskReason {
    /// The reported attempt is not the task's current attempt — genuine
    /// attempt fencing, e.g. a speculative duplicate losing the race.
    AttemptMismatch,
    /// The attempt matches, but this executor is not the task's current
    /// assignee: the assignment was cleared or re-placed underneath it.
    NotTheAssignee,
    /// The attempt and assignee match, but the task's state cannot accept
    /// this transition — it already reached a terminal state, or the update
    /// would move it out of a non-running state without going through
    /// `Running`.
    StateCannotAcceptUpdate,
}

impl std::fmt::Display for StaleTaskReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::AttemptMismatch => "reported attempt is not the current attempt",
            Self::NotTheAssignee => "reporter is no longer the assigned executor",
            Self::StateCannotAcceptUpdate => "task state cannot accept this transition",
        };
        f.write_str(s)
    }
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
    /// A task status update was fenced off: the reporter is no longer the
    /// authority for this task's current attempt.
    ///
    /// Three distinct conditions land here (see [`StaleTaskReason`]) and they
    /// are handled identically — the update is dropped and the reporter should
    /// stop — but they are not the same thing. Reporting all three as an
    /// attempt mismatch produced log lines reading "expected attempt 1,
    /// received 1", which sends whoever is debugging after an
    /// attempt-numbering bug that does not exist; the real cause is usually
    /// that the assignment was cleared out from under the reporter (a stall
    /// reset, an executor eviction) or that the task already concluded.
    #[error(
        "stale task update for {task_id} ({reason}): current attempt {expected}, reported {received}"
    )]
    StaleTaskAttempt {
        task_id: TaskId,
        expected: u32,
        received: u32,
        reason: StaleTaskReason,
    },
    /// Job submission was invalid.
    #[error("invalid job: {message}")]
    InvalidJob { message: String },
    /// Distributed DAG conversion failed.
    #[error("invalid plan: {message}")]
    InvalidPlan { message: String },
    /// A submitted job reached a terminal `Failed`/`Cancelled` state during
    /// execution. `reason` carries the terminal failure detail the coordinator
    /// recorded (typically the query-execution error from the worst-affected
    /// task); it is empty when no per-task reason was recorded. Distinct from
    /// [`Self::InvalidJob`] (rejected at submission) so the interface layer can
    /// surface an execution failure with its cause instead of an opaque
    /// transport error (Phase 63 / audit §11 error taxonomy).
    #[error("job {job_id} execution failed: {reason}")]
    JobFailed { job_id: JobId, reason: String },
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

impl SchedulerError {
    /// Classify every scheduler failure without parsing its display string.
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            Self::InactiveCoordinator { .. }
            | Self::StaleExecutorLease { .. }
            | Self::NoExecutors
            | Self::StaleTaskAttempt { .. }
            | Self::Transport { .. }
            | Self::ExecutorUnavailable { .. }
            | Self::Store { .. } => FailureClass::Retryable,
            Self::DuplicateExecutor { .. }
            | Self::UnknownExecutor { .. }
            | Self::DuplicateJob { .. }
            | Self::UnknownJob { .. }
            | Self::UnknownStage { .. }
            | Self::UnknownTask { .. }
            | Self::InvalidJob { .. }
            | Self::InvalidPlan { .. }
            | Self::AssignmentRejected { .. } => FailureClass::UserError,
            Self::JobFailed { .. } | Self::Optimizer(_) => FailureClass::Terminal,
        }
    }

    /// Stable machine-readable reason code for metrics, logs, and APIs.
    pub const fn failure_code(&self) -> &'static str {
        match self {
            Self::InactiveCoordinator { .. } => "inactive_coordinator",
            Self::DuplicateExecutor { .. } => "duplicate_executor",
            Self::UnknownExecutor { .. } => "unknown_executor",
            Self::StaleExecutorLease { .. } => "stale_executor_lease",
            Self::NoExecutors => "no_executors",
            Self::DuplicateJob { .. } => "duplicate_job",
            Self::UnknownJob { .. } => "unknown_job",
            Self::UnknownStage { .. } => "unknown_stage",
            Self::UnknownTask { .. } => "unknown_task",
            Self::StaleTaskAttempt { .. } => "stale_task_attempt",
            Self::InvalidJob { .. } => "invalid_job",
            Self::InvalidPlan { .. } => "invalid_plan",
            Self::JobFailed { .. } => "job_failed",
            Self::Optimizer(_) => "optimizer",
            Self::Transport { .. } => "transport",
            Self::AssignmentRejected { .. } => "assignment_rejected",
            Self::ExecutorUnavailable { .. } => "executor_unavailable",
            Self::Store { .. } => "store",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_taxonomy_is_typed_and_stable() {
        let retryable = SchedulerError::NoExecutors;
        assert_eq!(retryable.failure_class(), FailureClass::Retryable);
        assert_eq!(retryable.failure_code(), "no_executors");

        let user = SchedulerError::InvalidJob {
            message: "bad partition count".into(),
        };
        assert_eq!(user.failure_class(), FailureClass::UserError);
        assert_eq!(user.failure_code(), "invalid_job");

        let terminal = SchedulerError::JobFailed {
            job_id: JobId::try_new("terminal-job").unwrap(),
            reason: "retry budget exhausted".into(),
        };
        assert_eq!(terminal.failure_class(), FailureClass::Terminal);
        assert_eq!(terminal.failure_code(), "job_failed");
        assert_eq!(
            serde_json::to_string(&FailureClass::UserError).unwrap(),
            "\"user_error\""
        );
    }
}
