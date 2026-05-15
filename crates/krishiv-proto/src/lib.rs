#![forbid(unsafe_code)]

//! R2 control-plane contracts for Krishiv.
//!
//! This crate intentionally starts as Rust data contracts rather than a wire
//! transport. Later R2 slices can map these structs to gRPC/protobuf without
//! making scheduler code depend on Kubernetes or network details.

use std::error::Error;
use std::fmt;

/// Result alias for control-plane contract validation.
pub type ProtoResult<T> = Result<T, IdError>;

/// Identifier validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdError {
    kind: &'static str,
}

impl IdError {
    fn empty(kind: &'static str) -> Self {
        Self { kind }
    }

    /// Identifier kind that failed validation.
    pub fn kind(&self) -> &'static str {
        self.kind
    }
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} cannot be empty", self.kind)
    }
}

impl Error for IdError {}

macro_rules! id_type {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Typed ", $kind, " identifier.")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Create a ", $kind, " identifier after validation.")]
            pub fn try_new(value: impl Into<String>) -> ProtoResult<Self> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdError::empty($kind));
                }
                Ok(Self(value))
            }

            /// Borrow the identifier string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(CoordinatorId, "coordinator id");
id_type!(JobId, "job id");
id_type!(StageId, "stage id");
id_type!(TaskId, "task id");
id_type!(ExecutorId, "executor id");

/// Coordinator service state for the R2 single-active-coordinator model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorState {
    /// Coordinator may mutate job and executor state.
    Active,
    /// Coordinator may observe but must not schedule or mutate job state.
    Standby,
    /// Coordinator is no longer serving.
    Stopped,
}

impl fmt::Display for CoordinatorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Standby => f.write_str("standby"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

/// Distributed job execution kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    /// Bounded batch work.
    Batch,
    /// Early R2 streaming work with R1-level local state semantics.
    Streaming,
}

impl fmt::Display for JobKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Batch => f.write_str("batch"),
            Self::Streaming => f.write_str("streaming"),
        }
    }
}

/// Distributed job lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Job was accepted by the active coordinator.
    Accepted,
    /// Job is being converted into stages and tasks.
    Planning,
    /// At least one stage or task is active.
    Running,
    /// All stages completed successfully.
    Succeeded,
    /// One or more tasks failed and no retry is active.
    Failed,
    /// Job was cancelled by a user or controller.
    Cancelled,
}

impl JobState {
    /// Whether this state is terminal.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted => f.write_str("accepted"),
            Self::Planning => f.write_str("planning"),
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

/// Distributed stage lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageState {
    /// Stage is waiting for placement.
    Pending,
    /// Stage tasks are being assigned.
    Scheduling,
    /// At least one task is active.
    Running,
    /// All stage tasks succeeded.
    Succeeded,
    /// One or more stage tasks failed.
    Failed,
    /// Stage is waiting for a retry attempt.
    Retrying,
    /// Stage was cancelled.
    Cancelled,
}

impl StageState {
    /// Whether this state is terminal.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for StageState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Scheduling => f.write_str("scheduling"),
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::Retrying => f.write_str("retrying"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

/// Distributed task lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Task is waiting for placement.
    Pending,
    /// Task has an executor assignment but has not started.
    Assigned,
    /// Task is running on an executor.
    Running,
    /// Task completed successfully.
    Succeeded,
    /// Task failed on an executor.
    Failed,
    /// Task is waiting to be retried.
    Retrying,
    /// Task was cancelled.
    Cancelled,
}

impl TaskState {
    /// Whether this state is terminal.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Assigned => f.write_str("assigned"),
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::Retrying => f.write_str("retrying"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

/// Executor lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorState {
    /// Executor has registered but has not yet heartbeated.
    Registered,
    /// Executor is healthy and can receive work.
    Healthy,
    /// Executor missed heartbeats or was marked unavailable.
    Lost,
    /// Executor should finish current work but receive no new tasks.
    Draining,
    /// Executor was removed from the active pool.
    Removed,
}

impl ExecutorState {
    /// Whether this executor can receive new assignments.
    pub fn can_accept_work(self) -> bool {
        matches!(self, Self::Registered | Self::Healthy)
    }
}

impl fmt::Display for ExecutorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registered => f.write_str("registered"),
            Self::Healthy => f.write_str("healthy"),
            Self::Lost => f.write_str("lost"),
            Self::Draining => f.write_str("draining"),
            Self::Removed => f.write_str("removed"),
        }
    }
}

/// Job submission contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSpec {
    job_id: JobId,
    name: String,
    kind: JobKind,
    stages: Vec<StageSpec>,
}

impl JobSpec {
    /// Create a job spec.
    pub fn new(job_id: JobId, name: impl Into<String>, kind: JobKind) -> Self {
        Self {
            job_id,
            name: name.into(),
            kind,
            stages: Vec::new(),
        }
    }

    /// Attach a stage.
    #[must_use]
    pub fn with_stage(mut self, stage: StageSpec) -> Self {
        self.stages.push(stage);
        self
    }

    /// Job id.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Job name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Job kind.
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Stages in submission order.
    pub fn stages(&self) -> &[StageSpec] {
        &self.stages
    }

    /// Total task count across all stages.
    pub fn task_count(&self) -> usize {
        self.stages.iter().map(StageSpec::task_count).sum()
    }
}

/// Stage contract inside a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSpec {
    stage_id: StageId,
    name: String,
    tasks: Vec<TaskSpec>,
}

impl StageSpec {
    /// Create an empty stage spec.
    pub fn new(stage_id: StageId, name: impl Into<String>) -> Self {
        Self {
            stage_id,
            name: name.into(),
            tasks: Vec::new(),
        }
    }

    /// Attach a task.
    #[must_use]
    pub fn with_task(mut self, task: TaskSpec) -> Self {
        self.tasks.push(task);
        self
    }

    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Stage name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Tasks in submission order.
    pub fn tasks(&self) -> &[TaskSpec] {
        &self.tasks
    }

    /// Number of tasks in this stage.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }
}

/// Task contract inside a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    task_id: TaskId,
    description: String,
}

impl TaskSpec {
    /// Create a task spec.
    pub fn new(task_id: TaskId, description: impl Into<String>) -> Self {
        Self {
            task_id,
            description: description.into(),
        }
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Human-readable task description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Executor registration contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorDescriptor {
    executor_id: ExecutorId,
    host: String,
    slots: usize,
}

impl ExecutorDescriptor {
    /// Create an executor descriptor.
    pub fn new(executor_id: ExecutorId, host: impl Into<String>, slots: usize) -> Self {
        Self {
            executor_id,
            host: host.into(),
            slots,
        }
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Executor host or pod name.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Advertised task slots.
    pub fn slots(&self) -> usize {
        self.slots
    }
}

/// Executor heartbeat contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHeartbeat {
    executor_id: ExecutorId,
    state: ExecutorState,
    running_tasks: Vec<TaskId>,
}

impl ExecutorHeartbeat {
    /// Create a heartbeat.
    pub fn new(executor_id: ExecutorId, state: ExecutorState) -> Self {
        Self {
            executor_id,
            state,
            running_tasks: Vec::new(),
        }
    }

    /// Attach currently running task ids.
    #[must_use]
    pub fn with_running_tasks(mut self, running_tasks: Vec<TaskId>) -> Self {
        self.running_tasks = running_tasks;
        self
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Reported executor state.
    pub fn state(&self) -> ExecutorState {
        self.state
    }

    /// Running task ids.
    pub fn running_tasks(&self) -> &[TaskId] {
        &self.running_tasks
    }
}

/// Task placement result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAssignment {
    task_id: TaskId,
    executor_id: ExecutorId,
}

impl TaskAssignment {
    /// Create a task assignment.
    pub fn new(task_id: TaskId, executor_id: ExecutorId) -> Self {
        Self {
            task_id,
            executor_id,
        }
    }

    /// Assigned task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Target executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }
}

/// Task status update from an executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusUpdate {
    job_id: JobId,
    stage_id: StageId,
    task_id: TaskId,
    executor_id: ExecutorId,
    state: TaskState,
    attempt: u32,
    message: Option<String>,
}

impl TaskStatusUpdate {
    /// Create a task status update.
    pub fn new(
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
        executor_id: ExecutorId,
        state: TaskState,
        attempt: u32,
    ) -> Self {
        Self {
            job_id,
            stage_id,
            task_id,
            executor_id,
            state,
            attempt,
            message: None,
        }
    }

    /// Attach a human-readable status message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Job id.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Reported task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Task attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Optional status message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExecutorId, ExecutorState, JobId, JobKind, JobSpec, JobState, StageId, StageSpec, TaskId,
        TaskSpec, TaskState,
    };

    #[test]
    fn ids_reject_empty_values() {
        let error = JobId::try_new("   ").unwrap_err();

        assert_eq!(error.kind(), "job id");
    }

    #[test]
    fn job_spec_counts_stage_tasks() {
        let job = JobSpec::new(JobId::try_new("job-1").unwrap(), "demo", JobKind::Batch)
            .with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
                    .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a"))
                    .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "scan b")),
            );

        assert_eq!(job.task_count(), 2);
        assert_eq!(job.kind(), JobKind::Batch);
    }

    #[test]
    fn lifecycle_states_expose_terminal_and_capacity_rules() {
        assert!(JobState::Succeeded.is_terminal());
        assert!(TaskState::Failed.is_terminal());
        assert!(ExecutorState::Healthy.can_accept_work());
        assert!(!ExecutorState::Lost.can_accept_work());
        assert_eq!(ExecutorId::try_new("exec-1").unwrap().as_str(), "exec-1");
    }
}
