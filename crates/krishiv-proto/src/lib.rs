#![forbid(unsafe_code)]

//! R2/R3 control-plane contracts for Krishiv.
//!
//! This crate keeps Rust domain contracts as the source of scheduler semantics
//! and contains the generated protobuf/tonic edge for network transport. R3.1
//! maps coordinator/executor gRPC messages into these domain contracts without
//! making scheduler code depend on Kubernetes details.

use std::error::Error;
use std::fmt;

/// Result alias for control-plane contract validation.
pub type ProtoResult<T> = Result<T, IdError>;

/// Identifier validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdError {
    kind: &'static str,
    reason: &'static str,
}

impl IdError {
    fn empty(kind: &'static str) -> Self {
        Self {
            kind,
            reason: "cannot be empty",
        }
    }

    fn zero(kind: &'static str) -> Self {
        Self {
            kind,
            reason: "must be greater than zero",
        }
    }

    /// Identifier kind that failed validation.
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// Human-readable validation reason.
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.kind, self.reason)
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

/// Monotonic task attempt identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(u32);

impl AttemptId {
    /// Create an attempt id after validation.
    pub fn try_new(value: u32) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("attempt id"));
        }
        Ok(Self(value))
    }

    /// First attempt for a task.
    pub fn initial() -> Self {
        Self(1)
    }

    /// Next monotonic attempt id.
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Numeric attempt id.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic executor lease generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseGeneration(u64);

impl LeaseGeneration {
    /// Create a lease generation after validation.
    pub fn try_new(value: u64) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("lease generation"));
        }
        Ok(Self(value))
    }

    /// First lease generation for an executor registration.
    pub fn initial() -> Self {
        Self(1)
    }

    /// Next monotonic lease generation.
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Numeric lease generation.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LeaseGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Version for coordinator/executor transport contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransportVersion {
    major: u16,
    minor: u16,
}

impl TransportVersion {
    /// R3.1 transport contract version.
    pub const R3_1: Self = Self { major: 3, minor: 1 };

    /// Current transport contract version.
    pub const CURRENT: Self = Self::R3_1;

    /// Create a transport version.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Major version.
    pub fn major(self) -> u16 {
        self.major
    }

    /// Minor version.
    pub fn minor(self) -> u16 {
        self.minor
    }

    /// Whether this version can satisfy a peer requiring `required`.
    pub fn is_compatible_with(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

impl fmt::Display for TransportVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

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

/// Result classification for versioned coordinator/executor transport calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportDisposition {
    /// Request was accepted and applied.
    Accepted,
    /// Request was rejected before mutation.
    Rejected,
    /// Request was already applied and is safe to ignore.
    Duplicate,
    /// Request referenced an older task attempt.
    StaleAttempt,
    /// Request referenced an older executor lease generation.
    StaleLease,
    /// Request referenced an unknown job.
    UnknownJob,
    /// Request referenced an unknown task.
    UnknownTask,
    /// Request referenced an unknown executor.
    UnknownExecutor,
}

impl fmt::Display for TransportDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted => f.write_str("accepted"),
            Self::Rejected => f.write_str("rejected"),
            Self::Duplicate => f.write_str("duplicate"),
            Self::StaleAttempt => f.write_str("stale_attempt"),
            Self::StaleLease => f.write_str("stale_lease"),
            Self::UnknownJob => f.write_str("unknown_job"),
            Self::UnknownTask => f.write_str("unknown_task"),
            Self::UnknownExecutor => f.write_str("unknown_executor"),
        }
    }
}

/// Executor registration request sent from executor to coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterExecutorRequest {
    version: TransportVersion,
    descriptor: ExecutorDescriptor,
}

impl RegisterExecutorRequest {
    /// Create a registration request using the current transport version.
    pub fn new(descriptor: ExecutorDescriptor) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            descriptor,
        }
    }

    /// Create a registration request with an explicit transport version.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
    }

    /// Executor descriptor.
    pub fn descriptor(&self) -> &ExecutorDescriptor {
        &self.descriptor
    }
}

/// Executor registration response sent from coordinator to executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterExecutorResponse {
    version: TransportVersion,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    disposition: TransportDisposition,
    message: Option<String>,
}

impl RegisterExecutorResponse {
    /// Create a registration response using the current transport version.
    pub fn new(
        executor_id: ExecutorId,
        lease_generation: LeaseGeneration,
        disposition: TransportDisposition,
    ) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            executor_id,
            lease_generation,
            disposition,
            message: None,
        }
    }

    /// Override the transport version when mapping from a wire response.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a human-readable response message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Lease generation granted by the coordinator.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Response disposition.
    pub fn disposition(&self) -> TransportDisposition {
        self.disposition
    }

    /// Optional response message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Reference to a task attempt currently owned by an executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAttemptRef {
    job_id: JobId,
    stage_id: StageId,
    task_id: TaskId,
    attempt_id: AttemptId,
}

impl TaskAttemptRef {
    /// Create a task attempt reference.
    pub fn new(job_id: JobId, stage_id: StageId, task_id: TaskId, attempt_id: AttemptId) -> Self {
        Self {
            job_id,
            stage_id,
            task_id,
            attempt_id,
        }
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

    /// Attempt id.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
}

/// Executor heartbeat request sent from executor to coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHeartbeatRequest {
    version: TransportVersion,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    state: ExecutorState,
    running_attempts: Vec<TaskAttemptRef>,
}

impl ExecutorHeartbeatRequest {
    /// Create a heartbeat request using the current transport version.
    pub fn new(
        executor_id: ExecutorId,
        lease_generation: LeaseGeneration,
        state: ExecutorState,
    ) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            executor_id,
            lease_generation,
            state,
            running_attempts: Vec::new(),
        }
    }

    /// Override the transport version when mapping from a wire request.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach running attempts.
    #[must_use]
    pub fn with_running_attempts(mut self, running_attempts: Vec<TaskAttemptRef>) -> Self {
        self.running_attempts = running_attempts;
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Executor lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Reported executor state.
    pub fn state(&self) -> ExecutorState {
        self.state
    }

    /// Running task attempts.
    pub fn running_attempts(&self) -> &[TaskAttemptRef] {
        &self.running_attempts
    }
}

/// Executor heartbeat response sent from coordinator to executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHeartbeatResponse {
    version: TransportVersion,
    lease_generation: LeaseGeneration,
    disposition: TransportDisposition,
    message: Option<String>,
}

impl ExecutorHeartbeatResponse {
    /// Create a heartbeat response using the current transport version.
    pub fn new(lease_generation: LeaseGeneration, disposition: TransportDisposition) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            lease_generation,
            disposition,
            message: None,
        }
    }

    /// Override the transport version when mapping from a wire response.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a human-readable response message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
    }

    /// Current coordinator-side lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Response disposition.
    pub fn disposition(&self) -> TransportDisposition {
        self.disposition
    }

    /// Optional response message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Descriptor for one input partition assigned to an executor task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputPartition {
    partition_id: String,
    description: String,
}

impl InputPartition {
    /// Create an input partition descriptor.
    pub fn new(partition_id: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            partition_id: partition_id.into(),
            description: description.into(),
        }
    }

    /// Partition id.
    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    /// Human-readable partition description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Opaque local execution fragment assigned to an executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanFragment {
    description: String,
}

impl PlanFragment {
    /// Create a plan fragment descriptor.
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
        }
    }

    /// Human-readable fragment description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Output destination kind for an executor task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputContractKind {
    /// Return bounded record batches through the coordinator path.
    InlineRecordBatches,
    /// Write output to a local file path.
    LocalFile,
    /// Write output to the future R4 shuffle service.
    Shuffle,
    /// Write output to a future connector sink.
    Sink,
}

impl fmt::Display for OutputContractKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InlineRecordBatches => f.write_str("inline_record_batches"),
            Self::LocalFile => f.write_str("local_file"),
            Self::Shuffle => f.write_str("shuffle"),
            Self::Sink => f.write_str("sink"),
        }
    }
}

/// Output contract for an executor task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputContract {
    kind: OutputContractKind,
    description: String,
}

impl OutputContract {
    /// Create an output contract.
    pub fn new(kind: OutputContractKind, description: impl Into<String>) -> Self {
        Self {
            kind,
            description: description.into(),
        }
    }

    /// Output kind.
    pub fn kind(&self) -> OutputContractKind {
        self.kind
    }

    /// Human-readable output description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Versioned task assignment sent from coordinator to executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskAssignment {
    version: TransportVersion,
    job_id: JobId,
    stage_id: StageId,
    task_id: TaskId,
    attempt_id: AttemptId,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    input_partitions: Vec<InputPartition>,
    plan_fragment: PlanFragment,
    output_contract: OutputContract,
}

impl ExecutorTaskAssignment {
    /// Create a task assignment using the current transport version.
    pub fn new(
        ids: TaskAttemptRef,
        executor_id: ExecutorId,
        lease_generation: LeaseGeneration,
        plan_fragment: PlanFragment,
        output_contract: OutputContract,
    ) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            job_id: ids.job_id,
            stage_id: ids.stage_id,
            task_id: ids.task_id,
            attempt_id: ids.attempt_id,
            executor_id,
            lease_generation,
            input_partitions: Vec::new(),
            plan_fragment,
            output_contract,
        }
    }

    /// Attach input partitions.
    #[must_use]
    pub fn with_input_partitions(mut self, input_partitions: Vec<InputPartition>) -> Self {
        self.input_partitions = input_partitions;
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
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

    /// Attempt id.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Lease generation required by this assignment.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Input partitions.
    pub fn input_partitions(&self) -> &[InputPartition] {
        &self.input_partitions
    }

    /// Plan fragment.
    pub fn plan_fragment(&self) -> &PlanFragment {
        &self.plan_fragment
    }

    /// Output contract.
    pub fn output_contract(&self) -> &OutputContract {
        &self.output_contract
    }
}

/// Versioned task status update sent from executor to coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusRequest {
    version: TransportVersion,
    job_id: JobId,
    stage_id: StageId,
    task_id: TaskId,
    attempt_id: AttemptId,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    state: TaskState,
    message: Option<String>,
}

impl TaskStatusRequest {
    /// Create a task status request using the current transport version.
    pub fn new(
        ids: TaskAttemptRef,
        executor_id: ExecutorId,
        lease_generation: LeaseGeneration,
        state: TaskState,
    ) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            job_id: ids.job_id,
            stage_id: ids.stage_id,
            task_id: ids.task_id,
            attempt_id: ids.attempt_id,
            executor_id,
            lease_generation,
            state,
            message: None,
        }
    }

    /// Override the transport version when mapping from a wire request.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a human-readable status message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
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

    /// Attempt id.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Lease generation used by this status update.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Reported task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Optional status message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Versioned task status response sent from coordinator to executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusResponse {
    version: TransportVersion,
    disposition: TransportDisposition,
    message: Option<String>,
}

impl TaskStatusResponse {
    /// Create a task status response using the current transport version.
    pub fn new(disposition: TransportDisposition) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            disposition,
            message: None,
        }
    }

    /// Override the transport version when mapping from a wire response.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a human-readable response message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Transport version.
    pub fn version(&self) -> TransportVersion {
        self.version
    }

    /// Response disposition.
    pub fn disposition(&self) -> TransportDisposition {
        self.disposition
    }

    /// Optional response message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Tonic-shaped coordinator service implemented by the active job coordinator.
///
/// This trait is deliberately defined over Krishiv Rust contract structs first.
/// A later R3.1 slice can map these methods to generated protobuf messages and
/// a concrete network server without changing scheduler semantics.
#[tonic::async_trait]
pub trait CoordinatorExecutorService: Send + Sync + 'static {
    /// Register an executor with the active coordinator.
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status>;

    /// Apply an executor heartbeat to the active coordinator.
    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status>;

    /// Apply a task status update to the active coordinator.
    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;
}

/// Tonic-shaped executor service implemented by executor processes.
#[tonic::async_trait]
pub trait ExecutorTaskService: Send + Sync + 'static {
    /// Assign work to an executor.
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;
}

/// Generated protobuf contracts and conversions for the network transport.
pub mod wire {
    use std::error::Error;
    use std::fmt;

    use super::{
        AttemptId, ExecutorDescriptor, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
        ExecutorId, ExecutorState, JobId, LeaseGeneration, RegisterExecutorRequest,
        RegisterExecutorResponse, StageId, TaskAttemptRef, TaskId, TaskState, TaskStatusRequest,
        TaskStatusResponse, TransportDisposition, TransportVersion,
    };

    /// Generated protobuf and tonic service types for `krishiv.transport.v1`.
    pub mod v1 {
        tonic::include_proto!("krishiv.transport.v1");
    }

    /// Error raised when a protobuf message cannot be converted to a domain contract.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct WireError {
        message: String,
    }

    impl WireError {
        fn new(message: impl Into<String>) -> Self {
            Self {
                message: message.into(),
            }
        }

        /// Human-readable conversion failure.
        pub fn message(&self) -> &str {
            &self.message
        }
    }

    impl fmt::Display for WireError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl Error for WireError {}

    type WireResult<T> = Result<T, WireError>;

    /// Convert a domain registration request to protobuf.
    pub fn register_executor_request_to_wire(
        value: RegisterExecutorRequest,
    ) -> v1::RegisterExecutorRequest {
        v1::RegisterExecutorRequest {
            version: Some(transport_version_to_wire(value.version())),
            descriptor: Some(executor_descriptor_to_wire(value.descriptor())),
        }
    }

    /// Convert a protobuf registration request to the domain contract.
    pub fn register_executor_request_from_wire(
        value: v1::RegisterExecutorRequest,
    ) -> WireResult<RegisterExecutorRequest> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let descriptor = executor_descriptor_from_wire(required(value.descriptor, "descriptor")?)?;
        Ok(RegisterExecutorRequest::new(descriptor).with_version(version))
    }

    /// Convert a domain registration response to protobuf.
    pub fn register_executor_response_to_wire(
        value: RegisterExecutorResponse,
    ) -> v1::RegisterExecutorResponse {
        v1::RegisterExecutorResponse {
            version: Some(transport_version_to_wire(value.version())),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            disposition: transport_disposition_to_wire(value.disposition()) as i32,
            message: value.message().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf registration response to the domain contract.
    pub fn register_executor_response_from_wire(
        value: v1::RegisterExecutorResponse,
    ) -> WireResult<RegisterExecutorResponse> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let disposition = transport_disposition_from_wire(value.disposition)?;
        let mut response =
            RegisterExecutorResponse::new(executor_id, lease_generation, disposition)
                .with_version(version);
        if !value.message.is_empty() {
            response = response.with_message(value.message);
        }
        Ok(response)
    }

    /// Convert a domain heartbeat request to protobuf.
    pub fn executor_heartbeat_request_to_wire(
        value: ExecutorHeartbeatRequest,
    ) -> v1::ExecutorHeartbeatRequest {
        v1::ExecutorHeartbeatRequest {
            version: Some(transport_version_to_wire(value.version())),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            state: executor_state_to_wire(value.state()) as i32,
            running_attempts: value
                .running_attempts()
                .iter()
                .map(task_attempt_ref_to_wire)
                .collect(),
        }
    }

    /// Convert a protobuf heartbeat request to the domain contract.
    pub fn executor_heartbeat_request_from_wire(
        value: v1::ExecutorHeartbeatRequest,
    ) -> WireResult<ExecutorHeartbeatRequest> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let state = executor_state_from_wire(value.state)?;
        let running_attempts = value
            .running_attempts
            .into_iter()
            .map(task_attempt_ref_from_wire)
            .collect::<WireResult<Vec<_>>>()?;

        Ok(
            ExecutorHeartbeatRequest::new(executor_id, lease_generation, state)
                .with_version(version)
                .with_running_attempts(running_attempts),
        )
    }

    /// Convert a domain heartbeat response to protobuf.
    pub fn executor_heartbeat_response_to_wire(
        value: ExecutorHeartbeatResponse,
    ) -> v1::ExecutorHeartbeatResponse {
        v1::ExecutorHeartbeatResponse {
            version: Some(transport_version_to_wire(value.version())),
            lease_generation: value.lease_generation().as_u64(),
            disposition: transport_disposition_to_wire(value.disposition()) as i32,
            message: value.message().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf heartbeat response to the domain contract.
    pub fn executor_heartbeat_response_from_wire(
        value: v1::ExecutorHeartbeatResponse,
    ) -> WireResult<ExecutorHeartbeatResponse> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let disposition = transport_disposition_from_wire(value.disposition)?;
        let mut response =
            ExecutorHeartbeatResponse::new(lease_generation, disposition).with_version(version);
        if !value.message.is_empty() {
            response = response.with_message(value.message);
        }
        Ok(response)
    }

    /// Convert a domain task status request to protobuf.
    pub fn task_status_request_to_wire(value: TaskStatusRequest) -> v1::TaskStatusRequest {
        v1::TaskStatusRequest {
            version: Some(transport_version_to_wire(value.version())),
            job_id: value.job_id().as_str().to_owned(),
            stage_id: value.stage_id().as_str().to_owned(),
            task_id: value.task_id().as_str().to_owned(),
            attempt_id: value.attempt_id().as_u32(),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            state: task_state_to_wire(value.state()) as i32,
            message: value.message().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf task status request to the domain contract.
    pub fn task_status_request_from_wire(
        value: v1::TaskStatusRequest,
    ) -> WireResult<TaskStatusRequest> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let ids = TaskAttemptRef::new(
            JobId::try_new(value.job_id).map_err(WireError::from_id)?,
            StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
            TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
            AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
        );
        let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let state = task_state_from_wire(value.state)?;
        let mut request =
            TaskStatusRequest::new(ids, executor_id, lease_generation, state).with_version(version);
        if !value.message.is_empty() {
            request = request.with_message(value.message);
        }
        Ok(request)
    }

    /// Convert a domain task status response to protobuf.
    pub fn task_status_response_to_wire(value: TaskStatusResponse) -> v1::TaskStatusResponse {
        v1::TaskStatusResponse {
            version: Some(transport_version_to_wire(value.version())),
            disposition: transport_disposition_to_wire(value.disposition()) as i32,
            message: value.message().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf task status response to the domain contract.
    pub fn task_status_response_from_wire(
        value: v1::TaskStatusResponse,
    ) -> WireResult<TaskStatusResponse> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let disposition = transport_disposition_from_wire(value.disposition)?;
        let mut response = TaskStatusResponse::new(disposition).with_version(version);
        if !value.message.is_empty() {
            response = response.with_message(value.message);
        }
        Ok(response)
    }

    fn required<T>(value: Option<T>, field: &'static str) -> WireResult<T> {
        value.ok_or_else(|| WireError::new(format!("missing required field `{field}`")))
    }

    fn transport_version_to_wire(value: TransportVersion) -> v1::TransportVersion {
        v1::TransportVersion {
            major: value.major().into(),
            minor: value.minor().into(),
        }
    }

    fn transport_version_from_wire(value: v1::TransportVersion) -> WireResult<TransportVersion> {
        let major = value
            .major
            .try_into()
            .map_err(|_| WireError::new("transport version major is too large"))?;
        let minor = value
            .minor
            .try_into()
            .map_err(|_| WireError::new("transport version minor is too large"))?;
        Ok(TransportVersion::new(major, minor))
    }

    fn executor_descriptor_to_wire(value: &ExecutorDescriptor) -> v1::ExecutorDescriptor {
        v1::ExecutorDescriptor {
            executor_id: value.executor_id().as_str().to_owned(),
            host: value.host().to_owned(),
            slots: value.slots() as u64,
        }
    }

    fn executor_descriptor_from_wire(
        value: v1::ExecutorDescriptor,
    ) -> WireResult<ExecutorDescriptor> {
        let slots = value
            .slots
            .try_into()
            .map_err(|_| WireError::new("executor slots value is too large"))?;
        Ok(ExecutorDescriptor::new(
            ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?,
            value.host,
            slots,
        ))
    }

    fn task_attempt_ref_to_wire(value: &TaskAttemptRef) -> v1::TaskAttemptRef {
        v1::TaskAttemptRef {
            job_id: value.job_id().as_str().to_owned(),
            stage_id: value.stage_id().as_str().to_owned(),
            task_id: value.task_id().as_str().to_owned(),
            attempt_id: value.attempt_id().as_u32(),
        }
    }

    fn task_attempt_ref_from_wire(value: v1::TaskAttemptRef) -> WireResult<TaskAttemptRef> {
        Ok(TaskAttemptRef::new(
            JobId::try_new(value.job_id).map_err(WireError::from_id)?,
            StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
            TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
            AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
        ))
    }

    fn executor_state_to_wire(value: ExecutorState) -> v1::ExecutorState {
        match value {
            ExecutorState::Registered => v1::ExecutorState::Registered,
            ExecutorState::Healthy => v1::ExecutorState::Healthy,
            ExecutorState::Lost => v1::ExecutorState::Lost,
            ExecutorState::Draining => v1::ExecutorState::Draining,
            ExecutorState::Removed => v1::ExecutorState::Removed,
        }
    }

    fn executor_state_from_wire(value: i32) -> WireResult<ExecutorState> {
        match v1::ExecutorState::try_from(value)
            .map_err(|_| WireError::new(format!("unknown executor state value {value}")))?
        {
            v1::ExecutorState::Unspecified => {
                Err(WireError::new("executor state cannot be unspecified"))
            }
            v1::ExecutorState::Registered => Ok(ExecutorState::Registered),
            v1::ExecutorState::Healthy => Ok(ExecutorState::Healthy),
            v1::ExecutorState::Lost => Ok(ExecutorState::Lost),
            v1::ExecutorState::Draining => Ok(ExecutorState::Draining),
            v1::ExecutorState::Removed => Ok(ExecutorState::Removed),
        }
    }

    fn task_state_to_wire(value: TaskState) -> v1::TaskState {
        match value {
            TaskState::Pending => v1::TaskState::Pending,
            TaskState::Assigned => v1::TaskState::Assigned,
            TaskState::Running => v1::TaskState::Running,
            TaskState::Succeeded => v1::TaskState::Succeeded,
            TaskState::Failed => v1::TaskState::Failed,
            TaskState::Retrying => v1::TaskState::Retrying,
            TaskState::Cancelled => v1::TaskState::Cancelled,
        }
    }

    fn task_state_from_wire(value: i32) -> WireResult<TaskState> {
        match v1::TaskState::try_from(value)
            .map_err(|_| WireError::new(format!("unknown task state value {value}")))?
        {
            v1::TaskState::Unspecified => Err(WireError::new("task state cannot be unspecified")),
            v1::TaskState::Pending => Ok(TaskState::Pending),
            v1::TaskState::Assigned => Ok(TaskState::Assigned),
            v1::TaskState::Running => Ok(TaskState::Running),
            v1::TaskState::Succeeded => Ok(TaskState::Succeeded),
            v1::TaskState::Failed => Ok(TaskState::Failed),
            v1::TaskState::Retrying => Ok(TaskState::Retrying),
            v1::TaskState::Cancelled => Ok(TaskState::Cancelled),
        }
    }

    fn transport_disposition_to_wire(value: TransportDisposition) -> v1::TransportDisposition {
        match value {
            TransportDisposition::Accepted => v1::TransportDisposition::Accepted,
            TransportDisposition::Rejected => v1::TransportDisposition::Rejected,
            TransportDisposition::Duplicate => v1::TransportDisposition::Duplicate,
            TransportDisposition::StaleAttempt => v1::TransportDisposition::StaleAttempt,
            TransportDisposition::StaleLease => v1::TransportDisposition::StaleLease,
            TransportDisposition::UnknownJob => v1::TransportDisposition::UnknownJob,
            TransportDisposition::UnknownTask => v1::TransportDisposition::UnknownTask,
            TransportDisposition::UnknownExecutor => v1::TransportDisposition::UnknownExecutor,
        }
    }

    fn transport_disposition_from_wire(value: i32) -> WireResult<TransportDisposition> {
        match v1::TransportDisposition::try_from(value)
            .map_err(|_| WireError::new(format!("unknown transport disposition value {value}")))?
        {
            v1::TransportDisposition::Unspecified => Err(WireError::new(
                "transport disposition cannot be unspecified",
            )),
            v1::TransportDisposition::Accepted => Ok(TransportDisposition::Accepted),
            v1::TransportDisposition::Rejected => Ok(TransportDisposition::Rejected),
            v1::TransportDisposition::Duplicate => Ok(TransportDisposition::Duplicate),
            v1::TransportDisposition::StaleAttempt => Ok(TransportDisposition::StaleAttempt),
            v1::TransportDisposition::StaleLease => Ok(TransportDisposition::StaleLease),
            v1::TransportDisposition::UnknownJob => Ok(TransportDisposition::UnknownJob),
            v1::TransportDisposition::UnknownTask => Ok(TransportDisposition::UnknownTask),
            v1::TransportDisposition::UnknownExecutor => Ok(TransportDisposition::UnknownExecutor),
        }
    }

    impl WireError {
        fn from_id(value: super::IdError) -> Self {
            Self::new(value.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AttemptId, ExecutorHeartbeatRequest, ExecutorId, ExecutorState, ExecutorTaskAssignment,
        InputPartition, JobId, JobKind, JobSpec, JobState, LeaseGeneration, OutputContract,
        OutputContractKind, PlanFragment, RegisterExecutorRequest, StageId, StageSpec,
        TaskAttemptRef, TaskId, TaskSpec, TaskState, TaskStatusRequest, TaskStatusResponse,
        TransportDisposition, TransportVersion,
    };

    #[test]
    fn ids_reject_empty_values() {
        let error = JobId::try_new("   ").unwrap_err();

        assert_eq!(error.kind(), "job id");
    }

    #[test]
    fn numeric_ids_reject_zero_values() {
        let error = AttemptId::try_new(0).unwrap_err();

        assert_eq!(error.kind(), "attempt id");
        assert_eq!(error.reason(), "must be greater than zero");
        assert_eq!(AttemptId::initial().next().as_u32(), 2);
        assert_eq!(LeaseGeneration::initial().next().as_u64(), 2);
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

    #[test]
    fn transport_version_exposes_compatibility() {
        let current = TransportVersion::CURRENT;

        assert_eq!(current.to_string(), "3.1");
        assert!(current.is_compatible_with(TransportVersion::R3_1));
        assert!(!TransportVersion::new(4, 0).is_compatible_with(current));
    }

    #[test]
    fn registration_request_carries_current_version() {
        let request = RegisterExecutorRequest::new(super::ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ));

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn heartbeat_request_carries_running_attempts_and_lease() {
        let attempt = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let heartbeat = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_running_attempts(vec![attempt]);

        assert_eq!(heartbeat.version(), TransportVersion::CURRENT);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert_eq!(
            heartbeat.running_attempts()[0].attempt_id(),
            AttemptId::initial()
        );
    }

    #[test]
    fn executor_task_assignment_carries_attempt_lease_and_contracts() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "return result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first file split")]);

        assert_eq!(assignment.attempt_id(), AttemptId::initial());
        assert_eq!(assignment.lease_generation(), LeaseGeneration::initial());
        assert_eq!(assignment.input_partitions()[0].partition_id(), "part-1");
        assert_eq!(
            assignment.output_contract().kind(),
            OutputContractKind::InlineRecordBatches
        );
    }

    #[test]
    fn task_status_contract_can_report_stale_attempts() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::try_new(3).unwrap(),
        );
        let request = TaskStatusRequest::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::try_new(7).unwrap(),
            TaskState::Succeeded,
        )
        .with_message("complete");
        let response = TaskStatusResponse::new(TransportDisposition::StaleAttempt)
            .with_message("newer attempt already owns this task");

        assert_eq!(request.attempt_id().as_u32(), 3);
        assert_eq!(request.lease_generation().as_u64(), 7);
        assert_eq!(request.message(), Some("complete"));
        assert_eq!(response.disposition(), TransportDisposition::StaleAttempt);
        assert_eq!(
            response.message(),
            Some("newer attempt already owns this task")
        );
    }
}
