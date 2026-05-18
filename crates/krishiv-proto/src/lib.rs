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
    task_endpoint: Option<String>,
}

impl ExecutorDescriptor {
    /// Create an executor descriptor.
    pub fn new(executor_id: ExecutorId, host: impl Into<String>, slots: usize) -> Self {
        Self {
            executor_id,
            host: host.into(),
            slots,
            task_endpoint: None,
        }
    }

    /// Attach the executor-owned task assignment endpoint.
    #[must_use]
    pub fn with_task_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        if !endpoint.trim().is_empty() {
            self.task_endpoint = Some(endpoint);
        }
        self
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

    /// Optional executor-owned task assignment endpoint.
    pub fn task_endpoint(&self) -> Option<&str> {
        self.task_endpoint.as_deref()
    }
}

/// Output metadata reported by an executor without carrying Arrow payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputMetadata {
    output_kind: String,
    row_count: u64,
    batch_count: u64,
    column_count: u64,
}

impl TaskOutputMetadata {
    /// Create task output metadata.
    pub fn new(
        output_kind: impl Into<String>,
        row_count: u64,
        batch_count: u64,
        column_count: u64,
    ) -> Self {
        Self {
            output_kind: output_kind.into(),
            row_count,
            batch_count,
            column_count,
        }
    }

    /// Output kind label.
    pub fn output_kind(&self) -> &str {
        &self.output_kind
    }

    /// Number of rows produced.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Number of record batches produced.
    pub fn batch_count(&self) -> u64 {
        self.batch_count
    }

    /// Number of columns produced.
    pub fn column_count(&self) -> u64 {
        self.column_count
    }
}

/// Versioned executor deregistration request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeregisterExecutorRequest {
    version: TransportVersion,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    reason: Option<String>,
}

impl DeregisterExecutorRequest {
    /// Create a deregistration request using the current transport version.
    pub fn new(executor_id: ExecutorId, lease_generation: LeaseGeneration) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            executor_id,
            lease_generation,
            reason: None,
        }
    }

    /// Override transport version.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a reason.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
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

    /// Optional reason.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

/// Versioned executor deregistration response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeregisterExecutorResponse {
    version: TransportVersion,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    disposition: TransportDisposition,
    message: Option<String>,
}

impl DeregisterExecutorResponse {
    /// Create a deregistration response using the current transport version.
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

    /// Override transport version.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach response message.
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

    /// Current executor lease generation.
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

/// Executor heartbeat contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorHeartbeat {
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    state: ExecutorState,
    running_tasks: Vec<TaskId>,
    memory_used_bytes: Option<u64>,
    memory_limit_bytes: Option<u64>,
    active_task_count: Option<u32>,
}

impl ExecutorHeartbeat {
    /// Create a heartbeat.
    pub fn new(executor_id: ExecutorId, state: ExecutorState) -> Self {
        Self {
            executor_id,
            lease_generation: LeaseGeneration::initial(),
            state,
            running_tasks: Vec::new(),
            memory_used_bytes: None,
            memory_limit_bytes: None,
            active_task_count: None,
        }
    }

    /// Attach the executor lease generation used for this heartbeat.
    #[must_use]
    pub fn with_lease_generation(mut self, lease_generation: LeaseGeneration) -> Self {
        self.lease_generation = lease_generation;
        self
    }

    /// Attach currently running task ids.
    #[must_use]
    pub fn with_running_tasks(mut self, running_tasks: Vec<TaskId>) -> Self {
        self.running_tasks = running_tasks;
        self
    }

    /// Attach memory used bytes.
    #[must_use]
    pub fn with_memory_used_bytes(mut self, bytes: u64) -> Self {
        self.memory_used_bytes = Some(bytes);
        self
    }

    /// Attach memory limit bytes.
    #[must_use]
    pub fn with_memory_limit_bytes(mut self, bytes: u64) -> Self {
        self.memory_limit_bytes = Some(bytes);
        self
    }

    /// Attach active task count.
    #[must_use]
    pub fn with_active_task_count(mut self, count: u32) -> Self {
        self.active_task_count = Some(count);
        self
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

    /// Running task ids.
    pub fn running_tasks(&self) -> &[TaskId] {
        &self.running_tasks
    }

    /// Memory used bytes reported by executor.
    pub fn memory_used_bytes(&self) -> Option<u64> {
        self.memory_used_bytes
    }

    /// Memory limit bytes reported by executor.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        self.memory_limit_bytes
    }

    /// Active task count reported by executor.
    pub fn active_task_count(&self) -> Option<u32> {
        self.active_task_count
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
    lease_generation: LeaseGeneration,
    state: TaskState,
    attempt: u32,
    message: Option<String>,
    output_metadata: Option<TaskOutputMetadata>,
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
            lease_generation: LeaseGeneration::initial(),
            state,
            attempt,
            message: None,
            output_metadata: None,
        }
    }

    /// Attach the executor lease generation used for this status update.
    #[must_use]
    pub fn with_lease_generation(mut self, lease_generation: LeaseGeneration) -> Self {
        self.lease_generation = lease_generation;
        self
    }

    /// Attach a human-readable status message.
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Attach lightweight task output metadata.
    #[must_use]
    pub fn with_output_metadata(mut self, output_metadata: TaskOutputMetadata) -> Self {
        self.output_metadata = Some(output_metadata);
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

    /// Executor lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
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

    /// Optional lightweight task output metadata.
    pub fn output_metadata(&self) -> Option<&TaskOutputMetadata> {
        self.output_metadata.as_ref()
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
    memory_used_bytes: Option<u64>,
    memory_limit_bytes: Option<u64>,
    active_task_count: Option<u32>,
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
            memory_used_bytes: None,
            memory_limit_bytes: None,
            active_task_count: None,
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

    /// Attach memory used bytes.
    #[must_use]
    pub fn with_memory_used_bytes(mut self, bytes: u64) -> Self {
        self.memory_used_bytes = Some(bytes);
        self
    }

    /// Attach memory limit bytes.
    #[must_use]
    pub fn with_memory_limit_bytes(mut self, bytes: u64) -> Self {
        self.memory_limit_bytes = Some(bytes);
        self
    }

    /// Attach active task count.
    #[must_use]
    pub fn with_active_task_count(mut self, count: u32) -> Self {
        self.active_task_count = Some(count);
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

    /// Memory used bytes.
    pub fn memory_used_bytes(&self) -> Option<u64> {
        self.memory_used_bytes
    }

    /// Memory limit bytes.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        self.memory_limit_bytes
    }

    /// Active task count.
    pub fn active_task_count(&self) -> Option<u32> {
        self.active_task_count
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

    /// Override the transport version when mapping from a wire assignment.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
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
    output_metadata: Option<TaskOutputMetadata>,
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
            output_metadata: None,
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

    /// Attach lightweight output metadata for successful task completion.
    #[must_use]
    pub fn with_output_metadata(mut self, output_metadata: TaskOutputMetadata) -> Self {
        self.output_metadata = Some(output_metadata);
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

    /// Optional lightweight output metadata.
    pub fn output_metadata(&self) -> Option<&TaskOutputMetadata> {
        self.output_metadata.as_ref()
    }
}

/// Versioned task cancellation request sent to an executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCancellationRequest {
    version: TransportVersion,
    job_id: JobId,
    stage_id: StageId,
    task_id: TaskId,
    attempt_id: AttemptId,
    reason: Option<String>,
}

impl TaskCancellationRequest {
    /// Create a task cancellation request.
    pub fn new(ids: TaskAttemptRef) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            job_id: ids.job_id,
            stage_id: ids.stage_id,
            task_id: ids.task_id,
            attempt_id: ids.attempt_id,
            reason: None,
        }
    }

    /// Override transport version.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach cancellation reason.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
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

    /// Optional reason.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
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

    /// Deregister an executor from the active coordinator.
    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status>;

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

    /// Cancel work on an executor.
    async fn cancel_task(
        &self,
        request: tonic::Request<TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;
}

/// Generated protobuf contracts and conversions for the network transport.
pub mod wire {
    use std::error::Error;
    use std::fmt;

    use super::{
        AttemptId, DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
        ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
        ExecutorTaskAssignment, InputPartition, JobId, LeaseGeneration, OutputContract,
        OutputContractKind, PlanFragment, RegisterExecutorRequest, RegisterExecutorResponse,
        StageId, TaskAttemptRef, TaskCancellationRequest, TaskId, TaskOutputMetadata, TaskState,
        TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion,
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

    /// Convert a domain deregistration request to protobuf.
    pub fn deregister_executor_request_to_wire(
        value: DeregisterExecutorRequest,
    ) -> v1::DeregisterExecutorRequest {
        v1::DeregisterExecutorRequest {
            version: Some(transport_version_to_wire(value.version())),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            reason: value.reason().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf deregistration request to the domain contract.
    pub fn deregister_executor_request_from_wire(
        value: v1::DeregisterExecutorRequest,
    ) -> WireResult<DeregisterExecutorRequest> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let mut request =
            DeregisterExecutorRequest::new(executor_id, lease_generation).with_version(version);
        if !value.reason.is_empty() {
            request = request.with_reason(value.reason);
        }
        Ok(request)
    }

    /// Convert a domain deregistration response to protobuf.
    pub fn deregister_executor_response_to_wire(
        value: DeregisterExecutorResponse,
    ) -> v1::DeregisterExecutorResponse {
        v1::DeregisterExecutorResponse {
            version: Some(transport_version_to_wire(value.version())),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            disposition: transport_disposition_to_wire(value.disposition()) as i32,
            message: value.message().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf deregistration response to the domain contract.
    pub fn deregister_executor_response_from_wire(
        value: v1::DeregisterExecutorResponse,
    ) -> WireResult<DeregisterExecutorResponse> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let executor_id = ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?;
        let lease_generation =
            LeaseGeneration::try_new(value.lease_generation).map_err(WireError::from_id)?;
        let disposition = transport_disposition_from_wire(value.disposition)?;
        let mut response =
            DeregisterExecutorResponse::new(executor_id, lease_generation, disposition)
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

    /// Convert a domain executor task assignment to protobuf.
    pub fn executor_task_assignment_to_wire(
        value: ExecutorTaskAssignment,
    ) -> v1::ExecutorTaskAssignment {
        v1::ExecutorTaskAssignment {
            version: Some(transport_version_to_wire(value.version())),
            job_id: value.job_id().as_str().to_owned(),
            stage_id: value.stage_id().as_str().to_owned(),
            task_id: value.task_id().as_str().to_owned(),
            attempt_id: value.attempt_id().as_u32(),
            executor_id: value.executor_id().as_str().to_owned(),
            lease_generation: value.lease_generation().as_u64(),
            input_partitions: value
                .input_partitions()
                .iter()
                .map(input_partition_to_wire)
                .collect(),
            plan_fragment: Some(plan_fragment_to_wire(value.plan_fragment())),
            output_contract: Some(output_contract_to_wire(value.output_contract())),
        }
    }

    /// Convert a protobuf executor task assignment to the domain contract.
    pub fn executor_task_assignment_from_wire(
        value: v1::ExecutorTaskAssignment,
    ) -> WireResult<ExecutorTaskAssignment> {
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
        let input_partitions = value
            .input_partitions
            .into_iter()
            .map(input_partition_from_wire)
            .collect::<WireResult<Vec<_>>>()?;
        let plan_fragment =
            plan_fragment_from_wire(required(value.plan_fragment, "plan_fragment")?)?;
        let output_contract =
            output_contract_from_wire(required(value.output_contract, "output_contract")?)?;

        Ok(ExecutorTaskAssignment::new(
            ids,
            executor_id,
            lease_generation,
            plan_fragment,
            output_contract,
        )
        .with_version(version)
        .with_input_partitions(input_partitions))
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
            output_metadata: value.output_metadata().map(task_output_metadata_to_wire),
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
        if let Some(output_metadata) = value.output_metadata {
            request =
                request.with_output_metadata(task_output_metadata_from_wire(output_metadata)?);
        }
        Ok(request)
    }

    /// Convert a domain task cancellation request to protobuf.
    pub fn task_cancellation_request_to_wire(
        value: TaskCancellationRequest,
    ) -> v1::TaskCancellationRequest {
        v1::TaskCancellationRequest {
            version: Some(transport_version_to_wire(value.version())),
            job_id: value.job_id().as_str().to_owned(),
            stage_id: value.stage_id().as_str().to_owned(),
            task_id: value.task_id().as_str().to_owned(),
            attempt_id: value.attempt_id().as_u32(),
            reason: value.reason().unwrap_or_default().to_owned(),
        }
    }

    /// Convert a protobuf task cancellation request to the domain contract.
    pub fn task_cancellation_request_from_wire(
        value: v1::TaskCancellationRequest,
    ) -> WireResult<TaskCancellationRequest> {
        let version = transport_version_from_wire(required(value.version, "version")?)?;
        let ids = TaskAttemptRef::new(
            JobId::try_new(value.job_id).map_err(WireError::from_id)?,
            StageId::try_new(value.stage_id).map_err(WireError::from_id)?,
            TaskId::try_new(value.task_id).map_err(WireError::from_id)?,
            AttemptId::try_new(value.attempt_id).map_err(WireError::from_id)?,
        );
        let mut request = TaskCancellationRequest::new(ids).with_version(version);
        if !value.reason.is_empty() {
            request = request.with_reason(value.reason);
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

    fn task_output_metadata_to_wire(value: &TaskOutputMetadata) -> v1::TaskOutputMetadata {
        v1::TaskOutputMetadata {
            output_kind: value.output_kind().to_owned(),
            row_count: value.row_count(),
            batch_count: value.batch_count(),
            column_count: value.column_count(),
        }
    }

    fn task_output_metadata_from_wire(
        value: v1::TaskOutputMetadata,
    ) -> WireResult<TaskOutputMetadata> {
        if value.output_kind.trim().is_empty() {
            return Err(WireError::new("task output metadata kind cannot be empty"));
        }
        Ok(TaskOutputMetadata::new(
            value.output_kind,
            value.row_count,
            value.batch_count,
            value.column_count,
        ))
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
            task_endpoint: value.task_endpoint().unwrap_or_default().to_owned(),
        }
    }

    fn executor_descriptor_from_wire(
        value: v1::ExecutorDescriptor,
    ) -> WireResult<ExecutorDescriptor> {
        let slots = value
            .slots
            .try_into()
            .map_err(|_| WireError::new("executor slots value is too large"))?;
        let mut descriptor = ExecutorDescriptor::new(
            ExecutorId::try_new(value.executor_id).map_err(WireError::from_id)?,
            value.host,
            slots,
        );
        if !value.task_endpoint.is_empty() {
            descriptor = descriptor.with_task_endpoint(value.task_endpoint);
        }
        Ok(descriptor)
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

    fn input_partition_to_wire(value: &InputPartition) -> v1::InputPartition {
        v1::InputPartition {
            partition_id: value.partition_id().to_owned(),
            description: value.description().to_owned(),
        }
    }

    fn input_partition_from_wire(value: v1::InputPartition) -> WireResult<InputPartition> {
        if value.partition_id.trim().is_empty() {
            return Err(WireError::new("input partition id cannot be empty"));
        }
        Ok(InputPartition::new(value.partition_id, value.description))
    }

    fn plan_fragment_to_wire(value: &PlanFragment) -> v1::PlanFragment {
        v1::PlanFragment {
            description: value.description().to_owned(),
        }
    }

    fn plan_fragment_from_wire(value: v1::PlanFragment) -> WireResult<PlanFragment> {
        if value.description.trim().is_empty() {
            return Err(WireError::new("plan fragment description cannot be empty"));
        }
        Ok(PlanFragment::new(value.description))
    }

    fn output_contract_to_wire(value: &OutputContract) -> v1::OutputContract {
        v1::OutputContract {
            kind: output_contract_kind_to_wire(value.kind()) as i32,
            description: value.description().to_owned(),
        }
    }

    fn output_contract_from_wire(value: v1::OutputContract) -> WireResult<OutputContract> {
        if value.description.trim().is_empty() {
            return Err(WireError::new(
                "output contract description cannot be empty",
            ));
        }
        Ok(OutputContract::new(
            output_contract_kind_from_wire(value.kind)?,
            value.description,
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

    fn output_contract_kind_to_wire(value: OutputContractKind) -> v1::OutputContractKind {
        match value {
            OutputContractKind::InlineRecordBatches => v1::OutputContractKind::InlineRecordBatches,
            OutputContractKind::LocalFile => v1::OutputContractKind::LocalFile,
            OutputContractKind::Shuffle => v1::OutputContractKind::Shuffle,
            OutputContractKind::Sink => v1::OutputContractKind::Sink,
        }
    }

    fn output_contract_kind_from_wire(value: i32) -> WireResult<OutputContractKind> {
        match v1::OutputContractKind::try_from(value)
            .map_err(|_| WireError::new(format!("unknown output contract kind value {value}")))?
        {
            v1::OutputContractKind::Unspecified => {
                Err(WireError::new("output contract kind cannot be unspecified"))
            }
            v1::OutputContractKind::InlineRecordBatches => {
                Ok(OutputContractKind::InlineRecordBatches)
            }
            v1::OutputContractKind::LocalFile => Ok(OutputContractKind::LocalFile),
            v1::OutputContractKind::Shuffle => Ok(OutputContractKind::Shuffle),
            v1::OutputContractKind::Sink => Ok(OutputContractKind::Sink),
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
        AttemptId, DeregisterExecutorRequest, ExecutorDescriptor, ExecutorHeartbeatRequest,
        ExecutorId, ExecutorState, ExecutorTaskAssignment, InputPartition, JobId, JobKind, JobSpec,
        JobState, LeaseGeneration, OutputContract, OutputContractKind, PlanFragment,
        RegisterExecutorRequest, StageId, StageSpec, TaskAttemptRef, TaskCancellationRequest,
        TaskId, TaskOutputMetadata, TaskSpec, TaskState, TaskStatusRequest, TaskStatusResponse,
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
    fn executor_task_assignment_round_trips_through_wire_contract() {
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

        let wire = super::wire::executor_task_assignment_to_wire(assignment.clone());
        let round_trip = super::wire::executor_task_assignment_from_wire(wire).unwrap();

        assert_eq!(round_trip, assignment);
    }

    #[test]
    fn registration_descriptor_round_trips_task_endpoint() {
        let descriptor =
            ExecutorDescriptor::new(ExecutorId::try_new("exec-1").unwrap(), "pod-a", 2)
                .with_task_endpoint("http://127.0.0.1:9091");
        let request = RegisterExecutorRequest::new(descriptor.clone());

        let wire = super::wire::register_executor_request_to_wire(request);
        let round_trip = super::wire::register_executor_request_from_wire(wire).unwrap();

        assert_eq!(round_trip.descriptor(), &descriptor);
        assert_eq!(
            round_trip.descriptor().task_endpoint(),
            Some("http://127.0.0.1:9091")
        );
    }

    #[test]
    fn deregistration_round_trips_through_wire_contract() {
        let request = DeregisterExecutorRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::try_new(7).unwrap(),
        )
        .with_reason("shutdown");

        let wire = super::wire::deregister_executor_request_to_wire(request.clone());
        let round_trip = super::wire::deregister_executor_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
    }

    #[test]
    fn task_status_output_metadata_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let request = TaskStatusRequest::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )
        .with_output_metadata(TaskOutputMetadata::new("sql", 2, 1, 2));

        let wire = super::wire::task_status_request_to_wire(request.clone());
        let round_trip = super::wire::task_status_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
        assert_eq!(round_trip.output_metadata().unwrap().row_count(), 2);
    }

    #[test]
    fn task_cancellation_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let request = TaskCancellationRequest::new(ids).with_reason("user requested cancel");

        let wire = super::wire::task_cancellation_request_to_wire(request.clone());
        let round_trip = super::wire::task_cancellation_request_from_wire(wire).unwrap();

        assert_eq!(round_trip, request);
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
