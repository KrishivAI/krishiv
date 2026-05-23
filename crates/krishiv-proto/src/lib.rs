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

/// Monotonic fencing token for checkpoint epoch ownership.
///
/// The checkpoint store rejects metadata writes where the token is older than
/// the last committed writer's token, preventing stale coordinators from
/// committing superseded epochs (see `docs/architecture/checkpoint-protocol.md`
/// §Fencing Invariant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FencingToken(u64);

impl FencingToken {
    pub fn try_new(value: u64) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("fencing token"));
        }
        Ok(Self(value))
    }
    pub fn initial() -> Self {
        Self(1)
    }
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FencingToken {
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
    /// Checkpoint interval in milliseconds. `None` means checkpointing is disabled.
    checkpoint_interval_ms: Option<u64>,
    /// Storage path for checkpoint data. `None` means checkpointing is disabled.
    checkpoint_storage_path: Option<String>,
    /// Scheduling priority. 0 = lowest, 255 = highest. Default: 128 (normal).
    priority: u8,
    /// Namespace for quota and isolation grouping. `None` = default namespace.
    namespace_id: Option<String>,
    /// CPU time reservation in nanoseconds for admission control.
    cpu_limit_nanos: Option<u64>,
    /// Memory reservation in bytes for admission control.
    memory_limit_bytes: Option<u64>,
}

impl JobSpec {
    /// Create a job spec.
    pub fn new(job_id: JobId, name: impl Into<String>, kind: JobKind) -> Self {
        Self {
            job_id,
            name: name.into(),
            kind,
            stages: Vec::new(),
            checkpoint_interval_ms: None,
            checkpoint_storage_path: None,
            priority: 128,
            namespace_id: None,
            cpu_limit_nanos: None,
            memory_limit_bytes: None,
        }
    }

    /// Attach a stage.
    #[must_use]
    pub fn with_stage(mut self, stage: StageSpec) -> Self {
        self.stages.push(stage);
        self
    }

    /// Enable checkpointing with an interval and storage path.
    #[must_use]
    pub fn with_checkpoint(mut self, interval_ms: u64, storage_path: impl Into<String>) -> Self {
        self.checkpoint_interval_ms = Some(interval_ms);
        self.checkpoint_storage_path = Some(storage_path.into());
        self
    }

    /// Set the scheduling priority (0 = lowest, 255 = highest; default 128).
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Assign this job to a resource governance namespace.
    #[must_use]
    pub fn with_namespace(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = Some(namespace_id.into());
        self
    }

    /// Reserve CPU time (nanoseconds) for admission control.
    #[must_use]
    pub fn with_cpu_limit_nanos(mut self, nanos: u64) -> Self {
        self.cpu_limit_nanos = Some(nanos);
        self
    }

    /// Reserve memory (bytes) for admission control.
    #[must_use]
    pub fn with_memory_limit_bytes(mut self, bytes: u64) -> Self {
        self.memory_limit_bytes = Some(bytes);
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

    /// Checkpoint interval in milliseconds, if checkpointing is enabled.
    pub fn checkpoint_interval_ms(&self) -> Option<u64> {
        self.checkpoint_interval_ms
    }

    /// Storage path for checkpoint data, if checkpointing is enabled.
    pub fn checkpoint_storage_path(&self) -> Option<&str> {
        self.checkpoint_storage_path.as_deref()
    }

    /// Scheduling priority (0 = lowest, 255 = highest; default 128).
    pub fn priority(&self) -> u8 {
        self.priority
    }

    /// Namespace for quota grouping, if set.
    pub fn namespace_id(&self) -> Option<&str> {
        self.namespace_id.as_deref()
    }

    /// CPU time reservation in nanoseconds, if set.
    pub fn cpu_limit_nanos(&self) -> Option<u64> {
        self.cpu_limit_nanos
    }

    /// Memory reservation in bytes, if set.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        self.memory_limit_bytes
    }
}

// ── Checkpoint control-plane messages ─────────────────────────────────────────

/// One source partition offset captured at the barrier boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSourceOffset {
    pub partition_id: String,
    pub offset: i64,
}

/// Coordinator → Executor: begin checkpoint epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateCheckpointRequest {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
}

/// Executor → Coordinator: operator snapshot complete for epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAckRequest {
    pub job_id: JobId,
    pub operator_id: String,
    pub task_id: TaskId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    /// One per source partition this task owns.
    pub source_offsets: Vec<CheckpointSourceOffset>,
    /// None if operator has no state.
    pub snapshot_path: Option<String>,
}

/// Coordinator → Executor: abort the in-progress checkpoint epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortCheckpointRequest {
    pub job_id: JobId,
    pub epoch: u64,
}

/// Response to `InitiateCheckpointRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointInitiateResponse {
    Accepted,
    StaleEpoch { current_epoch: u64 },
    JobNotFound,
}

/// Response to `CheckpointAckRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckResponse {
    Accepted,
    StaleEpoch { current_epoch: u64 },
    JobNotFound,
}

/// Stage contract inside a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSpec {
    stage_id: StageId,
    name: String,
    tasks: Vec<TaskSpec>,
    /// Stage ids that must be fully Succeeded before this stage may launch.
    /// Empty means this stage has no upstream shuffle dependencies.
    upstream_stage_ids: Vec<StageId>,
    /// Number of shuffle output partitions this stage produces, if known.
    /// Coordinator uses this to pre-register Pending partition slots.
    output_partition_count: Option<u32>,
}

impl StageSpec {
    /// Create an empty stage spec.
    pub fn new(stage_id: StageId, name: impl Into<String>) -> Self {
        Self {
            stage_id,
            name: name.into(),
            tasks: Vec::new(),
            upstream_stage_ids: Vec::new(),
            output_partition_count: None,
        }
    }

    /// Attach a task.
    #[must_use]
    pub fn with_task(mut self, task: TaskSpec) -> Self {
        self.tasks.push(task);
        self
    }

    /// Declare that this stage depends on `upstream_stage_id` having all shuffle
    /// partitions Available before any task in this stage may be launched.
    #[must_use]
    pub fn with_upstream_stage(mut self, upstream_stage_id: StageId) -> Self {
        self.upstream_stage_ids.push(upstream_stage_id);
        self
    }

    /// Set the expected shuffle output partition count for this stage.
    #[must_use]
    pub fn with_output_partition_count(mut self, count: u32) -> Self {
        self.output_partition_count = Some(count);
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

    /// Stage ids that must be fully Succeeded before this stage may launch.
    pub fn upstream_stage_ids(&self) -> &[StageId] {
        &self.upstream_stage_ids
    }

    /// Expected shuffle output partition count, if declared at submission time.
    pub fn output_partition_count(&self) -> Option<u32> {
        self.output_partition_count
    }
}

/// Connector capability flags surfaced in task metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorCapabilityFlags {
    pub bounded: bool,
    pub unbounded: bool,
    pub rewindable: bool,
    pub transactional: bool,
    pub idempotent: bool,
}

// ── R4a Shuffle configs ────────────────────────────────────────────────────────

/// Configuration for a task that writes its output to the shuffle store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleWriteConfig {
    /// Stage whose output is being written.
    pub stage_id: StageId,
    /// Total number of output partitions.
    pub num_partitions: usize,
    /// Column names used as hash partitioning keys. Empty = round-robin.
    pub key_columns: Vec<String>,
    /// Lease token for fencing.
    pub lease_token: u64,
}

/// Configuration for a task that reads its input from the shuffle store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleReadConfig {
    /// Stage whose shuffle output to read from.
    pub stage_id: StageId,
    /// Partition index this task should read.
    pub partition_id: usize,
    /// Lease token for fencing.
    pub lease_token: u64,
}

/// Task contract inside a stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    task_id: TaskId,
    description: String,
    task_timeout_secs: Option<u64>,
    /// Capability flags declared by the source connector for this task, if known.
    pub source_capabilities: Option<ConnectorCapabilityFlags>,
    /// Capability flags declared by the sink connector for this task, if known.
    pub sink_capabilities: Option<ConnectorCapabilityFlags>,
    shuffle_write: Option<ShuffleWriteConfig>,
    shuffle_read: Option<ShuffleReadConfig>,
}

impl TaskSpec {
    /// Create a task spec.
    pub fn new(task_id: TaskId, description: impl Into<String>) -> Self {
        Self {
            task_id,
            description: description.into(),
            task_timeout_secs: None,
            source_capabilities: None,
            sink_capabilities: None,
            shuffle_write: None,
            shuffle_read: None,
        }
    }

    /// Attach a per-task execution timeout.
    #[must_use]
    pub fn with_task_timeout_secs(mut self, secs: u64) -> Self {
        self.task_timeout_secs = Some(secs);
        self
    }

    /// Attach source connector capability flags.
    #[must_use]
    pub fn with_source_capabilities(mut self, caps: ConnectorCapabilityFlags) -> Self {
        self.source_capabilities = Some(caps);
        self
    }

    /// Attach sink connector capability flags.
    #[must_use]
    pub fn with_sink_capabilities(mut self, caps: ConnectorCapabilityFlags) -> Self {
        self.sink_capabilities = Some(caps);
        self
    }

    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Human-readable task description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Per-task execution timeout in seconds, if set.
    pub fn task_timeout_secs(&self) -> Option<u64> {
        self.task_timeout_secs
    }

    /// Attach a shuffle write configuration.
    #[must_use]
    pub fn with_shuffle_write(mut self, config: ShuffleWriteConfig) -> Self {
        self.shuffle_write = Some(config);
        self
    }

    /// Attach a shuffle read configuration.
    #[must_use]
    pub fn with_shuffle_read(mut self, config: ShuffleReadConfig) -> Self {
        self.shuffle_read = Some(config);
        self
    }

    /// Shuffle write configuration, if this task writes to the shuffle store.
    pub fn shuffle_write(&self) -> Option<&ShuffleWriteConfig> {
        self.shuffle_write.as_ref()
    }

    /// Shuffle read configuration, if this task reads from the shuffle store.
    pub fn shuffle_read(&self) -> Option<&ShuffleReadConfig> {
        self.shuffle_read.as_ref()
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

/// Metadata for a single shuffle partition produced by a task.
///
/// The coordinator uses this to transition the partition from Pending → Available
/// in its `ShuffleMetadata` store and gate Stage N+1 launch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShufflePartitionOutput {
    /// Partition index within the stage.
    pub partition_id: u32,
    /// Bytes written to the shuffle store.
    pub size_bytes: u64,
    /// Arrow Flight endpoint the executor is serving for this partition.
    /// Format: `http://<host>:<port>`. Empty string means in-process (single-node mode).
    pub flight_endpoint: String,
}

impl ShufflePartitionOutput {
    /// Create a shuffle partition output descriptor.
    pub fn new(partition_id: u32, size_bytes: u64, flight_endpoint: impl Into<String>) -> Self {
        Self {
            partition_id,
            size_bytes,
            flight_endpoint: flight_endpoint.into(),
        }
    }

    /// In-process (single-node) partition with no Flight endpoint.
    pub fn inline(partition_id: u32, size_bytes: u64) -> Self {
        Self::new(partition_id, size_bytes, "")
    }
}

/// Runtime statistics collected by a task during execution.
///
/// Fed into AQE rules and stored in `TaskRecord` for job-level aggregation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskRuntimeStats {
    /// Rows read from all input sources.
    pub input_rows: u64,
    /// Rows written to output / shuffle.
    pub output_rows: u64,
    /// CPU time in nanoseconds.
    pub cpu_nanos: u64,
    /// Peak memory used in bytes.
    pub memory_bytes: u64,
    /// Bytes spilled to local disk.
    pub spill_bytes: u64,
}

/// Task output metadata reported when a task completes successfully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputMetadata {
    output_kind: String,
    row_count: u64,
    batch_count: u64,
    column_count: u64,
    /// Shuffle partitions produced by this task, if this was a shuffle-write task.
    shuffle_partitions: Vec<ShufflePartitionOutput>,
    /// Runtime statistics for this task execution.
    runtime_stats: Option<TaskRuntimeStats>,
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
            shuffle_partitions: Vec::new(),
            runtime_stats: None,
        }
    }

    /// Attach shuffle partition outputs (for shuffle-write tasks).
    #[must_use]
    pub fn with_shuffle_partitions(mut self, partitions: Vec<ShufflePartitionOutput>) -> Self {
        self.shuffle_partitions = partitions;
        self
    }

    /// Attach runtime statistics.
    #[must_use]
    pub fn with_runtime_stats(mut self, stats: TaskRuntimeStats) -> Self {
        self.runtime_stats = Some(stats);
        self
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

    /// Shuffle partitions produced by this task.
    pub fn shuffle_partitions(&self) -> &[ShufflePartitionOutput] {
        &self.shuffle_partitions
    }

    /// Runtime statistics for this task.
    pub fn runtime_stats(&self) -> Option<&TaskRuntimeStats> {
        self.runtime_stats.as_ref()
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

// ── R7.2 hot-key / throttle contract types ────────────────────────────────────

/// A key frequency estimate reported by an executor in its heartbeat.
///
/// Generated by the executor's `HeavyHittersTracker` (SpaceSaving algorithm).
/// The coordinator uses these to decide whether to split a hot key or throttle
/// an upstream source.
#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatHotKeyReport {
    /// The key value as a string representation.
    pub key: String,
    /// Estimated occurrence count (may be an overestimate due to SpaceSaving).
    pub estimated_count: u64,
    /// Maximum possible error in the count estimate.
    pub max_error: u64,
    /// Heat score: `estimated_count / total_items_seen` (0.0–1.0).
    pub heat_score: f64,
    /// Job id this report belongs to.
    pub job_id: String,
    /// Source or operator id that produced this report.
    pub source_id: String,
}

/// A throttle command sent from the coordinator to an executor in the
/// heartbeat response.  The executor forwards this to the matching source.
#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatThrottleCommand {
    /// Source operator id on the executor that should be throttled.
    pub source_id: String,
    /// Maximum rows per second (`None` clears the throttle / unlimited).
    pub rows_per_second: Option<u64>,
}

// ── Distributed tracing carrier ──────────────────────────────────────────────

/// W3C Trace Context carrier for distributed tracing (R8 wiring).
///
/// Populated by callers that have an active OpenTelemetry span. Receivers
/// forward this into their active span as a remote parent. When absent,
/// no tracing context is propagated.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceContext {
    /// W3C `traceparent` header value (e.g. "00-<trace-id>-<span-id>-01").
    pub traceparent: String,
    /// W3C `tracestate` header value (vendor-specific key=value pairs).
    pub tracestate: String,
}

impl TraceContext {
    /// Create a trace context with the given traceparent.
    pub fn new(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: traceparent.into(),
            tracestate: String::new(),
        }
    }

    /// Whether this context carries a real trace (non-empty traceparent).
    pub fn is_active(&self) -> bool {
        !self.traceparent.is_empty()
    }
}

// ── Per-task streaming state ──────────────────────────────────────────────────

/// Per-task streaming state reported by an executor during heartbeat.
///
/// Used by the streaming re-attach protocol: when a coordinator restarts while
/// streaming tasks are running, executors include this in their first heartbeat
/// so the coordinator can resume the job at the right watermark and source offset
/// instead of re-running from scratch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingTaskState {
    /// Task this state belongs to.
    pub task_id: TaskId,
    /// Current event-time watermark in milliseconds since epoch.
    pub watermark_ms: u64,
    /// Last committed source offset for this task's input partition.
    /// Encoded as a byte string whose interpretation is connector-specific.
    pub source_offset: Vec<u8>,
}

impl StreamingTaskState {
    /// Create a streaming task state report.
    pub fn new(task_id: TaskId, watermark_ms: u64, source_offset: Vec<u8>) -> Self {
        Self {
            task_id,
            watermark_ms,
            source_offset,
        }
    }
}

/// Executor heartbeat contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHeartbeat {
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    state: ExecutorState,
    running_tasks: Vec<TaskId>,
    memory_used_bytes: Option<u64>,
    memory_limit_bytes: Option<u64>,
    active_task_count: Option<u32>,
    /// Per-task streaming state for the re-attach protocol.
    /// Empty for batch tasks and executors that have no streaming tasks.
    streaming_task_states: Vec<StreamingTaskState>,
    /// Hot-key reports from the executor's SpaceSaving tracker (R7.2).
    hot_key_reports: Vec<HeartbeatHotKeyReport>,
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
            streaming_task_states: Vec::new(),
            hot_key_reports: Vec::new(),
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

    /// Attach streaming task states for the re-attach protocol.
    #[must_use]
    pub fn with_streaming_task_states(mut self, states: Vec<StreamingTaskState>) -> Self {
        self.streaming_task_states = states;
        self
    }

    /// Per-task streaming state reported by this executor.
    pub fn streaming_task_states(&self) -> &[StreamingTaskState] {
        &self.streaming_task_states
    }

    /// Attach hot-key reports from the SpaceSaving tracker (R7.2).
    #[must_use]
    pub fn with_hot_key_reports(mut self, reports: Vec<HeartbeatHotKeyReport>) -> Self {
        self.hot_key_reports = reports;
        self
    }

    /// Hot-key reports in this heartbeat.
    pub fn hot_key_reports(&self) -> &[HeartbeatHotKeyReport] {
        &self.hot_key_reports
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
    trace_context: Option<TraceContext>,
}

impl RegisterExecutorRequest {
    /// Create a registration request using the current transport version.
    pub fn new(descriptor: ExecutorDescriptor) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            descriptor,
            trace_context: None,
        }
    }

    /// Create a registration request with an explicit transport version.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
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

    /// W3C trace context, if provided by the caller.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
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
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHeartbeatRequest {
    version: TransportVersion,
    executor_id: ExecutorId,
    lease_generation: LeaseGeneration,
    state: ExecutorState,
    running_attempts: Vec<TaskAttemptRef>,
    memory_used_bytes: Option<u64>,
    memory_limit_bytes: Option<u64>,
    active_task_count: Option<u32>,
    /// CPU cores currently in use by this executor (P0.17).
    cpu_cores_used: Option<f64>,
    /// Network bytes sent by this executor since the last heartbeat (P0.17).
    network_bytes_sent: Option<u64>,
    /// Network bytes received by this executor since the last heartbeat (P0.17).
    network_bytes_recv: Option<u64>,
    /// Per-task streaming state for the re-attach protocol.
    /// Populated on the first heartbeat after a coordinator restart by executors
    /// that have running streaming tasks, so the coordinator can resume tracking
    /// watermark and offset without re-running the job from scratch.
    streaming_task_states: Vec<StreamingTaskState>,
    /// Hot-key reports from the executor's SpaceSaving tracker.
    /// Populated by streaming executors whenever the tracker has entries whose
    /// heat score exceeds the reporting threshold (default 5%).
    hot_key_reports: Vec<HeartbeatHotKeyReport>,
    /// W3C trace context for distributed tracing (R8 wiring).
    trace_context: Option<TraceContext>,
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
            cpu_cores_used: None,
            network_bytes_sent: None,
            network_bytes_recv: None,
            streaming_task_states: Vec::new(),
            hot_key_reports: Vec::new(),
            trace_context: None,
        }
    }

    /// Override the transport version when mapping from a wire request.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// W3C trace context, if provided by the caller.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
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

    /// Attach CPU cores in use (P0.17).
    #[must_use]
    pub fn with_cpu_cores_used(mut self, cores: f64) -> Self {
        self.cpu_cores_used = Some(cores);
        self
    }

    /// Attach network bytes sent since the last heartbeat (P0.17).
    #[must_use]
    pub fn with_network_bytes_sent(mut self, bytes: u64) -> Self {
        self.network_bytes_sent = Some(bytes);
        self
    }

    /// Attach network bytes received since the last heartbeat (P0.17).
    #[must_use]
    pub fn with_network_bytes_recv(mut self, bytes: u64) -> Self {
        self.network_bytes_recv = Some(bytes);
        self
    }

    /// Attach streaming task states for the re-attach protocol.
    #[must_use]
    pub fn with_streaming_task_states(mut self, states: Vec<StreamingTaskState>) -> Self {
        self.streaming_task_states = states;
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

    /// CPU cores in use on this executor (P0.17).
    pub fn cpu_cores_used(&self) -> Option<f64> {
        self.cpu_cores_used
    }

    /// Network bytes sent by this executor since the last heartbeat (P0.17).
    pub fn network_bytes_sent(&self) -> Option<u64> {
        self.network_bytes_sent
    }

    /// Network bytes received by this executor since the last heartbeat (P0.17).
    pub fn network_bytes_recv(&self) -> Option<u64> {
        self.network_bytes_recv
    }

    /// Per-task streaming state for the re-attach protocol.
    pub fn streaming_task_states(&self) -> &[StreamingTaskState] {
        &self.streaming_task_states
    }

    /// Attach hot-key reports from the executor's SpaceSaving tracker.
    #[must_use]
    pub fn with_hot_key_reports(mut self, reports: Vec<HeartbeatHotKeyReport>) -> Self {
        self.hot_key_reports = reports;
        self
    }

    /// Hot-key reports included in this heartbeat.
    pub fn hot_key_reports(&self) -> &[HeartbeatHotKeyReport] {
        &self.hot_key_reports
    }
}

/// Executor heartbeat response sent from coordinator to executor.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutorHeartbeatResponse {
    version: TransportVersion,
    lease_generation: LeaseGeneration,
    disposition: TransportDisposition,
    message: Option<String>,
    /// Throttle commands the executor must forward to its source operators.
    throttle_commands: Vec<HeartbeatThrottleCommand>,
    /// W3C trace context for distributed tracing (R8 wiring).
    trace_context: Option<TraceContext>,
}

impl ExecutorHeartbeatResponse {
    /// Create a heartbeat response using the current transport version.
    pub fn new(lease_generation: LeaseGeneration, disposition: TransportDisposition) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            lease_generation,
            disposition,
            message: None,
            throttle_commands: Vec::new(),
            trace_context: None,
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

    /// Attach throttle commands to be applied by the executor's sources.
    #[must_use]
    pub fn with_throttle_commands(mut self, cmds: Vec<HeartbeatThrottleCommand>) -> Self {
        self.throttle_commands = cmds;
        self
    }

    /// Throttle commands in this heartbeat response.
    pub fn throttle_commands(&self) -> &[HeartbeatThrottleCommand] {
        &self.throttle_commands
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

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// W3C trace context, if provided by the coordinator.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
    }
}

/// Typed connector/runtime input descriptor for one executor partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputPartitionDescriptor {
    /// Local Parquet file registered directly with DataFusion.
    LocalParquet { table_name: String, path: String },
    /// Parquet file read through the connector boundary.
    ConnectorParquet {
        table_name: Option<String>,
        path: String,
    },
    /// Object-store Parquet input using the R3 deterministic local-object backend.
    ObjectParquet {
        table_name: String,
        base_dir: String,
        object_path: String,
    },
    /// Deterministic Kafka-compatible in-memory records for connector certification.
    MemoryKafka {
        topic: String,
        partition: i32,
        start_offset: i64,
        records: Vec<MemoryKafkaRecord>,
    },
    /// Arrow Flight endpoint for reading an upstream shuffle partition.
    ///
    /// The executor connects to `flight_endpoint` and reads `partition_id`
    /// from the upstream stage. The data is registered as `table_name` in
    /// the local DataFusion context before executing the fragment SQL.
    ShuffleFlight {
        table_name: String,
        /// Arrow Flight endpoint of the upstream executor (e.g. `http://10.0.0.5:50051`).
        /// Empty string means in-process (single-node mode, read from InMemoryShuffleStore).
        flight_endpoint: String,
        /// Job id of the upstream stage.
        job_id: String,
        /// Stage id that produced this partition.
        upstream_stage_id: String,
        /// Partition index.
        partition_id: u32,
    },
}

impl InputPartitionDescriptor {
    /// Build a legacy-compatible human-readable descriptor string.
    pub fn legacy_description(&self) -> String {
        match self {
            Self::LocalParquet { table_name, path } => {
                format!("local-parquet:{table_name}:{path}")
            }
            Self::ConnectorParquet { path, .. } => format!("connector-parquet:{path}"),
            Self::ObjectParquet {
                table_name,
                base_dir,
                object_path,
            } => format!("object-parquet:{table_name}:{base_dir}:{object_path}"),
            Self::MemoryKafka {
                topic,
                partition,
                start_offset,
                records,
            } => {
                let records = records
                    .iter()
                    .map(|record| format!("{}={}", record.id, record.value))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("memory-kafka:{topic}:{partition}:{start_offset}:{records}")
            }
            Self::ShuffleFlight {
                table_name,
                flight_endpoint,
                upstream_stage_id,
                partition_id,
                ..
            } => {
                format!(
                    "shuffle-flight:{table_name}:{flight_endpoint}:{upstream_stage_id}:{partition_id}"
                )
            }
        }
    }
}

/// One deterministic in-memory Kafka record used by typed R3 test descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryKafkaRecord {
    /// Synthetic record id.
    pub id: i64,
    /// Synthetic record value.
    pub value: String,
}

impl MemoryKafkaRecord {
    /// Create a memory Kafka test record.
    pub fn new(id: i64, value: impl Into<String>) -> Self {
        Self {
            id,
            value: value.into(),
        }
    }
}

/// Descriptor for one input partition assigned to an executor task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputPartition {
    partition_id: String,
    description: String,
    descriptor: Option<InputPartitionDescriptor>,
}

impl InputPartition {
    /// Create a legacy string input partition descriptor.
    pub fn new(partition_id: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            partition_id: partition_id.into(),
            description: description.into(),
            descriptor: None,
        }
    }

    /// Create a typed input partition descriptor while retaining a legacy description.
    pub fn typed(partition_id: impl Into<String>, descriptor: InputPartitionDescriptor) -> Self {
        Self {
            partition_id: partition_id.into(),
            description: descriptor.legacy_description(),
            descriptor: Some(descriptor),
        }
    }

    /// Attach or replace the typed descriptor.
    #[must_use]
    pub fn with_descriptor(mut self, descriptor: InputPartitionDescriptor) -> Self {
        self.description = descriptor.legacy_description();
        self.descriptor = Some(descriptor);
        self
    }

    /// Partition id.
    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    /// Human-readable fallback partition description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Typed descriptor, when supplied by the scheduler/control plane.
    pub fn descriptor(&self) -> Option<&InputPartitionDescriptor> {
        self.descriptor.as_ref()
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

/// Typed output destination descriptor for executor task output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputContractDescriptor {
    /// Return bounded record batches via task metadata/control-plane test path.
    InlineRecordBatches,
    /// Write to a local file path.
    LocalFile { path: String },
    /// Write to a shuffle partition.
    Shuffle { partition: String },
    /// Write Parquet through the object-store connector boundary.
    ObjectParquetSink {
        base_dir: String,
        object_path: String,
    },
    /// Write Parquet to a local path through the connector sink.
    ParquetSink { path: String },
}

impl OutputContractDescriptor {
    /// Build a legacy-compatible human-readable output descriptor.
    pub fn legacy_description(&self) -> String {
        match self {
            Self::InlineRecordBatches => String::from("inline result"),
            Self::LocalFile { path } => path.clone(),
            Self::Shuffle { partition } => partition.clone(),
            Self::ObjectParquetSink {
                base_dir,
                object_path,
            } => format!("object-parquet-sink:{base_dir}:{object_path}"),
            Self::ParquetSink { path } => format!("parquet-sink:{path}"),
        }
    }
}

/// Output contract for an executor task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputContract {
    kind: OutputContractKind,
    description: String,
    descriptor: Option<OutputContractDescriptor>,
}

impl OutputContract {
    /// Create a legacy string output contract.
    pub fn new(kind: OutputContractKind, description: impl Into<String>) -> Self {
        Self {
            kind,
            description: description.into(),
            descriptor: None,
        }
    }

    /// Create a typed output contract while retaining a legacy description.
    pub fn typed(kind: OutputContractKind, descriptor: OutputContractDescriptor) -> Self {
        Self {
            kind,
            description: descriptor.legacy_description(),
            descriptor: Some(descriptor),
        }
    }

    /// Attach or replace the typed descriptor.
    #[must_use]
    pub fn with_descriptor(mut self, descriptor: OutputContractDescriptor) -> Self {
        self.description = descriptor.legacy_description();
        self.descriptor = Some(descriptor);
        self
    }

    /// Output kind.
    pub fn kind(&self) -> OutputContractKind {
        self.kind
    }

    /// Human-readable fallback output description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Typed output destination descriptor, when supplied.
    pub fn descriptor(&self) -> Option<&OutputContractDescriptor> {
        self.descriptor.as_ref()
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
    task_timeout_secs: Option<u64>,
    /// W3C trace context for distributed tracing (R8 wiring).
    trace_context: Option<TraceContext>,
    shuffle_write: Option<ShuffleWriteConfig>,
    shuffle_read: Option<ShuffleReadConfig>,
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
            task_timeout_secs: None,
            trace_context: None,
            shuffle_write: None,
            shuffle_read: None,
        }
    }

    /// Attach a per-task execution timeout.
    #[must_use]
    pub fn with_task_timeout_secs(mut self, secs: u64) -> Self {
        self.task_timeout_secs = Some(secs);
        self
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

    /// Per-task execution timeout in seconds, if set.
    pub fn task_timeout_secs(&self) -> Option<u64> {
        self.task_timeout_secs
    }

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// W3C trace context, if provided by the scheduler.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
    }

    /// Attach a shuffle write configuration.
    #[must_use]
    pub fn with_shuffle_write(mut self, config: ShuffleWriteConfig) -> Self {
        self.shuffle_write = Some(config);
        self
    }

    /// Attach a shuffle read configuration.
    #[must_use]
    pub fn with_shuffle_read(mut self, config: ShuffleReadConfig) -> Self {
        self.shuffle_read = Some(config);
        self
    }

    /// Shuffle write configuration, if this task writes to the shuffle store.
    pub fn shuffle_write(&self) -> Option<&ShuffleWriteConfig> {
        self.shuffle_write.as_ref()
    }

    /// Shuffle read configuration, if this task reads from the shuffle store.
    pub fn shuffle_read(&self) -> Option<&ShuffleReadConfig> {
        self.shuffle_read.as_ref()
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
    /// W3C trace context for distributed tracing (R8 wiring).
    trace_context: Option<TraceContext>,
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
            trace_context: None,
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

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// W3C trace context, if provided by the caller.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
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
    /// W3C trace context for distributed tracing (R8 wiring).
    trace_context: Option<TraceContext>,
}

impl TaskStatusResponse {
    /// Create a task status response using the current transport version.
    pub fn new(disposition: TransportDisposition) -> Self {
        Self {
            version: TransportVersion::CURRENT,
            disposition,
            message: None,
            trace_context: None,
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

    /// Attach a W3C trace context for distributed tracing (R8 wiring).
    #[must_use]
    pub fn with_trace_context(mut self, ctx: TraceContext) -> Self {
        self.trace_context = Some(ctx);
        self
    }

    /// W3C trace context, if provided by the coordinator.
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
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

    /// Route a checkpoint ack from an executor to the active coordinator (R6a).
    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status>;
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

/// Domain types for the coordinator management service (GAP-RT-04).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointRequest {
    pub job_id: String,
    /// Empty string means no label. Use `label_opt()` for `Option<String>`.
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointResponse {
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreJobRequest {
    pub job_id: String,
    pub epoch: u64,
    pub storage_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreJobResponse {
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCheckpointsRequest {
    pub job_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointEpochInfo {
    pub epoch: u64,
    pub is_savepoint: bool,
    pub savepoint_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCheckpointsResponse {
    pub epochs: Vec<CheckpointEpochInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectStateRequest {
    pub job_id: String,
    pub operator_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshotInfo {
    pub task_id: String,
    pub snapshot_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectStateResponse {
    pub snapshots: Vec<StateSnapshotInfo>,
}

/// Tonic-shaped coordinator management service for CLI→coordinator RPCs.
#[tonic::async_trait]
pub trait CoordinatorManagementService: Send + Sync + 'static {
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<TriggerSavepointRequest>,
    ) -> Result<tonic::Response<TriggerSavepointResponse>, tonic::Status>;

    async fn restore_job(
        &self,
        request: tonic::Request<RestoreJobRequest>,
    ) -> Result<tonic::Response<RestoreJobResponse>, tonic::Status>;

    async fn list_checkpoints(
        &self,
        request: tonic::Request<ListCheckpointsRequest>,
    ) -> Result<tonic::Response<ListCheckpointsResponse>, tonic::Status>;

    async fn inspect_state(
        &self,
        request: tonic::Request<InspectStateRequest>,
    ) -> Result<tonic::Response<InspectStateResponse>, tonic::Status>;
}

/// Generated protobuf contracts and conversions for the network transport.
pub mod wire {
    use std::error::Error;
    use std::fmt;

    use super::{
        AttemptId, CheckpointAckRequest, CheckpointAckResponse, CheckpointSourceOffset,
        DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorDescriptor,
        ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
        ExecutorTaskAssignment, FencingToken, InputPartition, InputPartitionDescriptor, JobId,
        LeaseGeneration, MemoryKafkaRecord, OutputContract, OutputContractDescriptor,
        OutputContractKind, PlanFragment, RegisterExecutorRequest, RegisterExecutorResponse,
        ShufflePartitionOutput, StageId, TaskAttemptRef, TaskCancellationRequest, TaskId,
        TaskOutputMetadata, TaskRuntimeStats, TaskState, TaskStatusRequest, TaskStatusResponse,
        TransportDisposition, TransportVersion,
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
    ///
    /// P0.17: Maps ALL task-resource fields so none are silently dropped.
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
            memory_used_bytes: value.memory_used_bytes().unwrap_or(0),
            memory_limit_bytes: value.memory_limit_bytes().unwrap_or(0),
            active_task_count: value.active_task_count().unwrap_or(0),
            cpu_cores_used: value.cpu_cores_used().unwrap_or(0.0),
            network_bytes_sent: value.network_bytes_sent().unwrap_or(0),
            network_bytes_recv: value.network_bytes_recv().unwrap_or(0),
        }
    }

    /// Convert a protobuf heartbeat request to the domain contract.
    ///
    /// P0.17: Restores ALL task-resource fields from the wire message.
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

        let mut req = ExecutorHeartbeatRequest::new(executor_id, lease_generation, state)
            .with_version(version)
            .with_running_attempts(running_attempts);

        if value.memory_used_bytes > 0 {
            req = req.with_memory_used_bytes(value.memory_used_bytes);
        }
        if value.memory_limit_bytes > 0 {
            req = req.with_memory_limit_bytes(value.memory_limit_bytes);
        }
        if value.active_task_count > 0 {
            req = req.with_active_task_count(value.active_task_count);
        }
        if value.cpu_cores_used > 0.0 {
            req = req.with_cpu_cores_used(value.cpu_cores_used);
        }
        if value.network_bytes_sent > 0 {
            req = req.with_network_bytes_sent(value.network_bytes_sent);
        }
        if value.network_bytes_recv > 0 {
            req = req.with_network_bytes_recv(value.network_bytes_recv);
        }

        Ok(req)
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
            task_timeout_secs: value.task_timeout_secs().unwrap_or(0),
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

        let mut assignment = ExecutorTaskAssignment::new(
            ids,
            executor_id,
            lease_generation,
            plan_fragment,
            output_contract,
        )
        .with_version(version)
        .with_input_partitions(input_partitions);
        if value.task_timeout_secs > 0 {
            assignment = assignment.with_task_timeout_secs(value.task_timeout_secs);
        }
        Ok(assignment)
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
            // Shuffle partition and runtime stats are carried in-process for R4;
            // proto encoding is deferred until the wire schema stabilises.
            shuffle_partition_ids: value
                .shuffle_partitions()
                .iter()
                .map(|p| p.partition_id)
                .collect(),
            shuffle_partition_bytes: value
                .shuffle_partitions()
                .iter()
                .map(|p| p.size_bytes)
                .collect(),
            shuffle_flight_endpoints: value
                .shuffle_partitions()
                .iter()
                .map(|p| p.flight_endpoint.clone())
                .collect(),
            input_rows: value.runtime_stats().map_or(0, |s| s.input_rows),
            output_rows: value.runtime_stats().map_or(0, |s| s.output_rows),
            cpu_nanos: value.runtime_stats().map_or(0, |s| s.cpu_nanos),
            spill_bytes: value.runtime_stats().map_or(0, |s| s.spill_bytes),
        }
    }

    fn task_output_metadata_from_wire(
        value: v1::TaskOutputMetadata,
    ) -> WireResult<TaskOutputMetadata> {
        if value.output_kind.trim().is_empty() {
            return Err(WireError::new("task output metadata kind cannot be empty"));
        }
        let shuffle_partitions: Vec<ShufflePartitionOutput> = value
            .shuffle_partition_ids
            .into_iter()
            .zip(value.shuffle_partition_bytes)
            .zip(value.shuffle_flight_endpoints)
            .map(|((id, bytes), endpoint)| ShufflePartitionOutput::new(id, bytes, endpoint))
            .collect();
        let mut meta = TaskOutputMetadata::new(
            value.output_kind,
            value.row_count,
            value.batch_count,
            value.column_count,
        );
        if !shuffle_partitions.is_empty() {
            meta = meta.with_shuffle_partitions(shuffle_partitions);
        }
        let has_stats = value.input_rows > 0
            || value.output_rows > 0
            || value.cpu_nanos > 0
            || value.spill_bytes > 0;
        if has_stats {
            meta = meta.with_runtime_stats(TaskRuntimeStats {
                input_rows: value.input_rows,
                output_rows: value.output_rows,
                cpu_nanos: value.cpu_nanos,
                memory_bytes: 0,
                spill_bytes: value.spill_bytes,
            });
        }
        Ok(meta)
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
            descriptor: value.descriptor().map(input_partition_descriptor_to_wire),
        }
    }

    fn input_partition_from_wire(value: v1::InputPartition) -> WireResult<InputPartition> {
        if value.partition_id.trim().is_empty() {
            return Err(WireError::new("input partition id cannot be empty"));
        }
        let partition = InputPartition::new(value.partition_id, value.description);
        match value.descriptor {
            Some(descriptor) => {
                Ok(partition.with_descriptor(input_partition_descriptor_from_wire(descriptor)?))
            }
            None => Ok(partition),
        }
    }

    fn input_partition_descriptor_to_wire(
        value: &InputPartitionDescriptor,
    ) -> v1::InputPartitionDescriptor {
        match value {
            InputPartitionDescriptor::LocalParquet { table_name, path } => {
                v1::InputPartitionDescriptor {
                    kind: v1::InputPartitionDescriptorKind::LocalParquet as i32,
                    table_name: table_name.clone(),
                    path: path.clone(),
                    ..Default::default()
                }
            }
            InputPartitionDescriptor::ConnectorParquet { table_name, path } => {
                v1::InputPartitionDescriptor {
                    kind: v1::InputPartitionDescriptorKind::ConnectorParquet as i32,
                    table_name: table_name.clone().unwrap_or_default(),
                    path: path.clone(),
                    ..Default::default()
                }
            }
            InputPartitionDescriptor::ObjectParquet {
                table_name,
                base_dir,
                object_path,
            } => v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::ObjectParquet as i32,
                table_name: table_name.clone(),
                object_base_dir: base_dir.clone(),
                object_path: object_path.clone(),
                ..Default::default()
            },
            InputPartitionDescriptor::MemoryKafka {
                topic,
                partition,
                start_offset,
                records,
            } => v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::MemoryKafka as i32,
                kafka_topic: topic.clone(),
                kafka_partition: *partition,
                kafka_start_offset: *start_offset,
                memory_kafka_records: records
                    .iter()
                    .map(|record| v1::MemoryKafkaRecord {
                        id: record.id,
                        value: record.value.clone(),
                    })
                    .collect(),
                ..Default::default()
            },
            InputPartitionDescriptor::ShuffleFlight {
                table_name,
                flight_endpoint,
                job_id,
                upstream_stage_id,
                partition_id,
            } => v1::InputPartitionDescriptor {
                kind: v1::InputPartitionDescriptorKind::ShuffleFlight as i32,
                table_name: table_name.clone(),
                shuffle_flight_endpoint: flight_endpoint.clone(),
                shuffle_job_id: job_id.clone(),
                shuffle_upstream_stage_id: upstream_stage_id.clone(),
                shuffle_partition_id: *partition_id,
                ..Default::default()
            },
        }
    }

    fn input_partition_descriptor_from_wire(
        value: v1::InputPartitionDescriptor,
    ) -> WireResult<InputPartitionDescriptor> {
        match v1::InputPartitionDescriptorKind::try_from(value.kind)
            .unwrap_or(v1::InputPartitionDescriptorKind::Unspecified)
        {
            v1::InputPartitionDescriptorKind::Unspecified => Err(WireError::new(
                "input partition descriptor kind must be specified",
            )),
            v1::InputPartitionDescriptorKind::LocalParquet => {
                require_non_empty(&value.table_name, "local parquet table name")?;
                require_non_empty(&value.path, "local parquet path")?;
                Ok(InputPartitionDescriptor::LocalParquet {
                    table_name: value.table_name,
                    path: value.path,
                })
            }
            v1::InputPartitionDescriptorKind::ConnectorParquet => {
                require_non_empty(&value.path, "connector parquet path")?;
                Ok(InputPartitionDescriptor::ConnectorParquet {
                    table_name: non_empty_string(value.table_name),
                    path: value.path,
                })
            }
            v1::InputPartitionDescriptorKind::ObjectParquet => {
                require_non_empty(&value.table_name, "object parquet table name")?;
                require_non_empty(&value.object_base_dir, "object parquet base dir")?;
                require_non_empty(&value.object_path, "object parquet path")?;
                Ok(InputPartitionDescriptor::ObjectParquet {
                    table_name: value.table_name,
                    base_dir: value.object_base_dir,
                    object_path: value.object_path,
                })
            }
            v1::InputPartitionDescriptorKind::MemoryKafka => {
                require_non_empty(&value.kafka_topic, "memory kafka topic")?;
                if value.memory_kafka_records.is_empty() {
                    return Err(WireError::new("memory kafka records cannot be empty"));
                }
                Ok(InputPartitionDescriptor::MemoryKafka {
                    topic: value.kafka_topic,
                    partition: value.kafka_partition,
                    start_offset: value.kafka_start_offset,
                    records: value
                        .memory_kafka_records
                        .into_iter()
                        .map(|record| MemoryKafkaRecord::new(record.id, record.value))
                        .collect(),
                })
            }
            v1::InputPartitionDescriptorKind::ShuffleFlight => {
                require_non_empty(&value.table_name, "shuffle flight table name")?;
                require_non_empty(
                    &value.shuffle_upstream_stage_id,
                    "shuffle upstream stage id",
                )?;
                Ok(InputPartitionDescriptor::ShuffleFlight {
                    table_name: value.table_name,
                    flight_endpoint: value.shuffle_flight_endpoint,
                    job_id: value.shuffle_job_id,
                    upstream_stage_id: value.shuffle_upstream_stage_id,
                    partition_id: value.shuffle_partition_id,
                })
            }
        }
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
            descriptor: value.descriptor().map(output_contract_descriptor_to_wire),
        }
    }

    fn output_contract_from_wire(value: v1::OutputContract) -> WireResult<OutputContract> {
        if value.description.trim().is_empty() {
            return Err(WireError::new(
                "output contract description cannot be empty",
            ));
        }
        let contract = OutputContract::new(
            output_contract_kind_from_wire(value.kind)?,
            value.description,
        );
        match value.descriptor {
            Some(descriptor) => {
                Ok(contract.with_descriptor(output_contract_descriptor_from_wire(descriptor)?))
            }
            None => Ok(contract),
        }
    }

    fn output_contract_descriptor_to_wire(
        value: &OutputContractDescriptor,
    ) -> v1::OutputContractDescriptor {
        match value {
            OutputContractDescriptor::InlineRecordBatches => v1::OutputContractDescriptor {
                kind: v1::OutputContractDescriptorKind::InlineRecordBatches as i32,
                ..Default::default()
            },
            OutputContractDescriptor::LocalFile { path } => v1::OutputContractDescriptor {
                kind: v1::OutputContractDescriptorKind::LocalFile as i32,
                path: path.clone(),
                ..Default::default()
            },
            OutputContractDescriptor::Shuffle { partition } => v1::OutputContractDescriptor {
                kind: v1::OutputContractDescriptorKind::Shuffle as i32,
                shuffle_partition: partition.clone(),
                ..Default::default()
            },
            OutputContractDescriptor::ObjectParquetSink {
                base_dir,
                object_path,
            } => v1::OutputContractDescriptor {
                kind: v1::OutputContractDescriptorKind::ObjectParquetSink as i32,
                object_base_dir: base_dir.clone(),
                object_path: object_path.clone(),
                ..Default::default()
            },
            OutputContractDescriptor::ParquetSink { path } => v1::OutputContractDescriptor {
                kind: v1::OutputContractDescriptorKind::ParquetSink as i32,
                path: path.clone(),
                ..Default::default()
            },
        }
    }

    fn output_contract_descriptor_from_wire(
        value: v1::OutputContractDescriptor,
    ) -> WireResult<OutputContractDescriptor> {
        match v1::OutputContractDescriptorKind::try_from(value.kind)
            .unwrap_or(v1::OutputContractDescriptorKind::Unspecified)
        {
            v1::OutputContractDescriptorKind::Unspecified => Err(WireError::new(
                "output contract descriptor kind must be specified",
            )),
            v1::OutputContractDescriptorKind::InlineRecordBatches => {
                Ok(OutputContractDescriptor::InlineRecordBatches)
            }
            v1::OutputContractDescriptorKind::LocalFile => {
                require_non_empty(&value.path, "local file output path")?;
                Ok(OutputContractDescriptor::LocalFile { path: value.path })
            }
            v1::OutputContractDescriptorKind::Shuffle => {
                require_non_empty(&value.shuffle_partition, "shuffle output partition")?;
                Ok(OutputContractDescriptor::Shuffle {
                    partition: value.shuffle_partition,
                })
            }
            v1::OutputContractDescriptorKind::ObjectParquetSink => {
                require_non_empty(&value.object_base_dir, "object parquet sink base dir")?;
                require_non_empty(&value.object_path, "object parquet sink path")?;
                Ok(OutputContractDescriptor::ObjectParquetSink {
                    base_dir: value.object_base_dir,
                    object_path: value.object_path,
                })
            }
            v1::OutputContractDescriptorKind::ParquetSink => {
                require_non_empty(&value.path, "parquet sink path")?;
                Ok(OutputContractDescriptor::ParquetSink { path: value.path })
            }
        }
    }

    fn require_non_empty(value: &str, field: &'static str) -> WireResult<()> {
        if value.trim().is_empty() {
            Err(WireError::new(format!("{field} cannot be empty")))
        } else {
            Ok(())
        }
    }

    fn non_empty_string(value: String) -> Option<String> {
        if value.trim().is_empty() {
            None
        } else {
            Some(value)
        }
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

    /// Convert a domain checkpoint ack request to protobuf.
    pub fn checkpoint_ack_request_to_wire(value: CheckpointAckRequest) -> v1::CheckpointAckRequest {
        v1::CheckpointAckRequest {
            job_id: value.job_id.as_str().to_owned(),
            operator_id: value.operator_id,
            task_id: value.task_id.as_str().to_owned(),
            epoch: value.epoch,
            fencing_token: value.fencing_token.as_u64(),
            source_offsets: value
                .source_offsets
                .into_iter()
                .map(|o| v1::CheckpointSourceOffset {
                    partition_id: o.partition_id,
                    offset: o.offset,
                })
                .collect(),
            snapshot_path: value.snapshot_path.unwrap_or_default(),
        }
    }

    /// Convert a protobuf checkpoint ack request to the domain contract.
    pub fn checkpoint_ack_request_from_wire(
        value: v1::CheckpointAckRequest,
    ) -> WireResult<CheckpointAckRequest> {
        let job_id = JobId::try_new(value.job_id).map_err(WireError::from_id)?;
        let task_id = TaskId::try_new(value.task_id).map_err(WireError::from_id)?;
        let fencing_token =
            FencingToken::try_new(value.fencing_token).map_err(WireError::from_id)?;
        let source_offsets = value
            .source_offsets
            .into_iter()
            .map(|o| CheckpointSourceOffset {
                partition_id: o.partition_id,
                offset: o.offset,
            })
            .collect();
        let snapshot_path = if value.snapshot_path.is_empty() {
            None
        } else {
            Some(value.snapshot_path)
        };
        Ok(CheckpointAckRequest {
            job_id,
            operator_id: value.operator_id,
            task_id,
            epoch: value.epoch,
            fencing_token,
            source_offsets,
            snapshot_path,
        })
    }

    /// Convert a domain checkpoint ack response to protobuf.
    pub fn checkpoint_ack_response_to_wire(
        value: CheckpointAckResponse,
    ) -> v1::CheckpointAckResponse {
        use v1::checkpoint_ack_response::Result as WireResult;
        let result = match value {
            CheckpointAckResponse::Accepted => WireResult::Accepted(v1::CheckpointAckAccepted {}),
            CheckpointAckResponse::StaleEpoch { current_epoch } => {
                WireResult::StaleEpoch(v1::CheckpointAckStaleEpoch { current_epoch })
            }
            CheckpointAckResponse::JobNotFound => {
                WireResult::JobNotFound(v1::CheckpointAckJobNotFound {})
            }
        };
        v1::CheckpointAckResponse {
            result: Some(result),
        }
    }

    /// Convert a protobuf checkpoint ack response to the domain contract.
    pub fn checkpoint_ack_response_from_wire(
        value: v1::CheckpointAckResponse,
    ) -> WireResult<CheckpointAckResponse> {
        use v1::checkpoint_ack_response::Result as WireVariant;
        match value.result {
            Some(WireVariant::Accepted(_)) => Ok(CheckpointAckResponse::Accepted),
            Some(WireVariant::StaleEpoch(s)) => Ok(CheckpointAckResponse::StaleEpoch {
                current_epoch: s.current_epoch,
            }),
            Some(WireVariant::JobNotFound(_)) => Ok(CheckpointAckResponse::JobNotFound),
            None => Err(WireError::new(
                "missing required field `checkpoint_ack_response.result`",
            )),
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
        AttemptId, ConnectorCapabilityFlags, DeregisterExecutorRequest, ExecutorDescriptor,
        ExecutorHeartbeatRequest, ExecutorId, ExecutorState, ExecutorTaskAssignment, FencingToken,
        InputPartition, InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState,
        LeaseGeneration, MemoryKafkaRecord, OutputContract, OutputContractDescriptor,
        OutputContractKind, PlanFragment, RegisterExecutorRequest, StageId, StageSpec,
        TaskAttemptRef, TaskCancellationRequest, TaskId, TaskOutputMetadata, TaskSpec, TaskState,
        TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion,
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
    fn connector_capability_flags_default_all_false() {
        let flags = ConnectorCapabilityFlags::default();
        assert!(!flags.bounded);
        assert!(!flags.unbounded);
        assert!(!flags.rewindable);
        assert!(!flags.transactional);
        assert!(!flags.idempotent);
    }

    #[test]
    fn task_spec_with_connector_capabilities() {
        let source_caps = ConnectorCapabilityFlags {
            bounded: true,
            rewindable: true,
            ..Default::default()
        };
        let sink_caps = ConnectorCapabilityFlags {
            idempotent: true,
            bounded: true,
            ..Default::default()
        };
        let task = TaskSpec::new(TaskId::try_new("task-caps-1").unwrap(), "parquet scan")
            .with_source_capabilities(source_caps.clone())
            .with_sink_capabilities(sink_caps.clone());
        assert_eq!(task.source_capabilities.as_ref(), Some(&source_caps));
        assert_eq!(task.sink_capabilities.as_ref(), Some(&sink_caps));
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
    fn typed_executor_task_assignment_round_trips_through_wire_contract() {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-typed").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("connector-pipeline:kafka-to-parquet"),
            OutputContract::typed(
                OutputContractKind::Sink,
                OutputContractDescriptor::ParquetSink {
                    path: String::from("/tmp/out.parquet"),
                },
            ),
        )
        .with_input_partitions(vec![InputPartition::typed(
            "part-1",
            InputPartitionDescriptor::MemoryKafka {
                topic: String::from("events"),
                partition: 0,
                start_offset: 42,
                records: vec![MemoryKafkaRecord::new(7, "seven")],
            },
        )]);

        let wire = super::wire::executor_task_assignment_to_wire(assignment.clone());
        let round_trip = super::wire::executor_task_assignment_from_wire(wire).unwrap();

        assert_eq!(round_trip, assignment);
        assert!(matches!(
            round_trip.input_partitions()[0].descriptor(),
            Some(InputPartitionDescriptor::MemoryKafka { topic, .. }) if topic == "events"
        ));
        assert!(matches!(
            round_trip.output_contract().descriptor(),
            Some(OutputContractDescriptor::ParquetSink { path }) if path == "/tmp/out.parquet"
        ));
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

    #[test]
    fn fencing_token_initial_is_one() {
        assert_eq!(FencingToken::initial().as_u64(), 1);
    }

    #[test]
    fn fencing_token_next_increments() {
        assert_eq!(FencingToken::initial().next().as_u64(), 2);
    }

    #[test]
    fn fencing_token_zero_rejected() {
        assert!(FencingToken::try_new(0).is_err());
    }

    #[test]
    fn fencing_token_ordering() {
        assert!(FencingToken::initial() < FencingToken::initial().next());
    }

    #[test]
    fn trace_context_is_active_when_non_empty() {
        let ctx = super::TraceContext::new("00-abc-def-01");
        assert!(ctx.is_active());
    }

    #[test]
    fn trace_context_inactive_by_default() {
        let ctx = super::TraceContext::default();
        assert!(!ctx.is_active());
    }

    #[test]
    fn executor_heartbeat_request_carries_trace_context() {
        let req = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_trace_context(super::TraceContext::new("00-trace-01-span-01-01"));
        assert!(req.trace_context().unwrap().is_active());
    }

    // ── R4a shuffle config tests ───────────────────────────────────────────────

    use super::{ShuffleReadConfig, ShuffleWriteConfig};

    #[test]
    fn test_shuffle_write_config_round_trip() {
        let write_cfg = ShuffleWriteConfig {
            stage_id: StageId::try_new("stage-write").unwrap(),
            num_partitions: 4,
            key_columns: vec![String::from("user_id")],
            lease_token: 99,
        };
        let task = TaskSpec::new(TaskId::try_new("task-write-rt").unwrap(), "sql: select 1")
            .with_shuffle_write(write_cfg.clone());
        let cfg = task.shuffle_write().expect("shuffle_write must be set");
        assert_eq!(cfg.num_partitions, 4);
        assert_eq!(cfg.lease_token, 99);
        assert_eq!(cfg, &write_cfg);
    }

    #[test]
    fn test_shuffle_read_config_round_trip() {
        let read_cfg = ShuffleReadConfig {
            stage_id: StageId::try_new("stage-read").unwrap(),
            partition_id: 7,
            lease_token: 42,
        };
        let task = TaskSpec::new(TaskId::try_new("task-read-rt").unwrap(), "shuffle-read")
            .with_shuffle_read(read_cfg.clone());
        let cfg = task.shuffle_read().expect("shuffle_read must be set");
        assert_eq!(cfg.partition_id, 7);
        assert_eq!(cfg, &read_cfg);
    }

    #[test]
    fn task_spec_with_no_shuffle_configs_has_none() {
        let task = TaskSpec::new(TaskId::try_new("task-plain").unwrap(), "sql: select 1");
        assert!(task.shuffle_write().is_none());
        assert!(task.shuffle_read().is_none());
    }

    // ── P0.17: heartbeat request resource fields round-trip ───────────────────

    #[test]
    fn heartbeat_request_all_resource_fields_round_trip() {
        let request = ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("exec-rt").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )
        .with_memory_used_bytes(512 * 1024 * 1024)
        .with_memory_limit_bytes(2 * 1024 * 1024 * 1024)
        .with_active_task_count(4)
        .with_cpu_cores_used(3.5)
        .with_network_bytes_sent(1_000_000)
        .with_network_bytes_recv(2_000_000);

        let wire = super::wire::executor_heartbeat_request_to_wire(request.clone());
        let round_trip = super::wire::executor_heartbeat_request_from_wire(wire).unwrap();

        assert_eq!(round_trip.memory_used_bytes(), request.memory_used_bytes());
        assert_eq!(round_trip.memory_limit_bytes(), request.memory_limit_bytes());
        assert_eq!(round_trip.active_task_count(), request.active_task_count());
        assert_eq!(round_trip.cpu_cores_used(), request.cpu_cores_used());
        assert_eq!(round_trip.network_bytes_sent(), request.network_bytes_sent());
        assert_eq!(round_trip.network_bytes_recv(), request.network_bytes_recv());
        assert_eq!(round_trip, request);
    }

    #[test]
    fn executor_task_assignment_carries_shuffle_write_config() {
        let write_cfg = ShuffleWriteConfig {
            stage_id: StageId::try_new("stage-sw").unwrap(),
            num_partitions: 3,
            key_columns: vec![String::from("id")],
            lease_token: 1,
        };
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-sw-assign").unwrap(),
                StageId::try_new("stage-sw").unwrap(),
                TaskId::try_new("task-sw-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select id from t"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        )
        .with_shuffle_write(write_cfg.clone());

        assert_eq!(assignment.shuffle_write().unwrap().num_partitions, 3);
        assert!(assignment.shuffle_read().is_none());
    }
}
