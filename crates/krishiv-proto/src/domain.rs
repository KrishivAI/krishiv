#![forbid(unsafe_code)]
//! Domain control-plane contracts.

use std::error::Error;
use std::fmt;

use crate::ids::*;

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
    barrier_endpoint: Option<String>,
}

impl ExecutorDescriptor {
    /// Create an executor descriptor.
    pub fn new(executor_id: ExecutorId, host: impl Into<String>, slots: usize) -> Self {
        Self {
            executor_id,
            host: host.into(),
            slots,
            task_endpoint: None,
            barrier_endpoint: None,
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

    /// Attach the executor-owned barrier gRPC endpoint (BarrierService).
    #[must_use]
    pub fn with_barrier_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        if !endpoint.trim().is_empty() {
            self.barrier_endpoint = Some(endpoint);
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

    /// Optional executor-owned barrier service endpoint.
    pub fn barrier_endpoint(&self) -> Option<&str> {
        self.barrier_endpoint.as_deref()
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

/// Per-model LLM API usage reported by an executor (R17).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmQuotaReport {
    pub model: String,
    pub requests_used: u64,
    pub tokens_used: u64,
    pub period_ms: u64,
}

/// Coordinator throttle directive for executor LLM rate limiters (R17).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmThrottleCommand {
    pub model: String,
    pub max_requests_per_minute: u32,
    pub max_tokens_per_minute: u64,
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
    /// LLM quota usage reports (R17).
    llm_quota_reports: Vec<LlmQuotaReport>,
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
            llm_quota_reports: Vec::new(),
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

    /// Attach LLM quota reports (R17).
    #[must_use]
    pub fn with_llm_quota_reports(mut self, reports: Vec<LlmQuotaReport>) -> Self {
        self.llm_quota_reports = reports;
        self
    }

    /// LLM quota reports in this heartbeat.
    pub fn llm_quota_reports(&self) -> &[LlmQuotaReport] {
        &self.llm_quota_reports
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
    /// LLM quota reports (R17).
    llm_quota_reports: Vec<LlmQuotaReport>,
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
            llm_quota_reports: Vec::new(),
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

    /// Attach LLM quota reports (R17).
    #[must_use]
    pub fn with_llm_quota_reports(mut self, reports: Vec<LlmQuotaReport>) -> Self {
        self.llm_quota_reports = reports;
        self
    }

    /// LLM quota reports in this request.
    pub fn llm_quota_reports(&self) -> &[LlmQuotaReport] {
        &self.llm_quota_reports
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

/// Coordinator → executor: begin checkpoint epoch (delivered via heartbeat).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateCheckpointCommand {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
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
    /// LLM throttle commands for executor-wide rate limiters (R17).
    llm_throttles: Vec<LlmThrottleCommand>,
    checkpoint_commands: Vec<InitiateCheckpointCommand>,
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
            llm_throttles: Vec::new(),
            checkpoint_commands: Vec::new(),
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

    /// Attach LLM throttle commands (R17).
    #[must_use]
    pub fn with_llm_throttles(mut self, cmds: Vec<LlmThrottleCommand>) -> Self {
        self.llm_throttles = cmds;
        self
    }

    /// LLM throttle commands in this response.
    pub fn llm_throttles(&self) -> &[LlmThrottleCommand] {
        &self.llm_throttles
    }

    #[must_use]
    pub fn with_checkpoint_commands(mut self, cmds: Vec<InitiateCheckpointCommand>) -> Self {
        self.checkpoint_commands = cmds;
        self
    }

    pub fn checkpoint_commands(&self) -> &[InitiateCheckpointCommand] {
        &self.checkpoint_commands
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

