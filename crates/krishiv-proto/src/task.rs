//! Task types.

use std::fmt;

use crate::executor::{
    ExecutorDescriptor, HeartbeatHotKeyReport, HeartbeatThrottleCommand, LlmQuotaReport,
    LlmThrottleCommand, StreamingProgressReport, StreamingTaskState, TaskOutputMetadata,
    TraceContext,
};
use crate::ids::*;
use crate::io::*;
use crate::lifecycle::*;

/// Inclusive key-group range assigned to a stateful task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyGroupRange {
    start: u32,
    end: u32,
}

impl KeyGroupRange {
    /// Create an inclusive key-group range.
    ///
    /// # Panics
    ///
    /// Panics if `start > end`. This check runs in release builds too (unlike
    /// `debug_assert!`) because an inverted range silently corrupts key-group
    /// partitioning. Use [`KeyGroupRange::try_new`] to handle invalid input
    /// without panicking.
    pub fn new(start: u32, end: u32) -> Self {
        Self::try_new(start, end).expect("KeyGroupRange::new: start must not exceed end")
    }

    /// Create an inclusive key-group range, returning an error if `start > end`.
    pub fn try_new(start: u32, end: u32) -> ProtoResult<Self> {
        if start > end {
            return Err(IdError::range("key-group range"));
        }
        Ok(Self { start, end })
    }

    /// Full single-node/default key-group range.
    pub fn full() -> Self {
        Self::new(0, 32_767)
    }

    /// First key group in the inclusive range.
    pub fn start(&self) -> u32 {
        self.start
    }

    /// Last key group in the inclusive range.
    pub fn end(&self) -> u32 {
        self.end
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
    missing_shuffle_partitions: Vec<MissingShufflePartition>,
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
            missing_shuffle_partitions: Vec::new(),
        }
    }

    /// Attach the executor lease generation used for this status update.
    #[must_use]
    pub fn with_lease_generation(mut self, lease_generation: LeaseGeneration) -> Self {
        self.lease_generation = lease_generation;
        self
    }

    /// Attach shuffle partitions the consumer found missing during input fetch.
    #[must_use]
    pub fn with_missing_shuffle_partitions(
        mut self,
        missing: Vec<MissingShufflePartition>,
    ) -> Self {
        self.missing_shuffle_partitions = missing;
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

    /// Shuffle partitions the consumer found missing during input fetch.
    ///
    /// Non-empty only on `Failed` updates where the failure was caused by a
    /// missing upstream shuffle partition. The coordinator reacts by marking
    /// the producing partitions failed and re-queuing the producer tasks.
    pub fn missing_shuffle_partitions(&self) -> &[MissingShufflePartition] {
        &self.missing_shuffle_partitions
    }
}

/// A shuffle partition a consumer task could not fetch because the producer's
/// output is gone (e.g. the producing executor's disk was lost).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingShufflePartition {
    stage_id: StageId,
    partition_id: u32,
}

impl MissingShufflePartition {
    /// Create a missing-partition reference for the producing stage.
    pub fn new(stage_id: StageId, partition_id: u32) -> Self {
        Self {
            stage_id,
            partition_id,
        }
    }

    /// Stage that produced the missing partition.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Partition index within the producing stage.
    pub fn partition_id(&self) -> u32 {
        self.partition_id
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

impl_with_version!(RegisterExecutorRequest);

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

impl_with_version!(RegisterExecutorResponse);

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
    /// Streaming progress snapshots (GAP-OB-04).
    streaming_progress: Vec<StreamingProgressReport>,
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
            streaming_progress: Vec::new(),
        }
    }

    /// Override the transport version when mapping from a wire request.
    #[must_use]
    pub fn with_version(mut self, version: TransportVersion) -> Self {
        self.version = version;
        self
    }

    /// Override the executor lease generation stamped on this heartbeat.
    ///
    /// Used by the gRPC client adapter to stamp the live atomic lease before
    /// transmission (B7) so that heartbeats sent after a lease bump cannot
    /// ship a stale generation.
    #[must_use]
    pub fn with_lease_generation(mut self, lease_generation: LeaseGeneration) -> Self {
        self.lease_generation = lease_generation;
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

    /// Attach streaming progress snapshots (GAP-OB-04).
    #[must_use]
    pub fn with_streaming_progress(mut self, reports: Vec<StreamingProgressReport>) -> Self {
        self.streaming_progress = reports;
        self
    }

    /// Periodic streaming progress snapshots in this request.
    pub fn streaming_progress(&self) -> &[StreamingProgressReport] {
        &self.streaming_progress
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

/// Coordinator → executor: checkpoint epoch is durably committed (delivered
/// via heartbeat).  Executors commit transactional-sink output prepared at or
/// before `epoch` when they receive this command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCompleteCommand {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
}

/// Coordinator → executor: restore job state from checkpoint `epoch`
/// (delivered via heartbeat).  Executors reload operator snapshots into their
/// state backends, re-seed source offsets, and abort transactional-sink output
/// prepared after `epoch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreFromCheckpointCommand {
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
    /// Committed-epoch notifications driving transactional-sink commits.
    checkpoint_complete_commands: Vec<CheckpointCompleteCommand>,
    /// Restore directives driving executor-side state/offset reload.
    restore_commands: Vec<RestoreFromCheckpointCommand>,
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
            checkpoint_complete_commands: Vec::new(),
            restore_commands: Vec::new(),
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

    /// Attach committed-epoch notifications for transactional-sink commits.
    #[must_use]
    pub fn with_checkpoint_complete_commands(
        mut self,
        cmds: Vec<CheckpointCompleteCommand>,
    ) -> Self {
        self.checkpoint_complete_commands = cmds;
        self
    }

    /// Committed-epoch notifications in this response.
    pub fn checkpoint_complete_commands(&self) -> &[CheckpointCompleteCommand] {
        &self.checkpoint_complete_commands
    }

    /// Attach restore directives for executor-side state/offset reload.
    #[must_use]
    pub fn with_restore_commands(mut self, cmds: Vec<RestoreFromCheckpointCommand>) -> Self {
        self.restore_commands = cmds;
        self
    }

    /// Restore directives in this response.
    pub fn restore_commands(&self) -> &[RestoreFromCheckpointCommand] {
        &self.restore_commands
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
#[derive(Debug, Clone, PartialEq)]
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
        job_id: JobId,
        /// Stage id that produced this partition.
        upstream_stage_id: StageId,
        /// Partition index.
        partition_id: u32,
    },
    /// Arrow IPC stream bytes delivered inline with the task assignment.
    ///
    /// The executor decodes the bytes and registers them as `table_name` in
    /// the local DataFusion context before executing the fragment SQL.
    InlineIpc {
        table_name: String,
        ipc_bytes: Vec<u8>,
    },
    /// Zero-copy in-process Arrow RecordBatches.
    ///
    /// Only valid when coordinator and executor run in the **same process**
    /// (embedded mode). Passes pre-built `RecordBatch` values directly,
    /// eliminating the IPC encode → Base64 → decode round-trip that
    /// `InlineIpc` incurs. Remote/distributed callers must use `InlineIpc`
    /// instead.
    InMemory {
        table_name: String,
        batches: Vec<std::sync::Arc<arrow::record_batch::RecordBatch>>,
    },
    /// Carries the upstream stage's output watermark to initialise the
    /// downstream stage's window operators. The executor reads this hint and
    /// applies it as the initial `prev_watermark_ms` so late-event detection
    /// is accurate from the very first batch.
    WatermarkHint { watermark_ms: i64 },
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
            Self::InlineIpc {
                table_name,
                ipc_bytes,
            } => {
                format!("inline-ipc:{table_name}:{}b", ipc_bytes.len())
            }
            Self::InMemory {
                table_name,
                batches,
            } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                format!("in-memory:{table_name}:{rows}rows")
            }
            Self::WatermarkHint { watermark_ms } => {
                format!("watermark-hint:{watermark_ms}")
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
#[derive(Debug, Clone, PartialEq)]
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
    /// Explicit execution model: `true` for streaming (continuous) tasks.
    /// When `false`, the executor falls back to description-based detection.
    is_streaming: bool,
}

impl PlanFragment {
    /// Create a plan fragment descriptor (batch by default).
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            is_streaming: false,
        }
    }

    /// Set the explicit streaming flag, consuming `self`.
    pub fn with_streaming(mut self, is_streaming: bool) -> Self {
        self.is_streaming = is_streaming;
        self
    }

    /// Human-readable fragment description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Whether this fragment runs a streaming (continuous) execution model.
    pub fn is_streaming(&self) -> bool {
        self.is_streaming
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
#[derive(Debug, Clone, PartialEq)]
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
    key_group_range: KeyGroupRange,

    /// Explicit signal whether this streaming task should report Succeeded
    /// (one-shot) or stay Running and reattach after recovery (continuous).
    /// Assignment producers derive this from their typed job/task contract.
    requires_reattach: bool,

    /// CPU time limit for this task in nanoseconds (from job spec).
    cpu_limit_nanos: Option<u64>,
    /// Memory limit for this task in bytes (from job spec).
    memory_limit_bytes: Option<u64>,
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
            key_group_range: KeyGroupRange::full(),
            requires_reattach: false,
            cpu_limit_nanos: None,
            memory_limit_bytes: None,
        }
    }

    /// Mark that this streaming task should stay in Running state and be
    /// re-attached on the next drain (typed replacement for old string heuristics).
    #[must_use]
    pub fn with_requires_reattach(mut self, reattach: bool) -> Self {
        self.requires_reattach = reattach;
        self
    }

    /// Whether the task needs re-attachment (continuous streaming).
    pub fn requires_reattach(&self) -> bool {
        self.requires_reattach
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

    /// Attach the inclusive key-group range owned by this task.
    #[must_use]
    pub fn with_key_group_range(mut self, range: KeyGroupRange) -> Self {
        self.key_group_range = range;
        self
    }

    /// Inclusive key-group range owned by this task.
    pub fn key_group_range(&self) -> KeyGroupRange {
        self.key_group_range
    }

    /// Attach CPU time limit for this task.
    #[must_use]
    pub fn with_cpu_limit_nanos(mut self, nanos: u64) -> Self {
        self.cpu_limit_nanos = Some(nanos);
        self
    }

    /// Attach memory limit for this task.
    #[must_use]
    pub fn with_memory_limit_bytes(mut self, bytes: u64) -> Self {
        self.memory_limit_bytes = Some(bytes);
        self
    }

    /// CPU time limit for this task in nanoseconds, if set.
    pub fn cpu_limit_nanos(&self) -> Option<u64> {
        self.cpu_limit_nanos
    }

    /// Memory limit for this task in bytes, if set.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        self.memory_limit_bytes
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
    missing_shuffle_partitions: Vec<MissingShufflePartition>,
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
            missing_shuffle_partitions: Vec::new(),
        }
    }

    /// Attach shuffle partitions the consumer found missing during input fetch.
    #[must_use]
    pub fn with_missing_shuffle_partitions(
        mut self,
        missing: Vec<MissingShufflePartition>,
    ) -> Self {
        self.missing_shuffle_partitions = missing;
        self
    }

    /// Shuffle partitions the consumer found missing during input fetch.
    pub fn missing_shuffle_partitions(&self) -> &[MissingShufflePartition] {
        &self.missing_shuffle_partitions
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

    /// Override the executor lease generation stamped on this request.
    ///
    /// Used by the gRPC client adapter to stamp the live atomic lease before
    /// transmission (B7) so that retries after a lease bump cannot ship a
    /// stale generation.
    #[must_use]
    pub fn with_lease_generation(mut self, lease_generation: LeaseGeneration) -> Self {
        self.lease_generation = lease_generation;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushContinuousInputRequest {
    pub version: TransportVersion,
    pub job_id: JobId,
    pub task_id: TaskId,
    pub ipc_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainContinuousOutputRequest {
    pub version: TransportVersion,
    pub job_id: JobId,
    pub task_id: TaskId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainContinuousOutputResponse {
    pub version: TransportVersion,
    pub disposition: TransportDisposition,
    pub ipc_bytes: Vec<u8>,
}
