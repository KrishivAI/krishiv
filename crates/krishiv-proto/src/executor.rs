//! Executor types.

use crate::ids::*;
use crate::lifecycle::*;
use crate::task::TransportDisposition;

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
    /// Total bytes written to the shuffle store across all output partitions.
    ///
    /// Zero for non-shuffle tasks (SQL collect, streaming window). When
    /// non-zero, AQE rules use this in preference to `memory_bytes` because
    /// it reflects the actual wire/disk cost of the shuffle exchange.
    pub serialized_bytes: u64,
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
    /// Arrow IPC stream bytes per result batch for inline SQL/window collect.
    inline_record_batch_ipc: Vec<Vec<u8>>,
    /// GAP-2: Maximum event-time watermark (milliseconds since epoch) emitted by
    /// this streaming window task.  `None` for batch and non-window tasks.
    ///
    /// Transmitted back to the coordinator in the `TaskStatusRequest` so the
    /// coordinator can track the global low-watermark across all executor tasks.
    watermark_ms: Option<i64>,
    /// Hot-key reports from `HeavyHittersTracker` observed during shuffle write.
    hot_key_reports: Vec<HeartbeatHotKeyReport>,
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
            inline_record_batch_ipc: Vec::new(),
            watermark_ms: None,
            hot_key_reports: Vec::new(),
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

    /// Attach hot-key reports.
    #[must_use]
    pub fn with_hot_key_reports(mut self, reports: Vec<HeartbeatHotKeyReport>) -> Self {
        self.hot_key_reports = reports;
        self
    }

    /// Hot-key reports.
    pub fn hot_key_reports(&self) -> &[HeartbeatHotKeyReport] {
        &self.hot_key_reports
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

    /// Inline Arrow IPC payloads for coordinator/Flight result fetch.
    pub fn inline_record_batch_ipc(&self) -> &[Vec<u8>] {
        &self.inline_record_batch_ipc
    }

    /// Attach inline result batches encoded as Arrow IPC stream bytes.
    #[must_use]
    pub fn with_inline_record_batch_ipc(mut self, batches: Vec<Vec<u8>>) -> Self {
        self.inline_record_batch_ipc = batches;
        self
    }

    /// Attach the maximum event-time watermark for a streaming window task.
    #[must_use]
    pub fn with_watermark_ms(mut self, watermark_ms: i64) -> Self {
        self.watermark_ms = Some(watermark_ms);
        self
    }

    /// Maximum event-time watermark (ms) emitted by this streaming window task.
    pub fn watermark_ms(&self) -> Option<i64> {
        self.watermark_ms
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

impl_with_version!(DeregisterExecutorRequest);

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

impl_with_version!(DeregisterExecutorResponse);

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
    pub job_id: JobId,
    /// Source or operator id that produced this report.
    pub source_id: String,
}

impl Eq for HeartbeatHotKeyReport {}

impl PartialOrd for HeartbeatHotKeyReport {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeartbeatHotKeyReport {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.heat_score
            .total_cmp(&other.heat_score)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| self.estimated_count.cmp(&other.estimated_count))
            .then_with(|| self.max_error.cmp(&other.max_error))
            .then_with(|| self.job_id.cmp(&other.job_id))
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
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
    pub watermark_ms: i64,
    /// Last committed source offset for this task's input partition.
    /// Encoded as a byte string whose interpretation is connector-specific.
    pub source_offset: Vec<u8>,
}

impl StreamingTaskState {
    /// Create a streaming task state report.
    pub fn new(task_id: TaskId, watermark_ms: i64, source_offset: Vec<u8>) -> Self {
        Self {
            task_id,
            watermark_ms,
            source_offset,
        }
    }
}

/// Periodic streaming progress report (GAP-OB-04).
///
/// Emitted by continuous streaming operators on the executor and forwarded to
/// the coordinator via the heartbeat. Used to populate per-job/task metrics
/// and detect silent hangs where the task is `Running` but making zero progress.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamingProgressReport {
    /// Job that owns the streaming task.
    pub job_id: JobId,
    /// Task that produced this snapshot.
    pub task_id: TaskId,
    /// Current event-time watermark in milliseconds since epoch.
    pub watermark_ms: i64,
    /// Total rows emitted since task start (cumulative).
    pub rows_emitted: u64,
    /// Total batches emitted since task start (cumulative).
    pub batches_emitted: u64,
    /// Approximate state backend byte size.
    pub state_bytes: u64,
    /// Current source offset (connector-specific encoding).
    pub source_offset: Vec<u8>,
    /// Wall-clock timestamp of this snapshot in milliseconds since epoch.
    pub timestamp_ms: u64,
}

impl StreamingProgressReport {
    pub fn new(job_id: JobId, task_id: TaskId) -> Self {
        Self {
            job_id,
            task_id,
            watermark_ms: 0,
            rows_emitted: 0,
            batches_emitted: 0,
            state_bytes: 0,
            source_offset: Vec::new(),
            timestamp_ms: 0,
        }
    }

    #[must_use]
    pub fn with_watermark_ms(mut self, ms: i64) -> Self {
        self.watermark_ms = ms;
        self
    }

    #[must_use]
    pub fn with_rows_emitted(mut self, rows: u64) -> Self {
        self.rows_emitted = rows;
        self
    }

    #[must_use]
    pub fn with_batches_emitted(mut self, batches: u64) -> Self {
        self.batches_emitted = batches;
        self
    }

    #[must_use]
    pub fn with_state_bytes(mut self, bytes: u64) -> Self {
        self.state_bytes = bytes;
        self
    }

    #[must_use]
    pub fn with_source_offset(mut self, offset: Vec<u8>) -> Self {
        self.source_offset = offset;
        self
    }

    #[must_use]
    pub fn with_timestamp_ms(mut self, ms: u64) -> Self {
        self.timestamp_ms = ms;
        self
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
    /// Periodic streaming progress snapshots (GAP-OB-04).
    ///
    /// Each entry is a mid-execution progress report from a continuous streaming
    /// task. Unlike `streaming_task_states` (which reports re-attach state),
    /// these snapshots are emitted periodically while the task is actively
    /// running and carry watermark, row throughput, and state-size information.
    streaming_progress: Vec<StreamingProgressReport>,
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
            streaming_progress: Vec::new(),
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

    /// Attach streaming progress snapshots (GAP-OB-04).
    #[must_use]
    pub fn with_streaming_progress(mut self, reports: Vec<StreamingProgressReport>) -> Self {
        self.streaming_progress = reports;
        self
    }

    /// Periodic streaming progress snapshots in this heartbeat.
    pub fn streaming_progress(&self) -> &[StreamingProgressReport] {
        &self.streaming_progress
    }
}
