//! Job specs.

use crate::ids::{JobId, StageId};
use crate::io::{ResourceProfile, TaskSpec};
use crate::lifecycle::JobKind;

/// Job submission contract.
#[derive(Debug, Clone, PartialEq)]
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
    /// Streaming execution profile for runtime behavior.
    streaming_profile: Option<StreamingExecutionProfile>,
    /// Output buffer policy for streaming emission.
    output_buffer: Option<OutputBufferPolicy>,
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
            streaming_profile: None,
            output_buffer: None,
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

    /// Set the streaming execution profile for runtime behavior.
    #[must_use]
    pub fn with_streaming_profile(mut self, profile: StreamingExecutionProfile) -> Self {
        self.streaming_profile = Some(profile);
        self
    }

    /// Set the output buffer policy for streaming emission.
    #[must_use]
    pub fn with_output_buffer(mut self, buffer: OutputBufferPolicy) -> Self {
        self.output_buffer = Some(buffer);
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

    /// Streaming execution profile, if set.
    pub fn streaming_profile(&self) -> Option<&StreamingExecutionProfile> {
        self.streaming_profile.as_ref()
    }

    /// Output buffer policy, if set.
    pub fn output_buffer(&self) -> Option<&OutputBufferPolicy> {
        self.output_buffer.as_ref()
    }
}

/// Distinguishes shuffle-writing stages from terminal result stages.
///
/// `ShuffleMap` stages write hash-partitioned output to the shuffle store so
/// downstream `Result` stages can fetch it.  The AQE optimizer fires only on
/// completed `ShuffleMap` stages — `Result` stages consume the coalesce hint
/// produced by their upstream map stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StageKind {
    /// Stage writes output to the shuffle store (default).
    #[default]
    ShuffleMap,
    /// Terminal stage that reads from the shuffle store and produces final output.
    Result,
}

/// Stage contract inside a job.
#[derive(Debug, Clone, PartialEq)]
pub struct StageSpec {
    stage_id: StageId,
    name: String,
    kind: StageKind,
    tasks: Vec<TaskSpec>,
    /// Stage ids that must be fully Succeeded before this stage may launch.
    /// Empty means this stage has no upstream shuffle dependencies.
    upstream_stage_ids: Vec<StageId>,
    /// Number of shuffle output partitions this stage produces, if known.
    /// Coordinator uses this to pre-register Pending partition slots.
    output_partition_count: Option<u32>,
    /// GAP-3: Maximum per-task failure attempts before the task is permanently
    /// failed.  Defaults to 1 (no retries).  Setting to N means the task will
    /// be retried up to N-1 times on transient failures before failing the stage.
    ///
    /// Per-task retries are preferred over whole-stage retries for large jobs
    /// because only the failed task is reset, not all tasks.
    max_task_attempts: u32,
    /// SC10: per-stage resource profile.  All tasks in this stage request the
    /// same CPU and memory allocation.  The placement layer uses this to skip
    /// executors that cannot satisfy the requirement.
    resource_profile: Option<ResourceProfile>,
}

impl StageSpec {
    /// Create an empty stage spec.
    pub fn new(stage_id: StageId, name: impl Into<String>) -> Self {
        Self {
            stage_id,
            name: name.into(),
            kind: StageKind::ShuffleMap,
            tasks: Vec::new(),
            upstream_stage_ids: Vec::new(),
            output_partition_count: None,
            max_task_attempts: 1, // default: no retries
            resource_profile: None,
        }
    }

    /// Mark this stage as a terminal result stage (reads shuffle, produces final output).
    #[must_use]
    pub fn with_kind(mut self, kind: StageKind) -> Self {
        self.kind = kind;
        self
    }

    /// Whether this stage writes to the shuffle store or reads from it.
    pub fn kind(&self) -> StageKind {
        self.kind
    }

    /// Set the maximum number of per-task execution attempts.
    ///
    /// A value of 1 means no retries (default).  A value of 3 means each task
    /// will be attempted up to 3 times on transient failure before the stage fails.
    #[must_use]
    pub fn with_max_task_attempts(mut self, n: u32) -> Self {
        self.max_task_attempts = n.max(1);
        self
    }

    /// Maximum per-task execution attempts configured for this stage.
    pub fn max_task_attempts(&self) -> u32 {
        self.max_task_attempts
    }

    /// SC10: Set the per-stage resource profile.
    ///
    /// All tasks in this stage will be placed only on executors that can
    /// satisfy the stated CPU and memory requirements.
    #[must_use]
    pub fn with_resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.resource_profile = Some(profile);
        self
    }

    /// SC10: per-stage resource profile, if one was declared.
    pub fn resource_profile(&self) -> Option<&ResourceProfile> {
        self.resource_profile.as_ref()
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

// ── Streaming execution profile ───────────────────────────────────────────────

/// Runtime execution profile for streaming jobs.
///
/// Determines batch sizing, flush intervals, and backpressure behavior
/// to optimize for either latency or throughput.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingExecutionProfile {
    /// Optimize for low latency (p99 < 100ms).
    LowLatency {
        /// Maximum rows per batch.
        max_rows: usize,
        /// Maximum bytes per batch.
        max_bytes: usize,
        /// Flush interval in milliseconds.
        flush_interval_ms: u64,
    },
    /// Optimize for throughput (rows/sec).
    Throughput {
        /// Maximum rows per batch.
        max_rows: usize,
        /// Maximum bytes per batch.
        max_bytes: usize,
        /// Flush interval in milliseconds.
        flush_interval_ms: u64,
    },
    /// Auto-switch based on backlog with hysteresis.
    Auto {
        /// Backlog threshold in bytes to switch to throughput mode.
        backlog_threshold_bytes: usize,
        /// Hysteresis factor (0.0–1.0) to prevent oscillation.
        hysteresis: f64,
        /// Minimum interval between profile switches in milliseconds.
        min_switch_interval_ms: u64,
    },
}

impl Default for StreamingExecutionProfile {
    fn default() -> Self {
        Self::LowLatency {
            max_rows: 10_000,
            max_bytes: 1024 * 1024, // 1 MB
            flush_interval_ms: 100,
        }
    }
}

/// Output buffer policy for controlling flush behavior in streaming emission.
///
/// Determines when buffered data should be flushed based on row count,
/// byte size, or time intervals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBufferPolicy {
    /// Maximum rows before flush.
    pub max_rows: Option<usize>,
    /// Maximum bytes before flush.
    pub max_bytes: Option<u64>,
    /// Maximum time (ms) before flush.
    pub flush_interval_ms: Option<u64>,
    /// If true, flush on any condition; if false, flush on all conditions.
    pub flush_on_any: bool,
}

impl Default for OutputBufferPolicy {
    fn default() -> Self {
        Self {
            max_rows: Some(10_000),
            max_bytes: Some(1024 * 1024), // 1 MB
            flush_interval_ms: Some(100),
            flush_on_any: true,
        }
    }
}

impl OutputBufferPolicy {
    /// Create a low-latency policy (flush quickly).
    pub fn low_latency() -> Self {
        Self {
            max_rows: Some(1_000),
            max_bytes: Some(64 * 1024), // 64 KB
            flush_interval_ms: Some(10),
            flush_on_any: true,
        }
    }

    /// Create a throughput policy (batch aggressively).
    pub fn throughput() -> Self {
        Self {
            max_rows: Some(100_000),
            max_bytes: Some(10 * 1024 * 1024), // 10 MB
            flush_interval_ms: Some(1_000),
            flush_on_any: true,
        }
    }
}
