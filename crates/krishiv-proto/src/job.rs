//! Job specs.

use crate::ids::*;
use crate::io::TaskSpec;
use crate::lifecycle::JobKind;

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
