use krishiv_proto::{
    ConnectorCapabilityFlags, ExecutorId, JobId, JobKind, JobState, StageId, StageState, TaskId,
    TaskOutputMetadata, TaskState,
};

use crate::ExecutorHeartbeatAge;

use super::scheduler::ResourceUsage;


/// Job status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSnapshot {
    pub(crate) job_id: JobId,
    pub(crate) kind: JobKind,
    pub(crate) state: JobState,
    pub(crate) stage_count: usize,
    pub(crate) task_count: usize,
    pub(crate) assigned_task_count: usize,
    pub(crate) running_task_count: usize,
    pub(crate) succeeded_task_count: usize,
    pub(crate) failed_task_count: usize,
    /// Scheduling priority (0 = lowest, 255 = highest).
    pub(crate) priority: u8,
    /// Governance namespace, if set.
    pub(crate) namespace_id: Option<String>,
    /// Accumulated resource consumption from completed tasks.
    pub(crate) resource_usage: ResourceUsage,
}

impl JobSnapshot {
    /// Job id.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Job kind.
    pub fn kind(&self) -> JobKind {
        self.kind
    }

    /// Job state.
    pub fn state(&self) -> JobState {
        self.state
    }

    /// Number of stages.
    pub fn stage_count(&self) -> usize {
        self.stage_count
    }

    /// Number of tasks.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Number of assigned tasks.
    pub fn assigned_task_count(&self) -> usize {
        self.assigned_task_count
    }

    /// Number of running tasks.
    pub fn running_task_count(&self) -> usize {
        self.running_task_count
    }

    /// Number of succeeded tasks.
    pub fn succeeded_task_count(&self) -> usize {
        self.succeeded_task_count
    }

    /// Number of failed tasks.
    pub fn failed_task_count(&self) -> usize {
        self.failed_task_count
    }

    /// Scheduling priority.
    pub fn priority(&self) -> u8 {
        self.priority
    }

    /// Governance namespace.
    pub fn namespace_id(&self) -> Option<&str> {
        self.namespace_id.as_deref()
    }

    /// Accumulated resource consumption.
    pub fn resource_usage(&self) -> &ResourceUsage {
        &self.resource_usage
    }
}

/// Detailed job status for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDetailSnapshot {
    pub(crate) job: JobSnapshot,
    pub(crate) stages: Vec<StageSnapshot>,
}

impl JobDetailSnapshot {
    /// Job summary.
    pub fn job(&self) -> &JobSnapshot {
        &self.job
    }

    /// Stage summaries.
    pub fn stages(&self) -> &[StageSnapshot] {
        &self.stages
    }
}

/// Stage status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSnapshot {
    pub(crate) stage_id: StageId,
    pub(crate) state: StageState,
    pub(crate) retry_count: u32,
    pub(crate) task_count: usize,
    pub(crate) tasks: Vec<TaskSnapshot>,
}

impl StageSnapshot {
    /// Stage id.
    pub fn stage_id(&self) -> &StageId {
        &self.stage_id
    }

    /// Stage state.
    pub fn state(&self) -> StageState {
        self.state
    }

    /// Number of stage-level retries already scheduled.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Number of tasks in this stage.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// Task summaries.
    pub fn tasks(&self) -> &[TaskSnapshot] {
        &self.tasks
    }
}

/// Task status summary for CLI/UI use in later R2 slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub(crate) task_id: TaskId,
    pub(crate) state: TaskState,
    pub(crate) assigned_executor: Option<ExecutorId>,
    pub(crate) attempt: u32,
    pub(crate) output_metadata: Option<TaskOutputMetadata>,
    pub(crate) last_failure_reason: Option<String>,
    pub(crate) failure_count: u32,
    /// Capability flags declared by the source connector for this task, if known.
    pub source_capabilities: Option<ConnectorCapabilityFlags>,
    /// Capability flags declared by the sink connector for this task, if known.
    pub sink_capabilities: Option<ConnectorCapabilityFlags>,
    /// Last event-time watermark reported by this streaming task's executor (ms since epoch).
    pub last_watermark_ms: Option<i64>,
    /// Last committed source offset reported by this streaming task's executor.
    pub last_source_offset: Option<Vec<u8>>,
}

impl TaskSnapshot {
    /// Task id.
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Task state.
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// Assigned executor, if any.
    pub fn assigned_executor(&self) -> Option<&ExecutorId> {
        self.assigned_executor.as_ref()
    }

    /// Current attempt number.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Last reported output metadata.
    pub fn output_metadata(&self) -> Option<&TaskOutputMetadata> {
        self.output_metadata.as_ref()
    }

    /// Last failure reason reported by the executor, if any.
    pub fn last_failure_reason(&self) -> Option<&str> {
        self.last_failure_reason.as_deref()
    }

    /// Number of times this specific task has failed (for observability).
    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }

    /// Last event-time watermark reported by this streaming task (ms since epoch).
    pub fn last_watermark_ms(&self) -> Option<i64> {
        self.last_watermark_ms
    }

    /// Last committed source offset reported by this streaming task.
    pub fn last_source_offset(&self) -> Option<&[u8]> {
        self.last_source_offset.as_deref()
    }
}

/// Basic R3.1 scheduler/executor stability metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StabilityMetrics {
    pub(crate) heartbeat_ages: Vec<ExecutorHeartbeatAge>,
    pub(crate) retry_count: usize,
    pub(crate) running_task_count: usize,
    pub(crate) failed_assignments: usize,
    /// Total shuffle partitions currently marked Available across all active jobs.
    pub shuffle_partitions_available: usize,
    /// Total shuffle bytes written across all active jobs.
    pub shuffle_bytes_written: u64,
}

impl StabilityMetrics {
    /// Zero-valued metrics for use when the coordinator lock is unavailable.
    pub fn empty() -> Self {
        Self {
            heartbeat_ages: Vec::new(),
            retry_count: 0,
            running_task_count: 0,
            failed_assignments: 0,
            shuffle_partitions_available: 0,
            shuffle_bytes_written: 0,
        }
    }

    /// Heartbeat age per executor.
    pub fn heartbeat_ages(&self) -> &[ExecutorHeartbeatAge] {
        &self.heartbeat_ages
    }

    /// Total stage retry count.
    pub fn retry_count(&self) -> usize {
        self.retry_count
    }

    /// Currently running task count.
    pub fn running_task_count(&self) -> usize {
        self.running_task_count
    }

    /// Failed assignment count.
    pub fn failed_assignments(&self) -> usize {
        self.failed_assignments
    }
}
