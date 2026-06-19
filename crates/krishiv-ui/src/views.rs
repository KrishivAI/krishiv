use askama::Template;
use krishiv_proto::ConnectorCapabilityFlags;
use krishiv_scheduler::metrics::SchedulerMetrics;
use krishiv_scheduler::{
    JobHistoryRecord, NamespaceQuotaSnapshot, ResourceUsage, StabilityMetrics,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobsResponse {
    /// Job summaries for the requested page.
    pub jobs: Vec<JobSummaryView>,
    /// Total number of jobs known to the coordinator (before pagination).
    pub total: usize,
    /// Page size applied.
    pub limit: usize,
    /// Page offset applied.
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobDetailResponse {
    /// Job detail.
    pub job: JobDetailView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorsResponse {
    /// Executor summaries.
    pub executors: Vec<ExecutorView>,
}

/// Response body for `GET /api/v1/jobs/{job_id}/checkpoints`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobCheckpointsResponse {
    /// Job id.
    pub job_id: String,
    /// All valid committed epochs for this job, in ascending order.
    pub epochs: Vec<u64>,
    /// Latest valid epoch, or `None` if no checkpoints exist.
    pub latest_epoch: Option<u64>,
}

/// R7.1 resource usage view — mirrors `ResourceUsage` for JSON serialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceUsageView {
    /// Total CPU nanoseconds consumed by completed tasks.
    pub cpu_nanos: u64,
    /// Peak memory bytes observed across completed tasks (max over tasks, not sum).
    pub memory_peak_task_bytes: u64,
    /// Sum of memory bytes across all completed tasks.
    pub memory_total_bytes: u64,
    /// Number of completed tasks that reported stats.
    pub task_count: u32,
}

impl ResourceUsageView {
    pub(crate) fn from_usage(u: &ResourceUsage) -> Self {
        Self {
            cpu_nanos: u.cpu_nanos,
            memory_peak_task_bytes: u.memory_peak_task_bytes,
            memory_total_bytes: u.memory_total_bytes,
            task_count: u.task_count,
        }
    }
}

/// R7.1 namespace quota view for `GET /api/v1/queues`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NamespaceQuotaView {
    /// Namespace identifier (`null` = default namespace).
    pub namespace_id: Option<String>,
    /// CPU nanoseconds currently reserved by active jobs.
    pub cpu_nanos_reserved: u64,
    /// Memory bytes currently reserved by active jobs.
    pub memory_bytes_reserved: u64,
    /// Number of active (non-terminal) jobs in this namespace.
    pub active_job_count: usize,
}

impl NamespaceQuotaView {
    pub(crate) fn from_snapshot(snap: &NamespaceQuotaSnapshot) -> Self {
        Self {
            namespace_id: snap.namespace_id.clone(),
            cpu_nanos_reserved: snap.cpu_nanos_reserved,
            memory_bytes_reserved: snap.memory_bytes_reserved,
            active_job_count: snap.active_job_count,
        }
    }
}

/// Response for `GET /api/v1/queues`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueuesResponse {
    /// Per-namespace quota snapshot (default namespace first, then alphabetical).
    pub namespaces: Vec<NamespaceQuotaView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobSummaryView {
    /// Job id.
    pub job_id: String,
    /// Job kind.
    pub kind: String,
    /// Job lifecycle state.
    pub state: String,
    /// Stage count.
    pub stage_count: usize,
    /// Total task count.
    pub task_count: usize,
    /// Assigned task count.
    pub assigned_task_count: usize,
    /// Running task count.
    pub running_task_count: usize,
    /// Succeeded task count.
    pub succeeded_task_count: usize,
    /// Failed task count.
    pub failed_task_count: usize,
    /// Scheduling priority (0 = lowest, 255 = highest).
    pub priority: u8,
    /// Governance namespace, if set.
    pub namespace_id: Option<String>,
    /// Accumulated resource consumption from completed tasks.
    pub resource_usage: ResourceUsageView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobDetailView {
    /// Job summary.
    pub summary: JobSummaryView,
    /// Stage details.
    pub stages: Vec<StageView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StageView {
    /// Stage id.
    pub stage_id: String,
    /// Stage lifecycle state.
    pub state: String,
    /// Number of stage-level retries already scheduled.
    pub retry_count: u32,
    /// Task count.
    pub task_count: usize,
    /// Task details.
    pub tasks: Vec<TaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectorCapabilityView {
    pub bounded: bool,
    pub unbounded: bool,
    pub rewindable: bool,
    pub transactional: bool,
    pub idempotent: bool,
}

impl ConnectorCapabilityView {
    pub(crate) fn from_flags(flags: &ConnectorCapabilityFlags) -> Self {
        Self {
            bounded: flags.bounded,
            unbounded: flags.unbounded,
            rewindable: flags.rewindable,
            transactional: flags.transactional,
            idempotent: flags.idempotent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskView {
    /// Task id.
    pub task_id: String,
    /// Task lifecycle state.
    pub state: String,
    /// Assigned executor id, if any.
    pub assigned_executor: String,
    /// Current attempt number.
    pub attempt: u32,
    /// Number of consecutive failures.
    pub failure_count: u32,
    /// Last failure reason reported by the executor, if any (empty string when none).
    pub failure_reason_display: String,
    /// Source connector capability flags for this task, if declared.
    pub source_capabilities: Option<ConnectorCapabilityView>,
    /// Sink connector capability flags for this task, if declared.
    pub sink_capabilities: Option<ConnectorCapabilityView>,
    /// Last event-time watermark display, or "-" when not available.
    pub last_watermark_display: String,
    /// Last committed source offset bytes, hex-encoded, or "-" when not available.
    pub last_source_offset_display: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorView {
    /// Executor id.
    pub executor_id: String,
    /// Executor lifecycle state.
    pub state: String,
    /// Advertised slots.
    pub slots: usize,
    /// Host or pod name.
    pub host: String,
    /// Running task ids.
    pub running_tasks: Vec<String>,
    /// Last deterministic scheduler heartbeat tick.
    pub last_heartbeat_tick: u64,
    /// Current lease generation.
    pub lease_generation: u64,
    /// Memory used in bytes as reported by the last heartbeat, if any.
    pub memory_used_bytes: Option<u64>,
    /// Memory limit in bytes as reported by the last heartbeat, if any.
    pub memory_limit_bytes: Option<u64>,
    /// Active task count as reported by the last heartbeat, if any.
    pub active_task_count: Option<u32>,
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Response for `GET /api/v1/executors/{executor_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorDetailResponse {
    pub executor: ExecutorView,
}

/// Response for `GET /api/v1/jobs/{job_id}/checkpoints` HTML rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(dead_code)]
pub struct CheckpointsView {
    pub job_id: String,
    pub epochs: Vec<u64>,
    pub latest_epoch: Option<u64>,
}

#[derive(Template)]
#[template(path = "jobs.html")]
pub(crate) struct JobsTemplate {
    pub(crate) jobs: Vec<JobSummaryView>,
    pub(crate) executors: Vec<ExecutorView>,
    pub(crate) bearer_token: Option<String>,
}

impl JobsTemplate {
    fn running_tasks(&self) -> usize {
        self.jobs.iter().map(|job| job.running_task_count).sum()
    }

    fn failed_tasks(&self) -> usize {
        self.jobs.iter().map(|job| job.failed_task_count).sum()
    }
}

#[derive(Template)]
#[template(path = "job.html")]
pub(crate) struct JobTemplate {
    pub(crate) job: JobDetailView,
    pub(crate) executors: Vec<ExecutorView>,
    pub(crate) bearer_token: Option<String>,
}

#[derive(Template)]
#[template(path = "executor.html")]
pub(crate) struct ExecutorTemplate {
    pub(crate) executor: ExecutorView,
    pub(crate) bearer_token: Option<String>,
}

#[derive(Template)]
#[template(path = "checkpoints.html")]
pub(crate) struct CheckpointsTemplate {
    pub(crate) job_id: String,
    pub(crate) epochs: Vec<u64>,
    pub(crate) latest_epoch: Option<u64>,
    pub(crate) bearer_token: Option<String>,
}

impl CheckpointsTemplate {
    fn is_latest(&self, epoch: &u64) -> bool {
        self.latest_epoch.as_ref() == Some(epoch)
    }
}

#[derive(Template)]
#[template(path = "submit.html")]
pub(crate) struct SubmitTemplate {
    pub(crate) bearer_token: Option<String>,
}

#[derive(Template)]
#[template(path = "health.html")]
pub(crate) struct HealthTemplate {
    pub(crate) executors: Vec<ExecutorView>,
    pub(crate) jobs: Vec<JobSummaryView>,
    pub(crate) bearer_token: Option<String>,
}

impl HealthTemplate {
    fn healthy_executors(&self) -> usize {
        self.executors
            .iter()
            .filter(|e| e.state == "healthy" || e.state == "active")
            .count()
    }
    fn lost_executors(&self) -> usize {
        self.executors.iter().filter(|e| e.state == "lost").count()
    }
    fn memory_used_pct(&self) -> f64 {
        let used: u64 = self
            .executors
            .iter()
            .filter_map(|e| e.memory_used_bytes)
            .sum();
        let limit: u64 = self
            .executors
            .iter()
            .filter_map(|e| e.memory_limit_bytes)
            .sum();
        if limit > 0 {
            used as f64 / limit as f64 * 100.0
        } else {
            0.0
        }
    }
    fn memory_used_pct_int(&self) -> u64 {
        self.memory_used_pct() as u64
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct GlobalMetricsView {
    pub tasks_submitted: u64,
    pub tasks_succeeded: u64,
    pub tasks_failed: u64,
    pub executor_lost: u64,
    pub shuffle_bytes_written: u64,
    pub job_queue_depth: u64,
    pub spill_bytes_total: u64,
    pub spill_files_total: u64,
    pub watermark_entry_count: usize,
    pub state_key_entry_count: usize,
}

#[derive(Template)]
#[template(path = "metrics.html")]
pub(crate) struct MetricsTemplate {
    pub(crate) scheduler: SchedulerMetrics,
    pub(crate) stability: StabilityMetrics,
    pub(crate) jobs_count: usize,
    pub(crate) executors_count: usize,
    pub(crate) avg_duration_ms: u64,
    pub(crate) global: GlobalMetricsView,
    pub(crate) bearer_token: Option<String>,
}

#[derive(Template)]
#[template(path = "job_diagnose.html")]
pub(crate) struct JobDiagnoseTemplate {
    pub(crate) job_id: String,
    pub(crate) report_json: String,
    pub(crate) bearer_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobHistoryView {
    pub job_id: String,
    pub job_kind: String,
    pub final_state: String,
    pub completed_at_ms: u64,
    pub stage_count: usize,
    pub task_count: usize,
    pub succeeded_task_count: u32,
    pub failed_task_count: u32,
    pub cpu_nanos: u64,
    pub memory_peak_task_bytes: u64,
    pub namespace_id: Option<String>,
    pub priority: u8,
}

impl JobHistoryView {
    pub(crate) fn from_record(r: &JobHistoryRecord) -> Self {
        Self {
            job_id: r.job_id.clone(),
            job_kind: r.job_kind.clone(),
            final_state: r.final_state.clone(),
            completed_at_ms: r.completed_at_ms,
            stage_count: r.stage_count,
            task_count: r.task_count,
            succeeded_task_count: r.succeeded_task_count,
            failed_task_count: r.failed_task_count,
            cpu_nanos: r.cpu_nanos,
            memory_peak_task_bytes: r.memory_peak_task_bytes,
            namespace_id: r.namespace_id.clone(),
            priority: r.priority,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JobHistoryListResponse {
    pub records: Vec<JobHistoryView>,
    /// Total archived records (before pagination).
    pub total: usize,
    /// Page size applied.
    pub limit: usize,
    /// Page offset applied.
    pub offset: usize,
}

#[derive(Template)]
#[template(path = "history.html")]
pub(crate) struct HistoryTemplate {
    pub(crate) records: Vec<JobHistoryView>,
    pub(crate) total: usize,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) bearer_token: Option<String>,
}

impl HistoryTemplate {
    /// True when more archived records exist beyond this page.
    fn has_more(&self) -> bool {
        self.offset + self.records.len() < self.total
    }

    /// Offset for the next page link.
    fn next_offset(&self) -> usize {
        self.offset + self.limit
    }
}

#[derive(Template)]
#[template(path = "history_detail.html")]
pub(crate) struct HistoryDetailTemplate {
    pub(crate) record: JobHistoryView,
    pub(crate) bearer_token: Option<String>,
}

#[derive(Serialize)]
pub struct SqlQueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub error: Option<String>,
    pub row_count: usize,
    pub elapsed_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct SqlQueryRequest {
    pub query: String,
}

#[derive(Debug, Deserialize)]
pub struct JobsFilter {
    pub state: Option<String>,
    pub kind: Option<String>,
}

impl JobsFilter {
    pub(crate) fn has_any(&self) -> bool {
        self.state.is_some() || self.kind.is_some()
    }
}

/// Page window for list endpoints. `limit` is clamped to `1..=MAX_PAGE_LIMIT`
/// and defaults to `DEFAULT_PAGE_LIMIT`; `offset` defaults to 0.
#[derive(Debug, Default, Deserialize)]
pub struct Pagination {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Default page size when the caller does not specify `limit`.
const DEFAULT_PAGE_LIMIT: usize = 100;
/// Hard cap on page size so a single request can't force a huge serialization.
const MAX_PAGE_LIMIT: usize = 1000;

impl Pagination {
    /// Resolve `(limit, offset)` applying defaults and the hard cap.
    pub(crate) fn resolved(&self) -> (usize, usize) {
        let limit = self
            .limit
            .unwrap_or(DEFAULT_PAGE_LIMIT)
            .clamp(1, MAX_PAGE_LIMIT);
        (limit, self.offset.unwrap_or(0))
    }
}
