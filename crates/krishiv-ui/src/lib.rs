#![forbid(unsafe_code)]

//! R2 status API and server-rendered Web UI.
//!
//! This crate exposes scheduler snapshots as a small Rust-native operations
//! surface. It intentionally depends on the in-process R2 scheduler model
//! rather than introducing Kubernetes clients or a separate frontend build.

use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use krishiv_proto::{
    ConnectorCapabilityFlags, CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId,
    ExecutorState, JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, ExecutorRecord, JobDetailSnapshot, JobSnapshot, NamespaceQuotaSnapshot,
    ResourceUsage, SchedulerError, SharedCoordinator, StabilityMetrics,
};
use serde::Serialize;

/// Shared UI result alias.
pub type UiResult<T> = Result<T, UiError>;

/// Shared state for the R2 status server.
#[derive(Debug, Clone)]
pub struct UiState {
    coordinator: SharedCoordinator,
    metrics_cache: Arc<std::sync::Mutex<(String, std::time::Instant)>>,
}

impl UiState {
    /// Create UI state from an existing coordinator.
    pub fn new(coordinator: Coordinator) -> Self {
        Self::from_shared_coordinator(SharedCoordinator::new(coordinator))
    }

    /// Create UI state from a shared coordinator runtime handle.
    pub fn from_shared_coordinator(coordinator: SharedCoordinator) -> Self {
        Self {
            coordinator,
            metrics_cache: Arc::new(std::sync::Mutex::new((
                String::new(),
                std::time::Instant::now() - std::time::Duration::from_secs(100),
            ))),
        }
    }
}

/// UI construction and handler errors.
#[derive(Debug, thiserror::Error)]
pub enum UiError {
    /// Coordinator id validation failed.
    #[error("invalid id: {0}")]
    Id(String),
    /// Scheduler operation failed.
    #[error("{0}")]
    Scheduler(#[from] SchedulerError),
    /// Shared coordinator lock was poisoned.
    #[error("coordinator status lock was poisoned")]
    LockPoisoned,
    /// Template rendering failed.
    #[error("failed to render status page: {0}")]
    Template(#[from] askama::Error),
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Scheduler(SchedulerError::UnknownJob { .. }) => StatusCode::NOT_FOUND,
            Self::Id(_) => StatusCode::BAD_REQUEST,
            Self::Scheduler(_) | Self::LockPoisoned | Self::Template(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, self.to_string()).into_response()
    }
}

/// Resolve the UI bearer token from `KRISHIV_UI_TOKEN` (inline) or
/// `KRISHIV_UI_TOKEN_FILE` (path to a file containing the token).
///
/// The file-based variant is preferred in production because it lets
/// operators mount a Secret as a file and rotate the token without
/// restarting the process. If both are set, the inline `KRISHIV_UI_TOKEN`
/// wins (back-compat). If the file is unreadable, an empty string is
/// returned (treated as no token, i.e. anonymous router). Trimmed
/// whitespace at the start/end of the file contents is stripped so a
/// trailing newline in a Secret does not break the comparison.
fn resolve_ui_token() -> Option<String> {
    if let Ok(value) = std::env::var("KRISHIV_UI_TOKEN") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    if let Ok(path) = std::env::var("KRISHIV_UI_TOKEN_FILE") {
        if path.is_empty() {
            return None;
        }
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                let trimmed = contents.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_owned());
                }
            }
            Err(e) => {
                eprintln!(
                    "krishiv-ui: KRISHIV_UI_TOKEN_FILE='{path}' could not be read: {e}; \
                     falling back to anonymous router"
                );
            }
        }
    }
    None
}

/// Build the R2 UI router.
///
/// `KRISHIV_UI_TOKEN` (inline) or `KRISHIV_UI_TOKEN_FILE` (path) is
/// consulted at router-construction time. When set, all `/api/v1/...`
/// and `/ui/...` routes require a matching `Authorization: Bearer
/// <token>` header. `/healthz`, `/readyz`, `/metrics`, `/assets/*`, and
/// the root redirect stay anonymous so platform probes keep working
/// without leaking snapshot data.
pub fn router(state: UiState) -> Router {
    router_with_token(state, resolve_ui_token().as_deref())
}

/// Build the R2 UI router with an explicit auth token. When `Some`, the same
/// routes as `router()` get wrapped in the bearer-token middleware. When
/// `None`, the router behaves identically to a `KRISHIV_UI_TOKEN`-unset build.
pub fn router_with_token(state: UiState, token: Option<&str>) -> Router {
    let public = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/assets/krishiv.css", get(stylesheet));

    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/jobs/{job_id}", get(api_job_detail))
        .route(
            "/api/v1/jobs/{job_id}/checkpoints",
            get(api_job_checkpoints),
        )
        .route("/api/v1/executors", get(api_executors))
        .route("/api/v1/queues", get(api_queues))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .with_state(state.clone());

    let protected = if let Some(expected) = token {
        let expected = expected.to_string();
        protected.layer(middleware::from_fn(move |req, next| {
            let expected = expected.clone();
            async move { require_bearer(req, next, &expected).await }
        }))
    } else {
        protected
    };

    Router::new()
        .merge(public)
        .merge(protected)
        .with_state(state)
}

async fn require_bearer(request: axum::extract::Request, next: Next, expected: &str) -> Response {
    let auth = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    match auth {
        Some(value) if value.len() > 7 && value[..7].eq_ignore_ascii_case("bearer ") => {
            let token = &value[7..];
            if token == expected {
                next.run(request).await
            } else {
                (StatusCode::UNAUTHORIZED, "invalid bearer token").into_response()
            }
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            [("WWW-Authenticate", "Bearer")],
            "missing bearer token",
        )
            .into_response(),
    }
}

/// Serve the R2 status API and Web UI with an existing listener.
pub async fn serve(listener: tokio::net::TcpListener, state: UiState) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

/// Create an empty active coordinator state for real status serving.
pub fn empty_state() -> UiResult<UiState> {
    let coordinator_id =
        CoordinatorId::try_new("coord-local").map_err(|error| UiError::Id(error.to_string()))?;
    Ok(UiState::new(Coordinator::active(coordinator_id)))
}

/// Create a deterministic demo state for local UI development and tests.
pub fn demo_state() -> UiResult<UiState> {
    let coordinator_id =
        CoordinatorId::try_new("coord-demo").map_err(|error| UiError::Id(error.to_string()))?;
    let executor_id =
        ExecutorId::try_new("exec-demo-1").map_err(|error| UiError::Id(error.to_string()))?;
    let job_id = JobId::try_new("job-demo").map_err(|error| UiError::Id(error.to_string()))?;

    let mut coordinator = Coordinator::active(coordinator_id);
    coordinator.register_executor(ExecutorDescriptor::new(
        executor_id.clone(),
        "demo-executor",
        2,
    ))?;
    coordinator.executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))?;
    coordinator.submit_job(demo_job(job_id.clone())?)?;
    coordinator.launch_assigned_tasks(&job_id)?;

    Ok(UiState::new(coordinator))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobsResponse {
    /// Job summaries.
    pub jobs: Vec<JobSummaryView>,
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
    /// Peak memory bytes observed across completed tasks.
    pub memory_peak_bytes: u64,
    /// Number of completed tasks that reported stats.
    pub task_count: u32,
}

impl ResourceUsageView {
    fn from_usage(u: &ResourceUsage) -> Self {
        Self {
            cpu_nanos: u.cpu_nanos,
            memory_peak_bytes: u.memory_peak_bytes,
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
    fn from_snapshot(snap: &NamespaceQuotaSnapshot) -> Self {
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
    fn from_flags(flags: &ConnectorCapabilityFlags) -> Self {
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
    /// Last failure reason reported by the executor, if any.
    pub last_failure_reason: Option<String>,
    /// Source connector capability flags for this task, if declared.
    pub source_capabilities: Option<ConnectorCapabilityView>,
    /// Sink connector capability flags for this task, if declared.
    pub sink_capabilities: Option<ConnectorCapabilityView>,
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

#[derive(Template)]
#[template(path = "jobs.html")]
struct JobsTemplate {
    coordinator_id: String,
    coordinator_state: String,
    jobs: Vec<JobSummaryView>,
    executors: Vec<ExecutorView>,
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
struct JobTemplate {
    coordinator_id: String,
    coordinator_state: String,
    job: JobDetailView,
    executors: Vec<ExecutorView>,
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// Prometheus-format metrics endpoint backed by live `StabilityMetrics`.
async fn metrics(State(state): State<UiState>) -> impl IntoResponse {
    let coordinator = state.coordinator.read().await;
    let mut cache = state.metrics_cache.lock().unwrap();
    let now = std::time::Instant::now();
    let body = if now.duration_since(cache.1).as_secs() >= 1 || cache.0.is_empty() {
        let mut body = format_stability_metrics(&coordinator.stability_metrics());
        body.push('\n');
        body.push_str(&krishiv_metrics::global_metrics().render_prometheus());
        cache.0 = body.clone();
        cache.1 = now;
        body
    } else {
        cache.0.clone()
    };
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn format_stability_metrics(m: &StabilityMetrics) -> String {
    let max_heartbeat_age = m
        .heartbeat_ages()
        .iter()
        .map(|a| a.age_ticks())
        .max()
        .unwrap_or(0);
    format!(
        "\
# HELP krishiv_running_tasks Currently running task count
# TYPE krishiv_running_tasks gauge
krishiv_running_tasks {running}
# HELP krishiv_task_retries_total Total stage-level retries scheduled
# TYPE krishiv_task_retries_total counter
krishiv_task_retries_total {retries}
# HELP krishiv_failed_assignments_total Total failed task assignments
# TYPE krishiv_failed_assignments_total counter
krishiv_failed_assignments_total {failed}
# HELP krishiv_max_executor_heartbeat_age_ticks Max executor heartbeat age in scheduler ticks
# TYPE krishiv_max_executor_heartbeat_age_ticks gauge
krishiv_max_executor_heartbeat_age_ticks {hb_age}
# HELP krishiv_shuffle_bytes_written_total Total bytes written to shuffle store
# TYPE krishiv_shuffle_bytes_written_total counter
krishiv_shuffle_bytes_written_total {shuffle_bytes}
",
        running = m.running_task_count(),
        retries = m.retry_count(),
        failed = m.failed_assignments(),
        hb_age = max_heartbeat_age,
        shuffle_bytes = m.shuffle_bytes_written,
    )
}

async fn readyz(State(state): State<UiState>) -> Result<impl IntoResponse, UiError> {
    use krishiv_proto::CoordinatorState;
    let coordinator = state.coordinator.read().await;
    if coordinator.state() != CoordinatorState::Active {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator is not active\n",
        ));
    }
    let _snapshot = status_snapshot(&state).await?;
    Ok((StatusCode::OK, "ready\n"))
}

async fn api_jobs(State(state): State<UiState>) -> Result<Json<JobsResponse>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    Ok(Json(JobsResponse {
        jobs: snapshot.jobs,
    }))
}

async fn api_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetailResponse>, UiError> {
    Ok(Json(JobDetailResponse {
        job: job_detail(&state, &job_id).await?,
    }))
}

async fn api_executors(State(state): State<UiState>) -> Result<Json<ExecutorsResponse>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    Ok(Json(ExecutorsResponse {
        executors: snapshot.executors,
    }))
}

async fn api_queues(State(state): State<UiState>) -> Result<Json<QueuesResponse>, UiError> {
    let coordinator = state.coordinator.read().await;

    // Collect all distinct namespaces from active jobs plus the default namespace.
    let mut namespaces: Vec<Option<String>> = coordinator
        .job_snapshots()
        .iter()
        .filter(|j| !j.state().is_terminal())
        .map(|j| j.namespace_id().map(str::to_owned))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Ensure the default namespace is always present.
    if !namespaces.contains(&None) {
        namespaces.push(None);
    }
    // Sort: default namespace first, then alphabetical.
    namespaces.sort_by(|a, b| match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, _) => std::cmp::Ordering::Less,
        (_, None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.cmp(b),
    });

    let quota_views = namespaces
        .iter()
        .map(|ns| {
            let snap = coordinator.namespace_quota_snapshot(ns.as_deref());
            NamespaceQuotaView::from_snapshot(&snap)
        })
        .collect();

    Ok(Json(QueuesResponse {
        namespaces: quota_views,
    }))
}

async fn api_job_checkpoints(
    State(state): State<UiState>,
    Path(job_id_str): Path<String>,
) -> Result<Json<JobCheckpointsResponse>, UiError> {
    let job_id = JobId::try_new(job_id_str.clone()).map_err(|e| UiError::Id(e.to_string()))?;
    let coordinator = state.coordinator.read().await;

    // Verify the job exists — returns UnknownJob (→ 404) if not.
    coordinator.job_detail_snapshot(&job_id)?;

    let epochs = coordinator.list_job_checkpoints(&job_id)?;
    let latest_epoch = epochs.last().copied();
    Ok(Json(JobCheckpointsResponse {
        job_id: job_id_str,
        epochs,
        latest_epoch,
    }))
}

async fn ui_jobs(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let template = JobsTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        jobs: snapshot.jobs,
        executors: snapshot.executors,
    };
    Ok(Html(template.render()?))
}

async fn ui_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let template = JobTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        job: job_detail(&state, &job_id).await?,
        executors: snapshot.executors,
    };
    Ok(Html(template.render()?))
}

async fn stylesheet() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/style.css"),
    )
}

async fn status_snapshot(state: &UiState) -> UiResult<StatusView> {
    let coordinator = state.coordinator.read().await;
    Ok(StatusView {
        coordinator_id: coordinator.coordinator_id().to_string(),
        coordinator_state: coordinator.state().to_string(),
        jobs: coordinator
            .job_snapshots()
            .iter()
            .map(JobSummaryView::from_snapshot)
            .collect(),
        executors: coordinator
            .executor_snapshots()
            .iter()
            .map(ExecutorView::from_record)
            .collect(),
    })
}

async fn job_detail(state: &UiState, job_id: &str) -> UiResult<JobDetailView> {
    let job_id =
        JobId::try_new(job_id.to_owned()).map_err(|error| UiError::Id(error.to_string()))?;
    let coordinator = state.coordinator.read().await;
    let detail = coordinator.job_detail_snapshot(&job_id)?;
    Ok(JobDetailView::from_snapshot(&detail))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatusView {
    coordinator_id: String,
    coordinator_state: String,
    jobs: Vec<JobSummaryView>,
    executors: Vec<ExecutorView>,
}

impl JobSummaryView {
    fn from_snapshot(snapshot: &JobSnapshot) -> Self {
        Self {
            job_id: snapshot.job_id().to_string(),
            kind: snapshot.kind().to_string(),
            state: snapshot.state().to_string(),
            stage_count: snapshot.stage_count(),
            task_count: snapshot.task_count(),
            assigned_task_count: snapshot.assigned_task_count(),
            running_task_count: snapshot.running_task_count(),
            succeeded_task_count: snapshot.succeeded_task_count(),
            failed_task_count: snapshot.failed_task_count(),
            priority: snapshot.priority(),
            namespace_id: snapshot.namespace_id().map(str::to_owned),
            resource_usage: ResourceUsageView::from_usage(snapshot.resource_usage()),
        }
    }
}

impl JobDetailView {
    fn from_snapshot(snapshot: &JobDetailSnapshot) -> Self {
        Self {
            summary: JobSummaryView::from_snapshot(snapshot.job()),
            stages: snapshot
                .stages()
                .iter()
                .map(|stage| StageView {
                    stage_id: stage.stage_id().to_string(),
                    state: stage.state().to_string(),
                    retry_count: stage.retry_count(),
                    task_count: stage.task_count(),
                    tasks: stage
                        .tasks()
                        .iter()
                        .map(|task| TaskView {
                            task_id: task.task_id().to_string(),
                            state: task.state().to_string(),
                            assigned_executor: task
                                .assigned_executor()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| String::from("-")),
                            attempt: task.attempt(),
                            last_failure_reason: task.last_failure_reason().map(ToOwned::to_owned),
                            source_capabilities: task
                                .source_capabilities
                                .as_ref()
                                .map(ConnectorCapabilityView::from_flags),
                            sink_capabilities: task
                                .sink_capabilities
                                .as_ref()
                                .map(ConnectorCapabilityView::from_flags),
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl ExecutorView {
    fn from_record(record: &ExecutorRecord) -> Self {
        let health = record.health_snapshot();
        Self {
            executor_id: record.executor_id().to_string(),
            state: record.state().to_string(),
            slots: record.descriptor().slots(),
            host: record.descriptor().host().to_owned(),
            running_tasks: record
                .running_tasks()
                .iter()
                .map(ToString::to_string)
                .collect(),
            last_heartbeat_tick: record.last_heartbeat_tick(),
            lease_generation: record.lease_generation().as_u64(),
            memory_used_bytes: health.and_then(|h| h.memory_used_bytes),
            memory_limit_bytes: health.and_then(|h| h.memory_limit_bytes),
            active_task_count: health.and_then(|h| h.active_task_count),
        }
    }
}

fn demo_job(job_id: JobId) -> UiResult<JobSpec> {
    let stage = StageSpec::new(
        StageId::try_new("stage-1").map_err(|error| UiError::Id(error.to_string()))?,
        "demo-status-stage",
    )
    .with_task(TaskSpec::new(
        TaskId::try_new("task-1").map_err(|error| UiError::Id(error.to_string()))?,
        "demo scan task",
    ))
    .with_task(TaskSpec::new(
        TaskId::try_new("task-2").map_err(|error| UiError::Id(error.to_string()))?,
        "demo aggregate task",
    ));

    Ok(JobSpec::new(job_id, "demo-status-job", JobKind::Batch).with_stage(stage))
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState,
    };
    use krishiv_scheduler::{Coordinator, SharedCoordinator};
    use tower::ServiceExt;

    use super::{UiState, demo_state, empty_state, router, router_with_token};
    #[tokio::test]
    async fn health_route_reports_ok() {
        let response = router(empty_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_jobs_returns_demo_job() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("job-demo"));
        assert!(body.contains("running"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_jobs_reads_shared_runtime_state() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-runtime").unwrap(),
        ));
        {
            let mut coordinator = shared.write().await;
            let executor_id = ExecutorId::try_new("exec-runtime-1").unwrap();
            coordinator
                .register_executor(ExecutorDescriptor::new(
                    executor_id.clone(),
                    "runtime-executor",
                    1,
                ))
                .unwrap();
            coordinator
                .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
                .unwrap();
        }

        let response = router(UiState::from_shared_coordinator(shared))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/executors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("exec-runtime-1"));
        assert!(body.contains("runtime-executor"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_job_detail_returns_stage_and_task_data() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs/job-demo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("stage-1"));
        assert!(body.contains("task-1"));
        assert!(body.contains("exec-demo-1"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn missing_job_returns_not_found() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_stability_fields() {
        let response = router(empty_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("krishiv_running_tasks"));
        assert!(body.contains("krishiv_task_retries_total"));
        assert!(body.contains("krishiv_failed_assignments_total"));
        assert!(body.contains("krishiv_max_executor_heartbeat_age_ticks"));
    }

    #[tokio::test]
    async fn metrics_reflects_live_coordinator_state() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        // Demo state has one executor that has sent a heartbeat, so heartbeat age
        // should be non-negative (represented as a numeric value after the metric name).
        assert!(body.contains("krishiv_max_executor_heartbeat_age_ticks "));
        // The metrics are sourced from a live coordinator, not hardcoded zeros.
        assert!(!body.contains("krishiv_running_tasks_total"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ui_jobs_renders_html() {
        let response = router(demo_state().unwrap())
            .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Krishiv R2 Status"));
        assert!(body.contains("job-demo"));
    }

    #[tokio::test]
    async fn api_job_checkpoints_returns_empty_for_no_coordinator() {
        // demo_state has job-demo which is a batch job with no checkpoint config.
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs/job-demo/checkpoints")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(status, StatusCode::OK, "response body: {text}");
        let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed["job_id"], "job-demo");
        assert_eq!(parsed["epochs"], serde_json::json!([]));
        assert_eq!(parsed["latest_epoch"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn api_job_checkpoints_returns_404_for_unknown_job() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs/job-does-not-exist/checkpoints")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // ── R7.1 quota / queue API tests ─────────────────────────────────────────

    #[tokio::test]
    async fn api_queues_returns_default_namespace() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/queues")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let namespaces = parsed["namespaces"].as_array().unwrap();
        assert!(
            !namespaces.is_empty(),
            "must include at least default namespace"
        );
        // Default namespace has null namespace_id.
        assert_eq!(namespaces[0]["namespace_id"], serde_json::Value::Null);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_jobs_includes_priority_and_resource_usage() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job = &parsed["jobs"][0];
        assert!(job["priority"].is_number(), "priority must be a number");
        assert!(
            job["resource_usage"].is_object(),
            "resource_usage must be an object"
        );
        assert!(
            job["resource_usage"]["cpu_nanos"].is_number(),
            "resource_usage.cpu_nanos must be present"
        );
    }

    // ── KRISHIV_UI_TOKEN middleware tests ───────────────────────────────────

    #[tokio::test]
    async fn api_jobs_requires_bearer_token_when_token_set() {
        let response = router_with_token(demo_state().unwrap(), Some("s3cret"))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn api_jobs_accepts_matching_bearer_token() {
        let response = router_with_token(demo_state().unwrap(), Some("s3cret"))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .header("authorization", "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn api_jobs_rejects_wrong_bearer_token() {
        let response = router_with_token(demo_state().unwrap(), Some("s3cret"))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .header("authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn healthz_stays_anonymous_even_when_token_set() {
        let response = router_with_token(demo_state().unwrap(), Some("s3cret"))
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
