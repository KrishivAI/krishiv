#![forbid(unsafe_code)]

//! R2 status API and server-rendered Web UI.
//!
//! This crate exposes scheduler snapshots as a small Rust-native operations
//! surface. It intentionally depends on the in-process R2 scheduler model
//! rather than introducing Kubernetes clients or a separate frontend build.

use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use krishiv_proto::{
    ConnectorCapabilityFlags, CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId,
    ExecutorState, JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, ExecutorRecord, JobDetailSnapshot, JobSnapshot, NamespaceQuotaSnapshot,
    ResourceUsage, SchedulerError, SharedCoordinator, StabilityMetrics,
};
use krishiv_scheduler::metrics::SchedulerMetrics;
use serde::{Deserialize, Serialize};

/// Shared UI result alias.
pub type UiResult<T> = Result<T, UiError>;

/// Shared state for the R2 status server.
#[derive(Clone)]
pub struct UiState {
    coordinator: SharedCoordinator,
    metrics_cache: Arc<std::sync::Mutex<(String, std::time::Instant)>>,
    sql: Option<Arc<krishiv_sql::SqlEngine>>,
}

impl std::fmt::Debug for UiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UiState")
            .field("coordinator", &self.coordinator)
            .field("has_sql", &self.sql.is_some())
            .finish_non_exhaustive()
    }
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
            sql: None,
        }
    }

    /// Attach a SQL engine to enable the query editor.
    pub fn with_sql_engine(mut self, engine: krishiv_sql::SqlEngine) -> Self {
        self.sql = Some(Arc::new(engine));
        self
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
    /// SQL execution failed.
    #[error("sql error: {0}")]
    Sql(String),
    /// Shared coordinator lock was poisoned.
    #[error("coordinator status lock was poisoned")]
    LockPoisoned,
    /// Template rendering failed.
    #[error("failed to render status page: {0}")]
    Template(#[from] askama::Error),
}

impl From<krishiv_sql::SqlError> for UiError {
    fn from(e: krishiv_sql::SqlError) -> Self {
        UiError::Sql(e.to_string())
    }
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Scheduler(SchedulerError::UnknownJob { .. }) => StatusCode::NOT_FOUND,
            Self::Id(_) => StatusCode::BAD_REQUEST,
            Self::Scheduler(_) | Self::LockPoisoned | Self::Template(_) | Self::Sql(_) => {
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
                if krishiv_common::profile_requires_authenticated_ui(
                    krishiv_common::resolve_durability_profile(),
                ) {
                    eprintln!(
                        "krishiv-ui: KRISHIV_UI_TOKEN_FILE='{path}' could not be read: {e}; \
                         denying all protected routes (production fail-closed)"
                    );
                    return Some(String::new());
                }
                eprintln!(
                    "krishiv-ui: KRISHIV_UI_TOKEN_FILE='{path}' could not be read: {e}; \
                     falling back to anonymous router"
                );
            }
        }
    }
    if krishiv_common::profile_requires_authenticated_ui(
        krishiv_common::resolve_durability_profile(),
    ) {
        eprintln!("krishiv-ui: no UI token configured; denying all protected routes (production fail-closed)");
        return Some(String::new());
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
        .route("/api/v1/executors/{executor_id}", get(api_executor_detail))
        .route("/api/v1/queues", get(api_queues))
        .route("/api/v1/sql", post(api_sql_execute))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .route("/ui/jobs/{job_id}/checkpoints", get(ui_job_checkpoints_page))
        .route("/ui/executors/{executor_id}", get(ui_executor_detail))
        .route("/ui/submit", get(ui_submit))
        .route("/ui/health", get(ui_health))
        .route("/ui/metrics", get(ui_metrics))
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

/// Build the UI-specific routes (jobs, executors, SQL editor, health dashboard)
/// for embedding inside the coordinator HTTP server.
///
/// Skips `/healthz`, `/readyz`, and `/metrics` — the coordinator already serves
/// those. Includes `/assets/*`, `/`, `/ui*`, and `/api/v1/*` routes.
pub fn embedded_router(state: UiState) -> Router {
    let public = Router::new().route("/assets/krishiv.css", get(stylesheet));

    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route("/api/v1/jobs/{job_id}", get(api_job_detail))
        .route(
            "/api/v1/jobs/{job_id}/checkpoints",
            get(api_job_checkpoints),
        )
        .route("/api/v1/executors/{executor_id}", get(api_executor_detail))
        .route("/api/v1/queues", get(api_queues))
        .route("/api/v1/sql", post(api_sql_execute))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .route("/ui/jobs/{job_id}/checkpoints", get(ui_job_checkpoints_page))
        .route("/ui/executors/{executor_id}", get(ui_executor_detail))
        .route("/ui/submit", get(ui_submit))
        .route("/ui/health", get(ui_health))
        .route("/ui/metrics", get(ui_metrics));

    let protected = if let Some(expected) = resolve_ui_token().as_deref() {
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
    if expected.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            "authentication not configured",
        )
            .into_response();
    }
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
    /// Peak memory bytes observed across completed tasks (max over tasks, not sum).
    pub memory_peak_task_bytes: u64,
    /// Sum of memory bytes across all completed tasks.
    pub memory_total_bytes: u64,
    /// Number of completed tasks that reported stats.
    pub task_count: u32,
}

impl ResourceUsageView {
    fn from_usage(u: &ResourceUsage) -> Self {
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

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Response for `GET /api/v1/executors/{executor_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutorDetailResponse {
    pub executor: ExecutorView,
}

/// Response for `GET /api/v1/jobs/{job_id}/checkpoints` HTML rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckpointsView {
    pub job_id: String,
    pub epochs: Vec<u64>,
    pub latest_epoch: Option<u64>,
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

#[derive(Template)]
#[template(path = "executor.html")]
struct ExecutorTemplate {
    coordinator_id: String,
    coordinator_state: String,
    executor: ExecutorView,
}

#[derive(Template)]
#[template(path = "checkpoints.html")]
struct CheckpointsTemplate {
    coordinator_id: String,
    coordinator_state: String,
    job_id: String,
    epochs: Vec<u64>,
    latest_epoch: Option<u64>,
}

impl CheckpointsTemplate {
    fn is_latest(&self, epoch: &u64) -> bool {
        self.latest_epoch.as_ref() == Some(epoch)
    }
}

#[derive(Template)]
#[template(path = "submit.html")]
struct SubmitTemplate {
    coordinator_id: String,
    coordinator_state: String,
}

#[derive(Template)]
#[template(path = "health.html")]
struct HealthTemplate {
    coordinator_id: String,
    coordinator_state: String,
    executors: Vec<ExecutorView>,
    jobs: Vec<JobSummaryView>,
}

impl HealthTemplate {
    fn healthy_executors(&self) -> usize {
        self.executors.iter().filter(|e| e.state == "healthy" || e.state == "active").count()
    }
    fn lost_executors(&self) -> usize {
        self.executors.iter().filter(|e| e.state == "lost").count()
    }
    fn memory_used_pct(&self) -> f64 {
        let used: u64 = self.executors.iter().filter_map(|e| e.memory_used_bytes).sum();
        let limit: u64 = self.executors.iter().filter_map(|e| e.memory_limit_bytes).sum();
        if limit > 0 { used as f64 / limit as f64 * 100.0 } else { 0.0 }
    }
    fn memory_used_pct_int(&self) -> u64 {
        self.memory_used_pct() as u64
    }
}

#[derive(Template)]
#[template(path = "metrics.html")]
struct MetricsTemplate {
    scheduler: SchedulerMetrics,
    stability: StabilityMetrics,
    jobs_count: usize,
    executors_count: usize,
    avg_duration_ms: u64,
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
    fn has_any(&self) -> bool {
        self.state.is_some() || self.kind.is_some()
    }
}

async fn healthz() -> &'static str {
    "ok\n"
}

/// Prometheus-format metrics endpoint backed by live `StabilityMetrics`.
async fn metrics(State(state): State<UiState>) -> impl IntoResponse {
    let coordinator = state.coordinator.read().await;
    let mut cache = state.metrics_cache.lock().unwrap_or_else(|e| e.into_inner());
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

async fn ui_submit(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let snapshot = status_snapshot_inner(&coordinator);
    let template = SubmitTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
    };
    Ok(Html(template.render()?))
}

async fn ui_health(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let snapshot = status_snapshot_inner(&coordinator);
    let template = HealthTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        executors: coordinator
            .executor_snapshots()
            .iter()
            .map(ExecutorView::from_record)
            .collect(),
        jobs: coordinator
            .job_snapshots()
            .iter()
            .map(JobSummaryView::from_snapshot)
            .collect(),
    };
    Ok(Html(template.render()?))
}

async fn ui_metrics(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let snapshot = status_snapshot_inner(&coordinator);
    let scheduler = krishiv_scheduler::metrics::scheduler_metrics();
    let stability = coordinator.stability_metrics();
    let avg = if scheduler.tasks_assigned_total > 0 {
        scheduler.task_assignment_duration_ms_sum / scheduler.tasks_assigned_total
    } else {
        0
    };
    let template = MetricsTemplate {
        scheduler,
        stability,
        jobs_count: snapshot.jobs.len(),
        executors_count: snapshot.executors.len(),
        avg_duration_ms: avg,
    };
    Ok(Html(template.render()?))
}

async fn api_sql_execute(
    State(state): State<UiState>,
    Json(req): Json<SqlQueryRequest>,
) -> Json<SqlQueryResponse> {
    let engine = match &state.sql {
        Some(e) => e.clone(),
        None => {
            return Json(SqlQueryResponse {
                columns: vec![],
                rows: vec![],
                error: Some("SQL engine not available. Start the UI with SQL support enabled.".to_string()),
                row_count: 0,
                elapsed_ms: 0,
            });
        }
    };

    let start = std::time::Instant::now();
    match engine.sql(&req.query).await {
        Ok(df) => {
            match df.collect().await {
                Ok(batches) => {
                    let (columns, rows) = extract_columns_and_rows(&batches);
                    let elapsed = start.elapsed().as_millis() as u64;
                    let row_count = rows.len();
                    Json(SqlQueryResponse {
                        columns,
                        rows,
                        error: None,
                        row_count,
                        elapsed_ms: elapsed,
                    })
                }
                Err(e) => {
                    Json(SqlQueryResponse {
                        columns: vec![],
                        rows: vec![],
                        error: Some(format!("execution error: {e}")),
                        row_count: 0,
                        elapsed_ms: start.elapsed().as_millis() as u64,
                    })
                }
            }
        }
        Err(e) => {
            Json(SqlQueryResponse {
                columns: vec![],
                rows: vec![],
                error: Some(format!("sql error: {e}")),
                row_count: 0,
                elapsed_ms: start.elapsed().as_millis() as u64,
            })
        }
    }
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

async fn ui_jobs(
    State(state): State<UiState>,
    filter: Query<JobsFilter>,
) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let jobs = if filter.has_any() {
        filter_jobs(snapshot.jobs, &filter)
    } else {
        snapshot.jobs
    };
    let template = JobsTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        jobs,
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

async fn api_executor_detail(
    State(state): State<UiState>,
    Path(executor_id): Path<String>,
) -> Result<Json<ExecutorDetailResponse>, UiError> {
    let snapshot = api_executor_detail_inner(&state, &executor_id).await?;
    Ok(Json(ExecutorDetailResponse { executor: snapshot }))
}

async fn ui_executor_detail(
    State(state): State<UiState>,
    Path(executor_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state).await?;
    let executor = api_executor_detail_inner(&state, &executor_id).await?;
    let template = ExecutorTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        executor,
    };
    Ok(Html(template.render()?))
}

async fn ui_job_checkpoints_page(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Html<String>, UiError> {
    let coordinator = state.coordinator.read().await;
    let snapshot = status_snapshot_inner(&coordinator);
    let jid = JobId::try_new(job_id.clone()).map_err(|e| UiError::Id(e.to_string()))?;
    coordinator.job_detail_snapshot(&jid)?;
    let epochs = coordinator.list_job_checkpoints(&jid)?;
    let latest_epoch = epochs.last().copied();
    let template = CheckpointsTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        job_id: job_id.clone(),
        epochs,
        latest_epoch,
    };
    Ok(Html(template.render()?))
}

async fn api_executor_detail_inner(state: &UiState, executor_id: &str) -> UiResult<ExecutorView> {
    let coordinator = state.coordinator.read().await;
    let executors = coordinator.executor_snapshots();
    let eid = krishiv_proto::ExecutorId::try_new(executor_id.to_owned())
        .map_err(|e| UiError::Id(e.to_string()))?;
    executors
        .iter()
        .find(|e| e.executor_id() == &eid)
        .map(ExecutorView::from_record)
        .ok_or_else(|| {
            UiError::Scheduler(krishiv_scheduler::SchedulerError::UnknownExecutor {
                executor_id: eid.clone(),
            })
        })
}

async fn stylesheet() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/style.css"),
    )
}

async fn status_snapshot(state: &UiState) -> UiResult<StatusView> {
    let coordinator = state.coordinator.read().await;
    Ok(status_snapshot_inner(&coordinator))
}

fn status_snapshot_inner(coordinator: &krishiv_scheduler::Coordinator) -> StatusView {
    StatusView {
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
    }
}

fn filter_jobs(jobs: Vec<JobSummaryView>, filter: &JobsFilter) -> Vec<JobSummaryView> {
    jobs.into_iter()
        .filter(|j| {
            if let Some(ref state) = filter.state {
                if !j.state.eq_ignore_ascii_case(state) {
                    return false;
                }
            }
            if let Some(ref kind) = filter.kind {
                if !j.kind.eq_ignore_ascii_case(kind) {
                    return false;
                }
            }
            true
        })
        .collect()
}

fn extract_columns_and_rows(batches: &[arrow::record_batch::RecordBatch]) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }
    let columns: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let val = if array.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    scalar_array_to_json(array.as_ref(), row_idx)
                };
                row.push(val);
            }
            rows.push(row);
        }
    }
    (columns, rows)
}

fn scalar_array_to_json(array: &dyn arrow::array::Array, idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::*;
    match array.data_type() {
        DataType::Int8 => array.as_any().downcast_ref::<Int8Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::Int16 => array.as_any().downcast_ref::<Int16Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::UInt8 => array.as_any().downcast_ref::<UInt8Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::UInt16 => array.as_any().downcast_ref::<UInt16Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array.as_any().downcast_ref::<UInt32Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array.as_any().downcast_ref::<Float32Array>().map(|a| serde_json::Value::Number(serde_json::Number::from_f64(a.value(idx) as f64).unwrap_or(serde_json::Number::from(0)))).unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().map(|a| serde_json::Value::Number(serde_json::Number::from_f64(a.value(idx)).unwrap_or(serde_json::Number::from(0)))).unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().map(|a| serde_json::Value::Bool(a.value(idx))).unwrap_or(serde_json::Value::Null),
        DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().map(|a| serde_json::Value::String(a.value(idx).to_string())).unwrap_or(serde_json::Value::Null),
        DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().map(|a| serde_json::Value::String(a.value(idx).to_string())).unwrap_or(serde_json::Value::Null),
        DataType::Timestamp(_, _) => {
            let v = array.as_any().downcast_ref::<TimestampSecondArray>().map(|a| serde_json::Value::Number(a.value(idx).into())).unwrap_or(serde_json::Value::Null);
            v
        }
        _ => serde_json::Value::String(format!("{:?}", array.data_type())),
    }
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
                        .map(|task| {
                            let wm = task.last_watermark_ms();
                            let off = task.last_source_offset();
                            TaskView {
                                task_id: task.task_id().to_string(),
                                state: task.state().to_string(),
                                assigned_executor: task
                                    .assigned_executor()
                                    .map(ToString::to_string)
                                    .unwrap_or_else(|| String::from("-")),
                                attempt: task.attempt(),
                                failure_count: task.failure_count(),
                                failure_reason_display: task
                                    .last_failure_reason()
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_default(),
                                source_capabilities: task
                                    .source_capabilities
                                    .as_ref()
                                    .map(ConnectorCapabilityView::from_flags),
                                sink_capabilities: task
                                    .sink_capabilities
                                    .as_ref()
                                    .map(ConnectorCapabilityView::from_flags),
                                last_watermark_display: match wm {
                                    Some(ms) => ms.to_string(),
                                    None => String::from("-"),
                                },
                                last_source_offset_display: match off {
                                    Some(b) => hex_encode(b),
                                    None => String::from("-"),
                                },
                            }
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

    /// Regression (Wave 1 — Lock Poisoning Recovery): if the `metrics_cache`
    /// mutex is poisoned by a panicking holder, the `/metrics` handler must
    /// recover via `.lock().unwrap_or_else(|e| e.into_inner())` and continue
    /// serving rather than panicking/cascading the poison to every caller.
    #[tokio::test]
    async fn metrics_handler_recovers_from_poisoned_cache_lock() {
        let state = empty_state().unwrap();
        let cache = state.metrics_cache.clone();

        let poison_result = std::thread::spawn(move || {
            let _guard = cache.lock().unwrap();
            panic!("intentional poison for metrics_cache mutex");
        })
        .join();
        assert!(poison_result.is_err(), "spawned thread must have panicked");
        assert!(
            state.metrics_cache.is_poisoned(),
            "metrics_cache mutex must be poisoned after the panicking holder"
        );

        let response = router(state)
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

        assert_eq!(
            status,
            StatusCode::OK,
            "metrics endpoint must recover from a poisoned cache lock, got body: {body}"
        );
        assert!(body.contains("krishiv_running_tasks"));
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
        assert!(body.contains("Krishiv"));
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

    #[tokio::test]
    async fn api_jobs_rejects_all_requests_when_empty_token() {
        // Empty expected token simulates fail-closed when auth is required but
        // no token is configured in production.
        let response = router_with_token(demo_state().unwrap(), Some(""))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .header("authorization", "Bearer anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn api_jobs_rejects_empty_token_even_with_matching_empty_bearer() {
        let response = router_with_token(demo_state().unwrap(), Some(""))
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .header("authorization", "Bearer ")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
