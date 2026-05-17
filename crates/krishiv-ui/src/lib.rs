#![forbid(unsafe_code)]

//! R2 status API and server-rendered Web UI.
//!
//! This crate exposes scheduler snapshots as a small Rust-native operations
//! surface. It intentionally depends on the in-process R2 scheduler model
//! rather than introducing Kubernetes clients or a separate frontend build.

use std::error::Error;
use std::fmt;

use askama::Template;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::{Json, Router};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
    JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use krishiv_scheduler::{
    Coordinator, ExecutorRecord, JobDetailSnapshot, JobSnapshot, SchedulerError, SharedCoordinator,
};
use serde::Serialize;

/// Shared UI result alias.
pub type UiResult<T> = Result<T, UiError>;

/// Shared state for the R2 status server.
#[derive(Debug, Clone)]
pub struct UiState {
    coordinator: SharedCoordinator,
}

impl UiState {
    /// Create UI state from an existing coordinator.
    pub fn new(coordinator: Coordinator) -> Self {
        Self::from_shared_coordinator(SharedCoordinator::new(coordinator))
    }

    /// Create UI state from a shared coordinator runtime handle.
    pub fn from_shared_coordinator(coordinator: SharedCoordinator) -> Self {
        Self { coordinator }
    }
}

/// UI construction and handler errors.
#[derive(Debug)]
pub enum UiError {
    /// Coordinator id validation failed.
    Id(String),
    /// Scheduler operation failed.
    Scheduler(SchedulerError),
    /// Shared coordinator lock was poisoned.
    LockPoisoned,
    /// Template rendering failed.
    Template(askama::Error),
}

impl fmt::Display for UiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(message) => write!(f, "invalid id: {message}"),
            Self::Scheduler(error) => write!(f, "{error}"),
            Self::LockPoisoned => f.write_str("coordinator status lock was poisoned"),
            Self::Template(error) => write!(f, "failed to render status page: {error}"),
        }
    }
}

impl Error for UiError {}

impl From<SchedulerError> for UiError {
    fn from(value: SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}

impl From<askama::Error> for UiError {
    fn from(value: askama::Error) -> Self {
        Self::Template(value)
    }
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

/// Build the R2 UI router.
pub fn router(state: UiState) -> Router {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/jobs/{job_id}", get(api_job_detail))
        .route("/api/v1/executors", get(api_executors))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .route("/assets/krishiv.css", get(stylesheet))
        .with_state(state)
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
pub struct TaskView {
    /// Task id.
    pub task_id: String,
    /// Task lifecycle state.
    pub state: String,
    /// Assigned executor id, if any.
    pub assigned_executor: String,
    /// Current attempt number.
    pub attempt: u32,
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

/// Prometheus-format metrics endpoint (stub with hardcoded 0 values).
async fn metrics() -> impl IntoResponse {
    const BODY: &str = "\
# HELP krishiv_jobs_total Total jobs submitted
# TYPE krishiv_jobs_total counter
krishiv_jobs_total 0
# HELP krishiv_tasks_total Total tasks submitted
# TYPE krishiv_tasks_total counter
krishiv_tasks_total 0
# HELP krishiv_shuffle_bytes_written_total Total bytes written to shuffle store
# TYPE krishiv_shuffle_bytes_written_total counter
krishiv_shuffle_bytes_written_total 0
# HELP krishiv_shuffle_partitions_total Total shuffle partitions finalized
# TYPE krishiv_shuffle_partitions_total counter
krishiv_shuffle_partitions_total 0
";
    (
        [(
            CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        BODY,
    )
}

async fn readyz(State(state): State<UiState>) -> Result<&'static str, UiError> {
    let _snapshot = status_snapshot(&state)?;
    Ok("ready\n")
}

async fn api_jobs(State(state): State<UiState>) -> Result<Json<JobsResponse>, UiError> {
    let snapshot = status_snapshot(&state)?;
    Ok(Json(JobsResponse {
        jobs: snapshot.jobs,
    }))
}

async fn api_job_detail(
    State(state): State<UiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobDetailResponse>, UiError> {
    Ok(Json(JobDetailResponse {
        job: job_detail(&state, &job_id)?,
    }))
}

async fn api_executors(State(state): State<UiState>) -> Result<Json<ExecutorsResponse>, UiError> {
    let snapshot = status_snapshot(&state)?;
    Ok(Json(ExecutorsResponse {
        executors: snapshot.executors,
    }))
}

async fn ui_jobs(State(state): State<UiState>) -> Result<Html<String>, UiError> {
    let snapshot = status_snapshot(&state)?;
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
    let snapshot = status_snapshot(&state)?;
    let template = JobTemplate {
        coordinator_id: snapshot.coordinator_id,
        coordinator_state: snapshot.coordinator_state,
        job: job_detail(&state, &job_id)?,
        executors: snapshot.executors,
    };
    Ok(Html(template.render()?))
}

async fn stylesheet() -> impl IntoResponse {
    ([(CONTENT_TYPE, "text/css; charset=utf-8")], STYLE)
}

fn status_snapshot(state: &UiState) -> UiResult<StatusView> {
    let coordinator = state
        .coordinator
        .read()
        .map_err(|_| UiError::LockPoisoned)?;
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

fn job_detail(state: &UiState, job_id: &str) -> UiResult<JobDetailView> {
    let job_id =
        JobId::try_new(job_id.to_owned()).map_err(|error| UiError::Id(error.to_string()))?;
    let coordinator = state
        .coordinator
        .read()
        .map_err(|_| UiError::LockPoisoned)?;
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
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

impl ExecutorView {
    fn from_record(record: &ExecutorRecord) -> Self {
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

const STYLE: &str = r#"
:root {
  color-scheme: light;
  --bg: #f7f8fa;
  --panel: #ffffff;
  --text: #1f2933;
  --muted: #64748b;
  --line: #d9e2ec;
  --accent: #0f766e;
  --accent-dark: #115e59;
  --warn: #b45309;
  --bad: #b91c1c;
  --good: #15803d;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

a {
  color: var(--accent-dark);
  font-weight: 600;
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}

.shell {
  margin: 0 auto;
  max-width: 1180px;
  padding: 28px 24px 48px;
}

.topbar {
  align-items: center;
  display: flex;
  gap: 18px;
  justify-content: space-between;
  margin-bottom: 24px;
}

.brand {
  display: grid;
  gap: 3px;
}

.brand strong {
  font-size: 20px;
  letter-spacing: 0;
}

.brand span,
.meta {
  color: var(--muted);
  font-size: 13px;
}

.summary {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin-bottom: 22px;
}

.metric {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 14px;
}

.metric span {
  color: var(--muted);
  display: block;
  font-size: 12px;
  margin-bottom: 6px;
}

.metric strong {
  font-size: 22px;
}

.section {
  margin-top: 26px;
}

.section h2 {
  font-size: 16px;
  margin: 0 0 10px;
}

.table-wrap {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  overflow-x: auto;
}

table {
  border-collapse: collapse;
  min-width: 760px;
  width: 100%;
}

th,
td {
  border-bottom: 1px solid var(--line);
  font-size: 13px;
  padding: 10px 12px;
  text-align: left;
  white-space: nowrap;
}

th {
  background: #eef3f7;
  color: #334155;
  font-size: 12px;
  text-transform: uppercase;
}

tr:last-child td {
  border-bottom: 0;
}

.state {
  border-radius: 999px;
  display: inline-block;
  font-size: 12px;
  font-weight: 700;
  line-height: 1;
  padding: 5px 8px;
}

.state.running,
.state.healthy,
.state.active {
  background: #dff7ec;
  color: var(--good);
}

.state.failed,
.state.lost {
  background: #fee2e2;
  color: var(--bad);
}

.state.assigned,
.state.scheduling,
.state.accepted,
.state.registered {
  background: #fef3c7;
  color: var(--warn);
}

.empty {
  background: var(--panel);
  border: 1px dashed var(--line);
  border-radius: 8px;
  color: var(--muted);
  padding: 18px;
}

@media (max-width: 760px) {
  .shell {
    padding: 20px 14px 36px;
  }

  .topbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
"#;

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use krishiv_proto::{
        CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState,
    };
    use krishiv_scheduler::{Coordinator, SharedCoordinator};
    use tower::ServiceExt;

    use super::{UiState, demo_state, empty_state, router};

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

    #[tokio::test]
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

    #[tokio::test]
    async fn api_jobs_reads_shared_runtime_state() {
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-runtime").unwrap(),
        ));
        {
            let mut coordinator = shared.write().unwrap();
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

    #[tokio::test]
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

    #[tokio::test]
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
    async fn metrics_returns_ok_with_prometheus_body() {
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
        assert!(body.contains("krishiv_jobs_total"));
    }

    #[tokio::test]
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
}
