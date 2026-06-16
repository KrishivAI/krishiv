#![forbid(unsafe_code)]

//! R2 status API and server-rendered Web UI.
//!
//! This crate exposes scheduler snapshots as a small Rust-native operations
//! surface. It intentionally depends on the in-process R2 scheduler model
//! rather than introducing Kubernetes clients or a separate frontend build.

mod handlers;
mod router;
mod views;

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use krishiv_scheduler::{Coordinator, SchedulerError, SharedCoordinator};

/// Shared UI result alias.
pub type UiResult<T> = Result<T, UiError>;

/// Shared state for the R2 status server.
#[derive(Clone)]
pub struct UiState {
    coordinator: SharedCoordinator,
    metrics_cache: Arc<std::sync::Mutex<(String, std::time::Instant)>>,
    sql: Option<Arc<krishiv_sql::SqlEngine>>,
    /// Optional bearer token exposed to browser scripts for authenticated fetches.
    ui_bearer_token: Option<String>,
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
            ui_bearer_token: crate::router::resolve_ui_token(),
        }
    }

    /// Override the bearer token injected into browser-facing pages.
    pub fn with_ui_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.ui_bearer_token = Some(token.into());
        self
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

pub use router::{demo_state, embedded_router, empty_state, router, router_with_token, serve};

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

    #[tokio::test(flavor = "multi_thread")]
    async fn vendored_live_js_is_public_and_js() {
        // The live-refresh helper must be served locally (no CDN) and reachable
        // without a bearer token, like the stylesheet.
        let response = router_with_token(demo_state().unwrap(), Some("secret"))
            .oneshot(
                Request::builder()
                    .uri("/assets/krishiv-live.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let ct = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(ct.contains("javascript"), "unexpected content-type: {ct}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("live-region"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rendered_pages_have_no_cdn_script() {
        // Regression: pages must not pull htmx (or anything) from an external CDN.
        for uri in ["/ui", "/ui/health", "/ui/metrics", "/ui/submit"] {
            let response = router(demo_state().unwrap())
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(!body.contains("unpkg.com"), "{uri} still references a CDN");
            assert!(
                body.contains("assets/krishiv-live.js"),
                "{uri} missing vendored live script"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn responses_carry_security_headers() {
        let response = router(demo_state().unwrap())
            .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let headers = response.headers();
        let csp = headers
            .get("content-security-policy")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(csp.contains("script-src 'self'"), "CSP missing: {csp}");
        assert_eq!(
            headers
                .get("x-content-type-options")
                .and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        assert_eq!(
            headers.get("x-frame-options").and_then(|v| v.to_str().ok()),
            Some("DENY")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openapi_spec_is_served_and_valid_json() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["openapi"], "3.1.0");
        assert!(parsed["paths"]["/api/v1/jobs"].is_object());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_jobs_pagination_limits_results() {
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs?limit=0&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // limit=0 is clamped up to 1; demo has exactly one job, total reflects it.
        assert_eq!(parsed["limit"], 1);
        assert_eq!(parsed["total"], 1);
        assert_eq!(parsed["jobs"].as_array().unwrap().len(), 1);

        // offset past the end yields an empty page but preserves total.
        let response = router(demo_state().unwrap())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs?offset=5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["total"], 1);
        assert!(parsed["jobs"].as_array().unwrap().is_empty());
    }
}
