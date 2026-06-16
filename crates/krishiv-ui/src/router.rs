use axum::Router;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use krishiv_proto::{
    CoordinatorId, ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, JobId,
};
use krishiv_scheduler::Coordinator;

use crate::handlers::{
    api_executor_detail, api_executors, api_history, api_history_detail, api_job_checkpoints,
    api_job_detail, api_job_diagnose, api_jobs, api_queues, api_sql_execute, auth_js, demo_job,
    healthz, live_js, metrics, openapi_json, readyz, sql_js, stylesheet, ui_executor_detail,
    ui_health, ui_history, ui_history_detail, ui_job_checkpoints_page, ui_job_detail,
    ui_job_diagnose, ui_jobs, ui_metrics, ui_submit,
};
use crate::{UiError, UiResult, UiState};

pub(crate) fn effective_ui_bearer_token(state: &UiState) -> Option<String> {
    state.ui_bearer_token.clone().or_else(resolve_ui_token)
}

pub(crate) fn ui_auth_token(state: &UiState) -> Option<String> {
    effective_ui_bearer_token(state)
}

pub(crate) fn resolve_ui_token() -> Option<String> {
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
                    tracing::error!(path = %path, error = %e, "krishiv-ui: token file could not be read; denying all protected routes (production fail-closed)");
                    return Some(String::new());
                }
                tracing::warn!(path = %path, error = %e, "krishiv-ui: token file could not be read; falling back to anonymous router");
            }
        }
    }
    if krishiv_common::profile_requires_authenticated_ui(
        krishiv_common::resolve_durability_profile(),
    ) {
        tracing::warn!(
            "krishiv-ui: no UI token configured; denying all protected routes (production fail-closed)"
        );
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
        .route("/assets/krishiv.css", get(stylesheet))
        .route("/assets/krishiv-auth.js", get(auth_js))
        .route("/assets/krishiv-live.js", get(live_js))
        .route("/assets/krishiv-sql.js", get(sql_js))
        // Alias under /ui/assets/ so the paths work through a path-prefix reverse
        // proxy (e.g. code-server /proxy/PORT/) where root-relative /assets/ URLs
        // resolve outside the proxied subtree.
        .route("/ui/assets/krishiv.css", get(stylesheet))
        .route("/ui/assets/krishiv-auth.js", get(auth_js))
        .route("/ui/assets/krishiv-live.js", get(live_js))
        .route("/ui/assets/krishiv-sql.js", get(sql_js))
        .route("/api/v1/openapi.json", get(openapi_json));

    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/jobs/{job_id}", get(api_job_detail))
        .route(
            "/api/v1/jobs/{job_id}/checkpoints",
            get(api_job_checkpoints),
        )
        .route("/api/v1/jobs/{job_id}/diagnose", get(api_job_diagnose))
        .route("/api/v1/executors", get(api_executors))
        .route("/api/v1/executors/{executor_id}", get(api_executor_detail))
        .route("/api/v1/queues", get(api_queues))
        .route("/api/v1/sql", post(api_sql_execute))
        .route("/api/v1/history", get(api_history))
        .route("/api/v1/history/{job_id}", get(api_history_detail))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .route(
            "/ui/jobs/{job_id}/checkpoints",
            get(ui_job_checkpoints_page),
        )
        .route("/ui/jobs/{job_id}/diagnose", get(ui_job_diagnose))
        .route("/ui/executors/{executor_id}", get(ui_executor_detail))
        .route("/ui/submit", get(ui_submit))
        .route("/ui/health", get(ui_health))
        .route("/ui/metrics", get(ui_metrics))
        .route("/ui/history", get(ui_history))
        .route("/ui/history/{job_id}", get(ui_history_detail))
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
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

/// Build the UI-specific routes (jobs, executors, SQL editor, health dashboard)
/// for embedding inside the coordinator HTTP server.
///
/// Skips `/healthz`, `/readyz`, and `/metrics` — the coordinator already serves
/// those. Includes `/assets/*`, `/`, `/ui*`, and `/api/v1/*` routes.
pub fn embedded_router(state: UiState) -> Router {
    let public = Router::new()
        .route("/assets/krishiv.css", get(stylesheet))
        .route("/assets/krishiv-auth.js", get(auth_js))
        .route("/assets/krishiv-live.js", get(live_js))
        .route("/assets/krishiv-sql.js", get(sql_js))
        .route("/ui/assets/krishiv.css", get(stylesheet))
        .route("/ui/assets/krishiv-auth.js", get(auth_js))
        .route("/ui/assets/krishiv-live.js", get(live_js))
        .route("/ui/assets/krishiv-sql.js", get(sql_js))
        .route("/api/v1/openapi.json", get(openapi_json));

    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/ui") }))
        .route("/api/v1/jobs", get(api_jobs))
        .route("/api/v1/jobs/{job_id}", get(api_job_detail))
        .route(
            "/api/v1/jobs/{job_id}/checkpoints",
            get(api_job_checkpoints),
        )
        .route("/api/v1/jobs/{job_id}/diagnose", get(api_job_diagnose))
        .route("/api/v1/executors", get(api_executors))
        .route("/api/v1/executors/{executor_id}", get(api_executor_detail))
        .route("/api/v1/queues", get(api_queues))
        .route("/api/v1/sql", post(api_sql_execute))
        .route("/api/v1/history", get(api_history))
        .route("/api/v1/history/{job_id}", get(api_history_detail))
        .route("/ui", get(ui_jobs))
        .route("/ui/jobs/{job_id}", get(ui_job_detail))
        .route(
            "/ui/jobs/{job_id}/checkpoints",
            get(ui_job_checkpoints_page),
        )
        .route("/ui/jobs/{job_id}/diagnose", get(ui_job_diagnose))
        .route("/ui/executors/{executor_id}", get(ui_executor_detail))
        .route("/ui/submit", get(ui_submit))
        .route("/ui/health", get(ui_health))
        .route("/ui/metrics", get(ui_metrics))
        .route("/ui/history", get(ui_history))
        .route("/ui/history/{job_id}", get(ui_history_detail));

    let protected = if let Some(expected) = ui_auth_token(&state).as_deref() {
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
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

/// Attach hardening headers to every response. With htmx vendored locally and
/// the SQL editor moved to a same-origin script, `script-src 'self'` holds.
/// Inline `style="..."` attributes still appear in a few templates, so
/// `style-src` keeps `'unsafe-inline'`; scripts (the real XSS vector) do not.
async fn security_headers(request: axum::extract::Request, next: Next) -> Response {
    use axum::http::HeaderValue;
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        "Content-Security-Policy",
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
             img-src 'self' data:; connect-src 'self'; base-uri 'none'; \
             form-action 'self'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("X-Frame-Options", HeaderValue::from_static("DENY"));
    headers.insert("Referrer-Policy", HeaderValue::from_static("no-referrer"));
    response
}

async fn require_bearer(request: axum::extract::Request, next: Next, expected: &str) -> Response {
    if expected.is_empty() {
        return (StatusCode::UNAUTHORIZED, "authentication not configured").into_response();
    }
    let auth = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    match auth {
        Some(value) if value.len() > 7 && value[..7].eq_ignore_ascii_case("bearer ") => {
            let token = &value[7..];
            // Constant-time comparison so a timing side-channel can't be used to
            // recover the token byte-by-byte.
            if constant_time_eq::constant_time_eq(token.as_bytes(), expected.as_bytes()) {
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
