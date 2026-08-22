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
    api_job_detail, api_job_diagnose, api_jobs, api_queues, api_sql_execute, demo_job, healthz,
    metrics, openapi_json, readyz,
};
use crate::{UiError, UiResult, UiState};

pub(crate) fn ui_auth_token(state: &UiState) -> Option<String> {
    state.ui_bearer_token.clone().or_else(resolve_ui_token)
}

pub(crate) fn resolve_ui_token() -> Option<String> {
    let file_contents = std::env::var("KRISHIV_UI_TOKEN_FILE")
        .ok()
        .filter(|path| !path.trim().is_empty())
        .map(|path| {
            let read = std::fs::read_to_string(&path);
            if let Err(e) = &read {
                tracing::warn!(path = %path, error = %e, "krishiv-ui: token file could not be read");
            }
            read.map_err(|e| e.to_string())
        });
    resolve_ui_token_from(
        std::env::var("KRISHIV_UI_TOKEN").ok(),
        file_contents,
        krishiv_common::profile_requires_authenticated_ui(
            krishiv_common::resolve_durability_profile(),
        ),
    )
}

/// Resolve the UI bearer token from already-read inputs.
///
/// `token_file` is `None` when `KRISHIV_UI_TOKEN_FILE` is unset **or set to an
/// empty/blank path**, `Some(Ok(contents))` when the file was read, and
/// `Some(Err(_))` when it could not be.
///
/// `Some("")` means **deny everything** (`require_bearer` rejects an empty
/// expected token); `None` means run the router anonymously.
///
/// Crate-23 audit (U1): the previous implementation early-returned `None` when
/// `KRISHIV_UI_TOKEN_FILE` was set to an empty string, jumping over the
/// production fail-closed check below. A deployment that renders that variable
/// empty (an unset Helm value, `FOO=${MISSING}`) therefore served every
/// `/api/v1/*` and `/ui/*` route anonymously in production. The empty path is
/// now treated as "no file configured", which falls through to the guard.
///
/// Split from the environment reads so the matrix is testable: mutating process
/// environment is unsound under a multi-threaded test runner and `set_var` is
/// unsafe since edition 2024, which this workspace denies.
pub(crate) fn resolve_ui_token_from(
    inline: Option<String>,
    token_file: Option<Result<String, String>>,
    production_requires_auth: bool,
) -> Option<String> {
    if let Some(value) = inline {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    match token_file {
        Some(Ok(contents)) => {
            let trimmed = contents.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
        Some(Err(_)) if production_requires_auth => {
            tracing::error!(
                "krishiv-ui: token file could not be read; denying all protected routes (production fail-closed)"
            );
            return Some(String::new());
        }
        Some(Err(_)) | None => {}
    }
    if production_requires_auth {
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
        .route("/api/v1/openapi.json", get(openapi_json))
        // The embedded TanStack console (SPA shell + hashed assets). Auth
        // lives in the SPA: it stores a bearer and sends it on every /api
        // call, which the coordinator's own middleware enforces — the static
        // assets carry no data, so they stay public like /assets/*.
        .route("/console", get(crate::console::serve))
        .route("/console/{*path}", get(crate::console::serve));

    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/console") }))
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
        .route("/api/v1/openapi.json", get(openapi_json))
        // The embedded TanStack console (SPA shell + hashed assets). Auth
        // lives in the SPA: it stores a bearer and sends it on every /api
        // call, which the coordinator's own middleware enforces — the static
        // assets carry no data, so they stay public like /assets/*.
        .route("/console", get(crate::console::serve))
        .route("/console/{*path}", get(crate::console::serve));

    // NOTE: API routes (/api/v1/jobs, /api/v1/executors, etc.) are served by
    // the coordinator's own HTTP router. This embedded router only provides
    // UI pages and static assets to avoid duplicate-route panics when merged
    // via `extra_http_factory`.
    let protected = Router::new()
        .route("/", get(|| async { Redirect::temporary("/console") }))
        .route("/api/v1/jobs/{job_id}/diagnose", get(api_job_diagnose))
        .route("/api/v1/queues", get(api_queues))
        .route("/api/v1/sql", post(api_sql_execute))
        .route("/api/v1/history", get(api_history))
        .route("/api/v1/history/{job_id}", get(api_history_detail));

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
             img-src 'self' data:; connect-src 'self'; base-uri 'self'; \
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
    const BEARER_PREFIX: &str = "bearer ";
    let auth = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    match auth {
        // Use `str::get` rather than indexing so a crafted header value whose
        // 7-byte cut point lands inside a multi-byte UTF-8 sequence cannot
        // panic the handler (and take down the server) — it simply falls
        // through to the missing-token branch.
        Some(value)
            if value.len() > BEARER_PREFIX.len()
                && value
                    .get(..BEARER_PREFIX.len())
                    .is_some_and(|p| p.eq_ignore_ascii_case(BEARER_PREFIX)) =>
        {
            let token = value.get(BEARER_PREFIX.len()..).unwrap_or("");
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

#[cfg(test)]
mod token_resolution_tests {
    use super::resolve_ui_token_from;

    /// Regression (crate-23 audit, U1): an **empty** `KRISHIV_UI_TOKEN_FILE`
    /// used to early-return `None`, skipping the production fail-closed check
    /// and serving every protected route anonymously. An empty/blank path must
    /// mean "no file configured" and still deny in production.
    #[test]
    fn empty_token_file_path_still_fails_closed_in_production() {
        // Empty path is normalised to `None` by the caller.
        assert_eq!(
            resolve_ui_token_from(None, None, true),
            Some(String::new()),
            "production with no token configured must deny (empty expected token)"
        );
        // Dev profile stays anonymous.
        assert_eq!(resolve_ui_token_from(None, None, false), None);
    }

    #[test]
    fn inline_token_wins_and_is_trimmed() {
        assert_eq!(
            resolve_ui_token_from(Some("  s3cret \n".into()), None, true),
            Some("s3cret".to_string())
        );
        // A blank inline token is not a token; production still denies.
        assert_eq!(
            resolve_ui_token_from(Some("   ".into()), None, true),
            Some(String::new())
        );
    }

    #[test]
    fn token_file_contents_are_used_and_unreadable_file_denies_in_production() {
        assert_eq!(
            resolve_ui_token_from(None, Some(Ok("from-file\n".into())), false),
            Some("from-file".to_string())
        );
        // Unreadable file: deny in production, anonymous in dev.
        assert_eq!(
            resolve_ui_token_from(None, Some(Err("ENOENT".into())), true),
            Some(String::new())
        );
        assert_eq!(
            resolve_ui_token_from(None, Some(Err("ENOENT".into())), false),
            None
        );
    }
}
