//! Unified `/api/v1/jobs` submit endpoint.
//!
//! Accepts a single `POST /api/v1/jobs` request with a discriminated body and
//! routes internally to the appropriate subsystem:
//!
//! | `kind`        | Delegates to                                              |
//! |---------------|-----------------------------------------------------------|
//! | `"batch_sql"` | `POST /api/v1/batch-sql/submit` (async submit-then-poll) |
//! | `"ivm"`       | `POST /api/v1/ivm/jobs`          (create-or-get job)      |
//! | `"streaming"` | `POST /api/v1/continuous-register`                        |
//!
//! All existing per-subsystem endpoints continue to work unchanged.  This
//! endpoint is purely additive — it gives clients a single URL for all job
//! types and avoids a multi-step "discover the right endpoint" round-trip.
//!
//! # Request body
//!
//! **Batch SQL:**
//! ```json
//! { "kind": "batch_sql", "query": "SELECT 1 + 1 AS n", "tables": [] }
//! ```
//!
//! **IVM:**
//! ```json
//! { "kind": "ivm", "job_id": "revenue" }
//! ```
//!
//! **Streaming:**
//! ```json
//! { "kind": "streaming", "job_id": "etl-job", "spec": { ... } }
//! ```
//!
//! # Response
//!
//! All responses include `job_id` and `kind`. Batch-SQL responses additionally
//! include `state: "Submitted"` and `poll_url` for polling results.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::batch_sql::{BatchSqlInlineTable, submit_batch_sql_job};
use crate::continuous_stream_http::register_continuous_stream_coordinated;
use crate::ivm_http::IvmRouterState;

// ── request body ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct UnifiedJobRequest {
    /// Job type discriminator: `"batch_sql"`, `"ivm"`, or `"streaming"`.
    pub kind: String,

    // ── batch_sql fields ──────────────────────────────────────────────────
    /// SQL query (required for `kind = "batch_sql"`).
    #[serde(default)]
    pub query: Option<String>,
    /// Inline Arrow-IPC tables for the query (optional).
    #[serde(default)]
    pub tables: Vec<UnifiedInlineTableJson>,

    // ── ivm / streaming fields ────────────────────────────────────────────
    /// Desired job ID (optional; coordinator assigns one if absent).
    #[serde(default)]
    pub job_id: Option<String>,

    // ── streaming fields ──────────────────────────────────────────────────
    /// Window execution spec (required for `kind = "streaming"`).
    #[serde(default)]
    pub spec: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UnifiedInlineTableJson {
    pub table_name: String,
    pub ipc_b64: String,
}

// ── response body ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct UnifiedJobResponse {
    pub job_id: String,
    pub kind: String,
    /// Terminal / intermediate state (batch_sql only: `"Submitted"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// URL to poll for results (batch_sql only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_url: Option<String>,
}

// ── handler ───────────────────────────────────────────────────────────────────

/// `POST /api/v1/jobs` — unified job submission endpoint.
///
/// Routes to the matching subsystem based on the `kind` field.
pub async fn api_unified_submit(
    State(state): State<IvmRouterState>,
    Json(body): Json<UnifiedJobRequest>,
) -> Result<(StatusCode, Json<UnifiedJobResponse>), (StatusCode, String)> {
    match body.kind.as_str() {
        "batch_sql" => handle_batch_sql(&state, body).await,
        "ivm" => handle_ivm(&state, body).await,
        "streaming" => handle_streaming(&state, body).await,
        other => Err((
            StatusCode::BAD_REQUEST,
            format!("unknown job kind '{other}'; expected batch_sql, ivm, or streaming"),
        )),
    }
}

// ── batch SQL ────────────────────────────────────────────────────────────────

async fn handle_batch_sql(
    state: &IvmRouterState,
    body: UnifiedJobRequest,
) -> Result<(StatusCode, Json<UnifiedJobResponse>), (StatusCode, String)> {
    let query = body.query.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "batch_sql jobs require a 'query' field".to_string(),
        )
    })?;

    let inline_tables: Vec<BatchSqlInlineTable> = body
        .tables
        .into_iter()
        .map(|t| BatchSqlInlineTable {
            table_name: t.table_name,
            ipc_b64: t.ipc_b64,
        })
        .collect();

    let job_id = submit_batch_sql_job(&state.coordinator, &query, &inline_tables, false)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let job_id_str = job_id.as_str().to_string();
    Ok((
        StatusCode::ACCEPTED,
        Json(UnifiedJobResponse {
            poll_url: Some(format!("/api/v1/batch-sql/{job_id_str}")),
            job_id: job_id_str,
            kind: "batch_sql".to_string(),
            state: Some("Submitted".to_string()),
        }),
    ))
}

// ── IVM ─────────────────────────────────────────────────────────────────────

async fn handle_ivm(
    state: &IvmRouterState,
    body: UnifiedJobRequest,
) -> Result<(StatusCode, Json<UnifiedJobResponse>), (StatusCode, String)> {
    let job_id = body
        .job_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    state
        .registry
        .create(job_id.clone())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(UnifiedJobResponse {
            job_id,
            kind: "ivm".to_string(),
            state: None,
            poll_url: None,
        }),
    ))
}

// ── Streaming ────────────────────────────────────────────────────────────────

async fn handle_streaming(
    state: &IvmRouterState,
    body: UnifiedJobRequest,
) -> Result<(StatusCode, Json<UnifiedJobResponse>), (StatusCode, String)> {
    let job_id = body.job_id.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "streaming jobs require a 'job_id' field".to_string(),
        )
    })?;

    let spec_value = body.spec.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "streaming jobs require a 'spec' field".to_string(),
        )
    })?;

    let spec: krishiv_plan::window::WindowExecutionSpec = serde_json::from_value(spec_value)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid spec: {e}")))?;

    register_continuous_stream_coordinated(&state.coordinator, &job_id, &spec)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(UnifiedJobResponse {
            job_id,
            kind: "streaming".to_string(),
            state: None,
            poll_url: None,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use krishiv_proto::CoordinatorId;

    use super::*;
    use crate::ivm::IvmJobRegistry;
    use crate::{Coordinator, SharedCoordinator};

    /// A cheap, real (not mocked) `IvmRouterState`: an in-memory registry and
    /// an active coordinator with no registered executors. Sufficient for
    /// every request-validation path (which errors before touching `state`
    /// at all) and for `handle_ivm`'s success path (pure in-memory registry
    /// write, no scheduling required).
    fn test_state() -> IvmRouterState {
        IvmRouterState {
            registry: Arc::new(IvmJobRegistry::new()),
            coordinator: SharedCoordinator::new(Coordinator::active(
                CoordinatorId::try_new("test-coord").unwrap(),
            )),
        }
    }

    fn request(kind: &str) -> UnifiedJobRequest {
        UnifiedJobRequest {
            kind: kind.to_owned(),
            query: None,
            tables: Vec::new(),
            job_id: None,
            spec: None,
        }
    }

    #[tokio::test]
    async fn unknown_kind_is_rejected_with_bad_request() {
        let state = test_state();
        let err = api_unified_submit(State(state), Json(request("nonsense")))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("nonsense"), "got: {}", err.1);
    }

    #[tokio::test]
    async fn batch_sql_without_query_is_rejected() {
        let state = test_state();
        let err = api_unified_submit(State(state), Json(request("batch_sql")))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("query"), "got: {}", err.1);
    }

    #[tokio::test]
    async fn streaming_without_job_id_is_rejected() {
        let state = test_state();
        let mut body = request("streaming");
        body.spec = Some(serde_json::json!({}));
        let err = api_unified_submit(State(state), Json(body))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("job_id"), "got: {}", err.1);
    }

    #[tokio::test]
    async fn streaming_without_spec_is_rejected() {
        let state = test_state();
        let mut body = request("streaming");
        body.job_id = Some("job-1".to_owned());
        let err = api_unified_submit(State(state), Json(body))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("spec"), "got: {}", err.1);
    }

    #[tokio::test]
    async fn streaming_with_malformed_spec_is_rejected_as_bad_request_not_500() {
        let state = test_state();
        let mut body = request("streaming");
        body.job_id = Some("job-1".to_owned());
        // Missing every required WindowExecutionSpec field.
        body.spec = Some(serde_json::json!({"not_a_real_field": true}));
        let err = api_unified_submit(State(state), Json(body))
            .await
            .unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("invalid spec"), "got: {}", err.1);
    }

    #[tokio::test]
    async fn ivm_without_job_id_generates_one() {
        let state = test_state();
        let (status, Json(resp)) = api_unified_submit(State(state), Json(request("ivm")))
            .await
            .expect("ivm submission with no explicit job_id must succeed");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resp.kind, "ivm");
        assert!(!resp.job_id.is_empty());
        assert!(resp.state.is_none());
        assert!(resp.poll_url.is_none());
    }

    #[tokio::test]
    async fn ivm_with_explicit_job_id_echoes_it_back() {
        let state = test_state();
        let mut body = request("ivm");
        body.job_id = Some("revenue".to_owned());
        let (status, Json(resp)) = api_unified_submit(State(state), Json(body))
            .await
            .expect("ivm submission must succeed");
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resp.job_id, "revenue");
    }

    #[tokio::test]
    async fn ivm_create_is_idempotent_for_a_repeated_job_id() {
        // IvmJobRegistry::create uses entry().or_insert_with — calling twice
        // with the same id must not error the second time.
        let state = test_state();
        for _ in 0..2 {
            let mut body = request("ivm");
            body.job_id = Some("revenue".to_owned());
            let (status, _) = api_unified_submit(State(state.clone()), Json(body))
                .await
                .expect("both submissions must succeed");
            assert_eq!(status, StatusCode::CREATED);
        }
    }

    #[test]
    fn unified_job_request_deserializes_minimal_batch_sql_body() {
        let body: UnifiedJobRequest =
            serde_json::from_str(r#"{"kind": "batch_sql", "query": "SELECT 1"}"#).unwrap();
        assert_eq!(body.kind, "batch_sql");
        assert_eq!(body.query.as_deref(), Some("SELECT 1"));
        assert!(body.tables.is_empty(), "tables must default to empty");
        assert!(body.job_id.is_none());
        assert!(body.spec.is_none());
    }

    #[test]
    fn unified_job_response_omits_null_optional_fields_when_serialized() {
        let resp = UnifiedJobResponse {
            job_id: "job-1".to_owned(),
            kind: "ivm".to_owned(),
            state: None,
            poll_url: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            !obj.contains_key("state"),
            "None state must be omitted, not serialized as null: {json}"
        );
        assert!(
            !obj.contains_key("poll_url"),
            "None poll_url must be omitted, not serialized as null: {json}"
        );
    }
}
