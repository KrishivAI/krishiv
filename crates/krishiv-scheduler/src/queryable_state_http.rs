//! E4.4 — REST API for QueryableState point lookups.
//!
//! Exposes two endpoints:
//!
//! * `GET /api/v1/jobs/{job_id}/state/{op_id}/{state_name}/{key_hex}` — point lookup.
//!   `key_hex` is the raw key bytes encoded as lowercase hex.
//!   Returns `{"found": true, "value_base64": "..."}` or `{"found": false}`.
//!
//! * `GET /api/v1/jobs/{job_id}/state/{op_id}` — list state names registered for the operator.
//!
//! Build the sub-router via [`queryable_state_router`] and merge it into the
//! main coordinator router.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use krishiv_state::QueryableStateStore;

// ── Response types ────────────────────────────────────────────────────────────

/// Response for a successful point lookup.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "found", rename_all = "snake_case")]
pub enum QueryStateResponse {
    #[serde(rename = "true")]
    Found {
        job_id: String,
        op_id: String,
        state_name: String,
        /// Key bytes encoded as lowercase hex (mirrors the request path parameter).
        key_hex: String,
        /// Value bytes base64-encoded (standard, no padding stripped).
        value_base64: String,
    },
    #[serde(rename = "false")]
    NotFound {
        job_id: String,
        op_id: String,
        state_name: String,
        key_hex: String,
    },
}

/// Response for listing operator state names.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListStateNamesResponse {
    pub job_id: String,
    pub op_id: String,
    /// Unique state names registered under this `(job_id, op_id)` pair.
    pub state_names: Vec<String>,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /api/v1/jobs/{job_id}/state/{op_id}/{state_name}/{key_hex}`
async fn api_query_state(
    State(store): State<Arc<QueryableStateStore>>,
    Path((job_id, op_id, state_name, key_hex)): Path<(String, String, String, String)>,
) -> Result<Json<QueryStateResponse>, StatusCode> {
    let key = hex::decode(&key_hex).map_err(|_| StatusCode::BAD_REQUEST)?;

    let value = store.get(&job_id, &op_id, &state_name, &key).map_err(|e| {
        tracing::warn!(job_id, op_id, state_name, error = %e, "queryable state lookup error");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let resp = match value {
        Some(bytes) => QueryStateResponse::Found {
            job_id,
            op_id,
            state_name,
            key_hex,
            value_base64: base64::engine::general_purpose::STANDARD.encode(&bytes),
        },
        None => QueryStateResponse::NotFound {
            job_id,
            op_id,
            state_name,
            key_hex,
        },
    };
    Ok(Json(resp))
}

/// `GET /api/v1/jobs/{job_id}/state/{op_id}`
async fn api_list_state_names(
    State(store): State<Arc<QueryableStateStore>>,
    Path((job_id, op_id)): Path<(String, String)>,
) -> Result<Json<ListStateNamesResponse>, StatusCode> {
    let namespaces = store.list_namespaces(&job_id, &op_id).map_err(|e| {
        tracing::warn!(job_id, op_id, error = %e, "queryable state list namespaces error");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut state_names: Vec<String> = namespaces
        .into_iter()
        .map(|ns| ns.state_name().to_owned())
        .collect();
    state_names.sort();
    state_names.dedup();

    Ok(Json(ListStateNamesResponse {
        job_id,
        op_id,
        state_names,
    }))
}

// ── Router factory ────────────────────────────────────────────────────────────

/// Build a sub-router exposing the queryable state endpoints.
///
/// Merge this into the main coordinator router:
/// ```ignore
/// let store = Arc::new(QueryableStateStore::new());
/// let main_router = coordinator_http_router(coord, &cfg);
/// let full_router = main_router.merge(queryable_state_router(store));
/// ```
pub fn queryable_state_router(store: Arc<QueryableStateStore>) -> Router {
    Router::new()
        .route(
            "/api/v1/jobs/{job_id}/state/{op_id}/{state_name}/{key_hex}",
            get(api_query_state),
        )
        .route(
            "/api/v1/jobs/{job_id}/state/{op_id}",
            get(api_list_state_names),
        )
        .with_state(store)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Decode hex string to bytes (re-exported so tests can use it easily).
pub fn decode_key_hex(hex_str: &str) -> Option<Vec<u8>> {
    hex::decode(hex_str).ok()
}

/// Encode bytes as lowercase hex (for constructing query URLs).
pub fn encode_key_hex(key: &[u8]) -> String {
    hex::encode(key)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use krishiv_state::backend::StateBackend;
    use krishiv_state::namespace::Namespace;
    use krishiv_state::rocksdb_backend::RocksDbStateBackend;
    use tower::ServiceExt as _;

    fn make_store_with_data() -> Arc<QueryableStateStore> {
        let store = Arc::new(QueryableStateStore::new());
        let mut b = RocksDbStateBackend::new().unwrap();
        let ns = Namespace::new("op-1", "counts");
        b.put(&ns, b"user-a".to_vec(), b"100".to_vec()).unwrap();
        b.put(&ns, b"user-b".to_vec(), b"200".to_vec()).unwrap();
        store.register("job-1", "op-1", Arc::new(b));
        store
    }

    #[tokio::test]
    async fn query_state_found_returns_200_with_value() {
        let store = make_store_with_data();
        let router = queryable_state_router(store);

        let key_hex = encode_key_hex(b"user-a");
        let uri = format!("/api/v1/jobs/job-1/state/op-1/counts/{key_hex}");
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["found"], "true");
        // Verify the decoded value.
        let v_b64 = parsed["value_base64"].as_str().unwrap();
        let v_bytes = base64::engine::general_purpose::STANDARD
            .decode(v_b64)
            .unwrap();
        assert_eq!(v_bytes, b"100");
    }

    #[tokio::test]
    async fn query_state_missing_key_returns_200_not_found() {
        let store = make_store_with_data();
        let router = queryable_state_router(store);

        let key_hex = encode_key_hex(b"no-such-key");
        let uri = format!("/api/v1/jobs/job-1/state/op-1/counts/{key_hex}");
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["found"], "false");
    }

    #[tokio::test]
    async fn query_state_invalid_hex_returns_400() {
        let store = make_store_with_data();
        let router = queryable_state_router(store);

        let req = Request::builder()
            .uri("/api/v1/jobs/job-1/state/op-1/counts/notvalidhex!!!")
            .body(Body::empty())
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_state_unregistered_job_returns_not_found_json() {
        let store = Arc::new(QueryableStateStore::new());
        let router = queryable_state_router(store);

        let key_hex = encode_key_hex(b"k");
        let uri = format!("/api/v1/jobs/no-such-job/state/op-1/counts/{key_hex}");
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["found"], "false");
    }

    #[tokio::test]
    async fn list_state_names_returns_namespace_list() {
        let store = make_store_with_data();
        let router = queryable_state_router(store);

        let req = Request::builder()
            .uri("/api/v1/jobs/job-1/state/op-1")
            .body(Body::empty())
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let names = parsed["state_names"].as_array().unwrap();
        assert!(names.iter().any(|n| n == "counts"));
    }
}
