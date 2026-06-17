#![forbid(unsafe_code)]

//! HTTP handlers for the IVM (DeltaBatch) API.
//!
//! # Protocol overview
//!
//! | Method | Path                                            | Description                       |
//! |--------|-------------------------------------------------|-----------------------------------|
//! | POST   | `/api/v1/ivm/jobs`                              | Create a new IVM job              |
//! | GET    | `/api/v1/ivm/jobs`                              | List all IVM job IDs              |
//! | DELETE | `/api/v1/ivm/jobs/{job_id}`                     | Delete an IVM job                 |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/views`               | Register or update a view         |
//! | DELETE | `/api/v1/ivm/jobs/{job_id}/views/{view_name}`   | Drop a view                       |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/sources/{src}/feed`  | Feed a DeltaBatch (Arrow IPC b64) |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/step`                | Run one IVM tick                  |
//! | GET    | `/api/v1/ivm/jobs/{job_id}/views/{view}/snap`   | Current snapshot (Arrow IPC b64)  |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/checkpoint`          | Serialize state to bytes (b64)    |
//! | POST   | `/api/v1/ivm/jobs/{job_id}/restore`             | Restore state from bytes (b64)    |

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use krishiv_ivm::{DeltaBatch, IncrementalViewSpec, deserialize_delta_batch, serialize_delta_batch};

use crate::ivm::SharedIvmJobRegistry;

// ── helpers ───────────────────────────────────────────────────────────────────

fn ivm_err(msg: impl std::fmt::Display) -> StatusCode {
    tracing::warn!("IVM error: {msg}");
    StatusCode::BAD_REQUEST
}

fn ivm_not_found(job_id: &str) -> StatusCode {
    tracing::warn!("IVM job not found: {job_id}");
    StatusCode::NOT_FOUND
}

// ── schema JSON ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SchemaFieldJson {
    pub name: String,
    /// Arrow DataType as a string: "Int32", "Int64", "Float32", "Float64",
    /// "Utf8", "LargeUtf8", "Boolean", "Binary", "TimestampMs".
    pub data_type: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Deserialize)]
pub struct SchemaJson {
    pub fields: Vec<SchemaFieldJson>,
}

fn parse_schema(s: &SchemaJson) -> Option<arrow::datatypes::SchemaRef> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let fields: Option<Vec<Field>> = s
        .fields
        .iter()
        .map(|f| {
            let dt = match f.data_type.as_str() {
                "Int8" => Some(DataType::Int8),
                "Int16" => Some(DataType::Int16),
                "Int32" => Some(DataType::Int32),
                "Int64" => Some(DataType::Int64),
                "UInt8" => Some(DataType::UInt8),
                "UInt16" => Some(DataType::UInt16),
                "UInt32" => Some(DataType::UInt32),
                "UInt64" => Some(DataType::UInt64),
                "Float32" => Some(DataType::Float32),
                "Float64" => Some(DataType::Float64),
                "Utf8" => Some(DataType::Utf8),
                "LargeUtf8" => Some(DataType::LargeUtf8),
                "Boolean" => Some(DataType::Boolean),
                "Binary" => Some(DataType::Binary),
                "TimestampMs" => {
                    Some(DataType::Timestamp(TimeUnit::Millisecond, None))
                }
                "TimestampUs" => {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                "Date32" => Some(DataType::Date32),
                "Date64" => Some(DataType::Date64),
                _ => None,
            }?;
            Some(Field::new(f.name.clone(), dt, f.nullable))
        })
        .collect();
    Some(std::sync::Arc::new(Schema::new(fields?)))
}

// ── POST /api/v1/ivm/jobs ─────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateJobRequest {
    /// Optional explicit job ID. If absent, a UUID v4 is generated.
    pub job_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateJobResponse {
    pub job_id: String,
}

pub async fn api_ivm_create_job(
    State(registry): State<SharedIvmJobRegistry>,
    Json(body): Json<CreateJobRequest>,
) -> Result<Json<CreateJobResponse>, StatusCode> {
    let job_id = body
        .job_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    registry
        .create(job_id.clone())
        .map_err(|e| ivm_err(e))?;
    Ok(Json(CreateJobResponse { job_id }))
}

// ── GET /api/v1/ivm/jobs ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ListJobsResponse {
    pub job_ids: Vec<String>,
}

pub async fn api_ivm_list_jobs(
    State(registry): State<SharedIvmJobRegistry>,
) -> Json<ListJobsResponse> {
    Json(ListJobsResponse { job_ids: registry.job_ids() })
}

// ── DELETE /api/v1/ivm/jobs/{job_id} ─────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct DeleteJobResponse {
    pub deleted: bool,
}

pub async fn api_ivm_delete_job(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Json<DeleteJobResponse> {
    Json(DeleteJobResponse { deleted: registry.delete(&job_id) })
}

// ── POST /api/v1/ivm/jobs/{job_id}/views ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RegisterViewRequest {
    pub name: String,
    pub body_sql: String,
    pub output_schema: SchemaJson,
    #[serde(default)]
    pub is_materialized: bool,
    #[serde(default)]
    pub is_recursive: bool,
}

#[derive(Debug, Serialize)]
pub struct RegisterViewResponse {
    pub success: bool,
}

pub async fn api_ivm_register_view(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RegisterViewRequest>,
) -> Result<Json<RegisterViewResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let output_schema =
        parse_schema(&body.output_schema).ok_or_else(|| ivm_err("invalid output_schema"))?;
    let spec = IncrementalViewSpec {
        name: body.name,
        body_sql: body.body_sql,
        output_schema,
        is_materialized: body.is_materialized,
        is_recursive: body.is_recursive,
        lateness: vec![],
    };
    flow.register_view(spec).map_err(|e| ivm_err(e))?;
    Ok(Json(RegisterViewResponse { success: true }))
}

// ── DELETE /api/v1/ivm/jobs/{job_id}/views/{view_name} ───────────────────────

#[derive(Debug, Serialize)]
pub struct DropViewResponse {
    pub dropped: bool,
}

pub async fn api_ivm_drop_view(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<DropViewResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let dropped = flow.drop_view(&view_name).map_err(|e| ivm_err(e))?;
    Ok(Json(DropViewResponse { dropped }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{src}/feed ────────────────────────

#[derive(Debug, Deserialize)]
pub struct FeedSourceRequest {
    /// Base64-encoded Arrow IPC bytes of a serialized `DeltaBatch`.
    pub delta_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct FeedSourceResponse {
    pub success: bool,
}

pub async fn api_ivm_feed_source(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<FeedSourceRequest>,
) -> Result<Json<FeedSourceResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.delta_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    let delta = deserialize_delta_batch(&ipc_bytes).map_err(|e| ivm_err(e))?;
    flow.feed_source(source_name, delta).map_err(|e| ivm_err(e))?;
    Ok(Json(FeedSourceResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/step ──────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StepResponse {
    pub active_views: usize,
    pub total_output_rows: usize,
    pub tick: u64,
}

pub async fn api_ivm_step(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Result<Json<StepResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let summary = flow.step_datafusion().await.map_err(|e| {
        tracing::error!("IVM step error for job {job_id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let tick = flow.tick().unwrap_or(0);
    Ok(Json(StepResponse {
        active_views: summary.active_views,
        total_output_rows: summary.total_output_rows,
        tick,
    }))
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/snap ─────────────────────

#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    /// Base64-encoded Arrow IPC bytes of a `DeltaBatch` (all +1 weights).
    pub snapshot_ipc_b64: Option<String>,
    pub num_rows: usize,
}

pub async fn api_ivm_snapshot(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<SnapshotResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let rb_opt = flow.source_snapshot(&view_name).map_err(|e| ivm_err(e))?;
    match rb_opt {
        None => Ok(Json(SnapshotResponse { snapshot_ipc_b64: None, num_rows: 0 })),
        Some(rb) => {
            let num_rows = rb.num_rows();
            let delta = DeltaBatch::from_inserts(rb).map_err(|e| ivm_err(e))?;
            let ipc = serialize_delta_batch(&delta).map_err(|e| ivm_err(e))?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(SnapshotResponse { snapshot_ipc_b64: Some(b64), num_rows }))
        }
    }
}

// ── GET /api/v1/ivm/jobs/{job_id}/views/{view_name}/output ───────────────────

#[derive(Debug, Serialize)]
pub struct ViewOutputResponse {
    /// Base64-encoded Arrow IPC of the latest delta (may be None if no output yet).
    pub delta_ipc_b64: Option<String>,
    pub num_rows: usize,
}

pub async fn api_ivm_view_output(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, view_name)): Path<(String, String)>,
) -> Result<Json<ViewOutputResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    // Peek at the current watch value without blocking.
    let rx = flow.view_output_stream(&view_name).map_err(|e| ivm_err(e))?;
    let guard = rx.borrow();
    match guard.as_ref() {
        None => Ok(Json(ViewOutputResponse { delta_ipc_b64: None, num_rows: 0 })),
        Some(delta) => {
            let num_rows = delta.num_rows();
            let ipc = serialize_delta_batch(delta).map_err(|e| ivm_err(e))?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(ViewOutputResponse { delta_ipc_b64: Some(b64), num_rows }))
        }
    }
}

// ── POST /api/v1/ivm/jobs/{job_id}/checkpoint ────────────────────────────────

#[derive(Debug, Serialize)]
pub struct CheckpointResponse {
    /// Base64-encoded checkpoint bytes (Arrow IPC length-prefix format).
    pub checkpoint_b64: String,
}

pub async fn api_ivm_checkpoint(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = flow.checkpoint().map_err(|e| ivm_err(e))?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    Ok(Json(CheckpointResponse { checkpoint_b64: b64 }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/restore ───────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    pub checkpoint_b64: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreResponse {
    pub success: bool,
}

pub async fn api_ivm_restore(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>, StatusCode> {
    let flow = registry.get(&job_id).ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    flow.restore(&bytes).map_err(|e| ivm_err(e))?;
    Ok(Json(RestoreResponse { success: true }))
}

// ── Router builder ────────────────────────────────────────────────────────────

use axum::Router;
use axum::routing::{delete, get, post};

/// Build the IVM sub-router with all endpoints wired up.
///
/// The returned `Router<()>` has the registry state already baked in and can
/// be merged into the main coordinator router.
pub fn ivm_router(registry: SharedIvmJobRegistry) -> Router<()> {
    Router::new()
        .route("/api/v1/ivm/jobs", post(api_ivm_create_job).get(api_ivm_list_jobs))
        .route("/api/v1/ivm/jobs/{job_id}", delete(api_ivm_delete_job))
        .route("/api/v1/ivm/jobs/{job_id}/views", post(api_ivm_register_view))
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}",
            delete(api_ivm_drop_view),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/feed",
            post(api_ivm_feed_source),
        )
        .route("/api/v1/ivm/jobs/{job_id}/step", post(api_ivm_step))
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/snap",
            get(api_ivm_snapshot),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}/output",
            get(api_ivm_view_output),
        )
        .route("/api/v1/ivm/jobs/{job_id}/checkpoint", post(api_ivm_checkpoint))
        .route("/api/v1/ivm/jobs/{job_id}/restore", post(api_ivm_restore))
        .with_state(registry)
}
