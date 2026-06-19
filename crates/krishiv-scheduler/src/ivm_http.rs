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

use axum::Json;
use axum::extract::{FromRef, Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use krishiv_ivm::{
    DeltaBatch, IncrementalViewSpec, deserialize_delta_batch, serialize_delta_batch,
};

use crate::SharedCoordinator;
use crate::ivm::SharedIvmJobRegistry;

// ── combined router state ─────────────────────────────────────────────────────

/// Router state for IVM endpoints: job registry + coordinator reference.
///
/// Carrying the coordinator enables the step handler to check executor
/// availability and log distributed-compute context (future: offload heavy
/// IVM computation to registered executors rather than always running on the
/// coordinator).
#[derive(Clone)]
pub struct IvmRouterState {
    pub registry: SharedIvmJobRegistry,
    pub coordinator: SharedCoordinator,
}

impl FromRef<IvmRouterState> for SharedIvmJobRegistry {
    fn from_ref(state: &IvmRouterState) -> Self {
        state.registry.clone()
    }
}

impl FromRef<IvmRouterState> for SharedCoordinator {
    fn from_ref(state: &IvmRouterState) -> Self {
        state.coordinator.clone()
    }
}

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
                "TimestampMs" => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
                "TimestampUs" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
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
    registry.create(job_id.clone()).map_err(ivm_err)?;
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
    Json(ListJobsResponse {
        job_ids: registry.job_ids(),
    })
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
    Json(DeleteJobResponse {
        deleted: registry.delete(&job_id),
    })
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
    // Existence is enforced by the registry (which also decides, on the first
    // view, whether to auto-partition the job by a single-column GROUP BY key).
    if registry.get(&job_id).is_none() {
        return Err(ivm_not_found(&job_id));
    }
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
    registry.register_view(&job_id, spec).map_err(ivm_err)?;
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
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let dropped = flow.drop_view(&view_name).map_err(ivm_err)?;
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
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.delta_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // G7: drop zero-weight rows on ingress so downstream operators never see them.
    let delta = deserialize_delta_batch(&ipc_bytes)
        .map_err(ivm_err)?
        .drop_zeros()
        .map_err(ivm_err)?;
    flow.feed(&source_name, delta).map_err(ivm_err)?;
    Ok(Json(FeedSourceResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{src}/stream-delta ────────────────
//
// Fast path for producers that already emit pre-computed ±1 DeltaBatches
// (CDC-native connectors, Debezium readers) and do not need the snapshot-diff
// overhead of the /stream-bridge endpoint.

#[derive(Debug, Deserialize)]
pub struct FeedStreamDeltaRequest {
    /// Base64-encoded Arrow IPC bytes of a pre-computed `DeltaBatch`.
    pub delta_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct FeedStreamDeltaResponse {
    pub success: bool,
}

pub async fn api_ivm_feed_stream_delta(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<FeedStreamDeltaRequest>,
) -> Result<Json<FeedStreamDeltaResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.delta_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    let delta = deserialize_delta_batch(&ipc_bytes)
        .map_err(ivm_err)?
        .drop_zeros()
        .map_err(ivm_err)?;
    // Pre-computed delta: feed directly (same as /feed; the distinct route is
    // kept for coordinator API/wire compatibility with CDC-native producers).
    flow.feed(&source_name, delta).map_err(ivm_err)?;
    Ok(Json(FeedStreamDeltaResponse { success: true }))
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
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<StepResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;

    // Log executor availability for observability. IVM computation always runs
    // centrally on the coordinator (distributed ingestion, centralized compute).
    // Executor-side dispatch via delta:step: fragments is a future optimization.
    let executor_count = coordinator.read().await.executor_snapshots().len();
    if executor_count > 0 {
        tracing::debug!(
            job_id = %job_id,
            executors = executor_count,
            "IVM step: executors registered; computing centrally on coordinator"
        );
    }

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
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let rb_opt = flow.source_snapshot(&view_name).map_err(ivm_err)?;
    match rb_opt {
        None => Ok(Json(SnapshotResponse {
            snapshot_ipc_b64: None,
            num_rows: 0,
        })),
        Some(rb) => {
            let num_rows = rb.num_rows();
            let delta = DeltaBatch::from_inserts(rb).map_err(ivm_err)?;
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(SnapshotResponse {
                snapshot_ipc_b64: Some(b64),
                num_rows,
            }))
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
    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    // Peek the latest output delta (merged across shards for partitioned jobs).
    match job.view_output_peek(&view_name).map_err(ivm_err)? {
        None => Ok(Json(ViewOutputResponse {
            delta_ipc_b64: None,
            num_rows: 0,
        })),
        Some(delta) => {
            let num_rows = delta.num_rows();
            let ipc = serialize_delta_batch(&delta).map_err(ivm_err)?;
            let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
            Ok(Json(ViewOutputResponse {
                delta_ipc_b64: Some(b64),
                num_rows,
            }))
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
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = flow.checkpoint().map_err(ivm_err)?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    Ok(Json(CheckpointResponse {
        checkpoint_b64: b64,
    }))
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
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    flow.restore(&bytes).map_err(ivm_err)?;
    Ok(Json(RestoreResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/checkpoint-delta ──────────────────────────

#[derive(Debug, Serialize)]
pub struct CheckpointDeltaResponse {
    /// Base64-encoded delta checkpoint bytes.
    pub checkpoint_delta_b64: String,
}

pub async fn api_ivm_checkpoint_delta(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
) -> Result<Json<CheckpointDeltaResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = flow.checkpoint_delta().map_err(ivm_err)?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
    Ok(Json(CheckpointDeltaResponse {
        checkpoint_delta_b64: b64,
    }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/restore-delta ─────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RestoreDeltaRequest {
    pub checkpoint_delta_b64: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreDeltaResponse {
    pub success: bool,
}

pub async fn api_ivm_restore_delta(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RestoreDeltaRequest>,
) -> Result<Json<RestoreDeltaResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.checkpoint_delta_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    flow.restore_delta(&bytes).map_err(ivm_err)?;
    Ok(Json(RestoreDeltaResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-bridge ───────

#[derive(Debug, Deserialize)]
pub struct StreamBridgeRequest {
    /// Base64-encoded Arrow IPC bytes for one or more RecordBatches (full snapshot).
    pub snapshot_ipc_b64: String,
}

#[derive(Debug, Serialize)]
pub struct StreamBridgeResponse {
    pub success: bool,
}

pub async fn api_ivm_stream_bridge(
    State(registry): State<SharedIvmJobRegistry>,
    Path((job_id, source_name)): Path<(String, String)>,
    Json(body): Json<StreamBridgeRequest>,
) -> Result<Json<StreamBridgeResponse>, StatusCode> {
    let flow = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;
    let ipc_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &body.snapshot_ipc_b64,
    )
    .map_err(|e| ivm_err(format!("base64 decode: {e}")))?;
    // Decode Arrow IPC stream to RecordBatches.
    let batches = {
        use arrow::ipc::reader::StreamReader;
        let cursor = std::io::Cursor::new(&ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| ivm_err(format!("IPC stream open: {e}")))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ivm_err(format!("IPC stream read: {e}")))?
    };
    flow.feed_snapshot(&source_name, &batches)
        .map_err(ivm_err)?;
    Ok(Json(StreamBridgeResponse { success: true }))
}

// ── POST /api/v1/ivm/jobs/{job_id}/vector-views ───────────────────────────────

use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RegisterVectorViewRequest {
    pub view_name: String,
    pub id_column: String,
    pub vector_column: String,
    /// Sink type: currently only "in_memory" is supported via HTTP.
    #[serde(default = "default_sink_type")]
    pub sink_type: String,
}

fn default_sink_type() -> String {
    "in_memory".to_string()
}

#[derive(Debug, Serialize)]
pub struct RegisterVectorViewResponse {
    pub success: bool,
    pub view_name: String,
}

pub async fn api_ivm_register_vector_view(
    State(registry): State<SharedIvmJobRegistry>,
    Path(job_id): Path<String>,
    Json(body): Json<RegisterVectorViewRequest>,
) -> Result<Json<RegisterVectorViewResponse>, StatusCode> {
    use krishiv_ivm::VectorViewSpec;

    let job = registry
        .get(&job_id)
        .ok_or_else(|| ivm_not_found(&job_id))?;

    if body.sink_type != "in_memory" {
        return Err(ivm_err(format!(
            "unsupported sink_type '{}'; only 'in_memory' is supported via HTTP",
            body.sink_type
        )));
    }

    let sink: Arc<dyn krishiv_ivm::IvmVectorSink> = krishiv_ivm::InMemoryVectorSink::new();
    let spec = VectorViewSpec {
        view_name: body.view_name.clone(),
        id_column: body.id_column.clone(),
        vector_column: body.vector_column.clone(),
        sink,
    };

    // Spawn and detach; one task per shard (partitioned jobs write a shared sink).
    // Tasks run until the flow is dropped.
    job.spawn_vector_views(spec).map_err(ivm_err)?;

    Ok(Json(RegisterVectorViewResponse {
        success: true,
        view_name: body.view_name,
    }))
}

// ── Router builder ────────────────────────────────────────────────────────────

use axum::Router;
use axum::routing::{delete, get, post};

/// Build the IVM sub-router with all endpoints wired up.
///
/// The returned `Router<()>` has combined `IvmRouterState` baked in and can
/// be merged into the main coordinator router.
pub fn ivm_router(state: IvmRouterState) -> Router<()> {
    Router::new()
        // Unified submit endpoint — dispatches by `kind` field.
        .route(
            "/api/v1/jobs",
            post(crate::unified_jobs_http::api_unified_submit),
        )
        .route(
            "/api/v1/ivm/jobs",
            post(api_ivm_create_job).get(api_ivm_list_jobs),
        )
        .route("/api/v1/ivm/jobs/{job_id}", delete(api_ivm_delete_job))
        .route(
            "/api/v1/ivm/jobs/{job_id}/views",
            post(api_ivm_register_view),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/views/{view_name}",
            delete(api_ivm_drop_view),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/feed",
            post(api_ivm_feed_source),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-bridge",
            post(api_ivm_stream_bridge),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-delta",
            post(api_ivm_feed_stream_delta),
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
        .route(
            "/api/v1/ivm/jobs/{job_id}/checkpoint",
            post(api_ivm_checkpoint),
        )
        .route("/api/v1/ivm/jobs/{job_id}/restore", post(api_ivm_restore))
        .route(
            "/api/v1/ivm/jobs/{job_id}/checkpoint-delta",
            post(api_ivm_checkpoint_delta),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/restore-delta",
            post(api_ivm_restore_delta),
        )
        .route(
            "/api/v1/ivm/jobs/{job_id}/vector-views",
            post(api_ivm_register_vector_view),
        )
        .with_state(state)
}
