//! HTTP handler for coordinated bounded window execution.

use arrow::ipc::reader::StreamReader;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use krishiv_plan::window::WindowExecutionSpec;

use crate::SharedCoordinator;
use crate::batch_sql::decode_inline_record_batches;
use crate::bounded_window::execute_bounded_window_coordinated;

#[derive(Debug, Deserialize)]
pub struct BoundedWindowRequest {
    pub topic: String,
    pub spec: WindowExecutionSpec,
    /// Arrow IPC stream bytes, base64-encoded. Input batches travel over HTTP
    /// rather than inside the task fragment string.
    pub input_batches_b64: String,
}

#[derive(Debug, Serialize)]
pub struct BoundedWindowResponse {
    pub job_id: String,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

pub async fn api_bounded_window(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<BoundedWindowRequest>,
) -> Result<Json<BoundedWindowResponse>, StatusCode> {
    let input_batches = decode_batches_b64(&body.input_batches_b64).map_err(|e| {
        tracing::error!(error = %e, "bounded-window: input decode failed");
        StatusCode::BAD_REQUEST
    })?;

    let outcome =
        execute_bounded_window_coordinated(&coordinator, &body.topic, &body.spec, &input_batches)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "execute_bounded_window_coordinated failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

    Ok(Json(BoundedWindowResponse {
        job_id: outcome.job_id.as_str().to_owned(),
        inline_record_batch_ipc: outcome.inline_record_batch_ipc,
    }))
}

fn decode_batches_b64(b64: &str) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if b64.is_empty() {
        return Ok(vec![]);
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("base64 decode: {e}"))?;
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| format!("ipc decode: {e}"))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("ipc read: {e}"))
}

#[derive(Debug, Serialize)]
pub struct BoundedWindowDecodeResponse {
    pub row_count: usize,
    pub batch_count: usize,
}

pub async fn api_bounded_window_decode_preview(
    Json(body): Json<BoundedWindowResponse>,
) -> Result<Json<BoundedWindowDecodeResponse>, StatusCode> {
    let decoded = decode_inline_record_batches(&body.inline_record_batch_ipc)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(BoundedWindowDecodeResponse {
        row_count: decoded.iter().map(|b| b.num_rows()).sum(),
        batch_count: decoded.len(),
    }))
}
