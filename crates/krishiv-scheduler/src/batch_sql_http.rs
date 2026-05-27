//! HTTP handlers for coordinated batch SQL.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::SharedCoordinator;
use crate::batch_sql::{
    BatchSqlTable, decode_inline_record_batches, execute_batch_sql_coordinated,
};

#[derive(Debug, Deserialize)]
pub struct BatchSqlRequest {
    pub query: String,
    #[serde(default)]
    pub tables: Vec<BatchSqlTableJson>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BatchSqlTableJson {
    pub table_name: String,
    pub path: String,
}

#[derive(Debug, Serialize)]
pub struct BatchSqlResponse {
    pub job_id: String,
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

pub async fn api_batch_sql(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<BatchSqlRequest>,
) -> Result<Json<BatchSqlResponse>, StatusCode> {
    if body.query.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tables: Vec<BatchSqlTable> = body
        .tables
        .into_iter()
        .map(|t| BatchSqlTable {
            table_name: t.table_name,
            path: t.path.into(),
        })
        .collect();
    let outcome = execute_batch_sql_coordinated(&coordinator, &body.query, &tables)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(BatchSqlResponse {
        job_id: outcome.job_id.as_str().to_owned(),
        inline_record_batch_ipc: outcome.inline_record_batch_ipc,
    }))
}

#[derive(Debug, Serialize)]
pub struct BatchSqlDecodeResponse {
    pub row_count: usize,
    pub batch_count: usize,
}

pub async fn api_batch_sql_decode_preview(
    Json(body): Json<BatchSqlResponse>,
) -> Result<Json<BatchSqlDecodeResponse>, StatusCode> {
    let decoded = decode_inline_record_batches(&body.inline_record_batch_ipc)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let row_count = decoded.iter().map(|b| b.num_rows()).sum();
    Ok(Json(BatchSqlDecodeResponse {
        row_count,
        batch_count: decoded.len(),
    }))
}
