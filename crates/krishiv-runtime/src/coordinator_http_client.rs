//! HTTP client for the cluster control plane batch-SQL API.

use krishiv_scheduler::decode_inline_record_batches;

use crate::in_process::BatchSqlTable;

use crate::{RuntimeError, RuntimeResult};

#[derive(serde::Serialize)]
struct BatchSqlRequestBody {
    query: String,
    tables: Vec<BatchSqlTableJson>,
}

#[derive(serde::Deserialize)]
struct BatchSqlResponseBody {
    job_id: String,
    inline_record_batch_ipc: Vec<Vec<u8>>,
}

#[derive(serde::Serialize)]
struct BatchSqlTableJson {
    table_name: String,
    path: String,
}

fn normalize_http_base(url: &str) -> RuntimeResult<String> {
    let trimmed = url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(RuntimeError::transport(
            "coordinator HTTP URL must not be empty",
        ));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

/// Execute batch SQL via `POST /api/v1/batch-sql` on the cluster control plane.
pub async fn execute_coordinator_batch_sql(
    coordinator_http: &str,
    query: &str,
    tables: &[BatchSqlTable],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/batch-sql");
    let body = BatchSqlRequestBody {
        query: query.to_string(),
        tables: tables
            .iter()
            .map(|t| BatchSqlTableJson {
                table_name: t.table_name.clone(),
                path: t.path.to_string_lossy().into_owned(),
            })
            .collect(),
    };
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql HTTP request failed: {e}")))?;
    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "batch-sql HTTP {} from {url}",
            response.status()
        )));
    }
    let payload: BatchSqlResponseBody = response.json().await.map_err(|e| {
        RuntimeError::transport(format!("batch-sql HTTP response decode failed: {e}"))
    })?;
    let _job_id = payload.job_id;
    decode_inline_record_batches(&payload.inline_record_batch_ipc).map_err(RuntimeError::transport)
}
