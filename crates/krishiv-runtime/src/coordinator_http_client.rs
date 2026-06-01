//! HTTP client for the cluster control plane APIs.

use base64::Engine as _;
use krishiv_scheduler::decode_inline_record_batches;

use crate::in_process::BatchSqlTable;
use crate::{RuntimeError, RuntimeResult};

/// Build a `reqwest::Client` with bundled Mozilla root certificates.
///
/// Using `reqwest::Client::new()` with the `rustls` feature calls
/// `rustls-platform-verifier`, which reads from the OS certificate store.
/// In minimal containers (scratch, Alpine without ca-certificates, air-gapped
/// environments) the store may be empty and the call panics.
///
/// This helper loads Mozilla's trusted CA roots at compile time via
/// `webpki-root-certs` so the binary is self-contained and the client never
/// panics due to a missing system cert store.
fn coordinator_http_client() -> RuntimeResult<reqwest::Client> {
    let mut builder = reqwest::ClientBuilder::new();
    for der in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        if let Ok(cert) = reqwest::Certificate::from_der(der) {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
        .build()
        .map_err(|e| RuntimeError::transport(format!("HTTP client build failed: {e}")))
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

// ── Batch SQL ──────────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct BatchSqlRequestBody {
    query: String,
    /// Inline Arrow IPC tables (base64-encoded).
    tables: Vec<BatchSqlInlineTableJson>,
}

#[derive(serde::Serialize)]
struct BatchSqlInlineTableJson {
    table_name: String,
    ipc_b64: String,
}

#[derive(serde::Deserialize)]
struct BatchSqlResponseBody {
    #[allow(dead_code)]
    job_id: String,
    state: String,
    #[serde(default)]
    inline_record_batch_ipc: Vec<Vec<u8>>,
    #[serde(default)]
    error: Option<String>,
}

/// Convert a local Parquet file to Arrow IPC bytes (base64-encoded).
///
/// Called before sending to the coordinator so executor pods need no access
/// to the client's local filesystem — the data travels inline in the task
/// assignment.
fn parquet_to_ipc_b64(path: &std::path::Path) -> RuntimeResult<String> {
    use arrow::ipc::writer::StreamWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path).map_err(|e| {
        RuntimeError::transport(format!(
            "failed to open parquet '{}': {e}",
            path.display()
        ))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        RuntimeError::transport(format!(
            "failed to build parquet reader for '{}': {e}",
            path.display()
        ))
    })?;
    let reader = builder.build().map_err(|e| {
        RuntimeError::transport(format!("parquet reader build failed: {e}"))
    })?;

    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RuntimeError::transport(format!("parquet read failed: {e}")))?;

    if batches.is_empty() {
        return Ok(String::new());
    }

    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| RuntimeError::transport(format!("ipc writer failed: {e}")))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| RuntimeError::transport(format!("ipc write failed: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RuntimeError::transport(format!("ipc finish failed: {e}")))?;
    }

    Ok(base64::engine::general_purpose::STANDARD.encode(&buf))
}

/// Execute batch SQL via the coordinator using an async submit-then-poll pattern.
///
/// 1. `POST /api/v1/batch-sql/submit` — submits the job, returns `job_id` immediately.
/// 2. `GET  /api/v1/batch-sql/{job_id}` — polls until the job reaches a terminal state.
///
/// This avoids holding a long-lived HTTP connection open while the job runs.
/// Parquet files referenced by `tables` are converted to inline Arrow IPC bytes
/// so executor pods need no shared filesystem.
pub async fn execute_coordinator_batch_sql(
    coordinator_http: &str,
    query: &str,
    tables: &[BatchSqlTable],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let base = normalize_http_base(coordinator_http)?;

    // Step 1: convert local parquet files to inline IPC and submit.
    let inline_tables: Vec<BatchSqlInlineTableJson> = tables
        .iter()
        .map(|t| {
            let ipc_b64 = parquet_to_ipc_b64(&t.path)?;
            Ok(BatchSqlInlineTableJson {
                table_name: t.table_name.clone(),
                ipc_b64,
            })
        })
        .collect::<RuntimeResult<_>>()?;

    let submit_body = BatchSqlRequestBody {
        query: query.to_string(),
        tables: inline_tables,
    };

    let client = coordinator_http_client()?;
    let submit_url = format!("{base}/api/v1/batch-sql/submit");
    let submit_resp = client
        .post(&submit_url)
        .json(&submit_body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql submit failed: {e}")))?;

    if !submit_resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "transport error: batch-sql HTTP {} from {submit_url}",
            submit_resp.status()
        )));
    }

    #[derive(serde::Deserialize)]
    struct SubmitResponse {
        job_id: String,
    }
    let job_id = submit_resp
        .json::<SubmitResponse>()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql submit decode failed: {e}")))?
        .job_id;

    // Step 2: poll until terminal state (timeout matches coordinator-side deadline).
    let poll_url = format!("{base}/api/v1/batch-sql/{job_id}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
    let mut backoff_ms = 50u64;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(RuntimeError::transport(format!(
                "batch-sql job {job_id} timed out after 300s"
            )));
        }

        let poll_resp = client
            .get(&poll_url)
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll failed: {e}")))?;

        if !poll_resp.status().is_success() {
            return Err(RuntimeError::transport(format!(
                "batch-sql poll HTTP {} from {poll_url}",
                poll_resp.status()
            )));
        }

        let payload: BatchSqlResponseBody = poll_resp.json().await.map_err(|e| {
            RuntimeError::transport(format!("batch-sql poll decode failed: {e}"))
        })?;

        match payload.state.as_str() {
            "Succeeded" => {
                return decode_inline_record_batches(&payload.inline_record_batch_ipc)
                    .map_err(RuntimeError::transport);
            }
            "Failed" | "Cancelled" => {
                return Err(RuntimeError::transport(format!(
                    "batch-sql job {job_id} finished in state {}{}",
                    payload.state,
                    payload
                        .error
                        .map(|e| format!(": {e}"))
                        .unwrap_or_default()
                )));
            }
            _ => {
                // Still running — back off then retry.
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(500);
            }
        }
    }
}

/// Execute batch SQL via the coordinator with **pre-encoded inline IPC** tables.
///
/// Called from the flight server when the client sent `RegisterParquetIpc`
/// directives.  The IPC bytes were encoded client-side; this function never
/// reads any local filesystem.
pub async fn execute_coordinator_batch_sql_inline(
    coordinator_http: &str,
    query: &str,
    tables: &[krishiv_scheduler::BatchSqlInlineTable],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let base = normalize_http_base(coordinator_http)?;

    let submit_body = BatchSqlRequestBody {
        query: query.to_string(),
        tables: tables
            .iter()
            .map(|t| BatchSqlInlineTableJson {
                table_name: t.table_name.clone(),
                ipc_b64: t.ipc_b64.clone(),
            })
            .collect(),
    };

    let client = coordinator_http_client()?;
    let submit_url = format!("{base}/api/v1/batch-sql/submit");
    let submit_resp = client
        .post(&submit_url)
        .json(&submit_body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql submit failed: {e}")))?;

    if !submit_resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "transport error: batch-sql HTTP {} from {submit_url}",
            submit_resp.status()
        )));
    }

    #[derive(serde::Deserialize)]
    struct SubmitResponse {
        job_id: String,
    }
    let job_id = submit_resp
        .json::<SubmitResponse>()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql submit decode failed: {e}")))?
        .job_id;

    let poll_url = format!("{base}/api/v1/batch-sql/{job_id}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
    let mut backoff_ms = 50u64;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(RuntimeError::transport(format!(
                "batch-sql job {job_id} timed out after 300s"
            )));
        }
        let poll_resp = client
            .get(&poll_url)
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll failed: {e}")))?;
        if !poll_resp.status().is_success() {
            return Err(RuntimeError::transport(format!(
                "batch-sql poll HTTP {} from {poll_url}",
                poll_resp.status()
            )));
        }
        let payload: BatchSqlResponseBody = poll_resp.json().await.map_err(|e| {
            RuntimeError::transport(format!("batch-sql poll decode failed: {e}"))
        })?;
        match payload.state.as_str() {
            "Succeeded" => {
                return decode_inline_record_batches(&payload.inline_record_batch_ipc)
                    .map_err(RuntimeError::transport);
            }
            "Failed" | "Cancelled" => {
                return Err(RuntimeError::transport(format!(
                    "batch-sql job {job_id} finished in state {}{}",
                    payload.state,
                    payload.error.map(|e| format!(": {e}")).unwrap_or_default()
                )));
            }
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(500);
            }
        }
    }
}

// ── Bounded Window ─────────────────────────────────────────────────────────────

/// Execute a bounded window via `POST /api/v1/bounded-window` on the coordinator.
pub async fn execute_coordinator_bounded_window(
    coordinator_http: &str,
    topic: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
    input_batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    use crate::flight_action::encode_batches;

    #[derive(serde::Serialize)]
    struct BoundedWindowRequest<'a> {
        topic: &'a str,
        spec: &'a krishiv_plan::window::WindowExecutionSpec,
        input_batches_b64: String,
    }

    #[derive(serde::Deserialize)]
    struct BoundedWindowResponse {
        job_id: String,
        inline_record_batch_ipc: Vec<Vec<u8>>,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/bounded-window");
    let input_batches_b64 = encode_batches(input_batches)?;
    let body = BoundedWindowRequest { topic, spec, input_batches_b64 };

    let client = coordinator_http_client()?;
    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("bounded-window HTTP request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "bounded-window HTTP {} from {url}",
            response.status()
        )));
    }

    let payload: BoundedWindowResponse = response.json().await.map_err(|e| {
        RuntimeError::transport(format!("bounded-window HTTP response decode failed: {e}"))
    })?;
    let _ = payload.job_id;
    decode_inline_record_batches(&payload.inline_record_batch_ipc).map_err(RuntimeError::transport)
}

#[cfg(test)]
mod tests {
    use super::normalize_http_base;

    #[test]
    fn normalize_http_base_empty_fails() {
        let err = normalize_http_base("").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn normalize_http_base_whitespace_only_fails() {
        let err = normalize_http_base("   ").unwrap_err();
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn normalize_http_base_http_unchanged() {
        let result = normalize_http_base("http://localhost:8080").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_https_unchanged() {
        let result = normalize_http_base("https://cluster.example.com").unwrap();
        assert_eq!(result, "https://cluster.example.com");
    }

    #[test]
    fn normalize_http_base_bare_adds_http() {
        let result = normalize_http_base("localhost:8080").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_strips_trailing_slash() {
        let result = normalize_http_base("http://localhost:8080/").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_strips_trailing_slashes() {
        let result = normalize_http_base("http://localhost:8080///").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_trims_whitespace() {
        let result = normalize_http_base("  http://localhost:8080  ").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_bare_trailing_slash() {
        let result = normalize_http_base("localhost:8080/").unwrap();
        assert_eq!(result, "http://localhost:8080");
    }

    #[test]
    fn normalize_http_base_preserves_path() {
        let result = normalize_http_base("http://host:8080/api/v1").unwrap();
        assert_eq!(result, "http://host:8080/api/v1");
    }
}
