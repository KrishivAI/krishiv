//! HTTP client for the cluster control plane APIs.

use krishiv_scheduler::configured_coordinator_bearer_token;
use krishiv_scheduler::decode_inline_record_batches;
use krishiv_scheduler::{LiveExecutorView, LiveJobView};

use crate::flight_protocol::parquet_file_to_ipc_b64;
use crate::in_process::BatchSqlTable;
use crate::{RuntimeError, RuntimeResult};

/// Per-request timeout for coordinator HTTP calls (seconds).
const COORDINATOR_HTTP_REQUEST_TIMEOUT_SECS: u64 = 60;

/// Job-level poll deadline for batch-SQL and bounded-window jobs (seconds).
const BOUNDED_WINDOW_POLL_TIMEOUT_SECS: u64 = 300;

/// Maximum coordinator HTTP response size (bytes) — guards against unbounded
/// memory growth when reading large JSON responses.
const COORDINATOR_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Process-global `reqwest::Client` shared across all coordinator HTTP calls.
/// Wrapped in a `Mutex<Option<...>>` so the client is lazily initialized once.
static COORDINATOR_HTTP_CLIENT: std::sync::Mutex<Option<reqwest::Client>> =
    std::sync::Mutex::new(None);

fn coordinator_http_client() -> RuntimeResult<reqwest::Client> {
    let mut guard = COORDINATOR_HTTP_CLIENT
        .lock()
        .map_err(|_| RuntimeError::transport("HTTP client mutex poisoned"))?;
    if let Some(ref client) = *guard {
        return Ok(client.clone());
    }
    // Load Mozilla's trusted CA roots at compile time via `webpki-root-certs`
    // so the binary is self-contained and never panics in containers that lack
    // a system certificate store (scratch, Alpine without ca-certificates, etc.).
    let mut builder = reqwest::ClientBuilder::new();
    for der in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        if let Ok(cert) = reqwest::Certificate::from_der(der) {
            builder = builder.add_root_certificate(cert);
        }
    }
    let client = builder
        // Per-request timeout caps individual HTTP calls.
        // The job-level poll loop enforces a separate deadline,
        // so this guards against TCP-level stalls within a single request.
        .timeout(std::time::Duration::from_secs(
            COORDINATOR_HTTP_REQUEST_TIMEOUT_SECS,
        ))
        .build()
        .map_err(|e| RuntimeError::transport(format!("HTTP client build failed: {e}")))?;
    *guard = Some(client.clone());
    Ok(client)
}

fn apply_coordinator_bearer(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(token) = configured_coordinator_bearer_token() {
        builder.header("Authorization", format!("Bearer {token}"))
    } else {
        builder
    }
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
    #[serde(default)]
    is_streaming: bool,
}

#[derive(serde::Serialize)]
struct BatchSqlInlineTableJson {
    table_name: String,
    ipc_b64: String,
}

#[derive(serde::Deserialize)]
struct BatchSqlResponseBody {
    job_id: String,
    state: String,
    #[serde(default)]
    inline_record_batch_ipc: Vec<Vec<u8>>,
    #[serde(default)]
    error: Option<String>,
}

/// One non-blocking poll result for a coordinator-managed batch SQL job.
#[derive(Debug, Clone)]
pub enum CoordinatorBatchSqlJobResult {
    Pending {
        job_id: String,
        state: String,
    },
    Succeeded {
        job_id: String,
        batches: Vec<arrow::record_batch::RecordBatch>,
    },
    Failed {
        job_id: String,
        error: Option<String>,
    },
    Cancelled {
        job_id: String,
        error: Option<String>,
    },
}

fn batch_sql_job_result_from_payload(
    payload: BatchSqlResponseBody,
) -> RuntimeResult<CoordinatorBatchSqlJobResult> {
    match payload.state.as_str() {
        "Succeeded" => {
            let batches = decode_inline_record_batches(&payload.inline_record_batch_ipc)
                .map_err(RuntimeError::transport)?;
            Ok(CoordinatorBatchSqlJobResult::Succeeded {
                job_id: payload.job_id,
                batches,
            })
        }
        "Failed" => Ok(CoordinatorBatchSqlJobResult::Failed {
            job_id: payload.job_id,
            error: payload.error,
        }),
        "Cancelled" => Ok(CoordinatorBatchSqlJobResult::Cancelled {
            job_id: payload.job_id,
            error: payload.error,
        }),
        state => Ok(CoordinatorBatchSqlJobResult::Pending {
            job_id: payload.job_id,
            state: state.to_owned(),
        }),
    }
}

/// Shared poll loop for batch-SQL jobs.
///
/// First poll is immediate; subsequent non-terminal responses back off
/// exponentially (50 ms → 500 ms) with ±25 % jitter derived from
/// `job_id` bytes so clients started simultaneously don't synchronise
/// their retries on a coordinator restart.
async fn poll_batch_sql_job(
    client: &reqwest::Client,
    poll_url: &str,
    job_id: &str,
    deadline: tokio::time::Instant,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    // Deterministic jitter seed: xor-fold of job_id bytes avoids rand dep.
    let seed: u64 = job_id
        .as_bytes()
        .iter()
        .fold(0u64, |acc, &b| acc ^ (acc << 5).wrapping_add(b as u64));

    let mut backoff_ms: Option<u64> = None;
    loop {
        if let Some(ms) = backoff_ms {
            // Apply ±25 % jitter; minimum 10 ms.
            let jitter_pct = (seed.wrapping_add(ms) % 51) as i64 - 25; // [-25, 25]
            let delta = ms / 100 * jitter_pct.unsigned_abs();
            let jittered = if jitter_pct >= 0 {
                ms.saturating_add(delta)
            } else {
                ms.saturating_sub(delta)
            }
            .max(10);
            tokio::time::sleep(std::time::Duration::from_millis(jittered)).await;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RuntimeError::transport(format!(
                "batch-sql job {job_id} timed out after 300s"
            )));
        }
        let poll_resp = apply_coordinator_bearer(client.get(poll_url))
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll failed: {e}")))?;
        if !poll_resp.status().is_success() {
            return Err(RuntimeError::transport(format!(
                "batch-sql poll HTTP {} from {poll_url}",
                poll_resp.status()
            )));
        }
        let resp_bytes = poll_resp
            .bytes()
            .await
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll read failed: {e}")))?;
        if resp_bytes.len() > COORDINATOR_MAX_RESPONSE_BYTES {
            return Err(RuntimeError::transport(format!(
                "batch-sql poll response exceeded {} MiB limit",
                COORDINATOR_MAX_RESPONSE_BYTES / (1024 * 1024)
            )));
        }
        let payload: BatchSqlResponseBody = serde_json::from_slice(&resp_bytes)
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll decode failed: {e}")))?;
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
                backoff_ms = Some(backoff_ms.map_or(50, |prev| (prev * 2).min(500)));
            }
        }
    }
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
    is_streaming: bool,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    let base = normalize_http_base(coordinator_http)?;

    // Step 1: convert local parquet files to inline IPC and submit.
    // parquet_file_to_ipc_b64 is CPU/IO-bound; run it on the blocking thread pool so
    // the async executor is not stalled while reading and encoding the files.
    let tables_owned: Vec<_> = tables.to_vec();
    let inline_tables: Vec<BatchSqlInlineTableJson> = tokio::task::spawn_blocking(move || {
        tables_owned
            .iter()
            .map(|t| {
                let ipc_b64 = parquet_file_to_ipc_b64(&t.path)?;
                Ok(BatchSqlInlineTableJson {
                    table_name: t.table_name.clone(),
                    ipc_b64,
                })
            })
            .collect::<RuntimeResult<_>>()
    })
    .await
    .map_err(|e| RuntimeError::transport(format!("parquet-to-ipc blocking task failed: {e}")))??;

    let submit_body = BatchSqlRequestBody {
        query: query.to_owned(),
        tables: inline_tables,
        is_streaming,
    };

    let client = coordinator_http_client()?;
    let submit_url = format!("{base}/api/v1/batch-sql/submit");
    let submit_resp = apply_coordinator_bearer(client.post(&submit_url).json(&submit_body))
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

    // Step 2: poll until terminal state.
    let poll_url = format!("{base}/api/v1/batch-sql/{job_id}");
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(BOUNDED_WINDOW_POLL_TIMEOUT_SECS);
    poll_batch_sql_job(&client, &poll_url, &job_id, deadline).await
}

/// Poll one existing coordinator batch-SQL job for materialized results.
///
/// This does not wait for job completion. Callers get the current terminal or
/// non-terminal state and can decide whether to poll again.
pub async fn execute_coordinator_batch_sql_result(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<CoordinatorBatchSqlJobResult> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let poll_url = format!("{base}/api/v1/batch-sql/{}", urlencoding::encode(job_id));
    let poll_resp = apply_coordinator_bearer(client.get(&poll_url))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql result poll failed: {e}")))?;
    if !poll_resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "batch-sql result poll HTTP {} from {poll_url}",
            poll_resp.status()
        )));
    }
    let resp_bytes = poll_resp
        .bytes()
        .await
        .map_err(|e| RuntimeError::transport(format!("batch-sql result poll read failed: {e}")))?;
    if resp_bytes.len() > COORDINATOR_MAX_RESPONSE_BYTES {
        return Err(RuntimeError::transport(format!(
            "batch-sql result poll response exceeded {} MiB limit",
            COORDINATOR_MAX_RESPONSE_BYTES / (1024 * 1024)
        )));
    }
    let payload: BatchSqlResponseBody = serde_json::from_slice(&resp_bytes).map_err(|e| {
        RuntimeError::transport(format!("batch-sql result poll decode failed: {e}"))
    })?;
    batch_sql_job_result_from_payload(payload)
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
    is_streaming: bool,
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
        is_streaming,
    };

    let client = coordinator_http_client()?;
    let submit_url = format!("{base}/api/v1/batch-sql/submit");
    let submit_resp = apply_coordinator_bearer(client.post(&submit_url).json(&submit_body))
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
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(BOUNDED_WINDOW_POLL_TIMEOUT_SECS);
    poll_batch_sql_job(&client, &poll_url, &job_id, deadline).await
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
        inline_record_batch_ipc: Vec<Vec<u8>>,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/bounded-window");
    let input_batches_b64 = encode_batches(input_batches)?;
    let body = BoundedWindowRequest {
        topic,
        spec,
        input_batches_b64,
    };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("bounded-window HTTP request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "bounded-window HTTP {} from {url}",
            response.status()
        )));
    }

    let resp_bytes = response
        .bytes()
        .await
        .map_err(|e| RuntimeError::transport(format!("bounded-window HTTP read failed: {e}")))?;
    if resp_bytes.len() > COORDINATOR_MAX_RESPONSE_BYTES {
        return Err(RuntimeError::transport(format!(
            "bounded-window response exceeded {} MiB limit",
            COORDINATOR_MAX_RESPONSE_BYTES / (1024 * 1024)
        )));
    }
    let payload: BoundedWindowResponse = serde_json::from_slice(&resp_bytes).map_err(|e| {
        RuntimeError::transport(format!("bounded-window HTTP response decode failed: {e}"))
    })?;
    decode_inline_record_batches(&payload.inline_record_batch_ipc).map_err(RuntimeError::transport)
}

#[cfg(test)]
mod tests {
    use super::{
        BatchSqlResponseBody, CoordinatorBatchSqlJobResult, batch_sql_job_result_from_payload,
        normalize_http_base,
    };
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;

    fn one_row_ipc() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![42]))])
                .expect("record batch");
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("ipc writer");
            writer.write(&batch).expect("ipc write");
            writer.finish().expect("ipc finish");
        }
        bytes
    }

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

    #[test]
    fn batch_sql_result_payload_decodes_succeeded_batches() {
        let payload = BatchSqlResponseBody {
            job_id: "job-result".to_owned(),
            state: "Succeeded".to_owned(),
            inline_record_batch_ipc: vec![one_row_ipc()],
            error: None,
        };
        let result = batch_sql_job_result_from_payload(payload).expect("poll result");
        match result {
            CoordinatorBatchSqlJobResult::Succeeded { job_id, batches } => {
                assert_eq!(job_id, "job-result");
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
            }
            other => panic!("expected succeeded result, got {other:?}"),
        }
    }
}

// ── Continuous Streaming ───────────────────────────────────────────────────────

pub async fn execute_coordinator_continuous_register(
    coordinator_http: &str,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
) -> RuntimeResult<()> {
    #[derive(serde::Serialize)]
    struct ContinuousRegisterRequest<'a> {
        job_id: &'a str,
        spec: &'a krishiv_plan::window::WindowExecutionSpec,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-register");
    let body = ContinuousRegisterRequest { job_id, spec };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-register request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "continuous-register HTTP {} from {url}",
            response.status()
        )));
    }
    Ok(())
}

pub async fn execute_coordinator_continuous_push(
    coordinator_http: &str,
    job_id: &str,
    input_batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<()> {
    use crate::flight_action::encode_batches;

    #[derive(serde::Serialize)]
    struct ContinuousPushRequest<'a> {
        job_id: &'a str,
        input_batches_b64: String,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-push");
    let input_batches_b64 = encode_batches(input_batches)?;
    let body = ContinuousPushRequest {
        job_id,
        input_batches_b64,
    };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-push request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "continuous-push HTTP {} from {url}",
            response.status()
        )));
    }
    Ok(())
}

pub async fn execute_coordinator_continuous_drain(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    #[derive(serde::Serialize)]
    struct ContinuousDrainRequest<'a> {
        job_id: &'a str,
    }

    #[derive(serde::Deserialize)]
    struct ContinuousDrainResponse {
        inline_record_batch_ipc: Vec<Vec<u8>>,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-drain");
    let body = ContinuousDrainRequest { job_id };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-drain request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "continuous-drain HTTP {} from {url}",
            response.status()
        )));
    }

    let payload: ContinuousDrainResponse = response.json().await.map_err(|e| {
        RuntimeError::transport(format!("continuous-drain response decode failed: {e}"))
    })?;

    decode_inline_record_batches(&payload.inline_record_batch_ipc)
        .map_err(RuntimeError::transport)
        .and_then(|batches| {
            const MAX_DRAIN_OUTPUT_BYTES: usize = 2 * 1024 * 1024 * 1024;
            let total: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
            if total > MAX_DRAIN_OUTPUT_BYTES {
                return Err(RuntimeError::transport(format!(
                    "coordinator continuous-drain response of {} bytes exceeds the \
                     {MAX_DRAIN_OUTPUT_BYTES}-byte limit",
                    total
                )));
            }
            Ok(batches)
        })
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RemoteContinuousStreamJobView {
    pub job_id: String,
    pub state: String,
    pub task_count: usize,
    pub assigned_task_count: usize,
    pub running_task_count: usize,
    pub succeeded_task_count: usize,
    pub failed_task_count: usize,
    pub last_watermark_ms: Option<i64>,
    pub persisted_watermark_ms: Option<i64>,
    pub snapshot_available: bool,
    pub cycle_in_flight: bool,
    pub spec: krishiv_plan::window::WindowExecutionSpec,
}

#[derive(serde::Deserialize)]
struct RemoteContinuousStreamsResponse {
    streams: Vec<RemoteContinuousStreamJobView>,
}

#[derive(serde::Deserialize)]
struct RemoteContinuousCheckpointResponse {
    job_id: String,
    snapshot_b64: Option<String>,
    watermark_ms: Option<i64>,
    snapshot_available: bool,
    spec: krishiv_plan::window::WindowExecutionSpec,
}

#[derive(Debug, Clone)]
pub struct RemoteContinuousStreamCheckpoint {
    pub job_id: String,
    pub snapshot_bytes: Option<Vec<u8>>,
    pub watermark_ms: Option<i64>,
    pub snapshot_available: bool,
    pub spec: krishiv_plan::window::WindowExecutionSpec,
}

pub async fn execute_coordinator_list_continuous_streams(
    coordinator_http: &str,
) -> RuntimeResult<Vec<RemoteContinuousStreamJobView>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(client.get(format!("{base}/api/v1/continuous")))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("list continuous streams: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "list continuous streams HTTP {}",
            resp.status()
        )));
    }
    let parsed: RemoteContinuousStreamsResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("list continuous streams decode: {e}")))?;
    Ok(parsed.streams)
}

pub async fn execute_coordinator_get_continuous_stream(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Option<RemoteContinuousStreamJobView>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let url = format!("{base}/api/v1/continuous/{}", urlencoding::encode(job_id));
    let resp = apply_coordinator_bearer(client.get(url))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("get continuous stream: {e}")))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "get continuous stream HTTP {}",
            resp.status()
        )));
    }
    let parsed: RemoteContinuousStreamJobView = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("get continuous stream decode: {e}")))?;
    Ok(Some(parsed))
}

pub async fn execute_coordinator_checkpoint_continuous_stream(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<RemoteContinuousStreamCheckpoint> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let url = format!(
        "{base}/api/v1/continuous/{}/checkpoint",
        urlencoding::encode(job_id)
    );
    let resp = apply_coordinator_bearer(client.post(url))
        .json(&serde_json::Value::Null)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("checkpoint continuous stream: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "checkpoint continuous stream HTTP {}",
            resp.status()
        )));
    }
    let parsed: RemoteContinuousCheckpointResponse = resp.json().await.map_err(|e| {
        RuntimeError::transport(format!("checkpoint continuous stream decode: {e}"))
    })?;
    let snapshot_bytes = match parsed.snapshot_b64 {
        Some(snapshot_b64) => Some(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &snapshot_b64)
                .map_err(|e| {
                    RuntimeError::transport(format!(
                        "checkpoint continuous stream base64 decode: {e}"
                    ))
                })?,
        ),
        None => None,
    };
    Ok(RemoteContinuousStreamCheckpoint {
        job_id: parsed.job_id,
        snapshot_bytes,
        watermark_ms: parsed.watermark_ms,
        snapshot_available: parsed.snapshot_available,
        spec: parsed.spec,
    })
}

#[derive(serde::Serialize)]
struct RemoteContinuousRestoreBody {
    snapshot_b64: String,
}

pub async fn execute_coordinator_restore_continuous_stream(
    coordinator_http: &str,
    job_id: &str,
    snapshot_bytes: &[u8],
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let url = format!(
        "{base}/api/v1/continuous/{}/restore",
        urlencoding::encode(job_id)
    );
    let body = RemoteContinuousRestoreBody {
        snapshot_b64: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            snapshot_bytes,
        ),
    };
    let resp = apply_coordinator_bearer(client.post(url))
        .json(&body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("restore continuous stream: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "restore continuous stream HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

/// Execute a physical plan on the coordinator via HTTP (batch SQL or continuous register).
pub async fn execute_coordinator_physical_plan(
    coordinator_http: &str,
    plan: &krishiv_plan::PhysicalPlan,
) -> RuntimeResult<()> {
    use krishiv_plan::ExecutionKind;

    plan.validate()
        .map_err(|error| RuntimeError::plan_rejected(error.to_string()))?;
    match plan.kind() {
        ExecutionKind::Batch => {
            let sql = crate::flight_client::plan_to_sql(plan);
            let _ =
                execute_coordinator_batch_sql_inline(coordinator_http, &sql, &[], false).await?;
            Ok(())
        }
        ExecutionKind::Streaming => {
            let spec = crate::plan::streaming_spec_from_plan(plan)?;
            execute_coordinator_continuous_register(
                coordinator_http,
                plan.name(),
                &spec.to_plan_spec(),
            )
            .await
        }
        ExecutionKind::DeltaBatch => {
            // Create the IVM job idempotently on the coordinator.
            // Plan name is the job ID so subsequent feed/step calls reference it.
            execute_coordinator_ivm_create_job(coordinator_http, Some(plan.name())).await?;
            Ok(())
        }
    }
}

// ── IVM HTTP client ────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct IvmCreateJobBody {
    job_id: Option<String>,
}

#[derive(serde::Deserialize)]
struct IvmCreateJobResponse {
    job_id: String,
}

/// Create a new IVM job on the coordinator. Returns the assigned job ID.
pub async fn execute_coordinator_ivm_create_job(
    coordinator_http: &str,
    job_id: Option<&str>,
) -> RuntimeResult<String> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let body = IvmCreateJobBody {
        job_id: job_id.map(|s| s.to_string()),
    };
    let resp = apply_coordinator_bearer(client.post(format!("{base}/api/v1/ivm/jobs")))
        .json(&body)
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm create job: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm create job HTTP {}",
            resp.status()
        )));
    }
    let parsed: IvmCreateJobResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm create job decode: {e}")))?;
    Ok(parsed.job_id)
}

#[derive(serde::Serialize)]
struct IvmRegisterViewBody<'a> {
    name: &'a str,
    body_sql: &'a str,
    output_schema: IvmSchemaJson<'a>,
    is_materialized: bool,
    is_recursive: bool,
}

#[derive(serde::Serialize)]
struct IvmSchemaJson<'a> {
    fields: &'a [IvmFieldJson],
}

#[derive(serde::Serialize)]
struct IvmFieldJson {
    name: String,
    data_type: String,
    nullable: bool,
}

fn arrow_dt_to_str(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::{DataType, TimeUnit};
    match dt {
        DataType::Int8 => "Int8".to_owned(),
        DataType::Int16 => "Int16".to_owned(),
        DataType::Int32 => "Int32".to_owned(),
        DataType::Int64 => "Int64".to_owned(),
        DataType::UInt8 => "UInt8".to_owned(),
        DataType::UInt16 => "UInt16".to_owned(),
        DataType::UInt32 => "UInt32".to_owned(),
        DataType::UInt64 => "UInt64".to_owned(),
        DataType::Float32 => "Float32".to_owned(),
        DataType::Float64 => "Float64".to_owned(),
        DataType::Utf8 => "Utf8".to_owned(),
        DataType::LargeUtf8 => "LargeUtf8".to_owned(),
        DataType::Boolean => "Boolean".to_owned(),
        DataType::Binary => "Binary".to_owned(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => "TimestampMs".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => "TimestampUs".to_owned(),
        DataType::Date32 => "Date32".to_owned(),
        DataType::Date64 => "Date64".to_owned(),
        other => format!("{other:?}"),
    }
}

/// Register or update an incremental view on a remote IVM job.
pub async fn execute_coordinator_ivm_register_view(
    coordinator_http: &str,
    job_id: &str,
    spec: &krishiv_ivm::IncrementalViewSpec,
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let fields: Vec<IvmFieldJson> = spec
        .output_schema
        .fields()
        .iter()
        .map(|f| IvmFieldJson {
            name: f.name().clone(),
            data_type: arrow_dt_to_str(f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect();
    let body = IvmRegisterViewBody {
        name: &spec.name,
        body_sql: &spec.body_sql,
        output_schema: IvmSchemaJson { fields: &fields },
        is_materialized: spec.is_materialized,
        is_recursive: spec.is_recursive,
    };
    let resp =
        apply_coordinator_bearer(client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/views")))
            .json(&body)
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("ivm register view: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm register view HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct IvmFeedSourceBody {
    delta_ipc_b64: String,
}

/// Feed a `DeltaBatch` to a named source on a remote IVM job.
pub async fn execute_coordinator_ivm_feed_source(
    coordinator_http: &str,
    job_id: &str,
    source_name: &str,
    delta: &krishiv_ivm::DeltaBatch,
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let ipc = krishiv_ivm::serialize_delta_batch(delta)
        .map_err(|e| RuntimeError::transport(format!("delta serialize: {e}")))?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
    let body = IvmFeedSourceBody { delta_ipc_b64: b64 };
    let resp = apply_coordinator_bearer(client.post(format!(
        "{base}/api/v1/ivm/jobs/{job_id}/sources/{source_name}/feed"
    )))
    .json(&body)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm feed source: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm feed source HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct IvmStepResponse {
    active_views: usize,
    total_output_rows: usize,
    tick: u64,
}

/// Summary returned by [`execute_coordinator_ivm_step`].
#[derive(Debug, Clone, Copy)]
pub struct RemoteStepSummary {
    pub active_views: usize,
    pub total_output_rows: usize,
    pub tick: u64,
}

/// Run one IVM tick on a remote job. Returns a [`RemoteStepSummary`].
pub async fn execute_coordinator_ivm_step(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<RemoteStepSummary> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp =
        apply_coordinator_bearer(client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/step")))
            .json(&serde_json::Value::Null)
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("ivm step: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm step HTTP {}",
            resp.status()
        )));
    }
    let parsed: IvmStepResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm step decode: {e}")))?;
    Ok(RemoteStepSummary {
        active_views: parsed.active_views,
        total_output_rows: parsed.total_output_rows,
        tick: parsed.tick,
    })
}

#[derive(serde::Deserialize)]
struct IvmCheckpointResponse {
    checkpoint_b64: String,
}

/// Retrieve a checkpoint from a remote IVM job.
pub async fn execute_coordinator_ivm_checkpoint(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Vec<u8>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/checkpoint")),
    )
    .json(&serde_json::Value::Null)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm checkpoint: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm checkpoint HTTP {}",
            resp.status()
        )));
    }
    let parsed: IvmCheckpointResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm checkpoint decode: {e}")))?;
    base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &parsed.checkpoint_b64,
    )
    .map_err(|e| RuntimeError::transport(format!("checkpoint base64 decode: {e}")))
}

#[derive(serde::Serialize)]
struct IvmRestoreBody {
    checkpoint_b64: String,
}

// ── Delta checkpoint ───────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct IvmCheckpointDeltaResponse {
    checkpoint_delta_b64: String,
}

/// Retrieve a delta checkpoint from a remote IVM job (deltas since last call).
pub async fn execute_coordinator_ivm_checkpoint_delta(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Vec<u8>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/checkpoint-delta")),
    )
    .json(&serde_json::Value::Null)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm checkpoint-delta: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm checkpoint-delta HTTP {}",
            resp.status()
        )));
    }
    let parsed: IvmCheckpointDeltaResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm checkpoint-delta decode: {e}")))?;
    base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &parsed.checkpoint_delta_b64,
    )
    .map_err(|e| RuntimeError::transport(format!("checkpoint-delta base64 decode: {e}")))
}

#[derive(serde::Deserialize)]
struct IvmSnapshotResponse {
    snapshot_ipc_b64: Option<String>,
}

/// Retrieve the current materialized snapshot of a view from a remote IVM job.
///
/// Returns `None` if the view has no snapshot yet. The coordinator serializes
/// the snapshot as an all-`+1` `DeltaBatch`; this strips the weight column and
/// returns the underlying data rows.
pub async fn execute_coordinator_ivm_snapshot(
    coordinator_http: &str,
    job_id: &str,
    view_name: &str,
) -> RuntimeResult<Option<arrow::record_batch::RecordBatch>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(client.get(format!(
        "{base}/api/v1/ivm/jobs/{job_id}/views/{view_name}/snap"
    )))
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm snapshot: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm snapshot HTTP {}",
            resp.status()
        )));
    }
    let parsed: IvmSnapshotResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("ivm snapshot decode: {e}")))?;
    let Some(b64) = parsed.snapshot_ipc_b64 else {
        return Ok(None);
    };
    let ipc = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &b64)
        .map_err(|e| RuntimeError::transport(format!("snapshot base64 decode: {e}")))?;
    let delta = krishiv_ivm::deserialize_delta_batch(&ipc)
        .map_err(|e| RuntimeError::transport(format!("snapshot delta decode: {e}")))?;
    Ok(Some(delta.data_batch()))
}

#[derive(serde::Serialize)]
struct IvmRestoreDeltaBody {
    checkpoint_delta_b64: String,
}

/// Apply delta checkpoint bytes on a remote IVM job.
pub async fn execute_coordinator_ivm_restore_delta(
    coordinator_http: &str,
    job_id: &str,
    bytes: &[u8],
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
    let body = IvmRestoreDeltaBody {
        checkpoint_delta_b64: b64,
    };
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/restore-delta")),
    )
    .json(&body)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm restore-delta: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm restore-delta HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

// ── Streaming → IVM bridge ─────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct IvmStreamBridgeBody {
    snapshot_ipc_b64: String,
}

/// Push streaming micro-batch snapshots to an IVM source via the stream-bridge endpoint.
///
/// The coordinator calls `feed_stream_output` which differentiates consecutive snapshots
/// and pushes the resulting delta to the IVM source.
pub async fn execute_coordinator_ivm_stream_bridge(
    coordinator_http: &str,
    job_id: &str,
    source_name: &str,
    batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<()> {
    use arrow::ipc::writer::StreamWriter;

    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;

    // Encode all batches as a single Arrow IPC stream.
    let schema = batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| RuntimeError::transport("stream-bridge: no batches provided"))?;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| RuntimeError::transport(format!("stream-bridge IPC writer: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| RuntimeError::transport(format!("stream-bridge IPC write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RuntimeError::transport(format!("stream-bridge IPC finish: {e}")))?;
    }
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &buf);

    let body = IvmStreamBridgeBody {
        snapshot_ipc_b64: b64,
    };
    let resp = apply_coordinator_bearer(client.post(format!(
        "{base}/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-bridge"
    )))
    .json(&body)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm stream-bridge: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm stream-bridge HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

/// Feed a pre-computed `DeltaBatch` to a source on the coordinator (G4 fast path).
///
/// Unlike `execute_coordinator_ivm_stream_bridge`, this does not materialise a
/// full snapshot: use it when your producer already emits ±1 `DeltaBatch`es.
pub async fn execute_coordinator_ivm_feed_stream_delta(
    coordinator_http: &str,
    job_id: &str,
    source_name: &str,
    delta: &krishiv_ivm::DeltaBatch,
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let ipc = krishiv_ivm::serialize_delta_batch(delta)
        .map_err(|e| RuntimeError::transport(format!("delta serialize: {e}")))?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &ipc);
    let body = IvmFeedSourceBody { delta_ipc_b64: b64 };
    let resp = apply_coordinator_bearer(client.post(format!(
        "{base}/api/v1/ivm/jobs/{job_id}/sources/{source_name}/stream-delta"
    )))
    .json(&body)
    .send()
    .await
    .map_err(|e| RuntimeError::transport(format!("ivm stream-delta: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm stream-delta HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

/// Restore an IVM job on the coordinator from checkpoint bytes.
pub async fn execute_coordinator_ivm_restore(
    coordinator_http: &str,
    job_id: &str,
    bytes: &[u8],
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
    let body = IvmRestoreBody {
        checkpoint_b64: b64,
    };
    let resp =
        apply_coordinator_bearer(client.post(format!("{base}/api/v1/ivm/jobs/{job_id}/restore")))
            .json(&body)
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("ivm restore: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "ivm restore HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}

// ── A-4: Job listing and lookup ────────────────────────────────────────────────

/// Response for `execute_coordinator_list_jobs`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ListJobsResponse {
    pub jobs: Vec<LiveJobView>,
}

/// Response for `execute_coordinator_list_executors`.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ListExecutorsResponse {
    pub executors: Vec<LiveExecutorView>,
}

/// List all jobs currently tracked by the coordinator (`GET /api/v1/jobs`).
///
/// The coordinator's `api_jobs` route returns `{ "jobs": [...] }` where each
/// entry is a `LiveJobView` (job_id, kind, state, task counts). Returns the
/// raw `Vec<LiveJobView>` so the session layer can render it however it
/// wants (Krishiv's `JobStatus` enum, a UI table, an HTTP JSON response, etc.).
pub async fn execute_coordinator_list_jobs(
    coordinator_http: &str,
) -> RuntimeResult<Vec<LiveJobView>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(client.get(format!("{base}/api/v1/jobs")))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("list jobs: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "list jobs HTTP {}",
            resp.status()
        )));
    }
    let parsed: ListJobsResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("list jobs decode: {e}")))?;
    Ok(parsed.jobs)
}

/// Look up a single job by ID on the coordinator
/// (`GET /api/v1/jobs/{job_id}`).
///
/// Returns `Ok(None)` when the coordinator reports the job is unknown (404);
/// any other non-2xx response is an error. The coordinator's
/// `api_job_by_id` route returns the same `LiveJobView` shape as
/// `api_jobs`.
pub async fn execute_coordinator_get_job(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Option<LiveJobView>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let url = format!("{base}/api/v1/jobs/{}", urlencoding::encode(job_id));
    let resp = apply_coordinator_bearer(client.get(url))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("get job: {e}")))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "get job HTTP {}",
            resp.status()
        )));
    }
    let view: LiveJobView = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("get job decode: {e}")))?;
    Ok(Some(view))
}

/// List executors currently tracked by the coordinator (`GET /api/v1/executors`).
pub async fn execute_coordinator_list_executors(
    coordinator_http: &str,
) -> RuntimeResult<Vec<LiveExecutorView>> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let resp = apply_coordinator_bearer(client.get(format!("{base}/api/v1/executors")))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("list executors: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "list executors HTTP {}",
            resp.status()
        )));
    }
    let parsed: ListExecutorsResponse = resp
        .json()
        .await
        .map_err(|e| RuntimeError::transport(format!("list executors decode: {e}")))?;
    Ok(parsed.executors)
}

/// Cancel a coordinator job (`POST /api/v1/jobs/{job_id}/cancel`).
pub async fn execute_coordinator_cancel_job(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let url = format!("{base}/api/v1/jobs/{}/cancel", urlencoding::encode(job_id));
    let resp = apply_coordinator_bearer(client.post(url))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("cancel job: {e}")))?;
    if !resp.status().is_success() {
        return Err(RuntimeError::transport(format!(
            "cancel job HTTP {}",
            resp.status()
        )));
    }
    Ok(())
}
