//! HTTP client for the cluster control plane APIs.

use krishiv_scheduler::configured_coordinator_bearer_token;
use krishiv_scheduler::decode_inline_record_batches;

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
/// Wrapped in a `Mutex<Option<...>>` so tests can inject a mock client and
/// reset between test runs for isolation.
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

/// Inject a custom HTTP client for test isolation. Only available in test builds.
#[cfg(test)]
pub(crate) fn set_test_http_client(client: reqwest::Client) {
    if let Ok(mut guard) = COORDINATOR_HTTP_CLIENT.lock() {
        *guard = Some(client);
    }
}

/// Reset the shared HTTP client so the next call rebuilds it. Only for tests.
#[cfg(test)]
pub(crate) fn reset_test_http_client() {
    if let Ok(mut guard) = COORDINATOR_HTTP_CLIENT.lock() {
        *guard = None;
    }
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
    #[serde(rename = "job_id")]
    _job_id: String,
    state: String,
    #[serde(default)]
    inline_record_batch_ipc: Vec<Vec<u8>>,
    #[serde(default)]
    error: Option<String>,
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
            let jittered = ((ms as i64) + (ms as i64) * jitter_pct / 100).max(10) as u64;
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

    decode_inline_record_batches(&payload.inline_record_batch_ipc).map_err(RuntimeError::transport)
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
    }
}
