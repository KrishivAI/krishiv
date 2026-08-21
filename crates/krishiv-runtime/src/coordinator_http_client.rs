//! HTTP client for the cluster control plane APIs.

use krishiv_scheduler::configured_coordinator_bearer_token;
use krishiv_scheduler::decode_inline_record_batches;
use krishiv_scheduler::{ContinuousJobMode, LiveExecutorView, LiveJobView};

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

/// Percent-encode one URL path segment (job ids, source and view names are
/// caller-supplied strings; a `/`, space, or `?` in one must not re-shape the
/// route). The job/continuous routes already encoded — the IVM routes did not.
fn seg(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Fold a non-success coordinator HTTP response into a transport error that
/// carries the response body. The coordinator puts the actual refusal reason
/// in the body ("run-loop job X has no launched subtasks to push to",
/// "unknown job ..."); an error that reports only the status code turns a
/// one-line diagnosis into a cluster-side log hunt.
async fn transport_error_with_body(prefix: String, response: reqwest::Response) -> RuntimeError {
    const MAX_BODY_CHARS: usize = 600;
    let body = response.text().await.unwrap_or_default();
    let body = body.trim();
    if body.is_empty() {
        RuntimeError::transport(prefix)
    } else {
        let snippet: String = body.chars().take(MAX_BODY_CHARS).collect();
        RuntimeError::transport(format!("{prefix}: {snippet}"))
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
                "batch-sql job {job_id} timed out after {BOUNDED_WINDOW_POLL_TIMEOUT_SECS}s"
            )));
        }
        let poll_resp = apply_coordinator_bearer(client.get(poll_url))
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("batch-sql poll failed: {e}")))?;
        if !poll_resp.status().is_success() {
            return Err(transport_error_with_body(
                format!("batch-sql poll HTTP {} from {poll_url}", poll_resp.status()),
                poll_resp,
            )
            .await);
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
        return Err(transport_error_with_body(
            format!(
                "transport error: batch-sql HTTP {} from {submit_url}",
                submit_resp.status()
            ),
            submit_resp,
        )
        .await);
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
    let poll_url = format!("{base}/api/v1/batch-sql/{}", seg(&job_id));
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
        return Err(transport_error_with_body(
            format!(
                "batch-sql result poll HTTP {} from {poll_url}",
                poll_resp.status()
            ),
            poll_resp,
        )
        .await);
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
        return Err(transport_error_with_body(
            format!(
                "transport error: batch-sql HTTP {} from {submit_url}",
                submit_resp.status()
            ),
            submit_resp,
        )
        .await);
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

    let poll_url = format!("{base}/api/v1/batch-sql/{}", seg(&job_id));
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
        return Err(transport_error_with_body(
            format!("bounded-window HTTP {} from {url}", response.status()),
            response,
        )
        .await);
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
        BatchSqlResponseBody, ContinuousRegisterAck, ContinuousRegisterOptions,
        CoordinatorBatchSqlJobResult, batch_sql_job_result_from_payload, normalize_http_base,
    };
    use std::sync::Arc;

    /// Serialise the request body exactly as `execute_coordinator_continuous_register`
    /// builds it, so these tests pin the bytes that actually go on the wire.
    fn register_body_json(options: &ContinuousRegisterOptions) -> serde_json::Value {
        #[derive(serde::Serialize)]
        struct Body<'a> {
            job_id: &'a str,
            spec: &'a krishiv_plan::window::WindowExecutionSpec,
            #[serde(flatten)]
            options: &'a ContinuousRegisterOptions,
        }
        let spec = krishiv_plan::window::WindowExecutionSpec::tumbling("k", "ts", 1_000);
        serde_json::to_value(Body {
            job_id: "j",
            spec: &spec,
            options,
        })
        .expect("register body serialises")
    }

    /// Default options must produce a body byte-identical to the old two-field
    /// one, so threading options through cannot change behaviour for any
    /// existing caller or against any older coordinator.
    #[test]
    fn default_options_add_no_fields_to_the_register_body() {
        let body = register_body_json(&ContinuousRegisterOptions::default());
        let object = body.as_object().expect("object body");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["job_id", "spec"],
            "a default-constructed options value must serialise to nothing"
        );
    }

    /// The run-loop request must actually carry mode and parallelism. Before
    /// this existed the client declared a two-field struct, so the coordinator
    /// -- whose handler has accepted these since Phase 55 -- filled in
    /// `mode: cycle, parallelism: 1` from serde defaults and every Rust caller
    /// silently got the single-subtask model.
    #[test]
    fn run_loop_options_reach_the_wire() {
        let options = ContinuousRegisterOptions::run_loop(4)
            .with_checkpointing(30_000, "file:///var/lib/krishiv/ckpt");
        let body = register_body_json(&options);
        assert_eq!(body["mode"], "run-loop");
        assert_eq!(body["parallelism"], 4);
        assert_eq!(body["checkpoint_interval_ms"], 30_000);
        assert_eq!(
            body["checkpoint_storage_path"],
            "file:///var/lib/krishiv/ckpt"
        );
    }

    /// The coordinator rejects a run-loop job that has one half of the
    /// checkpoint pair, so the builder sets both together and never emits a
    /// half-configured body.
    #[test]
    fn checkpointing_is_all_or_nothing_on_the_wire() {
        let body = register_body_json(&ContinuousRegisterOptions::run_loop(2));
        assert!(
            body.get("checkpoint_interval_ms").is_none()
                && body.get("checkpoint_storage_path").is_none(),
            "no checkpoint fields unless both were set: {body}"
        );
        let both = register_body_json(
            &ContinuousRegisterOptions::run_loop(2).with_checkpointing(1_000, "file:///ckpt"),
        );
        assert!(
            both.get("checkpoint_interval_ms").is_some()
                && both.get("checkpoint_storage_path").is_some()
        );
    }

    /// THE defect this whole seam exists to close.
    ///
    /// Neither wire body sets `deny_unknown_fields`, so a coordinator older
    /// than Phase 55 deserialises `{job_id, spec, mode, parallelism, ...}`
    /// happily, throws the options away, registers a single-subtask cycle job,
    /// and answers `{"success": true}` / an empty action body. Without this
    /// check the client reports success for a job that is not the job it asked
    /// for, and the divergence only surfaces later as "why is my 8-way
    /// parallel stream doing 1/8 the throughput".
    #[test]
    fn a_coordinator_that_dropped_the_options_is_not_reported_as_success() {
        let silent_old_server = ContinuousRegisterAck::default();

        // Default request: an old coordinator's bare success IS the truth.
        ContinuousRegisterOptions::default()
            .verify_ack(&silent_old_server, "test seam")
            .expect("a default registration is honoured by every coordinator");

        // Run-loop request: the same bare success is a lie.
        let error = ContinuousRegisterOptions::run_loop(8)
            .verify_ack(&silent_old_server, "test seam")
            .expect_err("an unacknowledged run-loop request must not report success");
        let message = error.to_string();
        assert!(
            message.contains("predates") && message.contains("cycle"),
            "the error must name the cause (an old coordinator registered a cycle job), got: \
             {message}"
        );
    }

    /// A coordinator that answers, but answers with a *different* shape, is the
    /// other half: it is not old, it just applied something else (a clamp, a
    /// policy, a rescale). Reporting success there is the same lie.
    #[test]
    fn an_echo_that_disagrees_with_the_request_is_an_error() {
        let requested = ContinuousRegisterOptions::run_loop(8);
        let downgraded = ContinuousRegisterAck {
            class: Some(String::from("window")),
            mode: Some(String::from("cycle-push")),
            parallelism: Some(1),
            checkpointing: Some(false),
            sources: Some(0),
        };
        let message = requested
            .verify_ack(&downgraded, "test seam")
            .expect_err("a downgraded registration must not report success")
            .to_string();
        assert!(
            message.contains("asked for run-loop, registered cycle-push"),
            "the error must show both sides, got: {message}"
        );
        assert!(
            message.contains("asked for 8, got 1"),
            "the error must show both parallelisms, got: {message}"
        );

        // The matching echo is accepted — otherwise the check would be a
        // blanket refusal rather than a comparison.
        let honoured = ContinuousRegisterAck {
            class: Some(String::from("window")),
            mode: Some(String::from("run-loop")),
            parallelism: Some(8),
            checkpointing: Some(false),
            sources: Some(0),
        };
        requested
            .verify_ack(&honoured, "test seam")
            .expect("an echo matching the request is accepted");
    }

    /// Sources are the quietest of the four: a coordinator that drops them
    /// registers run-loop subtasks at the right parallelism that own no source
    /// and therefore read nothing. From the outside that is indistinguishable
    /// from a healthy job idling on an empty topic — it never errors, it just
    /// never produces a row.
    #[test]
    fn dropped_sources_are_caught_even_when_mode_and_parallelism_match() {
        let requested = ContinuousRegisterOptions::run_loop(2).with_source(
            krishiv_scheduler::continuous_stream_http::ContinuousRegistrySource {
                kind: String::from("kafka"),
                table: String::from("events"),
                config: Default::default(),
            },
        );
        let mode_and_parallelism_honoured = ContinuousRegisterAck {
            class: Some(String::from("window")),
            mode: Some(String::from("run-loop")),
            parallelism: Some(2),
            checkpointing: Some(false),
            sources: Some(0),
        };
        let message = requested
            .verify_ack(&mode_and_parallelism_honoured, "test seam")
            .expect_err("a job whose subtasks own no source must not report success")
            .to_string();
        assert!(
            message.contains("sources: sent 1, coordinator took ownership of 0"),
            "the error must name the dropped sources, got: {message}"
        );
    }

    /// The client resolves the requested mode with the coordinator's own
    /// parser, so aliases the server accepts are not read as a disagreement by
    /// the client. Two parsers would make the verification itself the liar.
    #[test]
    fn mode_aliases_resolve_the_same_way_on_both_sides() {
        for alias in ["run-loop", "barrier-loop", "rloop"] {
            let options = ContinuousRegisterOptions {
                mode: Some(String::from(alias)),
                parallelism: Some(3),
                ..Default::default()
            };
            let server_echo = ContinuousRegisterAck {
                class: Some(String::from("window")),
                mode: Some(String::from("run-loop")),
                parallelism: Some(3),
                checkpointing: Some(false),
                sources: Some(0),
            };
            options
                .verify_ack(&server_echo, "test seam")
                .unwrap_or_else(|e| panic!("alias {alias} must resolve to run-loop: {e}"));
        }
    }

    /// `parallelism > 1` without run-loop is rejected by the coordinator. The
    /// client resolves the same shape locally, so the caller gets that message
    /// instead of a confusing transport error after a round trip.
    #[test]
    fn an_impossible_request_is_rejected_before_it_leaves_the_client() {
        let options = ContinuousRegisterOptions {
            parallelism: Some(4),
            ..Default::default()
        };
        let message = options
            .expected_shape()
            .expect_err("parallelism 4 on the cycle model is not a registerable shape")
            .to_string();
        assert!(
            message.contains("run-loop"),
            "the error must point at the mode that supports parallelism, got: {message}"
        );
    }

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

    /// IVM route segments are caller-supplied (job names, source and view
    /// names). A `/`, space, or `?` in one must be percent-encoded so it
    /// cannot re-shape the route — the job/continuous routes already encoded;
    /// the IVM routes did not.
    #[test]
    fn path_segments_are_percent_encoded() {
        assert_eq!(super::seg("plain-job_1.2"), "plain-job_1.2");
        assert_eq!(super::seg("a/b c"), "a%2Fb%20c");
        assert_eq!(super::seg("x?y=1"), "x%3Fy%3D1");
        let url = format!("http://h/api/v1/ivm/jobs/{}/step", super::seg("a/b c"));
        assert_eq!(url, "http://h/api/v1/ivm/jobs/a%2Fb%20c/step");
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

    /// One-shot HTTP server: accepts a single connection, captures the request
    /// head, answers with the given status line and body, and closes.
    async fn one_shot_http(
        status_line: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 65536];
            let mut request = Vec::new();
            loop {
                let n = stream.read(&mut buf).await.expect("read");
                request.extend_from_slice(&buf[..n]);
                if n == 0 || request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.expect("write");
            stream.shutdown().await.ok();
            String::from_utf8_lossy(&request).into_owned()
        });
        (format!("http://{addr}"), handle)
    }

    fn one_row_batch() -> arrow::record_batch::RecordBatch {
        use std::sync::Arc as SArc;
        let schema = SArc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![SArc::new(arrow::array::Int64Array::from(vec![1_i64]))],
        )
        .expect("batch")
    }

    /// The coordinator's refusal reason travels in the HTTP body ("run-loop
    /// job X has no launched subtasks to push to"). A push error that reports
    /// only "HTTP 503" sends the operator on a cluster-side log hunt — the
    /// live k3s benchmark run died exactly this way. The client must fold the
    /// body into the error.
    #[tokio::test]
    async fn push_error_carries_the_coordinator_refusal_body() {
        let (url, _server) = one_shot_http(
            "503 Service Unavailable",
            "run-loop job j has no launched subtasks to push to",
        )
        .await;
        let error = super::execute_coordinator_continuous_push(&url, "j", &[one_row_batch()])
            .await
            .expect_err("503 must fail the push")
            .to_string();
        assert!(
            error.contains("no launched subtasks to push to"),
            "the error must carry the coordinator body, got: {error}"
        );
        assert!(error.contains("503"), "and still name the status: {error}");
    }

    /// 429 is the coordinator's backpressure signal — the run loop's input
    /// buffer is full and will drain. A producer that treats it as fatal
    /// (the pre-fix client) kills a healthy benchmark/ingest run the moment
    /// the loop falls one buffer behind; the client must back off and retry.
    #[tokio::test]
    async fn push_backs_off_and_retries_on_429_backpressure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let server = tokio::spawn(async move {
            let scripted = [
                ("429 Too Many Requests", "backpressure: input buffer full"),
                ("429 Too Many Requests", "backpressure: input buffer full"),
                ("200 OK", "{\"success\":true}"),
            ];
            for (status_line, body) in scripted {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut buf = vec![0u8; 65536];
                let mut request = Vec::new();
                loop {
                    let n = stream.read(&mut buf).await.expect("read");
                    request.extend_from_slice(&buf[..n]);
                    if n == 0 || request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.expect("write");
                stream.shutdown().await.ok();
            }
        });
        super::execute_coordinator_continuous_push(&url, "j", &[one_row_batch()])
            .await
            .expect("push must ride out transient backpressure");
        server.await.expect("server served all three exchanges");
    }

    /// Deregister must issue DELETE against the job resource — the teardown
    /// that frees the job's executor slots. (A benchmark loop that never
    /// deregisters exhausts a 3x3-slot cluster after three parallelism-3
    /// jobs; this client call is what makes the loop lifecycle possible.)
    #[tokio::test]
    async fn deregister_sends_delete_to_the_job_resource() {
        let (url, server) = one_shot_http("200 OK", "{\"cancelled\":true}").await;
        super::execute_coordinator_continuous_deregister(&url, "nexd-q1-0")
            .await
            .expect("deregister succeeds");
        let request = server.await.expect("server");
        let request_line = request.lines().next().unwrap_or_default();
        assert!(
            request_line.starts_with("DELETE /api/v1/continuous/nexd-q1-0 "),
            "expected DELETE on the job resource, got: {request_line}"
        );
    }
}

// ── Continuous Streaming ───────────────────────────────────────────────────────

/// Execution-model options for a continuous streaming registration.
///
/// The coordinator's `/api/v1/continuous-register` handler has accepted these
/// since Phase 55, but the client body declared only `{job_id, spec}` — so
/// every registration made through this crate silently took the defaults
/// (`mode: "cycle"`, `parallelism: 1`, no executor-owned sources, no barrier
/// checkpointing). The parallel run-loop engine was unreachable from Rust not
/// because the server lacked it, but because the client never asked.
///
/// Every field is skipped when unset, so a default-constructed value serialises
/// to a byte-identical body and cannot change behaviour for existing callers.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContinuousRegisterOptions {
    /// `"cycle"` (default) or `"run-loop"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Run-loop subtask count. Values > 1 require `mode: "run-loop"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    /// Registry connector sources the run-loop subtasks own directly.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<krishiv_scheduler::continuous_stream_http::ContinuousRegistrySource>,
    /// Barrier checkpoint interval for run-loop jobs (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_interval_ms: Option<u64>,
    /// Checkpoint storage path for run-loop jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_storage_path: Option<String>,
}

impl ContinuousRegisterOptions {
    /// Ask for the parallel run-loop model at `parallelism` subtasks.
    pub fn run_loop(parallelism: u32) -> Self {
        Self {
            mode: Some(String::from("run-loop")),
            parallelism: Some(parallelism),
            ..Self::default()
        }
    }

    /// Attach barrier checkpointing. The coordinator requires both halves or
    /// neither, so they are set together.
    pub fn with_checkpointing(mut self, interval_ms: u64, storage_path: impl Into<String>) -> Self {
        self.checkpoint_interval_ms = Some(interval_ms);
        self.checkpoint_storage_path = Some(storage_path.into());
        self
    }

    /// Add a registry connector source owned by the run-loop subtasks.
    pub fn with_source(
        mut self,
        source: krishiv_scheduler::continuous_stream_http::ContinuousRegistrySource,
    ) -> Self {
        self.sources.push(source);
        self
    }

    /// True when this asks for exactly the coordinator's defaults, so an
    /// acknowledgement carries no information a caller could act on.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// The shape this request *should* produce: `(mode, parallelism,
    /// checkpointing, sources)`.
    ///
    /// Resolved with the coordinator's own [`ContinuousJobMode::parse`] rather
    /// than a second copy of the alias table — a client-side parser that drifts
    /// from the server's would turn the verification below into another thing
    /// that lies.
    pub fn expected_shape(&self) -> RuntimeResult<(ContinuousJobMode, u32, bool, usize)> {
        let parallelism = self.parallelism.unwrap_or(1).max(1);
        let mode = ContinuousJobMode::parse(self.mode.as_deref(), parallelism)
            .map_err(RuntimeError::plan_rejected)?;
        let checkpointing =
            self.checkpoint_interval_ms.is_some() && self.checkpoint_storage_path.is_some();
        Ok((mode, parallelism, checkpointing, self.sources.len()))
    }

    /// Check the coordinator's echo against what was asked for.
    ///
    /// This is the load-bearing half of the options seam, not the field itself.
    /// Neither the HTTP body nor the Flight action body uses
    /// `deny_unknown_fields`, so a coordinator that predates these options
    /// **silently discards them** and answers success — a client asking for
    /// run-loop parallelism 8 would get a single-subtask cycle job and no
    /// indication anything was ignored. Shipping the field without this check
    /// would be a feature that lies.
    ///
    /// Default-shaped requests skip the comparison: an old coordinator's bare
    /// `{"success": true}` is a truthful answer when the defaults are exactly
    /// what was asked for.
    pub fn verify_ack(&self, ack: &ContinuousRegisterAck, seam: &str) -> RuntimeResult<()> {
        if self.is_default() {
            return Ok(());
        }
        let (mode, parallelism, checkpointing, sources) = self.expected_shape()?;

        let Some(applied_mode) = ack.mode.as_deref() else {
            return Err(RuntimeError::transport(format!(
                "{seam} accepted the registration but did not report which execution model it \
                 registered. This coordinator predates the run-loop registration options, so it \
                 discarded them and registered a single-subtask cycle job — it did not run the \
                 mode={} parallelism={parallelism} job that was requested. Upgrade the \
                 coordinator, or register without run-loop options.",
                mode.as_str(),
            )));
        };
        let mut disagreements = Vec::new();
        if applied_mode != mode.as_str() {
            disagreements.push(format!(
                "mode: asked for {}, registered {applied_mode}",
                mode.as_str()
            ));
        }
        match ack.parallelism {
            Some(applied) if applied == parallelism => {}
            Some(applied) => disagreements.push(format!(
                "parallelism: asked for {parallelism}, got {applied}"
            )),
            None => disagreements.push(format!(
                "parallelism: asked for {parallelism}, not reported"
            )),
        }
        match ack.checkpointing {
            Some(applied) if applied == checkpointing => {}
            Some(applied) => disagreements.push(format!(
                "checkpointing: asked for {checkpointing}, got {applied}"
            )),
            None if checkpointing => {
                disagreements.push(String::from("checkpointing: asked for true, not reported"))
            }
            None => {}
        }
        match ack.sources {
            Some(applied) if applied == sources => {}
            Some(applied) => disagreements.push(format!(
                "sources: sent {sources}, coordinator took ownership of {applied}"
            )),
            None if sources > 0 => disagreements.push(format!(
                "sources: sent {sources}, not reported — the subtasks may own no source at all"
            )),
            None => {}
        }

        if disagreements.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::transport(format!(
                "{seam} registered a different job than was requested ({}). The job IS registered \
                 under this id in the shape the coordinator reported; deregister it before \
                 retrying.",
                disagreements.join("; ")
            )))
        }
    }
}

/// What the coordinator says it **actually** registered.
///
/// Every field is optional so the type also decodes an older coordinator's
/// `{"success": true}` — absence is the signal that the options were dropped,
/// and [`ContinuousRegisterOptions::verify_ack`] treats it as such rather than
/// as a default.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContinuousRegisterAck {
    /// Class echo (task #147). A non-window registration whose ack lacks the
    /// class was handled by a coordinator that silently dropped stream_spec.
    #[serde(default)]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpointing: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<usize>,
}

impl ContinuousRegisterAck {
    /// Build an acknowledgement from what a coordinator actually applied.
    pub fn applied(applied: &krishiv_scheduler::AppliedContinuousRegistration) -> Self {
        Self {
            class: None,
            mode: Some(applied.mode.as_str().to_string()),
            parallelism: Some(applied.parallelism),
            checkpointing: Some(applied.checkpointing),
            sources: Some(applied.sources),
        }
    }

    /// Encode for a Flight `do_action` response body.
    pub fn to_action_body(&self) -> RuntimeResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| RuntimeError::transport(format!("encode continuous-register ack: {e}")))
    }

    /// Decode a Flight `do_action` response body.
    ///
    /// An empty or unparseable body decodes to the all-`None` acknowledgement
    /// — the shape an older server produces. That is not silently treated as
    /// "defaults were applied": [`ContinuousRegisterOptions::verify_ack`] reads
    /// the absent fields as "the options were dropped" and errors for any
    /// non-default request.
    pub fn from_action_body(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
}

/// Class-routed registration (task #147). Window tasks serialize the legacy
/// `spec` field byte-identically (old coordinators keep working); every
/// other class sends ONLY `stream_spec` — an old coordinator then fails the
/// request on the missing `spec`, which is fail-closed, and a new one echoes
/// the class, which is verified.
pub async fn execute_coordinator_continuous_register_task(
    coordinator_http: &str,
    job_id: &str,
    task: &krishiv_plan::stream_task::StreamingTaskSpec,
    options: &ContinuousRegisterOptions,
) -> RuntimeResult<()> {
    use krishiv_plan::stream_task::StreamingTaskSpec;
    if let StreamingTaskSpec::Window(w) = task {
        return execute_coordinator_continuous_register(coordinator_http, job_id, w, options).await;
    }

    #[derive(serde::Serialize)]
    struct ClassedRegisterRequest<'a> {
        job_id: &'a str,
        stream_spec: &'a krishiv_plan::stream_task::StreamingTaskSpec,
        #[serde(flatten)]
        options: &'a ContinuousRegisterOptions,
    }
    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-register");
    let body = ClassedRegisterRequest {
        job_id,
        stream_spec: task,
        options,
    };
    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-register request failed: {e}")))?;
    if !response.status().is_success() {
        return Err(transport_error_with_body(
            format!("continuous-register HTTP {} from {url}", response.status()),
            response,
        )
        .await);
    }
    let ack = response
        .json::<ContinuousRegisterAck>()
        .await
        .unwrap_or_default();
    // Non-window classes ALWAYS verify the class echo — there is no
    // "default-shaped" classed request an old coordinator could truthfully
    // ack, so a missing echo means the class was silently dropped.
    match ack.class.as_deref() {
        Some(c) if c == task.class_name() => {}
        other => {
            return Err(RuntimeError::plan_rejected(format!(
                "coordinator did not echo the '{}' class (got {:?}): it either dropped \
                 stream_spec silently or planned a different job class",
                task.class_name(),
                other
            )));
        }
    }
    options.verify_ack(&ack, "the coordinator HTTP continuous-register endpoint")?;
    Ok(())
}

pub async fn execute_coordinator_continuous_register(
    coordinator_http: &str,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
    options: &ContinuousRegisterOptions,
) -> RuntimeResult<()> {
    #[derive(serde::Serialize)]
    struct ContinuousRegisterRequest<'a> {
        job_id: &'a str,
        spec: &'a krishiv_plan::window::WindowExecutionSpec,
        #[serde(flatten)]
        options: &'a ContinuousRegisterOptions,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-register");
    let body = ContinuousRegisterRequest {
        job_id,
        spec,
        options,
    };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-register request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(transport_error_with_body(
            format!("continuous-register HTTP {} from {url}", response.status()),
            response,
        )
        .await);
    }
    // Fail fast on an unparseable body only when we actually need the echo:
    // a default registration against any coordinator, old or new, is already
    // correct, and refusing it over a response-shape change would break
    // callers that asked for nothing unusual.
    let ack = response
        .json::<ContinuousRegisterAck>()
        .await
        .unwrap_or_default();
    options.verify_ack(&ack, "the coordinator HTTP continuous-register endpoint")?;
    Ok(())
}

/// Side-tagged push for two-source run-loop jobs (task #147).
/// POST a continuous push with backpressure-aware retry. HTTP 429 from the
/// coordinator means the target run loop's input buffer is full — flow
/// control, not failure — so the producer's only correct move is to wait for
/// the loop to drain and try again. Retries with capped exponential backoff
/// and gives up (returning the coordinator's reason) once the deadline is
/// spent: a loop that never drains must still surface as an error, not hang.
async fn post_push_with_backpressure<B: serde::Serialize>(
    url: &str,
    body: &B,
) -> RuntimeResult<()> {
    const BACKOFF_START: std::time::Duration = std::time::Duration::from_millis(100);
    const BACKOFF_CAP: std::time::Duration = std::time::Duration::from_millis(3_200);
    const RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);
    let client = coordinator_http_client()?;
    let started = std::time::Instant::now();
    let mut backoff = BACKOFF_START;
    loop {
        let response = apply_coordinator_bearer(client.post(url).json(body))
            .send()
            .await
            .map_err(|e| RuntimeError::transport(format!("continuous-push request failed: {e}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS && started.elapsed() < RETRY_BUDGET {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_CAP);
            continue;
        }
        return Err(transport_error_with_body(
            format!("continuous-push HTTP {status} from {url}"),
            response,
        )
        .await);
    }
}

pub async fn execute_coordinator_continuous_push_side(
    coordinator_http: &str,
    job_id: &str,
    side: &str,
    input_batches: &[arrow::record_batch::RecordBatch],
) -> RuntimeResult<()> {
    use crate::flight_action::encode_batches;
    #[derive(serde::Serialize)]
    struct SidePushRequest<'a> {
        job_id: &'a str,
        input_batches_b64: String,
        side: &'a str,
    }
    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-push");
    let body = SidePushRequest {
        job_id,
        input_batches_b64: encode_batches(input_batches)?,
        side,
    };
    post_push_with_backpressure(&url, &body).await
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

    post_push_with_backpressure(&url, &body).await
}

pub async fn execute_coordinator_continuous_drain(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    execute_coordinator_continuous_drain_wait(coordinator_http, job_id, 0).await
}

/// Drain with a long-poll budget (task #149 fix 12): when the job's egress
/// is empty the coordinator relays the wait to the executors, which park on
/// their egress notify instead of the caller busy-polling empty responses.
pub async fn execute_coordinator_continuous_drain_wait(
    coordinator_http: &str,
    job_id: &str,
    wait_ms: u64,
) -> RuntimeResult<Vec<arrow::record_batch::RecordBatch>> {
    #[derive(serde::Serialize)]
    struct ContinuousDrainRequest<'a> {
        job_id: &'a str,
        wait_ms: u64,
    }

    #[derive(serde::Deserialize)]
    struct ContinuousDrainResponse {
        inline_record_batch_ipc: Vec<Vec<u8>>,
    }

    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-drain");
    let body = ContinuousDrainRequest { job_id, wait_ms };

    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-drain request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(transport_error_with_body(
            format!("continuous-drain HTTP {} from {url}", response.status()),
            response,
        )
        .await);
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

/// Declare end-of-stream for a continuous job (bounded producers). Cycle
/// jobs run a final flush cycle; run-loop jobs stage every open window into
/// their egress buffers — call drain afterwards to collect the flushed rows.
pub async fn execute_coordinator_continuous_flush(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<()> {
    #[derive(serde::Serialize)]
    struct FlushRequest<'a> {
        job_id: &'a str,
    }
    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous-flush");
    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.post(&url).json(&FlushRequest { job_id }))
        .send()
        .await
        .map_err(|e| RuntimeError::transport(format!("continuous-flush request failed: {e}")))?;
    if !response.status().is_success() {
        return Err(transport_error_with_body(
            format!("continuous-flush HTTP {} from {url}", response.status()),
            response,
        )
        .await);
    }
    Ok(())
}

/// Deregister (tear down) a continuous job: cancels its tasks on their
/// executors, frees their slots, and clears the job's persisted snapshot so
/// the id can be reused. Callers that register short-lived jobs in a loop
/// (benchmark reps, tests) MUST call this between jobs — registered jobs hold
/// their executor slots until deregistered, and a cluster with all slots held
/// refuses the next job's pushes with "no launched subtasks".
pub async fn execute_coordinator_continuous_deregister(
    coordinator_http: &str,
    job_id: &str,
) -> RuntimeResult<()> {
    let base = normalize_http_base(coordinator_http)?;
    let url = format!("{base}/api/v1/continuous/{job_id}");
    let client = coordinator_http_client()?;
    let response = apply_coordinator_bearer(client.delete(&url))
        .send()
        .await
        .map_err(|e| {
            RuntimeError::transport(format!("continuous-deregister request failed: {e}"))
        })?;
    if !response.status().is_success() {
        return Err(transport_error_with_body(
            format!(
                "continuous-deregister HTTP {} from {url}",
                response.status()
            ),
            response,
        )
        .await);
    }
    Ok(())
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
                &ContinuousRegisterOptions::default(),
            )
            .await
        }
        ExecutionKind::DeltaBatch => {
            // Create the IVM job idempotently on the coordinator.
            // Plan name is the job ID so subsequent feed/step calls reference it.
            execute_coordinator_ivm_create_job(coordinator_http, Some(plan.name()), None).await?;
            Ok(())
        }
    }
}

// ── IVM HTTP client ────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct IvmCreateJobBody {
    job_id: Option<String>,
    /// `Some(false)` pins the job to a single (non-partitioned) flow so it can
    /// host a view-DAG. `None` keeps the coordinator's default auto-partitioning.
    #[serde(skip_serializing_if = "Option::is_none")]
    partitioned: Option<bool>,
}

#[derive(serde::Deserialize)]
struct IvmCreateJobResponse {
    job_id: String,
}

/// Create a new IVM job on the coordinator. Returns the assigned job ID.
///
/// `partitioned = Some(false)` pins the job to a single (non-partitioned) flow so
/// it can host a view-DAG; `None` keeps the coordinator's default.
pub async fn execute_coordinator_ivm_create_job(
    coordinator_http: &str,
    job_id: Option<&str>,
    partitioned: Option<bool>,
) -> RuntimeResult<String> {
    let base = normalize_http_base(coordinator_http)?;
    let client = coordinator_http_client()?;
    let body = IvmCreateJobBody {
        job_id: job_id.map(|s| s.to_string()),
        partitioned,
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
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{}/views", seg(job_id))),
    )
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
        "{base}/api/v1/ivm/jobs/{}/sources/{}/feed",
        seg(job_id),
        seg(source_name)
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
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{}/step", seg(job_id))),
    )
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
        client.post(format!("{base}/api/v1/ivm/jobs/{}/checkpoint", seg(job_id))),
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
    let resp = apply_coordinator_bearer(client.post(format!(
        "{base}/api/v1/ivm/jobs/{}/checkpoint-delta",
        seg(job_id)
    )))
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
        "{base}/api/v1/ivm/jobs/{}/views/{}/snap",
        seg(job_id),
        seg(view_name)
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
    let resp = apply_coordinator_bearer(client.post(format!(
        "{base}/api/v1/ivm/jobs/{}/restore-delta",
        seg(job_id)
    )))
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
        "{base}/api/v1/ivm/jobs/{}/sources/{}/stream-bridge",
        seg(job_id),
        seg(source_name)
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
        "{base}/api/v1/ivm/jobs/{}/sources/{}/stream-delta",
        seg(job_id),
        seg(source_name)
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
    let resp = apply_coordinator_bearer(
        client.post(format!("{base}/api/v1/ivm/jobs/{}/restore", seg(job_id))),
    )
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
