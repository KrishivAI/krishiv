//! HTTP handlers for coordinated batch SQL (synchronous and async submit/poll).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::SharedCoordinator;
use crate::batch_sql::{BatchSqlInlineTable, BatchSqlTable, submit_batch_sql_job_with_paths};

#[derive(Debug, Deserialize)]
pub struct BatchSqlRequest {
    pub query: String,
    /// Input tables as inline Arrow IPC (base64-encoded).
    /// Data travels in-band so executor pods need no shared filesystem.
    #[serde(default)]
    pub tables: Vec<BatchSqlInlineTable>,
    /// Input tables as parquet file paths readable by the coordinator and
    /// every executor (shared filesystem or single-node daemon). Plain
    /// SELECTs over path tables are eligible for partition-parallel staged
    /// execution (Phase 52).
    #[serde(default)]
    pub table_paths: Vec<BatchSqlTable>,
    #[serde(default)]
    pub is_streaming: bool,
}

// ── Async submit / poll ────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSqlSubmitResponse {
    pub job_id: String,
}

/// `POST /api/v1/batch-sql/submit` — submit a batch SQL job and return
/// immediately with the job id.  Poll `GET /api/v1/batch-sql/{job_id}` for
/// results.  The coordinator's background orchestration loop drives task
/// dispatch; this handler never blocks waiting for the job to complete.
pub async fn api_batch_sql_submit(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<BatchSqlRequest>,
) -> Result<Json<BatchSqlSubmitResponse>, StatusCode> {
    if body.query.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let job_id = submit_batch_sql_job_with_paths(
        &coordinator,
        &body.query,
        &body.tables,
        &body.table_paths,
        body.is_streaming,
    )
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "submit_batch_sql_job failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(BatchSqlSubmitResponse {
        job_id: job_id.as_str().to_owned(),
    }))
}

#[derive(Debug, Serialize)]
pub struct BatchSqlPollResponse {
    pub job_id: String,
    pub state: String,
    /// Present when state == "Succeeded".
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
    /// Present when state == "Failed" or "Cancelled".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many stages the job was planned into.
    ///
    /// This is the client's only way to tell a distributed run from one that
    /// quietly degraded to a single task. Staging is declined for many
    /// legitimate reasons (DDL, unsupported plan shapes) and one illegitimate
    /// one — a planning bug — and in every case the query still returns
    /// correct rows. A caller benchmarking a cluster, or an operator wondering
    /// why one node is hot, needs the execution shape as *data*, not as a log
    /// line they have to know to go looking for.
    pub stage_count: usize,
    /// Total tasks across all stages. `stage_count == 1 && task_count == 1`
    /// means the whole query ran on one executor.
    pub task_count: usize,
}

/// `GET /api/v1/batch-sql/{job_id}` — poll a submitted batch SQL job.
///
/// Returns the current state.  When `state == "Succeeded"` the inline IPC
/// result batches are included and consumed (subsequent calls return empty).
pub async fn api_batch_sql_poll(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id_str): Path<String>,
) -> Result<Json<BatchSqlPollResponse>, StatusCode> {
    use krishiv_proto::JobState;
    let job_id = krishiv_proto::JobId::try_new(&job_id_str).map_err(|_| StatusCode::BAD_REQUEST)?;

    // Read the execution shape and any task-level failure reason in the same
    // borrow as the state, so the response describes one consistent snapshot.
    let (state, stage_count, task_count, failure_reason) = {
        let coord = coordinator.read().await;
        let snapshot = coord
            .job_snapshot(&job_id)
            .map_err(|_| StatusCode::NOT_FOUND)?;
        // The real reason a job failed lives on the task that failed. Returning
        // a bare "job failed" forced every caller to go read executor logs to
        // learn anything at all — which is how a plain "No suitable object
        // store found for s3://" stayed invisible behind a generic string.
        let failure_reason = coord.job_detail_snapshot(&job_id).ok().and_then(|detail| {
            detail
                .stages()
                .iter()
                .flat_map(|stage| stage.tasks())
                .find_map(|task| task.last_failure_reason().map(str::to_owned))
        });
        (
            snapshot.state(),
            snapshot.stage_count(),
            snapshot.task_count(),
            failure_reason,
        )
    };

    let resp = match state {
        JobState::Succeeded => {
            let (mut batches, spools) = {
                let mut coord = coordinator.write().await;
                (
                    coord.take_job_inline_results(&job_id).unwrap_or_default(),
                    coord.take_job_result_spools(&job_id),
                )
            };
            // Phase 2.10: re-encode disk-spooled results as inline IPC for
            // this HTTP polling surface (the spool file IS one IPC stream).
            for spool in &spools {
                match std::fs::read(spool.path()) {
                    Ok(bytes) => batches.push(bytes),
                    Err(e) => {
                        tracing::error!(error = %e, "cannot read result spool for HTTP poll");
                    }
                }
            }
            BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Succeeded".into(),
                inline_record_batch_ipc: batches,
                error: None,
                stage_count,
                task_count,
            }
        }
        JobState::Failed => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Failed".into(),
            inline_record_batch_ipc: vec![],
            // Prefer the failing task's own message; fall back to the generic
            // string only when no task recorded a reason.
            error: Some(
                failure_reason
                    .unwrap_or_else(|| String::from("job failed (no task reported a reason)")),
            ),
            stage_count,
            task_count,
        },
        JobState::Cancelled => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Cancelled".into(),
            inline_record_batch_ipc: vec![],
            error: Some("job was cancelled".into()),
            stage_count,
            task_count,
        },
        s => BatchSqlPollResponse {
            job_id: job_id_str,
            state: format!("{s:?}"),
            inline_record_batch_ipc: vec![],
            error: None,
            stage_count,
            task_count,
        },
    };
    Ok(Json(resp))
}
