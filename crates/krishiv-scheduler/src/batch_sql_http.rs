//! HTTP handlers for coordinated batch SQL (synchronous and async submit/poll).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::SharedCoordinator;
use crate::batch_sql::{BatchSqlInlineTable, submit_batch_sql_job};

#[derive(Debug, Deserialize)]
pub struct BatchSqlRequest {
    pub query: String,
    /// Input tables as inline Arrow IPC (base64-encoded).
    /// Data travels in-band so executor pods need no shared filesystem.
    #[serde(default)]
    pub tables: Vec<BatchSqlInlineTable>,
    #[serde(default)]
    pub is_streaming: bool,
}

// ── Async submit / poll ────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
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
    let job_id = submit_batch_sql_job(&coordinator, &body.query, &body.tables, body.is_streaming)
        .await
        .map_err(|e| {
            eprintln!("submit_batch_sql_job failed: {:?}", e);
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

    let state = {
        let coord = coordinator.read().await;
        coord
            .job_snapshot(&job_id)
            .map(|s| s.state())
            .map_err(|_| StatusCode::NOT_FOUND)?
    };

    let resp = match state {
        JobState::Succeeded => {
            let batches = coordinator
                .write()
                .await
                .take_job_inline_results(&job_id)
                .unwrap_or_default();
            BatchSqlPollResponse {
                job_id: job_id_str,
                state: "Succeeded".into(),
                inline_record_batch_ipc: batches,
                error: None,
            }
        }
        JobState::Failed => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Failed".into(),
            inline_record_batch_ipc: vec![],
            error: Some("job failed".into()),
        },
        JobState::Cancelled => BatchSqlPollResponse {
            job_id: job_id_str,
            state: "Cancelled".into(),
            inline_record_batch_ipc: vec![],
            error: Some("job was cancelled".into()),
        },
        s => BatchSqlPollResponse {
            job_id: job_id_str,
            state: format!("{s:?}"),
            inline_record_batch_ipc: vec![],
            error: None,
        },
    };
    Ok(Json(resp))
}
