//! HTTP federation shim for multi-region routing (WS-11).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use krishiv_proto::{
    JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec,
};
use serde::{Deserialize, Serialize};

use crate::{SharedCoordinator, SchedulerError};

#[derive(Deserialize)]
pub struct FederationSubmitBody {
    pub job_id: String,
    pub spec_json: String,
}

#[derive(Serialize)]
pub struct FederationSubmitResponse {
    pub remote_job_id: String,
}

#[derive(Serialize)]
pub struct FederationStatusResponse {
    pub remote_job_id: String,
    pub state: String,
}

pub async fn federation_submit_job(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<FederationSubmitBody>,
) -> Result<Json<FederationSubmitResponse>, StatusCode> {
    let job_id = JobId::try_new(body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    tracing::debug!(spec_json = %body.spec_json, "federation submit (spec_json logged; built-in batch stub used)");
    let stage_id = StageId::try_new("stage-1").map_err(|_| StatusCode::BAD_REQUEST)?;
    let task_id = TaskId::try_new("task-1").map_err(|_| StatusCode::BAD_REQUEST)?;
    let stage = StageSpec::new(stage_id, "federated").with_task(TaskSpec::new(
        task_id,
        "sql: SELECT 1 AS federation_ok",
    ));
    let spec = JobSpec::new(job_id.clone(), "federated", JobKind::Batch).with_stage(stage);
    let mut coord = coordinator.write().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    coord.submit_job(spec).map_err(|e| match e {
        SchedulerError::NoExecutors => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_REQUEST,
    })?;
    Ok(Json(FederationSubmitResponse {
        remote_job_id: job_id.as_str().to_owned(),
    }))
}

pub async fn federation_job_status(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<FederationStatusResponse>, StatusCode> {
    let job_id = JobId::try_new(job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let coord = coordinator.read().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let snapshot = coord.job_snapshot(&job_id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(FederationStatusResponse {
        remote_job_id: job_id.as_str().to_owned(),
        state: format!("{:?}", snapshot.state()),
    }))
}

pub async fn federation_cancel_job(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let job_id = JobId::try_new(job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut coord = coordinator.write().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    coord.cancel_job(&job_id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(StatusCode::NO_CONTENT)
}
