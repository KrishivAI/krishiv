//! HTTP federation shim for multi-region routing (WS-11).

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use krishiv_proto::{JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};
use serde::{Deserialize, Serialize};

use crate::{SchedulerError, SharedCoordinator};

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

/// Deserializable wire representation of a federated job spec.
///
/// `JobSpec` itself does not implement `Deserialize`; this intermediate struct
/// captures the JSON fields and is converted into a `JobSpec` via [`Into`].
#[derive(Deserialize)]
struct FederatedJobWire {
    job_id: String,
    name: String,
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default)]
    stages: Vec<FederatedStageWire>,
}

#[derive(Deserialize)]
struct FederatedStageWire {
    stage_id: String,
    name: String,
    #[serde(default)]
    tasks: Vec<FederatedTaskWire>,
}

#[derive(Deserialize)]
struct FederatedTaskWire {
    task_id: String,
    description: String,
}

fn default_kind() -> String {
    "batch".to_string()
}

impl TryFrom<FederatedJobWire> for JobSpec {
    type Error = StatusCode;

    fn try_from(wire: FederatedJobWire) -> Result<Self, StatusCode> {
        let job_id = JobId::try_new(wire.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
        let kind = match wire.kind.as_str() {
            "batch" => JobKind::Batch,
            "streaming" => JobKind::Streaming,
            _ => return Err(StatusCode::BAD_REQUEST),
        };
        let mut spec = JobSpec::new(job_id, wire.name, kind);
        for sw in wire.stages {
            let stage_id = StageId::try_new(sw.stage_id).map_err(|_| StatusCode::BAD_REQUEST)?;
            let mut stage = StageSpec::new(stage_id, sw.name);
            for tw in sw.tasks {
                let task_id = TaskId::try_new(tw.task_id).map_err(|_| StatusCode::BAD_REQUEST)?;
                stage = stage.with_task(TaskSpec::new(task_id, tw.description));
            }
            spec = spec.with_stage(stage);
        }
        Ok(spec)
    }
}

pub async fn federation_submit_job(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<FederationSubmitBody>,
) -> Result<Json<FederationSubmitResponse>, StatusCode> {
    let job_id = JobId::try_new(body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let wire: FederatedJobWire = serde_json::from_str(&body.spec_json).map_err(|e| {
        tracing::warn!(error = %e, "failed to deserialize federation spec_json");
        StatusCode::BAD_REQUEST
    })?;
    let spec = JobSpec::try_from(wire)?;
    tracing::debug!(job_id = %job_id, "federation submit (deserialized spec_json)");
    let mut coord = coordinator.write().await;
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
    let coord = coordinator.read().await;
    let snapshot = coord
        .job_snapshot(&job_id)
        .map_err(|_| StatusCode::NOT_FOUND)?;
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
    let mut coord = coordinator.write().await;
    coord
        .cancel_job(&job_id)
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(StatusCode::NO_CONTENT)
}
