//! HTTP handlers for continuous streaming queries.
//!
//! All three endpoints (register / push / drain) are coordinator-mediated:
//! push stores batches as InlineIpc input partitions in the coordinator's job
//! state; drain returns results from the coordinator's inline result store.
//! This removes the direct executor gRPC path that bypassed the coordinator,
//! enforcing the same single-owner scheduling and task-delivery path as other
//! jobs. Cycle input and output buffers remain coordinator-memory state and do
//! not establish an exactly-once recovery guarantee.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use krishiv_plan::window::{
    WindowExecutionSpec, decode_window_execution_spec, encode_window_execution_spec,
};
use krishiv_plan::{ExecutionKind, TypedTaskFragment};
use krishiv_proto::{InputPartition, InputPartitionDescriptor, JobId, JobKind};
use serde::{Deserialize, Serialize};

use crate::{Coordinator, SchedulerError, SharedCoordinator};

fn scheduler_status(error: &SchedulerError) -> StatusCode {
    match error {
        SchedulerError::DuplicateJob { .. } => StatusCode::CONFLICT,
        SchedulerError::UnknownJob { .. } => StatusCode::NOT_FOUND,
        SchedulerError::InvalidJob { .. } => StatusCode::CONFLICT,
        SchedulerError::InactiveCoordinator { .. } => StatusCode::SERVICE_UNAVAILABLE,
        SchedulerError::NoExecutors | SchedulerError::ExecutorUnavailable { .. } => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRegisterRequest {
    pub job_id: String,
    pub spec: WindowExecutionSpec,
}

#[derive(Debug, Serialize)]
pub struct ContinuousRegisterResponse {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousJobView {
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
    pub spec: WindowExecutionSpec,
}

#[derive(Debug, Serialize)]
pub struct ContinuousListResponse {
    pub streams: Vec<ContinuousJobView>,
}

#[derive(Debug, Serialize)]
pub struct ContinuousCheckpointResponse {
    pub job_id: String,
    pub snapshot_b64: Option<String>,
    pub watermark_ms: Option<i64>,
    pub snapshot_available: bool,
    pub spec: WindowExecutionSpec,
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRestoreRequest {
    pub snapshot_b64: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousRestoreResponse {
    pub job_id: String,
    pub restored: bool,
    pub watermark_ms: i64,
}

fn invalid_continuous_job(job_id: &JobId, message: impl Into<String>) -> SchedulerError {
    SchedulerError::InvalidJob {
        message: format!("continuous job {} {}", job_id.as_str(), message.into()),
    }
}

fn decode_continuous_job_spec(
    record: &crate::JobRecord,
) -> crate::SchedulerResult<WindowExecutionSpec> {
    let job_id = record.job_id();
    let fragment = record
        .spec
        .stages()
        .first()
        .and_then(|stage| stage.tasks().first())
        .map(|task| task.description())
        .ok_or_else(|| invalid_continuous_job(job_id, "has no continuous task fragment"))?;
    let typed = TypedTaskFragment::decode(fragment)
        .ok_or_else(|| invalid_continuous_job(job_id, "typed fragment decode failed"))?;
    let prefix = format!("stream:loop:{}|", job_id.as_str());
    let encoded = typed
        .body
        .strip_prefix(&prefix)
        .ok_or_else(|| invalid_continuous_job(job_id, "does not use a stream:loop fragment"))?;
    decode_window_execution_spec(encoded).map_err(|error| {
        invalid_continuous_job(job_id, format!("window spec decode failed: {error}"))
    })
}

fn continuous_job_view(
    coordinator: &Coordinator,
    job_id: &JobId,
) -> crate::SchedulerResult<ContinuousJobView> {
    let job = coordinator
        .job_coordinator(job_id)
        .ok_or_else(|| SchedulerError::UnknownJob {
            job_id: job_id.clone(),
        })?;
    let record = job.read_record();
    if record.spec.kind() != JobKind::Streaming {
        return Err(invalid_continuous_job(job_id, "is not a streaming job"));
    }
    let spec = decode_continuous_job_spec(&record)?;
    let detail = record.detail_snapshot();
    let last_watermark_ms = detail
        .stages()
        .iter()
        .flat_map(|stage| stage.tasks().iter())
        .filter_map(|task| task.last_watermark_ms())
        .max();
    let persisted = coordinator.load_continuous_snapshot(job_id.as_str());
    Ok(ContinuousJobView {
        job_id: job_id.to_string(),
        state: format!("{:?}", detail.job().state()),
        task_count: detail.job().task_count(),
        assigned_task_count: detail.job().assigned_task_count(),
        running_task_count: detail.job().running_task_count(),
        succeeded_task_count: detail.job().succeeded_task_count(),
        failed_task_count: detail.job().failed_task_count(),
        last_watermark_ms,
        persisted_watermark_ms: persisted.as_ref().map(|snapshot| snapshot.watermark_ms),
        snapshot_available: persisted.is_some(),
        cycle_in_flight: coordinator.continuous_input_cycles.contains(job_id),
        spec,
    })
}

pub async fn api_continuous_register(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterRequest>,
) -> Result<Json<ContinuousRegisterResponse>, StatusCode> {
    use krishiv_proto::{JobSpec, StageId, StageSpec, TaskId, TaskSpec};
    let job_id = JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let stage_id = StageId::try_new("stage-streaming").map_err(|_| StatusCode::BAD_REQUEST)?;
    // The task_id is unique per job_id so per-task lease fencing, per-task
    // ack waiters, and apply_barrier_acks cannot collide between concurrent
    // continuous-streaming jobs. The fragment body already carries the
    // job_id for routing; this just gives the per-task bookkeeping its own
    // identity.
    let task_id = TaskId::try_new(format!("task-streaming-{}", body.job_id))
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    // Preserve the registered operation in a typed fragment. Each push
    // dispatches one bounded input cycle to a stateful stream:loop executor.
    let encoded_spec =
        encode_window_execution_spec(&body.spec).map_err(|_| StatusCode::BAD_REQUEST)?;
    let loop_fragment = format!("stream:loop:{}|{encoded_spec}", body.job_id);
    let fragment = TypedTaskFragment::new(ExecutionKind::Streaming, loop_fragment)
        .encode()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let stage = StageSpec::new(stage_id, "continuous-streaming")
        .with_task(TaskSpec::new(task_id, fragment));
    let spec =
        JobSpec::new(job_id.clone(), "continuous-streaming", JobKind::Streaming).with_stage(stage);

    {
        let mut coord = coordinator.write().await;
        coord
            .ensure_active()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        coord
            .submit_job(spec)
            .map_err(|error| scheduler_status(&error))?;
    }

    Ok(Json(ContinuousRegisterResponse { success: true }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousRegisterSqlRequest {
    pub job_id: String,
    /// A windowed streaming query:
    /// `SELECT key, AGG(col) FROM TUMBLE(TABLE src, DESCRIPTOR(ts), <ms>) GROUP BY key`.
    pub sql: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousRegisterSqlResponse {
    pub success: bool,
    /// The source table the window reads from (feed pushes target it).
    pub source: String,
}

/// Register a continuous streaming job from **SQL**: the coordinator compiles
/// the windowed query to a [`WindowExecutionSpec`] itself
/// (`krishiv_sql::streaming_window_plan`), so callers (the platform pipeline
/// reconciler) pass SQL and stay decoupled from the operator spec type.
pub async fn api_continuous_register_sql(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterSqlRequest>,
) -> Result<Json<ContinuousRegisterSqlResponse>, StatusCode> {
    let plan = krishiv_sql::streaming_window_plan::compile_streaming_window_sql(&body.sql)
        .map_err(|error| {
            tracing::warn!(error = %error, "continuous-register-sql: compile failed");
            StatusCode::BAD_REQUEST
        })?;
    register_continuous_stream_coordinated(&coordinator, &body.job_id, &plan.spec)
        .await
        .map_err(|error| match error {
            ContinuousStreamError::Scheduler(e) => scheduler_status(&e),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    Ok(Json(ContinuousRegisterSqlResponse {
        success: true,
        source: plan.source,
    }))
}

pub async fn api_continuous_list(
    State(coordinator): State<SharedCoordinator>,
) -> Result<Json<ContinuousListResponse>, StatusCode> {
    let streams = {
        let coord = coordinator.read().await;
        let mut streams = coord
            .job_snapshots()
            .into_iter()
            .filter(|job| job.kind() == JobKind::Streaming)
            .filter_map(|job| continuous_job_view(&coord, job.job_id()).ok())
            .collect::<Vec<_>>();
        streams.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        streams
    };
    Ok(Json(ContinuousListResponse { streams }))
}

pub async fn api_continuous_get(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousJobView>, StatusCode> {
    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let view = {
        let coord = coordinator.read().await;
        continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?
    };
    Ok(Json(view))
}

pub async fn api_continuous_checkpoint(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
) -> Result<Json<ContinuousCheckpointResponse>, StatusCode> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let response = {
        let coord = coordinator.read().await;
        let view =
            continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
        let persisted = coord.load_continuous_snapshot(job_id.as_str());
        ContinuousCheckpointResponse {
            job_id: view.job_id,
            snapshot_b64: persisted.as_ref().map(|snapshot| {
                base64::engine::general_purpose::STANDARD.encode(&snapshot.snapshot_bytes)
            }),
            watermark_ms: persisted.as_ref().map(|snapshot| snapshot.watermark_ms),
            snapshot_available: persisted.is_some(),
            spec: view.spec,
        }
    };
    Ok(Json(response))
}

pub async fn api_continuous_restore(
    State(coordinator): State<SharedCoordinator>,
    Path(job_id): Path<String>,
    Json(body): Json<ContinuousRestoreRequest>,
) -> Result<Json<ContinuousRestoreResponse>, StatusCode> {
    use base64::Engine as _;

    let job_id = JobId::try_new(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let snapshot_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.snapshot_b64.as_bytes())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if snapshot_bytes.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let watermark_ms = {
        let mut coord = coordinator.write().await;
        let view =
            continuous_job_view(&coord, &job_id).map_err(|error| scheduler_status(&error))?;
        let watermark_ms = view
            .persisted_watermark_ms
            .or(view.last_watermark_ms)
            .unwrap_or(i64::MIN);
        let snapshot = crate::ContinuousSnapshot {
            snapshot_bytes,
            watermark_ms,
        };
        coord
            .pending_continuous_restores
            .insert(job_id.clone(), snapshot.clone());
        coord.save_continuous_snapshot(job_id.as_str(), snapshot);
        // Keep the existing streaming job active; the restore is applied on the
        // next fenced cycle assignment, not out-of-band.
        watermark_ms
    };
    Ok(Json(ContinuousRestoreResponse {
        job_id: job_id.to_string(),
        restored: true,
        watermark_ms,
    }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousPushRequest {
    pub job_id: String,
    pub input_batches_b64: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousPushResponse {
    pub success: bool,
}

/// Dispatch one serialized input cycle through the job's retained window state.
///
/// The coordinator fences concurrent pushes, attaches the input as an InlineIpc
/// partition, and delivers a normal task assignment to the job's active
/// executor. The executor reports cycle output through the existing task-result
/// path.
pub async fn api_continuous_push(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousPushRequest>,
) -> Result<Json<ContinuousPushResponse>, StatusCode> {
    use base64::Engine as _;
    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.input_batches_b64.as_bytes())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if ipc_bytes.is_empty()
        || crate::batch_sql::decode_inline_record_batches(std::slice::from_ref(&ipc_bytes))
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .is_empty()
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let partition = InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: String::from("input"),
            ipc_bytes,
        },
    );

    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id, vec![partition])
            .map_err(|error| scheduler_status(&error))?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_status(&error));
            }
        };
        let targets = match coord.resolve_assignment_targets(assignments) {
            Ok(targets) => targets,
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id);
                return Err(scheduler_status(&error));
            }
        };
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            coord.abort_continuous_input_cycle(&job_id);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let target_count = targets.len();
        (targets, coord.executor_channels.clone(), target_count)
    };

    let responses =
        match Coordinator::deliver_assignment_targets_with_channels(channels, targets).await {
            Ok(responses) => responses,
            Err(_) => {
                coordinator
                    .write()
                    .await
                    .abort_continuous_input_cycle(&job_id);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };
    let mut coord = coordinator.write().await;
    if !coord.continuous_input_cycles.contains(&job_id) {
        return Err(StatusCode::CONFLICT);
    }
    let accepted = coord.apply_assignment_dispatch_responses(&job_id, &responses);
    if accepted != target_count {
        coord.abort_continuous_input_cycle(&job_id);
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(ContinuousPushResponse { success: true }))
}

#[derive(Debug, Deserialize)]
pub struct ContinuousDrainRequest {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
pub struct ContinuousDrainResponse {
    pub inline_record_batch_ipc: Vec<Vec<u8>>,
}

/// Return newly emitted window batches from the coordinator's inline result store.
///
/// Results are written by the executor after processing a fenced `stream:loop`
/// cycle and are consumed once from the coordinator's in-memory result store.
pub async fn api_continuous_drain(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousDrainRequest>,
) -> Result<Json<ContinuousDrainResponse>, StatusCode> {
    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let batches = {
        let mut coord = coordinator.write().await;
        let snapshot = coord
            .job_snapshot(&job_id)
            .map_err(|error| scheduler_status(&error))?;
        if snapshot.kind() != krishiv_proto::JobKind::Streaming {
            return Err(StatusCode::CONFLICT);
        }
        coord.take_job_inline_results(&job_id).unwrap_or_default()
    };

    Ok(Json(ContinuousDrainResponse {
        inline_record_batch_ipc: batches,
    }))
}

// -------------------------------------------------------------------------
// Public programmatic API — no HTTP types.
// Used by co-located services (e.g., Flight SQL sidecar) that call the
// coordinator directly without an HTTP round-trip.
// -------------------------------------------------------------------------

/// Error returned by the programmatic continuous-stream helpers.
#[derive(Debug)]
pub enum ContinuousStreamError {
    /// A `SchedulerError` wrapped for external callers.
    Scheduler(crate::SchedulerError),
    /// The push cycle was aborted (e.g., no executor available).
    Unavailable(String),
    /// A cycle was aborted because it conflicted with the current state.
    Aborted(String),
}

impl std::fmt::Display for ContinuousStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduler(e) => write!(f, "scheduler error: {e}"),
            Self::Unavailable(msg) => write!(f, "unavailable: {msg}"),
            Self::Aborted(msg) => write!(f, "aborted: {msg}"),
        }
    }
}

impl std::error::Error for ContinuousStreamError {}

impl From<crate::SchedulerError> for ContinuousStreamError {
    fn from(e: crate::SchedulerError) -> Self {
        Self::Scheduler(e)
    }
}

/// Register a new continuous streaming job with the coordinator.
///
/// This is the programmatic equivalent of `api_continuous_register` — it calls
/// the same coordinator methods without serialising to HTTP.
///
/// The job is identified by `job_id` and parameterised by `spec`.
pub async fn register_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    spec: &krishiv_plan::window::WindowExecutionSpec,
) -> Result<(), ContinuousStreamError> {
    use krishiv_plan::window::encode_window_execution_spec;
    use krishiv_plan::{ExecutionKind, TypedTaskFragment};
    use krishiv_proto::{JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};

    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    let stage_id = StageId::try_new("stage-streaming").map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    let task_id = TaskId::try_new("task-streaming").map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    let encoded_spec = encode_window_execution_spec(spec).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    let loop_fragment = format!("stream:loop:{job_id}|{encoded_spec}");
    let fragment = TypedTaskFragment::new(ExecutionKind::Streaming, loop_fragment)
        .encode()
        .map_err(|e| {
            ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
                message: e.to_string(),
            })
        })?;
    let stage = StageSpec::new(stage_id, "continuous-streaming")
        .with_task(TaskSpec::new(task_id, fragment));
    let job_spec = JobSpec::new(
        job_id_typed.clone(),
        "continuous-streaming",
        JobKind::Streaming,
    )
    .with_stage(stage);

    let mut coord = coordinator.write().await;
    coord
        .ensure_active()
        .map_err(ContinuousStreamError::Scheduler)?;
    coord
        .submit_job(job_spec)
        .map_err(ContinuousStreamError::Scheduler)?;
    Ok(())
}

/// Push one cycle of IPC bytes as input for a continuous streaming job.
///
/// This is the programmatic equivalent of `api_continuous_push` — it calls the
/// same coordinator methods without serialising to HTTP.
///
/// `ipc_bytes` must be a valid Arrow IPC stream (non-empty).
pub async fn push_continuous_input_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    ipc_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    use krishiv_proto::{InputPartition, InputPartitionDescriptor, JobId};

    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;

    let partition = InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: String::from("input"),
            ipc_bytes,
        },
    );

    let (targets, channels, target_count) = {
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id_typed, vec![partition])
            .map_err(ContinuousStreamError::Scheduler)?;
        let assignments = match coord.launch_assigned_task_assignments(&job_id_typed) {
            Ok(assignments) if !assignments.is_empty() => assignments,
            Ok(_) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Unavailable(String::from(
                    "no executor available for continuous cycle",
                )));
            }
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Scheduler(error));
            }
        };
        let targets = match coord.resolve_assignment_targets(assignments) {
            Ok(targets) => targets,
            Err(error) => {
                coord.abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Scheduler(error));
            }
        };
        if targets
            .iter()
            .any(|(endpoint, _)| crate::is_in_process_task_endpoint(endpoint))
        {
            coord.abort_continuous_input_cycle(&job_id_typed);
            return Err(ContinuousStreamError::Unavailable(String::from(
                "continuous push cannot reach in-process-only executor via co-located Flight SQL",
            )));
        }
        let target_count = targets.len();
        (targets, coord.executor_channels.clone(), target_count)
    };

    let responses =
        match Coordinator::deliver_assignment_targets_with_channels(channels, targets).await {
            Ok(responses) => responses,
            Err(_) => {
                coordinator
                    .write()
                    .await
                    .abort_continuous_input_cycle(&job_id_typed);
                return Err(ContinuousStreamError::Unavailable(String::from(
                    "assignment delivery failed",
                )));
            }
        };

    let mut coord = coordinator.write().await;
    if !coord.continuous_input_cycles.contains(&job_id_typed) {
        return Err(ContinuousStreamError::Aborted(String::from(
            "continuous cycle was aborted concurrently",
        )));
    }
    let accepted = coord.apply_assignment_dispatch_responses(&job_id_typed, &responses);
    if accepted != target_count {
        coord.abort_continuous_input_cycle(&job_id_typed);
        return Err(ContinuousStreamError::Unavailable(String::from(
            "not all assignment targets accepted the cycle",
        )));
    }
    Ok(())
}

/// Drain completed results from a continuous streaming job.
///
/// This is the programmatic equivalent of `api_continuous_drain` — it calls the
/// same coordinator methods without serialising to HTTP.
///
/// Returns IPC byte payloads (one per completed window), or an empty vec if no
/// results are available yet.
pub async fn drain_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
) -> Result<Vec<Vec<u8>>, ContinuousStreamError> {
    use krishiv_proto::JobId;

    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;

    let mut coord = coordinator.write().await;
    let snapshot = coord
        .job_snapshot(&job_id_typed)
        .map_err(ContinuousStreamError::Scheduler)?;
    if snapshot.kind() != krishiv_proto::JobKind::Streaming {
        return Err(ContinuousStreamError::Scheduler(
            crate::SchedulerError::InvalidJob {
                message: format!("job {job_id} is not a streaming job"),
            },
        ));
    }
    Ok(coord
        .take_job_inline_results(&job_id_typed)
        .unwrap_or_default())
}

/// Stage a one-shot continuous-stream restore snapshot for the next cycle.
pub async fn restore_continuous_stream_coordinated(
    coordinator: &SharedCoordinator,
    job_id: &str,
    snapshot_bytes: Vec<u8>,
) -> Result<(), ContinuousStreamError> {
    let job_id_typed = JobId::try_new(job_id).map_err(|e| {
        ContinuousStreamError::Scheduler(crate::SchedulerError::InvalidJob {
            message: e.to_string(),
        })
    })?;
    if snapshot_bytes.is_empty() {
        return Err(ContinuousStreamError::Scheduler(
            crate::SchedulerError::InvalidJob {
                message: format!("continuous job {job_id} restore snapshot must not be empty"),
            },
        ));
    }
    let mut coord = coordinator.write().await;
    let watermark_ms = continuous_job_view(&coord, &job_id_typed)
        .ok()
        .and_then(|view| view.persisted_watermark_ms.or(view.last_watermark_ms))
        .unwrap_or(i64::MIN);
    let snapshot = crate::ContinuousSnapshot {
        snapshot_bytes,
        watermark_ms,
    };
    coord
        .pending_continuous_restores
        .insert(job_id_typed.clone(), snapshot.clone());
    coord.save_continuous_snapshot(job_id, snapshot);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use axum::Json;
    use axum::extract::State;
    use krishiv_plan::window::{WindowExecutionSpec, WindowKind, decode_window_execution_spec};
    use krishiv_proto::{
        CoordinatorId, ExecutorTaskAssignment, TaskStatusResponse, TransportDisposition,
    };

    use crate::{Coordinator, SharedCoordinator};

    async fn make_coordinator_with_executor(suffix: &str) -> SharedCoordinator {
        use krishiv_proto::{ExecutorDescriptor, ExecutorId};
        let coord_id = CoordinatorId::try_new(format!("coord-cs-{suffix}")).unwrap();
        let coordinator = SharedCoordinator::new(Coordinator::active(coord_id));
        let exec_id = ExecutorId::try_new(format!("exec-cs-{suffix}")).unwrap();
        let desc = ExecutorDescriptor::new(exec_id, "localhost", 4)
            .with_task_endpoint(crate::IN_PROCESS_TASK_ENDPOINT);
        coordinator.write().await.register_executor(desc).unwrap();
        coordinator
    }

    fn tumbling_spec() -> WindowExecutionSpec {
        WindowExecutionSpec {
            key_column: "user_id".to_string(),
            key_column_type: String::from("utf8"),
            event_time_column: "ts".to_string(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: WindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        }
    }

    fn encoded_input() -> String {
        use base64::Engine as _;

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])) as _,
                Arc::new(Int64Array::from(vec![100_i64, 12_000_i64])) as _,
            ],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        base64::engine::general_purpose::STANDARD.encode(ipc)
    }

    fn input_partition() -> InputPartition {
        use base64::Engine as _;

        InputPartition::typed(
            "continuous-input",
            InputPartitionDescriptor::InlineIpc {
                table_name: String::from("input"),
                ipc_bytes: base64::engine::general_purpose::STANDARD
                    .decode(encoded_input())
                    .unwrap(),
            },
        )
    }

    async fn prepare_cycle(
        coordinator: &SharedCoordinator,
        job_id: &str,
    ) -> ExecutorTaskAssignment {
        let job_id = krishiv_proto::JobId::try_new(job_id).unwrap();
        let mut coord = coordinator.write().await;
        coord
            .prepare_continuous_input_cycle(&job_id, vec![input_partition()])
            .unwrap();
        let mut assignments = coord.launch_assigned_task_assignments(&job_id).unwrap();
        assert_eq!(assignments.len(), 1);
        assignments.remove(0)
    }

    #[tokio::test]
    async fn register_succeeds_and_drain_returns_empty() {
        let coordinator = make_coordinator_with_executor("reg-drain").await;

        let register_req = ContinuousRegisterRequest {
            job_id: "cs-test-job".to_string(),
            spec: tumbling_spec(),
        };
        let response = api_continuous_register(State(coordinator.clone()), Json(register_req))
            .await
            .unwrap();
        assert!(response.0.success, "register must succeed");
        {
            let coord = coordinator.read().await;
            let job_id = krishiv_proto::JobId::try_new("cs-test-job").unwrap();
            let job = coord.job_coordinator(&job_id).expect("registered job");
            let record = job.read_record();
            let fragment = record.spec.stages()[0].tasks()[0].description();
            let body = TypedTaskFragment::decode(fragment)
                .expect("continuous job must use a typed fragment")
                .body;
            let encoded_spec = body
                .strip_prefix("stream:loop:cs-test-job|")
                .expect("continuous task must retain its job id");
            assert_eq!(
                decode_window_execution_spec(encoded_spec).unwrap(),
                tumbling_spec()
            );
        }

        // Drain before any push — should return empty, not error.
        let drain_req = ContinuousDrainRequest {
            job_id: "cs-test-job".to_string(),
        };
        let drain_resp = api_continuous_drain(State(coordinator.clone()), Json(drain_req))
            .await
            .unwrap();
        assert!(
            drain_resp.0.inline_record_batch_ipc.is_empty(),
            "drain before push must return empty results"
        );
    }

    #[tokio::test]
    async fn list_get_and_checkpoint_expose_continuous_job_metadata() {
        use base64::Engine as _;

        let coordinator = make_coordinator_with_executor("list-checkpoint").await;
        coordinator
            .write()
            .await
            .attach_store(crate::InMemoryMetadataStore::default());

        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-list-job".into(),
                spec: tumbling_spec(),
            }),
        )
        .await
        .unwrap();

        coordinator.write().await.save_continuous_snapshot(
            "cs-list-job",
            crate::ContinuousSnapshot {
                snapshot_bytes: b"checkpoint".to_vec(),
                watermark_ms: 12_345,
            },
        );
        for _ in 0..50 {
            if coordinator
                .read()
                .await
                .load_continuous_snapshot("cs-list-job")
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let list = api_continuous_list(State(coordinator.clone()))
            .await
            .unwrap();
        assert_eq!(list.0.streams.len(), 1);
        assert_eq!(list.0.streams[0].job_id, "cs-list-job");
        assert!(list.0.streams[0].snapshot_available);
        assert_eq!(list.0.streams[0].persisted_watermark_ms, Some(12_345));

        let get = api_continuous_get(
            State(coordinator.clone()),
            Path(String::from("cs-list-job")),
        )
        .await
        .unwrap();
        assert_eq!(get.0.job_id, "cs-list-job");
        assert_eq!(get.0.spec, tumbling_spec());

        let checkpoint =
            api_continuous_checkpoint(State(coordinator), Path(String::from("cs-list-job")))
                .await
                .unwrap();
        assert_eq!(checkpoint.0.job_id, "cs-list-job");
        assert_eq!(checkpoint.0.watermark_ms, Some(12_345));
        assert_eq!(
            checkpoint.0.snapshot_b64,
            Some(base64::engine::general_purpose::STANDARD.encode("checkpoint"))
        );
    }

    #[tokio::test]
    async fn coordinator_prepares_one_fenced_executor_cycle() {
        let coordinator = make_coordinator_with_executor("push").await;

        // Register the job first.
        let register_req = ContinuousRegisterRequest {
            job_id: "cs-push-job".to_string(),
            spec: tumbling_spec(),
        };
        let _ = api_continuous_register(State(coordinator.clone()), Json(register_req))
            .await
            .unwrap();

        let assignment = prepare_cycle(&coordinator, "cs-push-job").await;
        assert!(assignment.requires_reattach());

        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-push-job").unwrap();
        assert!(coord.continuous_input_cycles.contains(&job_id));
        assert_eq!(coord.job_input_partitions[&job_id].len(), 1);
    }

    #[tokio::test]
    async fn restore_stages_snapshot_for_next_continuous_cycle() {
        use base64::Engine as _;

        let coordinator = make_coordinator_with_executor("restore").await;
        coordinator
            .write()
            .await
            .attach_store(crate::InMemoryMetadataStore::default());
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-restore-job".into(),
                spec: tumbling_spec(),
            }),
        )
        .await
        .unwrap();

        coordinator.write().await.save_continuous_snapshot(
            "cs-restore-job",
            crate::ContinuousSnapshot {
                snapshot_bytes: b"old-checkpoint".to_vec(),
                watermark_ms: 777,
            },
        );
        for _ in 0..50 {
            if coordinator
                .read()
                .await
                .load_continuous_snapshot("cs-restore-job")
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let restore = api_continuous_restore(
            State(coordinator.clone()),
            Path(String::from("cs-restore-job")),
            Json(ContinuousRestoreRequest {
                snapshot_b64: base64::engine::general_purpose::STANDARD.encode("new-checkpoint"),
            }),
        )
        .await
        .unwrap();
        assert!(restore.0.restored);
        assert_eq!(restore.0.watermark_ms, 777);

        let assignment = prepare_cycle(&coordinator, "cs-restore-job").await;
        assert_eq!(assignment.input_partitions().len(), 2);
        match assignment.input_partitions()[0].descriptor() {
            Some(InputPartitionDescriptor::ContinuousRestore {
                snapshot_bytes,
                watermark_ms,
            }) => {
                assert_eq!(snapshot_bytes.as_slice(), b"new-checkpoint");
                assert_eq!(*watermark_ms, 777);
            }
            other => panic!("expected restore descriptor, got {other:?}"),
        }

        let job_id = krishiv_proto::JobId::try_new("cs-restore-job").unwrap();
        {
            let coord = coordinator.read().await;
            assert!(coord.pending_continuous_restores.contains_key(&job_id));
        }
        {
            let mut coord = coordinator.write().await;
            let accepted = coord.apply_assignment_dispatch_responses(
                &job_id,
                &[(
                    assignment,
                    TaskStatusResponse::new(TransportDisposition::Accepted),
                )],
            );
            assert_eq!(accepted, 1);
            assert!(!coord.pending_continuous_restores.contains_key(&job_id));
        }
    }

    #[tokio::test]
    async fn push_rejects_undeliverable_in_process_target_and_rolls_back() {
        let coordinator = make_coordinator_with_executor("in-process-push").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-in-process-job".into(),
                spec: tumbling_spec(),
            }),
        )
        .await
        .unwrap();

        let error = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "cs-in-process-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("HTTP push must not pretend an in-process target was delivered");
        assert_eq!(error, StatusCode::SERVICE_UNAVAILABLE);

        let coord = coordinator.read().await;
        let job_id = krishiv_proto::JobId::try_new("cs-in-process-job").unwrap();
        assert!(!coord.continuous_input_cycles.contains(&job_id));
        assert!(!coord.job_input_partitions.contains_key(&job_id));
    }

    #[tokio::test]
    async fn register_with_invalid_job_id_returns_bad_request() {
        let coordinator = make_coordinator_with_executor("invalid").await;

        let req = ContinuousRegisterRequest {
            job_id: "".to_string(), // empty id is invalid
            spec: tumbling_spec(),
        };
        let result = api_continuous_register(State(coordinator.clone()), Json(req)).await;
        assert!(result.is_err(), "empty job_id must be rejected");
    }

    #[tokio::test]
    async fn register_rejects_invalid_window_spec_before_job_creation() {
        let coordinator = make_coordinator_with_executor("invalid-window").await;
        let mut spec = tumbling_spec();
        spec.window_size_ms = 0;

        let error = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-invalid-window".into(),
                spec,
            }),
        )
        .await
        .expect_err("invalid window spec must fail registration");

        assert_eq!(error, StatusCode::BAD_REQUEST);
        let job_id = krishiv_proto::JobId::try_new("cs-invalid-window").unwrap();
        assert!(matches!(
            coordinator.read().await.job_snapshot(&job_id),
            Err(SchedulerError::UnknownJob { .. })
        ));
    }

    #[tokio::test]
    async fn duplicate_register_returns_conflict_without_replacing_job() {
        let coordinator = make_coordinator_with_executor("duplicate").await;
        let request = || ContinuousRegisterRequest {
            job_id: "cs-duplicate-job".to_string(),
            spec: tumbling_spec(),
        };
        let _ = api_continuous_register(State(coordinator.clone()), Json(request()))
            .await
            .unwrap();
        let error = api_continuous_register(State(coordinator), Json(request()))
            .await
            .expect_err("duplicate job must fail");
        assert_eq!(error, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn push_and_drain_unknown_job_return_not_found() {
        let coordinator = make_coordinator_with_executor("unknown").await;
        let push = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "missing-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("unknown push must fail");
        assert_eq!(push, StatusCode::NOT_FOUND);

        let drain = api_continuous_drain(
            State(coordinator),
            Json(ContinuousDrainRequest {
                job_id: "missing-job".into(),
            }),
        )
        .await
        .expect_err("unknown drain must fail");
        assert_eq!(drain, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn concurrent_push_is_rejected_while_cycle_is_in_flight() {
        let coordinator = make_coordinator_with_executor("busy").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-busy-job".into(),
                spec: tumbling_spec(),
            }),
        )
        .await
        .unwrap();
        let _ = prepare_cycle(&coordinator, "cs-busy-job").await;
        let error = api_continuous_push(
            State(coordinator),
            Json(ContinuousPushRequest {
                job_id: "cs-busy-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("second concurrent cycle must be fenced");
        assert_eq!(error, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn successful_cycle_publishes_output_and_returns_task_to_idle() {
        use base64::Engine as _;
        use krishiv_proto::{TaskOutputMetadata, TaskState, TaskStatusUpdate};

        let coordinator = make_coordinator_with_executor("cycle").await;
        let _ = api_continuous_register(
            State(coordinator.clone()),
            Json(ContinuousRegisterRequest {
                job_id: "cs-cycle-job".into(),
                spec: tumbling_spec(),
            }),
        )
        .await
        .unwrap();
        let assignment = prepare_cycle(&coordinator, "cs-cycle-job").await;

        let job_id = krishiv_proto::JobId::try_new("cs-cycle-job").unwrap();
        let running = TaskStatusUpdate::new(
            job_id.clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.executor_id().clone(),
            TaskState::Running,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation());
        coordinator
            .write()
            .await
            .apply_task_update(running)
            .unwrap();

        let output_ipc = base64::engine::general_purpose::STANDARD
            .decode(encoded_input())
            .unwrap();
        let succeeded = TaskStatusUpdate::new(
            job_id.clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.executor_id().clone(),
            TaskState::Succeeded,
            assignment.attempt_id().as_u32(),
        )
        .with_lease_generation(assignment.lease_generation())
        .with_output_metadata(
            TaskOutputMetadata::new("streaming_window", 1, 1, 2)
                .with_inline_record_batch_ipc(vec![output_ipc.clone()]),
        );
        coordinator
            .write()
            .await
            .apply_task_update(succeeded.clone())
            .unwrap();
        assert_eq!(
            coordinator
                .write()
                .await
                .apply_task_update(succeeded)
                .unwrap(),
            crate::TaskUpdateOutcome::Duplicate
        );

        let blocked_push = api_continuous_push(
            State(coordinator.clone()),
            Json(ContinuousPushRequest {
                job_id: "cs-cycle-job".into(),
                input_batches_b64: encoded_input(),
            }),
        )
        .await
        .expect_err("undrained output must backpressure the next cycle");
        assert_eq!(blocked_push, StatusCode::CONFLICT);

        let mut coord = coordinator.write().await;
        let detail = coord.job_detail_snapshot(&job_id).unwrap();
        assert_eq!(detail.job().state(), krishiv_proto::JobState::Running);
        assert_eq!(detail.stages()[0].tasks()[0].state(), TaskState::Succeeded);
        assert!(!coord.continuous_input_cycles.contains(&job_id));
        assert!(!coord.job_input_partitions.contains_key(&job_id));
        assert_eq!(
            coord.take_job_inline_results(&job_id),
            Some(vec![output_ipc])
        );
    }
}
