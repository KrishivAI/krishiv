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
use axum::extract::State;
use axum::http::StatusCode;
use krishiv_plan::window::{WindowExecutionSpec, encode_window_execution_spec};
use krishiv_plan::{ExecutionKind, TypedTaskFragment};
use krishiv_proto::{InputPartition, InputPartitionDescriptor};
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

pub async fn api_continuous_register(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousRegisterRequest>,
) -> Result<Json<ContinuousRegisterResponse>, StatusCode> {
    use krishiv_proto::{JobId, JobKind, JobSpec, StageId, StageSpec, TaskId, TaskSpec};
    let job_id = JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let stage_id = StageId::try_new("stage-streaming").map_err(|_| StatusCode::BAD_REQUEST)?;
    let task_id = TaskId::try_new("task-streaming").map_err(|_| StatusCode::BAD_REQUEST)?;

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
    use krishiv_plan::{ExecutionKind, TypedTaskFragment};
    use krishiv_plan::window::encode_window_execution_spec;
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
    let job_spec =
        JobSpec::new(job_id_typed.clone(), "continuous-streaming", JobKind::Streaming)
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
    use krishiv_proto::{CoordinatorId, ExecutorTaskAssignment};

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
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
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

    #