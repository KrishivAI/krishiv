//! HTTP handlers for continuous streaming queries.
//!
//! All three endpoints (register / push / drain) are coordinator-mediated:
//! push stores batches as InlineIpc input partitions in the coordinator's job
//! state; drain returns results from the coordinator's inline result store.
//! This removes the direct executor gRPC path that bypassed the coordinator,
//! giving the continuous streaming path the same fault-tolerance properties as
//! batch SQL (coordinator recovery, job GC, single ownership invariant).

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use krishiv_plan::window::WindowExecutionSpec;
use krishiv_proto::{InputPartition, InputPartitionDescriptor};
use serde::{Deserialize, Serialize};

use crate::SharedCoordinator;

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

    // Use stream:continuous: prefix so the executor reads input from the
    // InlineIpc partitions registered via api_continuous_push.
    let fragment = format!("stream:continuous:{}", body.job_id);
    let stage =
        StageSpec::new(stage_id, "continuous-streaming").with_task(TaskSpec::new(task_id, fragment));
    let spec =
        JobSpec::new(job_id.clone(), "continuous-streaming", JobKind::Streaming).with_stage(stage);

    {
        let mut coord = coordinator.write().await;
        coord
            .ensure_active()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        coord
            .submit_job(spec)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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

/// Store input batches in the coordinator's job state as an InlineIpc partition.
///
/// The orchestration loop includes these partitions in the next task assignment
/// to the executor. No direct executor gRPC call is made, so this path survives
/// executor restarts and coordinator failover.
pub async fn api_continuous_push(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousPushRequest>,
) -> Result<Json<ContinuousPushResponse>, StatusCode> {
    use base64::Engine as _;
    let ipc_bytes = base64::engine::general_purpose::STANDARD
        .decode(body.input_batches_b64.as_bytes())
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let partition = InputPartition::typed(
        "continuous-input",
        InputPartitionDescriptor::InlineIpc {
            table_name: String::from("input"),
            ipc_bytes,
        },
    );

    coordinator
        .write()
        .await
        .register_job_input_partitions(job_id, vec![partition]);

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
/// Results are written by the executor after processing the stream:continuous:
/// fragment and are consumed exactly once (take semantics).
pub async fn api_continuous_drain(
    State(coordinator): State<SharedCoordinator>,
    Json(body): Json<ContinuousDrainRequest>,
) -> Result<Json<ContinuousDrainResponse>, StatusCode> {
    let job_id =
        krishiv_proto::JobId::try_new(&body.job_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let batches = coordinator
        .write()
        .await
        .take_job_inline_results(&job_id)
        .unwrap_or_default();

    Ok(Json(ContinuousDrainResponse {
        inline_record_batch_ipc: batches,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Json;
    use axum::extract::State;
    use krishiv_plan::window::{WindowExecutionSpec, WindowKind};
    use krishiv_proto::CoordinatorId;

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
            event_time_column: "ts".to_string(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 10_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![],
            state_ttl_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
        }
    }

    #[tokio::test]
    async fn register_succeeds_and_drain_returns_empty() {
        let coordinator = make_coordinator_with_executor("reg-drain").await;

        let register_req = ContinuousRegisterRequest {
            job_id: "cs-test-job".to_string(),
            spec: tumbling_spec(),
        };
        let response = api_continuous_register(
            State(coordinator.clone()),
            Json(register_req),
        )
        .await
        .unwrap();
        assert!(response.0.success, "register must succeed");

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
    async fn push_stores_partition_without_error() {
        use base64::Engine as _;
        let coordinator = make_coordinator_with_executor("push").await;

        // Register the job first.
        let register_req = ContinuousRegisterRequest {
            job_id: "cs-push-job".to_string(),
            spec: tumbling_spec(),
        };
        api_continuous_register(State(coordinator.clone()), Json(register_req))
            .await
            .unwrap();

        // Push some (empty) IPC bytes — the partition is stored but no execution runs.
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"");
        let push_req = ContinuousPushRequest {
            job_id: "cs-push-job".to_string(),
            input_batches_b64: encoded,
        };
        let push_resp = api_continuous_push(State(coordinator.clone()), Json(push_req))
            .await
            .unwrap();
        assert!(push_resp.0.success, "push must succeed");
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
}
