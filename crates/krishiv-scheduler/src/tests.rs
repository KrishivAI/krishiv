#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod scheduler_tests {
    use std::sync::{Arc, Mutex, Once};

    use krishiv_plan::{ExecutionKind as PlanExecutionKind, LogicalPlan, PhysicalPlan, PlanNode};
    use krishiv_proto::{
        AttemptId, CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
        CoordinatorId, DeregisterExecutorRequest, ExecutorDescriptor, ExecutorHeartbeat,
        ExecutorHeartbeatRequest, ExecutorId, ExecutorState, FencingToken, JobId, JobKind, JobSpec,
        JobState, LeaseGeneration, RegisterExecutorRequest, StageId, StageSpec, StreamingTaskState,
        TaskAttemptRef, TaskId, TaskOutputMetadata, TaskSpec, TaskState, TaskStatusRequest,
        TaskStatusResponse, TaskStatusUpdate, TransportDisposition, wire,
    };
    use krishiv_state::checkpoint::{
        CheckpointMetadata, CheckpointStorage, IntegrityManifest, LocalFsCheckpointStorage,
        list_valid_epochs, write_epoch_metadata, write_manifest,
    };

    use crate::{
        AdaptiveDecisionKind, AdaptiveOverrideConfig, CheckpointCoordinator,
        CheckpointCoordinatorState, Coordinator, CoordinatorConfig,
        CoordinatorExecutorTonicService, EventLogEvent, ExecutorRegistry, InMemoryQueueManager,
        InProcessCoordinatorBridge, LeaderElection, MetadataStore, NamespaceQuotaSnapshot,
        QueueManager, SchedulerError, SharedCoordinator, SingleNodeElection, StaticScheduler,
        SubmitOutcome, TaskUpdateOutcome, job_spec_from_logical_plan, job_spec_from_physical_plan,
        serve_coordinator_executor_grpc_with_listener,
    };

    fn allow_anonymous_for_tests() {
        static AUTH_INIT: Once = Once::new();
        AUTH_INIT.call_once(|| {
            let _ = crate::auth::set_allow_anonymous();
        });
    }

    fn in_process_bridge_for(coordinator: Coordinator) -> InProcessCoordinatorBridge {
        // Both exec and ckpt are now embedded in Coordinator; clone them directly
        // to seed the sharded inner locks.
        let executor_inner = Arc::new(tokio::sync::RwLock::new(coordinator.exec.clone()));
        let checkpoint_inner = Arc::new(tokio::sync::RwLock::new(coordinator.ckpt.clone()));
        InProcessCoordinatorBridge::new(
            Arc::new(Mutex::new(coordinator)),
            executor_inner,
            checkpoint_inner,
        )
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingExecutorTaskService {
        task_ids: Arc<Mutex<Vec<String>>>,
        cancelled_task_ids: Arc<Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl wire::v1::executor_task_server::ExecutorTask for RecordingExecutorTaskService {
        async fn assign_task(
            &self,
            request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            let assignment = wire::executor_task_assignment_from_wire(request.into_inner())
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            self.task_ids
                .lock()
                .unwrap()
                .push(assignment.task_id().as_str().to_owned());
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }

        async fn cancel_task(
            &self,
            request: tonic::Request<wire::v1::TaskCancellationRequest>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            let req = wire::task_cancellation_request_from_wire(request.into_inner())
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            self.cancelled_task_ids
                .lock()
                .unwrap()
                .push(req.task_id().as_str().to_owned());
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }

        async fn push_continuous_input(
            &self,
            _request: tonic::Request<wire::v1::PushContinuousInputRequest>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }

        async fn drain_continuous_output(
            &self,
            _request: tonic::Request<wire::v1::DrainContinuousOutputRequest>,
        ) -> Result<tonic::Response<wire::v1::DrainContinuousOutputResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used in tests"))
        }
    }

    include!("sections/core.rs.inc");
    include!("sections/retry_streaming.rs.inc");
    include!("sections/validation.rs.inc");
    include!("sections/recovery.rs.inc");
    include!("sections/checkpoint.rs.inc");
    include!("sections/savepoint.rs.inc");
    include!("sections/chaos_basic.rs.inc");
    include!("sections/checkpoint_timer.rs.inc");
    include!("sections/barrier_oob.rs.inc");
    include!("sections/queue_manager.rs.inc");
    include!("sections/adaptive.rs.inc");
    include!("sections/prr_parallel.rs.inc");
    include!("sections/chaos_jcp.rs.inc");
    include!("sections/failover.rs.inc");
    include!("sections/etcd_sim.rs.inc");
    include!("sections/placement.rs.inc");
    include!("sections/chaos_restart.rs.inc");
    include!("sections/streaming_recovery.rs.inc");
    include!("sections/dur1.rs.inc");
}
