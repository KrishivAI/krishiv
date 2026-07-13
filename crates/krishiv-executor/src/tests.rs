#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod executor_tests {
    use std::fs::File;
    use std::sync::Arc;
    use std::sync::Once;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use krishiv_proto::{
        AttemptId, CheckpointAckRequest, CheckpointAckResponse, CheckpointAlignment,
        CoordinatorExecutorService, CoordinatorId, DeregisterExecutorRequest,
        DeregisterExecutorResponse, ExecutorHeartbeat, ExecutorHeartbeatRequest,
        ExecutorHeartbeatResponse, ExecutorId, ExecutorState, ExecutorTaskAssignment,
        ExecutorTaskService, FencingToken, InputPartition, InputPartitionDescriptor, JobId,
        JobKind, JobSpec, JobState, LeaseGeneration, OutputContract, OutputContractDescriptor,
        OutputContractKind, PlanFragment, RegisterExecutorRequest, RegisterExecutorResponse,
        StageId, StageSpec, StreamingTaskState, TaskAttemptRef, TaskCancellationRequest, TaskId,
        TaskSpec, TaskStatusRequest, TaskStatusResponse, TransportDisposition, TransportVersion,
        wire,
    };

    use crate::execution_model::ExecutionModel;
    use krishiv_scheduler::{
        Coordinator, CoordinatorExecutorTonicService, InMemoryMetadataStore, SharedCoordinator,
        serve_coordinator_executor_grpc_with_listener as real_serve, set_allow_anonymous,
    };

    fn allow_anonymous_for_tests() {
        static AUTH_INIT: Once = Once::new();
        AUTH_INIT.call_once(|| {
            let _ = set_allow_anonymous();
        });
    }

    async fn serve_coordinator_executor_grpc_with_listener(
        listener: tokio::net::TcpListener,
        coordinator: SharedCoordinator,
    ) -> Result<(), tonic::transport::Error> {
        allow_anonymous_for_tests();
        real_serve(listener, coordinator).await
    }

    use crate::{
        ExecutorAssignmentInbox, ExecutorConfig, ExecutorError, ExecutorRuntime,
        ExecutorTaskAuthConfig, ExecutorTaskGrpcService, ExecutorTaskInboxService,
        ExecutorTaskOutputKind, ExecutorTaskRunner, serve_executor_task_grpc_with_listener,
    };

    struct AcceptingCoordinatorService;

    #[tonic::async_trait]
    impl CoordinatorExecutorService for AcceptingCoordinatorService {
        async fn push_task_result(
            &self,
            request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
        ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
            use futures::StreamExt as _;
            let mut stream = request.into_inner();
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(tonic::Response::new(
                krishiv_proto::PushTaskResultResponse::new(
                    krishiv_proto::TransportDisposition::Accepted,
                ),
            ))
        }

        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(RegisterExecutorResponse::new(
                request.descriptor().executor_id().clone(),
                LeaseGeneration::initial(),
                TransportDisposition::Accepted,
            )))
        }

        async fn deregister_executor(
            &self,
            request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            let request = request.into_inner();
            Ok(tonic::Response::new(DeregisterExecutorResponse::new(
                request.executor_id().clone(),
                request.lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            Ok(tonic::Response::new(ExecutorHeartbeatResponse::new(
                request.into_inner().lease_generation(),
                TransportDisposition::Accepted,
            )))
        }

        async fn task_status(
            &self,
            _request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            )))
        }

        async fn checkpoint_ack(
            &self,
            _request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
        }
    }

    #[derive(Debug, Clone)]
    struct NetworkCoordinatorService {
        endpoint: String,
    }

    impl NetworkCoordinatorService {
        fn new(endpoint: impl Into<String>) -> Self {
            Self {
                endpoint: endpoint.into(),
            }
        }
    }

    #[tonic::async_trait]
    impl CoordinatorExecutorService for NetworkCoordinatorService {
        async fn push_task_result(
            &self,
            request: tonic::Request<krishiv_proto::services::TaskResultChunkStream>,
        ) -> Result<tonic::Response<krishiv_proto::PushTaskResultResponse>, tonic::Status> {
            use futures::StreamExt as _;
            let mut stream = request.into_inner();
            while let Some(chunk) = stream.next().await {
                chunk?;
            }
            Ok(tonic::Response::new(
                krishiv_proto::PushTaskResultResponse::new(
                    krishiv_proto::TransportDisposition::Accepted,
                ),
            ))
        }

        async fn register_executor(
            &self,
            request: tonic::Request<RegisterExecutorRequest>,
        ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .register_executor(wire::register_executor_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::register_executor_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn deregister_executor(
            &self,
            request: tonic::Request<DeregisterExecutorRequest>,
        ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .deregister_executor(wire::deregister_executor_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::deregister_executor_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn executor_heartbeat(
            &self,
            request: tonic::Request<ExecutorHeartbeatRequest>,
        ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
                    request.into_inner(),
                ))
                .await?
                .into_inner();
            let response = wire::executor_heartbeat_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn task_status(
            &self,
            request: tonic::Request<TaskStatusRequest>,
        ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .task_status(wire::task_status_request_to_wire(request.into_inner()))
                .await?
                .into_inner();
            let response = wire::task_status_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }

        async fn checkpoint_ack(
            &self,
            request: tonic::Request<CheckpointAckRequest>,
        ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
            let mut client =
                wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
                    self.endpoint.clone(),
                )
                .await
                .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
            let response = client
                .checkpoint_ack(wire::checkpoint_ack_request_to_wire(request.into_inner()))
                .await?
                .into_inner();
            let response = wire::checkpoint_ack_response_from_wire(response)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
            Ok(tonic::Response::new(response))
        }
    }

    include!("sections/core.rs.inc");
    include!("sections/gap6.rs.inc");
    include!("sections/stream_loop.rs.inc");
    include!("sections/run_loop_v2.rs.inc");
    include!("sections/state_v2.rs.inc");
    include!("sections/recovery.rs.inc");
}
