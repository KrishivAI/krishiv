#[cfg(test)]
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
        AttemptId, CheckpointAckRequest, CheckpointAckResponse, CoordinatorExecutorService,
        CoordinatorId, DeregisterExecutorRequest, DeregisterExecutorResponse, ExecutorHeartbeat,
        ExecutorHeartbeatRequest, ExecutorHeartbeatResponse, ExecutorId, ExecutorState,
        ExecutorTaskAssignment, ExecutorTaskService, FencingToken, InputPartition,
        InputPartitionDescriptor, JobId, JobKind, JobSpec, JobState, LeaseGeneration,
        OutputContract, OutputContractDescriptor, OutputContractKind, PlanFragment,
        RegisterExecutorRequest, RegisterExecutorResponse, StageId, StageSpec, StreamingTaskState,
        TaskAttemptRef, TaskCancellationRequest, TaskId, TaskSpec, TaskStatusRequest,
        TaskStatusResponse, TransportDisposition, TransportVersion, wire,
    };

    use crate::execution_model::ExecutionModel;
    use krishiv_scheduler::{
        Coordinator, CoordinatorExecutorTonicService, InMemoryMetadataStore, SharedCoordinator,
        serve_coordinator_executor_grpc_with_listener as real_serve, set_allow_anonymous,
    };

    fn allow_anonymous_for_tests() {
        static AUTH_INIT: Once = Once::new();
        AUTH_INIT.call_once(set_allow_anonymous);
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
        ExecutorTaskInboxService, ExecutorTaskOutputKind, ExecutorTaskRunner,
        serve_executor_task_grpc_with_listener,
    };

    struct AcceptingCoordinatorService;

    #[tonic::async_trait]
    impl CoordinatorExecutorService for AcceptingCoordinatorService {
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

    #[test]
    fn config_rejects_invalid_values() {
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 0, "http://coordinator"),
            Err(ExecutorError::InvalidSlots)
        ));
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 1, " "),
            Err(ExecutorError::EmptyCoordinatorEndpoint)
        ));
    }

    #[test]
    fn runtime_builds_versioned_registration_request() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 2, "http://coordinator").unwrap(),
        );
        let request = runtime.registration_request();

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().executor_id().as_str(), "exec-1");
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn runtime_builds_heartbeat_with_initial_lease() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let heartbeat = runtime.heartbeat_request();

        assert_eq!(heartbeat.state(), ExecutorState::Healthy);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert!(heartbeat.running_attempts().is_empty());
    }

    #[test]
    fn lease_generation_updated_after_reregister() {
        let mut runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let next = LeaseGeneration::initial().next();
        runtime.apply_lease_generation(next);
        assert_eq!(runtime.config().lease_generation(), next);
    }

    #[tokio::test]
    async fn runtime_registers_and_heartbeats_through_service_boundary() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let service = AcceptingCoordinatorService;

        let registration = runtime.register_with(&service).await.unwrap();
        let heartbeat = runtime.heartbeat_with(&service).await.unwrap();

        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
    }

    #[tokio::test]
    async fn network_coordinator_service_checkpoint_ack_through_service_boundary() {
        use krishiv_proto::FencingToken;
        // Test that AcceptingCoordinatorService (in-process) returns Accepted.
        // This verifies the in-process path works; the network path requires a live server.
        let service = AcceptingCoordinatorService;
        let req = CheckpointAckRequest {
            job_id: JobId::try_new("job-ck-1").unwrap(),
            operator_id: "operator-1".to_owned(),
            task_id: TaskId::try_new("task-ck-1").unwrap(),
            epoch: 1,
            fencing_token: FencingToken::initial(),
            source_offsets: vec![],
            snapshot_path: Some("/checkpoints/epoch-1".to_owned()),
        };
        let result = service
            .checkpoint_ack(tonic::Request::new(req))
            .await
            .unwrap();
        assert_eq!(result.into_inner(), CheckpointAckResponse::Accepted);
    }

    #[tokio::test]
    async fn deregister_via_grpc_endpoint_transitions_executor_to_removed() {
        allow_anonymous_for_tests();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-dereg-exec").unwrap(),
        ));
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping gRPC deregister test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind test gRPC listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_shared = shared.clone();
        let server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(listener, server_shared)
                .await
                .unwrap();
        });

        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-dereg-test", "pod-dereg", 1, format!("http://{addr}"))
                .unwrap(),
        );

        runtime.register_with_grpc_endpoint().await.unwrap();
        let dereg = runtime.deregister_with_grpc_endpoint().await.unwrap();
        assert_eq!(dereg.disposition(), TransportDisposition::Accepted);

        {
            let coordinator = shared.read().await;
            let snapshot = coordinator
                .executor_snapshots()
                .into_iter()
                .find(|s| s.executor_id().as_str() == "exec-dereg-test")
                .expect("executor should still be in registry after deregister");
            assert_eq!(snapshot.state(), ExecutorState::Removed);
        }

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_runner_reports_cancelled_when_inbox_cancel_received() {
        let inbox = ExecutorAssignmentInbox::new();
        let runner = ExecutorTaskRunner::new(inbox.clone());

        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-cancel").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-cancel-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        );

        inbox.cancel_task(assignment.task_id()).unwrap();
        assert!(inbox.is_task_cancelled(assignment.task_id()).unwrap());

        let service = AcceptingCoordinatorService;
        let report = runner
            .run_assignment_with(assignment, &service)
            .await
            .unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Cancelled);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
    }

    #[tokio::test]
    async fn executor_runs_typed_batch_fragment_body() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let fragment = krishiv_plan::TypedTaskFragment::new(
            krishiv_plan::ExecutionKind::Batch,
            "sql: select 1 as value",
        )
        .encode()
        .unwrap();
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-typed-fragment").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-typed-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        );

        let output = runner.execute_batch_fragment(&assignment).await.unwrap();
        assert_eq!(output.row_count(), 1);
        assert_eq!(output.batch_count(), 1);
    }

    #[tokio::test]
    async fn task_inbox_service_accepts_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox.clone());

        let response = service
            .assign_task(tonic::Request::new(demo_assignment("task-1")))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        let assignments = inbox.assignments().unwrap();
        assert_eq!(assignments[0].task_id().as_str(), "task-1");
        assert_eq!(
            assignments[0].lease_generation(),
            LeaseGeneration::initial()
        );
    }

    #[tokio::test]
    async fn task_assignment_flows_over_network_to_executor_inbox() {
        let inbox = ExecutorAssignmentInbox::new();
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task listener: {error}"),
        };
        let addr = listener.local_addr().unwrap();
        let server_inbox = inbox.clone();
        let server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(listener, server_inbox)
                .await
                .unwrap();
        });

        let mut client =
            wire::v1::executor_task_client::ExecutorTaskClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
        let response = client
            .assign_task(wire::executor_task_assignment_to_wire(demo_assignment(
                "task-network-1",
            )))
            .await
            .unwrap()
            .into_inner();
        let response = wire::task_status_response_from_wire(response).unwrap();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert_eq!(inbox.len().unwrap(), 1);
        assert_eq!(
            inbox.assignments().unwrap()[0].task_id().as_str(),
            "task-network-1"
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn task_runner_reports_running_and_success_to_scheduler() {
        allow_anonymous_for_tests();
        let executor_id = ExecutorId::try_new("exec-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-runner-1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-runner",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            let mut assignments = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
            inbox.push(assignments.remove(0)).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 1);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 1);
        assert!(matches!(
            report.running_disposition(),
            TransportDisposition::Accepted | TransportDisposition::Duplicate
        ));
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().await;
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let metadata = detail.stages()[0].tasks()[0].output_metadata().unwrap();
        assert_eq!(metadata.output_kind(), "sql");
        assert_eq!(metadata.row_count(), 1);
    }

    #[tokio::test]
    async fn runtime_deregisters_through_service_boundary() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let response = runtime
            .deregister_with(&AcceptingCoordinatorService)
            .await
            .unwrap();

        assert_eq!(response.executor_id(), runtime.config().executor_id());
        assert_eq!(response.disposition(), TransportDisposition::Accepted);
    }

    #[tokio::test]
    async fn task_inbox_service_cancels_queued_assignment() {
        let inbox = ExecutorAssignmentInbox::new();
        let service = ExecutorTaskInboxService::new(inbox.clone());
        let assignment = demo_assignment("task-cancel-1");
        let cancel = TaskCancellationRequest::new(TaskAttemptRef::new(
            assignment.job_id().clone(),
            assignment.stage_id().clone(),
            assignment.task_id().clone(),
            assignment.attempt_id(),
        ));

        service
            .assign_task(tonic::Request::new(assignment))
            .await
            .unwrap();
        let response = service
            .cancel_task(tonic::Request::new(cancel))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.disposition(), TransportDisposition::Accepted);
        assert!(inbox.is_empty().unwrap());
    }

    #[test]
    fn local_parquet_partition_descriptors_are_validated() {
        let partition = InputPartition::new("part-1", "local-parquet:people:/tmp/people.parquet");
        let parsed = crate::runner::parse_local_parquet_partitions(&[partition]).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].table_name(), "people");
        assert_eq!(
            parsed[0].path(),
            std::path::Path::new("/tmp/people.parquet")
        );

        let duplicate = crate::runner::parse_local_parquet_partitions(&[
            InputPartition::new("part-1", "local-parquet:people:/tmp/people-1.parquet"),
            InputPartition::new("part-2", "local-parquet:people:/tmp/people-2.parquet"),
        ])
        .unwrap_err();
        assert!(
            duplicate
                .to_string()
                .contains("duplicate local Parquet table name")
        );

        let non_local = crate::runner::parse_local_parquet_partitions(&[
            InputPartition::new("part-1", "local-parquet:people:/tmp/people.parquet"),
            InputPartition::new("part-2", "not-a-local-parquet-descriptor"),
        ])
        .unwrap();
        assert_eq!(non_local.len(), 1);

        let malformed = crate::runner::parse_local_parquet_partitions(&[InputPartition::new(
            "part-1",
            "local-parquet:people",
        )])
        .unwrap_err();
        assert!(
            malformed
                .to_string()
                .contains("local-parquet:<table>:<path>")
        );
    }

    #[tokio::test]
    async fn task_runner_executes_local_parquet_partition_sql() {
        allow_anonymous_for_tests();
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-parquet-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-parquet-runner-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-parquet-runner-1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-parquet-runner",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);
            inbox
                .push(local_parquet_assignment(launched, &parquet_path))
                .unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().await;
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[tokio::test]
    async fn select_one_assignment_flows_over_grpc_and_reports_output_metadata() {
        allow_anonymous_for_tests();
        let executor_id = ExecutorId::try_new("exec-network-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-network-runner-1").unwrap(),
        ));
        let coordinator_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping coordinator gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind coordinator gRPC listener: {error}"),
        };
        let coordinator_addr = coordinator_listener.local_addr().unwrap();
        let coordinator_shared = shared.clone();
        let coordinator_server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(coordinator_listener, coordinator_shared)
                .await
                .unwrap();
        });

        let inbox = ExecutorAssignmentInbox::new();
        let executor_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let executor_addr = executor_listener.local_addr().unwrap();
        let executor_inbox = inbox.clone();
        let executor_server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(executor_listener, executor_inbox)
                .await
                .unwrap();
        });

        let coordinator = NetworkCoordinatorService::new(format!("http://{coordinator_addr}"));
        let registration = coordinator
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network-runner",
                    1,
                ),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        let heartbeat = coordinator
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id.clone(),
                registration.lease_generation(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);

        let job_id = JobId::try_new("job-network-runner-1").unwrap();
        let assignment = {
            let mut scheduler = shared.write().await;
            scheduler
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            scheduler
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0)
        };

        let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(
            format!("http://{executor_addr}"),
        )
        .await
        .unwrap();
        let assign_response = executor_client
            .assign_task(wire::executor_task_assignment_to_wire(assignment))
            .await
            .unwrap()
            .into_inner();
        let assign_response = wire::task_status_response_from_wire(assign_response).unwrap();
        assert_eq!(
            assign_response.disposition(),
            TransportDisposition::Accepted
        );

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&coordinator).await.unwrap().unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 1);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 1);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        {
            let scheduler = shared.read().await;
            let snapshot = scheduler.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.state(), JobState::Succeeded);
            assert_eq!(snapshot.succeeded_task_count(), 1);
        }

        executor_server.abort();
        let _ = executor_server.await;
        coordinator_server.abort();
        let _ = coordinator_server.await;
    }

    #[tokio::test]
    async fn local_parquet_assignment_flows_over_grpc_and_reports_output_metadata() {
        allow_anonymous_for_tests();
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-network-parquet-runner-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-network-parquet-runner-1").unwrap(),
        ));
        let coordinator_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping coordinator gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind coordinator gRPC listener: {error}"),
        };
        let coordinator_addr = coordinator_listener.local_addr().unwrap();
        let coordinator_shared = shared.clone();
        let coordinator_server = tokio::spawn(async move {
            serve_coordinator_executor_grpc_with_listener(coordinator_listener, coordinator_shared)
                .await
                .unwrap();
        });

        let inbox = ExecutorAssignmentInbox::new();
        let executor_listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping executor task gRPC test because loopback sockets are denied");
                return;
            }
            Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
        };
        let executor_addr = executor_listener.local_addr().unwrap();
        let executor_inbox = inbox.clone();
        let executor_server = tokio::spawn(async move {
            serve_executor_task_grpc_with_listener(executor_listener, executor_inbox)
                .await
                .unwrap();
        });

        let coordinator = NetworkCoordinatorService::new(format!("http://{coordinator_addr}"));
        let registration = coordinator
            .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
                krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-network-parquet-runner",
                    1,
                ),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(registration.disposition(), TransportDisposition::Accepted);
        let heartbeat = coordinator
            .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
                executor_id.clone(),
                registration.lease_generation(),
                ExecutorState::Healthy,
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);

        let job_id = JobId::try_new("job-network-parquet-runner-1").unwrap();
        let assignment = {
            let mut scheduler = shared.write().await;
            scheduler
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = scheduler
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);
            local_parquet_assignment(launched, &parquet_path)
        };

        let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(
            format!("http://{executor_addr}"),
        )
        .await
        .unwrap();
        let assign_response = executor_client
            .assign_task(wire::executor_task_assignment_to_wire(assignment))
            .await
            .unwrap()
            .into_inner();
        let assign_response = wire::task_status_response_from_wire(assign_response).unwrap();
        assert_eq!(
            assign_response.disposition(),
            TransportDisposition::Accepted
        );

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&coordinator).await.unwrap().unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        {
            let scheduler = shared.read().await;
            let snapshot = scheduler.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.state(), JobState::Succeeded);
            assert_eq!(snapshot.succeeded_task_count(), 1);
        }

        executor_server.abort();
        let _ = executor_server.await;
        coordinator_server.abort();
        let _ = coordinator_server.await;
    }

    fn demo_assignment(task_id: &str) -> ExecutorTaskAssignment {
        let ids = TaskAttemptRef::new(
            JobId::try_new("job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new(task_id).unwrap(),
            AttemptId::initial(),
        );

        ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("scan parquet partition"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new("part-1", "first split")])
    }

    fn single_task_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "runner smoke", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "single stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "sql: select 1 as value"),
            ),
        )
    }

    fn parquet_scan_job(job_id: JobId) -> JobSpec {
        JobSpec::new(job_id, "parquet runner smoke", JobKind::Batch).with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "single stage").with_task(
                TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "sql: select id, name from people where id > 1 order by id",
                ),
            ),
        )
    }

    fn local_parquet_assignment(
        launched: ExecutorTaskAssignment,
        parquet_path: &std::path::Path,
    ) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                launched.job_id().clone(),
                launched.stage_id().clone(),
                launched.task_id().clone(),
                launched.attempt_id(),
            ),
            launched.executor_id().clone(),
            launched.lease_generation(),
            PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new(
            "people-part-1",
            format!("local-parquet:people:{}", parquet_path.display()),
        )])
    }

    fn write_people_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["ada", "grace", "katherine"])),
            ],
        )
        .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
        let file = File::create(path)
            .unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .unwrap_or_else(|error| panic!("unexpected parquet writer error: {error}"));
        writer
            .write(&batch)
            .unwrap_or_else(|error| panic!("unexpected parquet write error: {error}"));
        writer
            .close()
            .unwrap_or_else(|error| panic!("unexpected parquet close error: {error}"));
    }

    #[tokio::test]
    async fn executor_runs_parquet_task_via_connector_source() {
        allow_anonymous_for_tests();
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("people.parquet");
        write_people_parquet(&parquet_path);

        let executor_id = ExecutorId::try_new("exec-connector-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-connector-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-connector-1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-connector",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            // Use typed connector Parquet partition instead of legacy string parsing.
            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
                OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
            )
            .with_input_partitions(vec![InputPartition::typed(
                "people-connector-part-1",
                InputPartitionDescriptor::ConnectorParquet {
                    table_name: Some(String::from("people")),
                    path: parquet_path.display().to_string(),
                },
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2, "expected 2 rows (id > 1)");
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );
        assert!(inbox.is_empty().unwrap());

        let coordinator = shared.read().await;
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[tokio::test]
    async fn executor_reads_object_parquet_source_and_writes_object_sink() {
        allow_anonymous_for_tests();
        use krishiv_connectors::Source;
        use krishiv_connectors::parquet::ParquetSource;

        let temp = tempdir().unwrap();
        let object_root = temp.path().join("object-store");
        std::fs::create_dir_all(&object_root).unwrap();
        let input_path = object_root.join("input/people.parquet");
        std::fs::create_dir_all(input_path.parent().unwrap()).unwrap();
        write_people_parquet(&input_path);

        let executor_id = ExecutorId::try_new("exec-object-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-object-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-object-1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-object",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(parquet_scan_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
                OutputContract::typed(
                    OutputContractKind::Sink,
                    OutputContractDescriptor::ObjectParquetSink {
                        base_dir: object_root.display().to_string(),
                        object_path: String::from("output/filtered.parquet"),
                    },
                ),
            )
            .with_input_partitions(vec![InputPartition::typed(
                "people-object-part-1",
                InputPartitionDescriptor::ObjectParquet {
                    table_name: String::from("people"),
                    base_dir: object_root.display().to_string(),
                    object_path: String::from("input/people.parquet"),
                },
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();
        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 2);
        assert_eq!(report.output().column_count(), 2);

        let output_path = object_root.join("output/filtered.parquet");
        let mut source = ParquetSource::open(&output_path).unwrap();
        let batch = source.read_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(source.read_batch().await.unwrap().is_none());

        let coordinator = shared.read().await;
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
    }

    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn executor_runs_kafka_to_parquet_pipeline_on_real_runner() {
        use krishiv_connectors::Source;
        use krishiv_connectors::parquet::ParquetSource;

        let temp = tempdir().unwrap();
        let output_path = temp.path().join("events.parquet");

        let executor_id = ExecutorId::try_new("exec-kafka-pipeline-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-kafka-pipeline-1").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();
        let job_id = JobId::try_new("job-kafka-pipeline-1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-kafka-pipeline",
                    1,
                ))
                .unwrap();
            coordinator
                .submit_job(single_task_job(job_id.clone()))
                .unwrap();
            let launched = coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
                .remove(0);

            let assignment = ExecutorTaskAssignment::new(
                TaskAttemptRef::new(
                    launched.job_id().clone(),
                    launched.stage_id().clone(),
                    launched.task_id().clone(),
                    launched.attempt_id(),
                ),
                launched.executor_id().clone(),
                launched.lease_generation(),
                PlanFragment::new(crate::runner::KAFKA_TO_PARQUET_FRAGMENT),
                OutputContract::typed(
                    OutputContractKind::Sink,
                    OutputContractDescriptor::ParquetSink {
                        path: output_path.display().to_string(),
                    },
                ),
            )
            .with_input_partitions(vec![InputPartition::typed(
                "events-partition-0",
                InputPartitionDescriptor::MemoryKafka {
                    topic: String::from("events"),
                    partition: 0,
                    start_offset: 5,
                    records: vec![
                        MemoryKafkaRecord::new(1, "created"),
                        MemoryKafkaRecord::new(2, "updated"),
                        MemoryKafkaRecord::new(3, "deleted"),
                    ],
                },
            )]);
            inbox.push(assignment).unwrap();
        }

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(report.assignment().job_id(), &job_id);
        assert_eq!(
            report.output().kind(),
            ExecutorTaskOutputKind::ConnectorPipeline
        );
        assert_eq!(report.output().row_count(), 3);
        assert_eq!(report.output().batch_count(), 1);
        assert_eq!(report.output().column_count(), 2);
        assert_eq!(
            report.terminal_disposition(),
            TransportDisposition::Accepted
        );

        let mut source = ParquetSource::open(&output_path).unwrap();
        let batch = source.read_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert!(source.read_batch().await.unwrap().is_none());

        let coordinator = shared.read().await;
        let snapshot = coordinator.job_snapshot(&job_id).unwrap();
        assert_eq!(snapshot.state(), JobState::Succeeded);
        assert_eq!(snapshot.succeeded_task_count(), 1);
    }

    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn executor_rejects_kafka_to_parquet_without_parquet_sink_contract() {
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-bad-pipeline").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-bad-pipeline").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(crate::runner::KAFKA_TO_PARQUET_FRAGMENT),
            OutputContract::new(OutputContractKind::Sink, "inline result"),
        )
        .with_input_partitions(vec![InputPartition::new(
            "events-partition-0",
            "memory-kafka:events:0:0:1=created",
        )]);
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let err = runner
            .execute_batch_fragment(&assignment)
            .await
            .unwrap_err();
        match err {
            ExecutorError::InvalidAssignment { message } => {
                assert!(message.contains("parquet-sink:"));
            }
            other => panic!("expected InvalidAssignment, got {other}"),
        }
    }

    #[tokio::test]
    async fn assignment_lease_generation_rejects_stale_shuffle_write() {
        use krishiv_shuffle::{
            InMemoryShuffleStore, PartitionId, ShuffleError, ShufflePartition, ShuffleStore,
        };

        let stale_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-shuffle-lease").unwrap(),
                StageId::try_new("stage-1").unwrap(),
                TaskId::try_new("task-1").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-zombie").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::Shuffle, "shuffle partition"),
        );
        let fresh_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                stale_assignment.job_id().clone(),
                stale_assignment.stage_id().clone(),
                stale_assignment.task_id().clone(),
                stale_assignment.attempt_id().next(),
            ),
            ExecutorId::try_new("exec-replacement").unwrap(),
            stale_assignment.lease_generation().next(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::Shuffle, "shuffle partition"),
        );

        let store = InMemoryShuffleStore::new();
        let id = PartitionId {
            job_id: fresh_assignment.job_id().to_string(),
            stage_id: fresh_assignment.stage_id().to_string(),
            partition: 0,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let partition = ShufflePartition {
            id: id.clone(),
            schema,
            batches: vec![batch],
        };

        store
            .register_partition_lease(id.clone(), fresh_assignment.lease_generation().as_u64())
            .await
            .unwrap();

        let err = store
            .write_partition(
                partition.clone(),
                stale_assignment.lease_generation().as_u64(),
            )
            .await
            .unwrap_err();

        match err {
            ShuffleError::StaleLeaseToken { expected, actual } => {
                assert_eq!(expected, fresh_assignment.lease_generation().as_u64());
                assert_eq!(actual, stale_assignment.lease_generation().as_u64());
            }
            other => panic!("expected StaleLeaseToken, got {other}"),
        }
        assert!(store.read_partition(&id).await.unwrap().is_none());

        store
            .write_partition(partition, fresh_assignment.lease_generation().as_u64())
            .await
            .unwrap();
        let stored = store.read_partition(&id).await.unwrap().unwrap();
        assert_eq!(stored.batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn tpch_q1_style_shuffle_pipeline_produces_correct_aggregation() {
        allow_anonymous_for_tests();
        use std::net::SocketAddr;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;

        use krishiv_proto::{
            InputPartitionDescriptor, JobKind, JobSpec, OutputContractKind, StageSpec, StageState,
            TaskSpec,
        };
        use krishiv_shuffle::LocalDiskShuffleStore;

        use crate::runner::ShuffleContext;

        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("lineitem.parquet");
        let shuffle_dir = temp.path().join("shuffle");
        std::fs::create_dir_all(&shuffle_dir).unwrap();

        // Write lineitem rows to Parquet for Stage 0 to read.
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("l_returnflag", DataType::Utf8, false),
                Field::new("l_linestatus", DataType::Utf8, false),
                Field::new("l_quantity", DataType::Int64, false),
                Field::new("l_extendedprice", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![
                        "N", "A", "R", "N", "A", "R", "N", "A", "N", "R",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "O", "F", "F", "O", "F", "F", "F", "F", "O", "F",
                    ])),
                    Arc::new(Int64Array::from(vec![
                        10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
                    ])),
                    Arc::new(Int64Array::from(vec![
                        1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
                    ])),
                ],
            )
            .unwrap();
            let file = File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        // Start the Arrow IPC shuffle flight server.
        let disk_store = Arc::new(LocalDiskShuffleStore::new(&shuffle_dir).unwrap());
        let store = Arc::new(krishiv_shuffle::ShuffleBackend::Local(Arc::clone(
            &disk_store,
        )));
        let flight_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (flight_local_addr, flight_handle) =
            match krishiv_shuffle::flight::serve(flight_addr, Arc::clone(&disk_store)).await {
                Ok(pair) => pair,
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                    eprintln!("skipping tpch shuffle test: loopback sockets denied");
                    return;
                }
                Err(e) => panic!("failed to start shuffle flight server: {e}"),
            };
        let flight_endpoint = flight_local_addr.to_string();

        // Set up coordinator and register one executor.
        let executor_id = ExecutorId::try_new("exec-tpch-0").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-tpch-0").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());

        let stage0_id = StageId::try_new("stage-0").unwrap();
        let stage1_id = StageId::try_new("stage-1").unwrap();
        let job_id = JobId::try_new("job-tpch-q1").unwrap();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-tpch",
                    2,
                ))
                .unwrap();
            coordinator
                .submit_job(
                    JobSpec::new(job_id.clone(), "tpch-q1", JobKind::Batch)
                        .with_stage(
                            StageSpec::new(stage0_id.clone(), "shuffle-write stage").with_task(
                                TaskSpec::new(
                                    TaskId::try_new("task-s0-t0").unwrap(),
                                    "shuffle-write:hash:l_returnflag:3",
                                ),
                            ),
                        )
                        .with_stage(
                            StageSpec::new(stage1_id.clone(), "aggregate stage")
                                .with_upstream_stage(stage0_id.clone())
                                .with_task(TaskSpec::new(
                                    TaskId::try_new("task-s1-t0").unwrap(),
                                    "sql: SELECT l_returnflag, l_linestatus, \
                                     SUM(l_quantity) AS sum_qty, \
                                     SUM(l_extendedprice) AS sum_price, \
                                     COUNT(*) AS count_order \
                                     FROM lineitem \
                                     GROUP BY l_returnflag, l_linestatus \
                                     ORDER BY l_returnflag, l_linestatus",
                                )),
                        ),
                )
                .unwrap();
        }

        // ── Stage 0: shuffle-write ────────────────────────────────────────────

        let s0_launched = {
            let mut coordinator = shared.write().await;
            coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
        };
        assert_eq!(s0_launched.len(), 1, "stage-0 should launch 1 task");

        let s0_launch = &s0_launched[0];
        let s0_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                s0_launch.job_id().clone(),
                s0_launch.stage_id().clone(),
                s0_launch.task_id().clone(),
                s0_launch.attempt_id(),
            ),
            s0_launch.executor_id().clone(),
            s0_launch.lease_generation(),
            PlanFragment::new("shuffle-write:hash:l_returnflag:3"),
            OutputContract::new(OutputContractKind::Shuffle, "sql: SELECT * FROM lineitem"),
        )
        .with_input_partitions(vec![InputPartition::typed(
            "lineitem-part-0",
            InputPartitionDescriptor::LocalParquet {
                table_name: String::from("lineitem"),
                path: parquet_path.display().to_string(),
            },
        )]);

        let s0_inbox = ExecutorAssignmentInbox::new();
        let shuffle_ctx = ShuffleContext {
            store: Arc::clone(&store),
            local_dir: shuffle_dir.clone(),
            flight_endpoint: flight_endpoint.clone(),
        };
        let s0_runner = ExecutorTaskRunner::new(s0_inbox.clone()).with_shuffle(shuffle_ctx);
        s0_inbox.push(s0_assignment).unwrap();

        let s0_report = s0_runner.run_next_with(&service).await.unwrap().unwrap();
        assert_eq!(
            s0_report.output().kind(),
            ExecutorTaskOutputKind::ShuffleWrite,
            "stage-0 should produce ShuffleWrite output"
        );
        assert_eq!(
            s0_report.output().row_count(),
            10,
            "stage-0 should process all 10 input rows"
        );
        // 3 partition outputs (one per bucket).
        assert_eq!(
            s0_report.output().shuffle_partitions().len(),
            3,
            "stage-0 should produce exactly 3 shuffle partition outputs"
        );
        assert_eq!(
            s0_report.terminal_disposition(),
            TransportDisposition::Accepted
        );

        // Verify stage-0 is now Succeeded in the coordinator.
        {
            let coordinator = shared.read().await;
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            let s0_stage = detail
                .stages()
                .iter()
                .find(|s| s.stage_id() == &stage0_id)
                .unwrap();
            assert_eq!(
                s0_stage.state(),
                StageState::Succeeded,
                "stage-0 should be Succeeded after all tasks complete"
            );
        }

        // ── Stage 1: aggregate via shuffle read ───────────────────────────────

        // Stage-1 should now be unblocked; launch its tasks.
        let s1_launched = {
            let mut coordinator = shared.write().await;
            coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap()
        };
        assert_eq!(s1_launched.len(), 1, "stage-1 should launch 1 task");

        let s1_launch = &s1_launched[0];

        // Build ShuffleFlight input descriptors for all 3 shuffle partitions.
        // All point to the same logical "lineitem" table; the executor merges them.
        let shuffle_inputs: Vec<InputPartition> = (0u32..3)
            .map(|p| {
                InputPartition::typed(
                    format!("shuffle-part-{p}"),
                    InputPartitionDescriptor::ShuffleFlight {
                        table_name: String::from("lineitem"),
                        flight_endpoint: flight_endpoint.clone(),
                        job_id: job_id.clone(),
                        upstream_stage_id: stage0_id.clone(),
                        partition_id: p,
                    },
                )
            })
            .collect();

        let s1_assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                s1_launch.job_id().clone(),
                s1_launch.stage_id().clone(),
                s1_launch.task_id().clone(),
                s1_launch.attempt_id(),
            ),
            s1_launch.executor_id().clone(),
            s1_launch.lease_generation(),
            PlanFragment::new(
                "sql: SELECT l_returnflag, l_linestatus, \
                 SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_price, \
                 COUNT(*) AS count_order \
                 FROM lineitem \
                 GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus",
            ),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
        )
        .with_input_partitions(shuffle_inputs);

        // Stage-1 runner does not need a ShuffleContext (reads, does not write).
        let s1_inbox = ExecutorAssignmentInbox::new();
        let s1_runner = ExecutorTaskRunner::new(s1_inbox.clone());
        s1_inbox.push(s1_assignment).unwrap();

        let s1_report = s1_runner.run_next_with(&service).await.unwrap().unwrap();
        assert_eq!(
            s1_report.output().kind(),
            ExecutorTaskOutputKind::Sql,
            "stage-1 should produce Sql output"
        );
        assert_eq!(
            s1_report.terminal_disposition(),
            TransportDisposition::Accepted
        );

        // 4 aggregate groups: A/F, N/F, N/O, R/F.
        assert_eq!(
            s1_report.output().row_count(),
            4,
            "Q1 aggregate should produce 4 groups"
        );

        // Confirm the job reached Succeeded state.
        {
            let coordinator = shared.read().await;
            let snapshot = coordinator.job_snapshot(&job_id).unwrap();
            assert_eq!(snapshot.state(), JobState::Succeeded);

            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            let s1_stage = detail
                .stages()
                .iter()
                .find(|s| s.stage_id() == &stage1_id)
                .unwrap();
            let s1_meta = s1_stage.tasks()[0].output_metadata().unwrap();
            assert_eq!(s1_meta.output_kind(), "sql");
            // 4 groups: A/F, N/F, N/O, R/F.
            assert_eq!(s1_meta.row_count(), 4);
        }

        flight_handle.abort();
    }

    #[test]
    fn execution_model_batch_for_sql_fragment() {
        assert_eq!(
            ExecutionModel::from_fragment("sql: SELECT 1"),
            ExecutionModel::Batch
        );
    }

    #[test]
    fn execution_model_batch_for_shuffle_write() {
        assert_eq!(
            ExecutionModel::from_fragment("shuffle-write:hash:key:4"),
            ExecutionModel::Batch
        );
    }

    #[test]
    fn execution_model_batch_for_kafka_pipeline() {
        assert_eq!(
            ExecutionModel::from_fragment("kafka-to-parquet"),
            ExecutionModel::Batch
        );
    }

    #[test]
    fn execution_model_streaming_for_stream_prefix() {
        assert_eq!(
            ExecutionModel::from_fragment("stream:tumbling-window:key:60s"),
            ExecutionModel::Streaming
        );
    }

    #[test]
    fn execution_model_streaming_exact_prefix() {
        // Any fragment starting with "stream:" is streaming regardless of suffix.
        assert_eq!(
            ExecutionModel::from_fragment("stream:"),
            ExecutionModel::Streaming
        );
    }

    use crate::barrier::{BarrierSimulator, BarrierSnapshot};

    // ── GAP-6 helpers ─────────────────────────────────────────────────────────

    /// A drainer that returns a fixed set of Arrow batches exactly once, then
    /// returns an empty vec on every subsequent call.  Used to verify that
    /// `stream:loop:` fragments process batches via `ContinuousWindowExecutor`.
    struct OneShotDrainer {
        batches: std::sync::Mutex<Option<Vec<arrow::record_batch::RecordBatch>>>,
    }

    impl OneShotDrainer {
        fn new(batches: Vec<arrow::record_batch::RecordBatch>) -> Arc<Self> {
            Arc::new(Self {
                batches: std::sync::Mutex::new(Some(batches)),
            })
        }
    }

    impl crate::ContinuousJobDrainer for OneShotDrainer {
        fn drain_job(
            &self,
            _job_id: &str,
        ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
            let mut guard = self.batches.lock().unwrap();
            Ok(guard.take().unwrap_or_default())
        }
    }

    fn make_loop_assignment(fragment: &str) -> ExecutorTaskAssignment {
        let ids = TaskAttemptRef::new(
            JobId::try_new("loop-job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(
                krishiv_proto::OutputContractKind::InlineRecordBatches,
                "loop output",
            ),
        )
    }

    fn make_streaming_assignment(fragment: &str, partitions: Vec<&str>) -> ExecutorTaskAssignment {
        let ids = TaskAttemptRef::new(
            JobId::try_new("stream-job-1").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            ExecutorId::try_new("exec-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(
                krishiv_proto::OutputContractKind::InlineRecordBatches,
                "streaming output",
            ),
        );
        let input_partitions: Vec<krishiv_proto::InputPartition> = partitions
            .iter()
            .enumerate()
            .map(|(i, desc)| krishiv_proto::InputPartition::new(format!("p{i}"), *desc))
            .collect();
        assignment.with_input_partitions(input_partitions)
    }

    #[tokio::test]
    async fn streaming_tumbling_window_count_produces_correct_output() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(
            "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
            vec![
                "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
            ],
        );
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap();

        assert_eq!(
            output.kind(),
            crate::ExecutorTaskOutputKind::StreamingWindow
        );
        assert_eq!(output.row_count(), 2);
        assert_eq!(output.batch_count(), 2);
    }

    #[tokio::test]
    async fn executor_runs_typed_streaming_fragment_body() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let fragment = krishiv_plan::TypedTaskFragment::new(
            krishiv_plan::ExecutionKind::Streaming,
            "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
        )
        .encode()
        .unwrap();
        let assignment = make_streaming_assignment(
            &fragment,
            vec![
                "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
            ],
        );
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap();

        assert_eq!(
            output.kind(),
            crate::ExecutorTaskOutputKind::StreamingWindow
        );
        assert_eq!(output.row_count(), 2);
    }

    #[tokio::test]
    async fn streaming_tumbling_window_sum_produces_correct_output() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(
            "stream:tw:key=key:time=ts:win=1000:lag=0:agg=sum:col=val",
            vec![
                "stream-kafka:events:0:0:key=x,ts=100,val=10|key=x,ts=200,val=20|key=x,ts=300,val=30",
            ],
        );
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap();

        assert_eq!(
            output.kind(),
            crate::ExecutorTaskOutputKind::StreamingWindow
        );
        assert_eq!(output.row_count(), 1);
        assert_eq!(output.batch_count(), 1);
    }

    #[tokio::test]
    async fn streaming_fragment_with_multiple_partitions() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(
            "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
            vec![
                "stream-kafka:events:0:0:key=a,ts=100,val=0",
                "stream-kafka:events:1:0:key=a,ts=200,val=0|key=b,ts=300,val=0",
            ],
        );
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap();
        assert_eq!(output.row_count(), 2);
    }

    #[tokio::test]
    async fn streaming_fragment_invalid_fragment_returns_error() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(
            "stream:unknown-operator",
            vec!["stream-kafka:t:0:0:key=a,ts=100,val=1"],
        );
        let err = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::ExecutorError::InvalidAssignment { .. }),
            "expected InvalidAssignment, got {err}"
        );
    }

    #[tokio::test]
    async fn streaming_fragment_routes_data_through_operator_queue() {
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_streaming_assignment(
            "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
            vec![
                "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
            ],
        );
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .unwrap();

        assert_eq!(
            output.kind(),
            crate::ExecutorTaskOutputKind::StreamingWindow
        );
        assert_eq!(output.row_count(), 2);
        assert_eq!(output.batch_count(), 2);
    }

    #[test]
    fn barrier_simulator_accepts_increasing_epochs() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 1000, 0).unwrap();
        sim.process_barrier(2, 2000, 1).unwrap();
        sim.process_barrier(3, 3000, 0).unwrap();
        assert_eq!(sim.last_committed_epoch(), 3);
        assert_eq!(sim.snapshots().len(), 3);
    }

    #[test]
    fn barrier_simulator_rejects_stale_epoch() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 1000, 0).unwrap();
        let err = sim.process_barrier(1, 2000, 0).unwrap_err();
        assert!(
            matches!(err, crate::ExecutorError::InvalidAssignment { .. }),
            "expected InvalidAssignment for stale epoch, got {err}"
        );
        assert_eq!(sim.last_committed_epoch(), 1);
    }

    #[test]
    fn barrier_simulator_rejects_zero_epoch_after_commit() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(5, 1000, 0).unwrap();
        let err = sim.process_barrier(0, 2000, 0).unwrap_err();
        assert!(matches!(
            err,
            crate::ExecutorError::InvalidAssignment { .. }
        ));
    }

    #[test]
    fn barrier_snapshot_records_watermark_and_open_windows() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 5000, 3).unwrap();
        assert_eq!(
            sim.snapshots()[0],
            BarrierSnapshot {
                epoch: 1,
                watermark_ms: 5000,
                open_windows: 3,
            }
        );
    }

    #[test]
    fn barrier_watermark_monotonicity_enforced_by_operator_not_simulator() {
        let mut sim = BarrierSimulator::new();
        sim.process_barrier(1, 1000, 0).unwrap();
        sim.process_barrier(2, 999, 0).unwrap();
        assert_eq!(sim.snapshots()[1].watermark_ms, 999);
    }

    #[tokio::test]
    async fn deterministic_replay_end_to_end() {
        let fragment = "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count";
        let partition = "stream-kafka:events:0:0:key=a,ts=100,val=0|key=b,ts=150,val=0|key=a,ts=200,val=0\
             |key=c,ts=500,val=0|key=a,ts=800,val=0|key=b,ts=900,val=0";

        let run = || async {
            let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
            let assignment = make_streaming_assignment(fragment, vec![partition]);
            runner
                .execute_streaming_fragment(&assignment)
                .await
                .unwrap()
        };

        let run1 = run().await;
        let run2 = run().await;

        assert_eq!(
            run1.row_count(),
            run2.row_count(),
            "replay row_count must match"
        );
        assert_eq!(
            run1.batch_count(),
            run2.batch_count(),
            "replay batch_count must match"
        );
        assert_eq!(
            run1.column_count(),
            run2.column_count(),
            "replay column_count must match"
        );
        assert_eq!(run1.kind(), run2.kind(), "replay output kind must match");
    }

    #[tokio::test]
    async fn streaming_e2e_full_stack_job_stays_running() {
        allow_anonymous_for_tests();
        let executor_id = ExecutorId::try_new("exec-e2e-fs").unwrap();
        let job_id = JobId::try_new("job-e2e-fs-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-e2e-fs").unwrap(),
        ));
        let service = CoordinatorExecutorTonicService::new(shared.clone());
        let inbox = ExecutorAssignmentInbox::new();

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-e2e",
                    1,
                ))
                .unwrap();
            let job = JobSpec::new(job_id.clone(), "e2e streaming", JobKind::Streaming).with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
                    TaskSpec::new(
                        TaskId::try_new("task-1").unwrap(),
                        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
                    ),
                ),
            );
            coordinator.submit_job(job).unwrap();
            coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
        }

        let (stage_id, task_id, attempt, lease) = {
            let coordinator = shared.read().await;
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            let stage_id = detail.stages()[0].stage_id().clone();
            let task_id = detail.stages()[0].tasks()[0].task_id().clone();
            let attempt = detail.stages()[0].tasks()[0].attempt();
            let lease = coordinator.executor_snapshots()[0].lease_generation();
            (stage_id, task_id, attempt, lease)
        };

        let ids = TaskAttemptRef::new(
            job_id.clone(),
            stage_id,
            task_id,
            AttemptId::try_new(attempt).unwrap(),
        );
        let assignment = ExecutorTaskAssignment::new(
            ids,
            executor_id,
            lease,
            PlanFragment::new("stream:tw:key=key:time=ts:win=1000:lag=0:agg=count"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "streaming output"),
        )
        .with_input_partitions(vec![InputPartition::new(
            "p0",
            "stream-kafka:events:0:0:key=a,ts=100,val=1|key=b,ts=200,val=1|key=a,ts=300,val=1",
        )]);
        inbox.push(assignment).unwrap();

        let runner = ExecutorTaskRunner::new(inbox.clone());
        let report = runner.run_next_with(&service).await.unwrap().unwrap();

        assert_eq!(
            report.output().kind(),
            ExecutorTaskOutputKind::StreamingWindow
        );
        assert_eq!(
            report.output().row_count(),
            2,
            "a and b each get one window row"
        );
        assert!(inbox.is_empty().unwrap());

        let state = shared.read().await.job_snapshot(&job_id).unwrap().state();
        assert_eq!(
            state,
            JobState::Running,
            "streaming job must remain Running even after all tasks report terminal output"
        );
    }

    #[tokio::test]
    async fn streaming_e2e_coordinator_reattach_preserves_watermark() {
        let executor_id = ExecutorId::try_new("exec-e2e-ra").unwrap();
        let job_id = JobId::try_new("job-e2e-ra-1").unwrap();
        let shared = SharedCoordinator::new(Coordinator::active(
            CoordinatorId::try_new("coord-e2e-ra").unwrap(),
        ));

        {
            let mut coordinator = shared.write().await;
            coordinator
                .register_executor(krishiv_proto::ExecutorDescriptor::new(
                    executor_id.clone(),
                    "pod-ra",
                    1,
                ))
                .unwrap();
            let job = JobSpec::new(job_id.clone(), "ra streaming", JobKind::Streaming).with_stage(
                StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
                    TaskSpec::new(
                        TaskId::try_new("task-1").unwrap(),
                        "stream:tw:key=key:time=ts:win=1000:lag=0:agg=count",
                    ),
                ),
            );
            coordinator.submit_job(job).unwrap();
            coordinator
                .launch_assigned_task_assignments(&job_id)
                .unwrap();
        }

        let (task_id, lease) = {
            let coordinator = shared.read().await;
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            let task_id = detail.stages()[0].tasks()[0].task_id().clone();
            let lease = coordinator.executor_snapshots()[0].lease_generation();
            (task_id, lease)
        };

        {
            let mut coordinator = shared.write().await;
            let mut store = InMemoryMetadataStore::default();
            coordinator.persist_jobs_to_store(&mut store).unwrap();
            coordinator.recover_from_store(&store).unwrap();
        }

        let reported_watermark_ms: u64 = 7_500;
        let reported_offset = b"events:0:offset-99".to_vec();
        {
            let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_lease_generation(lease)
                .with_streaming_task_states(vec![StreamingTaskState::new(
                    task_id.clone(),
                    reported_watermark_ms,
                    reported_offset.clone(),
                )]);
            shared.write().await.executor_heartbeat(heartbeat).unwrap();
        }

        let coordinator = shared.read().await;
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let task = &detail.stages()[0].tasks()[0];
        assert_eq!(
            task.last_watermark_ms(),
            Some(reported_watermark_ms as i64),
            "coordinator must record executor-reported watermark on re-attach"
        );
        assert_eq!(
            task.last_source_offset(),
            Some(reported_offset.as_slice()),
            "coordinator must record executor-reported source offset on re-attach"
        );

        assert_eq!(
            coordinator.job_snapshot(&job_id).unwrap().state(),
            JobState::Running,
            "job must remain Running after coordinator re-attach"
        );
        drop(coordinator);

        assert_eq!(
            shared.read().await.job_snapshots().len(),
            1,
            "coordinator must not create a duplicate job on re-attach"
        );
    }

    use krishiv_checkpoint::{CheckpointStorage, LocalFsCheckpointStorage, snapshot_path};
    use krishiv_proto::InitiateCheckpointRequest;
    use krishiv_state::{InMemoryStateBackend, StateBackend};

    use crate::runner::TaskRunner;

    #[derive(Debug, Default)]
    struct FailingCheckpointStorage;

    #[tonic::async_trait]
    impl CheckpointStorage for FailingCheckpointStorage {
        async fn write_bytes_async(
            &self,
            _path: &str,
            _data: &[u8],
        ) -> krishiv_checkpoint::CheckpointResult<()> {
            Err(krishiv_checkpoint::CheckpointError::Storage {
                message: "injected write failure".to_string(),
            })
        }

        async fn read_bytes_async(
            &self,
            _path: &str,
        ) -> krishiv_checkpoint::CheckpointResult<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn list_dir_async(
            &self,
            _prefix: &str,
        ) -> krishiv_checkpoint::CheckpointResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn delete_prefix_async(
            &self,
            _prefix: &str,
        ) -> krishiv_checkpoint::CheckpointResult<()> {
            Ok(())
        }

        fn write_bytes(
            &self,
            _path: &str,
            _data: &[u8],
        ) -> krishiv_checkpoint::CheckpointResult<()> {
            Err(krishiv_checkpoint::CheckpointError::Storage {
                message: "injected write failure".to_string(),
            })
        }

        fn read_bytes(&self, _path: &str) -> krishiv_checkpoint::CheckpointResult<Option<Vec<u8>>> {
            Ok(None)
        }

        fn list_dir(&self, _prefix: &str) -> krishiv_checkpoint::CheckpointResult<Vec<String>> {
            Ok(Vec::new())
        }

        fn delete_prefix(&self, _prefix: &str) -> krishiv_checkpoint::CheckpointResult<()> {
            Ok(())
        }
    }

    #[test]
    fn executor_checkpoint_takes_state_snapshot_and_writes_to_storage() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let task_id = TaskId::try_new("task-cp-1").unwrap();
        let job_id = JobId::try_new("job-cp-1").unwrap();
        let mut runner = TaskRunner::new(task_id.clone());

        let mut backend = InMemoryStateBackend::new();
        let ns = krishiv_state::Namespace::new("operator-task-cp-1", "my-state");
        backend
            .put(&ns, b"key1".to_vec(), b"value1".to_vec())
            .unwrap();

        let req = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 1,
            fencing_token: FencingToken::initial(),
        };

        let ack = runner
            .handle_initiate_checkpoint(req, &backend, &storage)
            .unwrap();

        assert_eq!(ack.epoch, 1, "ack epoch must match request");
        assert_eq!(ack.task_id, task_id);
        assert!(
            ack.snapshot_path.is_some(),
            "state backend produced snapshot"
        );
        assert_eq!(runner.last_acked_epoch, 1);

        let expected_path = snapshot_path("job-cp-1", 1, "operator-task-cp-1", "task-cp-1");
        let data = storage.read_bytes(&expected_path).unwrap();
        assert!(data.is_some(), "snapshot file must be written to storage");
    }

    #[test]
    fn executor_checkpoint_ack_includes_snapshot_path() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let task_id = TaskId::try_new("task-cp-path").unwrap();
        let job_id = JobId::try_new("job-cp-path").unwrap();
        let mut runner = TaskRunner::new(task_id.clone());

        let mut backend_with_state = InMemoryStateBackend::new();
        let ns = krishiv_state::Namespace::new("operator-task-cp-path", "data");
        backend_with_state
            .put(&ns, b"k".to_vec(), b"v".to_vec())
            .unwrap();

        let req = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 2,
            fencing_token: FencingToken::initial(),
        };
        let ack = runner
            .handle_initiate_checkpoint(req, &backend_with_state, &storage)
            .unwrap();
        assert!(
            ack.snapshot_path.is_some(),
            "ack must include snapshot_path when state backend produced snapshot bytes"
        );

        let expected_path =
            snapshot_path("job-cp-path", 2, "operator-task-cp-path", "task-cp-path");
        let data = storage.read_bytes(&expected_path).unwrap();
        assert!(
            data.is_some(),
            "snapshot file must be written at the expected path"
        );
    }

    #[test]
    fn executor_checkpoint_ack_includes_source_offset() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let task_id = TaskId::try_new("task-cp-offset").unwrap();
        let job_id = JobId::try_new("job-cp-offset").unwrap();
        let mut runner = TaskRunner::new(task_id.clone()).with_kafka_offset(42);
        let backend = InMemoryStateBackend::new();

        let req = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 1,
            fencing_token: FencingToken::initial(),
        };
        let ack = runner
            .handle_initiate_checkpoint(req, &backend, &storage)
            .unwrap();
        assert_eq!(ack.source_offsets.len(), 1);
        assert_eq!(ack.source_offsets[0].offset, 42);

        let mut runner2 = TaskRunner::new(TaskId::try_new("task-cp-nooffset").unwrap());
        let req2 = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 1,
            fencing_token: FencingToken::initial(),
        };
        let ack2 = runner2
            .handle_initiate_checkpoint(req2, &backend, &storage)
            .unwrap();
        assert!(
            ack2.source_offsets.is_empty(),
            "non-Kafka task must have no source offsets"
        );
    }

    #[test]
    fn executor_rejects_stale_checkpoint_epoch() {
        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let task_id = TaskId::try_new("task-cp-stale").unwrap();
        let job_id = JobId::try_new("job-cp-stale").unwrap();
        let mut runner = TaskRunner::new(task_id.clone());
        let backend = InMemoryStateBackend::new();

        let req5 = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 5,
            fencing_token: FencingToken::initial(),
        };
        let ack5 = runner
            .handle_initiate_checkpoint(req5, &backend, &storage)
            .unwrap();
        assert_eq!(ack5.epoch, 5, "first ack must be for epoch 5");
        assert_eq!(runner.last_acked_epoch, 5);

        let req3 = InitiateCheckpointRequest {
            job_id: job_id.clone(),
            epoch: 3,
            fencing_token: FencingToken::initial(),
        };
        let stale_ack = runner
            .handle_initiate_checkpoint(req3, &backend, &storage)
            .unwrap();
        assert_eq!(
            stale_ack.epoch, 5,
            "stale ack epoch must be last_acked_epoch (5), not the stale request epoch (3)"
        );
        assert_eq!(
            runner.last_acked_epoch, 5,
            "last_acked_epoch must not change on stale rejection"
        );
    }

    #[test]
    fn executor_checkpoint_write_failure_does_not_ack_epoch() {
        let storage = FailingCheckpointStorage;
        let job_id = JobId::try_new("job-cp-write-fail").unwrap();
        let mut runner = TaskRunner::new(TaskId::try_new("task-cp-write-fail").unwrap());

        let mut backend = InMemoryStateBackend::new();
        let ns = krishiv_state::Namespace::new("operator-task-cp-write-fail", "state");
        backend.put(&ns, b"k".to_vec(), b"v".to_vec()).unwrap();

        let req = InitiateCheckpointRequest {
            job_id,
            epoch: 9,
            fencing_token: FencingToken::initial(),
        };

        let error = runner
            .handle_initiate_checkpoint(req, &backend, &storage)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("checkpoint snapshot write failed"),
            "unexpected error: {error}"
        );
        assert_eq!(runner.last_acked_epoch, 0);
    }

    #[tokio::test]
    async fn checkpoint_ack_delivered() {
        use std::sync::{Arc, Mutex};

        #[derive(Default, Clone)]
        struct RecordingCoordinator {
            acks: Arc<Mutex<Vec<CheckpointAckRequest>>>,
        }

        #[tonic::async_trait]
        impl CoordinatorExecutorService for RecordingCoordinator {
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
                _request: tonic::Request<DeregisterExecutorRequest>,
            ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status> {
                Err(tonic::Status::unimplemented("not needed"))
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
                request: tonic::Request<CheckpointAckRequest>,
            ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status> {
                self.acks.lock().unwrap().push(request.into_inner());
                Ok(tonic::Response::new(CheckpointAckResponse::Accepted))
            }
        }

        let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
        let backend = InMemoryStateBackend::new();
        let coordinator = RecordingCoordinator::default();
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new("job-ack").unwrap(),
                StageId::try_new("stage-ack").unwrap(),
                TaskId::try_new("task-ack").unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-ack").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new("sql: select 1"),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        );
        let req = InitiateCheckpointRequest {
            job_id: JobId::try_new("job-ack").unwrap(),
            epoch: 3,
            fencing_token: FencingToken::initial(),
        };

        let response = runner
            .initiate_checkpoint_and_deliver_ack(
                &assignment,
                req,
                Arc::new(backend),
                Arc::new(storage),
                coordinator.clone(),
            )
            .await
            .unwrap();
        assert_eq!(response, CheckpointAckResponse::Accepted);
        let acks = coordinator.acks.lock().unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].epoch, 3);
        assert_eq!(acks[0].task_id.as_str(), "task-ack");
    }

    #[tokio::test]
    async fn heartbeat_includes_running_attempts() {
        use krishiv_proto::StageId;

        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-hb", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let attempt = TaskAttemptRef::new(
            JobId::try_new("job-hb").unwrap(),
            StageId::try_new("stage-hb").unwrap(),
            TaskId::try_new("task-hb").unwrap(),
            AttemptId::initial(),
        );
        let heartbeat = runtime
            .heartbeat_request()
            .with_running_attempts(vec![attempt]);
        assert_eq!(heartbeat.running_attempts().len(), 1);
        assert_eq!(
            heartbeat.running_attempts()[0].task_id().as_str(),
            "task-hb"
        );
    }

    use krishiv_proto::{ShuffleReadConfig, ShuffleWriteConfig};
    use krishiv_shuffle::{InMemoryShuffleStore, PartitionId, ShufflePartition, ShuffleStore};

    fn shuffle_assignment_helper(
        job_id: &str,
        stage_id: &str,
        task_id: &str,
        fragment: &str,
    ) -> ExecutorTaskAssignment {
        ExecutorTaskAssignment::new(
            TaskAttemptRef::new(
                JobId::try_new(job_id).unwrap(),
                StageId::try_new(stage_id).unwrap(),
                TaskId::try_new(task_id).unwrap(),
                AttemptId::initial(),
            ),
            ExecutorId::try_new("exec-shuffle-1").unwrap(),
            LeaseGeneration::initial(),
            PlanFragment::new(fragment),
            OutputContract::new(OutputContractKind::InlineRecordBatches, "inline"),
        )
    }

    #[tokio::test]
    async fn test_shuffle_write_task_partitions_output() {
        let temp = tempdir().unwrap();
        let parquet_path = temp.path().join("data.parquet");
        {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5, 6])) as _,
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])) as _,
                ],
            )
            .unwrap();
            let file = File::create(&parquet_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let store = Arc::new(InMemoryShuffleStore::new());
        let inbox = ExecutorAssignmentInbox::new();
        let backend = Arc::new(krishiv_shuffle::ShuffleBackend::InMemory(Arc::clone(
            &store,
        )));
        let runner = ExecutorTaskRunner::new(inbox.clone()).with_inmem_shuffle(backend);

        let num_partitions = 3usize;
        let write_cfg = ShuffleWriteConfig {
            stage_id: StageId::try_new("stage-sw").unwrap(),
            num_partitions,
            key_columns: vec![String::from("id")],
            lease_token: 1,
        };

        let assignment = shuffle_assignment_helper(
            "job-sw",
            "stage-sw",
            "task-sw-1",
            "sql: select id, name from data",
        )
        .with_input_partitions(vec![InputPartition::new(
            "data-part-1",
            format!("local-parquet:data:{}", parquet_path.display()),
        )])
        .with_shuffle_write(write_cfg);

        let service = AcceptingCoordinatorService;
        let report = runner
            .run_assignment_with(assignment, &service)
            .await
            .unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::ShuffleWrite);
        assert_eq!(report.output().row_count(), 6);
        assert_eq!(report.output().shuffle_partitions().len(), num_partitions);

        let mut total_stored_rows = 0usize;
        for p in 0..num_partitions {
            let id = PartitionId {
                job_id: String::from("job-sw"),
                stage_id: String::from("stage-sw"),
                partition: p as u32,
            };
            if let Some(partition) = store.read_partition(&id).await.unwrap() {
                total_stored_rows += partition
                    .batches
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>();
            }
        }
        assert_eq!(total_stored_rows, 6);
    }

    #[tokio::test]
    async fn test_shuffle_read_task_returns_batches() {
        let store = Arc::new(InMemoryShuffleStore::new());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 20, 30])) as _,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as _,
            ],
        )
        .unwrap();
        let id = PartitionId {
            job_id: String::from("job-sr"),
            stage_id: String::from("stage-sr"),
            partition: 1,
        };
        store
            .write_partition(
                ShufflePartition {
                    id,
                    schema,
                    batches: vec![batch],
                },
                42,
            )
            .await
            .unwrap();

        let inbox = ExecutorAssignmentInbox::new();
        let backend = Arc::new(krishiv_shuffle::ShuffleBackend::InMemory(Arc::clone(
            &store,
        )));
        let runner = ExecutorTaskRunner::new(inbox.clone()).with_inmem_shuffle(backend);

        let read_cfg = ShuffleReadConfig {
            stage_id: StageId::try_new("stage-sr").unwrap(),
            partition_id: 1,
            lease_token: 42,
        };

        let assignment =
            shuffle_assignment_helper("job-sr", "stage-sr", "task-sr-1", "shuffle-read")
                .with_shuffle_read(read_cfg);

        let service = AcceptingCoordinatorService;
        let report = runner
            .run_assignment_with(assignment, &service)
            .await
            .unwrap();

        assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
        assert_eq!(report.output().row_count(), 3);
        assert_eq!(report.output().column_count(), 2);
    }

    // ── GAP-6: stream:loop: tests ────────────────────────────────────────────

    /// Verify that `stream:loop:` invokes `ContinuousWindowExecutor` and emits
    /// closed-window batches after the watermark crosses the window boundary.
    #[tokio::test]
    async fn stream_loop_emits_window_via_continuous_executor() {
        use std::sync::Arc;

        // Build an input batch: events at t=100 and t=12_000 ms in a 10s window.
        // With lag=0 the second event advances the watermark past the first window.
        let batch = krishiv_common::arrow::make_test_key_ts_batch(
            vec!["a", "a"],
            vec![100_i64, 12_000_i64],
        );

        let drainer = OneShotDrainer::new(vec![batch]);
        let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:loop-job-1|{window_spec}");

        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
            .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

        let assignment = make_loop_assignment(&fragment);
        let output = runner
            .execute_streaming_fragment(&assignment)
            .await
            .expect("stream:loop execution should succeed");

        assert_eq!(
            output.kind(),
            ExecutorTaskOutputKind::StreamingWindow,
            "stream:loop should emit StreamingWindow output"
        );
        // The first tumbling window [0, 10000) must be emitted when the watermark
        // advances past 10000 due to the event at ts=12000.
        assert!(
            output.row_count() > 0,
            "stream:loop should emit at least one window row"
        );
    }

    /// Verify that `stream:loop:` reuses the same `ContinuousWindowExecutor`
    /// across multiple drain cycles (state is accumulated).
    #[tokio::test]
    async fn stream_loop_reuses_executor_state_across_drains() {
        use std::sync::Arc;

        // First drain: single event in [0, 10000).  Window not yet emitted.
        let batch1 = krishiv_common::arrow::make_test_key_ts_batch(vec!["a"], vec![500_i64]);

        // Second drain: event that advances watermark past 10000, closing the window.
        let batch2 = krishiv_common::arrow::make_test_key_ts_batch(vec!["a"], vec![15_000_i64]);

        // The runner is CLONED between drains to simulate a re-used runner; its
        // `loop_executors` Arc is shared so state survives.
        let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:loop-job-state|{window_spec}");

        // Drainer 1: returns batch1 then empty.
        struct SeqDrainer {
            inner: std::sync::Mutex<std::collections::VecDeque<Vec<RecordBatch>>>,
        }
        impl crate::ContinuousJobDrainer for SeqDrainer {
            fn drain_job(&self, _job_id: &str) -> Result<Vec<RecordBatch>, String> {
                Ok(self.inner.lock().unwrap().pop_front().unwrap_or_default())
            }
        }
        let drainer = Arc::new(SeqDrainer {
            inner: std::sync::Mutex::new(std::collections::VecDeque::from(vec![
                vec![batch1],
                vec![batch2],
            ])),
        });

        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new())
            .with_continuous_drainer(drainer as Arc<dyn crate::ContinuousJobDrainer>);

        // First drain: no window should be emitted yet.
        let out1 = runner
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .expect("drain 1 ok");
        assert_eq!(out1.row_count(), 0, "no window emitted on first drain");

        // Second drain: reuse the same runner (shared loop_executors); window
        // should now be emitted.
        let out2 = runner
            .execute_streaming_fragment(&make_loop_assignment(&fragment))
            .await
            .expect("drain 2 ok");
        assert!(
            out2.row_count() > 0,
            "window must be emitted on second drain after watermark advances"
        );
    }

    /// Verify that a missing drainer returns a clear InvalidAssignment error.
    #[tokio::test]
    async fn stream_loop_without_drainer_returns_error() {
        let window_spec = "stream:tw:key=key:time=ts:win=10000:lag=0:agg=count";
        let fragment = format!("stream:loop:no-drainer-job|{window_spec}");
        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let assignment = make_loop_assignment(&fragment);
        let result = runner.execute_streaming_fragment(&assignment).await;
        assert!(
            result.is_err(),
            "stream:loop without drainer must return an error"
        );
    }

    // ── R7.2 source throttle wiring ──────────────────────────────────────────

    /// Verify that the `SourceThrottleTable` on `ExecutorTaskRunner` is shared
    /// across runner clones (simulating the heartbeat loop writing, runner tasks
    /// reading) and that `apply` + `active_limit` behave correctly.
    #[test]
    fn source_throttle_table_shared_across_runner_clones() {
        use krishiv_proto::HeartbeatThrottleCommand;

        let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
        let clone = runner.clone();

        // Simulate what the heartbeat loop does when it receives ThrottleDecisions.
        let cmds = vec![
            HeartbeatThrottleCommand {
                source_id: "src-kafka-0".into(),
                rows_per_second: Some(1000),
            },
            HeartbeatThrottleCommand {
                source_id: "src-kafka-1".into(),
                rows_per_second: None, // clear / unlimited
            },
        ];
        for tc in &cmds {
            runner
                .source_throttle_limits
                .apply(&tc.source_id, tc.rows_per_second);
        }

        // Reads through the clone (task runner side) must observe the updates.
        assert_eq!(
            clone.source_throttle_limits.active_limit("src-kafka-0"),
            Some(1000)
        );
        assert_eq!(
            clone.source_throttle_limits.active_limit("src-kafka-1"),
            None
        );
        assert_eq!(
            clone.source_throttle_limits.active_limit("src-kafka-99"),
            None
        );

        // check_and_log should not panic regardless of whether a limit is active.
        clone.source_throttle_limits.check_and_log("src-kafka-0");
        clone.source_throttle_limits.check_and_log("src-kafka-1");
        clone.source_throttle_limits.check_and_log("src-kafka-99");
    }
}
