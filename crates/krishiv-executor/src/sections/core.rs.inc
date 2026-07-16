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
        operator_id: krishiv_proto::OperatorId::try_new("operator-1").unwrap(),
        task_id: TaskId::try_new("task-ck-1").unwrap(),
        epoch: 1,
        fencing_token: FencingToken::initial(),
        source_offsets: vec![],
        snapshot_path: Some("/checkpoints/epoch-1".to_owned()),
        unaligned_buffers: Vec::new(),
        sink_transactions: Vec::new(),
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
        ExecutorConfig::new("exec-dereg-test", "pod-dereg", 1, format!("http://{addr}")).unwrap(),
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
async fn task_inbox_service_reports_duplicate_assignment() {
    let inbox = ExecutorAssignmentInbox::new();
    let service = ExecutorTaskInboxService::new(inbox.clone());
    let assignment = demo_assignment("task-dup-service");

    let first = service
        .assign_task(tonic::Request::new(assignment.clone()))
        .await
        .unwrap()
        .into_inner();
    let duplicate = service
        .assign_task(tonic::Request::new(assignment))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(first.disposition(), TransportDisposition::Accepted);
    assert_eq!(duplicate.disposition(), TransportDisposition::Duplicate);
    assert_eq!(inbox.len().unwrap(), 1);
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
        .assign_task(
            wire::executor_task_assignment_to_wire(demo_assignment("task-network-1")).unwrap(),
        )
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
async fn executor_task_grpc_requires_configured_bearer_token() {
    let inbox = ExecutorAssignmentInbox::new();
    let service =
        ExecutorTaskGrpcService::new(inbox.clone()).with_required_bearer_token("task-secret");

    let err = wire::v1::executor_task_server::ExecutorTask::assign_task(
        &service,
        tonic::Request::new(
            wire::executor_task_assignment_to_wire(demo_assignment("task-auth-missing")).unwrap(),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(inbox.len().unwrap(), 0);

    let mut request = tonic::Request::new(
        wire::executor_task_assignment_to_wire(demo_assignment("task-auth-ok")).unwrap(),
    );
    request.metadata_mut().insert(
        "authorization",
        tonic::metadata::MetadataValue::from_static("Bearer task-secret"),
    );
    let response = wire::v1::executor_task_server::ExecutorTask::assign_task(&service, request)
        .await
        .unwrap()
        .into_inner();
    let response = wire::task_status_response_from_wire(response).unwrap();

    assert_eq!(response.disposition(), TransportDisposition::Accepted);
    assert_eq!(inbox.len().unwrap(), 1);
}

#[tokio::test]
async fn executor_task_grpc_rejects_when_required_auth_has_no_token() {
    let inbox = ExecutorAssignmentInbox::new();
    let service = ExecutorTaskGrpcService::with_auth_config(
        inbox.clone(),
        ExecutorTaskAuthConfig::new(true, None),
    );

    let err = wire::v1::executor_task_server::ExecutorTask::assign_task(
        &service,
        tonic::Request::new(
            wire::executor_task_assignment_to_wire(demo_assignment("task-auth-misconfigured"))
                .unwrap(),
        ),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert!(err.message().contains("KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH"));
    assert_eq!(inbox.len().unwrap(), 0);
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
async fn task_runner_retains_assignment_until_coordinator_is_active() {
    allow_anonymous_for_tests();
    let executor_id = ExecutorId::try_new("exec-runner-failover").unwrap();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-runner-failover").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let inbox = ExecutorAssignmentInbox::new();
    let job_id = JobId::try_new("job-runner-failover").unwrap();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(krishiv_proto::ExecutorDescriptor::new(
                executor_id,
                "pod-runner-failover",
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
        coordinator.demote_to_standby();
    }

    let promote = shared.clone();
    let promotion = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        promote.write().await.promote_to_active();
    });

    let report = ExecutorTaskRunner::new(inbox.clone())
        .run_next_with(&service)
        .await
        .unwrap()
        .unwrap();
    promotion.await.unwrap();

    assert_eq!(report.assignment().job_id(), &job_id);
    assert_eq!(report.terminal_disposition(), TransportDisposition::Accepted);
    assert!(inbox.is_empty().unwrap());
    assert_eq!(shared.read().await.job_snapshot(&job_id).unwrap().state(), JobState::Succeeded);
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
    assert!(duplicate
        .to_string()
        .contains("duplicate local Parquet table name"));

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
    assert!(malformed
        .to_string()
        .contains("local-parquet:<table>:<path>"));
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
            krishiv_proto::ExecutorDescriptor::new(executor_id.clone(), "pod-network-runner", 1),
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

    let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(format!(
        "http://{executor_addr}"
    ))
    .await
    .unwrap();
    let assign_response = executor_client
        .assign_task(wire::executor_task_assignment_to_wire(assignment).unwrap())
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

    let mut executor_client = wire::v1::executor_task_client::ExecutorTaskClient::connect(format!(
        "http://{executor_addr}"
    ))
    .await
    .unwrap();
    let assign_response = executor_client
        .assign_task(wire::executor_task_assignment_to_wire(assignment).unwrap())
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
    let file =
        File::create(path).unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
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
async fn batch_sql_registers_local_parquet_even_when_shared_cache_contains_key() {
    allow_anonymous_for_tests();
    let temp = tempdir().unwrap();
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);

    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    runner
        .registered_parquet_cache()
        .insert(format!("people:{}", parquet_path.display()), ());

    let assignment = ExecutorTaskAssignment::new(
        TaskAttemptRef::new(
            JobId::try_new("job-local-parquet-cache").unwrap(),
            StageId::try_new("stage-1").unwrap(),
            TaskId::try_new("task-1").unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-1").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new("sql: select id, name from people where id > 1 order by id"),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "inline result"),
    )
    .with_input_partitions(vec![InputPartition::new(
        "people-part-1",
        format!("local-parquet:people:{}", parquet_path.display()),
    )]);

    let report = runner
        .run_assignment_with(assignment, &AcceptingCoordinatorService)
        .await
        .unwrap();

    assert_eq!(report.output().kind(), ExecutorTaskOutputKind::Sql);
    assert_eq!(report.output().row_count(), 2);
    assert_eq!(report.output().batch_count(), 1);
    assert_eq!(report.output().column_count(), 2);
    assert_eq!(
        report.terminal_disposition(),
        TransportDisposition::Accepted
    );
}

fn bounded_window_assignment(
    job_id: &str,
    fragment_topic: &str,
    input_topics: &[&str],
) -> ExecutorTaskAssignment {
    use base64::Engine as _;

    let spec = krishiv_plan::window::WindowExecutionSpec::tumbling("key", "ts", 1_000);
    let spec_json = serde_json::to_vec(&spec).unwrap();
    let spec_b64 = base64::engine::general_purpose::STANDARD.encode(spec_json);
    ExecutorTaskAssignment::new(
        TaskAttemptRef::new(
            JobId::try_new(job_id).unwrap(),
            StageId::try_new("stage-window").unwrap(),
            TaskId::try_new("task-window").unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-window").unwrap(),
        LeaseGeneration::initial(),
        PlanFragment::new(format!("window:{fragment_topic}:{spec_b64}")),
        OutputContract::new(OutputContractKind::InlineRecordBatches, "window output"),
    )
    .with_input_partitions(
        input_topics
            .iter()
            .enumerate()
            .map(|(index, topic)| {
                InputPartition::typed(
                    format!("window-input-{index}"),
                    InputPartitionDescriptor::InlineIpc {
                        table_name: (*topic).to_owned(),
                        ipc_bytes: Vec::new(),
                    },
                )
            })
            .collect(),
    )
}

#[tokio::test]
async fn bounded_window_rejects_mismatched_input_topic() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let error = runner
        .run_assignment_with(
            bounded_window_assignment("job-window-topic", "events", &["other"]),
            &AcceptingCoordinatorService,
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().contains("does not match fragment topic"));
}

#[tokio::test]
async fn bounded_window_rejects_multiple_input_tables() {
    let runner = ExecutorTaskRunner::new(ExecutorAssignmentInbox::new());
    let error = runner
        .run_assignment_with(
            bounded_window_assignment("job-window-input-count", "events", &["events", "events"]),
            &AcceptingCoordinatorService,
        )
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Internal);
    assert!(error.message().contains("exactly one inline input table"));
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
    use krishiv_connectors::parquet::ParquetSource;
    use krishiv_connectors::Source;

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
    use krishiv_connectors::parquet::ParquetSource;
    use krishiv_connectors::Source;

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
        ess_index: None,
        push_store: None,
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
        ExecutionModel::from_fragment("sql: SELECT 1").unwrap(),
        ExecutionModel::Batch
    );
}

#[test]
fn execution_model_batch_for_shuffle_write() {
    assert_eq!(
        ExecutionModel::from_fragment("shuffle-write:hash:key:4").unwrap(),
        ExecutionModel::Batch
    );
}

#[test]
fn execution_model_batch_for_kafka_pipeline() {
    assert_eq!(
        ExecutionModel::from_fragment("kafka-to-parquet").unwrap(),
        ExecutionModel::Batch
    );
}

#[test]
fn execution_model_streaming_for_stream_prefix() {
    assert_eq!(
        ExecutionModel::from_fragment("stream:tumbling-window:key:60s").unwrap(),
        ExecutionModel::Streaming
    );
}

#[test]
fn execution_model_streaming_exact_prefix() {
    // Any fragment starting with "stream:" is streaming regardless of suffix.
    assert_eq!(
        ExecutionModel::from_fragment("stream:").unwrap(),
        ExecutionModel::Streaming
    );
}

use crate::barrier::{BarrierSimulator, BarrierSnapshot};
