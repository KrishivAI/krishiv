fn standby_coordinator_rejects_mutation() {
    let mut coordinator = Coordinator::standby(CoordinatorId::try_new("coord-1").unwrap());
    let executor = ExecutorDescriptor::new(ExecutorId::try_new("exec-1").unwrap(), "pod-a", 1);

    let error = coordinator.register_executor(executor).unwrap_err();

    assert!(matches!(error, SchedulerError::InactiveCoordinator { .. }));
}

#[test]
fn standby_coordinator_rejects_savepoint_mutation() {
    let mut coordinator = Coordinator::standby(CoordinatorId::try_new("coord-1").unwrap());
    let job_id = JobId::try_new("job-standby-savepoint").unwrap();

    let error = coordinator.savepoint_job(&job_id, None).unwrap_err();

    assert!(matches!(error, SchedulerError::InactiveCoordinator { .. }));
}

#[test]
fn executor_registry_accepts_registration_and_heartbeat() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut registry = ExecutorRegistry::default();
    registry
        .register(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();
    registry
        .heartbeat(ExecutorHeartbeat::new(
            executor_id.clone(),
            ExecutorState::Healthy,
        ))
        .unwrap();

    assert_eq!(registry.list().len(), 1);
    assert_eq!(registry.list()[0].state(), ExecutorState::Healthy);
    assert_eq!(registry.list()[0].last_heartbeat_tick(), 0);
}

#[test]
fn heartbeat_timeout_marks_executor_lost() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-1").unwrap(),
        CoordinatorConfig::new(1, 2),
    );
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();
    coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(
            executor_id.clone(),
            ExecutorState::Healthy,
        ))
        .unwrap();

    assert!(coordinator.advance_heartbeat_clock(1).unwrap().is_empty());
    let lost = coordinator.advance_heartbeat_clock(1).unwrap();

    assert_eq!(lost, vec![executor_id]);
    assert_eq!(
        coordinator.executor_snapshots()[0].state(),
        ExecutorState::Lost
    );
}

#[test]
fn stale_lease_heartbeat_is_rejected_after_executor_loss() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let lease_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    coordinator.mark_executor_lost(&executor_id).unwrap();
    let current_generation = coordinator.executor_snapshots()[0].lease_generation();
    let error = coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_lease_generation(lease_generation),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        SchedulerError::StaleExecutorLease {
            expected,
            received,
            ..
        } if expected == current_generation && received == lease_generation
    ));
}

/// Extract a Prometheus counter value rendered as `<name> <value>` (no
/// labels) from a full exposition body, for delta-based assertions on the
/// process-global metrics singleton (which other parallel tests may also
/// increment, so only monotonic growth — not an absolute value — is safe
/// to assert).
fn prometheus_counter_value(rendered: &str, metric_name: &str) -> u64 {
    let prefix = format!("{metric_name} ");
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("metric {metric_name} not found in:\n{rendered}"))
}

/// Regression (Wave 4 — Observability & Shutdown): `mark_executor_lost`
/// must call `inc_executor_lost` so heartbeat-timeout losses are visible
/// in `krishiv_executor_lost_total` (the metric call was added alongside
/// the counter and Prometheus renderer line in this wave).
#[test]
fn mark_executor_lost_increments_executor_lost_metric() {
    let executor_id = ExecutorId::try_new("exec-metric-lost").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-metric").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    let before = prometheus_counter_value(
        &krishiv_metrics::global_metrics().render_prometheus(),
        "krishiv_executor_lost_total",
    );
    coordinator.mark_executor_lost(&executor_id).unwrap();
    let after = prometheus_counter_value(
        &krishiv_metrics::global_metrics().render_prometheus(),
        "krishiv_executor_lost_total",
    );
    assert!(
        after > before,
        "mark_executor_lost must increment krishiv_executor_lost_total (before={before}, after={after})"
    );
}

#[test]
fn lost_executor_can_reregister_with_next_lease_generation() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let initial_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    coordinator.mark_executor_lost(&executor_id).unwrap();
    let next_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-b", 2))
        .unwrap();

    assert_eq!(next_generation, initial_generation.next());
    let executor = &coordinator.executor_snapshots()[0];
    assert_eq!(executor.state(), ExecutorState::Registered);
    assert_eq!(executor.descriptor().host(), "pod-b");
    assert_eq!(executor.descriptor().slots(), 2);
    assert_eq!(executor.lease_generation(), next_generation);
}

#[test]
fn fast_executor_reregister_reopens_unacknowledged_assignment_launch() {
    let executor_id = ExecutorId::try_new("exec-fast-replacement").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-fast-replacement").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-old", 1))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    coordinator.submit_job(job).unwrap();
    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 1);
    assert_eq!(
        coordinator.launch_assigned_tasks(&job_id).unwrap(),
        0,
        "accepted launch stays guarded until an executor acknowledgement"
    );

    // The replacement keeps the stable executor id and arrives before the
    // heartbeat timeout marks the old process lost.
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "pod-replacement", 1))
        .unwrap();

    assert_eq!(
        coordinator.launch_assigned_tasks(&job_id).unwrap(),
        1,
        "registration must reopen the accepted-but-unacknowledged launch"
    );
}

#[test]
fn same_endpoint_reregister_reopens_launch_without_marking_executor_lost() {
    let executor_id = ExecutorId::try_new("exec-same-endpoint").unwrap();
    let descriptor = ExecutorDescriptor::new(executor_id.clone(), "pod-stable", 1)
        .with_task_endpoint("http://pod-stable:2005");
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-same-endpoint").unwrap());
    coordinator.register_executor(descriptor.clone()).unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    coordinator.submit_job(job).unwrap();
    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 1);
    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 0);

    // This models the same surviving process refreshing its lease after a
    // coordinator failover. It needs idempotent redelivery, not task loss.
    coordinator.register_executor(descriptor).unwrap();

    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 1);
}

#[test]
fn executor_deregisters_with_valid_lease() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let lease_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    let next_generation = coordinator
        .deregister_executor(&executor_id, lease_generation)
        .unwrap();

    let executor = &coordinator.executor_snapshots()[0];
    assert_eq!(executor.state(), ExecutorState::Removed);
    assert_eq!(executor.lease_generation(), next_generation);
}

#[test]
fn cancel_job_marks_active_tasks_cancelled() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job_id = JobId::try_new("job-cancel").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap())
        .with_store(crate::InMemoryMetadataStore::default());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "pod-a", 1))
        .unwrap();
    coordinator
        .submit_job(demo_job_with_id(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    coordinator.cancel_job(&job_id).unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(detail.job().state(), JobState::Cancelled);
    assert_eq!(
        detail.stages()[0].state(),
        krishiv_proto::StageState::Cancelled
    );
    assert!(
        detail.stages()[0]
            .tasks()
            .iter()
            .all(|task| task.state() == TaskState::Cancelled)
    );
    let persisted = coordinator
        .store
        .as_ref()
        .unwrap()
        .inner()
        .jobs()
        .iter()
        .find(|job| job.job_id() == &job_id)
        .cloned()
        .unwrap();
    assert_eq!(persisted.state(), JobState::Cancelled);
    let history = coordinator.get_job_history(job_id.as_str()).unwrap();
    assert_eq!(history.final_state, "cancelled");
}

#[test]
fn submit_job_replaces_a_terminal_job_with_the_same_id() {
    // A pipeline reconcile deregisters (cancels) a live job, then re-registers
    // the same id. cancel_job marks the job GC-ready but keeps it in the
    // registry, so re-submission must evict the terminal job rather than reject
    // it as a duplicate (previously a 409 until the background GC tick ran).
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job_id = JobId::try_new("job-replace").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "pod-a", 1))
        .unwrap();
    coordinator
        .submit_job(demo_job_with_id(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    coordinator.cancel_job(&job_id).unwrap();
    assert!(
        coordinator
            .job_detail_snapshot(&job_id)
            .unwrap()
            .job()
            .state()
            .is_terminal()
    );

    // Re-submitting the same id succeeds (evicts the cancelled job) rather than
    // failing DuplicateJob.
    coordinator
        .submit_job(demo_job_with_id(job_id.clone()))
        .expect("re-submit of a terminal job id must replace it, not 409");
    assert!(
        !coordinator
            .job_detail_snapshot(&job_id)
            .unwrap()
            .job()
            .state()
            .is_terminal(),
        "the replacement job must be a fresh, non-terminal job"
    );

    // A *live* job with the same id is still a genuine duplicate.
    let err = coordinator
        .submit_job(demo_job_with_id(job_id.clone()))
        .unwrap_err();
    assert!(
        matches!(err, SchedulerError::DuplicateJob { .. }),
        "got {err:?}"
    );
}

#[test]
fn task_output_metadata_is_visible_in_job_detail_snapshot() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job_id = JobId::try_new("job-output-meta").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let lease_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();
    let assignment = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap()
        .remove(0);
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                executor_id.clone(),
                TaskState::Running,
                assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_generation),
        )
        .unwrap();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                executor_id,
                TaskState::Succeeded,
                assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_generation)
            .with_output_metadata(TaskOutputMetadata::new("sql", 2, 1, 2)),
        )
        .unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let metadata = detail.stages()[0].tasks()[0].output_metadata().unwrap();
    assert_eq!(metadata.output_kind(), "sql");
    assert_eq!(metadata.row_count(), 2);
    assert_eq!(metadata.batch_count(), 1);
    assert_eq!(metadata.column_count(), 2);
}

/// Regression (Wave 4 — Observability & Shutdown): `apply_task_update`
/// must call `inc_tasks_succeeded` / `inc_tasks_failed` on the
/// corresponding terminal transitions, so `krishiv_tasks_succeeded_total`
/// / `krishiv_tasks_failed_total` reflect actual job outcomes.
#[test]
fn apply_task_update_increments_succeeded_and_failed_metrics() {
    let executor_id = ExecutorId::try_new("exec-metric-task").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-metric-task").unwrap());
    let lease_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let succeeded_job_id = JobId::try_new("job-metric-succeeded").unwrap();
    coordinator
        .submit_job(single_task_job(succeeded_job_id.clone()))
        .unwrap();
    let succeeded_assignment = coordinator
        .launch_assigned_task_assignments(&succeeded_job_id)
        .unwrap()
        .remove(0);

    let failed_job_id = JobId::try_new("job-metric-failed").unwrap();
    coordinator
        .submit_job(single_task_job(failed_job_id.clone()))
        .unwrap();
    let failed_assignment = coordinator
        .launch_assigned_task_assignments(&failed_job_id)
        .unwrap()
        .remove(0);

    let rendered_before = krishiv_metrics::global_metrics().render_prometheus();
    let succeeded_before = prometheus_counter_value(
        &rendered_before,
        r#"krishiv_tasks_total{status="succeeded"}"#,
    );
    let failed_before =
        prometheus_counter_value(&rendered_before, r#"krishiv_tasks_total{status="failed"}"#);

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                succeeded_job_id.clone(),
                succeeded_assignment.stage_id().clone(),
                succeeded_assignment.task_id().clone(),
                executor_id.clone(),
                TaskState::Succeeded,
                succeeded_assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_generation),
        )
        .unwrap();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                failed_job_id.clone(),
                failed_assignment.stage_id().clone(),
                failed_assignment.task_id().clone(),
                executor_id,
                TaskState::Failed,
                failed_assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_generation),
        )
        .unwrap();

    let rendered_after = krishiv_metrics::global_metrics().render_prometheus();
    let succeeded_after = prometheus_counter_value(
        &rendered_after,
        r#"krishiv_tasks_total{status="succeeded"}"#,
    );
    let failed_after =
        prometheus_counter_value(&rendered_after, r#"krishiv_tasks_total{status="failed"}"#);

    assert!(
        succeeded_after > succeeded_before,
        "Succeeded transition must increment krishiv_tasks_total{{status=\"succeeded\"}} (before={succeeded_before}, after={succeeded_after})"
    );
    assert!(
        failed_after > failed_before,
        "Failed transition must increment krishiv_tasks_total{{status=\"failed\"}} (before={failed_before}, after={failed_after})"
    );
}

#[test]
fn stability_metrics_include_heartbeat_age_and_task_counts() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job_id = JobId::try_new("job-metrics").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();
    coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(
            executor_id.clone(),
            ExecutorState::Healthy,
        ))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    coordinator.advance_heartbeat_clock(1).unwrap();

    let metrics = coordinator.stability_metrics();
    assert_eq!(metrics.heartbeat_ages()[0].executor_id(), &executor_id);
    assert_eq!(metrics.heartbeat_ages()[0].age_ticks(), 1);
    assert_eq!(
        coordinator
            .job_snapshot(&job_id)
            .unwrap()
            .assigned_task_count(),
        1
    );
}

#[tokio::test]
async fn shared_coordinator_exposes_same_scheduler_state_to_clones() {
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let observer = shared.clone();
    let executor_id = ExecutorId::try_new("exec-1").unwrap();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator
            .executor_heartbeat(ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy))
            .unwrap();
    }

    let coordinator = observer.read().await;
    assert_eq!(coordinator.executor_snapshots().len(), 1);
    assert_eq!(
        coordinator.executor_snapshots()[0].state(),
        ExecutorState::Healthy
    );
}

#[test]
fn launched_task_stays_assigned_until_executor_reports_running() {
    let executor_id = ExecutorId::try_new("exec-launch-state").unwrap();
    let job_id = JobId::try_new("job-launch-state").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-launch").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "pod-a", 1))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    assert_eq!(assignments.len(), 1);

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.state(),
        TaskState::Assigned,
        "the coordinator must not mark a task Running before the executor acks launch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_registers_executor_through_shared_coordinator() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();

    let response = service
        .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
            ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::Accepted);
    assert_eq!(response.lease_generation(), LeaseGeneration::initial());
    let coordinator = shared.read().await;
    assert_eq!(coordinator.executor_snapshots().len(), 1);
    assert_eq!(
        coordinator.executor_snapshots()[0].executor_id(),
        &executor_id
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_rejects_checkpoint_ack_when_standby() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::standby(
        CoordinatorId::try_new("coord-standby").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared);
    let job_id = JobId::try_new("job-standby-ack").unwrap();
    let ack = make_ack(
        &job_id,
        "task-standby-ack",
        1,
        FencingToken::initial(),
        None,
    );

    let status = service
        .checkpoint_ack(tonic::Request::new(ack))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("only the active coordinator"),
        "unexpected status: {status}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_rejects_task_status_when_standby() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::standby(
        CoordinatorId::try_new("coord-standby-status").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared);
    let request = TaskStatusRequest::new(
        TaskAttemptRef::new(
            JobId::try_new("job-standby-status").unwrap(),
            StageId::try_new("stage-standby-status").unwrap(),
            TaskId::try_new("task-standby-status").unwrap(),
            AttemptId::initial(),
        ),
        ExecutorId::try_new("exec-standby-status").unwrap(),
        LeaseGeneration::initial(),
        TaskState::Running,
    );

    let status = service
        .task_status(tonic::Request::new(request))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("only the active coordinator"),
        "unexpected status: {status}"
    );
}

#[tokio::test]
async fn in_process_bridge_rejects_heartbeat_when_standby() {
    let bridge = in_process_bridge_for(Coordinator::standby(
        CoordinatorId::try_new("coord-standby").unwrap(),
    ));
    let heartbeat = ExecutorHeartbeatRequest::new(
        ExecutorId::try_new("exec-standby").unwrap(),
        LeaseGeneration::initial(),
        ExecutorState::Healthy,
    );

    let status = bridge
        .executor_heartbeat(tonic::Request::new(heartbeat))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("only the active coordinator"),
        "unexpected status: {status}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_register_executor_persists_descriptor() {
    allow_anonymous_for_tests();
    let store = crate::rocksdb_metadata::RocksDbMetadataStore::in_memory().unwrap();
    let shared = SharedCoordinator::new(
        Coordinator::active(CoordinatorId::try_new("coord-persist").unwrap()).with_store(store),
    );
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-persist").unwrap();

    let response = service
        .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
            ExecutorDescriptor::new(executor_id.clone(), "pod-persist", 2),
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::Accepted);

    if let Some(store) = &shared.read().await.store {
        store.flush().await;
    }
    let executors = shared.read().await.executors().list();
    assert_eq!(executors.len(), 1);
    assert_eq!(executors[0].descriptor.executor_id(), &executor_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_applies_executor_heartbeat_to_shared_coordinator() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let task_id = TaskId::try_new("task-1").unwrap();

    service
        .register_executor(tonic::Request::new(RegisterExecutorRequest::new(
            ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2),
        )))
        .await
        .unwrap();

    let heartbeat = ExecutorHeartbeatRequest::new(
        executor_id.clone(),
        LeaseGeneration::initial(),
        ExecutorState::Healthy,
    )
    .with_running_attempts(vec![TaskAttemptRef::new(
        JobId::try_new("job-1").unwrap(),
        StageId::try_new("stage-1").unwrap(),
        task_id.clone(),
        AttemptId::initial(),
    )]);
    let response = service
        .executor_heartbeat(tonic::Request::new(heartbeat))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::Accepted);
    let coordinator = shared.read().await;
    let executor = &coordinator.executor_snapshots()[0];
    assert_eq!(executor.state(), ExecutorState::Healthy);
    assert_eq!(executor.running_tasks(), &[task_id]);
}

#[tokio::test]
async fn tonic_service_reports_unknown_executor_heartbeat_as_domain_response() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared);

    let response = service
        .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
            ExecutorId::try_new("missing-exec").unwrap(),
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.disposition(),
        TransportDisposition::UnknownExecutor
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_reports_stale_lease_heartbeat_as_domain_response() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
            .unwrap();
        coordinator.mark_executor_lost(&executor_id).unwrap();
    }

    let response = service
        .executor_heartbeat(tonic::Request::new(ExecutorHeartbeatRequest::new(
            executor_id,
            LeaseGeneration::initial(),
            ExecutorState::Healthy,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::StaleLease);
    assert_eq!(
        response.lease_generation(),
        LeaseGeneration::initial().next()
    );
}

#[tokio::test]
async fn coordinator_pushes_assignments_to_executor_task_endpoint() {
    let service = RecordingExecutorTaskService::default();
    let recorded = service.task_ids.clone();
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping assignment push test because loopback sockets are denied");
            return;
        }
        Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job_id = JobId::try_new("job-push").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(
            ExecutorDescriptor::new(executor_id, "pod-a", 1)
                .with_task_endpoint(format!("http://{addr}")),
        )
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    let responses = coordinator
        .push_assigned_task_assignments(&job_id)
        .await
        .unwrap();

    assert_eq!(responses[0].disposition(), TransportDisposition::Accepted);
    assert_eq!(recorded.lock().unwrap().as_slice(), &["task-1".to_owned()]);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn coordinator_retries_transient_assignment_rpc_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct FlakyExecutorTaskService {
        calls: Arc<AtomicUsize>,
        accepted: Arc<Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl wire::v1::executor_task_server::ExecutorTask for FlakyExecutorTaskService {
        async fn assign_task(
            &self,
            request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(tonic::Status::unavailable("temporary executor rpc failure"));
            }
            let assignment = wire::executor_task_assignment_from_wire(request.into_inner())
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            self.accepted
                .lock()
                .unwrap()
                .push(assignment.task_id().as_str().to_owned());
            Ok(tonic::Response::new(wire::task_status_response_to_wire(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )))
        }

        async fn cancel_task(
            &self,
            _request: tonic::Request<wire::v1::TaskCancellationRequest>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn push_continuous_input(
            &self,
            _request: tonic::Request<wire::v1::PushContinuousInputRequest>,
        ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn drain_continuous_output(
            &self,
            _request: tonic::Request<wire::v1::DrainContinuousOutputRequest>,
        ) -> Result<tonic::Response<wire::v1::DrainContinuousOutputResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("not used"))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let service = FlakyExecutorTaskService {
        calls: Arc::clone(&calls),
        accepted: Arc::clone(&accepted),
    };
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping assignment retry test because loopback sockets are denied");
            return;
        }
        Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let executor_id = ExecutorId::try_new("exec-retry").unwrap();
    let job_id = JobId::try_new("job-retry").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-retry").unwrap());
    coordinator
        .register_executor(
            ExecutorDescriptor::new(executor_id, "pod-retry", 1)
                .with_task_endpoint(format!("http://{addr}")),
        )
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    let responses = coordinator
        .push_assigned_task_assignments(&job_id)
        .await
        .unwrap();

    assert_eq!(responses[0].disposition(), TransportDisposition::Accepted);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(accepted.lock().unwrap().as_slice(), &["task-1".to_owned()]);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn task_launch_drives_to_running() {
    let service = RecordingExecutorTaskService::default();
    let recorded = service.task_ids.clone();
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping task_launch_drives_to_running: loopback denied");
            return;
        }
        Err(error) => panic!("failed to bind executor task gRPC listener: {error}"),
    };
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(wire::v1::executor_task_server::ExecutorTaskServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let executor_id = ExecutorId::try_new("exec-launch").unwrap();
    let job_id = JobId::try_new("job-launch").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-launch").unwrap());
    coordinator
        .register_executor(
            ExecutorDescriptor::new(executor_id, "pod-launch", 1)
                .with_task_endpoint(format!("http://{addr}")),
        )
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    let shared = SharedCoordinator::new(coordinator);
    let launched = shared.drive_pending_task_launches().await.unwrap();
    assert_eq!(launched, 1);
    assert_eq!(recorded.lock().unwrap().as_slice(), &["task-1".to_owned()]);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_service_registers_and_heartbeats_over_network() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping networked gRPC test because loopback sockets are denied");
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

    let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
        format!("http://{addr}"),
    )
    .await
    .unwrap();
    let executor_id = ExecutorId::try_new("exec-network-1").unwrap();
    let registration = client
        .register_executor(wire::register_executor_request_to_wire(
            RegisterExecutorRequest::new(ExecutorDescriptor::new(
                executor_id.clone(),
                "pod-network",
                2,
            )),
        ))
        .await
        .unwrap()
        .into_inner();
    let registration = wire::register_executor_response_from_wire(registration).unwrap();

    assert_eq!(registration.disposition(), TransportDisposition::Accepted);
    assert_eq!(registration.executor_id(), &executor_id);

    let heartbeat = client
        .executor_heartbeat(wire::executor_heartbeat_request_to_wire(
            ExecutorHeartbeatRequest::new(
                executor_id.clone(),
                LeaseGeneration::initial(),
                ExecutorState::Healthy,
            ),
        ))
        .await
        .unwrap()
        .into_inner();
    let heartbeat = wire::executor_heartbeat_response_from_wire(heartbeat).unwrap();

    assert_eq!(heartbeat.disposition(), TransportDisposition::Accepted);
    {
        let coordinator = shared.read().await;
        assert_eq!(coordinator.executor_snapshots().len(), 1);
        assert_eq!(
            coordinator.executor_snapshots()[0].state(),
            ExecutorState::Healthy
        );
    }

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();
    {
        let mut coordinator = shared.write().await;
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
    }

    let task_status = client
        .task_status(wire::task_status_request_to_wire(TaskStatusRequest::new(
            TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )))
        .await
        .unwrap()
        .into_inner();
    let task_status = wire::task_status_response_from_wire(task_status).unwrap();

    assert_eq!(task_status.disposition(), TransportDisposition::Accepted);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_deregister_transitions_executor_to_removed() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-deregister").unwrap(),
    ));
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping networked gRPC test because loopback sockets are denied");
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

    let mut client = wire::v1::coordinator_executor_client::CoordinatorExecutorClient::connect(
        format!("http://{addr}"),
    )
    .await
    .unwrap();

    let executor_id = ExecutorId::try_new("exec-dereg-1").unwrap();
    let register_resp = client
        .register_executor(wire::register_executor_request_to_wire(
            RegisterExecutorRequest::new(ExecutorDescriptor::new(
                executor_id.clone(),
                "pod-dereg",
                1,
            )),
        ))
        .await
        .unwrap()
        .into_inner();
    let register_resp = wire::register_executor_response_from_wire(register_resp).unwrap();
    assert_eq!(register_resp.disposition(), TransportDisposition::Accepted);

    let lease_generation = {
        let coordinator = shared.read().await;
        coordinator
            .executor_snapshots()
            .into_iter()
            .find(|s| s.executor_id() == &executor_id)
            .expect("executor should be registered")
            .lease_generation()
    };

    let dereg_resp = client
        .deregister_executor(wire::deregister_executor_request_to_wire(
            DeregisterExecutorRequest::new(executor_id.clone(), lease_generation),
        ))
        .await
        .unwrap()
        .into_inner();
    let dereg_resp = wire::deregister_executor_response_from_wire(dereg_resp).unwrap();
    assert_eq!(dereg_resp.disposition(), TransportDisposition::Accepted);

    {
        let coordinator = shared.read().await;
        let snapshot = coordinator
            .executor_snapshots()
            .into_iter()
            .find(|s| s.executor_id() == &executor_id)
            .expect("executor should still be in registry after deregister");
        assert_eq!(snapshot.state(), ExecutorState::Removed);
    }

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_routes_task_status_updates() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
    }

    let status = TaskStatusRequest::new(
        TaskAttemptRef::new(job_id.clone(), stage_id, task_id, AttemptId::initial()),
        executor_id,
        LeaseGeneration::initial(),
        TaskState::Succeeded,
    );
    let response = service
        .task_status(tonic::Request::new(status))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::Accepted);
    assert_eq!(
        shared.read().await.job_snapshot(&job_id).unwrap().state(),
        JobState::Running
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_reports_duplicate_task_status_as_domain_response() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("coord-1").unwrap(),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();
    let ids = TaskAttemptRef::new(
        job_id.clone(),
        stage_id.clone(),
        task_id.clone(),
        AttemptId::initial(),
    );

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
    }

    let accepted = service
        .task_status(tonic::Request::new(TaskStatusRequest::new(
            ids.clone(),
            executor_id.clone(),
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )))
        .await
        .unwrap()
        .into_inner();
    let duplicate = service
        .task_status(tonic::Request::new(TaskStatusRequest::new(
            ids,
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(accepted.disposition(), TransportDisposition::Accepted);
    assert_eq!(duplicate.disposition(), TransportDisposition::Duplicate);
}

#[tokio::test]
async fn in_process_bridge_reports_duplicate_task_status() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();
    let ids = TaskAttemptRef::new(job_id.clone(), stage_id, task_id, AttemptId::initial());

    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();
    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    let bridge = in_process_bridge_for(coordinator);

    let accepted = bridge
        .task_status(tonic::Request::new(TaskStatusRequest::new(
            ids.clone(),
            executor_id.clone(),
            LeaseGeneration::initial(),
            TaskState::Running,
        )))
        .await
        .unwrap()
        .into_inner();
    let duplicate = bridge
        .task_status(tonic::Request::new(TaskStatusRequest::new(
            ids,
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Running,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(accepted.disposition(), TransportDisposition::Accepted);
    assert_eq!(duplicate.disposition(), TransportDisposition::Duplicate);
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_service_reports_stale_task_attempt_as_domain_response() {
    allow_anonymous_for_tests();
    let shared = SharedCoordinator::new(Coordinator::active_with_config(
        CoordinatorId::try_new("coord-1").unwrap(),
        CoordinatorConfig::new(1, 3),
    ));
    let service = CoordinatorExecutorTonicService::new(shared.clone());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    {
        let mut coordinator = shared.write().await;
        coordinator
            .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
            .unwrap();
        coordinator.submit_job(job).unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
        coordinator
            .apply_task_update(TaskStatusUpdate::new(
                job_id.clone(),
                stage_id.clone(),
                task_id.clone(),
                executor_id.clone(),
                TaskState::Failed,
                1,
            ))
            .unwrap();
        coordinator.launch_assigned_tasks(&job_id).unwrap();
    }

    let response = service
        .task_status(tonic::Request::new(TaskStatusRequest::new(
            TaskAttemptRef::new(job_id, stage_id, task_id, AttemptId::initial()),
            executor_id,
            LeaseGeneration::initial(),
            TaskState::Succeeded,
        )))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.disposition(), TransportDisposition::StaleAttempt);
}

#[test]
fn coordinator_rejects_task_status_with_stale_executor_lease() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();
    let stale_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    coordinator.mark_executor_lost(&executor_id).unwrap();

    let error = coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id,
                stage_id,
                task_id,
                executor_id,
                TaskState::Succeeded,
                1,
            )
            .with_lease_generation(stale_generation),
        )
        .unwrap_err();

    assert!(matches!(error, SchedulerError::StaleExecutorLease { .. }));
}

#[test]
fn duplicate_terminal_task_status_is_idempotent() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();
    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    let update = TaskStatusUpdate::new(
        job_id.clone(),
        stage_id,
        task_id,
        executor_id,
        TaskState::Succeeded,
        1,
    );
    assert_eq!(
        coordinator.apply_task_update(update.clone()).unwrap(),
        TaskUpdateOutcome::Applied
    );
    assert_eq!(
        coordinator.apply_task_update(update).unwrap(),
        TaskUpdateOutcome::Duplicate
    );
    assert_eq!(
        coordinator
            .job_snapshot(&job_id)
            .unwrap()
            .succeeded_task_count(),
        1
    );
}

#[test]
fn duplicate_failed_task_status_does_not_replay_circuit_breaker_side_effects() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let config = CoordinatorConfig::new(0, 2);
    let failure_threshold = config.circuit_breaker_failure_threshold();
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-1").unwrap(), config);
    let job = single_task_job(JobId::try_new("job-duplicate-failure").unwrap());
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();
    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    let update =
        TaskStatusUpdate::new(job_id, stage_id, task_id, executor_id, TaskState::Failed, 1);
    assert_eq!(
        coordinator.apply_task_update(update.clone()).unwrap(),
        TaskUpdateOutcome::Applied
    );
    for _ in 1..failure_threshold {
        assert_eq!(
            coordinator.apply_task_update(update.clone()).unwrap(),
            TaskUpdateOutcome::Duplicate
        );
    }

    assert!(
        coordinator
            .exec
            .executors
            .executors_over_failure_threshold(failure_threshold)
            .is_empty(),
        "duplicate failed task reports must not advance executor circuit-breaker counters"
    );
}

#[test]
fn coordinator_launch_returns_executor_task_assignments_with_attempt_and_lease() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let lease_generation = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();
    let job = demo_job();
    let job_id = job.job_id().clone();

    coordinator.submit_job(job).unwrap();
    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();

    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].job_id(), &job_id);
    assert_eq!(assignments[0].executor_id(), &executor_id);
    assert_eq!(assignments[0].attempt_id(), AttemptId::initial());
    assert_eq!(assignments[0].lease_generation(), lease_generation);
    assert_eq!(
        assignments[0].output_contract().kind(),
        krishiv_proto::OutputContractKind::InlineRecordBatches
    );
    assert!(!assignments[0].input_partitions().is_empty());
    assert!(
        coordinator
            .job_snapshot(&job_id)
            .unwrap()
            .assigned_task_count()
            > 0
    );
}

#[test]
fn static_scheduler_places_tasks_round_robin() {
    let job = demo_job();
    let exec_a = ExecutorDescriptor::new(ExecutorId::try_new("exec-a").unwrap(), "pod-a", 1);
    let exec_b = ExecutorDescriptor::new(ExecutorId::try_new("exec-b").unwrap(), "pod-b", 1);
    let executors = vec![&exec_a, &exec_b];

    let assignments = StaticScheduler::place(&job, &executors).unwrap();

    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].executor_id().as_str(), "exec-a");
    assert_eq!(assignments[1].executor_id().as_str(), "exec-b");
}

#[test]
fn converts_batch_logical_plan_into_distributed_job_spec() {
    let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
        .with_node(PlanNode::new(
            "scan",
            "scan parquet",
            PlanExecutionKind::Batch,
        ))
        .with_node(
            PlanNode::new("aggregate", "count", PlanExecutionKind::Batch).with_inputs(["scan"]),
        );

    let job = job_spec_from_logical_plan(JobId::try_new("job-batch").unwrap(), &plan).unwrap();

    assert_eq!(job.kind(), JobKind::Batch);
    assert_eq!(job.name(), "batch-dag");
    assert_eq!(job.task_count(), 2);
    assert!(job.stages()[0].tasks()[1].description().contains("scan"));
}

#[test]
fn coordinator_routes_batch_logical_plan_through_scheduler() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ))
        .unwrap();

    let plan = LogicalPlan::new("batch-dag", PlanExecutionKind::Batch)
        .with_node(PlanNode::new(
            "scan",
            "scan parquet",
            PlanExecutionKind::Batch,
        ))
        .with_node(
            PlanNode::new("project", "project columns", PlanExecutionKind::Batch)
                .with_inputs(["scan"]),
        );
    let job_id = JobId::try_new("job-batch").unwrap();

    coordinator
        .submit_logical_plan(job_id.clone(), &plan)
        .unwrap();
    let snapshot = coordinator.job_snapshot(&job_id).unwrap();

    assert_eq!(snapshot.kind(), JobKind::Batch);
    assert_eq!(snapshot.task_count(), 2);
    assert_eq!(snapshot.assigned_task_count(), 2);
    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
    assert_eq!(
        coordinator
            .job_snapshot(&job_id)
            .unwrap()
            .assigned_task_count(),
        2
    );
}

#[test]
fn coordinator_routes_streaming_physical_plan_with_local_state_semantics() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            1,
        ))
        .unwrap();

    let plan =
        PhysicalPlan::new("stream-dag", PlanExecutionKind::Streaming).with_node(PlanNode::new(
            "memory-source",
            "local memory stream",
            PlanExecutionKind::Streaming,
        ));
    let job_id = JobId::try_new("job-stream").unwrap();

    coordinator
        .submit_physical_plan(job_id.clone(), &plan)
        .unwrap();
    let snapshot = coordinator.job_snapshot(&job_id).unwrap();

    assert_eq!(snapshot.kind(), JobKind::Streaming);
    assert_eq!(snapshot.task_count(), 1);
    assert_eq!(snapshot.assigned_task_count(), 1);
}

#[test]
fn empty_plan_routes_as_single_distributed_task() {
    let plan = PhysicalPlan::new("empty-physical", PlanExecutionKind::Batch);

    let job = job_spec_from_physical_plan(JobId::try_new("job-empty").unwrap(), &plan).unwrap();

    assert_eq!(job.kind(), JobKind::Batch);
    assert_eq!(job.task_count(), 1);
    assert!(
        job.stages()[0].tasks()[0]
            .description()
            .contains("empty-physical")
    );
}

#[test]
fn coordinator_submits_launches_and_completes_job() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let first_task = job.stages()[0].tasks()[0].task_id().clone();
    let second_task = job.stages()[0].tasks()[1].task_id().clone();

    coordinator.submit_job(job).unwrap();
    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.assigned_task_count(), 2);

    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.assigned_task_count(), 2);

    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id.clone(),
            first_task,
            executor_id.clone(),
            TaskState::Succeeded,
            1,
        ))
        .unwrap();
    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id,
            second_task,
            executor_id,
            TaskState::Succeeded,
            1,
        ))
        .unwrap();

    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.state(), JobState::Succeeded);
    assert_eq!(snapshot.succeeded_task_count(), 2);

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(detail.stages().len(), 1);
    assert_eq!(detail.stages()[0].tasks().len(), 2);
    assert_eq!(coordinator.job_snapshots().len(), 1);
}

#[test]
fn task_failure_marks_stage_and_job_failed() {
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-1").unwrap(),
        CoordinatorConfig::new(0, 3),
    );
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                task_id,
                executor_id,
                TaskState::Failed,
                1,
            )
            .with_message("executor reported failure"),
        )
        .unwrap();

    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.state(), JobState::Failed);
    assert_eq!(snapshot.failed_task_count(), 1);
}

#[test]
fn task_failure_retries_entire_stage_before_terminal_failure() {
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-1").unwrap(),
        CoordinatorConfig::new(1, 3),
    );
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let first_task = job.stages()[0].tasks()[0].task_id().clone();
    let second_task = job.stages()[0].tasks()[1].task_id().clone();

    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();
    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id.clone(),
            first_task.clone(),
            executor_id.clone(),
            TaskState::Failed,
            1,
        ))
        .unwrap();

    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.state(), JobState::Running);
    assert_eq!(snapshot.failed_task_count(), 0);

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(detail.stages()[0].retry_count(), 1);
    // P1.24 + Phase 53: the stage retry resets tasks to Pending and the
    // capacity-freed backlog drain re-places them through the normal
    // placement path in the same apply — so tasks are Pending or already
    // re-Assigned, never stuck on the failed attempt.
    for task in detail.stages()[0].tasks() {
        assert!(
            matches!(task.state(), TaskState::Pending | TaskState::Assigned),
            "retried task {} must be re-queued, got {:?}",
            task.task_id(),
            task.state()
        );
    }

    // Re-assign whatever the eager drain did not already place, then launch.
    let already_assigned = detail.stages()[0]
        .tasks()
        .iter()
        .filter(|t| t.state() == TaskState::Assigned)
        .count();
    let newly_assigned = coordinator.assign_pending_tasks(&job_id).unwrap();
    assert_eq!(already_assigned + newly_assigned, 2);
    assert_eq!(coordinator.launch_assigned_tasks(&job_id).unwrap(), 2);
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(detail.stages()[0].tasks()[0].attempt(), 2);
    assert_eq!(detail.stages()[0].tasks()[1].attempt(), 2);

    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id.clone(),
            first_task,
            executor_id.clone(),
            TaskState::Succeeded,
            2,
        ))
        .unwrap();
    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id,
            second_task,
            executor_id,
            TaskState::Succeeded,
            2,
        ))
        .unwrap();

    let snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snapshot.state(), JobState::Succeeded);
    assert_eq!(snapshot.succeeded_task_count(), 2);
}
