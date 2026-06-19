// ── Items 3+4: QueueManager trait + SubmitOutcome ────────────────────────

#[test]
fn in_memory_queue_manager_always_accepts() {
    let qm = InMemoryQueueManager;
    let spec = demo_job();
    let quota = NamespaceQuotaSnapshot::default();
    assert_eq!(qm.admit(&spec, &quota), SubmitOutcome::Accepted);
}

#[test]
fn namespace_quota_snapshot_sums_active_jobs() {
    let coordinator_id = CoordinatorId::try_new("coord-quota").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);
    let executor_id = ExecutorId::try_new("exec-quota").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 4))
        .unwrap();

    let job_id_a = JobId::try_new("quota-a").unwrap();
    let job_id_b = JobId::try_new("quota-b").unwrap();

    let spec_a = single_task_job(job_id_a.clone())
        .with_namespace("team-a")
        .with_cpu_limit_nanos(500_000_000)
        .with_memory_limit_bytes(256 * 1024 * 1024);

    let spec_b = single_task_job(job_id_b.clone())
        .with_namespace("team-a")
        .with_cpu_limit_nanos(300_000_000)
        .with_memory_limit_bytes(128 * 1024 * 1024);

    coordinator.submit_job(spec_a).unwrap();
    coordinator.submit_job(spec_b).unwrap();

    let snap = coordinator.namespace_quota_snapshot(Some("team-a"));
    assert_eq!(snap.active_job_count, 2);
    assert_eq!(snap.cpu_nanos_reserved, 800_000_000);
    assert_eq!(snap.memory_bytes_reserved, (256 + 128) * 1024 * 1024);

    let snap_other = coordinator.namespace_quota_snapshot(Some("team-b"));
    assert_eq!(snap_other.active_job_count, 0);
}

#[test]
fn resource_usage_accumulates_from_task_stats() {
    let coordinator_id = CoordinatorId::try_new("coord-ru").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);
    let executor_id = ExecutorId::try_new("exec-ru").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 2))
        .unwrap();
    coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(
            executor_id.clone(),
            ExecutorState::Healthy,
        ))
        .unwrap();

    let job_id = JobId::try_new("ru-job").unwrap();
    let stage_id = StageId::try_new("stage-1").unwrap();
    let task_id = TaskId::try_new("task-1").unwrap();

    use krishiv_proto::{TaskRuntimeStats, TaskStatusUpdate};

    let spec = JobSpec::new(job_id.clone(), "ru", JobKind::Batch).with_stage(
        StageSpec::new(stage_id.clone(), "s").with_task(TaskSpec::new(task_id.clone(), "t")),
    );
    coordinator.submit_job(spec).unwrap();
    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let assignment = assignments.first().unwrap();

    let mut meta = TaskOutputMetadata::new("inline", 10, 1, 5);
    meta = meta.with_runtime_stats(TaskRuntimeStats {
        input_rows: 0,
        output_rows: 10,
        cpu_nanos: 1_000_000,
        memory_bytes: 0,
        spill_bytes: 0,
        serialized_bytes: 0,
    });

    let update = TaskStatusUpdate::new(
        assignment.job_id().clone(),
        assignment.stage_id().clone(),
        assignment.task_id().clone(),
        executor_id,
        TaskState::Succeeded,
        assignment.attempt_id().as_u32(),
    )
    .with_lease_generation(assignment.lease_generation())
    .with_output_metadata(meta);

    coordinator.apply_task_update(update).unwrap();

    let snap = coordinator.job_snapshot(&job_id).unwrap();
    assert_eq!(snap.resource_usage().cpu_nanos, 1_000_000);
    assert_eq!(snap.resource_usage().task_count, 1);
}

#[test]
fn job_spec_priority_and_namespace_round_trip() {
    let job_id = JobId::try_new("prio-job").unwrap();
    let spec = JobSpec::new(job_id, "test", JobKind::Batch)
        .with_priority(200)
        .with_namespace("eng")
        .with_cpu_limit_nanos(1_000_000)
        .with_memory_limit_bytes(1024);

    assert_eq!(spec.priority(), 200);
    assert_eq!(spec.namespace_id(), Some("eng"));
    assert_eq!(spec.cpu_limit_nanos(), Some(1_000_000));
    assert_eq!(spec.memory_limit_bytes(), Some(1024));
}

#[test]
fn coordinator_uses_queue_manager_on_submit() {
    let coordinator_id = CoordinatorId::try_new("coord-qm").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-qm").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
        .unwrap();

    let outcome = coordinator.submit_job(demo_job()).unwrap();
    assert_eq!(outcome, SubmitOutcome::Accepted);
}

#[test]
fn coordinator_with_blocking_queue_manager_returns_queued() {
    #[derive(Debug)]
    struct BlockAllQueueManager;
    impl QueueManager for BlockAllQueueManager {
        fn admit(&self, _spec: &JobSpec, _quota: &NamespaceQuotaSnapshot) -> SubmitOutcome {
            SubmitOutcome::Queued { position: 0 }
        }
    }

    let coordinator_id = CoordinatorId::try_new("coord-block").unwrap();
    let mut coordinator =
        Coordinator::active(coordinator_id).with_queue_manager(BlockAllQueueManager);

    let executor_id = ExecutorId::try_new("exec-block").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "host-1", 2))
        .unwrap();

    // Job is queued, not accepted — coordinator has no JobRecord yet.
    let outcome = coordinator.submit_job(demo_job()).unwrap();
    assert_eq!(outcome, SubmitOutcome::Queued { position: 0 });
    assert!(
        coordinator
            .job_snapshot(&demo_job().job_id().clone())
            .is_err(),
        "queued job must not appear in job list"
    );
}

