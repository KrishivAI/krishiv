// ── Item 2: checkpoint timer wired into advance_heartbeat_clock ──────────

#[test]
fn checkpoint_coordinator_try_tick_fires_after_interval() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-tick").unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 5_000, 0);

    // Accumulate 4 000 ms — below the 5 000 ms interval.
    assert_eq!(coord.try_tick(4_000, 60_000), None, "not yet due");
    assert_eq!(
        coord.try_tick(2_000, 60_000),
        None,
        "zero running tasks skips initiate"
    );
    coord.set_expected_task_count(1);
    assert_eq!(coord.try_tick(5_000, 60_000), Some(1), "epoch 1 initiated");
    // Epoch 1 is now in AwaitingAcks. Abort it to return to Idle.
    coord.abort_epoch("test reset");
    // Clock resets on initiate: another 5 000 ms triggers epoch 2.
    assert_eq!(coord.try_tick(5_000, 60_000), Some(2), "epoch 2 initiated");
}

#[test]
fn checkpoint_coordinator_try_tick_skips_while_awaiting_acks() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-tick-busy").unwrap();
    // expected_task_count = 1 so the coordinator will wait for an ack.
    let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 1_000, 1);

    // First tick crosses the interval — epoch 1 initiated (now AwaitingAcks).
    assert_eq!(coord.try_tick(1_000, 60_000), Some(1));
    // While awaiting acks, further ticks must not initiate.
    assert_eq!(
        coord.try_tick(10_000, 60_000),
        None,
        "in-flight checkpoint blocks next"
    );
}

#[test]
fn checkpoint_coordinator_aborts_stuck_epoch_after_timeout() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-tick-timeout").unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 1_000, 1);

    assert_eq!(coord.try_tick(1_000, 5_000), Some(1));
    assert!(coord.is_awaiting_acks());

    assert_eq!(coord.try_tick(4_000, 5_000), None);
    assert!(coord.is_awaiting_acks());

    assert_eq!(coord.try_tick(1_000, 5_000), None);
    assert!(
        matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::Failed { epoch: 1, .. }
        ),
        "stuck epoch must transition to Failed after the timeout elapses"
    );

    assert_eq!(
        coord.try_tick(1_000, 5_000),
        Some(2),
        "timeout must unblock the next checkpoint epoch"
    );
}

#[test]
fn advance_heartbeat_clock_drives_checkpoint_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap().to_owned();
    let job_id = JobId::try_new("job-clock").unwrap();

    // heartbeat_timeout_ticks is large enough that advance_heartbeat_clock(2)
    // does not mark the executor Lost (which would reset the Running task
    // back to Assigned and zero out the checkpoint quorum, D3).
    let config = CoordinatorConfig::new(1, 100).with_tick_period_ms(1_000);
    let coordinator_id = CoordinatorId::try_new("coord-clock").unwrap();
    let mut coordinator = Coordinator::active_with_config(coordinator_id, config);

    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 2))
        .unwrap();

    // Submit a streaming job with a 3-second checkpoint interval.
    let task_id = TaskId::try_new("t1").unwrap();
    let stage_id = StageId::try_new("s1").unwrap();
    let stage = StageSpec::new(stage_id.clone(), "stage-1")
        .with_task(TaskSpec::new(task_id.clone(), "task-1"));
    let spec = JobSpec::new(job_id.clone(), "clock-test", JobKind::Streaming)
        .with_stage(stage)
        .with_checkpoint(3_000, storage_path);
    coordinator.submit_job(spec).unwrap();

    // D3: checkpoint epochs require Running tasks (not just Assigned),
    // so transition the task to Running before ticking.
    let lease = coordinator
        .executors
        .find_executor(&executor_id)
        .unwrap()
        .lease_generation();
    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let attempt = assignments
        .first()
        .map(|a| a.attempt_id().as_u32())
        .unwrap_or(1);
    let update = TaskStatusUpdate::new(
        job_id.clone(),
        stage_id,
        task_id,
        executor_id,
        TaskState::Running,
        attempt,
    )
    .with_lease_generation(lease);
    coordinator.apply_task_update(update).unwrap();

    // Sanity: one Running task means the checkpoint quorum should be 1.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let running_count = detail
        .stages()
        .iter()
        .flat_map(|s| s.tasks().iter())
        .filter(|t| matches!(t.state(), TaskState::Running))
        .count();
    assert_eq!(
        running_count, 1,
        "task should be Running after status update"
    );

    // 2 ticks × 1 000 ms = 2 000 ms < 3 000 ms — no checkpoint yet.
    coordinator.advance_heartbeat_clock(2).unwrap();
    assert_eq!(
        coordinator
            .checkpoint_coordinator(&job_id)
            .unwrap()
            .current_epoch(),
        0,
        "epoch 0 — not yet due"
    );

    // 2 more ticks: 4 000 ms total >= 3 000 ms — epoch 1 fires.
    coordinator.advance_heartbeat_clock(2).unwrap();
    assert_eq!(
        coordinator
            .checkpoint_coordinator(&job_id)
            .unwrap()
            .current_epoch(),
        1,
        "epoch 1 initiated after 4 ticks × 1 000 ms"
    );
}

