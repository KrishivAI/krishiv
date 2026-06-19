// ── R6a: Out-of-band barrier trigger ──────────────────────────────────────

#[test]
fn trigger_checkpoint_for_job_returns_initiate_request() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap().to_owned();
    let coordinator_id = CoordinatorId::try_new("coord-r6a").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-r6a").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "host", 2))
        .unwrap();

    let job_id = JobId::try_new("job-r6a").unwrap();
    let stage_id = StageId::try_new("s-r6a").unwrap();
    let task_id = TaskId::try_new("t-r6a").unwrap();

    let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming)
        .with_stage(StageSpec::new(stage_id, "stage").with_task(TaskSpec::new(task_id, "task")))
        .with_checkpoint(1_000, storage_path);
    coordinator.submit_job(spec).unwrap();

    // trigger_checkpoint_for_job initiates epoch 1 and returns the request.
    let requests = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
    assert_eq!(requests.len(), 1, "one broadcast request");
    assert_eq!(requests[0].epoch, 1, "first epoch");
    assert_eq!(requests[0].job_id, job_id);

    // A second trigger while epoch 1 is in flight must fail.
    assert!(
        coordinator.trigger_checkpoint_for_job(&job_id).is_err(),
        "cannot trigger while acks are pending"
    );
}

#[test]
fn trigger_checkpoint_then_ack_commits_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap().to_owned();
    let coordinator_id = CoordinatorId::try_new("coord-r6b").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-r6b").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host", 2))
        .unwrap();

    let job_id = JobId::try_new("job-r6b").unwrap();
    let stage_id = StageId::try_new("s-r6b").unwrap();
    let task_id = TaskId::try_new("t-r6b-1").unwrap();

    let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming)
        .with_stage(
            StageSpec::new(stage_id, "stage").with_task(TaskSpec::new(task_id.clone(), "task")),
        )
        .with_checkpoint(1_000, storage_path);
    coordinator.submit_job(spec).unwrap();

    // Trigger checkpoint — epoch 1.
    let requests = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
    let req = &requests[0];
    let epoch = req.epoch;
    let fencing_token = req.fencing_token;

    // Simulate executor acking the checkpoint.
    let ack = CheckpointAckRequest {
        job_id: job_id.clone(),
        operator_id: krishiv_proto::OperatorId::try_new(format!(
            "operator-{}",
            task_id.as_str()
        ))
        .unwrap(),
        task_id: task_id.clone(),
        epoch,
        fencing_token,
        source_offsets: vec![],
        snapshot_path: None,
    };

    let response = coordinator.handle_checkpoint_ack(ack);
    assert_eq!(
        response,
        CheckpointAckResponse::Accepted,
        "ack must be accepted"
    );

    // After all tasks ack, coordinator should commit epoch 1.
    let coord = coordinator.checkpoint_coordinator(&job_id).unwrap();
    assert_eq!(coord.current_epoch(), 1);
    assert!(
        !coord.is_awaiting_acks(),
        "epoch 1 should be committed after all acks received"
    );
}

#[test]
fn trigger_checkpoint_fails_without_checkpoint_config() {
    let coordinator_id = CoordinatorId::try_new("coord-r6c").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-r6c").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id, "host", 2))
        .unwrap();

    let job_id = JobId::try_new("job-r6c").unwrap();
    let spec = JobSpec::new(job_id.clone(), "stream", JobKind::Streaming).with_stage(
        StageSpec::new(StageId::try_new("s-r6c").unwrap(), "stage")
            .with_task(TaskSpec::new(TaskId::try_new("t-r6c").unwrap(), "task")),
    );
    coordinator.submit_job(spec).unwrap();

    // No checkpoint_interval_ms set — must fail.
    assert!(
        coordinator.trigger_checkpoint_for_job(&job_id).is_err(),
        "trigger must fail when job has no checkpoint coordinator"
    );
}

#[test]
fn checkpoint_epoch_overflow_returns_error() {
    let job_id = JobId::try_new("job-epoch-overflow").unwrap();
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 1_000, 1);
    // Manually push current_epoch to u64::MAX so the next checked_add overflows.
    coord.current_epoch = u64::MAX;
    let result = coord.initiate();
    assert!(
        result.is_err(),
        "initiating past u64::MAX must return Err, got {result:?}"
    );
    assert!(
        result.unwrap_err().contains("overflow"),
        "error must mention overflow"
    );
}

