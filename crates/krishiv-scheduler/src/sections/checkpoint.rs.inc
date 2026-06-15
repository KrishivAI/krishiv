// ── CheckpointCoordinator tests ───────────────────────────────────────────

fn make_ack(
    job_id: &JobId,
    task_id: &str,
    epoch: u64,
    fencing_token: FencingToken,
    snapshot_path: Option<String>,
) -> CheckpointAckRequest {
    CheckpointAckRequest {
        job_id: job_id.clone(),
        operator_id: krishiv_proto::OperatorId::try_new(format!("operator-{task_id}")).unwrap(),
        task_id: TaskId::try_new(task_id).unwrap(),
        epoch,
        fencing_token,
        source_offsets: vec![krishiv_proto::CheckpointSourceOffset {
            partition_id: krishiv_proto::PartitionId::try_new(format!("partition-{task_id}"))
                .unwrap(),
            offset: 100,
        }],
        snapshot_path,
    }
}

#[test]
fn checkpoint_coordinator_initiates_and_collects_acks() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-ck-1").unwrap();
    let mut coord =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 2);

    // Write state snapshots so the manifest can hash them.
    krishiv_state::checkpoint::write_operator_snapshot(
        storage.as_ref(),
        "job-ck-1",
        1,
        "operator-task-1",
        "task-1",
        b"state bytes",
    )
    .unwrap();
    krishiv_state::checkpoint::write_operator_snapshot(
        storage.as_ref(),
        "job-ck-1",
        1,
        "operator-task-2",
        "task-2",
        b"state bytes 2",
    )
    .unwrap();

    let epoch = coord.initiate().unwrap();
    assert_eq!(epoch, 1);
    assert!(coord.is_awaiting_acks());

    let snap_path1 =
        krishiv_state::checkpoint::snapshot_path("job-ck-1", 1, "operator-task-1", "task-1");
    let snap_path2 =
        krishiv_state::checkpoint::snapshot_path("job-ck-1", 1, "operator-task-2", "task-2");
    let ack1 = make_ack(
        &job_id,
        "task-1",
        1,
        FencingToken::initial(),
        Some(snap_path1),
    );
    let ack2 = make_ack(
        &job_id,
        "task-2",
        1,
        FencingToken::initial(),
        Some(snap_path2),
    );

    // First ack: not yet quorum.
    let done = coord.receive_ack(ack1).unwrap();
    assert!(!done);
    assert!(coord.is_awaiting_acks());

    // Second ack: quorum complete, epoch committed.
    let done = coord.receive_ack(ack2).unwrap();
    assert!(done);
    assert!(!coord.is_awaiting_acks());
    assert_eq!(coord.current_epoch(), 1);

    // Verify metadata was written to storage.
    let meta = krishiv_state::checkpoint::read_epoch_metadata(storage.as_ref(), "job-ck-1", 1)
        .unwrap()
        .unwrap();
    assert_eq!(meta.epoch, 1);
    assert_eq!(meta.job_id, "job-ck-1");
    assert!(!meta.is_savepoint);

    // Verify manifest exists and epoch validates.
    assert!(
        krishiv_state::checkpoint::validate_epoch(storage.as_ref(), "job-ck-1", 1).unwrap()
    );
}

#[test]
fn checkpoint_coordinator_rejects_stale_epoch_ack() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-ck-stale").unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(job_id.clone(), storage, 5000, 1);
    let _ = coord.initiate().unwrap(); // epoch = 1

    // Send ack with wrong epoch.
    let ack = make_ack(&job_id, "task-1", 99, FencingToken::initial(), None);
    let result = coord.receive_ack(ack);
    assert!(result.is_err(), "stale epoch ack must be rejected");
}

#[test]
fn checkpoint_coordinator_abort_resets_state() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-ck-abort").unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(job_id.clone(), storage, 5000, 2);
    let _ = coord.initiate().unwrap();
    assert!(coord.is_awaiting_acks());

    coord.abort_epoch("timeout");
    assert!(!coord.is_awaiting_acks());
    assert!(matches!(
        coord.coordinator_state(),
        CheckpointCoordinatorState::Failed { epoch: 1, .. }
    ));

    // Can initiate again after abort.
    let _ = coord.initiate().unwrap();
    assert!(coord.is_awaiting_acks());
}

#[test]
fn checkpoint_coordinator_recover_finds_latest_epoch() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-ck-recover").unwrap();

    // Write two complete epochs manually.
    for epoch in [1u64, 2] {
        let meta = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: "job-ck-recover".to_owned(),
            fencing_token: 1,
            coordinator_id: None,
            timestamp_ms: epoch * 5000,
            source_offsets: vec![],
            operator_snapshots: vec![],
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };
        write_epoch_metadata(storage.as_ref(), "job-ck-recover", epoch, &meta).unwrap();
        let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &meta_json);
        write_manifest(storage.as_ref(), "job-ck-recover", epoch, &manifest).unwrap();
    }

    let mut coord = CheckpointCoordinator::new_for_test(job_id, storage, 5000, 1);
    let recovered = coord.recover_from_storage().unwrap();
    assert_eq!(recovered, Some(2));
    assert_eq!(coord.current_epoch(), 2);
}

#[test]
fn checkpoint_coordinator_savepoint_sets_flag() {
    let storage: Arc<dyn CheckpointStorage> =
        Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-ck-sp").unwrap();
    let mut coord =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 1);

    let epoch = coord
        .initiate_savepoint(Some("my-savepoint".to_owned()))
        .unwrap();
    assert_eq!(epoch, 1);

    let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
    let done = coord.receive_ack(ack).unwrap();
    assert!(done);

    let meta = krishiv_state::checkpoint::read_epoch_metadata(storage.as_ref(), "job-ck-sp", 1)
        .unwrap()
        .unwrap();
    assert!(
        meta.is_savepoint,
        "is_savepoint must be true for savepoints"
    );
    assert_eq!(meta.savepoint_label.as_deref(), Some("my-savepoint"));
}

#[test]
fn coordinator_creates_checkpoint_coordinator_for_streaming_job_with_config() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-ck-test").unwrap());
    let executor_id = ExecutorId::try_new("exec-ck-test").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-ck", 1))
        .unwrap();

    let job_id = JobId::try_new("job-ck-stream").unwrap();
    let spec = JobSpec::new(job_id.clone(), "stream-ck", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
            ),
        );
    coordinator.submit_job(spec).unwrap();

    assert!(
        coordinator.checkpoint_coordinator(&job_id).is_some(),
        "streaming job with checkpoint config must have a CheckpointCoordinator"
    );
}

#[test]
fn coordinator_routes_ack_to_correct_job() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-ck-route").unwrap());
    let executor_id = ExecutorId::try_new("exec-ck-route").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-ck", 1))
        .unwrap();

    let job_id = JobId::try_new("job-ck-route").unwrap();
    let spec = JobSpec::new(job_id.clone(), "route-ck", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
            ),
        );
    coordinator.submit_job(spec).unwrap();

    // Initiate an epoch on the coordinator's checkpoint coordinator.
    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(1);
        coord.initiate().unwrap();
    }

    // Route an ack through the coordinator.
    let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
    let response = coordinator.handle_checkpoint_ack(ack);
    assert_eq!(
        response,
        CheckpointAckResponse::Accepted,
        "ack for valid epoch must be accepted"
    );

    // Unknown job → JobNotFound.
    let unknown_job_id = JobId::try_new("job-unknown").unwrap();
    let ack = make_ack(&unknown_job_id, "task-1", 1, FencingToken::initial(), None);
    let response = coordinator.handle_checkpoint_ack(ack);
    assert_eq!(response, CheckpointAckResponse::JobNotFound);
}

/// Regression (Wave 4 — Observability & Shutdown): the async checkpoint
/// ack path (`handle_checkpoint_ack_async`, used by the gRPC service) must
/// record `inc_checkpoint_committed` on quorum just like the synchronous
/// path — the metric call was previously present only in
/// `handle_checkpoint_ack`, leaving async-routed commits invisible in
/// `krishiv_checkpoint_epochs_total{status="committed"}`.
#[tokio::test]
async fn async_checkpoint_ack_quorum_increments_committed_metric() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-ck-async-metric").unwrap());
    let executor_id = ExecutorId::try_new("exec-ck-async-metric").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-ck", 1))
        .unwrap();

    let job_id = JobId::try_new("job-ck-async-metric").unwrap();
    let spec = JobSpec::new(job_id.clone(), "async-ck-metric", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
            ),
        );
    coordinator.submit_job(spec).unwrap();

    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(1);
        coord.initiate().unwrap();
    }

    let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
    let (response, _pending) = coordinator.handle_checkpoint_ack_async(ack).await;
    assert_eq!(
        response,
        CheckpointAckResponse::Accepted,
        "quorum ack must be accepted"
    );

    let rendered = krishiv_metrics::global_metrics().render_prometheus();
    assert!(
        rendered.contains(&format!(
            "krishiv_checkpoint_epochs_total{{job_id=\"{}\",status=\"committed\"}} 1",
            job_id.as_str()
        )),
        "async ack quorum must record inc_checkpoint_committed for the job, got: {rendered}"
    );
}

#[test]
fn checkpoint_coordinator_rejects_non_quorum_ack_as_stale_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-nq").unwrap());
    let executor_id = ExecutorId::try_new("exec-nq").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-nq", 1))
        .unwrap();

    let job_id = JobId::try_new("job-nq").unwrap();
    let spec = JobSpec::new(job_id.clone(), "non-quorum", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-1").unwrap(),
                    "stream:tw",
                ))
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-2").unwrap(),
                    "stream:tw",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    // Store the notify count before the ack.
    let notify_count_before = coordinator.checkpoint_notify_sent.len();

    // Initiate an epoch — expected_task_count = 2.
    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(2);
        coord.initiate().unwrap();
    }

    // Send one ack — NOT enough for quorum (needs 2).
    let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
    let response = coordinator.handle_checkpoint_ack(ack);
    assert_eq!(
        response,
        CheckpointAckResponse::StaleEpoch { current_epoch: 1 },
        "single ack with 2 expected tasks must return StaleEpoch, not Accepted"
    );

    // Notify entries must NOT have been cleared by a non-quorum ack.
    assert_eq!(
        coordinator.checkpoint_notify_sent.len(),
        notify_count_before,
        "non-quorum ack must not clear checkpoint_notify_sent"
    );
}

#[tokio::test]
async fn shared_coordinator_seeds_checkpoint_inner_from_existing_state() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-shared-ck-seed").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-shared-ck-seed").unwrap(),
            "pod-shared-ck-seed",
            1,
        ))
        .unwrap();

    let job_id = JobId::try_new("job-shared-ck-seed").unwrap();
    let spec = JobSpec::new(job_id.clone(), "shared-ck-seed", JobKind::Streaming)
        .with_checkpoint(5_000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-shared-ck-seed").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-shared-ck-seed").unwrap(),
                    "stream:shared-ck-seed",
                )),
        );
    coordinator.submit_job(spec).unwrap();
    assert!(coordinator.checkpoint_coordinator(&job_id).is_some());

    let shared = SharedCoordinator::new(coordinator);
    let checkpoint_inner = shared.checkpoint_inner.read().await;
    assert!(
        checkpoint_inner.coordinators.contains_key(&job_id),
        "SharedCoordinator must seed CheckpointInner from existing coordinator state"
    );
}

#[tokio::test]
async fn shared_submit_job_refreshes_checkpoint_inner() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-shared-submit").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-shared-submit").unwrap(),
            "pod-shared-submit",
            1,
        ))
        .unwrap();
    let shared = SharedCoordinator::new(coordinator);

    let job_id = JobId::try_new("job-shared-submit").unwrap();
    let spec = JobSpec::new(job_id.clone(), "shared-submit", JobKind::Streaming)
        .with_checkpoint(5_000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-shared-submit").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-shared-submit").unwrap(),
                    "stream:shared-submit",
                )),
        );

    let outcome = shared.submit_job(spec).await.unwrap();
    assert!(matches!(outcome, SubmitOutcome::Accepted));
    let checkpoint_inner = shared.checkpoint_inner.read().await;
    assert!(
        checkpoint_inner.coordinators.contains_key(&job_id),
        "SharedCoordinator::submit_job must keep CheckpointInner current"
    );
}

