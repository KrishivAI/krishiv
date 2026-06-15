// ── Group D: savepoint_job / list_job_checkpoints / restore ───────────────

#[test]
fn coordinator_savepoint_job_initiates_savepoint() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-sp").unwrap());
    let exec_id = ExecutorId::try_new("exec-sp").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-sp", 1))
        .unwrap();
    let job_id = JobId::try_new("job-sp").unwrap();
    let spec = JobSpec::new(job_id.clone(), "streaming-sp", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
            ),
        );
    coordinator.submit_job(spec).unwrap();

    let epoch = coordinator
        .savepoint_job(&job_id, Some("my-label".to_string()))
        .unwrap();
    assert_eq!(epoch, 1, "first savepoint must be epoch 1");

    // Batch job without checkpoint config → error.
    let batch_id = JobId::try_new("job-batch-sp").unwrap();
    let result = coordinator.savepoint_job(&batch_id, None);
    assert!(result.is_err(), "batch job has no checkpoint coordinator");
}

#[test]
fn coordinator_list_job_checkpoints_returns_empty_for_new_job() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-lc").unwrap());
    let exec_id = ExecutorId::try_new("exec-lc").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-lc", 1))
        .unwrap();
    let job_id = JobId::try_new("job-lc").unwrap();
    let spec = JobSpec::new(job_id.clone(), "streaming-lc", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1").with_task(
                TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream:tw"),
            ),
        );
    coordinator.submit_job(spec).unwrap();

    let epochs = coordinator.list_job_checkpoints(&job_id).unwrap();
    assert!(epochs.is_empty(), "no epochs committed yet");

    // Job without coordinator → empty vec (not an error).
    let unknown = JobId::try_new("job-unknown-lc").unwrap();
    let epochs = coordinator.list_job_checkpoints(&unknown).unwrap();
    assert!(epochs.is_empty());
}

#[test]
fn coordinator_restore_rejects_missing_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let coordinator = Coordinator::active(CoordinatorId::try_new("coord-restore").unwrap());
    let job_id = JobId::try_new("job-restore").unwrap();
    let result = coordinator.restore_job_from_checkpoint(&job_id, 99, &storage_path);
    assert!(
        result.is_err(),
        "epoch 99 does not exist; restore must fail"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("not found") || msg.contains("cannot read"),
        "error message must explain why: {msg}"
    );
}

fn write_scheduler_restore_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    fencing_token: u64,
) {
    let meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch,
        job_id: job_id.to_owned(),
        fencing_token,
        coordinator_id: Some("coord-restore-writer".to_owned()),
        timestamp_ms: 1_716_000_000_000 + epoch,
        source_offsets: Vec::new(),
        operator_snapshots: Vec::new(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    write_scheduler_restore_metadata(storage, job_id, epoch, &meta);
}

fn write_scheduler_restore_metadata(
    storage: &dyn CheckpointStorage,
    storage_job_id: &str,
    storage_epoch: u64,
    meta: &CheckpointMetadata,
) {
    use krishiv_state::checkpoint::{metadata_path, write_epoch_hint};

    storage
        .write_bytes(
            &metadata_path(storage_job_id, storage_epoch),
            &serde_json::to_vec_pretty(meta).unwrap(),
        )
        .unwrap();
    let stored_metadata = storage
        .read_bytes(&metadata_path(storage_job_id, storage_epoch))
        .unwrap()
        .unwrap();
    let mut manifest = IntegrityManifest::new();
    manifest.insert_bytes("metadata.json", &stored_metadata);
    write_manifest(storage, storage_job_id, storage_epoch, &manifest).unwrap();
    write_epoch_hint(storage, storage_job_id, storage_epoch).unwrap();
}

#[test]
fn coordinator_restore_rejects_hash_mismatched_epoch() {
    use krishiv_state::checkpoint::metadata_path;

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let coordinator = Coordinator::active(CoordinatorId::try_new("coord-restore-bad").unwrap());
    let job_id = JobId::try_new("job-restore-bad").unwrap();
    write_scheduler_restore_epoch(&storage, job_id.as_str(), 1, 1);
    let tampered_meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: job_id.as_str().to_owned(),
        fencing_token: 1,
        coordinator_id: Some("coord-restore-writer".to_owned()),
        timestamp_ms: 9_999,
        source_offsets: Vec::new(),
        operator_snapshots: Vec::new(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    storage
        .write_bytes(
            &metadata_path(job_id.as_str(), 1),
            &serde_json::to_vec_pretty(&tampered_meta).unwrap(),
        )
        .unwrap();

    let result = coordinator.restore_job_from_checkpoint(&job_id, 1, &storage_path);
    let error = result.expect_err("hash mismatch must reject restore");
    assert!(
        error.to_string().contains("failed integrity check"),
        "restore must report integrity failure, got: {error}"
    );
}

#[test]
fn coordinator_restore_rejects_metadata_identity_mismatch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let coordinator = Coordinator::active(CoordinatorId::try_new("coord-restore-id").unwrap());
    let job_id = JobId::try_new("job-restore-id").unwrap();
    let mismatched = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "other-job".to_owned(),
        fencing_token: 1,
        coordinator_id: Some("coord-restore-writer".to_owned()),
        timestamp_ms: 1_716_000_000_001,
        source_offsets: Vec::new(),
        operator_snapshots: Vec::new(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    write_scheduler_restore_metadata(&storage, job_id.as_str(), 1, &mismatched);

    let result = coordinator.restore_job_from_checkpoint(&job_id, 1, &storage_path);
    let error = result.expect_err("metadata job mismatch must reject restore");
    assert!(
        error.to_string().contains("metadata mismatch"),
        "expected metadata mismatch, got: {error}"
    );
}

#[test]
fn coordinator_restore_rejects_incompatible_metadata_version() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-restore-version").unwrap());
    let job_id = JobId::try_new("job-restore-version").unwrap();
    let incompatible = CheckpointMetadata {
        version: CheckpointMetadata::VERSION + 1,
        epoch: 1,
        job_id: job_id.as_str().to_owned(),
        fencing_token: 1,
        coordinator_id: Some("coord-restore-writer".to_owned()),
        timestamp_ms: 1_716_000_000_001,
        source_offsets: Vec::new(),
        operator_snapshots: Vec::new(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    write_scheduler_restore_metadata(&storage, job_id.as_str(), 1, &incompatible);

    let result = coordinator.restore_job_from_checkpoint(&job_id, 1, &storage_path);
    let error = result.expect_err("metadata version mismatch must reject restore");
    assert!(
        error.to_string().contains("incompatible"),
        "expected incompatible version error, got: {error}"
    );
}

#[test]
fn restore_activation_does_not_prune_when_metadata_identity_is_invalid() {
    use krishiv_state::checkpoint::{latest_valid_epoch, metadata_path};

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-restore-no-prune").unwrap());
    let job_id = JobId::try_new("job-restore-no-prune").unwrap();
    let spec = JobSpec::new(job_id.clone(), "streaming-restore", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-no-prune").unwrap(), "restore-stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-no-prune").unwrap(),
                    "stream:restore",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    let mismatched = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "other-job".to_owned(),
        fencing_token: 1,
        coordinator_id: Some("coord-restore-writer".to_owned()),
        timestamp_ms: 1_716_000_000_001,
        source_offsets: Vec::new(),
        operator_snapshots: Vec::new(),
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    write_scheduler_restore_metadata(&storage, job_id.as_str(), 1, &mismatched);
    write_scheduler_restore_epoch(&storage, job_id.as_str(), 2, 1);

    let result = coordinator.activate_job_restore_from_checkpoint_with_fencing(
        &job_id,
        1,
        &storage_path,
        Some(7),
    );
    let error = result.expect_err("invalid metadata must reject before activation");
    assert!(
        error.to_string().contains("metadata mismatch"),
        "expected metadata mismatch, got: {error}"
    );
    assert_eq!(
        list_valid_epochs(&storage, job_id.as_str()).unwrap(),
        vec![2],
        "invalid rollback epoch must stay excluded while future valid epoch remains"
    );
    assert!(
        storage
            .read_bytes(&metadata_path(job_id.as_str(), 1))
            .unwrap()
            .is_some(),
        "failed activation must not delete the rejected rollback epoch"
    );
    assert_eq!(
        latest_valid_epoch(&storage, job_id.as_str()).unwrap(),
        2,
        "failed activation must leave latest valid epoch unchanged"
    );
    let coord = coordinator.checkpoint_coordinator(&job_id).unwrap();
    assert_eq!(coord.current_epoch(), 0);
    assert_eq!(coord.fencing_token().as_u64(), 1);
}

#[test]
fn coordinator_restore_activation_prunes_future_epochs_and_uses_live_token() {
    use krishiv_state::checkpoint::latest_valid_epoch;

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-restore-act").unwrap());
    let job_id = JobId::try_new("job-restore-act").unwrap();
    let spec = JobSpec::new(job_id.clone(), "streaming-restore", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-restore").unwrap(), "restore-stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-restore").unwrap(),
                    "stream:restore",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    write_scheduler_restore_epoch(&storage, job_id.as_str(), 1, 1);
    write_scheduler_restore_epoch(&storage, job_id.as_str(), 2, 1);
    write_scheduler_restore_epoch(&storage, job_id.as_str(), 3, 1);
    assert_eq!(
        list_valid_epochs(&storage, job_id.as_str()).unwrap(),
        vec![1, 2, 3]
    );

    let metadata = coordinator
        .activate_job_restore_from_checkpoint_with_fencing(&job_id, 1, &storage_path, Some(7))
        .unwrap();
    assert_eq!(metadata.epoch, 1);

    let coord = coordinator
        .checkpoint_coordinator(&job_id)
        .expect("checkpoint coordinator remains registered");
    assert_eq!(coord.current_epoch(), 1);
    assert_eq!(coord.fencing_token().as_u64(), 7);
    assert_eq!(
        coord.coordinator_state(),
        &CheckpointCoordinatorState::Committed { epoch: 1 }
    );
    assert_eq!(
        list_valid_epochs(&storage, job_id.as_str()).unwrap(),
        vec![1],
        "future checkpoint epochs must be pruned after rollback activation"
    );
    assert_eq!(
        latest_valid_epoch(&storage, job_id.as_str()).unwrap(),
        1,
        "restart recovery must not resurrect pruned future epochs"
    );

    let request = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
    assert_eq!(request[0].epoch, 2);
    assert_eq!(request[0].fencing_token.as_u64(), 7);
}

