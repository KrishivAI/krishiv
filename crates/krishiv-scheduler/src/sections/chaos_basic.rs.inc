// ── Group E: Chaos tests ──────────────────────────────────────────────────

#[test]
fn chaos_1_coordinator_kill_mid_checkpoint_no_duplicate_commit() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(
        JobId::try_new("job-chaos1").unwrap(),
        std::sync::Arc::new(storage),
        5000,
        2,
    );

    // Epoch 1 initiated; only one ack arrives before "kill".
    let epoch = coord.initiate().unwrap();
    assert_eq!(epoch, 1);
    let ack = make_ack(
        &JobId::try_new("job-chaos1").unwrap(),
        "task-0",
        1,
        coord.fencing_token(),
        None,
    );
    coord.receive_ack(ack).unwrap(); // partial — quorum not met

    // Simulate coordinator kill → abort.
    coord.abort_epoch("coordinator killed");
    assert!(
        matches!(
            coord.coordinator_state(),
            CheckpointCoordinatorState::Failed { .. }
        ),
        "state must be Failed after abort"
    );

    // Nothing committed to storage.
    let epochs = coord.list_epochs().unwrap();
    assert!(epochs.is_empty(), "no epoch must be committed after abort");

    // Epoch 2 succeeds after "restart".
    let epoch2 = coord.initiate().unwrap();
    assert_eq!(epoch2, 2);
    for task in &["task-0", "task-1"] {
        let ack = make_ack(
            &JobId::try_new("job-chaos1").unwrap(),
            task,
            2,
            coord.fencing_token(),
            None,
        );
        coord.receive_ack(ack).unwrap();
    }
    let committed = coord.list_epochs().unwrap();
    assert_eq!(committed, vec![2], "only epoch 2 must be committed");
}

#[test]
fn chaos_1a_coordinator_restart_recovers_from_durable_metadata() {
    let storage: std::sync::Arc<dyn CheckpointStorage> =
        std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-chaos1a").unwrap();

    // Coordinator A: commit epoch 1.
    let mut coord_a =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 1);
    coord_a.initiate().unwrap();
    let ack = make_ack(&job_id, "task-0", 1, coord_a.fencing_token(), None);
    coord_a.receive_ack(ack).unwrap();
    let epochs = coord_a.list_epochs().unwrap();
    assert_eq!(epochs, vec![1]);

    // Coordinator B: new instance, same storage — recover.
    let mut coord_b =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 1);
    let recovered = coord_b.recover_from_storage().unwrap();
    assert_eq!(recovered, Some(1), "must recover epoch 1");
    assert_eq!(coord_b.current_epoch(), 1);

    // Coordinator B can initiate epoch 2 without re-committing epoch 1.
    let epoch2 = coord_b.initiate().unwrap();
    assert_eq!(epoch2, 2);
    let epochs_before = coord_b.list_epochs().unwrap();
    assert_eq!(epochs_before, vec![1], "epoch 2 not yet committed");
}

#[test]
fn chaos_2_executor_kill_mid_checkpoint_abort_is_clean() {
    let storage: std::sync::Arc<dyn CheckpointStorage> =
        std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-chaos2").unwrap();
    let mut coord = CheckpointCoordinator::new_for_test(job_id.clone(), storage, 5000, 2);

    coord.initiate().unwrap();
    // Only task-0 acks; task-1 is "dead".
    let ack = make_ack(&job_id, "task-0", 1, coord.fencing_token(), None);
    coord.receive_ack(ack).unwrap();

    coord.abort_epoch("executor-1 lost");
    let epochs = coord.list_epochs().unwrap();
    assert!(epochs.is_empty(), "partial epoch must not be committed");
    assert!(matches!(
        coord.coordinator_state(),
        CheckpointCoordinatorState::Failed { .. }
    ));

    // Epoch 2 with both tasks succeeds.
    coord.initiate().unwrap();
    for task in &["task-0", "task-1"] {
        let ack = make_ack(&job_id, task, 2, coord.fencing_token(), None);
        coord.receive_ack(ack).unwrap();
    }
    assert_eq!(coord.list_epochs().unwrap(), vec![2]);
}

#[test]
fn chaos_3_sink_kill_mid_write_abort_discards_staged_output() {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::TwoPhaseCommitSink;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

    let mut sink = krishiv_connectors::InMemoryTwoPhaseCommitSink::new();

    // Prepare epoch 1 then abort (simulating sink kill).
    let handle = sink.prepare(1, &batch).unwrap();
    sink.abort(handle).unwrap();

    assert!(sink.committed().is_empty(), "abort must not commit");
    assert_eq!(
        sink.staged_count(),
        0,
        "staged area must be cleared after abort"
    );

    // Epoch 2 prepare + commit succeeds.
    let handle2 = sink.prepare(2, &batch).unwrap();
    sink.commit(handle2).unwrap();
    assert_eq!(
        sink.committed().len(),
        1,
        "commit must land exactly one batch"
    );
    assert_eq!(sink.committed()[0].0, 2, "committed epoch must be 2");
}

#[test]
fn chaos_4_corrupt_checkpoint_fallback_to_prior_valid_epoch() {
    use krishiv_state::checkpoint::{
        CheckpointStorage, metadata_path, validate_epoch, write_epoch_metadata, write_manifest,
    };

    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let job_id = "job-chaos4";

    // Helper: write a minimal valid epoch and build the manifest from the
    // actual stored bytes (write_epoch_metadata uses to_vec_pretty internally).
    let write_valid_epoch = |epoch: u64, storage: &LocalFsCheckpointStorage| {
        let meta = CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: job_id.to_string(),
            fencing_token: FencingToken::initial().as_u64(),
            coordinator_id: None,
            timestamp_ms: epoch * 1000,
            source_offsets: vec![],
            operator_snapshots: vec![],
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        };
        let storage_dyn: &dyn CheckpointStorage = storage;
        write_epoch_metadata(storage_dyn, job_id, epoch, &meta).unwrap();
        // Read back the actual bytes so the manifest hash matches exactly.
        let stored_bytes = storage_dyn
            .read_bytes(&metadata_path(job_id, epoch))
            .unwrap()
            .unwrap();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &stored_bytes);
        write_manifest(storage_dyn, job_id, epoch, &manifest).unwrap();
    };

    write_valid_epoch(1, &storage);
    write_valid_epoch(2, &storage);

    // Corrupt epoch 2 metadata by overwriting with invalid JSON.
    let storage_dyn: &dyn CheckpointStorage = &storage;
    storage_dyn
        .write_bytes(&metadata_path(job_id, 2), b"not-valid-json")
        .unwrap();

    // latest_valid_epoch falls back to epoch 1.
    let valid_epochs = list_valid_epochs(&storage, job_id).unwrap();
    assert_eq!(
        valid_epochs,
        vec![1],
        "only epoch 1 is valid after corrupting epoch 2"
    );

    // Confirm individual epoch verdicts.
    // validate_epoch returns Ok(false) for hash mismatches, Ok(true) for valid.
    assert!(
        !validate_epoch(&storage, job_id, 2).unwrap_or(true),
        "corrupt epoch 2 must fail validation"
    );
    assert!(
        validate_epoch(&storage, job_id, 1).unwrap_or(false),
        "intact epoch 1 must pass validation"
    );
}

#[test]
fn chaos_e6_rolling_upgrade_savepoint_restore_preserves_epoch_sequence() {
    use krishiv_state::checkpoint::read_epoch_metadata;

    let storage: std::sync::Arc<dyn CheckpointStorage> =
        std::sync::Arc::new(LocalFsCheckpointStorage::ephemeral().unwrap());
    let job_id = JobId::try_new("job-chaos-e6").unwrap();

    // Coordinator A: normal epoch 1, then savepoint epoch 2.
    let mut coord_a =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 1);
    coord_a.initiate().unwrap();
    coord_a
        .receive_ack(make_ack(
            &job_id,
            "task-0",
            1,
            coord_a.fencing_token(),
            None,
        ))
        .unwrap();
    assert_eq!(coord_a.list_epochs().unwrap(), vec![1]);

    // Initiate savepoint (epoch 2).
    coord_a
        .initiate_savepoint(Some("pre-upgrade".to_string()))
        .unwrap();
    coord_a
        .receive_ack(make_ack(
            &job_id,
            "task-0",
            2,
            coord_a.fencing_token(),
            None,
        ))
        .unwrap();
    assert_eq!(coord_a.list_epochs().unwrap(), vec![1, 2]);

    // Verify savepoint metadata.
    let meta = read_epoch_metadata(&*storage, job_id.as_str(), 2)
        .unwrap()
        .unwrap();
    assert!(meta.is_savepoint, "epoch 2 must be a savepoint");
    assert_eq!(
        meta.savepoint_label.as_deref(),
        Some("pre-upgrade"),
        "savepoint label must match"
    );

    // Coordinator B (simulated "upgraded binary"): recover from same storage.
    let mut coord_b =
        CheckpointCoordinator::new_for_test(job_id.clone(), storage.clone(), 5000, 1);
    let recovered = coord_b.recover_from_storage().unwrap();
    assert_eq!(recovered, Some(2), "must recover savepoint epoch 2");

    // Initiate epoch 3 — no re-commit of epoch 2.
    let epoch3 = coord_b.initiate().unwrap();
    assert_eq!(epoch3, 3);
    // Epoch 2 still committed; epoch 3 not yet.
    assert_eq!(
        coord_b.list_epochs().unwrap(),
        vec![1, 2],
        "epoch 3 not committed yet — only 1 and 2 exist"
    );
}

