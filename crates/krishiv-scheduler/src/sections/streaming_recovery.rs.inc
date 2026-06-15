// ── Phase 3: streaming recovery authority ─────────────────────────────────

/// Build a streaming job with a checkpoint coordinator and `task_count`
/// Running tasks on one executor.  Returns the assignments for ack
/// construction.
fn streaming_checkpoint_job(
    coord_name: &str,
    exec_name: &str,
    job_name: &str,
    storage_path: &str,
    task_count: usize,
) -> (
    Coordinator,
    ExecutorId,
    JobId,
    Vec<krishiv_proto::ExecutorTaskAssignment>,
) {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new(coord_name).unwrap());
    let executor_id = ExecutorId::try_new(exec_name).unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(
            executor_id.clone(),
            "pod-p3",
            task_count,
        ))
        .unwrap();

    let job_id = JobId::try_new(job_name).unwrap();
    let mut stage = StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage");
    for idx in 1..=task_count {
        stage = stage.with_task(TaskSpec::new(
            TaskId::try_new(format!("task-{idx}")).unwrap(),
            "stream:tw",
        ));
    }
    let spec = JobSpec::new(job_id.clone(), job_name, JobKind::Streaming)
        .with_checkpoint(5_000, storage_path)
        .with_stage(stage);
    coordinator.submit_job(spec).unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    assert_eq!(assignments.len(), task_count);
    for assignment in &assignments {
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
                .with_lease_generation(lease),
            )
            .unwrap();
    }
    (coordinator, executor_id, job_id, assignments)
}

/// Drive one full checkpoint epoch (initiate + acks with real snapshots)
/// to a durable commit and return the committed epoch number.
fn commit_one_epoch(
    coordinator: &mut Coordinator,
    storage: &dyn CheckpointStorage,
    job_id: &JobId,
    assignments: &[krishiv_proto::ExecutorTaskAssignment],
    snapshot_payloads: &[Vec<u8>],
) -> u64 {
    let requests = coordinator.trigger_checkpoint_for_job(job_id).unwrap();
    let epoch = requests[0].epoch;
    let token = requests[0].fencing_token;
    for (assignment, payload) in assignments.iter().zip(snapshot_payloads) {
        let task = assignment.task_id().as_str();
        let operator = format!("operator-{task}");
        krishiv_state::checkpoint::write_operator_snapshot(
            storage,
            job_id.as_str(),
            epoch,
            &operator,
            task,
            payload,
        )
        .unwrap();
        let snap_path =
            krishiv_state::checkpoint::snapshot_path(job_id.as_str(), epoch, &operator, task);
        let ack = CheckpointAckRequest {
            job_id: job_id.clone(),
            operator_id: krishiv_proto::OperatorId::try_new(operator).unwrap(),
            task_id: assignment.task_id().clone(),
            epoch,
            fencing_token: token,
            source_offsets: vec![krishiv_proto::CheckpointSourceOffset {
                partition_id: krishiv_proto::PartitionId::try_new(format!(
                    "kafka-events-{}",
                    assignment.task_id().as_str()
                ))
                .unwrap(),
                offset: 42,
            }],
            snapshot_path: Some(snap_path),
        };
        coordinator.handle_checkpoint_ack(ack);
    }
    assert!(matches!(
        coordinator
            .checkpoint_coordinator(job_id)
            .unwrap()
            .coordinator_state(),
        CheckpointCoordinatorState::Committed { .. }
    ));
    epoch
}

fn window_state_payload(keys: &[&str]) -> Vec<u8> {
    let entries: Vec<krishiv_state::SnapshotEntry> = keys
        .iter()
        .map(|key| {
            let kb = key.as_bytes();
            let mut state_key = Vec::new();
            state_key.extend_from_slice(b"tw:");
            state_key.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            state_key.extend_from_slice(kb);
            state_key.extend_from_slice(&0i64.to_le_bytes());
            (
                "continuous-window".to_owned(),
                "tumbling".to_owned(),
                state_key,
                format!(
                    "{{\"values\":[1],\"has_value\":[true],\"avg_sums\":[],\"avg_counts\":[]}}"
                )
                .into_bytes(),
            )
        })
        .collect();
    krishiv_state::encode_snapshot_entries(&entries)
}

#[test]
fn executor_loss_aborts_inflight_epoch_and_directs_global_rollback() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, executor_id, job_id, assignments) = streaming_checkpoint_job(
        "coord-p3-loss",
        "exec-p3-loss",
        "job-p3-loss",
        &storage_path,
        1,
    );

    // Epoch 1 commits durably.
    let committed = commit_one_epoch(
        &mut coordinator,
        &storage,
        &job_id,
        &assignments,
        &[window_state_payload(&["a"])],
    );
    assert_eq!(committed, 1);

    // Epoch 2 goes in flight, then the executor dies.
    let _ = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
    assert!(
        coordinator
            .checkpoint_coordinator(&job_id)
            .unwrap()
            .is_awaiting_acks()
    );
    coordinator.mark_executor_lost(&executor_id).unwrap();

    // The in-flight epoch aborted immediately (no ack-timeout wait)…
    assert!(matches!(
        coordinator
            .checkpoint_coordinator(&job_id)
            .unwrap()
            .coordinator_state(),
        CheckpointCoordinatorState::Failed { epoch: 2, .. }
    ));
    // …and a global-rollback directive points at the committed epoch.
    let directive = coordinator
        .restore_directive(&job_id)
        .expect("executor loss in a checkpointed job must set a restore directive");
    assert_eq!(directive.epoch, 1);
}

#[test]
fn executor_loss_without_committed_epoch_sets_no_directive() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, executor_id, job_id, _assignments) = streaming_checkpoint_job(
        "coord-p3-nodir",
        "exec-p3-nodir",
        "job-p3-nodir",
        &storage_path,
        1,
    );

    coordinator.mark_executor_lost(&executor_id).unwrap();
    assert!(
        coordinator.restore_directive(&job_id).is_none(),
        "no committed epoch means nothing to roll back to"
    );
}

#[test]
fn heartbeat_delivers_checkpoint_complete_exactly_once_per_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, executor_id, job_id, assignments) = streaming_checkpoint_job(
        "coord-p3-complete",
        "exec-p3-complete",
        "job-p3-complete",
        &storage_path,
        1,
    );
    let committed = commit_one_epoch(
        &mut coordinator,
        &storage,
        &job_id,
        &assignments,
        &[window_state_payload(&["a"])],
    );

    let effects = coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();
    assert_eq!(effects.checkpoint_complete_commands.len(), 1);
    assert_eq!(effects.checkpoint_complete_commands[0].epoch, committed);
    assert_eq!(effects.checkpoint_complete_commands[0].job_id, job_id);

    // Second heartbeat: already delivered, nothing new.
    let effects = coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();
    assert!(effects.checkpoint_complete_commands.is_empty());
}

#[test]
fn heartbeat_delivers_restore_command_after_restore_activation() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, executor_id, job_id, assignments) = streaming_checkpoint_job(
        "coord-p3-restorecmd",
        "exec-p3-restorecmd",
        "job-p3-restorecmd",
        &storage_path,
        1,
    );
    let committed = commit_one_epoch(
        &mut coordinator,
        &storage,
        &job_id,
        &assignments,
        &[window_state_payload(&["a"])],
    );

    coordinator
        .activate_job_restore_from_checkpoint_with_fencing(
            &job_id,
            committed,
            &storage_path,
            None,
        )
        .unwrap();

    let effects = coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();
    assert_eq!(effects.restore_commands.len(), 1);
    assert_eq!(effects.restore_commands[0].epoch, committed);

    // Delivered once per (job, executor, epoch).
    let effects = coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();
    assert!(effects.restore_commands.is_empty());
}

#[test]
fn savepoint_epoch_is_preserved_in_durable_savepoints_area() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, _executor_id, job_id, assignments) =
        streaming_checkpoint_job("coord-p3-sp", "exec-p3-sp", "job-p3-sp", &storage_path, 1);

    let epoch = coordinator
        .savepoint_job(&job_id, Some("upgrade-v2".into()))
        .unwrap();
    let token = coordinator
        .checkpoint_coordinator(&job_id)
        .unwrap()
        .fencing_token();
    let task = assignments[0].task_id().as_str();
    let operator = format!("operator-{task}");
    krishiv_state::checkpoint::write_operator_snapshot(
        &storage,
        job_id.as_str(),
        epoch,
        &operator,
        task,
        &window_state_payload(&["a", "b"]),
    )
    .unwrap();
    let ack = CheckpointAckRequest {
        job_id: job_id.clone(),
        operator_id: krishiv_proto::OperatorId::try_new(operator.clone()).unwrap(),
        task_id: assignments[0].task_id().clone(),
        epoch,
        fencing_token: token,
        source_offsets: vec![],
        snapshot_path: Some(krishiv_state::checkpoint::snapshot_path(
            job_id.as_str(),
            epoch,
            &operator,
            task,
        )),
    };
    assert_eq!(
        coordinator.handle_checkpoint_ack(ack),
        CheckpointAckResponse::Accepted
    );

    // The committed savepoint epoch was copied into the savepoints area.
    let savepoints =
        krishiv_state::checkpoint::list_savepoints(&storage, job_id.as_str()).unwrap();
    assert_eq!(savepoints, vec![epoch]);

    // Restore from the savepoint reactivates the epoch and directs executors.
    let meta = coordinator
        .restore_job_from_savepoint(&job_id, epoch, &storage_path, None)
        .unwrap();
    assert_eq!(meta.epoch, epoch);
    assert!(meta.is_savepoint);
    assert_eq!(meta.savepoint_label.as_deref(), Some("upgrade-v2"));
    let directive = coordinator.restore_directive(&job_id).unwrap();
    assert_eq!(directive.epoch, epoch);
}

#[test]
fn stop_with_savepoint_cancels_job_after_durable_preserve() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, _executor_id, job_id, assignments) = streaming_checkpoint_job(
        "coord-p3-stop",
        "exec-p3-stop",
        "job-p3-stop",
        &storage_path,
        1,
    );

    let epoch = coordinator
        .stop_job_with_savepoint(&job_id, Some("drain".into()))
        .unwrap();
    // Job keeps running until the savepoint epoch commits.
    assert_ne!(
        coordinator.job_snapshot(&job_id).unwrap().state(),
        JobState::Cancelled
    );

    let token = coordinator
        .checkpoint_coordinator(&job_id)
        .unwrap()
        .fencing_token();
    let task = assignments[0].task_id().as_str();
    let operator = format!("operator-{task}");
    krishiv_state::checkpoint::write_operator_snapshot(
        &storage,
        job_id.as_str(),
        epoch,
        &operator,
        task,
        &window_state_payload(&["a"]),
    )
    .unwrap();
    let ack = CheckpointAckRequest {
        job_id: job_id.clone(),
        operator_id: krishiv_proto::OperatorId::try_new(operator.clone()).unwrap(),
        task_id: assignments[0].task_id().clone(),
        epoch,
        fencing_token: token,
        source_offsets: vec![],
        snapshot_path: Some(krishiv_state::checkpoint::snapshot_path(
            job_id.as_str(),
            epoch,
            &operator,
            task,
        )),
    };
    assert_eq!(
        coordinator.handle_checkpoint_ack(ack),
        CheckpointAckResponse::Accepted
    );

    // Savepoint durably preserved AND the job stopped.
    assert_eq!(
        krishiv_state::checkpoint::list_savepoints(&storage, job_id.as_str()).unwrap(),
        vec![epoch]
    );
    assert_eq!(
        coordinator.job_snapshot(&job_id).unwrap().state(),
        JobState::Cancelled,
        "stop-with-savepoint must cancel the job once the savepoint is durable"
    );
}

#[test]
fn rescaled_restore_redistributes_state_across_new_parallelism() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    // Job runs with 3 tasks now…
    let (mut coordinator, _executor_id, job_id, _assignments) = streaming_checkpoint_job(
        "coord-p3-rescale",
        "exec-p3-rescale",
        "job-p3-rescale",
        &storage_path,
        3,
    );

    // …but the checkpoint being restored was taken at parallelism 1.
    // Write it manually: one operator snapshot with many keys.
    let keys: Vec<String> = (0..60).map(|i| format!("key-{i}")).collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let payload = window_state_payload(&key_refs);
    krishiv_state::checkpoint::write_operator_snapshot(
        &storage,
        job_id.as_str(),
        1,
        "operator-old-task",
        "old-task",
        &payload,
    )
    .unwrap();
    let snap_path = krishiv_state::checkpoint::snapshot_path(
        job_id.as_str(),
        1,
        "operator-old-task",
        "old-task",
    );
    let meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: job_id.as_str().to_owned(),
        fencing_token: 1,
        coordinator_id: Some("old-coord".into()),
        timestamp_ms: 1,
        source_offsets: vec![krishiv_state::checkpoint::SourceOffsetRecord {
            partition_id: "kafka-events-0".into(),
            offset: 99,
        }],
        operator_snapshots: vec![krishiv_state::checkpoint::OperatorSnapshotRef {
            operator_id: "operator-old-task".into(),
            task_id: "old-task".into(),
            snapshot_path: snap_path,
        }],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
    };
    let mut manifest = IntegrityManifest::new();
    manifest.insert_bytes("metadata.json", &serde_json::to_vec_pretty(&meta).unwrap());
    manifest.insert_bytes("operator-old-task/old-task/state.bin", &payload);
    write_epoch_metadata(&storage, job_id.as_str(), 1, &meta).unwrap();
    write_manifest(&storage, job_id.as_str(), 1, &manifest).unwrap();
    krishiv_state::checkpoint::write_epoch_hint(&storage, job_id.as_str(), 1).unwrap();

    // Read-only restore rejects the mismatch…
    let err = coordinator
        .restore_job_from_checkpoint(&job_id, 1, &storage_path)
        .unwrap_err();
    assert!(err.to_string().contains("redistributes keyed state"));

    // …while the activating restore redistributes into a rescaled epoch.
    let restored = coordinator
        .activate_job_restore_from_checkpoint_with_fencing(&job_id, 1, &storage_path, None)
        .unwrap();
    assert_eq!(restored.epoch, 2, "rescaled epoch = source epoch + 1");
    assert!(
        !restored.operator_snapshots.is_empty(),
        "rescaled epoch must carry redistributed snapshots"
    );
    assert!(
        restored.operator_snapshots.len() <= 3,
        "at most one snapshot per current task"
    );
    // Source offsets carry over unchanged.
    assert_eq!(restored.source_offsets[0].offset, 99);
    // The rescaled epoch is sealed and valid.
    assert!(krishiv_state::checkpoint::validate_epoch(&storage, job_id.as_str(), 2).unwrap());
    // Every original key survived exactly once across the new snapshots.
    let mut recovered = std::collections::HashSet::new();
    for snap in &restored.operator_snapshots {
        let bytes = storage.read_bytes(&snap.snapshot_path).unwrap().unwrap();
        for (_, _, key, _) in krishiv_state::decode_snapshot_entries(&bytes).unwrap() {
            let group = krishiv_state::window_group_key(&key)
                .expect("window state key")
                .to_vec();
            assert!(recovered.insert(group), "key routed to more than one task");
        }
    }
    assert_eq!(recovered.len(), 60, "all keys redistributed");
    // The directive points at the rescaled epoch.
    assert_eq!(coordinator.restore_directive(&job_id).unwrap().epoch, 2);
}

#[test]
fn coordinator_restart_midepoch_resumes_from_last_committed() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();
    let (mut coordinator, _executor_id, job_id, assignments) = streaming_checkpoint_job(
        "coord-p3-restart",
        "exec-p3-restart",
        "job-p3-restart",
        &storage_path,
        1,
    );
    let committed = commit_one_epoch(
        &mut coordinator,
        &storage,
        &job_id,
        &assignments,
        &[window_state_payload(&["a"])],
    );

    // Epoch 2 in flight when the coordinator "crashes".
    let _ = coordinator.trigger_checkpoint_for_job(&job_id).unwrap();
    drop(coordinator);

    // A fresh coordinator recovers checkpoint state from storage alone.
    let mut recovered = CheckpointCoordinator::new(
        job_id.clone(),
        "coord-p3-restart-2".into(),
        Arc::new(LocalFsCheckpointStorage::new(storage.base_dir()).unwrap()),
        5_000,
        1,
    );
    let epoch = recovered.recover_from_storage().unwrap();
    assert_eq!(
        epoch,
        Some(committed),
        "restart must resume from the last durably committed epoch, \
         not the in-flight one"
    );
    assert_eq!(recovered.committed_epoch(), Some(committed));

    // The next epoch continues the sequence and stale acks are rejected.
    let next = recovered.initiate().unwrap();
    assert_eq!(next, committed + 1);
}
