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
            encoded_offset: 100_i64.to_le_bytes().to_vec(),
        }],
        snapshot_path,
        unaligned_buffers: Vec::new(),
        sink_transactions: Vec::new(),
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
    assert_eq!(meta.source_offsets.len(), 2);
    assert!(
        meta.source_offsets
            .iter()
            .all(|offset| offset.encoded_offset == 100_i64.to_le_bytes().to_vec())
    );

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
            unaligned_buffer_refs: Vec::new(),
            sink_transactions: Vec::new(),
            streaming_profile: None,
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
    let notify_count_before = coordinator.ckpt.notify_sent.len();

    // Initiate an epoch — expected_task_count = 2.
    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(2);
        coord.initiate().unwrap();
    }

    // Send one ack — NOT enough for quorum (needs 2).
    // The ack is accepted (recorded in pending_acks) but quorum is not reached.
    // Per the corrected semantics (matching the async sharded path), a
    // non-quorum ack returns Accepted, not StaleEpoch. StaleEpoch is reserved
    // for actual Err results (epoch mismatch, fencing failure, etc.).
    let ack = make_ack(&job_id, "task-1", 1, FencingToken::initial(), None);
    let response = coordinator.handle_checkpoint_ack(ack);
    assert_eq!(
        response,
        CheckpointAckResponse::Accepted,
        "single ack with 2 expected tasks must return Accepted (ack recorded, quorum pending)"
    );

    // Notify entries must NOT have been cleared by a non-quorum ack.
    assert_eq!(
        coordinator.ckpt.notify_sent.len(),
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

// ── Tier 1A regression tests ────────────────────────────────────────────────

/// Regression for fix #4: `Ok(false)` (ack accepted, quorum not reached) must
/// return `Accepted`, not `StaleEpoch`. The async sharded path already does
/// this; the sync path was returning `StaleEpoch`, causing barrier fanout to
/// log N-1 spurious "rejected" per checkpoint round.
#[test]
fn non_quorum_ack_returns_accepted_not_stale_epoch() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-nq-accepted").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-nq-accepted").unwrap(),
            "pod-nq-accepted",
            1,
        ))
        .unwrap();

    let job_id = JobId::try_new("job-nq-accepted").unwrap();
    let spec = JobSpec::new(job_id.clone(), "nq-accepted", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-nq-a").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-nq-1").unwrap(),
                    "stream:tw",
                ))
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-nq-2").unwrap(),
                    "stream:tw",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(2);
        coord.initiate().unwrap();
    }

    // First ack: accepted but not quorum.
    let ack1 = make_ack(&job_id, "task-nq-1", 1, FencingToken::initial(), None);
    let response1 = coordinator.handle_checkpoint_ack(ack1);
    assert_eq!(
        response1,
        CheckpointAckResponse::Accepted,
        "non-quorum ack must return Accepted (ack recorded, quorum pending)"
    );

    // Second ack: quorum reached, still Accepted.
    let ack2 = make_ack(&job_id, "task-nq-2", 1, FencingToken::initial(), None);
    let response2 = coordinator.handle_checkpoint_ack(ack2);
    assert_eq!(
        response2,
        CheckpointAckResponse::Accepted,
        "quorum-reaching ack must return Accepted"
    );
}

/// Regression for fix #4 (deferred variant): `handle_checkpoint_ack_deferred`
/// returns `(Accepted, Some(_))` on quorum, `(Accepted, None)` on non-quorum,
/// and never returns `StaleEpoch` for `Ok(false)`.
#[test]
fn deferred_ack_returns_post_commit_only_on_quorum() {
    let storage = LocalFsCheckpointStorage::ephemeral().unwrap();
    let storage_path = storage.base_dir().to_string_lossy().to_string();

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-deferred").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-deferred").unwrap(),
            "pod-deferred",
            1,
        ))
        .unwrap();

    let job_id = JobId::try_new("job-deferred").unwrap();
    let spec = JobSpec::new(job_id.clone(), "deferred", JobKind::Streaming)
        .with_checkpoint(5000, &storage_path)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-def").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-def-1").unwrap(),
                    "stream:tw",
                ))
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-def-2").unwrap(),
                    "stream:tw",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    {
        let coord = coordinator.checkpoint_coordinator_mut(&job_id).unwrap();
        coord.set_expected_task_count(2);
        coord.initiate().unwrap();
    }

    // Non-quorum: no post-commit work.
    let ack1 = make_ack(&job_id, "task-def-1", 1, FencingToken::initial(), None);
    let (response1, post1) = coordinator.handle_checkpoint_ack_deferred(ack1);
    assert_eq!(response1, CheckpointAckResponse::Accepted);
    assert!(post1.is_none(), "non-quorum ack must not trigger post-commit");

    // Quorum: post-commit work returned for caller to execute outside lock.
    let ack2 = make_ack(&job_id, "task-def-2", 1, FencingToken::initial(), None);
    let (response2, post2) = coordinator.handle_checkpoint_ack_deferred(ack2);
    assert_eq!(response2, CheckpointAckResponse::Accepted);
    assert_eq!(
        post2,
        Some((job_id.clone(), 1)),
        "quorum ack must return post-commit work"
    );
}

/// Regression for fix #5: circuit-breaker assignment clearing is synchronous.
/// When an executor exceeds the failure threshold, its assignments are cleared
/// inline under the coordinator write lock — no `tokio::spawn` race.
#[test]
fn circuit_breaker_clears_assignments_synchronously() {
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-cb-sync").unwrap());
    let exec_id = ExecutorId::try_new("exec-cb-bad").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-cb-bad", 2))
        .unwrap();

    let job_id = JobId::try_new("job-cb-sync").unwrap();
    let spec = JobSpec::new(job_id.clone(), "cb-sync", JobKind::Batch)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-cb").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-cb-1").unwrap(),
                    "batch",
                ))
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-cb-2").unwrap(),
                    "batch",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    // Simulate assigning tasks to the bad executor.
    {
        let jc = coordinator.job_coordinator(&job_id).unwrap();
        let mut record = jc.write_record();
        for stage in record.stages_mut() {
            for task in stage.tasks_mut() {
                task.assigned_executor = Some(exec_id.clone());
                task.state = TaskState::Running;
                task.launch_in_flight = false;
                task.attempt = 1;
            }
        }
    }

    // Directly trigger the circuit breaker by recording failures up to the
    // threshold, then verify the sync clearing path clears assignments.
    let threshold = coordinator.config.circuit_breaker_failure_threshold();
    let mut exceeded = false;
    for _ in 0..threshold {
        exceeded = coordinator.exec.executors.record_task_failure(&exec_id, threshold);
    }
    assert!(exceeded, "circuit breaker must trip after threshold failures");

    // Call the sync clearing method directly (as the fixed code does).
    let jc = coordinator.job_coordinator(&job_id).unwrap();
    let cleared = jc.clear_assignments_for_bad_executor_and_count_sync(&exec_id);
    assert_eq!(cleared, 2, "both tasks must be cleared from the bad executor");

    // Verify assignments are cleared.
    let record = jc.read_record();
    for stage in record.stages() {
        for task in stage.tasks() {
            assert_ne!(
                task.assigned_executor(),
                Some(&exec_id),
                "bad executor assignments must be cleared after circuit breaker"
            );
        }
    }
}

/// Regression for fix #3: stall detection uses `last_progress_ms` (refreshed
/// on status updates) instead of only `assigned_at_ms`. A task that reports
/// progress (output metadata) within the stall window must NOT be flagged.
#[test]
fn stall_detection_respects_progress_updates() {
    use krishiv_common::async_util::unix_now_ms;

    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-stall-progress").unwrap());
    coordinator.config = coordinator
        .config
        .with_task_stall_timeout_ms(60_000); // 1 minute stall timeout

    let exec_id = ExecutorId::try_new("exec-stall").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-stall", 1))
        .unwrap();

    let job_id = JobId::try_new("job-stall-progress").unwrap();
    let spec = JobSpec::new(job_id.clone(), "stall-progress", JobKind::Batch)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-stall").unwrap(), "stage")
                .with_task(TaskSpec::new(
                    TaskId::try_new("task-stall-1").unwrap(),
                    "batch",
                )),
        );
    coordinator.submit_job(spec).unwrap();

    // Simulate a task that was assigned 2 minutes ago (past the 1-min timeout).
    {
        let jc = coordinator.job_coordinator(&job_id).unwrap();
        let mut record = jc.write_record();
        for stage in record.stages_mut() {
            for task in stage.tasks_mut() {
                task.assigned_executor = Some(exec_id.clone());
                task.state = TaskState::Running;
                task.launch_in_flight = false;
                let two_min_ago = u64::try_from(unix_now_ms()).unwrap_or(0) - 120_000;
                task.assigned_at_ms = Some(two_min_ago);
                // last_progress_ms is None — no progress reported.
                task.last_progress_ms = None;
            }
        }
    }

    // Should be flagged as stalled (no progress, assigned 2 min ago).
    let stalled = coordinator.collect_stall_cancel_work();
    assert_eq!(stalled.len(), 1, "task with no progress for >1 min must be flagged");

    // Now simulate a progress update (output metadata) that refreshes
    // last_progress_ms to "just now".
    {
        let jc = coordinator.job_coordinator(&job_id).unwrap();
        let mut record = jc.write_record();
        for stage in record.stages_mut() {
            for task in stage.tasks_mut() {
                task.last_progress_ms =
                    Some(u64::try_from(unix_now_ms()).unwrap_or(0));
            }
        }
    }

    // Should NOT be flagged — progress was reported recently.
    let stalled = coordinator.collect_stall_cancel_work();
    assert!(
        stalled.is_empty(),
        "task with recent progress must NOT be flagged as stalled"
    );
}
