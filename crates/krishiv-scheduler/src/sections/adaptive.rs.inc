// ── R7.2 Adaptive decision log tests ─────────────────────────────────────

#[test]
fn adaptive_decision_log_empty_for_unknown_job() {
    let coordinator_id = CoordinatorId::try_new("coord-adaptive").unwrap();
    let coordinator = Coordinator::active(coordinator_id);
    let job_id = JobId::try_new("unknown-job").unwrap();
    assert!(coordinator.adaptive_decision_log(&job_id).is_empty());
}

#[test]
fn hot_key_reports_appended_to_decision_log() {
    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

    let coordinator_id = CoordinatorId::try_new("coord-hk").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-hk").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
        .unwrap();

    let job_id = JobId::try_new("job-hk-1").unwrap();
    let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
        .with_hot_key_reports(vec![HeartbeatHotKeyReport {
            key: "hot-key".into(),
            estimated_count: 500,
            max_error: 10,
            heat_score: 0.35,
            job_id: job_id.clone(),
            source_id: "src-0".into(),
        }]);

    let effects = coordinator.executor_heartbeat(heartbeat).unwrap();
    // heat_score 0.35 >= HOT_KEY_HEAT_THRESHOLD 0.3 → throttle IS applied.
    assert!(
        !effects.source_throttles.is_empty(),
        "source throttle must be issued when heat_score >= threshold"
    );

    let log = coordinator.adaptive_decision_log(&job_id);
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].kind, AdaptiveDecisionKind::HotKeySplit);
    assert!(log[0].applied, "hot-key split must be applied by default");
    assert!(log[0].details.contains("hot-key"));
}

#[test]
fn hot_key_split_suppressed_by_override() {
    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

    let coordinator_id = CoordinatorId::try_new("coord-hk-override").unwrap();
    let mut coordinator =
        Coordinator::active(coordinator_id).with_adaptive_override(AdaptiveOverrideConfig {
            disable_hot_key_splitting: true,
            ..AdaptiveOverrideConfig::default()
        });

    let executor_id = ExecutorId::try_new("exec-hk-override").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
        .unwrap();

    let job_id = JobId::try_new("job-hk-2").unwrap();
    let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
        .with_hot_key_reports(vec![HeartbeatHotKeyReport {
            key: "skewed-key".into(),
            estimated_count: 1000,
            max_error: 0,
            heat_score: 0.9,
            job_id: job_id.clone(),
            source_id: "src-0".into(),
        }]);

    coordinator.executor_heartbeat(heartbeat).unwrap();

    let log = coordinator.adaptive_decision_log(&job_id);
    assert_eq!(log.len(), 1);
    assert!(
        !log[0].applied,
        "decision must be suppressed when disable_hot_key_splitting=true"
    );
    assert!(log[0].details.contains("skewed-key"));
}

#[test]
fn multiple_hot_key_reports_all_logged() {
    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

    let coordinator_id = CoordinatorId::try_new("coord-hk-multi").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-hk-multi").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
        .unwrap();

    let job_id = JobId::try_new("job-hk-3").unwrap();
    let reports = vec![
        HeartbeatHotKeyReport {
            key: "key-a".into(),
            estimated_count: 300,
            max_error: 5,
            heat_score: 0.3,
            job_id: job_id.clone(),
            source_id: "src-0".into(),
        },
        HeartbeatHotKeyReport {
            key: "key-b".into(),
            estimated_count: 200,
            max_error: 3,
            heat_score: 0.2,
            job_id: job_id.clone(),
            source_id: "src-0".into(),
        },
    ];

    let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
        .with_hot_key_reports(reports);
    coordinator.executor_heartbeat(heartbeat).unwrap();

    let log = coordinator.adaptive_decision_log(&job_id);
    assert_eq!(log.len(), 2, "one log entry per hot-key report");
}

#[test]
fn hot_key_reports_from_task_status_queue_throttles_and_skew_override() {
    use krishiv_proto::{
        ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport, TaskOutputMetadata, TaskState,
        TaskStatusUpdate,
    };

    let coordinator_id = CoordinatorId::try_new("coord-hk-task-status").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-hk-ts").unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
        .unwrap();

    let job_id = JobId::try_new("job-hk-ts").unwrap();
    let stage_id = StageId::try_new("stage-1").unwrap();
    let task_id = TaskId::try_new("task-1").unwrap();

    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "hk-ts-test", JobKind::Batch).with_stage(
                StageSpec::new(stage_id.clone(), "stage")
                    .with_task(TaskSpec::new(task_id.clone(), "batch")),
            ),
        )
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
            .with_lease_generation(lease),
        )
        .unwrap();

    let meta = TaskOutputMetadata::new("shuffle", 100, 1, 2).with_hot_key_reports(vec![
        HeartbeatHotKeyReport {
            key: "hot-key".into(),
            estimated_count: 500,
            max_error: 10,
            heat_score: 0.35,
            job_id: job_id.clone(),
            source_id: "src-0".into(),
        },
    ]);

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                assignment.stage_id().clone(),
                assignment.task_id().clone(),
                executor_id.clone(),
                TaskState::Succeeded,
                assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease)
            .with_output_metadata(meta),
        )
        .unwrap();

    assert!(
        coordinator.skew_repartition_overrides.contains_key(&job_id),
        "batch hot-key from task status must set skew repartition override"
    );
    assert!(
        coordinator
            .pending_source_throttles
            .contains_key(&executor_id),
        "throttles from task status must be queued for next heartbeat"
    );

    let effects = coordinator
        .executor_heartbeat(ExecutorHeartbeat::new(
            executor_id.clone(),
            ExecutorState::Healthy,
        ))
        .unwrap();
    assert!(
        !effects.source_throttles.is_empty(),
        "queued throttles must be delivered on heartbeat"
    );
    assert!(coordinator.pending_source_throttles.is_empty());
}

#[test]
fn adaptive_override_config_defaults_all_false() {
    let cfg = AdaptiveOverrideConfig::default();
    assert!(!cfg.disable_hot_key_splitting);
    assert!(!cfg.disable_adaptive_repartition);
    assert!(!cfg.disable_source_throttling);
}

/// GAP-5: When a checkpoint epoch is aborted due to ack timeout, the
/// coordinator's checkpoint_notify_sent and barrier_dispatch_sent sets must
/// be cleaned up so they don't accumulate indefinitely.
#[test]
fn checkpoint_abort_cleans_up_stale_tracking_entries() {
    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().to_str().unwrap().to_owned();

    // Short ack timeout (100 ms) and 1-second tick so a single tick of
    // 1 000 ms blows past the 100 ms ack timeout.
    let config = CoordinatorConfig::new(1, 100)
        .with_tick_period_ms(1_000)
        .with_checkpoint_ack_timeout_ms(100);
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-gap5").unwrap(), config);

    let exec_id = ExecutorId::try_new("exec-gap5").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "host-1", 2))
        .unwrap();

    // Submit a streaming job; this creates a CheckpointCoordinator automatically.
    let job_id = JobId::try_new("job-gap5").unwrap();
    let task_id = TaskId::try_new("t-gap5").unwrap();
    let stage_id = StageId::try_new("s-gap5").unwrap();
    let stage = StageSpec::new(stage_id.clone(), "stage-gap5")
        .with_task(TaskSpec::new(task_id.clone(), "fragment-gap5"));
    let spec = JobSpec::new(job_id.clone(), "gap5-test", JobKind::Streaming)
        .with_stage(stage)
        .with_checkpoint(5_000, storage_path);
    coordinator.submit_job(spec).unwrap();

    // Transition the task to Running so the checkpoint quorum == 1.
    let lease = coordinator
        .executors
        .find_executor(&exec_id)
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
        exec_id.clone(),
        TaskState::Running,
        attempt,
    )
    .with_lease_generation(lease);
    coordinator.apply_task_update(update).unwrap();

    // Manually initiate an epoch so the checkpoint coordinator is in
    // AwaitingAcks state, then inject stale tracking entries.
    coordinator
        .checkpoint_coordinators
        .get_mut(&job_id)
        .unwrap()
        .initiate()
        .unwrap();
    let epoch = 1u64;
    coordinator
        .checkpoint_notify_sent
        .insert((job_id.clone(), exec_id.clone(), epoch));
    coordinator
        .barrier_dispatch_sent
        .insert((job_id.clone(), epoch));

    assert_eq!(coordinator.checkpoint_notify_sent.len(), 1);
    assert_eq!(coordinator.barrier_dispatch_sent.len(), 1);

    // A single tick of 1_000 ms is well above the 100 ms ack timeout, so
    // advance_heartbeat_clock must abort the epoch and clean up the stale entries.
    coordinator.advance_heartbeat_clock(1).unwrap();

    assert_eq!(
        coordinator.checkpoint_notify_sent.len(),
        0,
        "checkpoint_notify_sent must be cleared after epoch abort (GAP-5)"
    );
    assert_eq!(
        coordinator.barrier_dispatch_sent.len(),
        0,
        "barrier_dispatch_sent must be cleared after epoch abort (GAP-5)"
    );
}

#[test]
fn batch_sql_decode_inline_ipc_roundtrip() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::batch_sql::decode_inline_record_batches;

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![7_i64])) as _],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema().as_ref())
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let decoded = decode_inline_record_batches(&[buf]).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].num_rows(), 1);
}

// ─────────────────────────────────────────────────────────────────────
// PRR Parallel Execution: High-priority failure-mode tests (Track B)
// ─────────────────────────────────────────────────────────────────────

#[test]
fn circuit_breaker_actually_clears_assignments_from_bad_executor() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("cb-reassign").unwrap());
    let exec_id = ExecutorId::try_new("bad-exec").unwrap();
    let executor = ExecutorDescriptor::new(exec_id.clone(), "pod-bad", 2);

    coordinator.register_executor(executor).unwrap();

    // Drive the failure counter directly (this is what apply_task_update does internally)
    // until the executor is marked bad.
    for _ in 0..6 {
        coordinator.executors.record_task_failure(&exec_id, 5);
    }

    let bad = coordinator.executors.executors_over_failure_threshold(5);
    assert!(
        bad.contains(&exec_id),
        "executor must be in bad set after repeated failures"
    );
}

#[test]
fn assignment_flood_protection_basic() {
    // Validates the registry side of flood/circuit breaker protection.
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("flood-test").unwrap());
    let exec_id = ExecutorId::try_new("flood-exec").unwrap();

    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-flood", 1))
        .unwrap();

    for _ in 0..10 {
        let _ = coordinator.executors.record_task_failure(&exec_id, 5);
    }

    let broken = coordinator.executors.executors_over_failure_threshold(5);

    assert!(
        !broken.is_empty(),
        "flood protection machinery should detect bad executor"
    );
}

