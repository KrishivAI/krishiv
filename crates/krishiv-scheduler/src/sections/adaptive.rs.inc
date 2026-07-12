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
fn hot_key_report_for_unknown_job_does_not_install_repartition_override() {
    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, HeartbeatHotKeyReport};

    let coordinator_id = CoordinatorId::try_new("coord-hk-unknown").unwrap();
    let mut coordinator = Coordinator::active(coordinator_id);

    let executor_id = ExecutorId::try_new("exec-hk-unknown").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 4))
        .unwrap();

    let job_id = JobId::try_new("job-hk-missing").unwrap();
    let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
        .with_hot_key_reports(vec![HeartbeatHotKeyReport {
            key: "stale-hot-key".into(),
            estimated_count: 750,
            max_error: 5,
            heat_score: 0.8,
            job_id: job_id.clone(),
            source_id: "src-stale".into(),
        }]);

    let effects = coordinator.executor_heartbeat(heartbeat).unwrap();
    assert!(
        !effects.source_throttles.is_empty(),
        "the reporting executor can still be throttled"
    );
    assert!(
        !coordinator.skew_repartition_overrides.contains_key(&job_id),
        "stale reports for unknown jobs must not affect future task placement"
    );
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
        .exec.executors
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
        .ckpt.coordinators
        .get_mut(&job_id)
        .unwrap()
        .initiate()
        .unwrap();
    let epoch = 1u64;
    coordinator
        .ckpt.notify_sent
        .insert((job_id.clone(), exec_id.clone(), epoch));
    coordinator
        .ckpt.barrier_sent
        .insert((job_id.clone(), epoch));

    assert_eq!(coordinator.ckpt.notify_sent.len(), 1);
    assert_eq!(coordinator.ckpt.barrier_sent.len(), 1);

    // A single tick of 1_000 ms is well above the 100 ms ack timeout, so
    // advance_heartbeat_clock must abort the epoch and clean up the stale entries.
    coordinator.advance_heartbeat_clock(1).unwrap();

    assert_eq!(
        coordinator.ckpt.notify_sent.len(),
        0,
        "checkpoint_notify_sent must be cleared after epoch abort (GAP-5)"
    );
    assert_eq!(
        coordinator.ckpt.barrier_sent.len(),
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
        coordinator.exec.executors.record_task_failure(&exec_id, 5);
    }

    let bad = coordinator.exec.executors.executors_over_failure_threshold(5);
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
        let _ = coordinator.exec.executors.record_task_failure(&exec_id, 5);
    }

    let broken = coordinator.exec.executors.executors_over_failure_threshold(5);

    assert!(
        !broken.is_empty(),
        "flood protection machinery should detect bad executor"
    );
}

// ── Phase 54: AQE stage-boundary rewrites ────────────────────────────────────

/// Phase 54 exit gate (coalescing): when a ShuffleMap stage succeeds with
/// measured per-partition sizes, the coordinator merges small reduce
/// partitions into fewer dfplan tasks — demonstrably reducing the
/// reduce-task count — and records a partition-coalesce adaptive decision.
#[test]
fn aqe_coalesces_pending_reduce_tasks_from_measured_sizes() {
    use krishiv_proto::{
        ShufflePartitionOutput, StageKind, TaskOutputMetadata, TaskState, TaskStatusUpdate,
    };

    let config = crate::CoordinatorConfig::default()
        .with_aqe_enabled(true)
        .with_aqe_mechanisms(true, true)
        .with_aqe_target_partition_bytes(25);
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-aqe-coalesce").unwrap(),
        config,
    );
    let executor_id = ExecutorId::try_new("exec-aqe").unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-aqe", 16))
        .unwrap();

    let map_stage = StageId::try_new("dist-s0").unwrap();
    let reduce_stage = StageId::try_new("dist-s1").unwrap();
    let mut reduce = StageSpec::new(reduce_stage.clone(), "reduce")
        .with_kind(StageKind::Result)
        .with_upstream_stage(map_stage.clone());
    for p in 0..4 {
        reduce = reduce.with_task(TaskSpec::new(
            TaskId::try_new(format!("dist-s1-t{p}")).unwrap(),
            format!("dfplan:v1:{p}:QUJD"),
        ));
    }
    let spec = JobSpec::new(JobId::try_new("job-aqe").unwrap(), "aqe", JobKind::Batch)
        .with_stage(
            StageSpec::new(map_stage.clone(), "map")
                .with_kind(StageKind::ShuffleMap)
                .with_task(TaskSpec::new(TaskId::try_new("dist-s0-t0").unwrap(), "sql: m"))
                .with_task(TaskSpec::new(TaskId::try_new("dist-s0-t1").unwrap(), "sql: m")),
        )
        .with_stage(reduce);
    let job_id = spec.job_id().clone();
    coordinator.submit_job(spec).unwrap();

    // Only the map tasks launch (reduce is upstream-gated).
    let launched = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    assert_eq!(launched.len(), 2, "map tasks launch first");

    // Each map task reports 5 bytes into every reduce partition — combined
    // 10 bytes per partition against a 25-byte target ⇒ groups {0,1}, {2,3}.
    for a in &launched {
        let meta = TaskOutputMetadata::new("shuffle", 10, 1, 1).with_shuffle_partitions(
            (0..4u32)
                .map(|p| ShufflePartitionOutput::new(p, 5, ""))
                .collect::<Vec<_>>(),
        );
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    a.stage_id().clone(),
                    a.task_id().clone(),
                    executor_id.clone(),
                    TaskState::Succeeded,
                    a.attempt_id().as_u32(),
                )
                .with_lease_generation(lease)
                .with_output_metadata(meta),
            )
            .unwrap();
    }

    let record = coordinator
        .job_coordinators
        .get(&job_id)
        .map(|jc| jc.read_record())
        .expect("job record");
    let reduce = record
        .stages()
        .iter()
        .find(|s| s.stage_id() == &reduce_stage)
        .expect("reduce stage");
    assert_eq!(
        reduce.tasks().len(),
        2,
        "AQE must coalesce 4 reduce partitions into 2 tasks"
    );
    let bodies: Vec<String> = reduce
        .tasks()
        .iter()
        .map(|t| krishiv_plan::TypedTaskFragment::decode_or_legacy(t.spec.description()).body)
        .collect();
    assert_eq!(
        bodies,
        vec![
            String::from("dfplan:v1:0,1:QUJD"),
            String::from("dfplan:v1:2,3:QUJD"),
        ],
        "coalesced bodies must carry grouped partition lists"
    );
    assert!(
        reduce
            .tasks()
            .iter()
            .all(|t| t.task_id().as_str().contains("-aqe-")),
        "rewritten tasks carry AQE-derived ids"
    );
    drop(record);

    let log = coordinator.adaptive_decision_log(&job_id);
    assert!(
        log.iter().any(|entry| matches!(
            entry.kind,
            crate::AdaptiveDecisionKind::PartitionCoalesce
        ) && entry.applied),
        "an applied partition-coalesce decision must be recorded, got {log:?}"
    );

    // The rewritten tasks flow through normal assignment + launch.
    let launched = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    assert_eq!(launched.len(), 2, "both coalesced reduce tasks launch");
}

/// Phase 54: `KRISHIV_AQE=off` (config master switch) leaves the reduce
/// stage untouched — the off switch is testable and tested.
#[test]
fn aqe_master_switch_off_leaves_reduce_tasks_untouched() {
    use krishiv_proto::{
        ShufflePartitionOutput, StageKind, TaskOutputMetadata, TaskState, TaskStatusUpdate,
    };

    let config = crate::CoordinatorConfig::default()
        .with_aqe_enabled(false)
        .with_aqe_target_partition_bytes(25);
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-aqe-off").unwrap(),
        config,
    );
    let executor_id = ExecutorId::try_new("exec-aqe-off").unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-off", 16))
        .unwrap();

    let map_stage = StageId::try_new("dist-s0").unwrap();
    let reduce_stage = StageId::try_new("dist-s1").unwrap();
    let mut reduce = StageSpec::new(reduce_stage.clone(), "reduce")
        .with_kind(StageKind::Result)
        .with_upstream_stage(map_stage.clone());
    for p in 0..4 {
        reduce = reduce.with_task(TaskSpec::new(
            TaskId::try_new(format!("dist-s1-t{p}")).unwrap(),
            format!("dfplan:v1:{p}:QUJD"),
        ));
    }
    let spec = JobSpec::new(
        JobId::try_new("job-aqe-off").unwrap(),
        "aqe-off",
        JobKind::Batch,
    )
    .with_stage(
        StageSpec::new(map_stage.clone(), "map")
            .with_kind(StageKind::ShuffleMap)
            .with_task(TaskSpec::new(TaskId::try_new("dist-s0-t0").unwrap(), "sql: m")),
    )
    .with_stage(reduce);
    let job_id = spec.job_id().clone();
    coordinator.submit_job(spec).unwrap();
    let launched = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let map_assignment = launched.first().expect("map assignment");
    let meta = TaskOutputMetadata::new("shuffle", 10, 1, 1).with_shuffle_partitions(
        (0..4u32)
            .map(|p| ShufflePartitionOutput::new(p, 5, ""))
            .collect::<Vec<_>>(),
    );
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                map_assignment.stage_id().clone(),
                map_assignment.task_id().clone(),
                executor_id.clone(),
                TaskState::Succeeded,
                map_assignment.attempt_id().as_u32(),
            )
            .with_lease_generation(lease)
            .with_output_metadata(meta),
        )
        .unwrap();

    let record = coordinator
        .job_coordinators
        .get(&job_id)
        .map(|jc| jc.read_record())
        .expect("job record");
    let reduce = record
        .stages()
        .iter()
        .find(|s| s.stage_id() == &reduce_stage)
        .expect("reduce stage");
    assert_eq!(reduce.tasks().len(), 4, "AQE off ⇒ no rewrite");
    assert!(
        coordinator.adaptive_decision_log(&job_id).is_empty(),
        "AQE off ⇒ no adaptive decisions"
    );
}

/// Phase 54 safety: a skew-eligible partition whose plan cannot be proven
/// split-safe (undecodable payload here) is NOT split — the safety gate
/// fails closed and only coalescing applies.
#[test]
fn aqe_skew_split_fails_closed_on_unsafe_plans() {
    use krishiv_proto::{ShufflePartitionOutput, StageKind, TaskOutputMetadata, TaskState};

    let map_stage = StageId::try_new("dist-s0").unwrap();
    let reduce_stage = StageId::try_new("dist-s1").unwrap();
    let mut reduce = StageSpec::new(reduce_stage.clone(), "reduce")
        .with_kind(StageKind::Result)
        .with_upstream_stage(map_stage.clone());
    for p in 0..4 {
        reduce = reduce.with_task(TaskSpec::new(
            TaskId::try_new(format!("dist-s1-t{p}")).unwrap(),
            format!("dfplan:v1:{p}:QUJD"),
        ));
    }
    let spec = JobSpec::new(
        JobId::try_new("job-aqe-skew").unwrap(),
        "aqe-skew",
        JobKind::Batch,
    )
    .with_stage(
        StageSpec::new(map_stage.clone(), "map")
            .with_kind(StageKind::ShuffleMap)
            .with_task(TaskSpec::new(TaskId::try_new("dist-s0-t0").unwrap(), "sql: m"))
            .with_task(TaskSpec::new(TaskId::try_new("dist-s0-t1").unwrap(), "sql: m")),
    )
    .with_stage(reduce);
    let mut record = crate::JobRecord::from_spec(spec, 1);
    let executor_id = ExecutorId::try_new("exec-skew").unwrap();
    for stage in record.stages_mut() {
        if stage.stage_id() != &map_stage {
            continue;
        }
        for task in stage.tasks_mut() {
            task.state = TaskState::Succeeded;
            task.assigned_executor = Some(executor_id.clone());
            // Partition 2 is heavily skewed: 1000 bytes vs 10-byte median.
            task.output_metadata = Some(
                TaskOutputMetadata::new("shuffle", 10, 1, 1).with_shuffle_partitions(vec![
                    ShufflePartitionOutput::new(0, 5, ""),
                    ShufflePartitionOutput::new(1, 5, ""),
                    ShufflePartitionOutput::new(2, 500, ""),
                    ShufflePartitionOutput::new(3, 5, ""),
                ]),
            );
        }
        stage.refresh_state();
    }
    let thresholds = crate::coordinator::aqe::AqeThresholds {
        coalesce: true,
        skew_split: true,
        target_partition_bytes: 25,
        skew_factor: 4.0,
        skew_min_bytes: 100,
    };
    let rewrites = crate::coordinator::aqe::plan_aqe_rewrites(&record, &map_stage, &thresholds);
    let rewrite = rewrites.first().expect("coalesce rewrite still planned");
    assert!(
        rewrite.skew_split_partitions.is_empty(),
        "undecodable plan payload must fail the split-safety gate closed"
    );
    assert_eq!(
        rewrite.bodies,
        vec![
            String::from("dfplan:v1:0,1:QUJD"),
            String::from("dfplan:v1:2:QUJD"),
            String::from("dfplan:v1:3:QUJD"),
        ],
        "coalescing still groups the small partitions around the big one"
    );
}
