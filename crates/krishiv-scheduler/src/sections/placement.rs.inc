// ── Deferred placement: submit without executors ──────────────────────────

#[test]
fn submit_job_without_executors_queues_as_pending() {
    allow_anonymous_for_tests();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-defer").unwrap());

    // Submit with NO executors registered.
    let job_id = JobId::try_new("deferred-job").unwrap();
    let outcome = coordinator
        .submit_job(single_task_job(job_id.clone()))
        .expect("submit_job must succeed even with no executors");
    assert!(
        matches!(outcome, SubmitOutcome::Accepted),
        "job must be Accepted, not rejected for missing executors; got {outcome:?}"
    );

    // All tasks must be Pending — not Failed or Assigned.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    for stage in detail.stages() {
        for task in stage.tasks() {
            assert_eq!(
                task.state(),
                TaskState::Pending,
                "tasks must be Pending when submitted without executors"
            );
        }
    }
}

#[test]
fn deferred_job_assigned_after_executor_registers() {
    allow_anonymous_for_tests();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-defer2").unwrap());

    // Submit before any executor is registered.
    let job_id = JobId::try_new("deferred-job-2").unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    // Now register an executor.
    let exec_id = ExecutorId::try_new("exec-deferred").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-d", 4))
        .unwrap();

    // The orchestration tick should assign the pending task.
    coordinator.coordinator_tick().unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.state(),
        TaskState::Assigned,
        "task must be Assigned after executor registers and tick fires"
    );
}

/// Phase 2.5: When an executor is lost, shuffle partitions produced by
/// tasks that ran on it are marked Failed and those tasks are reset to Pending.
#[test]
fn executor_loss_invalidates_remote_shuffle_partitions() {
    use krishiv_proto::{ShufflePartitionOutput, StageState, TaskRuntimeStats};

    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-shuffle-loss").unwrap();
    let coord_id = CoordinatorId::try_new("coord-shuffle-loss").unwrap();
    let job_id = JobId::try_new("job-shuffle-loss").unwrap();
    let stage0_id = StageId::try_new("stage-0").unwrap();
    let stage1_id = StageId::try_new("stage-1").unwrap();
    let task0_id = TaskId::try_new("task-0").unwrap();
    let task1_id = TaskId::try_new("task-1").unwrap();

    let mut coordinator = Coordinator::active(coord_id);
    let lease_gen = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-loss", 2))
        .unwrap();

    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "shuffle-loss-test", JobKind::Batch)
                .with_stage(
                    StageSpec::new(stage0_id.clone(), "write stage")
                        .with_task(TaskSpec::new(task0_id.clone(), "shuffle-write:hash:col:2")),
                )
                .with_stage(
                    StageSpec::new(stage1_id.clone(), "read stage")
                        .with_upstream_stage(stage0_id.clone())
                        .with_task(TaskSpec::new(task1_id.clone(), "sql: SELECT 1")),
                ),
        )
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let assign = assignments.first().unwrap();

    // Simulate stage-0 task completing with remote shuffle partitions.
    let shuffle_meta = TaskOutputMetadata::new("shuffle", 10, 1, 1)
        .with_shuffle_partitions(vec![
            ShufflePartitionOutput::new(0, 1024, "http://exec-loss-host:9000"),
            ShufflePartitionOutput::new(1, 1024, "http://exec-loss-host:9000"),
        ])
        .with_runtime_stats(TaskRuntimeStats {
            serialized_bytes: 2048,
            ..Default::default()
        });

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                assign.job_id().clone(),
                assign.stage_id().clone(),
                assign.task_id().clone(),
                exec_id.clone(),
                TaskState::Succeeded,
                assign.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_gen)
            .with_output_metadata(shuffle_meta),
        )
        .unwrap();

    // Stage-0 should be Succeeded.
    {
        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let s0 = detail
            .stages()
            .iter()
            .find(|s| s.stage_id() == &stage0_id)
            .unwrap();
        assert_eq!(
            s0.state(),
            StageState::Succeeded,
            "stage-0 must be Succeeded"
        );
    }

    // Now mark the executor as lost — shuffle partitions should be invalidated.
    coordinator.mark_executor_lost(&exec_id).unwrap();

    // The stage-0 task should now be Pending (its shuffle data is gone).
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let s0 = detail
        .stages()
        .iter()
        .find(|s| s.stage_id() == &stage0_id)
        .unwrap();
    let t0 = s0
        .tasks()
        .iter()
        .find(|t| t.task_id() == &task0_id)
        .unwrap();
    assert_eq!(
        t0.state(),
        TaskState::Pending,
        "task-0 must be reset to Pending after executor loss"
    );
}

/// Phase 2.9: After a shuffle stage completes with serialized_bytes reported,
/// the AQE coalesce hint is stored on the coordinator.
#[test]
fn aqe_stage_boundary_hint_stored_after_shuffle_stage_completes() {
    use krishiv_proto::{ShufflePartitionOutput, TaskRuntimeStats};

    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-aqe-hint").unwrap();
    let coord_id = CoordinatorId::try_new("coord-aqe-hint").unwrap();
    let job_id = JobId::try_new("job-aqe-hint").unwrap();
    let stage0_id = StageId::try_new("stage-aqe-0").unwrap();
    let stage1_id = StageId::try_new("stage-aqe-1").unwrap();
    let task0_id = TaskId::try_new("task-aqe-0").unwrap();

    let mut coordinator = Coordinator::active(coord_id);
    let lease_gen = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-aqe", 2))
        .unwrap();

    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "aqe-hint-test", JobKind::Batch)
                .with_stage(
                    StageSpec::new(stage0_id.clone(), "shuffle write").with_task(
                        TaskSpec::new(task0_id.clone(), "shuffle-write:hash:col:200"),
                    ),
                )
                .with_stage(
                    StageSpec::new(stage1_id.clone(), "aggregate")
                        .with_upstream_stage(stage0_id.clone())
                        .with_task(TaskSpec::new(
                            TaskId::try_new("task-aqe-1").unwrap(),
                            "sql: SELECT 1",
                        )),
                ),
        )
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let assign = assignments.first().unwrap();

    // 200 partitions × 1 byte serialized — well below 128 MiB CoalesceRule target.
    let shuffle_meta = TaskOutputMetadata::new("shuffle", 200, 1, 1)
        .with_shuffle_partitions(
            (0u32..200)
                .map(|p| ShufflePartitionOutput::new(p, 1, "http://aqe-host:9000"))
                .collect(),
        )
        .with_runtime_stats(TaskRuntimeStats {
            serialized_bytes: 200,
            ..Default::default()
        });

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                assign.job_id().clone(),
                assign.stage_id().clone(),
                assign.task_id().clone(),
                exec_id.clone(),
                TaskState::Succeeded,
                assign.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_gen)
            .with_output_metadata(shuffle_meta),
        )
        .unwrap();

    // The AQE hint must be stored and collapse 200 tiny partitions to ≤ 10.
    let hint_key = (job_id.clone(), stage0_id.clone());
    assert!(
        coordinator.aqe_coalesce_hints.contains_key(&hint_key),
        "AQE coalesce hint must be stored after shuffle stage completes"
    );
    let hint = coordinator.aqe_coalesce_hints[&hint_key];
    assert!(
        hint <= 10,
        "coalesced hint must collapse 200 × 1-byte partitions to ≤ 10, got {hint}"
    );
}

/// SC1 + T7: a Result stage (terminal) must NOT trigger AQE hint storage even
/// when `serialized_bytes > 0`.  Only ShuffleMap stages write to the shuffle
/// store, so coalesce hints for them are meaningful; hints for Result stages
/// would refer to no downstream shuffle consumer.
#[test]
fn aqe_hint_not_stored_for_result_stage() {
    use krishiv_proto::{ShufflePartitionOutput, StageKind, TaskRuntimeStats};

    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-aqe-result").unwrap();
    let coord_id = CoordinatorId::try_new("coord-aqe-result").unwrap();
    let job_id = JobId::try_new("job-aqe-result").unwrap();
    let stage0_id = StageId::try_new("stage-aqe-r0").unwrap();
    let task0_id = TaskId::try_new("task-aqe-r0").unwrap();

    let mut coordinator = Coordinator::active(coord_id);
    let lease_gen = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-aqe-r", 2))
        .unwrap();

    // Single Result stage (no upstream, no shuffle write) — simulates a
    // terminal aggregation stage that reads shuffle but does not produce it.
    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "aqe-result-test", JobKind::Batch).with_stage(
                StageSpec::new(stage0_id.clone(), "terminal-reduce")
                    .with_kind(StageKind::Result)
                    .with_task(TaskSpec::new(task0_id.clone(), "sql: SELECT 1")),
            ),
        )
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    let assign = assignments.first().unwrap();

    // Report non-zero serialized_bytes — would trigger AQE if kind check is absent.
    let meta = TaskOutputMetadata::new("result", 100, 100, 1)
        .with_shuffle_partitions(
            (0u32..100)
                .map(|p| ShufflePartitionOutput::new(p, 10, "http://result-host:9000"))
                .collect(),
        )
        .with_runtime_stats(TaskRuntimeStats {
            serialized_bytes: 1000,
            ..Default::default()
        });

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                assign.job_id().clone(),
                assign.stage_id().clone(),
                assign.task_id().clone(),
                exec_id.clone(),
                TaskState::Succeeded,
                assign.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_gen)
            .with_output_metadata(meta),
        )
        .unwrap();

    let hint_key = (job_id.clone(), stage0_id.clone());
    assert!(
        !coordinator.aqe_coalesce_hints.contains_key(&hint_key),
        "AQE coalesce hint must NOT be stored for a Result (terminal) stage"
    );
}

/// Speculative execution: a slow Running task in a ShuffleMap stage is
/// re-scheduled to Pending when its running time exceeds the median × factor.
/// `failure_count` must not be incremented (no retry budget consumed).
///
/// Uses `collect_speculation_work_at(now_ms)` with an injected wall-clock so
/// results are deterministic regardless of real elapsed time.
#[test]
fn speculative_execution_reschedules_straggler_without_consuming_retry_budget() {
    use krishiv_proto::StageKind;

    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-spec").unwrap();
    let coord_id = CoordinatorId::try_new("coord-spec").unwrap();
    let job_id = JobId::try_new("job-spec").unwrap();
    let stage_id = StageId::try_new("stage-spec").unwrap();
    let task0 = TaskId::try_new("task-spec-0").unwrap();
    let task1 = TaskId::try_new("task-spec-1").unwrap();
    let task2 = TaskId::try_new("task-spec-2").unwrap();
    let task_slow = TaskId::try_new("task-spec-slow").unwrap();

    let config = CoordinatorConfig::default()
        .with_speculative_execution(true)
        .with_speculative_slowdown_factor(1.5)
        .with_speculative_min_completed_tasks(3);

    let mut coordinator = Coordinator::active_with_config(coord_id, config);
    let lease_gen = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-spec", 8))
        .unwrap();

    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "spec-test", JobKind::Batch).with_stage(
                StageSpec::new(stage_id.clone(), "map")
                    .with_kind(StageKind::ShuffleMap)
                    .with_task(TaskSpec::new(task0.clone(), "sql: SELECT 1"))
                    .with_task(TaskSpec::new(task1.clone(), "sql: SELECT 1"))
                    .with_task(TaskSpec::new(task2.clone(), "sql: SELECT 1"))
                    .with_task(TaskSpec::new(task_slow.clone(), "sql: SELECT 1")),
            ),
        )
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();

    let find = |tid: &TaskId| {
        assignments
            .iter()
            .find(|a| a.task_id() == tid)
            .unwrap()
            .clone()
    };

    // Succeed 3 fast tasks via the normal path.
    let fast_meta = TaskOutputMetadata::new("map", 1, 1, 1);
    for tid in [&task0, &task1, &task2] {
        let a = find(tid);
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    a.job_id().clone(),
                    a.stage_id().clone(),
                    a.task_id().clone(),
                    exec_id.clone(),
                    TaskState::Succeeded,
                    a.attempt_id().as_u32(),
                )
                .with_lease_generation(lease_gen)
                .with_output_metadata(fast_meta.clone()),
            )
            .unwrap();
    }

    // Inject deterministic timing:
    //   fast tasks → completed_duration_ms = 100 ms  (median = 100)
    //   slow task  → state = Running, assigned_at_ms = 0  (running_ms = 1000 at fake now)
    {
        let jc = coordinator.job_coordinators.get(&job_id).unwrap();
        let mut rec = jc.write_record();
        for stage in rec.stages_mut() {
            for task in stage.tasks_mut() {
                let tid = task.task_id().clone();
                if tid == task0 || tid == task1 || tid == task2 {
                    task.completed_duration_ms = Some(100);
                } else if tid == task_slow {
                    task.state = TaskState::Running;
                    task.assigned_at_ms = Some(0);
                }
            }
            stage.refresh_state();
        }
    }

    // Fake now_ms = 1_000.
    // running_ms  = 1000 - 0 = 1000.
    // threshold_ms = 100 * 1.5 = 150.
    // 1000 > 150 → straggler found.
    let work = coordinator.collect_speculation_work_at(1_000);
    let slow_item = work
        .iter()
        .find(|w| w.task_id == task_slow)
        .expect("straggler task-spec-slow must be identified for speculation");
    assert_eq!(slow_item.median_ms, 100, "median should be 100 ms");

    let failure_count_before = {
        let jc = coordinator.job_coordinators.get(&job_id).unwrap();
        let rec = jc.read_record();
        rec.stages()
            .iter()
            .flat_map(|s| s.tasks())
            .find(|t| t.task_id() == &task_slow)
            .map(|t| t.failure_count)
            .unwrap_or(0)
    };

    coordinator.apply_speculation_preempts(&work);

    let jc = coordinator.job_coordinators.get(&job_id).unwrap();
    let rec = jc.read_record();
    let slow_task_rec = rec
        .stages()
        .iter()
        .flat_map(|s| s.tasks())
        .find(|t| t.task_id() == &task_slow)
        .unwrap();
    assert_eq!(
        slow_task_rec.state(),
        TaskState::Pending,
        "straggler must be reset to Pending"
    );
    assert_eq!(
        slow_task_rec.failure_count,
        failure_count_before,
        "speculative preemption must NOT increment failure_count"
    );
}

// --- Phase 2.1: memory-estimate admission control ---

#[test]
fn memory_admission_queues_job_exceeding_cluster_capacity() {
    let executor_id = ExecutorId::try_new("exec-mem-adm").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-mem-adm").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 2))
        .unwrap();
    // Executor reports 1 GiB limit with 900 MiB used → 124 MiB available.
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_memory_used_bytes(900 * 1024 * 1024)
                .with_memory_limit_bytes(1024 * 1024 * 1024),
        )
        .unwrap();

    // Job asks for 512 MiB — more than the cluster has available.
    let spec = demo_job().with_memory_limit_bytes(512 * 1024 * 1024);
    let outcome = coordinator.submit_job(spec).unwrap();
    assert_eq!(
        outcome,
        SubmitOutcome::Queued { position: 0 },
        "job asking beyond available cluster memory must be queued"
    );
}

#[test]
fn memory_admission_accepts_job_within_cluster_capacity() {
    let executor_id = ExecutorId::try_new("exec-mem-ok").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-mem-ok").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "host-1", 2))
        .unwrap();
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
                .with_memory_used_bytes(100 * 1024 * 1024)
                .with_memory_limit_bytes(1024 * 1024 * 1024),
        )
        .unwrap();

    let spec = demo_job().with_memory_limit_bytes(512 * 1024 * 1024);
    let outcome = coordinator.submit_job(spec).unwrap();
    assert_eq!(outcome, SubmitOutcome::Accepted);
}

#[test]
fn slot_aware_scheduler_prefers_executor_with_live_free_slots() {
    let job_id = JobId::try_new("job-live-placement").unwrap();
    let task_id = TaskId::try_new("task-live-placement").unwrap();
    let spec = JobSpec::new(job_id, "live-placement", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-live-placement").unwrap(), "stage")
            .with_task(TaskSpec::new(task_id, "sql: SELECT 1")),
    );
    let busy = crate::job::ExecutorPlacement::new(
        ExecutorId::try_new("exec-busy").unwrap(),
        4,
        4,
    );
    let idle = crate::job::ExecutorPlacement::new(
        ExecutorId::try_new("exec-idle").unwrap(),
        4,
        0,
    );

    let assignments = crate::job::SlotAwareScheduler::place_with_load(&spec, &[busy, idle])
        .expect("placement must succeed");

    assert_eq!(
        assignments[0].executor_id(),
        &ExecutorId::try_new("exec-idle").unwrap()
    );
}

#[test]
fn submit_placement_uses_heartbeat_active_task_count() {
    let exec_busy = ExecutorId::try_new("exec-submit-busy").unwrap();
    let exec_idle = ExecutorId::try_new("exec-submit-idle").unwrap();
    let job_id = JobId::try_new("job-submit-live-placement").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-submit-live-placement").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_busy.clone(), "busy", 4))
        .unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_idle.clone(), "idle", 4))
        .unwrap();
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(exec_busy, ExecutorState::Healthy).with_active_task_count(4),
        )
        .unwrap();
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(exec_idle.clone(), ExecutorState::Healthy)
                .with_active_task_count(0),
        )
        .unwrap();

    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .expect("submit must succeed");

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let assigned = detail.stages()[0].tasks()[0].assigned_executor().unwrap();
    assert_eq!(assigned, &exec_idle);
}

#[tokio::test]
async fn launch_target_resolution_error_clears_in_flight_guard() {
    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-no-task-endpoint").unwrap();
    let job_id = JobId::try_new("job-resolve-clears-launch").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-resolve-clears-launch").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id, "host-no-endpoint", 1))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    let shared = SharedCoordinator::new(coordinator);
    let launched = shared.drive_pending_task_launches().await.unwrap();
    assert_eq!(launched, 0);

    let retry_assignments = {
        let mut coord = shared.write().await;
        coord.launch_assigned_task_assignments(&job_id).unwrap()
    };
    assert_eq!(
        retry_assignments.len(),
        1,
        "resolution failure must clear launch_in_flight so the task can retry"
    );
}

#[test]
fn recovery_clears_persisted_in_flight_launch_guards() {
    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-recover-inflight").unwrap();
    let job_id = JobId::try_new("job-recover-inflight").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-recover-inflight").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "host-recover", 1))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();
    assert_eq!(
        coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap()
            .len(),
        1
    );

    let mut store = crate::store::InMemoryMetadataStore::default();
    coordinator.persist_jobs_to_store(&mut store).unwrap();
    store
        .save_executor(&ExecutorDescriptor::new(exec_id, "host-recover", 1))
        .unwrap();

    let mut restarted =
        Coordinator::active(CoordinatorId::try_new("coord-recover-inflight-2").unwrap());
    restarted.recover_from_store(&mut store).unwrap();

    assert_eq!(
        restarted
            .launch_assigned_task_assignments(&job_id)
            .unwrap()
            .len(),
        1,
        "recovered in-flight assignment must be retryable"
    );
}

// --- Phase 2.6: post-restart shuffle availability audit ---

/// Build a two-stage shuffle job, succeed the producer with remote shuffle
/// output on `exec_id`, and return the coordinator.
fn coordinator_with_completed_shuffle_producer(
    coord_name: &str,
    exec_id: &ExecutorId,
    job_id: &JobId,
) -> Coordinator {
    use krishiv_proto::{ShufflePartitionOutput, TaskRuntimeStats};

    let stage0_id = StageId::try_new("stage-0").unwrap();
    let stage1_id = StageId::try_new("stage-1").unwrap();
    let task0_id = TaskId::try_new("task-0").unwrap();
    let task1_id = TaskId::try_new("task-1").unwrap();

    let mut coordinator = Coordinator::active(CoordinatorId::try_new(coord_name).unwrap());
    let lease_gen = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-audit", 2))
        .unwrap();

    coordinator
        .submit_job(
            JobSpec::new(job_id.clone(), "shuffle-audit-test", JobKind::Batch)
                .with_stage(
                    StageSpec::new(stage0_id.clone(), "write stage")
                        .with_task(TaskSpec::new(task0_id, "shuffle-write:hash:col:2")),
                )
                .with_stage(
                    StageSpec::new(stage1_id, "read stage")
                        .with_upstream_stage(stage0_id)
                        .with_task(TaskSpec::new(task1_id, "sql: SELECT 1")),
                ),
        )
        .unwrap();

    let assignments = coordinator
        .launch_assigned_task_assignments(job_id)
        .unwrap();
    let assign = assignments.first().unwrap();
    let shuffle_meta = TaskOutputMetadata::new("shuffle", 10, 1, 1)
        .with_shuffle_partitions(vec![ShufflePartitionOutput::new(
            0,
            1024,
            "http://audit-host:9000",
        )])
        .with_runtime_stats(TaskRuntimeStats {
            serialized_bytes: 1024,
            ..Default::default()
        });
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                assign.job_id().clone(),
                assign.stage_id().clone(),
                assign.task_id().clone(),
                exec_id.clone(),
                TaskState::Succeeded,
                assign.attempt_id().as_u32(),
            )
            .with_lease_generation(lease_gen)
            .with_output_metadata(shuffle_meta),
        )
        .unwrap();
    coordinator
}

#[test]
fn restart_audit_invalidates_shuffle_from_unknown_executor() {
    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-audit-gone").unwrap();
    let job_id = JobId::try_new("job-audit-1").unwrap();
    let coordinator =
        coordinator_with_completed_shuffle_producer("coord-audit-1", &exec_id, &job_id);

    // Persist the job but NOT the executor descriptor, then recover into a
    // fresh coordinator: the producer's executor is unknown after restart.
    let mut store = crate::store::InMemoryMetadataStore::default();
    coordinator.persist_jobs_to_store(&mut store).unwrap();

    let mut restarted = Coordinator::active(CoordinatorId::try_new("coord-audit-1b").unwrap());
    restarted.recover_from_store(&mut store).unwrap();

    // The audit inside recover_from_store must have reset the producer to
    // Pending because its shuffle host no longer exists.
    let detail = restarted.job_detail_snapshot(&job_id).unwrap();
    let t0 = detail.stages()[0].tasks()[0].state();
    assert_eq!(
        t0,
        TaskState::Pending,
        "shuffle producer on an unknown executor must be re-queued by the restart audit"
    );
}

#[test]
fn restart_audit_keeps_shuffle_from_restored_executor() {
    allow_anonymous_for_tests();

    let exec_id = ExecutorId::try_new("exec-audit-alive").unwrap();
    let job_id = JobId::try_new("job-audit-2").unwrap();
    let coordinator =
        coordinator_with_completed_shuffle_producer("coord-audit-2", &exec_id, &job_id);

    // Persist BOTH the job and the executor descriptor.
    let mut store = crate::store::InMemoryMetadataStore::default();
    coordinator.persist_jobs_to_store(&mut store).unwrap();
    store
        .save_executor(&ExecutorDescriptor::new(exec_id, "pod-audit", 2))
        .unwrap();

    let mut restarted = Coordinator::active(CoordinatorId::try_new("coord-audit-2b").unwrap());
    restarted.recover_from_store(&mut store).unwrap();

    // The executor descriptor was restored: the grace period applies and
    // the producer's output must stay Succeeded.
    let detail = restarted.job_detail_snapshot(&job_id).unwrap();
    let t0 = detail.stages()[0].tasks()[0].state();
    assert_eq!(
        t0,
        TaskState::Succeeded,
        "restored executor's shuffle output must survive the restart audit"
    );
}

// ── SC11: cascade circuit breaker ─────────────────────────────────────────

/// Verify that N executor losses within the window trips the cascade circuit
/// breaker (assignment returns empty) and that it auto-resets after cooldown.
#[test]
fn cascade_circuit_breaker_trips_and_auto_resets() {
    allow_anonymous_for_tests();

    // Configure: threshold=3 losses, window=10s, cooldown=20s.
    let config = CoordinatorConfig::new(3, 10)
        .with_cascade_failure_threshold(3)
        .with_cascade_window_ms(10_000)
        .with_cascade_cooldown_ms(20_000);
    let coord_id = CoordinatorId::try_new("coord-cascade").unwrap();
    let mut coordinator = Coordinator::active_with_config(coord_id, config);

    // Register some executors so the coordinator is in a valid state.
    let base_t: u64 = 1_000_000;

    // Breaker is open=false initially.
    assert!(
        !coordinator.cascade_breaker_open(base_t),
        "breaker must start closed"
    );

    // Record 2 losses — below threshold (3).
    coordinator.record_cascade_loss(base_t);
    coordinator.record_cascade_loss(base_t + 1_000);
    assert!(
        !coordinator.cascade_breaker_open(base_t + 2_000),
        "2 losses < threshold 3: breaker must remain closed"
    );

    // Record 3rd loss within the 10s window — should trip.
    coordinator.record_cascade_loss(base_t + 3_000);
    assert!(
        coordinator.cascade_breaker_open(base_t + 3_000),
        "3rd loss in window: breaker must be OPEN"
    );

    // Still open before cooldown (10 s into 20 s cooldown).
    assert!(
        coordinator.cascade_breaker_open(base_t + 13_000),
        "within cooldown: breaker must remain OPEN"
    );

    // After cooldown (>= 20_000 ms after trip at base_t+3_000 → >= base_t+23_000).
    assert!(
        !coordinator.cascade_breaker_open(base_t + 24_000),
        "after cooldown: breaker must auto-reset to CLOSED"
    );

    // Calling again confirms reset is permanent (not oscillating).
    assert!(
        !coordinator.cascade_breaker_open(base_t + 30_000),
        "breaker must stay closed after auto-reset"
    );
}

/// Old losses outside the window should NOT count toward the threshold.
#[test]
fn cascade_circuit_breaker_prunes_old_losses() {
    allow_anonymous_for_tests();

    let config = CoordinatorConfig::new(3, 10)
        .with_cascade_failure_threshold(3)
        .with_cascade_window_ms(5_000);
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-cascade-prune").unwrap(), config);

    // Two losses at t=0.
    coordinator.record_cascade_loss(0);
    coordinator.record_cascade_loss(1_000);

    // Move time forward past the 5s window.
    let after_window: u64 = 8_000;

    // Third loss at t=8_000 — but the first two are now outside the 5s window.
    coordinator.record_cascade_loss(after_window);
    assert!(
        !coordinator.cascade_breaker_open(after_window),
        "old losses pruned: only 1 fresh loss in window; breaker must remain CLOSED"
    );
}

// ── SC10: ResourceProfile per-stage placement filtering ───────────────────

/// Verify that `schedulable_placements_for_profile` filters out executors that
/// cannot satisfy a stage's memory requirement, while returning the same set as
/// the unfiltered path when no profile is given or the profile fits.
#[test]
fn resource_profile_filters_executors_by_memory() {
    allow_anonymous_for_tests();

    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, ResourceProfile};

    let coord_id = CoordinatorId::try_new("coord-rp").unwrap();
    let mut coordinator = Coordinator::active(coord_id);

    // One executor: 2 GiB limit, 256 MiB used → 1.75 GiB (1,879,048,192 bytes) available.
    let exec_id = ExecutorId::try_new("exec-rp-1").unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-rp", 4))
        .unwrap();

    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(lease)
                .with_memory_used_bytes(256 * 1024 * 1024)
                .with_memory_limit_bytes(2 * 1024 * 1024 * 1024),
        )
        .unwrap();

    // No profile → executor is always eligible.
    let unfiltered = coordinator
        .exec
        .executors
        .schedulable_placements_for_profile(None);
    assert_eq!(unfiltered.len(), 1, "unfiltered: executor must appear");

    // Small profile (512 MiB) → fits within 1.75 GiB available.
    let small = ResourceProfile {
        task_cpus: 0.5,
        task_memory_bytes: 512 * 1024 * 1024,
    };
    let fits = coordinator
        .exec
        .executors
        .schedulable_placements_for_profile(Some(&small));
    assert_eq!(fits.len(), 1, "512 MiB profile fits: executor must appear");

    // Huge profile (3 GiB) → exceeds 1.75 GiB available.
    let huge = ResourceProfile {
        task_cpus: 1.0,
        task_memory_bytes: 3 * 1024 * 1024 * 1024,
    };
    let rejected = coordinator
        .exec
        .executors
        .schedulable_placements_for_profile(Some(&huge));
    assert!(
        rejected.is_empty(),
        "3 GiB profile exceeds available memory: executor must be excluded"
    );
}

/// When a stage with a large memory profile has its tasks reset to Pending
/// (e.g. after a stage retry), `assign_pending_tasks` must leave those tasks
/// Pending if no executor satisfies the requirement.
#[test]
fn resource_profile_assign_pending_respects_profile_after_retry() {
    allow_anonymous_for_tests();

    use krishiv_proto::{ExecutorHeartbeat, ExecutorState, ResourceProfile, StageSpec, TaskSpec};

    let coord_id = CoordinatorId::try_new("coord-rp2").unwrap();
    let mut coordinator = Coordinator::active(coord_id);

    // Submit with no executor first → all tasks start Pending.
    let huge_profile = ResourceProfile {
        task_cpus: 1.0,
        task_memory_bytes: 3 * 1024 * 1024 * 1024,
    };
    let stage = StageSpec::new(StageId::try_new("s0").unwrap(), "heavy")
        .with_resource_profile(huge_profile)
        .with_max_task_attempts(3)
        .with_task(TaskSpec::new(TaskId::try_new("t0").unwrap(), "t"));
    let job_id = JobId::try_new("job-rp2").unwrap();
    let spec = JobSpec::new(job_id.clone(), "rp-heavy", JobKind::Batch).with_stage(stage);
    coordinator.submit_job(spec).unwrap();

    // Register an executor with only 512 MiB available.
    let exec_id = ExecutorId::try_new("exec-rp2").unwrap();
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod2", 4))
        .unwrap();
    // Heartbeat: 1 GiB limit, 512 MiB used → only 512 MiB free.
    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(exec_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(lease)
                .with_memory_used_bytes(512 * 1024 * 1024)
                .with_memory_limit_bytes(1024 * 1024 * 1024),
        )
        .unwrap();

    // Manually force the task back to Pending (simulating a retry after the
    // executor registered without a memory snapshot).
    {
        let jc = coordinator.job_coordinators.get(&job_id).unwrap();
        let mut rec = jc.write_record();
        for stage in rec.stages.iter_mut() {
            for task in stage.tasks_mut() {
                task.state = TaskState::Pending;
                task.assigned_executor = None;
            }
        }
    }

    // assign_pending_tasks must leave the task Pending — no executor satisfies 3 GiB.
    let placed = coordinator.assign_pending_tasks(&job_id).unwrap();
    assert_eq!(placed, 0, "no executor satisfies 3 GiB requirement; placed={placed}");

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(
        detail.stages()[0].tasks()[0].state(),
        TaskState::Pending,
        "task must remain Pending when no executor can satisfy the resource profile"
    );
}
