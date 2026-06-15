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
    restarted.recover_from_store(&store).unwrap();

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
    restarted.recover_from_store(&store).unwrap();

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
