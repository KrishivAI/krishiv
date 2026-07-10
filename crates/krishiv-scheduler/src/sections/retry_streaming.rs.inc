// ── P1.24: retry_stage sets Pending (not Assigned) ───────────────────────

#[test]
fn retried_tasks_are_pending_and_become_schedulable() {
    // P1.24: Verify that after a stage retry all tasks transition to Pending
    // so the scheduler can re-queue them through the normal placement path.
    let mut coordinator = Coordinator::active_with_config(
        CoordinatorId::try_new("coord-p124").unwrap(),
        CoordinatorConfig::new(1, 3),
    );
    let executor_id = ExecutorId::try_new("exec-p124").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = job.stages()[0].stage_id().clone();
    let task_id = job.stages()[0].tasks()[0].task_id().clone();

    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    // Report task failure to trigger a retry.
    coordinator
        .apply_task_update(TaskStatusUpdate::new(
            job_id.clone(),
            stage_id.clone(),
            task_id.clone(),
            executor_id.clone(),
            TaskState::Failed,
            1,
        ))
        .unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    assert_eq!(detail.stages()[0].retry_count(), 1);

    // All tasks must be Pending — not Assigned — so placement runs again.
    for task in detail.stages()[0].tasks() {
        assert_eq!(
            task.state(),
            TaskState::Pending,
            "retried task {} must be Pending, got {:?}",
            task.task_id(),
            task.state()
        );
    }

    // assign_pending_tasks + launch confirms tasks are re-schedulable.
    let assigned = coordinator.assign_pending_tasks(&job_id).unwrap();
    assert_eq!(assigned, 2, "both tasks must be re-assigned after retry");
    let launched = coordinator.launch_assigned_tasks(&job_id).unwrap();
    assert_eq!(
        launched, 2,
        "both tasks must be launchable after re-assignment"
    );
}

#[test]
fn coordinator_marks_executor_lost() {
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    coordinator.mark_executor_lost(&executor_id).unwrap();

    assert_eq!(
        coordinator.executor_snapshots()[0].state(),
        ExecutorState::Lost
    );
}

fn demo_job() -> JobSpec {
    demo_job_with_id(JobId::try_new("job-1").unwrap())
}

fn demo_job_with_id(job_id: JobId) -> JobSpec {
    JobSpec::new(job_id, "demo batch", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
            .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a"))
            .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "scan b")),
    )
}

fn single_task_job(job_id: JobId) -> JobSpec {
    JobSpec::new(job_id, "single task", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "scan")
            .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "scan a")),
    )
}

fn single_task_streaming_job(job_id: JobId) -> JobSpec {
    JobSpec::new(job_id, "streaming job", JobKind::Streaming).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "stream-stage").with_task(
            TaskSpec::new(TaskId::try_new("task-1").unwrap(), "stream-task"),
        ),
    )
}

// ── streaming refresh_state guard ─────────────────────────────────────

#[test]
fn streaming_job_does_not_succeed_when_all_stages_succeed() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            1,
        ))
        .unwrap();
    let job_id = JobId::try_new("job-stream-1").unwrap();
    coordinator
        .submit_job(single_task_streaming_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let executor_id = detail.stages()[0].tasks()[0]
        .assigned_executor()
        .unwrap()
        .clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();
    let attempt = detail.stages()[0].tasks()[0].attempt();

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                StageId::try_new("stage-1").unwrap(),
                task_id,
                executor_id,
                TaskState::Succeeded,
                attempt,
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    // Streaming jobs must never reach Succeeded — they stay Running.
    let final_snapshot = coordinator.job_snapshot(&job_id).unwrap();
    assert_ne!(
        final_snapshot.state(),
        JobState::Succeeded,
        "streaming job must not transition to Succeeded"
    );
    assert_eq!(final_snapshot.state(), JobState::Running);
}

#[test]
fn batch_job_succeeds_when_all_stages_succeed() {
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            1,
        ))
        .unwrap();
    let job_id = JobId::try_new("job-batch-1").unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let executor_id = detail.stages()[0].tasks()[0]
        .assigned_executor()
        .unwrap()
        .clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();
    let attempt = detail.stages()[0].tasks()[0].attempt();

    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                StageId::try_new("stage-1").unwrap(),
                task_id,
                executor_id,
                TaskState::Succeeded,
                attempt,
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    assert_eq!(
        coordinator.job_snapshot(&job_id).unwrap().state(),
        JobState::Succeeded,
        "batch job must transition to Succeeded"
    );
}

// ── streaming re-attach grace period ──────────────────────────────────

#[test]
fn streaming_executor_not_evicted_within_grace_period() {
    let config = CoordinatorConfig::new(1, 2).with_streaming_reattach_grace_ticks(10);
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-1").unwrap(), config);
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    let job_id = JobId::try_new("job-s-1").unwrap();
    coordinator
        .submit_job(single_task_streaming_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    // Mark the task Running so it has a committed executor assignment.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let exec_id_clone = detail.stages()[0].tasks()[0]
        .assigned_executor()
        .unwrap()
        .clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();
    let attempt = detail.stages()[0].tasks()[0].attempt();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                StageId::try_new("stage-1").unwrap(),
                task_id,
                exec_id_clone,
                TaskState::Running,
                attempt,
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    // Simulate coordinator restart via recover_from_store.
    // P1.23: the store must contain the streaming job so recovery can restore it.
    let mut store = crate::store::InMemoryMetadataStore::default();
    store
        .save_job(
            &coordinator
                .job_coordinators
                .values()
                .map(|jc| jc.read_record())
                .next()
                .unwrap(),
        )
        .unwrap();
    coordinator.recover_from_store(&mut store).unwrap();

    // Advance 3 ticks (> timeout of 2, but < grace period of 10).
    let evicted = coordinator.advance_heartbeat_clock(3).unwrap();
    assert!(
        !evicted.contains(&executor_id),
        "streaming executor must not be evicted within grace period"
    );
}

#[test]
fn streaming_executor_evicted_after_grace_period() {
    let config = CoordinatorConfig::new(1, 2).with_streaming_reattach_grace_ticks(2);
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-1").unwrap(), config);
    let executor_id = ExecutorId::try_new("exec-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 1))
        .unwrap();

    let job_id = JobId::try_new("job-s-2").unwrap();
    coordinator
        .submit_job(single_task_streaming_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    // Mark task Running.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let exec_id_clone = detail.stages()[0].tasks()[0]
        .assigned_executor()
        .unwrap()
        .clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();
    let attempt = detail.stages()[0].tasks()[0].attempt();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                StageId::try_new("stage-1").unwrap(),
                task_id,
                exec_id_clone,
                TaskState::Running,
                attempt,
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    // Recover from an empty store: the streaming job is not restored, so after
    // recovery the executor owns no streaming task and is therefore *not*
    // protected by the reattach grace window — it must be evicted.
    let mut store = crate::store::InMemoryMetadataStore::default();
    coordinator.recover_from_store(&mut store).unwrap();

    // Drive the heartbeat clock well past the grace period (2) + heartbeat
    // timeout (2). The eviction may land during recovery's stale-executor sweep
    // or in this explicit tick, so assert on the executor's resulting state
    // rather than a single call's return value.
    let evicted = coordinator.advance_heartbeat_clock(5).unwrap();
    let still_working = coordinator
        .executor_snapshots()
        .iter()
        .find(|e| e.executor_id() == &executor_id)
        .is_some_and(|e| e.state().can_accept_work());
    assert!(
        evicted.contains(&executor_id) || !still_working,
        "streaming executor owning no tasks must be evicted by the reattach window"
    );
}

#[test]
fn streaming_reattach_updates_task_watermark_and_offset() {
    // Scenario: coordinator has a running streaming job. The coordinator
    // "restarts" (recover_from_store). The executor re-registers and sends
    // a heartbeat with its current watermark and source offset. The coordinator
    // must update the task record without creating a new job.

    let config = CoordinatorConfig::new(1, 10).with_streaming_reattach_grace_ticks(20);
    let mut coordinator =
        Coordinator::active_with_config(CoordinatorId::try_new("coord-ra").unwrap(), config);

    let executor_id = ExecutorId::try_new("exec-ra-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job_id = JobId::try_new("job-ra-1").unwrap();
    coordinator
        .submit_job(single_task_streaming_job(job_id.clone()))
        .unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    // Retrieve task/stage ids and mark the task Running.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let stage_id = detail.stages()[0].stage_id().clone();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let exec_id = detail.stages()[0].tasks()[0]
        .assigned_executor()
        .unwrap()
        .clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();
    let attempt = detail.stages()[0].tasks()[0].attempt();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                stage_id,
                task_id.clone(),
                exec_id,
                TaskState::Running,
                attempt,
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    // Confirm job is Running before simulated restart.
    assert_eq!(
        coordinator
            .job_detail_snapshot(&job_id)
            .unwrap()
            .job()
            .state(),
        JobState::Running
    );

    // Simulate coordinator restart: persist the streaming job to the store
    // so recovery (P1.23) can restore it (in a real restart the store
    // would have been written before the coordinator process exited).
    let mut store = crate::store::InMemoryMetadataStore::default();
    store
        .save_job(
            &coordinator
                .job_coordinators
                .values()
                .map(|jc| jc.read_record())
                .next()
                .unwrap(),
        )
        .unwrap();
    coordinator.recover_from_store(&mut store).unwrap();

    // After a coordinator restart the executor re-registers (as the test comment
    // describes), which advances its lease generation. Stale, pre-restart leases
    // are fenced, so the post-restart heartbeat must carry the new lease.
    let lease = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    // Executor sends its first post-restart heartbeat carrying streaming state.
    let reported_watermark_ms: i64 = 12_000;
    let reported_offset = b"kafka-partition-0:offset-42".to_vec();
    let heartbeat = ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
        .with_lease_generation(lease)
        .with_streaming_task_states(vec![StreamingTaskState::new(
            task_id.clone(),
            reported_watermark_ms,
            reported_offset.clone(),
        )]);
    coordinator.executor_heartbeat(heartbeat).unwrap();

    // The coordinator must NOT have submitted a new job.
    let snapshots = coordinator.job_snapshots();
    assert_eq!(snapshots.len(), 1, "no duplicate job should be created");
    assert_eq!(snapshots[0].job_id(), &job_id);

    // The task record must now carry the executor-reported watermark and offset.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.last_watermark_ms(),
        Some(reported_watermark_ms),
        "task watermark must be updated from heartbeat"
    );
    assert_eq!(
        task.last_source_offset(),
        Some(reported_offset.as_slice()),
        "task source offset must be updated from heartbeat"
    );

    // Job must still be Running (not re-submitted as Accepted/Pending).
    assert_eq!(
        coordinator
            .job_detail_snapshot(&job_id)
            .unwrap()
            .job()
            .state(),
        JobState::Running,
        "job must remain Running after re-attach"
    );
}

#[test]
fn streaming_reattach_does_not_affect_batch_tasks() {
    // A batch job's tasks must not be disturbed by streaming_task_states
    // arriving from an unrelated executor heartbeat.
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-bt").unwrap());

    let executor_id = ExecutorId::try_new("exec-bt-1").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job_id = JobId::try_new("job-bt-1").unwrap();
    let spec = JobSpec::new(job_id.clone(), "batch", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "s1")
            .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
    );
    coordinator.submit_job(spec).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_id = detail.stages()[0].tasks()[0].task_id().clone();
    let lease = coordinator.executor_snapshots()[0].lease_generation();

    // Heartbeat with a streaming_task_state referencing the batch task id.
    let heartbeat = ExecutorHeartbeat::new(executor_id, ExecutorState::Healthy)
        .with_lease_generation(lease)
        .with_streaming_task_states(vec![StreamingTaskState::new(
            task_id.clone(),
            9999,
            vec![],
        )]);
    coordinator.executor_heartbeat(heartbeat).unwrap();

    // The watermark is applied (apply_streaming_state is task-kind-agnostic).
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.last_watermark_ms(),
        Some(9999),
        "apply_streaming_state is task-agnostic; the coordinator applies it if IDs match"
    );
    // Task state must be unchanged by the heartbeat (still Assigned until an
    // executor status update transitions it to Running).
    assert_eq!(task.state(), TaskState::Assigned);
}

#[test]
fn validate_job_rejects_unknown_upstream_stage() {
    let job_id = JobId::try_new("job-1").unwrap();
    let spec = JobSpec::new(job_id, "bad upstream", JobKind::Batch).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "stage1")
            .with_upstream_stage(StageId::try_new("ghost-stage").unwrap())
            .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
    );
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-1").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            1,
        ))
        .unwrap();
    let result = coordinator.submit_job(spec);
    assert!(
        matches!(result, Err(SchedulerError::InvalidJob { .. })),
        "expected InvalidJob, got {result:?}"
    );
}

#[test]
fn validate_job_rejects_legacy_fragment_in_durable_profile() {
    use krishiv_common::DurabilityProfile;
    use krishiv_plan::validate_job_fragments;
    let job_id = JobId::try_new("job-legacy-frag").unwrap();
    let spec = JobSpec::new(job_id, "ns", JobKind::Streaming).with_stage(
        StageSpec::new(StageId::try_new("stage-1").unwrap(), "s").with_task(TaskSpec::new(
            TaskId::try_new("task-1").unwrap(),
            "stream:tw:key=u",
        )),
    );
    let err = validate_job_fragments(&spec, DurabilityProfile::SingleNodeDurable)
        .expect_err("legacy fragment must fail in durable profile");
    assert!(err.to_string().contains("legacy untyped"), "got {err}");
}

#[test]
fn validate_job_rejects_empty_namespace() {
    use crate::job::validate_job;
    let job_id = JobId::try_new("job-empty-ns").unwrap();
    let spec = JobSpec::new(job_id, "ns", JobKind::Batch)
        .with_namespace("")
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t")),
        );
    let err = validate_job(&spec).expect_err("empty namespace must fail");
    assert!(format!("{err:?}").contains("namespace_id"), "got {err:?}");
}

#[test]
fn validate_job_rejects_oversized_namespace() {
    use crate::job::validate_job;
    let job_id = JobId::try_new("job-big-ns").unwrap();
    let long_ns = "a".repeat(300);
    let spec = JobSpec::new(job_id, "ns", JobKind::Batch)
        .with_namespace(long_ns)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t")),
        );
    let err = validate_job(&spec).expect_err("oversized namespace must fail");
    assert!(format!("{err:?}").contains("253"), "got {err:?}");
}

#[test]
fn validate_job_rejects_zero_checkpoint_interval() {
    use crate::job::validate_job;
    let job_id = JobId::try_new("job-zero-cp").unwrap();
    let spec = JobSpec::new(job_id, "ns", JobKind::Streaming)
        .with_checkpoint(0, "/tmp/cp")
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "s")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t")),
        );
    let err = validate_job(&spec).expect_err("zero checkpoint interval must fail");
    assert!(
        format!("{err:?}").contains("checkpoint_interval_ms"),
        "got {err:?}"
    );
}

#[test]
fn validate_job_accepts_valid_upstream_stage() {
    let job_id = JobId::try_new("job-2").unwrap();
    let spec = JobSpec::new(job_id, "good upstream", JobKind::Batch)
        .with_stage(
            StageSpec::new(StageId::try_new("stage-1").unwrap(), "producer")
                .with_task(TaskSpec::new(TaskId::try_new("task-1").unwrap(), "t1")),
        )
        .with_stage(
            StageSpec::new(StageId::try_new("stage-2").unwrap(), "consumer")
                .with_upstream_stage(StageId::try_new("stage-1").unwrap())
                .with_task(TaskSpec::new(TaskId::try_new("task-2").unwrap(), "t2")),
        );
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("coord-2").unwrap());
    coordinator
        .register_executor(ExecutorDescriptor::new(
            ExecutorId::try_new("exec-1").unwrap(),
            "pod-a",
            2,
        ))
        .unwrap();
    assert!(coordinator.submit_job(spec).is_ok());
}

