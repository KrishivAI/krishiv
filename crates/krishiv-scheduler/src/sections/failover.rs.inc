// ── Executor failover ─────────────────────────────────────────────────────

#[test]
fn executor_failover_reassigns_task_to_surviving_executor() {
    allow_anonymous_for_tests();
    let exec_a = ExecutorId::try_new("failover-exec-a").unwrap();
    let exec_b = ExecutorId::try_new("failover-exec-b").unwrap();
    let job_id = JobId::try_new("failover-job").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("failover-coord").unwrap());

    for id in [&exec_a, &exec_b] {
        coordinator
            .register_executor(ExecutorDescriptor::new(id.clone(), "pod", 4))
            .unwrap();
    }

    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    // Assign task and launch it so it has an assigned executor.
    let assignments = coordinator
        .launch_assigned_task_assignments(&job_id)
        .unwrap();
    assert_eq!(
        assignments.len(),
        1,
        "single-task job must produce one assignment"
    );
    let lost = assignments[0].executor_id().clone();
    let survivor = if lost == exec_a { exec_b } else { exec_a };

    // Mark the task Running before losing its executor. Without this the task
    // is merely Assigned, `reset_running_tasks_for_lost_executor` matches
    // nothing, and the assertion below would be measuring an untouched
    // assignment rather than a failover — which is how the original version of
    // this test passed while accepting `Assigned || Pending` on either
    // executor.
    let lease = coordinator
        .executor_snapshots()
        .iter()
        .find(|e| e.executor_id() == &lost)
        .unwrap()
        .lease_generation();
    coordinator
        .apply_task_update(
            TaskStatusUpdate::new(
                job_id.clone(),
                assignments[0].stage_id().clone(),
                assignments[0].task_id().clone(),
                lost.clone(),
                TaskState::Running,
                assignments[0].attempt_id().as_u32(),
            )
            .with_lease_generation(lease),
        )
        .unwrap();

    // The executor goes away for real: it must leave the placement pool, or
    // "reassigns to a surviving executor" is not what is being tested.
    coordinator.mark_executor_lost(&lost).unwrap();

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert_eq!(
        task.state(),
        TaskState::Assigned,
        "a task whose executor was lost must be re-placed while a healthy one remains"
    );
    assert_eq!(
        task.assigned_executor(),
        Some(&survivor),
        "and re-placed on the survivor, not left pointing at the lost executor"
    );
}

/// Phase 58 streaming×shuffle-kill regression: a chaos `shuffle-kill` is
/// `kubectl delete pod` = SIGTERM, so the killed executor SELF-reports its
/// continuous task `Failed` in ~25ms — far ahead of the heartbeat timeout
/// that drives `reset_running_tasks_for_lost_executor`. Without the
/// fast-path rescue, `refresh_state` (which has no streaming exception for a
/// Failed stage) drove the job terminal and the next push got `unknown job`.
/// The rescue must keep the job Running below the loss budget, and the
/// budget must still fail a task that dies every single time it runs.
#[test]
fn fast_failed_continuous_task_is_rescued_not_terminal() {
    allow_anonymous_for_tests();
    use crate::coordinator::executor_ops::MAX_EXECUTOR_LOSSES_BEFORE_FAIL as MAX_LOSSES;

    let exec_a = ExecutorId::try_new("rescue-exec-a").unwrap();
    let exec_b = ExecutorId::try_new("rescue-exec-b").unwrap();
    let job_id = JobId::try_new("rescue-stream-job").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("rescue-coord").unwrap());
    // No stage retries: the ordinary retry ladder must not absorb the
    // failures, so the rescue path is the only thing between one fast
    // executor loss and a terminal streaming job — the production shape.
    coordinator.config = CoordinatorConfig::new(0, 9);
    for id in [&exec_a, &exec_b] {
        coordinator
            .register_executor(ExecutorDescriptor::new(id.clone(), "pod", 4))
            .unwrap();
    }

    let spec = JobSpec::new(job_id.clone(), "continuous-streaming", JobKind::Streaming)
        .with_stage(
            StageSpec::new(
                StageId::try_new("stage-streaming").unwrap(),
                "continuous-streaming",
            )
            .with_task(TaskSpec::new(
                TaskId::try_new("task-streaming").unwrap(),
                format!("stream:loop:{}|cycle", job_id.as_str()),
            )),
        );
    coordinator.submit_job(spec).unwrap();

    for loss in 1..=MAX_LOSSES {
        // A `stream:loop:` task is input-driven: it only launches once a push
        // has prepared a cycle, so drive each round exactly like
        // `api_continuous_push` does.
        coordinator.assign_pending_tasks(&job_id).unwrap();
        coordinator
            .prepare_continuous_input_cycle(
                &job_id,
                vec![krishiv_proto::InputPartition::typed(
                    "continuous-input",
                    krishiv_proto::InputPartitionDescriptor::InlineIpc {
                        table_name: String::from("input"),
                        ipc_bytes: vec![1, 2, 3],
                    },
                )],
            )
            .unwrap();
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        assert_eq!(
            assignments.len(),
            1,
            "loss {loss}: the continuous task must be re-placed below the budget"
        );
        let executor = assignments[0].executor_id().clone();
        let stage_id = assignments[0].stage_id().clone();
        let task_id = assignments[0].task_id().clone();
        let attempt = assignments[0].attempt_id().as_u32();
        let lease = coordinator
            .executor_snapshots()
            .iter()
            .find(|e| e.executor_id() == &executor)
            .unwrap()
            .lease_generation();
        // Running, then the fast SIGTERM self-report — the path that used to
        // beat the heartbeat-based reset and terminate the whole job.
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id.clone(),
                    task_id.clone(),
                    executor.clone(),
                    TaskState::Running,
                    attempt,
                )
                .with_lease_generation(lease),
            )
            .unwrap();
        coordinator
            .apply_task_update(
                TaskStatusUpdate::new(
                    job_id.clone(),
                    stage_id,
                    task_id,
                    executor,
                    TaskState::Failed,
                    attempt,
                )
                .with_lease_generation(lease)
                .with_message("executor shutting down (SIGTERM)"),
            )
            .unwrap();

        let snapshot = coordinator
            .job_snapshot(&job_id)
            .expect("the job must stay in the registry, not be evicted mid-budget");
        if loss < MAX_LOSSES {
            assert!(
                !snapshot.state().is_terminal(),
                "loss {loss} of {MAX_LOSSES}: one fast executor loss must not terminate the streaming job"
            );
            let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
            let task = &detail.stages()[0].tasks()[0];
            assert_ne!(
                task.state(),
                TaskState::Failed,
                "loss {loss}: the continuous task must be rescued for reassignment"
            );
        } else {
            assert_eq!(
                snapshot.state(),
                JobState::Failed,
                "the loss budget must still fail a repeatedly dying continuous job"
            );
        }
    }
}

#[test]
fn executor_max_losses_permanently_fails_task() {
    allow_anonymous_for_tests();
    use crate::coordinator::executor_ops::MAX_EXECUTOR_LOSSES_BEFORE_FAIL as MAX_LOSSES;

    let exec_id = ExecutorId::try_new("loss-exec").unwrap();
    let job_id = JobId::try_new("loss-job").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("loss-coord").unwrap());

    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-loss", 4))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    // Drive exactly the threshold. The previous version looped a hardcoded 5
    // times with a `break` on "no assignments — task might already be Failed",
    // so it could pass having driven a single loss; it proved only that the
    // task ends Failed *eventually*, not that the threshold is where it says.
    for loss in 1..=MAX_LOSSES {
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        assert_eq!(
            assignments.len(),
            1,
            "loss {loss}: the task must still be re-placed below the threshold"
        );
        let exec_for_task = assignments[0].executor_id().clone();
        coordinator.reset_running_tasks_for_lost_executor(&exec_for_task);

        let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
        let state = detail.stages()[0].tasks()[0].state();
        if loss < MAX_LOSSES {
            assert_ne!(
                state,
                TaskState::Failed,
                "loss {loss} of {MAX_LOSSES}: the task must survive below the threshold"
            );
        }
    }

    let jc = coordinator.job_coordinator(&job_id).unwrap();
    let record = jc.read_record();
    let task = &record.stages()[0].tasks()[0];
    assert_eq!(
        task.state(),
        TaskState::Failed,
        "the task must be permanently Failed at {MAX_LOSSES} consecutive executor losses"
    );
    assert_eq!(
        task.executor_loss_count, MAX_LOSSES,
        "and the recorded loss count must be exactly the threshold"
    );
}
