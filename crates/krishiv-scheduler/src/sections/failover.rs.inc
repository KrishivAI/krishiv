// ── Executor failover ─────────────────────────────────────────────────────

#[test]
fn executor_failover_reassigns_task_to_surviving_executor() {
    allow_anonymous_for_tests();
    let exec_a = ExecutorId::try_new("failover-exec-a").unwrap();
    let exec_b = ExecutorId::try_new("failover-exec-b").unwrap();
    let job_id = JobId::try_new("failover-job").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("failover-coord").unwrap());

    let _lease_a = coordinator
        .register_executor(ExecutorDescriptor::new(exec_a.clone(), "pod-a", 4))
        .unwrap();
    let _lease_b = coordinator
        .register_executor(ExecutorDescriptor::new(exec_b.clone(), "pod-b", 4))
        .unwrap();

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
    let assigned_exec = assignments[0].executor_id().clone();

    // The task is in-flight on `assigned_exec`. Simulate that executor going lost.
    coordinator.reset_running_tasks_for_lost_executor(&assigned_exec);

    // After reset, the task should be pending/assigned on the surviving executor.
    // reset_running_tasks_for_lost_executor calls assign_pending_tasks internally.
    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task = &detail.stages()[0].tasks()[0];
    assert!(
        task.state() == TaskState::Assigned || task.state() == TaskState::Pending,
        "task must be re-queued (Pending or Assigned) after executor loss, got {:?}",
        task.state()
    );
}

#[test]
fn executor_max_losses_permanently_fails_task() {
    allow_anonymous_for_tests();
    const MAX_LOSSES: u32 = 5;
    let exec_id = ExecutorId::try_new("loss-exec").unwrap();
    let job_id = JobId::try_new("loss-job").unwrap();
    let mut coordinator = Coordinator::active(CoordinatorId::try_new("loss-coord").unwrap());

    coordinator
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-loss", 4))
        .unwrap();
    coordinator
        .submit_job(single_task_job(job_id.clone()))
        .unwrap();

    // Simulate MAX_LOSSES consecutive executor losses.
    for i in 0..MAX_LOSSES {
        // Launch to get an assignment so the task is in-flight.
        let assignments = coordinator
            .launch_assigned_task_assignments(&job_id)
            .unwrap();
        if assignments.is_empty() {
            // Task might already be Failed at this point.
            break;
        }
        let exec_for_task = assignments[0].executor_id().clone();
        let _ = i;
        coordinator.reset_running_tasks_for_lost_executor(&exec_for_task);
    }

    let detail = coordinator.job_detail_snapshot(&job_id).unwrap();
    let task_state = detail.stages()[0].tasks()[0].state();
    assert_eq!(
        task_state,
        TaskState::Failed,
        "task must be permanently Failed after {MAX_LOSSES} consecutive executor losses"
    );
}

