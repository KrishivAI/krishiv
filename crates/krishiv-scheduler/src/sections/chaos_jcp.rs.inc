// ── Per-job coordinator (JCP) ownership surfaces ─────────────────────────────
//
// This file used to hold ~45 "chaos" tests that exercised only
// `MiniSimulationHarness` — a test double — and never reached production code.
// Their own comments said so ("In real flow this would be…", "the harness
// models the conditions…", "In the integrated system this would have…").
//
// Four things were wrong with them, and each is the exact failure mode this
// audit exists to find:
//
//   * **Tautologies.** The dominant assertion was
//     `assert!(!h.is_partitioned(&x) || h.current_tick() > 10)` after a
//     `simulate_partition_and_recovery` and 14 ticks. Both disjuncts are true
//     by construction; the assertion could not fail for any implementation.
//   * **Assertions on the harness's own setters.** `h.partition(x)` followed
//     by `assert!(h.is_partitioned(&x))` tests the double, not the scheduler.
//   * **Verbatim duplicates.** Twelve copies of one body, identical down to
//     the executor-id string, named `_stress` and `_stress_v2` … `_v12`.
//   * **Names that promised what the body never did.**
//     `job_coordinator_clear_assignments_for_bad_executor_works` never called
//     `clear_assignments_for_bad_executor`; it asserted `jc.job_id()`.
//     `prr_new_surfaces_all_green_…` had an empty body.
//
// They also hid un-awaited futures. Every JCP accessor here is `async`, so
// `let _eligible = jc.has_tasks_eligible_for_launch();` in a sync `#[test]`
// builds a future and drops it unpolled. This file already documented that
// bug being found and fixed in **one** place (see the note that survives on
// `job_coordinator_launch_eligibility_…` below) — and left seven more, hidden
// from `clippy::let_underscore_future` because a *named* `_eligible` binding
// does not trip a lint that only fires on the bare `_` pattern. The lint was
// satisfied by renaming rather than by awaiting.
//
// Replaced with tests that call the JCP surfaces those tests named and assert
// what they actually do. Fewer tests, but each one can fail.

/// Build a `JobCoordinator` over a one-task job, with that task assigned to
/// `executor` and its launch guard raised — the state the recovery paths below
/// are meant to unwind.
fn jcp_with_assigned_task(
    job: &str,
    executor: &ExecutorId,
) -> crate::job_coordinator::JobCoordinator {
    let job_id = JobId::try_new(job).unwrap();
    let record = crate::job::JobRecord::from_spec(single_task_job(job_id.clone()), 0);
    let jc = crate::job_coordinator::JobCoordinator::new(job_id, record);
    {
        let mut rec = jc.write_record();
        for task in rec.stages_mut().iter_mut().flat_map(|s| s.tasks_mut()) {
            task.state = TaskState::Assigned;
            task.assigned_executor = Some(executor.clone());
            task.launch_in_flight = true;
        }
    }
    jc
}

/// The method the deleted `..._clear_assignments_for_bad_executor_works` was
/// named after, actually called.
#[tokio::test]
async fn job_coordinator_clear_assignments_for_bad_executor_detaches_its_tasks() {
    let bad = ExecutorId::try_new("jcp-clear-bad").unwrap();
    let other = ExecutorId::try_new("jcp-clear-other").unwrap();
    let jc = jcp_with_assigned_task("jcp-clear-test", &bad);

    // A different executor's id must not touch this job's tasks.
    assert_eq!(
        jc.clear_assignments_for_bad_executor_and_count(&other).await,
        0,
        "clearing an unrelated executor must not detach anything"
    );
    assert_eq!(
        jc.read_record().stages()[0].tasks()[0].assigned_executor(),
        Some(&bad)
    );

    assert_eq!(
        jc.clear_assignments_for_bad_executor_and_count(&bad).await,
        1
    );
    let rec = jc.read_record();
    let task = &rec.stages()[0].tasks()[0];
    assert_eq!(
        task.assigned_executor(),
        None,
        "the task must be detached so the launch loop stops targeting the bad executor"
    );
    assert!(
        !task.launch_in_flight,
        "the launch guard must reopen or the task is stranded behind it forever"
    );
    // Documenting real behaviour, not aspiration: this method detaches, it does
    // not re-queue. The task keeps its Assigned state and it is the
    // coordinator's placement round that moves it on.
    assert_eq!(task.state(), TaskState::Assigned);
}

/// `handle_executor_loss` is `clear_assignments_…_and_count` **plus** forgetting
/// the executor's heartbeat timestamp. The count is the easy half; the forget is
/// the half that has no other observer, so it is asserted through the staleness
/// seam that reads the same map.
#[tokio::test]
async fn job_coordinator_handle_executor_loss_also_forgets_the_heartbeat() {
    let bad = ExecutorId::try_new("jcp-loss-bad").unwrap();
    let jc = jcp_with_assigned_task("jcp-loss-test", &bad).with_stale_threshold(10_000);

    // Establish a heartbeat, then confirm a prompt follow-up reads as fresh.
    assert!(
        jc.record_heartbeat_and_detect_stale(&bad, 1_000_000),
        "the first heartbeat from an unknown executor is stale by definition"
    );
    assert!(
        !jc.record_heartbeat_and_detect_stale(&bad, 1_001_000),
        "1s later with a 10s threshold is fresh"
    );

    assert_eq!(jc.handle_executor_loss(&bad).await, 1, "one task detached");

    assert!(
        jc.record_heartbeat_and_detect_stale(&bad, 1_002_000),
        "losing an executor must forget its heartbeat, so the next one is \
         treated as a first contact rather than measured against a timestamp \
         from before the loss"
    );
}

/// The staleness seam honours its configured threshold in both directions.
#[test]
fn job_coordinator_heartbeat_staleness_uses_the_configured_threshold() {
    let exec = ExecutorId::try_new("jcp-stale-exec").unwrap();
    let job_id = JobId::try_new("jcp-stale-test").unwrap();
    let record = crate::job::JobRecord::from_spec(single_task_job(job_id.clone()), 0);
    let jc =
        crate::job_coordinator::JobCoordinator::new(job_id, record).with_stale_threshold(5_000);

    assert!(jc.record_heartbeat_and_detect_stale(&exec, 100_000));
    assert!(
        !jc.record_heartbeat_and_detect_stale(&exec, 105_000),
        "exactly at the threshold is not yet stale"
    );
    assert!(
        jc.record_heartbeat_and_detect_stale(&exec, 110_001),
        "past the threshold must report stale"
    );
    // A clock that jumps backwards must not read as stale — saturating_sub
    // makes the gap 0, which is the safe answer.
    assert!(!jc.record_heartbeat_and_detect_stale(&exec, 100));
}

/// Launch eligibility gates on assignment, the launch guard, and terminal
/// state. The `.await` here is load-bearing: this method is `async`, and the
/// version of this test that shipped first wrote
/// `let _ = jc.has_tasks_eligible_for_launch();`, which built a future and
/// dropped it without polling — it claimed to prove the surface "real and
/// callable" while executing none of it, and would have passed with the body
/// replaced by `unreachable!()`.
#[tokio::test]
async fn job_coordinator_launch_eligibility_tracks_assignment_and_launch_guard() {
    let exec = ExecutorId::try_new("jcp-eligible-exec").unwrap();
    let job_id = JobId::try_new("jcp-eligible-test").unwrap();
    let record = crate::job::JobRecord::from_spec(single_task_job(job_id.clone()), 0);
    let jc = crate::job_coordinator::JobCoordinator::new(job_id, record);

    assert!(
        !jc.has_tasks_eligible_for_launch().await,
        "a job whose tasks are not assigned yet must report nothing eligible"
    );

    {
        let mut rec = jc.write_record();
        let task = &mut rec.stages_mut()[0].tasks_mut()[0];
        task.state = TaskState::Assigned;
        task.assigned_executor = Some(exec.clone());
    }
    assert!(
        jc.has_tasks_eligible_for_launch().await,
        "an assigned task with no launch in flight is eligible"
    );

    jc.write_record().stages_mut()[0].tasks_mut()[0].launch_in_flight = true;
    assert!(
        !jc.has_tasks_eligible_for_launch().await,
        "a launch already in flight must not be re-dispatched"
    );

    {
        let mut rec = jc.write_record();
        let task = &mut rec.stages_mut()[0].tasks_mut()[0];
        task.launch_in_flight = false;
        task.state = TaskState::Running;
    }
    assert!(
        !jc.has_tasks_eligible_for_launch().await,
        "a Running task is not awaiting launch"
    );
}

/// `get_launch_work_summary` returns (eligible tasks, stages carrying work).
#[tokio::test]
async fn job_coordinator_launch_work_summary_counts_eligible_tasks_and_stages() {
    let exec = ExecutorId::try_new("jcp-summary-exec").unwrap();
    let job_id = JobId::try_new("jcp-summary-test").unwrap();
    let record = crate::job::JobRecord::from_spec(single_task_job(job_id.clone()), 0);
    let jc = crate::job_coordinator::JobCoordinator::new(job_id, record);

    assert_eq!(
        jc.get_launch_work_summary().await,
        (0, 0),
        "nothing assigned yet"
    );

    {
        let mut rec = jc.write_record();
        let task = &mut rec.stages_mut()[0].tasks_mut()[0];
        task.state = TaskState::Assigned;
        task.assigned_executor = Some(exec);
    }
    assert_eq!(jc.get_launch_work_summary().await, (1, 1));
}

/// The Track E limits seam: a budget declared on the `JobSpec` must be readable
/// through the per-job coordinator, because that is where a launch site asks
/// for it. This is the one real assertion salvaged from the deleted
/// "ideal state" chaos test.
#[tokio::test]
async fn job_coordinator_udf_resource_limits_carry_the_job_spec_budget() {
    let job_id = JobId::try_new("jcp-limits-test").unwrap();
    let spec = single_task_job(job_id.clone()).with_memory_limit_bytes(256 * 1024 * 1024);
    let record = crate::job::JobRecord::from_spec(spec, 0);
    let jc = crate::job_coordinator::JobCoordinator::new(job_id, record);

    let (time_cap, mem_limit) = jc.udf_resource_limits().await;
    assert_eq!(mem_limit, Some(256 * 1024 * 1024));
    assert_eq!(mem_limit, jc.udf_memory_limit_bytes().await);
    assert_eq!(time_cap, jc.udf_execution_time_cap_ms().await);
}

#[test]
fn job_record_exposes_raw_udf_limits_for_track_e_seam() {
    // Track E: the scheduler-native raw accessors on JobRecord are the
    // boundary-safe seam for deriving ResourceLimits in higher layers
    // (krishiv-sql / executor runner) without pulling udf types into scheduler.
    let job_id = JobId::try_new("udf-limits-seam").unwrap();
    let spec = single_task_job(job_id.clone());
    let job = crate::job::JobRecord::from_spec(spec, 0);

    let time_cap = job.udf_execution_time_cap_ms();
    assert!(time_cap.is_some() && time_cap.unwrap() > 0);
    // memory may be None (unlimited) for the test job — that's valid.
    let _ = job.udf_memory_limit_bytes();
}

#[test]
fn chaos_speculative_execution_stale_lease_rejected() {
    let executor_id = ExecutorId::try_new("exec-speculative").unwrap();
    let mut coordinator =
        Coordinator::active(CoordinatorId::try_new("coord-speculative").unwrap());

    // 1. First registration: executor joins the cluster, receives LeaseGeneration (e.g. G1)
    let lease_g1 = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    let job = demo_job();
    let job_id = job.job_id().clone();
    let stage_id = StageId::try_new("stage-1").unwrap();
    let task_id = TaskId::try_new("task-1").unwrap();

    coordinator.submit_job(job).unwrap();
    coordinator.launch_assigned_tasks(&job_id).unwrap();

    // 2. Simulated crash/slow network recovery: Executor re-registers, advancing its lease generation to G2.
    let lease_g2 = coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "pod-a", 2))
        .unwrap();

    assert!(
        lease_g2.as_u64() > lease_g1.as_u64(),
        "Lease generation must bump upon re-registration"
    );

    // 3. Stale commit attempt: The slow/stale executor attempt from the first lease generation (G1)
    // attempts to report task success. It uses lease_g1.
    let stale_update = TaskStatusUpdate::new(
        job_id.clone(),
        stage_id.clone(),
        task_id.clone(),
        executor_id.clone(),
        TaskState::Succeeded,
        1,
    )
    .with_lease_generation(lease_g1);

    // This update MUST be rejected by the coordinator because of the stale lease generation!
    let outcome = coordinator.apply_task_update(stale_update);
    assert!(
        outcome.is_err(),
        "Stale update from G1 must be rejected after re-registration to G2"
    );

    let err = outcome.unwrap_err();
    assert!(
        matches!(err, SchedulerError::StaleExecutorLease { .. }),
        "Expected StaleExecutorLease error, got: {:?}",
        err
    );

    // 4. Valid commit attempt: The executor under the new lease generation G2 commits successfully.
    let valid_update = TaskStatusUpdate::new(
        job_id.clone(),
        stage_id,
        task_id,
        executor_id,
        TaskState::Succeeded,
        1,
    )
    .with_lease_generation(lease_g2);

    let valid_outcome = coordinator.apply_task_update(valid_update).unwrap();
    assert_eq!(valid_outcome, TaskUpdateOutcome::Applied);
}
