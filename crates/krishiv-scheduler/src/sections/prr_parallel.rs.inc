// ── Coordinator change notification ──────────────────────────────────────────
//
// This file used to hold four "PRR failure-mode" tests, a `MiniSimulationHarness`
// with five self-tests, and `real_job_coordinator_extraction`. None of them
// reached a behaviour worth asserting:
//
//   * `frozen_executor_detected_via_missing_progress` and
//     `slow_frozen_executor_detected_by_progress_stall` were the same test
//     twice — register an executor, assert it is registered. Both comments
//     conceded the point ("In a real system we would check
//     StreamingProgressSnapshot staleness"; "this test documents the scenario").
//   * `duplicate_task_assignment_after_partition_is_limited` and
//     `network_partition_causes_lease_bump_and_task_replay` were also the same
//     test twice — register twice, assert the lease bumped. Neither assigned a
//     task, so neither could observe a duplicate assignment or a replay.
//     `lost_executor_can_reregister_with_next_lease_generation` in
//     `core.rs.inc` already covers the lease bump properly.
//   * `real_job_coordinator_extraction` asserted that a string it had just
//     built equalled itself.
//   * `MiniSimulationHarness` was a test double whose only remaining consumers,
//     once the `chaos_jcp` suite was deleted, were its own five self-tests — a
//     closed loop with no production coverage.
//
// What survives is the one test that drove real code, with the assertion it
// was missing.

/// `notify_waiters()` must wake a waiter that is already parked.
///
/// **The waiter is registered first, and that ordering is the whole test.**
/// Tokio's `Notify::notify_waiters()` wakes only waiters already registered and
/// stores no permit — unlike `notify_one()` — so a notification fired before
/// anyone is waiting is dropped on the floor. Every producer in this crate uses
/// `notify_waiters()`, and `SharedCoordinator::wait_for_change` consumes it with
/// `notified().await`.
///
/// The version that shipped had the ordering backwards: it registered the
/// executor, *then* built the wait future, then wrote
/// `let _ = timeout(100ms, wait).await`. Its comment claimed "the wait helper
/// should return promptly because a notification was already sent" — which is
/// the opposite of what `notify_waiters()` guarantees. So it always timed out,
/// and `let _ =` swallowed the `Err(Elapsed)`. Deleting the `notify_waiters()`
/// call from `register_executor` outright left it green.
///
/// `biased` makes this deterministic rather than timing-dependent: select polls
/// the wait arm first, so the waiter is parked before the trigger arm starts.
#[tokio::test]
async fn notify_wakes_a_parked_waiter_on_executor_registration() {
    use std::time::Duration;

    let coordinator = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("notify-coord").unwrap(),
    ));
    let exec_id = ExecutorId::try_new("notify-test-exec").unwrap();
    let desc = ExecutorDescriptor::new(exec_id.clone(), "pod-notify", 2);

    let mut woken = false;
    tokio::select! {
        biased;
        () = coordinator.wait_for_change() => woken = true,
        () = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            coordinator
                .write()
                .await
                .register_executor(desc)
                .expect("register should succeed");
            tokio::time::sleep(Duration::from_secs(2)).await;
        } => {}
    }
    assert!(
        woken,
        "registering an executor must wake a parked `wait_for_change` waiter, \
         or the daemon sleeps through the state change until its next timer tick"
    );
}

/// Same contract for the departure half of the executor lifecycle.
#[tokio::test]
async fn notify_wakes_a_parked_waiter_on_executor_deregistration() {
    use std::time::Duration;

    let coordinator = SharedCoordinator::new(Coordinator::active(
        CoordinatorId::try_new("notify-coord-dereg").unwrap(),
    ));
    let exec_id = ExecutorId::try_new("notify-dereg-exec").unwrap();
    let lease = coordinator
        .write()
        .await
        .register_executor(ExecutorDescriptor::new(exec_id.clone(), "pod-notify", 2))
        .expect("register should succeed");

    let mut woken = false;
    tokio::select! {
        biased;
        () = coordinator.wait_for_change() => woken = true,
        () = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            coordinator
                .write()
                .await
                .deregister_executor(&exec_id, lease)
                .expect("deregister should succeed");
            tokio::time::sleep(Duration::from_secs(2)).await;
        } => {}
    }
    assert!(
        woken,
        "deregistering an executor must wake a parked `wait_for_change` waiter"
    );
}
