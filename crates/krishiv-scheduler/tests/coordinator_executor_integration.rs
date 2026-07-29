//! In-process coordinator → executor integration (GAP-T2).

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use krishiv_proto::{
    ExecutorDescriptor, ExecutorHeartbeat, ExecutorId, ExecutorState, LeaseGeneration,
};
use krishiv_scheduler::Coordinator;

#[test]
fn coordinator_tick_after_executor_heartbeat() {
    let mut coordinator = Coordinator::new_active(None).unwrap();
    let executor_id = ExecutorId::try_new("exec-integration").unwrap();
    coordinator
        .register_executor(ExecutorDescriptor::new(executor_id.clone(), "localhost", 2))
        .unwrap();

    coordinator
        .executor_heartbeat(
            ExecutorHeartbeat::new(executor_id.clone(), ExecutorState::Healthy)
                .with_lease_generation(LeaseGeneration::initial()),
        )
        .unwrap();

    coordinator.coordinator_tick().unwrap();

    assert!(
        coordinator
            .executor_snapshots()
            .iter()
            .any(|e| e.executor_id() == &executor_id && e.state() == ExecutorState::Healthy)
    );
}
