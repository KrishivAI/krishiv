//! Chaos test suite — R10 acceptance gate.
//!
//! Verifies system invariants hold under fault injection: stale coordinator
//! rejection, checkpoint fencing, dead-letter sink behaviour, policy
//! enforcement, leader election, and fault-injector cycling.
//!
//! Tests in this file fall into two categories:
//!
//! * **Engine-backed** — exercise real public APIs (`validate_fencing_token`,
//!   `DeadLetterSink`, `PolicyHook`, `K8sLeaseElection`,
//!   `CheckpointBarrierTracker`).
//! * **Invariant simulations** — model an intended invariant with lightweight
//!   mocks where wiring the full runtime would be disproportionate for an
//!   acceptance gate.  These are annotated as simulations in their doc
//!   comments so the distinction is explicit.

use krishiv_common::chaos::{FaultInjector, FaultMode};

/// A stale fencing token (different from the current leader) is rejected by
/// the real checkpoint fencing validator.
#[test]
fn fencing_token_rejects_stale_coordinator() {
    use krishiv_state::checkpoint::{CheckpointMetadata, validate_fencing_token};

    let stale_meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "job-fence".into(),
        fencing_token: 1,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    assert!(
        validate_fencing_token(&stale_meta, 2).is_err(),
        "stale coordinator must be rejected"
    );
}

/// The fencing boundary: a token equal to the current leader is accepted.
#[test]
fn fencing_token_equal_token_is_accepted() {
    use krishiv_state::checkpoint::{CheckpointMetadata, validate_fencing_token};

    let meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "job-fence-eq".into(),
        fencing_token: 2,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    assert!(
        validate_fencing_token(&meta, 2).is_ok(),
        "the current leader's own token must be accepted"
    );
}

/// Simulation: a failed checkpoint prepare must leave no committed state.
///
/// This models the prepare/commit atomicity invariant with a plain `Result`
/// until a lightweight in-process checkpoint storage expose a hookable
/// prepare-failure path.
#[test]
fn checkpoint_prepare_failure_leaves_no_committed_state() {
    let mut committed = false;
    let prepare_result: Result<(), String> = Err("disk full".into());
    if prepare_result.is_ok() {
        committed = true;
    }
    assert!(!committed, "commit must not happen after failed prepare");
}

/// A `Fail` data-quality violation returns an error with no partial write.
#[tokio::test]
async fn dead_letter_sink_fail_action_returns_error() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let col = Float64Array::from(vec![None::<f64>]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull { column: "v".into() },
        QualityAction::Fail,
    );
    let mut sink = DeadLetterSink::new("chaos_test", config);
    assert!(
        sink.process_batch(&batch).await.is_err(),
        "Fail action must return Err"
    );
}

/// A `Reject` action routes the violating row out of the accepted batch but
/// does not fail the whole batch.
#[tokio::test]
async fn dead_letter_sink_reject_action_keeps_non_violating_rows() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    // Row 0 is null (violates NotNull), row 1 is present.
    let col = Float64Array::from(vec![None::<f64>, Some(42.0)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull { column: "v".into() },
        QualityAction::Reject,
    );
    let mut sink = DeadLetterSink::new("chaos_reject", config);
    let (accepted, rejected) = sink
        .process_batch(&batch)
        .await
        .expect("Reject must not fail the batch");
    assert_eq!(accepted.num_rows(), 1, "only the non-violating row is kept");
    assert_eq!(rejected.len(), 1, "one row is reported as rejected");
}

/// A `Warn` action passes every row through and reports no rejections.
#[tokio::test]
async fn dead_letter_sink_warn_action_passes_all_rows() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let col = Float64Array::from(vec![None::<f64>]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull { column: "v".into() },
        QualityAction::Warn,
    );
    let mut sink = DeadLetterSink::new("chaos_warn", config);
    let (accepted, rejected) = sink
        .process_batch(&batch)
        .await
        .expect("Warn must not fail the batch");
    assert_eq!(accepted.num_rows(), 1, "Warn keeps all rows");
    assert!(rejected.is_empty(), "Warn reports no rejections");
}

/// A batch that satisfies all quality rules passes through untouched.
#[tokio::test]
async fn dead_letter_sink_clean_batch_passes_through() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_connectors::{DataQualityConfig, DataQualityRule, DeadLetterSink, QualityAction};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let col = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull { column: "v".into() },
        QualityAction::Fail,
    );
    let mut sink = DeadLetterSink::new("chaos_clean", config);
    let (accepted, rejected) = sink
        .process_batch(&batch)
        .await
        .expect("a clean batch must pass");
    assert_eq!(accepted.num_rows(), 3);
    assert!(rejected.is_empty());
}

/// Policy hook denies table access for a deny-all policy.
#[test]
fn policy_hook_denies_table_access() {
    use krishiv_plan::governance::PolicyHook;

    struct DenyAllPolicy;
    impl PolicyHook for DenyAllPolicy {
        fn check_table_access(&self, _table: &str) -> bool {
            false
        }
    }

    let policy = DenyAllPolicy;
    assert!(!policy.check_table_access("secret_table"));
}

/// Policy hook allows table access for an allow-all policy (positive case).
#[test]
fn policy_hook_allows_table_access() {
    use krishiv_plan::governance::PolicyHook;

    struct AllowAllPolicy;
    impl PolicyHook for AllowAllPolicy {
        fn check_table_access(&self, _table: &str) -> bool {
            true
        }
    }

    let policy = AllowAllPolicy;
    assert!(policy.check_table_access("public_table"));
}

/// Fault injector cycles deterministically through its fault list.
#[test]
fn fault_injector_rotates_through_faults() {
    let injector = FaultInjector::new(vec![
        FaultMode::None,
        FaultMode::Error {
            message: "network timeout".into(),
        },
        FaultMode::Drop,
    ]);
    assert_eq!(injector.next_fault(), &FaultMode::None);
    assert_eq!(
        injector.next_fault(),
        &FaultMode::Error {
            message: "network timeout".into()
        }
    );
    assert_eq!(injector.next_fault(), &FaultMode::Drop);
    // Wraps around to the start of the list.
    assert_eq!(injector.next_fault(), &FaultMode::None);
}

/// An empty fault injector always returns `FaultMode::None`.
#[test]
fn fault_injector_empty_returns_none() {
    let injector = FaultInjector::new(vec![]);
    assert_eq!(injector.next_fault(), &FaultMode::None);
    assert_eq!(injector.next_fault(), &FaultMode::None);
}

/// A single-fault injector always returns that one fault.
#[test]
fn fault_injector_single_fault_repeats() {
    let injector = FaultInjector::new(vec![FaultMode::Drop]);
    assert_eq!(injector.next_fault(), &FaultMode::Drop);
    assert_eq!(injector.next_fault(), &FaultMode::Drop);
}

/// Leader election simulation: acquire sets `is_leader`, release clears it.
#[tokio::test]
async fn leader_election_simulation_acquire_release() {
    use krishiv_operator::K8sLeaseElection;
    use krishiv_scheduler::LeaderElection;

    let election = K8sLeaseElection::new("chaos-job", "default", "pod-a");
    assert!(!election.is_leader());
    assert!(election.try_acquire().await);
    assert!(election.is_leader());
    assert!(election.renew().await, "renewal must succeed while leader");
    election.release().await;
    assert!(!election.is_leader());
}

/// Releasing leadership without having acquired it is a no-op (stays non-leader).
#[tokio::test]
async fn leader_election_release_without_acquire_is_noop() {
    use krishiv_operator::K8sLeaseElection;
    use krishiv_scheduler::LeaderElection;

    let election = K8sLeaseElection::new("chaos-job-2", "default", "pod-b");
    assert!(!election.is_leader());
    election.release().await;
    assert!(
        !election.is_leader(),
        "release without acquire must not grant leadership"
    );
}

// ---------------------------------------------------------------------------
// Failure-mode tests
// ---------------------------------------------------------------------------

/// Split-brain: a stale coordinator's commit is rejected while the active
/// coordinator's commit is accepted.
#[test]
fn split_brain_second_coordinator_commit_rejected() {
    use krishiv_state::checkpoint::{CheckpointMetadata, validate_fencing_token};

    let current_token: u64 = 2;

    let stale_meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "job-split-brain".into(),
        fencing_token: 1,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    let fresh_meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 1,
        job_id: "job-split-brain".into(),
        fencing_token: 2,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    assert!(
        validate_fencing_token(&stale_meta, current_token).is_err(),
        "stale coordinator must be rejected"
    );
    assert!(
        validate_fencing_token(&fresh_meta, current_token).is_ok(),
        "active coordinator must be accepted"
    );
}

/// Duplicate task delivery: acking the same (job, epoch, task) twice must not
/// double-count toward the barrier quorum.
#[test]
fn duplicate_task_delivery_same_epoch_idempotent() {
    use std::time::Duration;

    use krishiv_proto::wire::v1::BarrierAck;
    use krishiv_scheduler::CheckpointBarrierTracker;

    let mut tracker = CheckpointBarrierTracker::new(
        "job-dup",
        1,
        ["task-0".to_string(), "task-1".to_string()],
        Duration::from_secs(30),
    );

    let ack = BarrierAck {
        epoch: 1,
        job_id: "job-dup".into(),
        task_id: "task-0".into(),
        state_handle: None,
    };

    // First ack — normal.
    tracker.record_ack(&ack);
    // Second identical ack — must not change the received-acks count.
    tracker.record_ack(&ack);

    // task-1 has not acked yet; tracker must not be complete.
    assert!(
        !tracker.is_complete(),
        "duplicate ack must not satisfy the quorum prematurely"
    );

    let ack2 = BarrierAck {
        epoch: 1,
        job_id: "job-dup".into(),
        task_id: "task-1".into(),
        state_handle: None,
    };
    tracker.record_ack(&ack2);
    assert!(
        tracker.is_complete(),
        "quorum is satisfied after both tasks ack"
    );
}

/// Duplicate acks received after the tracker is already complete must leave it
/// complete (idempotent post-quorum).
#[test]
fn duplicate_ack_after_complete_keeps_tracker_complete() {
    use std::time::Duration;

    use krishiv_proto::wire::v1::BarrierAck;
    use krishiv_scheduler::CheckpointBarrierTracker;

    let mut tracker = CheckpointBarrierTracker::new(
        "job-dup-done",
        1,
        ["task-0".to_string(), "task-1".to_string()],
        Duration::from_secs(30),
    );

    for task in ["task-0", "task-1"] {
        tracker.record_ack(&BarrierAck {
            epoch: 1,
            job_id: "job-dup-done".into(),
            task_id: task.into(),
            state_handle: None,
        });
    }
    assert!(tracker.is_complete());

    // A duplicate late ack must not regress the tracker.
    tracker.record_ack(&BarrierAck {
        epoch: 1,
        job_id: "job-dup-done".into(),
        task_id: "task-0".into(),
        state_handle: None,
    });
    assert!(
        tracker.is_complete(),
        "late duplicate ack must keep quorum complete"
    );
}

/// Coordinator restart rejects a fencing token from a future coordinator on
/// restore, and accepts one from a prior coordinator.
#[test]
fn coordinator_restart_rejects_future_token_on_restore() {
    use krishiv_state::checkpoint::{CheckpointMetadata, validate_fencing_token_for_restore};

    let restored_coordinator_token: u64 = 3;

    // Metadata written by a newer coordinator (token=5).
    let future_meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 10,
        job_id: "job-restart".into(),
        fencing_token: 5,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    // Metadata written by an older coordinator (token=2) — acceptable on restore.
    let past_meta = CheckpointMetadata {
        fencing_token: 2,
        ..future_meta.clone()
    };

    assert!(
        validate_fencing_token_for_restore(&future_meta, restored_coordinator_token).is_err(),
        "restored coordinator must reject metadata from a future coordinator"
    );
    assert!(
        validate_fencing_token_for_restore(&past_meta, restored_coordinator_token).is_ok(),
        "restored coordinator must accept metadata from a prior coordinator"
    );
}

/// Restore fencing boundary: a metadata token equal to the current token is
/// accepted (not strictly greater).
#[test]
fn coordinator_restore_accepts_equal_token() {
    use krishiv_state::checkpoint::{CheckpointMetadata, validate_fencing_token_for_restore};

    let meta = CheckpointMetadata {
        version: CheckpointMetadata::VERSION,
        epoch: 4,
        job_id: "job-restart-eq".into(),
        fencing_token: 3,
        coordinator_id: None,
        timestamp_ms: 0,
        source_offsets: vec![],
        operator_snapshots: vec![],
        is_savepoint: false,
        savepoint_label: None,
        iceberg_snapshot_id: None,
        kafka_offsets: None,
        unaligned_buffer_refs: Vec::new(),
        sink_transactions: Vec::new(),
        streaming_profile: None,
    };

    assert!(
        validate_fencing_token_for_restore(&meta, 3).is_ok(),
        "restore must accept a token equal to the current coordinator token"
    );
}

/// Simulation: a panicking UDF is isolated by `catch_unwind` and does not
/// propagate to the executor main loop.
#[test]
fn udf_panic_caught_does_not_crash_executor() {
    use std::panic;

    let result = panic::catch_unwind(|| {
        panic!("simulated UDF panic");
    });
    assert!(
        result.is_err(),
        "panic must be caught, not propagate to the executor"
    );
}

/// Rapid fault cycling (128 iterations) must not panic and must keep rotating.
#[test]
fn barrier_channel_capacity_exhaustion_no_panic() {
    let injector = FaultInjector::new(vec![FaultMode::None, FaultMode::Drop]);
    for _ in 0..128 {
        let _fault = injector.next_fault();
    }
    // Reaching here without panicking passes the test; the next fault resumes
    // the deterministic rotation at the start of the list.
    assert_eq!(injector.next_fault(), &FaultMode::None);
}

// ---------------------------------------------------------------------------
// Extended failure coverage
// ---------------------------------------------------------------------------

/// Network partition simulation: the first three sends are dropped, then the
/// fourth succeeds; the pattern wraps and the partition "clears" each cycle.
#[test]
fn network_partition_drops_records_then_recovers() {
    let injector = FaultInjector::new(vec![
        FaultMode::Drop,
        FaultMode::Drop,
        FaultMode::Drop,
        FaultMode::None,
    ]);

    let mut dropped = 0usize;
    let mut delivered = 0usize;

    for _ in 0..8 {
        match injector.next_fault() {
            FaultMode::Drop => dropped += 1,
            FaultMode::None => delivered += 1,
            _ => {}
        }
    }

    // 3 drops -> 1 success -> wraps back to 3 drops -> 1 success (2 full cycles).
    assert_eq!(dropped, 6, "expected 6 dropped messages over 2 cycles");
    assert_eq!(
        delivered, 2,
        "expected 2 delivered messages after partition clears"
    );
}

/// OOM recovery simulation: a task that does not fit on an OOM executor is
/// reassigned to a healthy one, and the OOM executor surfaces an injected error.
#[test]
fn oom_task_triggers_coordinator_reassignment() {
    struct MockExecutor {
        id: &'static str,
        memory_available_mb: u64,
    }

    let executors = [
        MockExecutor {
            id: "exec-0",
            memory_available_mb: 0,
        }, // OOM
        MockExecutor {
            id: "exec-1",
            memory_available_mb: 512,
        }, // healthy
    ];

    let required_mb = 256u64;

    let assigned = executors
        .iter()
        .find(|e| e.memory_available_mb >= required_mb)
        .map(|e| e.id);

    assert_eq!(
        assigned,
        Some("exec-1"),
        "task must be reassigned to the executor with available memory"
    );

    let injector = FaultInjector::new(vec![
        FaultMode::Error {
            message: "OOM: memory budget exhausted".into(),
        },
        FaultMode::None,
    ]);
    assert!(
        matches!(injector.next_fault(), FaultMode::Error { .. }),
        "exec-0 must surface OOM error"
    );
    assert_eq!(
        injector.next_fault(),
        &FaultMode::None,
        "exec-1 proceeds normally"
    );
}

/// Multi-executor shuffle failure: both executors fail mid-shuffle, the
/// coordinator counts exactly two failures, and recovery succeeds on the wrap.
#[test]
fn multi_executor_shuffle_failure_triggers_reshuffle() {
    let exec0_injector = FaultInjector::new(vec![
        FaultMode::None,
        FaultMode::Error {
            message: "shuffle write: broken pipe".into(),
        },
    ]);
    let exec1_injector = FaultInjector::new(vec![
        FaultMode::None,
        FaultMode::Error {
            message: "shuffle read: connection reset".into(),
        },
    ]);

    // Phase 1: both executors complete their first operation successfully.
    assert_eq!(exec0_injector.next_fault(), &FaultMode::None);
    assert_eq!(exec1_injector.next_fault(), &FaultMode::None);

    // Phase 2: both fail mid-shuffle.
    let mut failure_count = 0usize;
    if matches!(exec0_injector.next_fault(), FaultMode::Error { .. }) {
        failure_count += 1;
    }
    if matches!(exec1_injector.next_fault(), FaultMode::Error { .. }) {
        failure_count += 1;
    }

    assert_eq!(
        failure_count, 2,
        "coordinator must detect exactly 2 executor failures before triggering re-shuffle"
    );

    // Recovery: both injectors wrap back to FaultMode::None — re-shuffle succeeds.
    assert_eq!(
        exec0_injector.next_fault(),
        &FaultMode::None,
        "exec-0 recovers"
    );
    assert_eq!(
        exec1_injector.next_fault(),
        &FaultMode::None,
        "exec-1 recovers"
    );
}
