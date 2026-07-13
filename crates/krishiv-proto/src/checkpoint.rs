//! Checkpoint messages.

use crate::ids::{FencingToken, JobId, OperatorId, PartitionId, TaskId};

// ── Checkpoint alignment ─────────────────────────────────────────────────────

/// Alignment mode for checkpoint barriers.
///
/// Matches `krishiv_dataflow::queue::CheckpointAlignment` — the canonical
/// definition lives there; this proto copy exists so that checkpoint messages
/// do not depend on the dataflow crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointAlignment {
    /// Aligned checkpoint: wait for all input channels to drain past the barrier.
    #[default]
    Aligned,
    /// Unaligned checkpoint: barrier can overtake in-flight data.
    Unaligned,
}

// ── Checkpoint control-plane messages ─────────────────────────────────────────

/// One source partition offset captured at the barrier boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSourceOffset {
    pub partition_id: PartitionId,
    /// Legacy numeric offset for connectors whose offsets fit in an integer.
    ///
    /// Kafka continues to populate this for compatibility with existing
    /// checkpoint metadata and status surfaces.
    pub offset: i64,
    /// Connector-encoded exact offset bytes used by checkpoint restore.
    pub encoded_offset: Vec<u8>,
}

/// Reference to an in-flight buffer captured during an unaligned checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnalignedBufferRef {
    /// Operator that owns this buffer.
    pub operator_id: OperatorId,
    /// Input channel index that received the buffered records.
    pub channel_index: u32,
    /// Number of records in the buffer.
    pub record_count: u64,
    /// Path to the serialized buffer data.
    pub buffer_path: String,
}

/// Reference to a durable prepared-sink transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkTransactionRef {
    /// Sink identifier.
    pub sink_id: String,
    /// Epoch in which the sink was prepared.
    pub epoch: u64,
    /// Path to the prepared transaction data.
    pub prepare_path: String,
    /// Whether this transaction has been committed.
    pub committed: bool,
}

/// DUR-2 recovery plan: which prepared-sink transactions to commit vs abort when
/// restoring to a checkpoint epoch. Produced by [`plan_sink_transaction_recovery`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkRecoveryPlan {
    /// Prepared-but-uncommitted transactions at epoch ≤ restored: their output was
    /// durably prepared as part of the restored checkpoint and must be committed
    /// (the two-phase second phase, replayed on recovery).
    pub commit: Vec<SinkTransactionRef>,
    /// Transactions from epochs *after* the restore point: they belong to work
    /// that is being rolled back and must be aborted so recovery does not
    /// double-write already-superseded output.
    pub abort: Vec<SinkTransactionRef>,
}

/// Decide, deterministically, how to finalize the prepared-sink transactions
/// recorded in a checkpoint when restoring to `restored_epoch` (DUR-2).
///
/// - epoch > `restored_epoch` → **abort** (rolled-back work).
/// - epoch ≤ `restored_epoch` and not yet committed → **commit** (replay the
///   two-phase second phase; the checkpoint at this epoch is durable, so every
///   transaction it prepared belongs to the restored state).
/// - epoch ≤ `restored_epoch` and already committed → skip (idempotent).
///
/// Pure and connector-agnostic; the executor drives the per-connector
/// commit/abort against the reconstructed handles.
pub fn plan_sink_transaction_recovery(
    prepared: &[SinkTransactionRef],
    restored_epoch: u64,
) -> SinkRecoveryPlan {
    let mut plan = SinkRecoveryPlan::default();
    for txn in prepared {
        if txn.epoch > restored_epoch {
            plan.abort.push(txn.clone());
        } else if !txn.committed {
            plan.commit.push(txn.clone());
        }
    }
    plan
}

/// Coordinator → Executor: begin checkpoint epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateCheckpointRequest {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    /// Alignment mode for this checkpoint.
    pub alignment: CheckpointAlignment,
}

/// Executor → Coordinator: operator snapshot complete for epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAckRequest {
    pub job_id: JobId,
    pub operator_id: OperatorId,
    pub task_id: TaskId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    /// One per source partition this task owns.
    pub source_offsets: Vec<CheckpointSourceOffset>,
    /// None if operator has no state.
    pub snapshot_path: Option<String>,
    /// In-flight buffers captured during unaligned checkpoint (empty if aligned).
    pub unaligned_buffers: Vec<UnalignedBufferRef>,
    /// Durable sink transactions prepared during this epoch.
    pub sink_transactions: Vec<SinkTransactionRef>,
}

/// Response to `CheckpointAckRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckResponse {
    Accepted,
    StaleEpoch { current_epoch: u64 },
    JobNotFound,
    StaleFencingToken { current_token: u64 },
}

#[cfg(test)]
mod sink_recovery_tests {
    use super::*;

    fn txn(sink: &str, epoch: u64, committed: bool) -> SinkTransactionRef {
        SinkTransactionRef {
            sink_id: sink.to_string(),
            epoch,
            prepare_path: format!("/prep/{sink}/{epoch}"),
            committed,
        }
    }

    #[test]
    fn recovery_commits_uncommitted_at_or_before_epoch_and_aborts_after() {
        let prepared = vec![
            txn("s", 1, true),  // already committed ≤ E → skip
            txn("s", 2, false), // uncommitted ≤ E → commit
            txn("s", 3, false), // uncommitted == E → commit
            txn("s", 4, false), // > E → abort
            txn("s", 5, true),  // > E even if committed → abort (rolled back)
        ];
        let plan = plan_sink_transaction_recovery(&prepared, 3);
        assert_eq!(
            plan.commit.iter().map(|t| t.epoch).collect::<Vec<_>>(),
            vec![2, 3],
            "uncommitted txns at epoch ≤ restored must be committed"
        );
        assert_eq!(
            plan.abort.iter().map(|t| t.epoch).collect::<Vec<_>>(),
            vec![4, 5],
            "txns from epochs after the restore point must be aborted"
        );
    }

    #[test]
    fn recovery_of_empty_or_all_committed_is_a_noop() {
        assert_eq!(
            plan_sink_transaction_recovery(&[], 7),
            SinkRecoveryPlan::default()
        );
        let all_committed = vec![txn("s", 1, true), txn("s", 2, true)];
        let plan = plan_sink_transaction_recovery(&all_committed, 5);
        assert!(plan.commit.is_empty() && plan.abort.is_empty());
    }
}
