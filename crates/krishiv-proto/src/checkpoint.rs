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
