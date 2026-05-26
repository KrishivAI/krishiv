//! Checkpoint messages.

use crate::ids::*;

// ── Checkpoint control-plane messages ─────────────────────────────────────────

/// One source partition offset captured at the barrier boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSourceOffset {
    pub partition_id: String,
    pub offset: i64,
}

/// Coordinator → Executor: begin checkpoint epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitiateCheckpointRequest {
    pub job_id: JobId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
}

/// Executor → Coordinator: operator snapshot complete for epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointAckRequest {
    pub job_id: JobId,
    pub operator_id: String,
    pub task_id: TaskId,
    pub epoch: u64,
    pub fencing_token: FencingToken,
    /// One per source partition this task owns.
    pub source_offsets: Vec<CheckpointSourceOffset>,
    /// None if operator has no state.
    pub snapshot_path: Option<String>,
}

/// Coordinator → Executor: abort the in-progress checkpoint epoch E.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortCheckpointRequest {
    pub job_id: JobId,
    pub epoch: u64,
}

/// Response to `InitiateCheckpointRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointInitiateResponse {
    Accepted,
    StaleEpoch { current_epoch: u64 },
    JobNotFound,
}

/// Response to `CheckpointAckRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckResponse {
    Accepted,
    StaleEpoch { current_epoch: u64 },
    JobNotFound,
}
