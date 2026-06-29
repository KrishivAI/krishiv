//! Management RPC types.

use crate::ids::JobId;

/// Errors from request validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError(pub String);

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validation error: {}", self.0)
    }
}

impl std::error::Error for ValidationError {}

/// Domain types for the coordinator management service (GAP-RT-04).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointRequest {
    pub job_id: JobId,
    /// Empty string means no label.
    pub label: String,
    /// When true, the job is cancelled once the savepoint epoch is durably
    /// committed and preserved (stop-with-savepoint).
    pub stop: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointResponse {
    pub epoch: u64,
    /// Optional human-readable status message from the coordinator.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreJobRequest {
    pub job_id: JobId,
    pub epoch: u64,
    pub storage_path: String,
    /// When true, `epoch` names a savepoint in the durable savepoints area:
    /// the coordinator copies it back into the active checkpoint chain before
    /// activating the restore.
    pub from_savepoint: bool,
}

impl RestoreJobRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.storage_path.trim().is_empty() {
            return Err(ValidationError("storage_path must not be empty".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreJobResponse {
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCheckpointsRequest {
    pub job_id: JobId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointEpochInfo {
    pub epoch: u64,
    pub is_savepoint: bool,
    pub savepoint_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCheckpointsResponse {
    pub epochs: Vec<CheckpointEpochInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectStateRequest {
    pub job_id: JobId,
    pub operator_id: String,
}

impl InspectStateRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.operator_id.trim().is_empty() {
            return Err(ValidationError("operator_id must not be empty".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshotInfo {
    pub task_id: String,
    pub snapshot_path: String,
}

impl StateSnapshotInfo {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.task_id.trim().is_empty() {
            return Err(ValidationError("task_id must not be empty".into()));
        }
        if self.snapshot_path.trim().is_empty() {
            return Err(ValidationError("snapshot_path must not be empty".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectStateResponse {
    pub snapshots: Vec<StateSnapshotInfo>,
}

/// Tonic-shaped coordinator management service for CLI→coordinator RPCs.
#[tonic::async_trait]
pub trait CoordinatorManagementService: Send + Sync + 'static {
    async fn trigger_savepoint(
        &self,
        request: tonic::Request<TriggerSavepointRequest>,
    ) -> Result<tonic::Response<TriggerSavepointResponse>, tonic::Status>;

    async fn restore_job(
        &self,
        request: tonic::Request<RestoreJobRequest>,
    ) -> Result<tonic::Response<RestoreJobResponse>, tonic::Status>;

    async fn list_checkpoints(
        &self,
        request: tonic::Request<ListCheckpointsRequest>,
    ) -> Result<tonic::Response<ListCheckpointsResponse>, tonic::Status>;

    async fn inspect_state(
        &self,
        request: tonic::Request<InspectStateRequest>,
    ) -> Result<tonic::Response<InspectStateResponse>, tonic::Status>;
}
