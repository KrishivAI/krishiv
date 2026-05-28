//! Management RPC types.

use crate::ids::JobId;

/// Domain types for the coordinator management service (GAP-RT-04).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointRequest {
    pub job_id: JobId,
    /// Empty string means no label.
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerSavepointResponse {
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreJobRequest {
    pub job_id: JobId,
    pub epoch: u64,
    pub storage_path: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSnapshotInfo {
    pub task_id: String,
    pub snapshot_path: String,
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
