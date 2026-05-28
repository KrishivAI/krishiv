//! Service traits.

use crate::checkpoint::*;
use crate::executor::*;
use crate::task::*;

/// Tonic-shaped coordinator service implemented by the active job coordinator.
///
/// This trait is deliberately defined over Krishiv Rust contract structs first.
/// A later R3.1 slice can map these methods to generated protobuf messages and
/// a concrete network server without changing scheduler semantics.
#[tonic::async_trait]
pub trait CoordinatorExecutorService: Send + Sync + 'static {
    /// Register an executor with the active coordinator.
    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status>;

    /// Deregister an executor from the active coordinator.
    async fn deregister_executor(
        &self,
        request: tonic::Request<DeregisterExecutorRequest>,
    ) -> Result<tonic::Response<DeregisterExecutorResponse>, tonic::Status>;

    /// Apply an executor heartbeat to the active coordinator.
    async fn executor_heartbeat(
        &self,
        request: tonic::Request<ExecutorHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorHeartbeatResponse>, tonic::Status>;

    /// Apply a task status update to the active coordinator.
    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;

    /// Route a checkpoint ack from an executor to the active coordinator (R6a).
    async fn checkpoint_ack(
        &self,
        request: tonic::Request<CheckpointAckRequest>,
    ) -> Result<tonic::Response<CheckpointAckResponse>, tonic::Status>;
}

/// Tonic-shaped executor service implemented by executor processes.
#[tonic::async_trait]
pub trait ExecutorTaskService: Send + Sync + 'static {
    /// Assign work to an executor.
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;

    /// Cancel work on an executor.
    async fn cancel_task(
        &self,
        request: tonic::Request<TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status>;
}


