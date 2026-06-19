//! Control-plane transport abstraction (ADR-DIST-05).
//!
//! The production coordinator/executor path uses tonic gRPC via
//! [`super::CoordinatorExecutorTonicService`]. In-process and test transports
//! reuse the same [`Coordinator`] logic through [`super::InProcessCoordinatorBridge`].

use krishiv_proto::{
    CheckpointAckRequest, CheckpointAckResponse, DeregisterExecutorRequest,
    DeregisterExecutorResponse, ExecutorHeartbeatRequest, ExecutorHeartbeatResponse,
    RegisterExecutorRequest, RegisterExecutorResponse, TaskStatusRequest, TaskStatusResponse,
};

use crate::SchedulerResult;

/// Coordinator ↔ executor control RPCs without exposing tonic at the core layer.
pub trait CoordinatorExecutorTransport: Send + Sync {
    fn register_executor(
        &self,
        request: RegisterExecutorRequest,
    ) -> SchedulerResult<RegisterExecutorResponse>;

    fn deregister_executor(
        &self,
        request: DeregisterExecutorRequest,
    ) -> SchedulerResult<DeregisterExecutorResponse>;

    fn executor_heartbeat(
        &self,
        request: ExecutorHeartbeatRequest,
    ) -> SchedulerResult<ExecutorHeartbeatResponse>;

    fn task_status(&self, request: TaskStatusRequest) -> SchedulerResult<TaskStatusResponse>;

    fn checkpoint_ack(
        &self,
        request: CheckpointAckRequest,
    ) -> SchedulerResult<CheckpointAckResponse>;
}
