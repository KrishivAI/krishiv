//! gRPC service types for the executor task assignment protocol.

use krishiv_proto::{
    ExecutorTaskAssignment, ExecutorTaskService, TaskCancellationRequest, TaskStatusResponse,
    TransportDisposition, TransportVersion, wire,
};

use crate::{ExecutorAssignmentInbox, ExecutorError};

/// Executor-side task assignment service backed by an in-memory inbox.
#[derive(Debug, Clone)]
pub struct ExecutorTaskInboxService {
    inbox: ExecutorAssignmentInbox,
}

impl ExecutorTaskInboxService {
    /// Create a task assignment service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self { inbox }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        &self.inbox
    }
}

#[tonic::async_trait]
impl ExecutorTaskService for ExecutorTaskInboxService {
    async fn assign_task(
        &self,
        request: tonic::Request<ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let assignment = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(assignment.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                assignment.version(),
                TransportVersion::CURRENT
            )));
        }

        match self.inbox.push(assignment) {
            Ok(()) => Ok(tonic::Response::new(TaskStatusResponse::new(
                TransportDisposition::Accepted,
            ))),
            Err(ExecutorError::AssignmentQueueFull { current, max }) => {
                // Proper backpressure signal to the coordinator.
                Err(tonic::Status::resource_exhausted(format!(
                    "executor assignment queue full (current={current}, max={max})"
                )))
            }
            Err(other) => Err(tonic::Status::internal(other.to_string())),
        }
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<TaskCancellationRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        let request = request.into_inner();
        if !TransportVersion::CURRENT.is_compatible_with(request.version()) {
            return Err(tonic::Status::invalid_argument(format!(
                "unsupported executor task transport version {}; current version is {}",
                request.version(),
                TransportVersion::CURRENT
            )));
        }
        let removed = self
            .inbox
            .cancel_task(request.task_id())
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let response = if removed {
            TaskStatusResponse::new(TransportDisposition::Accepted)
        } else {
            TaskStatusResponse::new(TransportDisposition::UnknownTask)
                .with_message("task is not queued on this executor")
        };
        Ok(tonic::Response::new(response))
    }
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
        }
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::executor_task_assignment_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .assign_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn cancel_task(
        &self,
        request: tonic::Request<wire::v1::TaskCancellationRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        let request = wire::task_cancellation_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .cancel_task(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
}
