//! gRPC service types for the executor task assignment protocol.

pub const EXECUTOR_TASK_BEARER_TOKEN_ENV: &str = "KRISHIV_EXECUTOR_TASK_BEARER_TOKEN";
pub const REQUIRE_EXECUTOR_TASK_AUTH_ENV: &str = "KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH";

use krishiv_proto::{
    ExecutorTaskAssignment, ExecutorTaskService, TaskStatusResponse, TransportDisposition,
    TransportVersion, wire,
};

use crate::{AssignmentPushOutcome, ExecutorAssignmentInbox, ExecutorError};

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

/// Authentication settings for the executor task-control gRPC API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorTaskAuthConfig {
    require_auth: bool,
    bearer_token: Option<String>,
}

impl ExecutorTaskAuthConfig {
    /// Build auth config from process environment.
    pub fn from_env() -> Self {
        Self {
            require_auth: parse_bool_env(REQUIRE_EXECUTOR_TASK_AUTH_ENV),
            bearer_token: configured_executor_task_bearer_token(),
        }
    }

    /// Build auth config directly for tests and embedders.
    pub fn new(require_auth: bool, bearer_token: Option<String>) -> Self {
        Self {
            require_auth,
            bearer_token: bearer_token
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty()),
        }
    }

    /// Whether the process must fail closed if no bearer token is configured.
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    /// Whether a non-empty bearer token is configured.
    pub fn has_bearer_token(&self) -> bool {
        self.bearer_token.is_some()
    }

    /// The configured bearer token, if any.
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Validate the required-auth startup contract.
    pub fn validate_required(&self) -> crate::ExecutorResult<()> {
        if self.require_auth && self.bearer_token.is_none() {
            return Err(crate::ExecutorError::LocalExecution {
                message: format!(
                    "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
                ),
            });
        }
        Ok(())
    }
}

fn parse_bool_env(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
}

fn configured_executor_task_bearer_token() -> Option<String> {
    std::env::var(EXECUTOR_TASK_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

pub fn bearer_token_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<&str> {
    metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
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

        match self.inbox.push_with_outcome(assignment) {
            Ok(AssignmentPushOutcome::Enqueued) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Accepted),
            )),
            Ok(AssignmentPushOutcome::Duplicate) => Ok(tonic::Response::new(
                TaskStatusResponse::new(TransportDisposition::Duplicate),
            )),
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
        request: tonic::Request<krishiv_proto::task::TaskCancellationRequest>,
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

    async fn push_continuous_input(
        &self,
        _request: tonic::Request<krishiv_proto::task::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<TaskStatusResponse>, tonic::Status> {
        tracing::warn!(
            "push_continuous_input is not routed through executor gRPC; \
             use coordinator task assignments"
        );
        Err(tonic::Status::unimplemented(
            "continuous input must be delivered via coordinator task assignments or Flight SQL",
        ))
    }

    async fn drain_continuous_output(
        &self,
        _request: tonic::Request<krishiv_proto::task::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<krishiv_proto::task::DrainContinuousOutputResponse>, tonic::Status>
    {
        tracing::warn!(
            "drain_continuous_output is not routed through executor gRPC; \
             output is returned via coordinator task-result path"
        );
        Err(tonic::Status::unimplemented(
            "output is returned via the coordinator task-result path, not polled from the executor",
        ))
    }
}

/// Networked gRPC adapter for executor-side task assignment calls.
#[derive(Debug, Clone)]
pub struct ExecutorTaskGrpcService {
    inner: ExecutorTaskInboxService,
    required_bearer_token: Option<String>,
    auth_misconfiguration: Option<String>,
}

impl ExecutorTaskGrpcService {
    /// Create a networked executor task service.
    pub fn new(inbox: ExecutorAssignmentInbox) -> Self {
        Self::with_auth_config(inbox, ExecutorTaskAuthConfig::from_env())
    }

    /// Create a networked executor task service with explicit auth config.
    pub fn with_auth_config(inbox: ExecutorAssignmentInbox, auth: ExecutorTaskAuthConfig) -> Self {
        let auth_misconfiguration = (auth.require_auth() && !auth.has_bearer_token()).then(|| {
            format!(
                "{REQUIRE_EXECUTOR_TASK_AUTH_ENV}=true requires non-empty \
                 {EXECUTOR_TASK_BEARER_TOKEN_ENV}"
            )
        });
        Self {
            inner: ExecutorTaskInboxService::new(inbox),
            required_bearer_token: auth.bearer_token().map(ToOwned::to_owned),
            auth_misconfiguration,
        }
    }

    /// Require a bearer token for network task-control RPCs.
    #[must_use]
    pub fn with_required_bearer_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.required_bearer_token = (!token.trim().is_empty()).then(|| token.trim().to_owned());
        self.auth_misconfiguration = None;
        self
    }

    /// Assignment inbox backing this service.
    pub fn inbox(&self) -> &ExecutorAssignmentInbox {
        self.inner.inbox()
    }

    fn validate_auth(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> {
        if let Some(message) = &self.auth_misconfiguration {
            return Err(tonic::Status::unauthenticated(message.clone()));
        }
        let Some(expected) = &self.required_bearer_token else {
            return Ok(());
        };
        match bearer_token_from_metadata(metadata) {
            Some(actual)
                if constant_time_eq::constant_time_eq(actual.as_bytes(), expected.as_bytes()) =>
            {
                Ok(())
            }
            Some(_) => Err(tonic::Status::unauthenticated(
                "invalid executor task bearer token",
            )),
            None => Err(tonic::Status::unauthenticated(
                "missing executor task bearer token",
            )),
        }
    }
}

#[tonic::async_trait]
impl wire::v1::executor_task_server::ExecutorTask for ExecutorTaskGrpcService {
    async fn assign_task(
        &self,
        request: tonic::Request<wire::v1::ExecutorTaskAssignment>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
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
        self.validate_auth(request.metadata())?;
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

    async fn push_continuous_input(
        &self,
        request: tonic::Request<wire::v1::PushContinuousInputRequest>,
    ) -> Result<tonic::Response<wire::v1::TaskStatusResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::push_continuous_input_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .push_continuous_input(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(wire::task_status_response_to_wire(
            response,
        )))
    }

    async fn drain_continuous_output(
        &self,
        request: tonic::Request<wire::v1::DrainContinuousOutputRequest>,
    ) -> Result<tonic::Response<wire::v1::DrainContinuousOutputResponse>, tonic::Status> {
        self.validate_auth(request.metadata())?;
        let request = wire::drain_continuous_output_request_from_wire(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let response = self
            .inner
            .drain_continuous_output(tonic::Request::new(request))
            .await?
            .into_inner();
        Ok(tonic::Response::new(
            wire::drain_continuous_output_response_to_wire(response),
        ))
    }
}

/// Build the generated tonic server around an executor task inbox.
pub fn executor_task_grpc_server(
    inbox: ExecutorAssignmentInbox,
) -> wire::v1::executor_task_server::ExecutorTaskServer<ExecutorTaskGrpcService> {
    wire::v1::executor_task_server::ExecutorTaskServer::new(ExecutorTaskGrpcService::new(inbox))
}
