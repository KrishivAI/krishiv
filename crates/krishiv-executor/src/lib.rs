#![forbid(unsafe_code)]

//! R3.1 executor process skeleton.
//!
//! This crate owns executor-side process configuration and builds the first
//! versioned coordinator/executor transport requests. The actual gRPC client,
//! task runner loop, and DataFusion execution path land in later R3.1 slices.

use std::error::Error;
use std::fmt;

use krishiv_proto::{
    ExecutorDescriptor, ExecutorHeartbeatRequest, ExecutorId, ExecutorState, LeaseGeneration,
    RegisterExecutorRequest, TransportVersion,
};

/// Executor crate result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor configuration or startup error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    /// Executor id failed validation.
    InvalidExecutorId { message: String },
    /// Task slots must be greater than zero.
    InvalidSlots,
    /// Coordinator endpoint cannot be empty.
    EmptyCoordinatorEndpoint,
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutorId { message } => write!(f, "invalid executor id: {message}"),
            Self::InvalidSlots => f.write_str("task slots must be greater than zero"),
            Self::EmptyCoordinatorEndpoint => f.write_str("coordinator endpoint cannot be empty"),
        }
    }
}

impl Error for ExecutorError {}

/// R3.1 executor startup configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorConfig {
    executor_id: ExecutorId,
    host: String,
    slots: usize,
    coordinator_endpoint: String,
    lease_generation: LeaseGeneration,
}

impl ExecutorConfig {
    /// Create executor configuration.
    pub fn new(
        executor_id: impl Into<String>,
        host: impl Into<String>,
        slots: usize,
        coordinator_endpoint: impl Into<String>,
    ) -> ExecutorResult<Self> {
        if slots == 0 {
            return Err(ExecutorError::InvalidSlots);
        }

        let coordinator_endpoint = coordinator_endpoint.into();
        if coordinator_endpoint.trim().is_empty() {
            return Err(ExecutorError::EmptyCoordinatorEndpoint);
        }

        let executor_id =
            ExecutorId::try_new(executor_id).map_err(|error| ExecutorError::InvalidExecutorId {
                message: error.to_string(),
            })?;

        Ok(Self {
            executor_id,
            host: host.into(),
            slots,
            coordinator_endpoint,
            lease_generation: LeaseGeneration::initial(),
        })
    }

    /// Executor id.
    pub fn executor_id(&self) -> &ExecutorId {
        &self.executor_id
    }

    /// Host or pod name advertised by the executor.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Advertised task slots.
    pub fn slots(&self) -> usize {
        self.slots
    }

    /// Coordinator endpoint the executor will connect to in a later R3.1 slice.
    pub fn coordinator_endpoint(&self) -> &str {
        &self.coordinator_endpoint
    }

    /// Current executor lease generation.
    pub fn lease_generation(&self) -> LeaseGeneration {
        self.lease_generation
    }

    /// Build an executor descriptor for registration.
    pub fn descriptor(&self) -> ExecutorDescriptor {
        ExecutorDescriptor::new(self.executor_id.clone(), self.host.clone(), self.slots)
    }
}

/// Minimal executor runtime facade for the R3.1 bootstrap slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorRuntime {
    config: ExecutorConfig,
}

impl ExecutorRuntime {
    /// Create an executor runtime.
    pub fn new(config: ExecutorConfig) -> Self {
        Self { config }
    }

    /// Runtime configuration.
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// Build the versioned registration request this executor will send.
    pub fn registration_request(&self) -> RegisterExecutorRequest {
        RegisterExecutorRequest::new(self.config.descriptor())
    }

    /// Build an empty healthy heartbeat request for this executor.
    pub fn heartbeat_request(&self) -> ExecutorHeartbeatRequest {
        ExecutorHeartbeatRequest::new(
            self.config.executor_id.clone(),
            self.config.lease_generation,
            ExecutorState::Healthy,
        )
    }

    /// Human-readable startup summary for the binary.
    pub fn startup_summary(&self) -> String {
        format!(
            "Krishiv executor {} ready for transport {} at {} with {} slot(s)",
            self.config.executor_id(),
            TransportVersion::CURRENT,
            self.config.coordinator_endpoint(),
            self.config.slots()
        )
    }
}

#[cfg(test)]
mod tests {
    use krishiv_proto::{ExecutorState, LeaseGeneration, TransportVersion};

    use super::{ExecutorConfig, ExecutorError, ExecutorRuntime};

    #[test]
    fn config_rejects_invalid_values() {
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 0, "http://coordinator"),
            Err(ExecutorError::InvalidSlots)
        ));
        assert!(matches!(
            ExecutorConfig::new("exec-1", "host", 1, " "),
            Err(ExecutorError::EmptyCoordinatorEndpoint)
        ));
    }

    #[test]
    fn runtime_builds_versioned_registration_request() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 2, "http://coordinator").unwrap(),
        );
        let request = runtime.registration_request();

        assert_eq!(request.version(), TransportVersion::CURRENT);
        assert_eq!(request.descriptor().executor_id().as_str(), "exec-1");
        assert_eq!(request.descriptor().slots(), 2);
    }

    #[test]
    fn runtime_builds_heartbeat_with_initial_lease() {
        let runtime = ExecutorRuntime::new(
            ExecutorConfig::new("exec-1", "pod-a", 1, "http://coordinator").unwrap(),
        );
        let heartbeat = runtime.heartbeat_request();

        assert_eq!(heartbeat.state(), ExecutorState::Healthy);
        assert_eq!(heartbeat.lease_generation(), LeaseGeneration::initial());
        assert!(heartbeat.running_attempts().is_empty());
    }
}
