//! Operator errors.

use krishiv_scheduler::SchedulerError;

/// Operator result alias.
pub type OperatorResult<T> = Result<T, OperatorError>;

/// Operator and reconciliation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperatorError {
    /// Resource validation failed before scheduling.
    #[error("invalid KrishivJob: {message}")]
    InvalidResource { message: String },
    /// Scheduler operation failed.
    #[error("{0}")]
    Scheduler(#[from] SchedulerError),
    /// Kubernetes client or runtime operation failed.
    #[error("kubernetes operation failed: {message}")]
    Kubernetes { message: String },
    /// Serialization or deserialization failed.
    #[error("serialization failed: {message}")]
    Serialization { message: String },
    /// Shared coordinator lock was poisoned.
    #[error("shared coordinator lock was poisoned")]
    CoordinatorLockPoisoned,
}

#[cfg(feature = "k8s")]
impl From<kube::Error> for OperatorError {
    fn from(value: kube::Error) -> Self {
        Self::Kubernetes {
            message: value.to_string(),
        }
    }
}

#[cfg(feature = "k8s")]
impl From<kube::runtime::watcher::Error> for OperatorError {
    fn from(value: kube::runtime::watcher::Error) -> Self {
        Self::Kubernetes {
            message: value.to_string(),
        }
    }
}

impl From<serde_json::Error> for OperatorError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization {
            message: value.to_string(),
        }
    }
}
