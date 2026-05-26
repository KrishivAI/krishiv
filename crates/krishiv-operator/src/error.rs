//! Operator errors.

use std::error::Error;
use std::fmt;

use krishiv_scheduler::SchedulerError;

/// Operator result alias.
pub type OperatorResult<T> = Result<T, OperatorError>;

/// Operator and reconciliation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorError {
    /// Resource validation failed before scheduling.
    InvalidResource { message: String },
    /// Scheduler operation failed.
    Scheduler(SchedulerError),
    /// Kubernetes client or runtime operation failed.
    Kubernetes { message: String },
    /// Serialization or deserialization failed.
    Serialization { message: String },
    /// Shared coordinator lock was poisoned.
    CoordinatorLockPoisoned,
}

impl fmt::Display for OperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResource { message } => write!(f, "invalid KrishivJob: {message}"),
            Self::Scheduler(error) => write!(f, "{error}"),
            Self::Kubernetes { message } => write!(f, "kubernetes operation failed: {message}"),
            Self::Serialization { message } => write!(f, "serialization failed: {message}"),
            Self::CoordinatorLockPoisoned => f.write_str("shared coordinator lock was poisoned"),
        }
    }
}

impl Error for OperatorError {}

impl From<SchedulerError> for OperatorError {
    fn from(value: SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}

impl From<kube::Error> for OperatorError {
    fn from(value: kube::Error) -> Self {
        Self::Kubernetes {
            message: value.to_string(),
        }
    }
}

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
