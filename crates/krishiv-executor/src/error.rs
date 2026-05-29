use std::error::Error;
use std::fmt;

/// Executor crate result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor transport result alias.
pub type ExecutorTransportResult<T> = Result<T, crate::transport::ExecutorTransportError>;

/// Executor configuration or startup error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    /// Executor id failed validation.
    InvalidExecutorId { message: String },
    /// Task slots must be greater than zero.
    InvalidSlots,
    /// Coordinator endpoint cannot be empty.
    EmptyCoordinatorEndpoint,
    /// The executor assignment inbox lock was poisoned.
    AssignmentInboxPoisoned,
    /// A received task assignment cannot be executed.
    InvalidAssignment { message: String },
    /// Local stage fragment execution failed.
    LocalExecution { message: String },
    /// A streaming task fragment was submitted but the streaming runner is not
    /// yet implemented.  This becomes a real runner in R5.
    StreamingNotImplemented,
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutorId { message } => write!(f, "invalid executor id: {message}"),
            Self::InvalidSlots => f.write_str("task slots must be greater than zero"),
            Self::EmptyCoordinatorEndpoint => f.write_str("coordinator endpoint cannot be empty"),
            Self::AssignmentInboxPoisoned => f.write_str("executor assignment inbox is poisoned"),
            Self::InvalidAssignment { message } => write!(f, "invalid task assignment: {message}"),
            Self::LocalExecution { message } => {
                write!(f, "local stage fragment execution failed: {message}")
            }
            Self::StreamingNotImplemented => f.write_str(
                "streaming task runner not yet implemented; available in R5 \
                 (fragment must not use the 'stream:' prefix until R5.1)",
            ),
        }
    }
}

impl Error for ExecutorError {}

impl From<String> for ExecutorError {
    fn from(message: String) -> Self {
        Self::LocalExecution { message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_invalid_executor_id() {
        let err = ExecutorError::InvalidExecutorId {
            message: "bad id".into(),
        };
        assert_eq!(err.to_string(), "invalid executor id: bad id");
    }

    #[test]
    fn error_display_invalid_slots() {
        let err = ExecutorError::InvalidSlots;
        assert_eq!(err.to_string(), "task slots must be greater than zero");
    }

    #[test]
    fn error_display_empty_coordinator_endpoint() {
        let err = ExecutorError::EmptyCoordinatorEndpoint;
        assert_eq!(err.to_string(), "coordinator endpoint cannot be empty");
    }

    #[test]
    fn error_display_assignment_inbox_poisoned() {
        let err = ExecutorError::AssignmentInboxPoisoned;
        assert_eq!(err.to_string(), "executor assignment inbox is poisoned");
    }

    #[test]
    fn error_display_invalid_assignment() {
        let err = ExecutorError::InvalidAssignment {
            message: "bad task".into(),
        };
        assert_eq!(err.to_string(), "invalid task assignment: bad task");
    }

    #[test]
    fn error_display_local_execution() {
        let err = ExecutorError::LocalExecution {
            message: "query failed".into(),
        };
        assert_eq!(
            err.to_string(),
            "local stage fragment execution failed: query failed"
        );
    }

    #[test]
    fn error_display_streaming_not_implemented() {
        let err = ExecutorError::StreamingNotImplemented;
        assert!(err.to_string().contains("streaming task runner"));
        assert!(err.to_string().contains("R5"));
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn Error> = Box::new(ExecutorError::InvalidSlots);
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn error_from_string() {
        let err: ExecutorError = "something failed".to_string().into();
        assert!(matches!(err, ExecutorError::LocalExecution { .. }));
        assert_eq!(
            err.to_string(),
            "local stage fragment execution failed: something failed"
        );
    }

    #[test]
    fn error_clone() {
        let err = ExecutorError::InvalidAssignment {
            message: "test".into(),
        };
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn error_debug_format() {
        let err = ExecutorError::InvalidSlots;
        let debug = format!("{:?}", err);
        assert!(debug.contains("InvalidSlots"));
    }
}
