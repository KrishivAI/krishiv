use thiserror::Error;

/// Executor crate result alias.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

/// Executor transport result alias.
pub type ExecutorTransportResult<T> = Result<T, crate::transport::ExecutorTransportError>;

/// Executor configuration or startup error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExecutorError {
    /// Executor id failed validation.
    #[error("invalid executor id: {message}")]
    InvalidExecutorId { message: String },

    /// Task slots must be greater than zero.
    #[error("task slots must be greater than zero")]
    InvalidSlots,

    /// Coordinator endpoint cannot be empty.
    #[error("coordinator endpoint cannot be empty")]
    EmptyCoordinatorEndpoint,

    /// The executor assignment inbox lock was poisoned.
    #[error("executor assignment inbox is poisoned")]
    AssignmentInboxPoisoned,

    /// The assignment inbox has reached its configured capacity.
    /// This is the backpressure signal — the coordinator should slow down
    /// or the executor is overloaded / recovering.
    #[error("executor assignment inbox full (current={current}, max={max}) — backpressure")]
    AssignmentQueueFull { current: usize, max: usize },

    /// A received task assignment cannot be executed.
    #[error("invalid task assignment: {message}")]
    InvalidAssignment { message: String },

    /// Local stage fragment execution failed.
    #[error("local stage fragment execution failed: {message}")]
    LocalExecution { message: String },

    /// A streaming task fragment was submitted but the streaming runner is not
    /// yet implemented.
    #[error(
        "streaming task runner not yet implemented; available in R5 (fragment must not use the 'stream:' prefix until R5.1)"
    )]
    StreamingNotImplemented,
}

impl From<String> for ExecutorError {
    fn from(message: String) -> Self {
        Self::LocalExecution { message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

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

    #[test]
    fn error_display_assignment_queue_full() {
        let err = ExecutorError::AssignmentQueueFull {
            current: 42,
            max: 100,
        };
        assert_eq!(
            err.to_string(),
            "executor assignment inbox full (current=42, max=100) — backpressure"
        );
    }
}
