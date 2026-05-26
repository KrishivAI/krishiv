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
