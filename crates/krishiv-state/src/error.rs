/// Errors from keyed state operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("state backend unavailable: {message}")]
    BackendUnavailable {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error("snapshot not supported by backend: {backend}")]
    SnapshotUnsupported { backend: &'static str },
    #[error("snapshot corrupt: {message}")]
    SnapshotCorrupt { message: String },
    /// A stored entry could not be deserialized; the byte representation is invalid.
    #[error("state entry corrupt: {message}")]
    CorruptEntry { message: String },
    /// The system clock returned a value that would cause an arithmetic underflow.
    #[error("clock error: {message}")]
    ClockError { message: String },
    #[error("state lock poisoned: {message}")]
    LockPoisoned { message: String },
}

/// Convenience alias for state operation results.
pub type StateResult<T> = Result<T, StateError>;
