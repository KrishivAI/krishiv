use std::fmt;

/// Errors from keyed state operations.
#[derive(Debug)]
pub enum StateError {
    BackendUnavailable {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    SnapshotUnsupported {
        backend: &'static str,
    },
    SnapshotCorrupt {
        message: String,
    },
    /// A stored entry could not be deserialized; the byte representation is invalid.
    CorruptEntry {
        message: String,
    },
    /// A snapshot scan was interrupted before completion; the result is partial.
    SnapshotIncomplete {
        message: String,
    },
    /// The system clock returned a value that would cause an arithmetic underflow.
    ClockError {
        message: String,
    },
    LockPoisoned {
        message: String,
    },
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendUnavailable { message, .. } => {
                write!(f, "state backend unavailable: {message}")
            }
            Self::SnapshotUnsupported { backend } => {
                write!(f, "snapshot not supported by backend: {backend}")
            }
            Self::SnapshotCorrupt { message } => {
                write!(f, "snapshot corrupt: {message}")
            }
            Self::CorruptEntry { message } => {
                write!(f, "state entry corrupt: {message}")
            }
            Self::SnapshotIncomplete { message } => {
                write!(f, "snapshot incomplete: {message}")
            }
            Self::ClockError { message } => {
                write!(f, "clock error: {message}")
            }
            Self::LockPoisoned { message } => {
                write!(f, "state lock poisoned: {message}")
            }
        }
    }
}

impl std::error::Error for StateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BackendUnavailable { source, .. } => source
                .as_ref()
                .map(|e| e.as_ref() as &(dyn std::error::Error + 'static)),
            _ => None,
        }
    }
}

/// Convenience alias for state operation results.
pub type StateResult<T> = Result<T, StateError>;
