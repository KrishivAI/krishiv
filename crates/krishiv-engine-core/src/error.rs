//! Engine error type shared by the contract and its services.

use crate::kind::EngineKind;

/// Errors surfaced by the engine contract and its runtime services.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The job uses a feature the chosen engine does not support.
    #[error("{engine} engine does not support this job: {reason}")]
    Unsupported {
        /// The engine that rejected the job.
        engine: EngineKind,
        /// Why the job is unsupported.
        reason: String,
    },
    /// The compiled job is structurally invalid.
    #[error("invalid job: {0}")]
    InvalidJob(String),
    /// A source failed to open or read.
    #[error("source error: {0}")]
    Source(String),
    /// A sink failed to open or write.
    #[error("sink error: {0}")]
    Sink(String),
    /// A state-backend operation failed.
    #[error("state error: {0}")]
    State(String),
    /// A checkpoint operation failed.
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
    /// Any other runtime failure.
    #[error("engine runtime error: {0}")]
    Runtime(String),
}

/// Result alias for engine operations.
pub type EngineResult<T> = Result<T, EngineError>;
