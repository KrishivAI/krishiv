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

impl EngineError {
    /// Returns `true` for errors that may be transient (network blips, executor
    /// restarts) and for which the caller should retry. Structural errors
    /// (`InvalidJob`, `Unsupported`) are never transient.
    ///
    /// BATCH-1: Previously ALL `Runtime` and `Source` errors were classified as
    /// transient, causing permanent failures (SQL parse errors, "table not
    /// found", "source produced no batches") to be retried 3× with backoff.
    /// Now only checkpoint I/O errors and runtime/source errors whose message
    /// contains transient indicators (connection, timeout, reset, unavailable)
    /// are considered retryable.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Checkpoint(_) => true,
            Self::Runtime(msg) | Self::Source(msg) => {
                let lower = msg.to_lowercase();
                lower.contains("connection")
                    || lower.contains("connect")
                    || lower.contains("timeout")
                    || lower.contains("timed out")
                    || lower.contains("reset")
                    || lower.contains("unavailable")
                    || lower.contains("broken pipe")
                    || lower.contains("temporary")
                    || lower.contains("retry")
                    || lower.contains("transport")
                    || lower.contains("rpc")
            }
            _ => false,
        }
    }
}

/// Result alias for engine operations.
pub type EngineResult<T> = Result<T, EngineError>;
