#![forbid(unsafe_code)]

/// Errors produced by the incremental computing layer.
#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("operator error: {0}")]
    Operator(String),

    #[error("view not found: {0}")]
    ViewNotFound(String),

    #[error("recursive view cycle limit exceeded after {0} iterations")]
    CycleLimitExceeded(usize),

    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type DeltaResult<T> = Result<T, DeltaError>;
