//! error.

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by connector operations.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Configuration problem (missing required property, bad value, etc.).
    #[error("connector config error: {message}")]
    Config { message: String },
    /// Kafka-specific error (connection, produce, consume).
    #[error("connector Kafka error (retriable={retriable}): {message}")]
    Kafka { message: String, retriable: bool },
    /// Parquet read/write error.
    #[error("connector Parquet error: {0}")]
    Parquet(String),
    /// Object-store (S3/GCS/Azure) error with optional HTTP status code.
    #[error("connector object-store error{status}: {message}",
        status = match .status {
            Some(code) => format!(" (HTTP {code})"),
            None => String::new(),
        })]
    ObjectStore {
        message: String,
        status: Option<u16>,
    },
    /// CDC (change-data-capture) pipeline error.
    #[error("connector CDC error: {0}")]
    Cdc(String),
    /// Typed I/O error from the operating system.
    #[error("connector I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Schema mismatch or incompatible field types.
    #[error("connector schema error: {message}")]
    Schema { message: String },
    /// Operation is not supported by this connector.
    #[error("connector unsupported: {message}")]
    Unsupported { message: String },
    /// A certification test assertion failed.
    #[error("connector certification failed: {reason}")]
    CertificationFailed { reason: String },
    /// Migration alias: callers that previously used `Io { message }` form.
    #[allow(non_camel_case_types)]
    #[error("connector I/O error: {message}")]
    IoStr { message: String },
}

/// Convenience result alias for connector operations.
pub type ConnectorResult<T> = Result<T, ConnectorError>;
