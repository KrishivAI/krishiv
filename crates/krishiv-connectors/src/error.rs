//! error.

use std::fmt;

// ---------------------------------------------------------------------------
// Error and Result
// ---------------------------------------------------------------------------

/// Errors produced by connector operations.
#[non_exhaustive]
#[derive(Debug)]
pub enum ConnectorError {
    /// Configuration problem (missing required property, bad value, etc.).
    Config { message: String },
    /// Kafka-specific error (connection, produce, consume).
    Kafka { message: String, retriable: bool },
    /// Parquet read/write error.
    Parquet(String),
    /// Object-store (S3/GCS/Azure) error with optional HTTP status code.
    ObjectStore {
        message: String,
        status: Option<u16>,
    },
    /// CDC (change-data-capture) pipeline error.
    Cdc(String),
    /// Typed I/O error from the operating system.
    Io(std::io::Error),
    /// Schema mismatch or incompatible field types.
    Schema { message: String },
    /// Operation is not supported by this connector.
    Unsupported { message: String },
    /// A certification test assertion failed.
    CertificationFailed { reason: String },
    /// Migration alias: callers that previously used `Io { message }` form.
    #[allow(non_camel_case_types)]
    IoStr { message: String },
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::Config { message } => write!(f, "connector config error: {message}"),
            ConnectorError::Kafka { message, retriable } => write!(
                f,
                "connector Kafka error (retriable={retriable}): {message}"
            ),
            ConnectorError::Parquet(message) => write!(f, "connector Parquet error: {message}"),
            ConnectorError::ObjectStore { message, status } => match status {
                Some(code) => write!(f, "connector object-store error (HTTP {code}): {message}"),
                None => write!(f, "connector object-store error: {message}"),
            },
            ConnectorError::Cdc(message) => write!(f, "connector CDC error: {message}"),
            ConnectorError::Io(e) => write!(f, "connector I/O error: {e}"),
            ConnectorError::Schema { message } => write!(f, "connector schema error: {message}"),
            ConnectorError::Unsupported { message } => {
                write!(f, "connector unsupported: {message}")
            }
            ConnectorError::CertificationFailed { reason } => {
                write!(f, "connector certification failed: {reason}")
            }
            ConnectorError::IoStr { message } => write!(f, "connector I/O error: {message}"),
        }
    }
}

impl std::error::Error for ConnectorError {}

/// Convenience result alias for connector operations.
pub type ConnectorResult<T> = Result<T, ConnectorError>;
