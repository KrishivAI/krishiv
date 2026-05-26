use std::error::Error;
use std::fmt;

/// API result alias.
pub type Result<T> = std::result::Result<T, KrishivError>;

/// Public API errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KrishivError {
    /// A requested capability is not available in the current release.
    Unsupported { feature: String },
    /// User-provided configuration is invalid.
    InvalidConfig { message: String },
    /// Runtime error surfaced through the public API.
    Runtime { message: String },
    /// Access denied by auth or policy.
    AccessDenied { reason: String },
}

impl KrishivError {
    /// Create an unsupported-feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::Unsupported {
            feature: feature.into(),
        }
    }
}

impl fmt::Display for KrishivError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { feature } => write!(f, "unsupported Krishiv feature: {feature}"),
            Self::InvalidConfig { message } => write!(f, "invalid Krishiv config: {message}"),
            Self::Runtime { message } => write!(f, "Krishiv runtime error: {message}"),
            Self::AccessDenied { reason } => write!(f, "access denied: {reason}"),
        }
    }
}

impl Error for KrishivError {}

impl From<krishiv_runtime::RuntimeError> for KrishivError {
    fn from(value: krishiv_runtime::RuntimeError) -> Self {
        Self::Runtime {
            message: value.to_string(),
        }
    }
}

impl From<krishiv_sql::SqlError> for KrishivError {
    fn from(value: krishiv_sql::SqlError) -> Self {
        match value {
            krishiv_sql::SqlError::AccessDenied { reason } => Self::AccessDenied { reason },
            other => Self::Runtime {
                message: other.to_string(),
            },
        }
    }
}
