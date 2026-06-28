/// API result alias.
pub type Result<T> = std::result::Result<T, KrishivError>;

/// Public API errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KrishivError {
    /// A requested capability is not available in the current release.
    #[error("unsupported Krishiv feature: {feature}")]
    Unsupported { feature: String },
    /// User-provided configuration is invalid.
    #[error("invalid Krishiv config: {message}")]
    InvalidConfig { message: String },
    /// Runtime error surfaced through the public API.
    #[error("Krishiv runtime error: {message}")]
    Runtime { message: String },
    /// Access denied by auth or policy.
    #[error("access denied: {reason}")]
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

impl From<krishiv_runtime::RuntimeError> for KrishivError {
    fn from(value: krishiv_runtime::RuntimeError) -> Self {
        Self::Runtime {
            message: value.to_string(),
        }
    }
}

impl From<krishiv_engine_core::EngineError> for KrishivError {
    fn from(value: krishiv_engine_core::EngineError) -> Self {
        use krishiv_engine_core::EngineError as E;
        let message = value.to_string();
        match value {
            E::Unsupported { .. } => Self::Unsupported { feature: message },
            E::InvalidJob(_) => Self::InvalidConfig { message },
            E::Source(_) | E::Sink(_) | E::State(_) | E::Checkpoint(_) | E::Runtime(_) => {
                Self::Runtime { message }
            }
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

impl From<krishiv_sql::ContinuousInputError> for KrishivError {
    fn from(value: krishiv_sql::ContinuousInputError) -> Self {
        match value {
            error @ krishiv_sql::ContinuousInputError::SchemaMismatch { .. } => {
                Self::InvalidConfig {
                    message: error.to_string(),
                }
            }
            error @ (krishiv_sql::ContinuousInputError::QueueFull
            | krishiv_sql::ContinuousInputError::Closed
            | krishiv_sql::ContinuousInputError::LockPoisoned(_)) => Self::Runtime {
                message: error.to_string(),
            },
        }
    }
}
