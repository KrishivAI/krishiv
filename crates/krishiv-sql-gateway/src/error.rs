use krishiv_api::KrishivError;
use krishiv_sql::sqlstate::{INSUFFICIENT_PRIVILEGE, INTERNAL_ERROR};
use krishiv_sql::{SqlError, SqlStateError, sqlstate_for};

/// Gateway-level error carrying JDBC/ODBC-compatible SQLSTATE metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayError {
    /// 5-character SQLSTATE code (ISO/IEC 9075).
    pub sqlstate: String,
    /// Human-readable error message.
    pub message: String,
}

impl GatewayError {
    /// The 5-character SQLSTATE code.
    pub fn sqlstate(&self) -> &str {
        &self.sqlstate
    }

    /// The human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Map a public [`KrishivError`] to a gateway error, selecting an
    /// appropriate SQLSTATE code for each variant.
    pub fn from_krishiv_error(error: KrishivError) -> Self {
        let message = error.to_string();
        let sqlstate = match &error {
            KrishivError::InvalidConfig { .. } => sqlstate_for(&SqlError::Unsupported {
                feature: "invalid configuration".into(),
            }),
            KrishivError::Unsupported { .. } => sqlstate_for(&SqlError::Unsupported {
                feature: message.clone(),
            }),
            KrishivError::Runtime { .. } => INTERNAL_ERROR,
            KrishivError::AccessDenied { .. } => INSUFFICIENT_PRIVILEGE,
        };
        Self {
            sqlstate: sqlstate.to_string(),
            message,
        }
    }

    /// Map a SQL-layer [`SqlError`] to a gateway error using its canonical
    /// SQLSTATE code.
    pub fn from_sql_error(error: &SqlError) -> Self {
        let mapped = SqlStateError::from_sql_error(error);
        Self {
            sqlstate: mapped.code.to_string(),
            message: mapped.message,
        }
    }
}

impl From<KrishivError> for GatewayError {
    fn from(error: KrishivError) -> Self {
        Self::from_krishiv_error(error)
    }
}

impl From<SqlError> for GatewayError {
    fn from(error: SqlError) -> Self {
        Self::from_sql_error(&error)
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.sqlstate, self.message)
    }
}

impl std::error::Error for GatewayError {}

/// Convenience alias for results that surface [`GatewayError`].
pub type GatewayResult<T> = std::result::Result<T, GatewayError>;
