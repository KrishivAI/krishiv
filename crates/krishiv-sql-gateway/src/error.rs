use krishiv_api::{KrishivError, QueryResult};
use krishiv_sql::sqlstate::{INTERNAL_ERROR, INSUFFICIENT_PRIVILEGE};
use krishiv_sql::{SqlError, SqlStateError, sqlstate_for};

/// Gateway-level error with JDBC/ODBC-compatible SQLSTATE metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayError {
    pub sqlstate: String,
    pub message: String,
}

impl GatewayError {
    pub fn sqlstate(&self) -> &str {
        &self.sqlstate
    }

    pub fn message(&self) -> &str {
        &self.message
    }

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

    pub fn from_sql_error(error: &SqlError) -> Self {
        let mapped = SqlStateError::from_sql_error(error);
        Self {
            sqlstate: mapped.code.to_string(),
            message: mapped.message,
        }
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.sqlstate, self.message)
    }
}

impl std::error::Error for GatewayError {}

pub type GatewayResult<T> = std::result::Result<T, GatewayError>;

pub struct GatewayQueryResult {
    pub result: QueryResult,
}

impl std::fmt::Debug for GatewayQueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayQueryResult")
            .field("row_count", &self.result.row_count())
            .finish()
    }
}

impl GatewayQueryResult {
    pub fn into_inner(self) -> QueryResult {
        self.result
    }
}
