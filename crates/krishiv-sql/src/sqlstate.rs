#![forbid(unsafe_code)]
//! SQLSTATE code mapping for Krishiv SQL errors.
//!
//! Maps [`SqlError`] variants to the 5-character SQLSTATE codes defined by
//! ISO/IEC 9075 (SQL standard) and widely adopted by JDBC/ODBC drivers.
//! Clients can surface these codes over the Flight SQL wire protocol in the
//! `grpc-status-details` trailer.

use crate::SqlError;

// ── Well-known SQLSTATE codes ─────────────────────────────────────────────────

/// `00000` — Successful completion.
pub const SUCCESS: &str = "00000";
/// `0A000` — Feature not supported.
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// `22000` — Data exception (general).
pub const DATA_EXCEPTION: &str = "22000";
/// `28000` — Invalid authorisation specification (access denied).
pub const INVALID_AUTHORIZATION: &str = "28000";
/// `42000` — Syntax error or access rule violation.
pub const SYNTAX_ERROR: &str = "42000";
/// `42501` — Insufficient privilege.
pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
/// `42P01` — Undefined table.
pub const UNDEFINED_TABLE: &str = "42P01";
/// `57014` — Query cancelled (due to operator or timeout).
pub const QUERY_CANCELLED: &str = "57014";
/// `57P05` — Query execution timeout.
pub const QUERY_TIMEOUT: &str = "57P05";
/// `58000` — System error (external component failure).
pub const SYSTEM_ERROR: &str = "58000";
/// `XX000` — Internal error (engine fault).
pub const INTERNAL_ERROR: &str = "XX000";
/// `HY000` — General error (catch-all for driver-level errors).
pub const GENERAL_ERROR: &str = "HY000";

// ── Mapping ───────────────────────────────────────────────────────────────────

/// Return the SQLSTATE code for the given [`SqlError`].
///
/// The returned string is always a 5-character SQLSTATE code conforming to
/// ISO/IEC 9075.
pub fn sqlstate_for(error: &SqlError) -> &'static str {
    match error {
        SqlError::EmptyQuery => SYNTAX_ERROR,
        SqlError::EmptyTableName => SYNTAX_ERROR,
        SqlError::Unsupported { .. } => FEATURE_NOT_SUPPORTED,
        SqlError::InvalidTableFunction { .. } => SYNTAX_ERROR,
        SqlError::DataFusion { .. } => INTERNAL_ERROR,
        SqlError::Optimizer(_) => INTERNAL_ERROR,
        SqlError::AccessDenied { .. } => INSUFFICIENT_PRIVILEGE,
        SqlError::OperationCancelled { .. } => QUERY_CANCELLED,
        SqlError::Timeout { .. } => QUERY_TIMEOUT,
    }
}

/// A structured error envelope carrying the SQLSTATE code alongside the
/// original error message.  Suitable for embedding in Flight SQL or JDBC
/// error responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStateError {
    /// 5-character SQLSTATE code.
    pub code: &'static str,
    /// Human-readable error message.
    pub message: String,
}

impl SqlStateError {
    /// Build a `SqlStateError` from a [`SqlError`].
    pub fn from_sql_error(error: &SqlError) -> Self {
        Self {
            code: sqlstate_for(error),
            message: error.to_string(),
        }
    }
}

impl std::fmt::Display for SqlStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQLSTATE {} — {}", self.code, self.message)
    }
}

impl std::error::Error for SqlStateError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_query_maps_to_syntax_error() {
        let e = SqlError::EmptyQuery;
        assert_eq!(sqlstate_for(&e), SYNTAX_ERROR);
    }

    #[test]
    fn unsupported_maps_to_feature_not_supported() {
        let e = SqlError::Unsupported { feature: "TABLESAMPLE".into() };
        assert_eq!(sqlstate_for(&e), FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn datafusion_maps_to_internal_error() {
        let e = SqlError::DataFusion { message: "panic in executor".into() };
        assert_eq!(sqlstate_for(&e), INTERNAL_ERROR);
    }

    #[test]
    fn access_denied_maps_to_insufficient_privilege() {
        let e = SqlError::AccessDenied { reason: "no read permission".into() };
        assert_eq!(sqlstate_for(&e), INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn cancelled_maps_to_query_cancelled() {
        let e = SqlError::OperationCancelled { operation_id: 42 };
        assert_eq!(sqlstate_for(&e), QUERY_CANCELLED);
    }

    #[test]
    fn timeout_maps_to_query_timeout() {
        let e = SqlError::Timeout { timeout_ms: 5000 };
        assert_eq!(sqlstate_for(&e), QUERY_TIMEOUT);
    }

    #[test]
    fn sql_state_error_display() {
        let e = SqlError::EmptyQuery;
        let se = SqlStateError::from_sql_error(&e);
        let s = se.to_string();
        assert!(s.contains(SYNTAX_ERROR));
        assert!(s.contains("empty"));
    }

    #[test]
    fn sql_state_error_is_std_error() {
        let e = SqlError::EmptyQuery;
        let se = SqlStateError::from_sql_error(&e);
        let _: &dyn std::error::Error = &se;
    }

    #[test]
    fn all_sqlstate_codes_are_5_chars() {
        for code in &[
            SUCCESS, FEATURE_NOT_SUPPORTED, DATA_EXCEPTION, INVALID_AUTHORIZATION,
            SYNTAX_ERROR, INSUFFICIENT_PRIVILEGE, UNDEFINED_TABLE, QUERY_CANCELLED,
            QUERY_TIMEOUT, SYSTEM_ERROR, INTERNAL_ERROR, GENERAL_ERROR,
        ] {
            assert_eq!(code.len(), 5, "SQLSTATE {code} must be 5 characters");
        }
    }
}
