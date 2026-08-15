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
        SqlError::DataFusion { message } => datafusion_sqlstate(message),
        SqlError::Optimizer(_) => INTERNAL_ERROR,
        SqlError::AccessDenied { .. } => INSUFFICIENT_PRIVILEGE,
        SqlError::OperationCancelled { .. } => QUERY_CANCELLED,
        SqlError::Timeout { .. } => QUERY_TIMEOUT,
    }
}

/// Classify a `SqlError::DataFusion` message into a SQLSTATE.
///
/// Every DataFusion error used to map to `XX000` — "internal error (engine
/// fault)". Most of them are nothing of the sort: an unknown table, a column
/// typo, a type mismatch and a plain syntax error all arrive here, because
/// `impl From<DataFusionError> for SqlError` keeps the rendered string and
/// discards the variant. JDBC/ODBC clients key on SQLSTATE, so a user typo was
/// reported to them as an engine bug.
///
/// The variant is recoverable from the message: DataFusion renders every error
/// as `error_prefix() + message`, and those prefixes are fixed string literals
/// in one `match` (`datafusion-common/src/error.rs`). Matching them is stable
/// in a way that matching arbitrary message text would not be.
///
/// Anything unrecognised keeps `XX000`, so this can only ever be an
/// improvement on the previous blanket mapping.
fn datafusion_sqlstate(message: &str) -> &'static str {
    // User errors in the statement itself.
    if message.starts_with("SQL error: ") {
        return SYNTAX_ERROR;
    }
    if message.starts_with("Error during planning: ") {
        // `table '<ref>' not found` is the one planning failure with a
        // dedicated code that clients act on differently.
        return if message.contains("not found") || message.contains("No table named") {
            UNDEFINED_TABLE
        } else {
            SYNTAX_ERROR
        };
    }
    // "Schema error: No field named x" — an undefined column, i.e. the
    // statement refers to something that does not exist.
    if message.starts_with("Schema error: ") {
        return SYNTAX_ERROR;
    }
    if message.starts_with("This feature is not implemented: ") {
        return FEATURE_NOT_SUPPORTED;
    }
    // Bad data or a failed cast, not a broken engine.
    if message.starts_with("Arrow error: ") {
        return DATA_EXCEPTION;
    }
    // An external component failed: storage, network, a foreign library.
    if message.starts_with("Parquet error: ")
        || message.starts_with("Object Store error: ")
        || message.starts_with("IO error: ")
        || message.starts_with("External error: ")
        || message.starts_with("FFI error: ")
        || message.starts_with("Substrait error: ")
    {
        return SYSTEM_ERROR;
    }
    // Runtime conditions that are neither the statement's fault nor an engine
    // defect — resource limits, misconfiguration, execution-time failures.
    if message.starts_with("Resources exhausted: ")
        || message.starts_with("Invalid or Unsupported Configuration: ")
        || message.starts_with("Execution error: ")
        || message.starts_with("ExecutionJoin error: ")
    {
        return GENERAL_ERROR;
    }
    // "Internal error: " and anything else: an engine fault.
    INTERNAL_ERROR
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
        let e = SqlError::Unsupported {
            feature: "TABLESAMPLE".into(),
        };
        assert_eq!(sqlstate_for(&e), FEATURE_NOT_SUPPORTED);
    }

    #[test]
    fn unrecognised_datafusion_message_keeps_internal_error() {
        let e = SqlError::DataFusion {
            message: "panic in executor".into(),
        };
        assert_eq!(sqlstate_for(&e), INTERNAL_ERROR);
    }

    /// A user's mistake must not be reported to a JDBC/ODBC client as an
    /// engine fault. Every one of these used to map to `XX000`.
    #[test]
    fn datafusion_user_errors_do_not_map_to_internal_error() {
        let cases: &[(&str, &str)] = &[
            ("SQL error: ParserError(\"Expected: ...\")", SYNTAX_ERROR),
            (
                "Error during planning: table 'orders' not found",
                UNDEFINED_TABLE,
            ),
            ("Error during planning: No table named foo", UNDEFINED_TABLE),
            (
                "Error during planning: Coercion from [Utf8] to ... failed",
                SYNTAX_ERROR,
            ),
            ("Schema error: No field named custkey.", SYNTAX_ERROR),
            (
                "This feature is not implemented: GROUPING SETS",
                FEATURE_NOT_SUPPORTED,
            ),
            (
                "Arrow error: Cast error: Cannot cast 'x' to Int64",
                DATA_EXCEPTION,
            ),
            ("Object Store error: Generic S3 error", SYSTEM_ERROR),
            ("Parquet error: EOF", SYSTEM_ERROR),
            ("IO error: broken pipe", SYSTEM_ERROR),
            ("External error: connector failed", SYSTEM_ERROR),
            ("Resources exhausted: memory limit", GENERAL_ERROR),
            ("Execution error: divide by zero", GENERAL_ERROR),
            ("Internal error: this is a bug", INTERNAL_ERROR),
        ];
        for (message, expected) in cases {
            let error = SqlError::DataFusion {
                message: (*message).to_string(),
            };
            assert_eq!(
                sqlstate_for(&error),
                *expected,
                "wrong SQLSTATE for {message:?}"
            );
        }
    }

    /// The prefixes matched above are DataFusion's own, so they must survive a
    /// real error round-tripping through `From<DataFusionError>`.
    #[test]
    fn prefixes_match_real_datafusion_errors() {
        use datafusion::error::DataFusionError;

        let planning: SqlError = DataFusionError::Plan("table 'nope' not found".to_string()).into();
        assert_eq!(sqlstate_for(&planning), UNDEFINED_TABLE);

        let internal: SqlError = DataFusionError::Internal("bug".to_string()).into();
        assert_eq!(sqlstate_for(&internal), INTERNAL_ERROR);

        let exhausted: SqlError = DataFusionError::ResourcesExhausted("pool".to_string()).into();
        assert_eq!(sqlstate_for(&exhausted), GENERAL_ERROR);
    }

    #[test]
    fn access_denied_maps_to_insufficient_privilege() {
        let e = SqlError::AccessDenied {
            reason: "no read permission".into(),
        };
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
            SUCCESS,
            FEATURE_NOT_SUPPORTED,
            DATA_EXCEPTION,
            INVALID_AUTHORIZATION,
            SYNTAX_ERROR,
            INSUFFICIENT_PRIVILEGE,
            UNDEFINED_TABLE,
            QUERY_CANCELLED,
            QUERY_TIMEOUT,
            SYSTEM_ERROR,
            INTERNAL_ERROR,
            GENERAL_ERROR,
        ] {
            assert_eq!(code.len(), 5, "SQLSTATE {code} must be 5 characters");
        }
    }
}
