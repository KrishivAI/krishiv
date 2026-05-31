//! Canonical identifier and path validation.
//!
//! Consolidates validation logic from:
//! - `krishiv-shuffle::validate_safe_id` (blocklist approach)
//! - `krishiv-runtime::flight_protocol::{is_safe_identifier, is_safe_path, is_safe_base64}` (allowlist approach)
//! - `krishiv-vector-sinks::validate_identifier` (strict SQL identifier)
//!
//! All crates that validate untrusted identifiers should import from here.

/// Error type for validation failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ValidationError {}

/// Validate that an identifier is safe for use in filesystem paths.
///
/// Uses a blocklist approach: rejects empty strings and strings containing
/// path separators (`/`, `\`), null bytes (`\0`), or parent-directory
/// traversal (`..`).
///
/// This is the canonical replacement for `krishiv_shuffle::validate_safe_id`.
///
/// ```
/// use krishiv_common::validate::validate_safe_id;
/// assert!(validate_safe_id("my-job", "job_id").is_ok());
/// assert!(validate_safe_id("../etc/passwd", "job_id").is_err());
/// assert!(validate_safe_id("", "job_id").is_err());
/// ```
pub fn validate_safe_id(id: &str, label: &str) -> Result<(), ValidationError> {
    if id.is_empty() {
        return Err(ValidationError {
            message: format!("{label} cannot be empty"),
        });
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') || id.contains("..") {
        return Err(ValidationError {
            message: format!("{label} contains invalid characters: {id}"),
        });
    }
    Ok(())
}

/// Check that an identifier matches the safe character class `[A-Za-z0-9_.-]+`.
///
/// Rejects empty strings and any character outside the allowed set.
/// Prevents comment-injection attacks (the `*/` sequence is impossible
/// inside a valid identifier).
///
/// This is the canonical replacement for
/// `krishiv_runtime::flight_protocol::is_safe_identifier`.
pub fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Check that a filesystem path field matches `[A-Za-z0-9_.-/ ]+`.
///
/// Same as `is_safe_identifier` plus `/` and space. Prevents `*/` injection
/// through path fields in comment protocols.
///
/// This is the canonical replacement for
/// `krishiv_runtime::flight_protocol::is_safe_path`.
pub fn is_safe_path(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/' || c == ' '
        })
}

/// Check that a base64 payload matches `[A-Za-z0-9+/=]+`.
///
/// Rejects empty strings and characters outside the base64 alphabet.
///
/// This is the canonical replacement for
/// `krishiv_runtime::flight_protocol::is_safe_base64`.
pub fn is_safe_base64(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
}

/// Validate a SQL/GraphQL identifier: `^[A-Za-z_][A-Za-z0-9_]*$`.
///
/// Stricter than `is_safe_identifier` — requires the identifier to start
/// with a letter or underscore, and only allows alphanumeric + underscore.
///
/// This is the canonical replacement for
/// `krishiv_vector_sinks::validate_identifier`.
///
/// ```
/// use krishiv_common::validate::validate_sql_identifier;
/// assert!(validate_sql_identifier("my_table").is_ok());
/// assert!(validate_sql_identifier("1table").is_err());
/// assert!(validate_sql_identifier("my-table").is_err());
/// ```
pub fn validate_sql_identifier(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError {
            message: "identifier cannot be empty".into(),
        });
    }
    let mut chars = name.chars();
    let first = chars.next().ok_or_else(|| ValidationError {
        message: "identifier cannot be empty".into(),
    })?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(ValidationError {
            message: format!("invalid identifier (must start with letter or _): {name}"),
        });
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ValidationError {
            message: format!("invalid identifier (only alphanumeric + _ allowed): {name}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_safe_id ──────────────────────────────────────────────

    #[test]
    fn safe_id_valid() {
        assert!(validate_safe_id("my-job", "job_id").is_ok());
        assert!(validate_safe_id("abc123", "stage_id").is_ok());
    }

    #[test]
    fn safe_id_empty_rejected() {
        assert!(validate_safe_id("", "job_id").is_err());
    }

    #[test]
    fn safe_id_slash_rejected() {
        assert!(validate_safe_id("job/stage", "id").is_err());
    }

    #[test]
    fn safe_id_backslash_rejected() {
        assert!(validate_safe_id("job\\stage", "id").is_err());
    }

    #[test]
    fn safe_id_null_rejected() {
        assert!(validate_safe_id("job\0stage", "id").is_err());
    }

    #[test]
    fn safe_id_dotdot_rejected() {
        assert!(validate_safe_id("../etc/passwd", "id").is_err());
        assert!(validate_safe_id("job..stage", "id").is_err());
    }

    // ── is_safe_identifier ────────────────────────────────────────────

    #[test]
    fn safe_identifier_valid() {
        assert!(is_safe_identifier("my-table"));
        assert!(is_safe_identifier("table_v2"));
        assert!(is_safe_identifier("a"));
        assert!(is_safe_identifier("123"));
    }

    #[test]
    fn safe_identifier_empty_rejected() {
        assert!(!is_safe_identifier(""));
    }

    #[test]
    fn safe_identifier_special_chars_rejected() {
        assert!(!is_safe_identifier("my table"));
        assert!(!is_safe_identifier("table/name"));
        assert!(!is_safe_identifier("table\0name"));
    }

    // ── is_safe_path ──────────────────────────────────────────────────

    #[test]
    fn safe_path_valid() {
        assert!(is_safe_path("/tmp/data.parquet"));
        assert!(is_safe_path("bucket/key/file.txt"));
    }

    #[test]
    fn safe_path_empty_rejected() {
        assert!(!is_safe_path(""));
    }

    #[test]
    fn safe_path_special_chars_rejected() {
        assert!(!is_safe_path("path\0with null"));
        assert!(!is_safe_path("path*with star"));
    }

    // ── is_safe_base64 ────────────────────────────────────────────────

    #[test]
    fn safe_base64_valid() {
        assert!(is_safe_base64("SGVsbG8="));
        assert!(is_safe_base64("abc123+/"));
    }

    #[test]
    fn safe_base64_empty_rejected() {
        assert!(!is_safe_base64(""));
    }

    #[test]
    fn safe_base64_special_chars_rejected() {
        assert!(!is_safe_base64("has space"));
        assert!(!is_safe_base64("has*star"));
    }

    // ── validate_sql_identifier ────────────────────────────────────────

    #[test]
    fn sql_identifier_valid() {
        assert!(validate_sql_identifier("my_table").is_ok());
        assert!(validate_sql_identifier("_private").is_ok());
        assert!(validate_sql_identifier("Table123").is_ok());
    }

    #[test]
    fn sql_identifier_empty_rejected() {
        assert!(validate_sql_identifier("").is_err());
    }

    #[test]
    fn sql_identifier_starts_with_digit_rejected() {
        assert!(validate_sql_identifier("1table").is_err());
    }

    #[test]
    fn sql_identifier_hyphen_rejected() {
        assert!(validate_sql_identifier("my-table").is_err());
    }

    #[test]
    fn sql_identifier_dot_rejected() {
        assert!(validate_sql_identifier("schema.table").is_err());
    }
}
