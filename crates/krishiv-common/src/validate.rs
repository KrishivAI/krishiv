//! Canonical identifier and path validation.
//!
//! Consolidates validation logic from:
//! - `krishiv-shuffle::validate_safe_id` (blocklist approach)
//! - `krishiv-runtime::flight_protocol::{is_safe_identifier, is_safe_path, is_safe_base64}` (allowlist approach)
//! - `krishiv-ai::vector_sinks::validate_identifier` (strict SQL identifier)
//!
//! All crates that validate untrusted identifiers should import from here.

/// Error type for validation failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ValidationError {
    pub message: String,
}

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

/// Check that an identifier matches the safe character class `[A-Za-z0-9_.-]+`
/// and contains no `..` traversal sequence.
///
/// Rejects empty strings, any character outside the allowed set, and `..`.
/// Prevents comment-injection attacks (the `*/` sequence is impossible
/// inside a valid identifier) and keeps this allowlist consistent with
/// [`validate_safe_id`]'s blocklist: without the `..` check, a string like
/// `".."` would pass this allowlist's character class yet be rejected by
/// `validate_safe_id`, so a caller could be misled into treating it as safe
/// for path construction.
///
/// This is the canonical replacement for
/// `krishiv_runtime::flight_protocol::is_safe_identifier`.
pub fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && !s.contains("..")
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Check that a filesystem path field matches `[A-Za-z0-9_.-/ ]+` and
/// contains no `..` traversal sequence.
///
/// Same as `is_safe_identifier` plus `/` and space, with the same `..`
/// rejection for consistency with [`validate_safe_id`]. Prevents `*/`
/// injection through path fields in comment protocols and keeps this
/// allowlist from accepting traversal sequences that the canonical
/// path-safety blocklist would reject.
///
/// This is the canonical replacement for
/// `krishiv_runtime::flight_protocol::is_safe_path`.
pub fn is_safe_path(s: &str) -> bool {
    !s.is_empty()
        && !s.contains("..")
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
/// `krishiv_ai::vector_sinks::validate_identifier`.
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
    // SAFETY: non-empty guard above guarantees at least one char.
    let first = chars.next().expect("non-empty checked above");
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

    #[test]
    fn safe_identifier_dotdot_rejected() {
        // Regression: ".." matches the `[A-Za-z0-9_.-]+` character class but
        // must be rejected so this allowlist stays consistent with
        // `validate_safe_id`'s traversal blocklist (P1 roadmap finding).
        assert!(!is_safe_identifier(".."));
        assert!(!is_safe_identifier("../etc"));
        assert!(!is_safe_identifier("table..name"));
        assert!(is_safe_identifier("table.v1"));
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

    #[test]
    fn safe_path_dotdot_rejected() {
        // Regression: traversal sequences must be rejected consistently with
        // `validate_safe_id` (P1 roadmap finding).
        assert!(!is_safe_path("../etc/passwd"));
        assert!(!is_safe_path("bucket/../secret"));
        assert!(is_safe_path("bucket/key/file.v1.txt"));
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
