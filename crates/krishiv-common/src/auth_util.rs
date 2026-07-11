#![forbid(unsafe_code)]

//! Shared bearer-token parsing + redaction (Phase 51, audit §13a).
//!
//! Bearer auth was implemented four times (coordinator gRPC, executor task
//! gRPC, shuffle HTTP, Flight SQL) with per-site parse quirks — which is how
//! the §11 LOG-1 token-in-logs leak and §12 FLAG-2 parse skew happened per
//! site instead of once. This module is now the only place allowed to parse
//! an `Authorization` header: a source-scan test fails the build when
//! `strip_prefix("Bearer` appears anywhere else in the workspace.
//!
//! Logging rule: never log a raw token. Log [`redact_token`] output instead —
//! it is collision-resistant enough to correlate a caller across log lines
//! and far too short to recover a high-entropy credential.

/// Extract the token from an `Authorization: Bearer <token>` header value.
///
/// Returns `None` for a missing header, a non-Bearer scheme, or an
/// empty/whitespace-only token. The returned slice is trimmed.
pub fn bearer_token(header_value: Option<&str>) -> Option<&str> {
    header_value?
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

/// Redact a credential for logging: a 16-hex-char hash tagged `bearer:`.
///
/// Stable within a process run so operators can correlate requests from the
/// same caller, without ever writing token material to the log stream.
pub fn redact_token(token: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.hash(&mut hasher);
    format!("bearer:{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_token_parses_and_trims() {
        assert_eq!(bearer_token(Some("Bearer abc")), Some("abc"));
        assert_eq!(bearer_token(Some("Bearer  abc ")), Some("abc"));
        assert_eq!(bearer_token(Some("Bearer ")), None);
        assert_eq!(bearer_token(Some("Bearer   ")), None);
        assert_eq!(bearer_token(Some("Basic abc")), None);
        assert_eq!(bearer_token(Some("")), None);
        assert_eq!(bearer_token(None), None);
    }

    #[test]
    fn redact_token_never_contains_the_token() {
        let token = "super-secret-token-value";
        let redacted = redact_token(token);
        assert!(!redacted.contains(token));
        assert!(redacted.starts_with("bearer:"));
        assert_eq!(redacted.len(), "bearer:".len() + 16);
        // stable within a process
        assert_eq!(redacted, redact_token(token));
        // distinct tokens redact differently
        assert_ne!(redacted, redact_token("other-token"));
    }

    /// Structural guard (audit §11): hand-rolled bearer parsing must not
    /// reappear. Any `strip_prefix("Bearer` outside this module is a failure —
    /// new call sites must go through [`bearer_token`], which keeps parse
    /// semantics and redaction discipline in one reviewed place.
    #[test]
    fn bearer_parsing_exists_only_in_this_module() {
        fn scan(dir: &std::path::Path, hits: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).expect("read_dir") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if name == "target" || name.starts_with('.') {
                        continue;
                    }
                    scan(&path, hits);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs")
                    && !path.ends_with("krishiv-common/src/auth_util.rs")
                {
                    let src = std::fs::read_to_string(&path).expect("read");
                    if src.contains("strip_prefix(\"Bearer") {
                        hits.push(path.display().to_string());
                    }
                }
            }
        }
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .to_path_buf();
        let mut hits = Vec::new();
        scan(&crates, &mut hits);
        assert!(
            hits.is_empty(),
            "hand-rolled Bearer parsing found outside krishiv_common::auth_util \
             (route through auth_util::bearer_token): {hits:?}"
        );
    }
}
