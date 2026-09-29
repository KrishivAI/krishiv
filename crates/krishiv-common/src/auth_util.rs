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

/// Env flag that explicitly permits an unauthenticated listener.
pub const ALLOW_ANONYMOUS_ENV: &str = "KRISHIV_ALLOW_ANONYMOUS";

/// Refuse to serve `surface` without authentication on an address other
/// machines can reach, unless the operator opted out explicitly.
///
/// The durability-profile guards only fail closed under durable profiles, so
/// the default `dev-local` profile bound to `0.0.0.0` used to serve an open
/// data or control plane with nothing but a log line. A loopback bind stays
/// permissive (nothing off the host can reach it); anything wider needs either
/// credentials or a stated decision.
pub fn check_anonymous_exposure(
    surface: &str,
    addr: std::net::SocketAddr,
    authenticated: bool,
    anonymous_allowed: bool,
) -> Result<(), String> {
    if authenticated || anonymous_allowed || addr.ip().is_loopback() {
        return Ok(());
    }
    Err(format!(
        "refusing to serve {surface} on {addr} without authentication: the address is          reachable from other machines. Configure credentials for it, bind a loopback          address, or set {ALLOW_ANONYMOUS_ENV}=true to accept an open listener."
    ))
}

/// [`check_anonymous_exposure`] with the opt-out read from
/// [`ALLOW_ANONYMOUS_ENV`].
pub fn check_anonymous_exposure_from_env(
    surface: &str,
    addr: std::net::SocketAddr,
    authenticated: bool,
) -> Result<(), String> {
    check_anonymous_exposure(
        surface,
        addr,
        authenticated,
        crate::truthy_env(ALLOW_ANONYMOUS_ENV),
    )
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

    #[test]
    fn an_open_listener_off_loopback_needs_a_stated_decision() {
        let public: std::net::SocketAddr = "0.0.0.0:7000".parse().expect("addr");
        let local: std::net::SocketAddr = "127.0.0.1:7000".parse().expect("addr");
        let local6: std::net::SocketAddr = "[::1]:7000".parse().expect("addr");

        let refused = check_anonymous_exposure("shuffle", public, false, false)
            .expect_err("open public listener must be refused");
        assert!(refused.contains(ALLOW_ANONYMOUS_ENV), "{refused}");
        assert!(refused.contains("shuffle"), "{refused}");

        assert!(check_anonymous_exposure("shuffle", public, true, false).is_ok());
        assert!(check_anonymous_exposure("shuffle", public, false, true).is_ok());
        assert!(check_anonymous_exposure("shuffle", local, false, false).is_ok());
        assert!(check_anonymous_exposure("shuffle", local6, false, false).is_ok());
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
