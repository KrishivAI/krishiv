//! Shared shuffle data-plane token authentication (SEC-3, Phase 63).
//!
//! Both the HTTP shuffle service ([`crate::shuffle_svc`]) and the Arrow Flight
//! shuffle server ([`crate::flight`]) carry intermediate query results — real
//! user data — between executors. Under a durable/production profile they MUST
//! be authenticated with a shared bearer token; a missing token is a
//! fail-closed startup error, not a silently-open endpoint.
//!
//! The token is a symmetric secret shared by every executor (and the
//! coordinator) via `KRISHIV_SHUFFLE_TOKEN` or `KRISHIV_SHUFFLE_TOKEN_FILE`.

use std::sync::OnceLock;

use constant_time_eq::constant_time_eq;
use krishiv_common::durability::DurabilityProfile;

/// Env var holding the shuffle bearer token inline.
pub(crate) const SHUFFLE_TOKEN_ENV: &str = "KRISHIV_SHUFFLE_TOKEN";
/// Env var holding a path to a file whose contents are the shuffle token.
pub(crate) const SHUFFLE_TOKEN_FILE_ENV: &str = "KRISHIV_SHUFFLE_TOKEN_FILE";

/// Resolve the shuffle token from `KRISHIV_SHUFFLE_TOKEN`, falling back to the
/// file named by `KRISHIV_SHUFFLE_TOKEN_FILE`. Returns `None` when neither is
/// set (permitted only under `DevLocal`).
pub(crate) fn resolve_shuffle_token() -> Option<String> {
    if let Ok(v) = std::env::var(SHUFFLE_TOKEN_ENV) {
        let t = v.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Ok(path) = std::env::var(SHUFFLE_TOKEN_FILE_ENV)
        && !path.is_empty()
        && let Ok(contents) = std::fs::read_to_string(&path)
    {
        let t = contents.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    None
}

/// Process-cached shuffle token for the client fetch/push path, read once.
///
/// Executors are long-lived and share a single token via env/secret, so caching
/// avoids a filesystem read on every partition fetch. The server side reads the
/// token freshly at startup (and, for the HTTP service, supports live reload);
/// the client cache is acceptable because a rotated token is picked up on the
/// next executor restart.
pub(crate) fn cached_shuffle_token() -> Option<&'static str> {
    static CACHED: OnceLock<Option<String>> = OnceLock::new();
    CACHED.get_or_init(resolve_shuffle_token).as_deref()
}

/// SEC-3 startup guard: refuse to run an unauthenticated shuffle data plane
/// under a profile that requires shuffle auth.
///
/// Extracted so the fail-closed rule is unit-testable without binding a socket.
pub(crate) fn require_shuffle_token_or_fail(
    token_present: bool,
    profile: DurabilityProfile,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !token_present && krishiv_common::profile_requires_authenticated_shuffle(profile) {
        return Err(format!(
            "shuffle auth is required under durability profile '{profile}' but no shuffle \
             token is configured; set KRISHIV_SHUFFLE_TOKEN or KRISHIV_SHUFFLE_TOKEN_FILE. \
             Refusing to start an unauthenticated shuffle data plane (SEC-3)."
        )
        .into());
    }
    Ok(())
}

/// Constant-time check of a presented `Authorization` header value against the
/// expected `Bearer <token>`.
///
/// Returns `true` when `expected` is `None` (auth disabled — only reachable
/// under `DevLocal`, enforced at startup by [`require_shuffle_token_or_fail`])
/// or when the header matches in constant time.
pub(crate) fn bearer_ok(provided_authorization: &str, expected: Option<&str>) -> bool {
    match expected {
        None => true,
        Some(tok) => {
            let want = format!("Bearer {tok}");
            constant_time_eq(provided_authorization.as_bytes(), want.as_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_fails_closed_without_token_under_durable_profiles() {
        assert!(require_shuffle_token_or_fail(false, DurabilityProfile::SingleNodeDurable).is_err());
        assert!(
            require_shuffle_token_or_fail(false, DurabilityProfile::DistributedDurable).is_err()
        );
        // DevLocal without a token stays permissive (eval/loopback).
        assert!(require_shuffle_token_or_fail(false, DurabilityProfile::DevLocal).is_ok());
        // A configured token is always accepted, every profile.
        assert!(require_shuffle_token_or_fail(true, DurabilityProfile::DistributedDurable).is_ok());
        assert!(require_shuffle_token_or_fail(true, DurabilityProfile::DevLocal).is_ok());
    }

    #[test]
    fn bearer_ok_disabled_when_no_expected_token() {
        assert!(bearer_ok("", None));
        assert!(bearer_ok("anything", None));
    }

    #[test]
    fn bearer_ok_requires_exact_match() {
        assert!(bearer_ok("Bearer s3cret", Some("s3cret")));
        assert!(!bearer_ok("Bearer wrong", Some("s3cret")));
        assert!(!bearer_ok("s3cret", Some("s3cret"))); // missing "Bearer " prefix
        assert!(!bearer_ok("", Some("s3cret")));
    }
}
