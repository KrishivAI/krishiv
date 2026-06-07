//! Production-mode guards shared across Krishiv crates.
//!
//! Set `KRISHIV_PRODUCTION=1` to enable fail-closed behavior for metadata writes,
//! anonymous HTTP/Flight surfaces, simulation connectors, and legacy task fragments.

use std::sync::OnceLock;

use crate::durability::DurabilityProfile;

/// When set to `1`, `true`, or `yes`, enables production fail-closed defaults.
pub const PRODUCTION_ENV: &str = "KRISHIV_PRODUCTION";

/// Opt-in escape hatch for legacy untyped task fragment strings.
pub const ALLOW_LEGACY_FRAGMENTS_ENV: &str = "KRISHIV_ALLOW_LEGACY_FRAGMENTS";

/// When set, allows anonymous coordinator HTTP even in durable profiles (dev only).
pub const ALLOW_ANONYMOUS_HTTP_ENV: &str = "KRISHIV_ALLOW_ANONYMOUS_HTTP";

/// Process-wide durability profile (`KRISHIV_DURABILITY_PROFILE`).
pub const DURABILITY_PROFILE_ENV: &str = "KRISHIV_DURABILITY_PROFILE";

/// Resolve the active durability profile from the environment.
pub fn resolve_durability_profile() -> DurabilityProfile {
    resolve_durability_profile_from(std::env::var(DURABILITY_PROFILE_ENV).ok())
}

/// Resolve a durability profile from an already-read env value.
///
/// Factored out of `resolve_durability_profile` so the parse-failure fallback
/// (an invalid `KRISHIV_DURABILITY_PROFILE` value silently resolving to
/// `DevLocal` with a logged warning) can be exercised directly in tests
/// without mutating process-global environment state.
fn resolve_durability_profile_from(value: Option<String>) -> DurabilityProfile {
    value
        .and_then(|value| match value.parse() {
            Ok(profile) => Some(profile),
            Err(e) => {
                tracing::warn!(
                    env = DURABILITY_PROFILE_ENV,
                    value = %value,
                    error = %e,
                    "invalid durability profile; falling back to DevLocal"
                );
                None
            }
        })
        .unwrap_or(DurabilityProfile::DevLocal)
}

/// Returns whether the process is running in production mode.
///
/// Cached on first call via `OnceLock` so the env var is read exactly once per
/// process lifetime, guaranteeing a consistent value across all callers.
pub fn is_production_mode() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| truthy_env(PRODUCTION_ENV))
}

fn truthy_env(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Whether untyped legacy task fragments (`stream:*`, raw SQL strings) are permitted.
pub fn allow_legacy_task_fragments(profile: DurabilityProfile) -> bool {
    if truthy_env(ALLOW_LEGACY_FRAGMENTS_ENV) {
        return true;
    }
    profile == DurabilityProfile::DevLocal && !is_production_mode()
}

/// Metadata/event writes must not be dropped silently when this returns true.
pub fn profile_requires_fail_closed_metadata(profile: DurabilityProfile) -> bool {
    profile != DurabilityProfile::DevLocal || is_production_mode()
}

/// Window operator state must survive restarts when this returns true.
pub fn profile_requires_durable_window_state(profile: DurabilityProfile) -> bool {
    matches!(
        profile,
        DurabilityProfile::SingleNodeDurable | DurabilityProfile::DistributedDurable
    )
}

/// State backends must be file-backed (not ephemeral/in-memory) when this returns true.
pub fn requires_file_backed_state(profile: DurabilityProfile) -> bool {
    profile_requires_durable_window_state(profile) || is_production_mode()
}

/// In-memory simulation connectors (transactional Kafka, etc.) are forbidden.
pub fn forbids_simulation_connectors(profile: DurabilityProfile) -> bool {
    matches!(
        profile,
        DurabilityProfile::SingleNodeDurable | DurabilityProfile::DistributedDurable
    ) || is_production_mode()
}

/// Anonymous HTTP control-plane routes must be rejected when this returns true.
pub fn requires_http_auth(profile: DurabilityProfile) -> bool {
    profile != DurabilityProfile::DevLocal || is_production_mode()
}

/// Whether anonymous HTTP is explicitly allowed via env override.
///
/// Logs a warning at first call when the override is active in production mode,
/// since this bypasses HTTP authentication for control-plane routes.
/// Cached on first call via `OnceLock`.
pub fn allow_anonymous_http_override() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let enabled = truthy_env(ALLOW_ANONYMOUS_HTTP_ENV);
        if enabled && is_production_mode() {
            tracing::warn!(
                env = ALLOW_ANONYMOUS_HTTP_ENV,
                "anonymous HTTP override is active in production mode; \
                 control-plane routes are unauthenticated"
            );
        }
        enabled
    })
}

/// Kafka SQL streaming should disable auto-commit when this returns true.
pub fn requires_manual_kafka_commit(profile: DurabilityProfile) -> bool {
    profile_requires_durable_window_state(profile) || is_production_mode()
}

/// `memory://` checkpoint URIs are dev-only.
pub fn allows_memory_checkpoint_uri(profile: DurabilityProfile) -> bool {
    profile == DurabilityProfile::DevLocal && !is_production_mode()
}

/// Public in-memory shuffle constructors should be capped or hidden.
pub fn allows_unbounded_shuffle_store(profile: DurabilityProfile) -> bool {
    profile == DurabilityProfile::DevLocal && !is_production_mode()
}

/// Whether remote Flight SQL-comment fallbacks are permitted (dev-local only).
pub fn allows_remote_sql_comment_fallback() -> bool {
    allow_legacy_task_fragments(resolve_durability_profile())
}

/// Whether alpha / placeholder public APIs may be invoked.
pub fn allows_alpha_api() -> bool {
    resolve_durability_profile() == DurabilityProfile::DevLocal && !is_production_mode()
}

/// Opt-in escape hatch for native scalar UDF execution under durable profiles.
pub const ALLOW_FULL_PRIVILEGE_UDFS_ENV: &str = "KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS";

/// Immutable native scalar UDF policy used across one registration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeScalarUdfPolicy {
    profile: DurabilityProfile,
    forbidden: bool,
}

impl NativeScalarUdfPolicy {
    /// Snapshot the current process policy for `profile`.
    pub fn resolve(profile: DurabilityProfile) -> Self {
        Self {
            profile,
            forbidden: profile_forbids_native_scalar_udfs(profile),
        }
    }

    /// Construct a policy from an already-resolved decision.
    pub const fn from_decision(profile: DurabilityProfile, forbidden: bool) -> Self {
        Self { profile, forbidden }
    }

    /// Durability profile associated with this decision.
    pub const fn profile(self) -> DurabilityProfile {
        self.profile
    }

    /// Whether native scalar UDF registration and execution are forbidden.
    pub const fn is_forbidden(self) -> bool {
        self.forbidden
    }
}

fn allows_full_privilege_udfs() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| truthy_env(ALLOW_FULL_PRIVILEGE_UDFS_ENV))
}

/// Native scalar UDF execution is forbidden under durable profiles unless opted in.
pub fn profile_forbids_native_scalar_udfs(profile: DurabilityProfile) -> bool {
    if allows_full_privilege_udfs() {
        return false;
    }
    profile_requires_durable_window_state(profile) || is_production_mode()
}

/// Flight SQL and UI surfaces require API keys under durable profiles.
pub fn profile_requires_authenticated_flight(profile: DurabilityProfile) -> bool {
    requires_http_auth(profile)
}

/// Whether anonymous UI/status HTTP is forbidden.
pub fn profile_requires_authenticated_ui(profile: DurabilityProfile) -> bool {
    requires_http_auth(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_local_allows_legacy_fragments_by_default() {
        assert!(allow_legacy_task_fragments(DurabilityProfile::DevLocal));
    }

    #[test]
    fn durable_profiles_require_fail_closed_metadata() {
        assert!(profile_requires_fail_closed_metadata(
            DurabilityProfile::SingleNodeDurable
        ));
        assert!(profile_requires_fail_closed_metadata(
            DurabilityProfile::DistributedDurable
        ));
    }

    #[test]
    fn durable_profiles_forbid_simulation_connectors() {
        assert!(forbids_simulation_connectors(
            DurabilityProfile::DistributedDurable
        ));
    }

    /// Regression: a malformed `KRISHIV_DURABILITY_PROFILE` value must fall back
    /// to `DevLocal` (with a logged warning) rather than panicking or silently
    /// resolving to a more-durable profile than configured.
    #[test]
    fn malformed_durability_profile_value_falls_back_to_dev_local() {
        assert_eq!(
            resolve_durability_profile_from(Some("not-a-real-profile".to_string())),
            DurabilityProfile::DevLocal
        );
        assert_eq!(
            resolve_durability_profile_from(Some(String::new())),
            DurabilityProfile::DevLocal
        );
    }

    #[test]
    fn missing_durability_profile_env_falls_back_to_dev_local() {
        assert_eq!(
            resolve_durability_profile_from(None),
            DurabilityProfile::DevLocal
        );
    }

    #[test]
    fn valid_durability_profile_value_is_honored() {
        assert_eq!(
            resolve_durability_profile_from(Some("single-node-durable".to_string())),
            DurabilityProfile::SingleNodeDurable
        );
    }
}
