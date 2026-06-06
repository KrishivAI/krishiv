//! Production-mode guards shared across Krishiv crates.
//!
//! Set `KRISHIV_PRODUCTION=1` to enable fail-closed behavior for metadata writes,
//! anonymous HTTP/Flight surfaces, simulation connectors, and legacy task fragments.

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
    std::env::var(DURABILITY_PROFILE_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DurabilityProfile::DevLocal)
}

/// Returns whether the process is running in production mode.
pub fn is_production_mode() -> bool {
    truthy_env(PRODUCTION_ENV)
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
pub fn allow_anonymous_http_override() -> bool {
    truthy_env(ALLOW_ANONYMOUS_HTTP_ENV)
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
    truthy_env(ALLOW_FULL_PRIVILEGE_UDFS_ENV)
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
}
