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

/// Fjall `ephemeral()` / `in_memory()` constructors are forbidden for durable profiles.
pub fn requires_file_backed_state(profile: DurabilityProfile) -> bool {
    profile_requires_durable_window_state(profile)
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
