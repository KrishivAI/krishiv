#![forbid(unsafe_code)]

//! Shared utilities for the Krishiv workspace.
//!
//! Provides canonical implementations of:
//! - SHA-256 hashing (`hash` module)
//! - Identifier and path validation (`validate` module)

pub mod arrow;
pub mod async_util;
pub mod blocking;
#[cfg(feature = "chaos")]
pub mod chaos;
pub mod durability;
pub mod hash;
pub mod partition;
pub mod production;
pub mod validate;

pub use durability::{
    CheckpointDurability, DurabilityProfile, DurabilityProfileParseError, DurabilityProfileSpec,
    MetadataDurability, ShuffleDurability, StateDurability,
};
pub use production::{
    ALLOW_ANONYMOUS_HTTP_ENV, ALLOW_LEGACY_FRAGMENTS_ENV, DURABILITY_PROFILE_ENV,
    NativeScalarUdfPolicy, PRODUCTION_ENV, allow_anonymous_http_override,
    allow_legacy_task_fragments, allows_alpha_api, allows_memory_checkpoint_uri,
    allows_remote_sql_comment_fallback, allows_unbounded_shuffle_store,
    forbids_simulation_connectors, is_production_mode, profile_forbids_native_scalar_udfs,
    profile_requires_authenticated_flight, profile_requires_authenticated_ui,
    profile_requires_durable_window_state, profile_requires_fail_closed_metadata,
    requires_file_backed_state, requires_http_auth, requires_manual_kafka_commit,
    resolve_durability_profile,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_validate_independent() {
        let h = hash::sha256_hex(b"hello");
        assert_eq!(h.len(), 64);
        assert!(validate::is_safe_identifier("my-table"));
    }
}
