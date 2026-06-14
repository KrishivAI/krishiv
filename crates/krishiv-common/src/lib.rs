#![forbid(unsafe_code)]

//! Shared utilities for the Krishiv workspace.
//!
//! Provides canonical implementations of:
//! - SHA-256 hashing (`hash` module)
//! - Identifier and path validation (`validate` module)

pub mod async_util;
pub mod backpressure;
#[cfg(feature = "chaos")]
pub mod chaos;
pub mod durability;
pub mod hash;
pub mod memory_budget;
pub mod panic_util;
pub mod partition;
pub mod production;
pub mod stream_quality;
pub mod test_fixtures;
pub mod validate;
pub mod write_commit;

pub use backpressure::BackpressureSignal;
pub use durability::{CheckpointDurability, DurabilityProfile, ShuffleDurability, StateDurability};
pub use memory_budget::MemoryBudget;
pub use panic_util::panic_payload_to_string;
pub use production::{
    ALLOW_ANONYMOUS_HTTP_ENV, NativeScalarUdfPolicy, PRODUCTION_ENV, allow_anonymous_http_override,
    allow_legacy_task_fragments, allows_alpha_api, allows_memory_checkpoint_uri,
    allows_remote_sql_comment_fallback, allows_unbounded_shuffle_store,
    forbids_simulation_connectors, is_production_mode, profile_forbids_native_scalar_udfs,
    profile_requires_authenticated_flight, profile_requires_authenticated_ui,
    profile_requires_durable_window_state, profile_requires_fail_closed_metadata,
    requires_file_backed_state, requires_http_auth, requires_manual_kafka_commit,
    resolve_durability_profile,
};
pub use stream_quality::{StreamQualityHook, StreamQualityResult};

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
