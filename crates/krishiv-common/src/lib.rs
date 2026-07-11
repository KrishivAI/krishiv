#![forbid(unsafe_code)]

//! Shared utilities for the Krishiv workspace.
//!
//! Cross-cutting primitives used by runtime, scheduler, executor, and connector
//! crates:
//!
//! - [`hash`] — SHA-256 helpers
//! - [`validate`] — identifier, path, and SQL identifier validation
//! - [`durability`] — durability profile contracts
//! - [`production`] — production-mode guards and policy hooks
//! - [`write_commit`] — staged sink commit and publish
//! - [`partition`] — Arrow record-batch partitioning
//! - [`async_util`] — `block_on` bridge and wall-clock helpers
//! - [`memory_budget`] — in-process memory accounting
//! - [`backpressure`] — credit-based backpressure signals
//! - [`stream_quality`] — streaming quality hook trait

pub mod async_util;
pub mod auth_util;
pub mod backpressure;
#[cfg(feature = "chaos")]
pub mod chaos;
pub mod durability;
pub mod env_registry;
pub mod hash;
pub mod memory_budget;
pub mod panic_util;
pub mod partition;
pub mod production;
pub mod sql_util;
pub mod stream_quality;
pub mod test_fixtures;
pub mod unified_memory_manager;
pub mod validate;
pub mod write_commit;

pub use backpressure::BackpressureSignal;
pub use auth_util::{bearer_token, redact_token};
pub use env_registry::{
    EnvIssue, FlagKind, FlagScope, FlagSpec, coordinator_url_env, env_u64, env_usize,
    log_env_issues, truthy_env, validate_env,
};
pub use durability::{CheckpointDurability, DurabilityProfile, ShuffleDurability, StateDurability};
pub use memory_budget::{MemoryBudget, cgroup_memory_limit_bytes};
pub use panic_util::panic_payload_to_string;
pub use production::{
    ALLOW_ANONYMOUS_HTTP_ENV, NativeScalarUdfPolicy, PRODUCTION_ENV, allow_anonymous_http_override,
    allow_legacy_task_fragments, allows_alpha_api, allows_memory_checkpoint_uri,
    allows_remote_sql_comment_fallback, allows_unbounded_shuffle_store,
    forbids_simulation_connectors, is_production_mode, profile_forbids_native_scalar_udfs,
    profile_requires_authenticated_flight, profile_requires_authenticated_shuffle,
    profile_requires_authenticated_ui, profile_requires_durable_window_state,
    profile_requires_fail_closed_metadata,
    requires_file_backed_state, requires_http_auth, requires_manual_kafka_commit,
    resolve_durability_profile,
};
pub use stream_quality::{StreamQualityHook, StreamQualityResult};
pub use unified_memory_manager::{
    MemoryRegion, MemoryUsageSnapshot, StageReservationMap, UnifiedMemoryConfig,
    UnifiedMemoryManager,
};

#[cfg(test)]
mod gap_tests;
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
