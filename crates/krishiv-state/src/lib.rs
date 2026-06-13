#![forbid(unsafe_code)]

//! Public facade for `krishiv-state`.
//!
//! Keyed state API with RocksDB backend for both ephemeral (tests/dev) and
//! durable (file-backed) state storage.
//!
//! State must be accessed only within `process_batch` or
//! `flush_triggered_windows` on the executor operator loop — never from
//! timer callbacks.

// Declared here:
pub mod checkpoint;
pub mod compatibility;
pub mod incremental;
pub mod key_group;
pub mod migration;

pub mod incremental_checkpoint;
pub mod queryable;
pub mod savepoint;

// Named modules
pub mod backend;
pub mod error;
pub mod inspector;
pub mod namespace;
pub mod processing_time;
pub mod rocksdb_backend;
pub mod snapshot;
pub mod timer;
pub mod ttl;

#[cfg(test)]
mod tests;

// Re-export the full public API at crate root for source compatibility.
pub use backend::StateBackend;
pub use checkpoint::rescaling::{
    EntryRouting, KeyGroupRescaler, RescaleChecksum, redistribute_snapshots, window_group_key,
};
pub use checkpoint::{
    CheckpointDurability, CheckpointError, CheckpointResult, CheckpointStorage,
    EphemeralCheckpointStorage, LocalFsCheckpointStorage, ObjectStoreCheckpointStorage,
    open_checkpoint_storage_from_uri,
};
pub use compatibility::OperatorStateDescriptor;
pub use error::{StateError, StateResult};
pub use incremental_checkpoint::{
    EpochMetaFile, RocksDbIncrementalCheckpointer, SstEpochManifest, SstFileRef,
};
pub use inspector::StateInspector;
pub use krishiv_common::durability::{DurabilityProfile, StateDurability};
pub use migration::{
    CURRENT_STATE_SCHEMA_VERSION, SharedStateMigrationRegistry, StateMigrationError,
    StateMigrationFn, StateMigrationRegistry, migrate_snapshot,
};
pub use namespace::Namespace;
pub use processing_time::{
    InMemoryProcessingTimeTimerService, ProcessingTimeTimerKey, ProcessingTimeTimerService,
};
pub use queryable::{QueryableStateHandle, QueryableStateStore};
/// Primary state backend — RocksDB-backed LSM-tree, ephemeral or file-backed.
pub use rocksdb_backend::RocksDbStateBackend;
pub use savepoint::{SAVEPOINT_FORMAT_VERSION, SavepointCoordinator, SavepointMeta};
pub use snapshot::{SnapshotEntry, decode_snapshot_entries, encode_snapshot_entries};
pub use timer::{InMemoryTimerService, TimerKey, TimerService};
pub use ttl::{TtlConfig, TtlStateBackend};
