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
pub use checkpoint::{
    CheckpointDurability, CheckpointError, CheckpointResult, CheckpointStorage,
    EphemeralCheckpointStorage, LocalFsCheckpointStorage, ObjectStoreCheckpointStorage,
    open_checkpoint_storage_from_uri,
};
pub use error::{StateError, StateResult};
pub use inspector::StateInspector;
/// Primary state backend — RocksDB-backed LSM-tree, ephemeral or file-backed.
pub use rocksdb_backend::RocksDbStateBackend;
/// Legacy alias kept for source compatibility.
pub type FjallStateBackend = RocksDbStateBackend;
pub use krishiv_common::durability::{DurabilityProfile, StateDurability};
pub use migration::{
    SharedStateMigrationRegistry, StateMigrationError, StateMigrationFn, StateMigrationRegistry,
};
pub use namespace::Namespace;
pub use processing_time::{
    InMemoryProcessingTimeTimerService, ProcessingTimeTimerKey, ProcessingTimeTimerService,
};
pub use snapshot::{SnapshotEntry, decode_snapshot_entries};
pub use timer::{InMemoryTimerService, TimerKey, TimerService};
pub use ttl::{TtlConfig, TtlStateBackend};
pub use queryable::{QueryableStateHandle, QueryableStateStore};
pub use incremental_checkpoint::{
    EpochMetaFile, RocksDbIncrementalCheckpointer, SstEpochManifest, SstFileRef,
};
pub use savepoint::{SavepointCoordinator, SavepointMeta};
