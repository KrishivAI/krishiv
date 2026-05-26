#![forbid(unsafe_code)]

//! Public facade for `krishiv-state`.
//!
//! Keyed state API, in-memory backend (R5.1), and durable redb backend (R5.2).
//!
//! State must be accessed only within `process_batch` or
//! `flush_triggered_windows` on the executor operator loop — never from
//! timer callbacks.
//!
//! Backend summary:
//! - `InMemoryStateBackend` — R5.1; state is lost on executor restart.
//! - `RedbStateBackend` — R5.2; ACID-durable state backed by `redb`, a
//!   pure-Rust embedded B-tree database.  Supports file-backed persistence and
//!   an in-memory mode for tests.  All I/O is synchronous; callers must use
//!   `spawn_blocking` when called from async tasks.
//! - `RocksDbStateBackend` — type alias for `RedbStateBackend` (kept for
//!   source compatibility; the old filesystem-based placeholder is removed).

// Declared here:
pub mod incremental;
pub mod key_group;
pub mod migration;

// Named modules
pub mod backend;
pub mod error;
pub mod inspector;
pub mod memory;
pub mod namespace;
pub mod processing_time;
pub mod redb_backend;
pub mod snapshot;
pub mod timer;
pub mod ttl;

#[cfg(test)]
mod tests;

// Re-export the full public API at crate root for source compatibility.
pub use backend::StateBackend;
pub use error::{StateError, StateResult};
pub use inspector::StateInspector;
pub use memory::InMemoryStateBackend;
pub use migration::{
    SharedStateMigrationRegistry, StateMigrationError, StateMigrationFn, StateMigrationRegistry,
};
pub use namespace::Namespace;
pub use processing_time::{
    InMemoryProcessingTimeTimerService, ProcessingTimeTimerKey, ProcessingTimeTimerService,
};
pub use redb_backend::{RedbStateBackend, RocksDbStateBackend};
pub use snapshot::{SnapshotEntry, decode_snapshot_entries};
pub use timer::{InMemoryTimerService, TimerKey, TimerService};
pub use ttl::{TtlConfig, TtlStateBackend};
