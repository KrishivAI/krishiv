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
//! - `FjallStateBackend` — R5.1; state is lost on executor restart.
//! - `FjallStateBackend` — R5.2; ACID-durable state backed by `redb`, a
//!   pure-Rust embedded B-tree database.  Supports file-backed persistence and
//!   an in-memory mode for tests.  All I/O is synchronous; callers must use
//!   `spawn_blocking` when called from async tasks.
//!   removed; use `FjallStateBackend` directly.

// Declared here:
pub mod incremental;
pub mod key_group;
pub mod migration;

// Named modules
pub mod backend;
pub mod error;
pub mod inspector;
pub mod namespace;
pub mod processing_time;
pub mod fjall_backend;
pub mod snapshot;
pub mod timer;
pub mod ttl;

#[cfg(test)]
mod tests;

// Re-export the full public API at crate root for source compatibility.
pub use backend::StateBackend;
pub use error::{StateError, StateResult};
pub use inspector::StateInspector;
pub use krishiv_common::durability::{DurabilityProfile, StateDurability};
pub use migration::{
    SharedStateMigrationRegistry, StateMigrationError, StateMigrationFn, StateMigrationRegistry,
};
pub use namespace::Namespace;
pub use processing_time::{
    InMemoryProcessingTimeTimerService, ProcessingTimeTimerKey, ProcessingTimeTimerService,
};
pub use fjall_backend::FjallStateBackend;
pub use snapshot::{SnapshotEntry, decode_snapshot_entries};
pub use timer::{InMemoryTimerService, TimerKey, TimerService};
pub use ttl::{TtlConfig, TtlStateBackend};
