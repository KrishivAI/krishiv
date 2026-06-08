#![forbid(unsafe_code)]

//! Public facade for `krishiv-shuffle`.
//!
//! Shuffle data service: partitioning, local disk / object-store storage,
//! and Arrow-IPC-over-TCP transport of Arrow record batches.

// Named module facades for shuffle storage, metadata, and Flight I/O.
pub mod compression;
pub mod disk_store;
pub mod error;
pub mod flight;
pub mod lease_persistence;
pub mod local_store;
pub mod memory_store;
pub mod metadata;
pub mod object_store;
pub mod orphan;
pub mod partitioner;
pub mod path;
pub mod shuffle_svc;
pub mod storage_uri;
pub mod store;
pub mod tiered_store;

/// Validate that an identifier (job_id, stage_id, etc.) is safe for use in a
/// filesystem path.  Rejects empty strings and strings containing path
/// separators, null bytes, or parent-directory traversal (`..`).
///
/// S4 in crate-stability-resolution-plan — prevents path traversal via
/// untrusted identifiers flowing into disk/object/local-store paths.
pub fn validate_safe_id(id: &str, label: &str) -> ShuffleResult<()> {
    krishiv_common::validate::validate_safe_id(id, label).map_err(|e| {
        ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e.message,
        ))
    })
}

// Re-export the public API at the crate root for source compatibility.
pub use compression::{CompressionCodec, ShuffleCompression};
pub use disk_store::LocalDiskShuffleStore;
pub use error::{ShuffleError, ShuffleResult, StoreResult};
pub use krishiv_common::durability::{DurabilityProfile, ShuffleDurability};
pub use local_store::LocalShuffleStore;
pub use memory_store::InMemoryShuffleStore;
pub use metadata::{PartitionState, ShuffleMetadata};
pub use object_store::ObjectStoreShuffleStore;
pub use orphan::{cleanup_orphans, scan_orphans};
pub use partitioner::HashPartitioner;
pub use path::ShufflePath;
pub use storage_uri::open_shuffle_backend_from_uri;
pub use store::{PartitionId, ShuffleBackend, ShufflePartition, ShuffleStore, ShuffleStream};
pub use tiered_store::TieredShuffleStore;


#[cfg(test)]
mod tests;
