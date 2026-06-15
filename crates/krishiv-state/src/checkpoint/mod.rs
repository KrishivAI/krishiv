#![forbid(unsafe_code)]

//! Checkpoint and savepoint storage for Krishiv R6.
//!
//! This crate provides:
//! - [`CheckpointStorage`] trait — filesystem or object-store backend.
//! - [`LocalFsCheckpointStorage`] — `std::fs`-backed implementation for tests.
//! - [`CheckpointMetadata`] — versioned JSON envelope for committed epochs.
//! - [`IntegrityManifest`] — SHA-256 per-file hashes written alongside metadata.
//! - Free helper functions that compose the four [`CheckpointStorage`] primitives
//!   into higher-level checkpoint operations.
//!
//! # Key layout
//!
//! ```text
//! {base_dir}/{job_id}/checkpoints/{epoch:020}/metadata.json
//! {base_dir}/{job_id}/checkpoints/{epoch:020}/{op_id}/{task_id}/state.bin
//! {base_dir}/{job_id}/checkpoints/{epoch:020}/manifest.sha256
//! ```
//!
//! An epoch is considered complete only when `manifest.sha256` is present,
//! covers `metadata.json`, the metadata belongs to the requested job/epoch, and
//! every manifest-listed file passes SHA-256 validation.  Epochs missing the
//! manifest or required metadata coverage are treated as incomplete during
//! restore.

mod ephemeral;
mod io;
mod local_fs;
mod metadata;
mod paths;
mod storage_trait;

#[cfg(test)]
mod tests;

pub mod object_store;
pub mod rescaling;
pub mod storage_uri;

pub use krishiv_common::durability::{CheckpointDurability, DurabilityProfile};
pub use object_store::ObjectStoreCheckpointStorage;
pub use storage_uri::open_checkpoint_storage_from_uri;

pub use metadata::{
    CheckpointError, CheckpointMetadata, CheckpointResult, IntegrityManifest,
    OperatorSnapshotRef, SourceOffsetRecord,
};
pub use paths::{epoch_dir, manifest_path, metadata_path, snapshot_path};
pub use storage_trait::{CheckpointStorage, run_blocking_on_tokio};
pub use io::{
    create_savepoint, create_savepoint_at_epoch, delete_epoch, delete_epoch_async,
    delete_savepoint, latest_valid_epoch, latest_valid_epoch_async, list_savepoints,
    list_valid_epochs, list_valid_epochs_async, read_epoch_metadata,
    read_epoch_metadata_async, read_operator_snapshot, read_operator_snapshot_async,
    restore_savepoint, validate_epoch, validate_epoch_async, validate_fencing_token,
    validate_fencing_token_for_restore, write_epoch_hint, write_epoch_hint_async,
    write_epoch_metadata, write_epoch_metadata_async, write_manifest, write_manifest_async,
    write_operator_snapshot, write_operator_snapshot_async,
};
pub use ephemeral::EphemeralCheckpointStorage;
pub use local_fs::LocalFsCheckpointStorage;
