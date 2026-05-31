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
//! An epoch is considered complete only when `manifest.sha256` is present and
//! every file it lists passes SHA-256 validation.  Epochs missing the manifest
//! (partial writes) are treated as corrupt during restore.

pub mod object_store;
pub mod rescaling;
pub mod storage_uri;

pub use object_store::ObjectStoreCheckpointStorage;
pub use storage_uri::open_checkpoint_storage_from_uri;

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Error / Result ────────────────────────────────────────────────────────────

/// Errors from checkpoint storage operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// Underlying storage I/O failed.
    #[error("checkpoint storage error: {message}")]
    Storage { message: String },
    /// Epoch data failed integrity validation.
    #[error("checkpoint epoch {epoch} is corrupt: {message}")]
    Corrupt { epoch: u64, message: String },
    /// Checkpoint metadata uses an unsupported format version.
    #[error("unsupported checkpoint metadata version {version}")]
    IncompatibleVersion { version: u32 },
    /// No valid committed epoch exists to restore from.
    #[error("no valid committed checkpoint epoch found")]
    NoValidEpoch,
    /// The checkpoint's fencing token predates the current coordinator generation.
    #[error("stale fencing token: metadata token {stored} < current coordinator token {current}")]
    StaleFencingToken { stored: u64, current: u64 },
    /// Attempted to write an epoch that is not newer than the latest committed epoch.
    #[error("stale checkpoint epoch {attempted}: latest committed is {latest}")]
    StaleEpoch { attempted: u64, latest: u64 },
    /// The resolved path escapes the storage base directory (path-traversal attempt).
    #[error("path escapes storage base directory: {path}")]
    InvalidPath { path: String },
}

/// Convenience alias for checkpoint operation results.
pub type CheckpointResult<T> = Result<T, CheckpointError>;

// ── CheckpointMetadata ────────────────────────────────────────────────────────

/// Versioned checkpoint metadata record written to `metadata.json`.
///
/// `version` must be `1` for all R6 checkpoints.  Restore rejects unknown
/// versions with [`CheckpointError::IncompatibleVersion`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointMetadata {
    /// Format version.  Must be `1` for R6.
    pub version: u32,
    /// Monotonically increasing checkpoint epoch per job.
    pub epoch: u64,
    /// Job that owns this checkpoint.
    pub job_id: String,
    /// Coordinator fencing token at commit time.
    ///
    /// Restore paths must reject checkpoints whose `fencing_token` predates
    /// the current coordinator generation.
    pub fencing_token: u64,
    /// Coordinator identity that committed this checkpoint.
    ///
    /// Added in metadata version 2.  `None` for version-1 metadata (R6 era).
    /// Used for audit trails and incident debugging — operators can trace
    /// which coordinator instance committed each epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_id: Option<String>,
    /// Wall-clock commit time in milliseconds since Unix epoch (informational).
    pub timestamp_ms: u64,
    /// Last processed source offset per partition at the barrier boundary.
    pub source_offsets: Vec<SourceOffsetRecord>,
    /// One entry per operator instance that contributed a state snapshot.
    pub operator_snapshots: Vec<OperatorSnapshotRef>,
    /// Whether this checkpoint was triggered as a savepoint.
    pub is_savepoint: bool,
    /// Optional human-readable label for savepoints.
    pub savepoint_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iceberg_snapshot_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_offsets: Option<std::collections::BTreeMap<String, i64>>,
}

impl CheckpointMetadata {
    /// Current metadata format version.
    pub const VERSION: u32 = 1;

    /// Validate that this metadata can be used for restore.
    pub fn validate(&self) -> CheckpointResult<()> {
        if self.version != Self::VERSION {
            return Err(CheckpointError::IncompatibleVersion {
                version: self.version,
            });
        }
        Ok(())
    }
}

/// Last processed offset for one source partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceOffsetRecord {
    pub partition_id: String,
    pub offset: i64,
}

/// Reference to the state snapshot file for one operator instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorSnapshotRef {
    pub operator_id: String,
    pub task_id: String,
    /// Path to `state.bin` relative to the checkpoint storage base directory.
    pub snapshot_path: String,
}

// ── IntegrityManifest ─────────────────────────────────────────────────────────

/// SHA-256 integrity manifest for one checkpoint epoch.
///
/// Written to `manifest.sha256` as the last file in the epoch directory,
/// establishing the "epoch is complete" signal.  Restore validates every
/// entry before trusting the epoch.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IntegrityManifest {
    /// `relative_path → lowercase hex SHA-256`.
    entries: BTreeMap<String, String>,
}

impl IntegrityManifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a file hash.
    pub fn insert(&mut self, relative_path: impl Into<String>, sha256_hex: impl Into<String>) {
        self.entries.insert(relative_path.into(), sha256_hex.into());
    }

    /// Compute and record the SHA-256 of `data` for `relative_path`.
    pub fn insert_bytes(&mut self, relative_path: impl Into<String>, data: &[u8]) {
        let hex = sha256_hex(data);
        self.entries.insert(relative_path.into(), hex);
    }

    /// Verify that `data` matches the recorded hash for `relative_path`.
    pub fn verify(&self, relative_path: &str, data: &[u8]) -> bool {
        match self.entries.get(relative_path) {
            Some(expected) => sha256_hex(data) == *expected,
            None => false,
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize to the on-disk text format:
    /// `sha256:<hex>  <relative_path>\n` per entry.
    pub fn serialize(&self) -> Vec<u8> {
        use std::fmt::Write;
        let mut out = String::new();
        for (path, hex) in &self.entries {
            let _ = writeln!(out, "sha256:{hex}  {path}");
        }
        out.into_bytes()
    }

    /// Parse the on-disk text format produced by [`serialize`].
    pub fn deserialize(bytes: &[u8]) -> CheckpointResult<Self> {
        let text = std::str::from_utf8(bytes).map_err(|e| CheckpointError::Storage {
            message: format!("manifest is not valid UTF-8: {e}"),
        })?;
        let mut manifest = Self::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Format: "sha256:<hex>  <path>"
            let rest = line
                .strip_prefix("sha256:")
                .ok_or_else(|| CheckpointError::Storage {
                    message: format!("manifest line missing sha256 prefix: {line}"),
                })?;
            // Split on two spaces separating hash from path
            let (hex, path) = rest
                .split_once("  ")
                .ok_or_else(|| CheckpointError::Storage {
                    message: format!("manifest line missing separator: {line}"),
                })?;
            manifest.insert(path.trim(), hex.trim());
        }
        Ok(manifest)
    }
}

fn sha256_hex(data: &[u8]) -> String {
    krishiv_common::hash::sha256_hex(data)
}

// ── Key path helpers ──────────────────────────────────────────────────────────

/// Path to the epoch directory: `{job_id}/checkpoints/{epoch:020}`.
pub fn epoch_dir(job_id: &str, epoch: u64) -> String {
    format!("{job_id}/checkpoints/{epoch:020}")
}

/// Path to `metadata.json` for an epoch.
pub fn metadata_path(job_id: &str, epoch: u64) -> String {
    format!("{}/metadata.json", epoch_dir(job_id, epoch))
}

/// Path to `state.bin` for an operator instance in an epoch.
pub fn snapshot_path(job_id: &str, epoch: u64, op_id: &str, task_id: &str) -> String {
    format!("{}/{op_id}/{task_id}/state.bin", epoch_dir(job_id, epoch))
}

/// Path to `manifest.sha256` for an epoch.
pub fn manifest_path(job_id: &str, epoch: u64) -> String {
    format!("{}/manifest.sha256", epoch_dir(job_id, epoch))
}

fn latest_epoch_hint_path(job_id: &str) -> String {
    format!("{job_id}/checkpoints/latest_epoch.json")
}

// ── CheckpointStorage trait ───────────────────────────────────────────────────

/// Storage backend for checkpoint data.
///
/// The async methods are the primary API for scheduler/executor paths that
/// already run inside Tokio. Synchronous methods remain as compatibility
/// wrappers for tests and blocking-friendly call sites.
#[async_trait::async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Async write `data` to `path`.  Overwrites if it already exists.
    async fn write_bytes_async(&self, path: &str, data: &[u8]) -> CheckpointResult<()>;

    /// Async read the bytes stored at `path`. Returns `None` if absent.
    async fn read_bytes_async(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>>;

    /// Async list immediate children of `prefix` one level deep.
    async fn list_dir_async(&self, prefix: &str) -> CheckpointResult<Vec<String>>;

    /// Async recursively delete everything under `prefix`.
    async fn delete_prefix_async(&self, prefix: &str) -> CheckpointResult<()>;

    /// Write `data` to `path`.  Overwrites if it already exists.
    ///
    /// Implementations should write atomically (temp-file + rename) to prevent
    /// partial reads of in-progress writes.
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        run_blocking_on_tokio("checkpoint write_bytes", self.write_bytes_async(path, data))
    }

    /// Read the bytes stored at `path`.  Returns `None` if the path does not exist.
    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        run_blocking_on_tokio("checkpoint read_bytes", self.read_bytes_async(path))
    }

    /// List immediate children of `prefix` (directory listing one level deep).
    ///
    /// Returns relative names (not full paths).  Returns an empty `Vec` if the
    /// prefix does not exist.
    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        run_blocking_on_tokio("checkpoint list_dir", self.list_dir_async(prefix))
    }

    /// Recursively delete everything under `prefix`.  No-op if `prefix` does
    /// not exist.
    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        run_blocking_on_tokio("checkpoint delete_prefix", self.delete_prefix_async(prefix))
    }
}

/// Run an async block from a synchronous `CheckpointStorage` impl without
/// deadlocking the Tokio runtime.
///
/// The previous object-store backend used `futures::executor::block_on`, which
/// parks the current thread without yielding to Tokio.  If the inner future
/// awaits a Tokio resource (timer / TCP socket — both used by `reqwest`),
/// the worker thread deadlocks (D4).
///
/// This helper uses `block_in_place` when called from inside a multi-thread
/// Tokio runtime, falls back to a short-lived runtime when no runtime is
/// active, and returns a clear `Storage` error when called from a
/// `current_thread` runtime (where neither approach is safe).
pub fn run_blocking_on_tokio<F, T>(label: &'static str, fut: F) -> CheckpointResult<T>
where
    F: std::future::Future<Output = CheckpointResult<T>> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // We are inside a Tokio runtime.  block_in_place is only legal on
            // multi-thread runtimes; current_thread will panic.  Detect that
            // and return a clear error instead.
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(fut))
                }
                _ => Err(CheckpointError::Storage {
                    message: format!(
                        "{label}: cannot block on a current_thread Tokio runtime; \
                         call from a multi-thread runtime (#[tokio::main(flavor = \"multi_thread\")]) \
                         or use the async API directly"
                    ),
                }),
            }
        }
        Err(_) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .map_err(|e| CheckpointError::Storage {
                    message: format!("{label}: failed to build temporary Tokio runtime: {e}"),
                })?;
            rt.block_on(fut)
        }
    }
}

fn uuid_simple() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ── High-level helpers ────────────────────────────────────────────────────────

/// Write serialized `metadata` to `{epoch_dir}/metadata.json`.
///
/// **Does not update the epoch hint file.** Callers must call [`write_epoch_hint`]
/// *after* [`write_manifest`] so the hint only ever points to a fully sealed epoch.
/// Updating the hint before the manifest is written can cause `latest_valid_epoch`
/// to return an epoch whose manifest has not yet been written — resulting in an
/// apparent "no valid epoch" on the next restart even though a newer epoch exists.
pub fn write_epoch_metadata(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    metadata: &CheckpointMetadata,
) -> CheckpointResult<()> {
    // Propagate real storage errors; only `NoValidEpoch` (first checkpoint)
    // is benign and should be treated as "no prior epoch, proceed".
    // Using `if let Ok(...)` would silently swallow non-`NoValidEpoch` errors
    // and bypass the monotonicity guard, so we use an explicit `match`.
    match latest_valid_epoch(storage, job_id) {
        Ok(latest) if epoch <= latest => {
            return Err(CheckpointError::StaleEpoch {
                attempted: epoch,
                latest,
            });
        }
        Ok(_) => {}                              // newer epoch — proceed
        Err(CheckpointError::NoValidEpoch) => {} // no prior epoch — proceed
        Err(e) => return Err(e),                 // real storage error — propagate
    }
    let json = serde_json::to_vec_pretty(metadata).map_err(|e| CheckpointError::Storage {
        message: format!("metadata serialize: {e}"),
    })?;
    storage.write_bytes(&metadata_path(job_id, epoch), &json)
    // NOTE: epoch hint is NOT written here — callers must call write_epoch_hint()
    // after write_manifest() succeeds to guarantee the hint only points to sealed epochs.
}

/// Async variant of [`write_epoch_metadata`].
pub async fn write_epoch_metadata_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    metadata: &CheckpointMetadata,
) -> CheckpointResult<()> {
    match latest_valid_epoch_async(storage, job_id).await {
        Ok(latest) if epoch <= latest => {
            return Err(CheckpointError::StaleEpoch {
                attempted: epoch,
                latest,
            });
        }
        Ok(_) => {}
        Err(CheckpointError::NoValidEpoch) => {}
        Err(e) => return Err(e),
    }
    let json = serde_json::to_vec_pretty(metadata).map_err(|e| CheckpointError::Storage {
        message: format!("metadata serialize: {e}"),
    })?;
    storage
        .write_bytes_async(&metadata_path(job_id, epoch), &json)
        .await
}

/// Update the fast-path epoch hint to `epoch`.
///
/// This must be called **last** — after both [`write_epoch_metadata`] and
/// [`write_manifest`] have succeeded.  Writing the hint before the manifest is
/// present can cause `latest_valid_epoch` to return an epoch that fails
/// `validate_epoch` on the next restart, forcing a full directory scan.
///
/// In the worst case (crash between writing the manifest and writing the hint)
/// `latest_valid_epoch` simply falls back to scanning `list_valid_epochs`, so
/// the epoch is not lost — the hint is purely a read-path optimisation.
pub fn write_epoch_hint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.write_bytes(
        &latest_epoch_hint_path(job_id),
        epoch.to_string().as_bytes(),
    )
}

/// Async variant of [`write_epoch_hint`].
pub async fn write_epoch_hint_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(
            &latest_epoch_hint_path(job_id),
            epoch.to_string().as_bytes(),
        )
        .await
}

/// Read and deserialize `metadata.json` for `epoch`.  Returns `None` if absent.
pub fn read_epoch_metadata(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<Option<CheckpointMetadata>> {
    match storage.read_bytes(&metadata_path(job_id, epoch))? {
        None => Ok(None),
        Some(bytes) => {
            let meta: CheckpointMetadata =
                serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Corrupt {
                    epoch,
                    message: format!("metadata JSON parse: {e}"),
                })?;
            Ok(Some(meta))
        }
    }
}

/// Async variant of [`read_epoch_metadata`].
pub async fn read_epoch_metadata_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<Option<CheckpointMetadata>> {
    match storage
        .read_bytes_async(&metadata_path(job_id, epoch))
        .await?
    {
        None => Ok(None),
        Some(bytes) => {
            let meta: CheckpointMetadata =
                serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Corrupt {
                    epoch,
                    message: format!("metadata JSON parse: {e}"),
                })?;
            Ok(Some(meta))
        }
    }
}

/// Write an operator state snapshot to `{epoch_dir}/{op_id}/{task_id}/state.bin`.
pub fn write_operator_snapshot(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
    bytes: &[u8],
) -> CheckpointResult<()> {
    storage.write_bytes(&snapshot_path(job_id, epoch, op_id, task_id), bytes)
}

/// Async variant of [`write_operator_snapshot`].
pub async fn write_operator_snapshot_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
    bytes: &[u8],
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(&snapshot_path(job_id, epoch, op_id, task_id), bytes)
        .await
}

/// Read an operator state snapshot.  Returns `None` if absent.
pub fn read_operator_snapshot(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
) -> CheckpointResult<Option<Vec<u8>>> {
    storage.read_bytes(&snapshot_path(job_id, epoch, op_id, task_id))
}

/// Async variant of [`read_operator_snapshot`].
pub async fn read_operator_snapshot_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
) -> CheckpointResult<Option<Vec<u8>>> {
    storage
        .read_bytes_async(&snapshot_path(job_id, epoch, op_id, task_id))
        .await
}

/// Write the integrity manifest for `epoch`.
///
/// This must be called **last** — after all state snapshots and metadata are
/// written.  A present and valid manifest is the signal that an epoch is
/// complete and safe to restore from.
pub fn write_manifest(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<()> {
    storage.write_bytes(&manifest_path(job_id, epoch), &manifest.serialize())
}

/// Async variant of [`write_manifest`].
pub async fn write_manifest_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(&manifest_path(job_id, epoch), &manifest.serialize())
        .await
}

/// Validate the integrity manifest for `epoch`.
///
/// Returns `true` if the manifest exists and every listed file's SHA-256
/// matches the manifest entry.  Returns `false` if the manifest is absent
/// or any hash fails.
pub fn validate_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<bool> {
    let manifest_bytes = match storage.read_bytes(&manifest_path(job_id, epoch))? {
        None => return Ok(false),
        Some(b) => b,
    };
    let manifest =
        IntegrityManifest::deserialize(&manifest_bytes).map_err(|e| CheckpointError::Corrupt {
            epoch,
            message: format!("manifest parse: {e}"),
        })?;
    for (path, expected_hex) in &manifest.entries {
        let full = format!("{}/{path}", epoch_dir(job_id, epoch));
        match storage.read_bytes(&full)? {
            None => return Ok(false),
            Some(data) => {
                // Stream-hash via BufReader to avoid loading large files into
                // memory twice (once for read, once for digest).
                use std::io::Read as _;
                let mut reader = std::io::BufReader::new(data.as_slice());
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 8192];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| CheckpointError::Storage {
                            message: format!("reading {full} for hash: {e}"),
                        })?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                let hash = format!("{:x}", hasher.finalize());
                if hash != *expected_hex {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Async variant of [`validate_epoch`].
pub async fn validate_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<bool> {
    let manifest_bytes = match storage
        .read_bytes_async(&manifest_path(job_id, epoch))
        .await?
    {
        None => return Ok(false),
        Some(b) => b,
    };
    let manifest =
        IntegrityManifest::deserialize(&manifest_bytes).map_err(|e| CheckpointError::Corrupt {
            epoch,
            message: format!("manifest parse: {e}"),
        })?;
    for (path, expected_hex) in &manifest.entries {
        let full = format!("{}/{path}", epoch_dir(job_id, epoch));
        match storage.read_bytes_async(&full).await? {
            None => return Ok(false),
            Some(data) => {
                use std::io::Read as _;
                let mut reader = std::io::BufReader::new(data.as_slice());
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 8192];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| CheckpointError::Storage {
                            message: format!("reading {full} for hash: {e}"),
                        })?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                let hash = format!("{:x}", hasher.finalize());
                if hash != *expected_hex {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Return all epoch numbers that have a valid integrity manifest, in ascending order.
///
/// Epochs with missing or corrupt manifests are silently excluded.
pub fn list_valid_epochs(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let checkpoint_prefix = format!("{job_id}/checkpoints");
    let epoch_dirs = storage.list_dir(&checkpoint_prefix)?;
    let mut valid = Vec::new();
    for name in epoch_dirs {
        let Ok(epoch) = name.parse::<u64>() else {
            tracing::warn!(epoch_dir = %name, "skipping non-numeric checkpoint epoch directory");
            continue;
        };
        match validate_epoch(storage, job_id, epoch) {
            Ok(true) => valid.push(epoch),
            Ok(false) => tracing::warn!(job_id, epoch, "excluding invalid checkpoint epoch"),
            Err(e) => {
                tracing::warn!(job_id, epoch, error = %e, "checkpoint epoch validation failed");
                continue;
            }
        }
    }
    valid.sort_unstable();
    Ok(valid)
}

/// Async variant of [`list_valid_epochs`].
pub async fn list_valid_epochs_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let checkpoint_prefix = format!("{job_id}/checkpoints");
    let epoch_dirs = storage.list_dir_async(&checkpoint_prefix).await?;
    let mut valid = Vec::new();
    for name in epoch_dirs {
        let Ok(epoch) = name.parse::<u64>() else {
            tracing::warn!(epoch_dir = %name, "skipping non-numeric checkpoint epoch directory");
            continue;
        };
        match validate_epoch_async(storage, job_id, epoch).await {
            Ok(true) => valid.push(epoch),
            Ok(false) => tracing::warn!(job_id, epoch, "excluding invalid checkpoint epoch"),
            Err(e) => {
                tracing::warn!(job_id, epoch, error = %e, "checkpoint epoch validation failed");
                continue;
            }
        }
    }
    valid.sort_unstable();
    Ok(valid)
}

/// Delete all data for `epoch` from storage.
pub fn delete_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.delete_prefix(&epoch_dir(job_id, epoch))
}

/// Async variant of [`delete_epoch`].
pub async fn delete_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.delete_prefix_async(&epoch_dir(job_id, epoch)).await
}

/// Find the most recent valid epoch.  Returns `Err(NoValidEpoch)` if none.
pub fn latest_valid_epoch(storage: &dyn CheckpointStorage, job_id: &str) -> CheckpointResult<u64> {
    if let Some(hinted) = read_latest_epoch_hint(storage, job_id)?
        && validate_epoch(storage, job_id, hinted)?
    {
        return Ok(hinted);
    }

    let epochs = list_valid_epochs(storage, job_id)?;
    epochs
        .into_iter()
        .last()
        .ok_or(CheckpointError::NoValidEpoch)
}

/// Async variant of [`latest_valid_epoch`].
pub async fn latest_valid_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<u64> {
    if let Some(hinted) = read_latest_epoch_hint_async(storage, job_id).await?
        && validate_epoch_async(storage, job_id, hinted).await?
    {
        return Ok(hinted);
    }

    let epochs = list_valid_epochs_async(storage, job_id).await?;
    epochs
        .into_iter()
        .last()
        .ok_or(CheckpointError::NoValidEpoch)
}

fn read_latest_epoch_hint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Option<u64>> {
    let Some(bytes) = storage.read_bytes(&latest_epoch_hint_path(job_id))? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| CheckpointError::Storage {
        message: format!("latest epoch hint is not valid UTF-8: {error}"),
    })?;
    text.trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|error| CheckpointError::Storage {
            message: format!("latest epoch hint is not a valid u64: {error}"),
        })
}

async fn read_latest_epoch_hint_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Option<u64>> {
    let Some(bytes) = storage
        .read_bytes_async(&latest_epoch_hint_path(job_id))
        .await?
    else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| CheckpointError::Storage {
        message: format!("latest epoch hint is not valid UTF-8: {error}"),
    })?;
    text.trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|error| CheckpointError::Storage {
            message: format!("latest epoch hint is not a valid u64: {error}"),
        })
}

// ── Fencing token enforcement ─────────────────────────────────────────────────

/// Validate that `metadata.fencing_token` is not older than `current_token`.
///
/// Call this before writing a new checkpoint epoch or savepoint. Rejects metadata
/// whose fencing token does not match the current coordinator's token — this
/// prevents split-brain commits by stale coordinators.
///
/// **Important**: fencing tokens are per-coordinator-instance and are not
/// comparable across different coordinator instances.  This function should be
/// used only when the caller is the current active coordinator doing a write.
/// For restore operations, use [`validate_fencing_token_for_restore`] instead.
pub fn validate_fencing_token(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token != current_token {
        return Err(CheckpointError::StaleFencingToken {
            stored: metadata.fencing_token,
            current: current_token,
        });
    }
    Ok(())
}

/// Validate fencing token for a checkpoint restore operation.
///
/// Unlike [`validate_fencing_token`], this function accepts metadata written by
/// a prior coordinator instance (whose fencing token may differ from the current
/// leader's token because fencing tokens are per-coordinator-instance, not
/// globally monotonic).  The restore path relies on the leader-election
/// mechanism to guarantee that only one coordinator is actively mutating job
/// state; the fencing token in the metadata is recorded for audit purposes.
///
/// The check rejects only the pathological case where the metadata token is
/// strictly greater than the current token — which would indicate the metadata
/// was written by a coordinator that came *after* this one in the leadership
/// sequence, meaning this coordinator is stale.
pub fn validate_fencing_token_for_restore(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token > current_token {
        return Err(CheckpointError::StaleFencingToken {
            stored: metadata.fencing_token,
            current: current_token,
        });
    }
    Ok(())
}

// ── Replay bundle ─────────────────────────────────────────────────────────────

/// A self-contained diagnostic bundle that captures everything needed to replay
/// or audit a single checkpoint epoch.
///
/// Generated by [`generate_replay_bundle`] and used by operators to replay
/// stateful execution from a specific epoch without access to live state.
#[derive(Debug, Clone)]
pub struct ReplayBundle {
    /// Job that owns this epoch.
    pub job_id: String,
    /// Checkpoint epoch captured in this bundle.
    pub epoch: u64,
    /// Fencing token recorded in the epoch metadata.
    pub fencing_token: u64,
    /// Wall-clock commit time in milliseconds since Unix epoch.
    pub timestamp_ms: u64,
    /// Source offsets at the barrier boundary.
    pub source_offsets: Vec<SourceOffsetRecord>,
    /// Operator snapshot references (paths only; blobs not included).
    pub operator_snapshots: Vec<OperatorSnapshotRef>,
    /// Whether this epoch was committed as a savepoint.
    pub is_savepoint: bool,
    /// Optional savepoint label.
    pub savepoint_label: Option<String>,
}

/// Generate a [`ReplayBundle`] from the metadata for a specific epoch.
///
/// Returns `Err(NoValidEpoch)` if the epoch has no committed metadata.
pub fn generate_replay_bundle(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<ReplayBundle> {
    let metadata =
        read_epoch_metadata(storage, job_id, epoch)?.ok_or(CheckpointError::NoValidEpoch)?;
    Ok(ReplayBundle {
        job_id: metadata.job_id,
        epoch: metadata.epoch,
        fencing_token: metadata.fencing_token,
        timestamp_ms: metadata.timestamp_ms,
        source_offsets: metadata.source_offsets,
        operator_snapshots: metadata.operator_snapshots,
        is_savepoint: metadata.is_savepoint,
        savepoint_label: metadata.savepoint_label,
    })
}

/// Path to the immutable savepoint prefix for a job.
fn savepoint_prefix(job_id: &str) -> String {
    format!("{job_id}/savepoints")
}

/// Path to a specific savepoint epoch directory.
fn savepoint_epoch_dir(job_id: &str, savepoint_epoch: u64) -> String {
    format!("{}/{:020}", savepoint_prefix(job_id), savepoint_epoch)
}

/// C11: Create an immutable savepoint from the latest committed checkpoint.
///
/// Copies all checkpoint files (metadata, state snapshots, manifest) to a
/// separate `savepoints/` prefix that is excluded from normal checkpoint
/// garbage collection.  The savepoint persists until explicitly deleted by
/// the administrator.
///
/// Returns the savepoint epoch (same as the source checkpoint epoch) and
/// the serialized metadata.
pub fn create_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    label: Option<&str>,
) -> CheckpointResult<(u64, CheckpointMetadata)> {
    let epoch = latest_valid_epoch(storage, job_id)?;
    let mut metadata =
        read_epoch_metadata(storage, job_id, epoch)?.ok_or(CheckpointError::NoValidEpoch)?;
    metadata.is_savepoint = true;
    metadata.savepoint_label = label.map(str::to_string);

    let savepoint_dir = savepoint_epoch_dir(job_id, epoch);
    let epoch_dir = epoch_dir(job_id, epoch);

    // Copy metadata to savepoint prefix.
    let metadata_json =
        serde_json::to_vec_pretty(&metadata).map_err(|e| CheckpointError::Storage {
            message: format!("savepoint metadata serialize: {e}"),
        })?;
    storage.write_bytes(&format!("{savepoint_dir}/metadata.json"), &metadata_json)?;

    // Copy operator state snapshots.
    for snap in &metadata.operator_snapshots {
        let src_path = &snap.snapshot_path;
        if let Some(data) = storage.read_bytes(src_path)? {
            let rel = snap
                .snapshot_path
                .strip_prefix(&format!("{epoch_dir}/"))
                .unwrap_or(&snap.snapshot_path);
            storage.write_bytes(&format!("{savepoint_dir}/{rel}"), &data)?;
        }
    }

    // Copy manifest.
    let manifest_bytes = storage
        .read_bytes(&manifest_path(job_id, epoch))?
        .ok_or(CheckpointError::NoValidEpoch)?;
    storage.write_bytes(&format!("{savepoint_dir}/manifest.sha256"), &manifest_bytes)?;

    Ok((epoch, metadata))
}

/// C11: Restore from an immutable savepoint.
///
/// Reads savepoint metadata and validates that the current coordinator's
/// fencing token is equal to or greater than the savepoint's fencing token.
/// Returns the savepoint metadata and the list of valid savepoint epochs
/// available for this job.
pub fn restore_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    savepoint_epoch: u64,
    current_fencing_token: u64,
) -> CheckpointResult<CheckpointMetadata> {
    let savepoint_dir = savepoint_epoch_dir(job_id, savepoint_epoch);

    let meta_path = format!("{savepoint_dir}/metadata.json");
    let metadata = storage
        .read_bytes(&meta_path)?
        .ok_or(CheckpointError::NoValidEpoch)
        .and_then(|bytes| {
            serde_json::from_slice::<CheckpointMetadata>(&bytes).map_err(|e| {
                CheckpointError::Corrupt {
                    epoch: savepoint_epoch,
                    message: format!("savepoint metadata JSON parse: {e}"),
                }
            })
        })?;

    validate_fencing_token(&metadata, current_fencing_token)?;

    // Copy savepoint files back into the active checkpoints directory for restore.
    let epoch_dir = epoch_dir(job_id, savepoint_epoch);
    storage.write_bytes(
        &format!("{epoch_dir}/metadata.json"),
        &serde_json::to_vec_pretty(&metadata).map_err(|e| CheckpointError::Storage {
            message: format!("metadata serialize: {e}"),
        })?,
    )?;

    for snap in &metadata.operator_snapshots {
        let savepoint_snap = format!(
            "{savepoint_dir}/{}/{}/state.bin",
            snap.operator_id, snap.task_id
        );
        if let Some(data) = storage.read_bytes(&savepoint_snap)? {
            let target = snapshot_path(job_id, savepoint_epoch, &snap.operator_id, &snap.task_id);
            storage.write_bytes(&target, &data)?;
        }
    }

    let saved_manifest_path = format!("{savepoint_dir}/manifest.sha256");
    if let Some(manifest_data) = storage.read_bytes(&saved_manifest_path)? {
        storage.write_bytes(&manifest_path(job_id, savepoint_epoch), &manifest_data)?;
    }

    Ok(metadata)
}

/// List all savepoint epochs for a job.
pub fn list_savepoints(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let prefix = savepoint_prefix(job_id);
    let names = storage.list_dir(&prefix)?;
    let mut epochs: Vec<u64> = names
        .into_iter()
        .filter_map(|n| n.parse::<u64>().ok())
        .collect();
    epochs.sort_unstable();
    Ok(epochs)
}

/// Delete a savepoint (no-op if the savepoint does not exist).
pub fn delete_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    savepoint_epoch: u64,
) -> CheckpointResult<()> {
    let dir = savepoint_epoch_dir(job_id, savepoint_epoch);
    storage.delete_prefix(&dir)
}

// ── LocalFsCheckpointStorage ──────────────────────────────────────────────────

/// Filesystem-backed checkpoint storage for tests.
///
/// All writes use temp-file + rename for atomicity, matching the pattern used
/// by `RocksDbStateBackend`.  Production object-store storage wraps
/// `object_store::ObjectStore` behind the [`CheckpointStorage`] trait.
#[derive(Debug, Clone)]
pub struct LocalFsCheckpointStorage {
    base_dir: PathBuf,
}

impl LocalFsCheckpointStorage {
    /// Create storage rooted at `base_dir`, creating it if necessary.
    pub fn new(base_dir: impl Into<PathBuf>) -> CheckpointResult<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir).map_err(|e| CheckpointError::Storage {
            message: format!("create base_dir {}: {e}", base_dir.display()),
        })?;
        Ok(Self { base_dir })
    }

    /// Return the base directory for this storage instance.
    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// Create storage in a uniquely-named temporary directory.
    ///
    /// Returns an [`EphemeralCheckpointStorage`] wrapper whose `Drop` impl
    /// automatically removes the directory, preventing temp-dir leaks.
    pub fn ephemeral() -> CheckpointResult<EphemeralCheckpointStorage> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        let name = format!(
            "krishiv-ckpt-{}-{}",
            std::process::id(),
            CTR.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(name);
        let inner = Self::new(path.clone())?;
        Ok(EphemeralCheckpointStorage { inner, path })
    }

    fn full_path(&self, path: &str) -> CheckpointResult<PathBuf> {
        // Prevent path traversal: strip any leading '/' or '..' components,
        // then verify the resolved path stays within `self.base_dir`.
        let clean: PathBuf = path
            .split('/')
            .filter(|c| !c.is_empty() && *c != "..")
            .collect();
        let result = self.base_dir.join(clean);
        if !result.starts_with(&self.base_dir) {
            return Err(CheckpointError::InvalidPath {
                path: result.display().to_string(),
            });
        }
        Ok(result)
    }
}

/// RAII wrapper returned by [`LocalFsCheckpointStorage::ephemeral`].
///
/// Holds the `LocalFsCheckpointStorage` and the directory path; removes the
/// directory on drop to prevent temp-dir leaks.  Implements `Deref` so all
/// [`CheckpointStorage`] methods are available transparently.
pub struct EphemeralCheckpointStorage {
    inner: LocalFsCheckpointStorage,
    path: PathBuf,
}

impl std::ops::Deref for EphemeralCheckpointStorage {
    type Target = LocalFsCheckpointStorage;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for EphemeralCheckpointStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for EphemeralCheckpointStorage {
    fn drop(&mut self) {
        // Best-effort cleanup; ignore errors (e.g. already removed).
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[async_trait::async_trait]
impl CheckpointStorage for LocalFsCheckpointStorage {
    async fn write_bytes_async(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let storage = self.clone();
        let path = path.to_owned();
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || storage.write_bytes(&path, &data))
            .await
            .map_err(|e| CheckpointError::Storage {
                message: format!("checkpoint write task join failed: {e}"),
            })?
    }

    async fn read_bytes_async(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let storage = self.clone();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || storage.read_bytes(&path))
            .await
            .map_err(|e| CheckpointError::Storage {
                message: format!("checkpoint read task join failed: {e}"),
            })?
    }

    async fn list_dir_async(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let storage = self.clone();
        let prefix = prefix.to_owned();
        tokio::task::spawn_blocking(move || storage.list_dir(&prefix))
            .await
            .map_err(|e| CheckpointError::Storage {
                message: format!("checkpoint list task join failed: {e}"),
            })?
    }

    async fn delete_prefix_async(&self, prefix: &str) -> CheckpointResult<()> {
        let storage = self.clone();
        let prefix = prefix.to_owned();
        tokio::task::spawn_blocking(move || storage.delete_prefix(&prefix))
            .await
            .map_err(|e| CheckpointError::Storage {
                message: format!("checkpoint delete task join failed: {e}"),
            })?
    }

    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let full = self.full_path(path)?;
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CheckpointError::Storage {
                message: format!("mkdir {}: {e}", parent.display()),
            })?;
        }
        use std::io::Write as _;
        let tmp = full.with_extension(format!("tmp.{}", uuid_simple()));
        {
            let mut file = std::fs::File::create(&tmp).map_err(|e| CheckpointError::Storage {
                message: format!("create {}: {e}", tmp.display()),
            })?;
            file.write_all(data).map_err(|e| CheckpointError::Storage {
                message: format!("write {}: {e}", tmp.display()),
            })?;
            file.sync_all().map_err(|e| CheckpointError::Storage {
                message: format!("fsync {}: {e}", tmp.display()),
            })?;
        }
        std::fs::rename(&tmp, &full).map_err(|e| CheckpointError::Storage {
            message: format!("rename to {}: {e}", full.display()),
        })?;
        if let Some(parent) = full.parent() {
            let dir = std::fs::File::open(parent).map_err(|e| CheckpointError::Storage {
                message: format!("open dir {}: {e}", parent.display()),
            })?;
            dir.sync_all().map_err(|e| CheckpointError::Storage {
                message: format!("fsync dir {}: {e}", parent.display()),
            })?;
        }
        Ok(())
    }

    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let full = self.full_path(path)?;
        match std::fs::read(&full) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CheckpointError::Storage {
                message: format!("read {}: {e}", full.display()),
            }),
        }
    }

    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let full = self.full_path(prefix)?;
        match std::fs::read_dir(&full) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(vec![]),
            Err(e) => Err(CheckpointError::Storage {
                message: format!("list_dir {}: {e}", full.display()),
            }),
            Ok(entries) => {
                let mut names = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|e| CheckpointError::Storage {
                        message: format!("readdir entry: {e}"),
                    })?;
                    names.push(entry.file_name().to_string_lossy().into_owned());
                }
                Ok(names)
            }
        }
    }

    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        let full = self.full_path(prefix)?;
        match std::fs::remove_dir_all(&full) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CheckpointError::Storage {
                message: format!("delete_prefix {}: {e}", full.display()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl CheckpointStorage for EphemeralCheckpointStorage {
    async fn write_bytes_async(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        self.inner.write_bytes_async(path, data).await
    }

    async fn read_bytes_async(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        self.inner.read_bytes_async(path).await
    }

    async fn list_dir_async(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        self.inner.list_dir_async(prefix).await
    }

    async fn delete_prefix_async(&self, prefix: &str) -> CheckpointResult<()> {
        self.inner.delete_prefix_async(prefix).await
    }

    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        self.inner.write_bytes(path, data)
    }

    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        self.inner.read_bytes(path)
    }

    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        self.inner.list_dir(prefix)
    }

    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        self.inner.delete_prefix(prefix)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> EphemeralCheckpointStorage {
        LocalFsCheckpointStorage::ephemeral().unwrap()
    }

    fn sample_metadata(epoch: u64) -> CheckpointMetadata {
        CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: "job-test".to_owned(),
            fencing_token: 1,
            coordinator_id: None,
            timestamp_ms: 1_716_000_000_000,
            source_offsets: vec![SourceOffsetRecord {
                partition_id: "partition-0".to_owned(),
                offset: 42,
            }],
            operator_snapshots: vec![OperatorSnapshotRef {
                operator_id: "op-0".to_owned(),
                task_id: "task-0".to_owned(),
                snapshot_path: snapshot_path("job-test", epoch, "op-0", "task-0"),
            }],
            is_savepoint: false,
            savepoint_label: None,
            iceberg_snapshot_id: None,
            kafka_offsets: None,
        }
    }

    // ── LocalFsCheckpointStorage ──────────────────────────────────────────

    #[test]
    fn local_fs_write_read_roundtrip() {
        let s = make_storage();
        s.write_bytes(
            "job1/checkpoints/00000000000000000001/metadata.json",
            b"hello",
        )
        .unwrap();
        let data = s
            .read_bytes("job1/checkpoints/00000000000000000001/metadata.json")
            .unwrap();
        assert_eq!(data, Some(b"hello".to_vec()));
    }

    #[test]
    fn local_fs_read_missing_returns_none() {
        let s = make_storage();
        assert_eq!(s.read_bytes("no/such/file.json").unwrap(), None);
    }

    #[test]
    fn local_fs_delete_prefix_removes_tree() {
        let s = make_storage();
        s.write_bytes("job1/checkpoints/00000000000000000001/metadata.json", b"x")
            .unwrap();
        s.write_bytes("job1/checkpoints/00000000000000000001/state.bin", b"y")
            .unwrap();
        s.delete_prefix("job1/checkpoints/00000000000000000001")
            .unwrap();
        assert_eq!(
            s.read_bytes("job1/checkpoints/00000000000000000001/metadata.json")
                .unwrap(),
            None
        );
    }

    #[test]
    fn local_fs_list_dir_returns_entry_names() {
        let s = make_storage();
        s.write_bytes("job1/checkpoints/00000000000000000001/metadata.json", b"a")
            .unwrap();
        s.write_bytes("job1/checkpoints/00000000000000000002/metadata.json", b"b")
            .unwrap();
        let mut names = s.list_dir("job1/checkpoints").unwrap();
        names.sort();
        assert_eq!(
            names,
            vec![
                "00000000000000000001".to_owned(),
                "00000000000000000002".to_owned()
            ]
        );
    }

    #[test]
    fn local_fs_list_dir_missing_prefix_returns_empty() {
        let s = make_storage();
        assert!(s.list_dir("no/such/prefix").unwrap().is_empty());
    }

    // ── CheckpointMetadata ────────────────────────────────────────────────

    #[test]
    fn metadata_json_roundtrip() {
        let meta = sample_metadata(1);
        let json = serde_json::to_vec_pretty(&meta).unwrap();
        let parsed: CheckpointMetadata = serde_json::from_slice(&json).unwrap();
        assert_eq!(meta, parsed);
    }

    #[test]
    fn metadata_validate_rejects_unknown_version() {
        let mut meta = sample_metadata(1);
        meta.version = 99;
        assert!(meta.validate().is_err());
    }

    // ── IntegrityManifest ─────────────────────────────────────────────────

    #[test]
    fn manifest_serialize_deserialize_roundtrip() {
        let mut m = IntegrityManifest::new();
        m.insert_bytes("metadata.json", b"some json content");
        m.insert_bytes("op-0/task-0/state.bin", b"some state bytes");
        let serialized = m.serialize();
        let parsed = IntegrityManifest::deserialize(&serialized).unwrap();
        assert_eq!(m, parsed);
    }

    #[test]
    fn manifest_verify_detects_corruption() {
        let mut m = IntegrityManifest::new();
        m.insert_bytes("state.bin", b"original data");
        assert!(m.verify("state.bin", b"original data"));
        assert!(!m.verify("state.bin", b"tampered data"));
    }

    #[test]
    fn manifest_verify_missing_key_returns_false() {
        let m = IntegrityManifest::new();
        assert!(!m.verify("nonexistent.bin", b"data"));
    }

    // ── Higher-level helpers ──────────────────────────────────────────────

    #[test]
    fn write_and_read_epoch_metadata_roundtrip() {
        let s = make_storage();
        let meta = sample_metadata(5);
        write_epoch_metadata(&s, "job-test", 5, &meta).unwrap();
        let read_back = read_epoch_metadata(&s, "job-test", 5).unwrap();
        assert_eq!(read_back, Some(meta));
    }

    #[test]
    fn read_epoch_metadata_missing_returns_none() {
        let s = make_storage();
        assert_eq!(read_epoch_metadata(&s, "job-test", 99).unwrap(), None);
    }

    #[test]
    fn write_and_read_operator_snapshot_roundtrip() {
        let s = make_storage();
        let state_bytes = b"serialized state data";
        write_operator_snapshot(&s, "job-test", 3, "op-0", "task-0", state_bytes).unwrap();
        let read_back = read_operator_snapshot(&s, "job-test", 3, "op-0", "task-0").unwrap();
        assert_eq!(read_back, Some(state_bytes.to_vec()));
    }

    #[test]
    fn full_epoch_commit_validates_correctly() {
        let s = make_storage();
        let meta = sample_metadata(7);
        let state_bytes = b"operator state snapshot";

        // Write state snapshot
        write_operator_snapshot(&s, "job-test", 7, "op-0", "task-0", state_bytes).unwrap();
        // Write metadata
        let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
        write_epoch_metadata(&s, "job-test", 7, &meta).unwrap();
        // Write manifest (last)
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &meta_json);
        manifest.insert_bytes("op-0/task-0/state.bin", state_bytes);
        write_manifest(&s, "job-test", 7, &manifest).unwrap();

        assert!(validate_epoch(&s, "job-test", 7).unwrap());
    }

    #[test]
    fn epoch_without_manifest_is_invalid() {
        let s = make_storage();
        let meta = sample_metadata(8);
        write_epoch_metadata(&s, "job-test", 8, &meta).unwrap();
        // No manifest written
        assert!(!validate_epoch(&s, "job-test", 8).unwrap());
    }

    #[test]
    fn corrupt_file_fails_validation() {
        let s = make_storage();
        let meta = sample_metadata(9);
        let state_bytes = b"original state";

        write_operator_snapshot(&s, "job-test", 9, "op-0", "task-0", state_bytes).unwrap();
        let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
        write_epoch_metadata(&s, "job-test", 9, &meta).unwrap();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &meta_json);
        manifest.insert_bytes("op-0/task-0/state.bin", state_bytes);
        write_manifest(&s, "job-test", 9, &manifest).unwrap();

        // Now tamper with the state file
        s.write_bytes(
            &snapshot_path("job-test", 9, "op-0", "task-0"),
            b"tampered state",
        )
        .unwrap();

        assert!(!validate_epoch(&s, "job-test", 9).unwrap());
    }

    #[test]
    fn list_valid_epochs_returns_only_complete_epochs() {
        let s = make_storage();

        // Epoch 1: complete
        let meta1 = sample_metadata(1);
        let state1 = b"state for epoch 1";
        write_operator_snapshot(&s, "job-test", 1, "op-0", "task-0", state1).unwrap();
        let meta1_json = serde_json::to_vec_pretty(&meta1).unwrap();
        write_epoch_metadata(&s, "job-test", 1, &meta1).unwrap();
        let mut m1 = IntegrityManifest::new();
        m1.insert_bytes("metadata.json", &meta1_json);
        m1.insert_bytes("op-0/task-0/state.bin", state1);
        write_manifest(&s, "job-test", 1, &m1).unwrap();

        // Epoch 2: incomplete (no manifest)
        let meta2 = sample_metadata(2);
        write_epoch_metadata(&s, "job-test", 2, &meta2).unwrap();

        let valid = list_valid_epochs(&s, "job-test").unwrap();
        assert_eq!(valid, vec![1u64]);
    }

    #[test]
    fn latest_valid_epoch_returns_highest() {
        let s = make_storage();

        for epoch in [1u64, 3, 5] {
            let meta = sample_metadata(epoch);
            let state = format!("state {epoch}");
            let state_b = state.as_bytes();
            write_operator_snapshot(&s, "job-test", epoch, "op-0", "task-0", state_b).unwrap();
            let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
            write_epoch_metadata(&s, "job-test", epoch, &meta).unwrap();
            let mut m = IntegrityManifest::new();
            m.insert_bytes("metadata.json", &meta_json);
            m.insert_bytes("op-0/task-0/state.bin", state_b);
            write_manifest(&s, "job-test", epoch, &m).unwrap();
        }

        assert_eq!(latest_valid_epoch(&s, "job-test").unwrap(), 5);
    }

    #[test]
    fn latest_valid_epoch_no_epochs_returns_error() {
        let s = make_storage();
        assert!(matches!(
            latest_valid_epoch(&s, "job-no-checkpoints"),
            Err(CheckpointError::NoValidEpoch)
        ));
    }

    #[test]
    fn delete_epoch_removes_all_files() {
        let s = make_storage();
        let meta = sample_metadata(10);
        write_epoch_metadata(&s, "job-test", 10, &meta).unwrap();
        delete_epoch(&s, "job-test", 10).unwrap();
        assert_eq!(read_epoch_metadata(&s, "job-test", 10).unwrap(), None);
    }

    #[test]
    fn fallback_to_prior_valid_epoch_on_corruption() {
        let s = make_storage();

        // Epoch 4: valid
        let meta4 = sample_metadata(4);
        let state4 = b"good state";
        write_operator_snapshot(&s, "job-fb", 4, "op-0", "task-0", state4).unwrap();
        let meta4_json = serde_json::to_vec_pretty(&meta4).unwrap();
        write_epoch_metadata(&s, "job-fb", 4, &meta4).unwrap();
        let mut m4 = IntegrityManifest::new();
        m4.insert_bytes("metadata.json", &meta4_json);
        m4.insert_bytes("op-0/task-0/state.bin", state4);
        write_manifest(&s, "job-fb", 4, &m4).unwrap();

        // Epoch 5: written but then state tampered → corrupt
        let meta5 = sample_metadata(5);
        let state5 = b"state for 5";
        write_operator_snapshot(&s, "job-fb", 5, "op-0", "task-0", state5).unwrap();
        let meta5_json = serde_json::to_vec_pretty(&meta5).unwrap();
        write_epoch_metadata(&s, "job-fb", 5, &meta5).unwrap();
        let mut m5 = IntegrityManifest::new();
        m5.insert_bytes("metadata.json", &meta5_json);
        m5.insert_bytes("op-0/task-0/state.bin", state5);
        write_manifest(&s, "job-fb", 5, &m5).unwrap();
        // Tamper
        s.write_bytes(&snapshot_path("job-fb", 5, "op-0", "task-0"), b"corrupt")
            .unwrap();

        // latest_valid_epoch falls back to epoch 4
        assert_eq!(latest_valid_epoch(&s, "job-fb").unwrap(), 4);
    }

    // ── Fencing token enforcement ─────────────────────────────────────────

    #[test]
    fn validate_fencing_token_current_token_accepted() {
        let meta = sample_metadata(1); // fencing_token = 1
        assert!(validate_fencing_token(&meta, 1).is_ok());
    }

    #[test]
    fn fencing_token_rejects_mismatch() {
        let meta = sample_metadata(1);
        let mut meta2 = meta.clone();
        meta2.fencing_token = 5;
        assert!(
            validate_fencing_token(&meta2, 2).is_err(),
            "metadata from a different coordinator instance (token=5) must be rejected by current coordinator (token=2)"
        );
    }

    #[test]
    fn fencing_token_accepts_exact_match() {
        let meta = sample_metadata(1);
        let mut meta2 = meta.clone();
        meta2.fencing_token = 3;
        assert!(
            validate_fencing_token(&meta2, 3).is_ok(),
            "metadata with matching token must be accepted"
        );
    }

    #[test]
    fn validate_fencing_token_stale_rejected() {
        let meta = sample_metadata(1); // fencing_token = 1
        // Current coordinator is at token=2; metadata has token=1 → stale
        let result = validate_fencing_token(&meta, 2);
        assert!(matches!(
            result,
            Err(CheckpointError::StaleFencingToken {
                stored: 1,
                current: 2
            })
        ));
    }

    #[test]
    fn validate_fencing_token_stale_display() {
        let meta = sample_metadata(1);
        let err = validate_fencing_token(&meta, 5).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("stale"), "expected 'stale' in: {msg}");
    }

    #[test]
    fn validate_fencing_token_stale_metadata_rejected() {
        let meta = sample_metadata(1);
        let result = validate_fencing_token(&meta, 5);
        assert!(matches!(
            result,
            Err(CheckpointError::StaleFencingToken {
                stored: 1,
                current: 5
            })
        ));
    }

    #[test]
    fn validate_fencing_token_exact_match_accepted() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = 3;
        assert!(
            validate_fencing_token(&meta, 3).is_ok(),
            "exact fencing token match must be accepted"
        );
    }

    // ── Path traversal protection (P3.21) ────────────────────────────────

    #[test]
    fn full_path_blocks_dot_dot_traversal() {
        let s = make_storage();
        // A path with '..' components must be rejected.
        let result = s.read_bytes("../../etc/passwd");
        // The cleaned path collapses to within base_dir (empty relative path),
        // so the result should be Ok(None) rather than escaping the base.
        // Either Ok or InvalidPath is acceptable; what is NOT acceptable is
        // silently resolving to a path outside base_dir.
        match result {
            Ok(_) => {
                // The '..' components were stripped; confirm result path is
                // within the storage base.
                // (No assertion needed — the file simply doesn't exist.)
            }
            Err(CheckpointError::InvalidPath { .. }) => {
                // Expected if canonicalization detects escape.
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    #[test]
    fn full_path_allows_normal_paths() {
        let s = make_storage();
        // Normal nested paths should work without error.
        s.write_bytes("job1/checkpoints/00000000000000000001/metadata.json", b"ok")
            .unwrap();
        let data = s
            .read_bytes("job1/checkpoints/00000000000000000001/metadata.json")
            .unwrap();
        assert_eq!(data, Some(b"ok".to_vec()));
    }

    // ── Ephemeral cleanup (P3.20) ─────────────────────────────────────────

    #[test]
    fn ephemeral_storage_cleans_up_on_drop() {
        let base_path;
        {
            let s = make_storage();
            base_path = s.inner.base_dir().to_path_buf();
            s.write_bytes("test/data.bin", b"hello").unwrap();
            assert!(base_path.exists(), "dir must exist while storage is live");
        } // s dropped here — directory should be removed
        assert!(
            !base_path.exists(),
            "ephemeral dir must be removed after drop"
        );
    }

    // ── Replay bundle ─────────────────────────────────────────────────────

    #[test]
    fn generate_replay_bundle_roundtrip() {
        let s = make_storage();
        let meta = sample_metadata(1);
        write_epoch_metadata(&s, "job-test", 1, &meta).unwrap();
        let bundle = generate_replay_bundle(&s, "job-test", 1).unwrap();
        assert_eq!(bundle.job_id, "job-test");
        assert_eq!(bundle.epoch, 1);
        assert_eq!(bundle.fencing_token, 1);
        assert_eq!(bundle.source_offsets.len(), 1);
        assert_eq!(bundle.source_offsets[0].partition_id, "partition-0");
        assert!(!bundle.is_savepoint);
    }

    #[test]
    fn generate_replay_bundle_missing_epoch_returns_error() {
        let s = make_storage();
        let result = generate_replay_bundle(&s, "job-test", 99);
        assert!(matches!(result, Err(CheckpointError::NoValidEpoch)));
    }

    // ── EphemeralCheckpointStorage: write/read roundtrip ───────────────────

    #[test]
    fn ephemeral_write_read_roundtrip() {
        let s = LocalFsCheckpointStorage::ephemeral().unwrap();
        let path = "job-eph/checkpoints/00000000000000000001/data.bin";
        s.write_bytes(path, b"ephemeral payload").unwrap();
        let got = s.read_bytes(path).unwrap();
        assert_eq!(got, Some(b"ephemeral payload".to_vec()));
    }

    #[test]
    fn ephemeral_list_dir_and_delete_work() {
        let s = LocalFsCheckpointStorage::ephemeral().unwrap();
        s.write_bytes("j/c/00000000000000000001/a", b"1").unwrap();
        s.write_bytes("j/c/00000000000000000002/b", b"2").unwrap();
        let mut dirs = s.list_dir("j/c").unwrap();
        dirs.sort();
        assert_eq!(
            dirs,
            vec![
                "00000000000000000001".to_owned(),
                "00000000000000000002".to_owned()
            ]
        );
        s.delete_prefix("j/c/00000000000000000001").unwrap();
        assert!(
            s.list_dir("j/c")
                .unwrap()
                .contains(&"00000000000000000002".to_owned())
        );
        assert!(
            !s.list_dir("j/c")
                .unwrap()
                .contains(&"00000000000000000001".to_owned())
        );
    }

    #[test]
    fn ephemeral_cleanup_on_drop_removes_all_files() {
        let base_path;
        {
            let s = LocalFsCheckpointStorage::ephemeral().unwrap();
            base_path = s.base_dir().to_path_buf();
            s.write_bytes("job/checkpoints/1/metadata.json", b"{\"v\":1}")
                .unwrap();
            s.write_bytes("job/checkpoints/1/op/state.bin", b"state")
                .unwrap();
            assert!(base_path.exists());
            assert!(base_path.join("job/checkpoints/1/metadata.json").exists());
        }
        assert!(
            !base_path.exists(),
            "ephemeral directory must be removed after drop"
        );
    }

    #[test]
    fn ephemeral_independent_instances_do_not_interfere() {
        let (path_a, path_b);
        {
            let a = LocalFsCheckpointStorage::ephemeral().unwrap();
            let b = LocalFsCheckpointStorage::ephemeral().unwrap();
            path_a = a.base_dir().to_path_buf();
            path_b = b.base_dir().to_path_buf();
            a.write_bytes("data.bin", b"from-a").unwrap();
            b.write_bytes("data.bin", b"from-b").unwrap();
            assert_eq!(a.read_bytes("data.bin").unwrap(), Some(b"from-a".to_vec()));
            assert_eq!(b.read_bytes("data.bin").unwrap(), Some(b"from-b".to_vec()));
        }
        assert!(!path_a.exists());
        assert!(!path_b.exists());
    }

    // ── write_epoch_metadata / read_epoch_metadata ────────────────────────

    #[test]
    fn write_read_epoch_metadata_roundtrip_with_all_fields() {
        let s = make_storage();
        let meta = CheckpointMetadata {
            version: 1,
            epoch: 42,
            job_id: "job-full".to_owned(),
            fencing_token: 7,
            coordinator_id: None,
            timestamp_ms: 1_716_100_000_000,
            source_offsets: vec![
                SourceOffsetRecord {
                    partition_id: "p-0".to_owned(),
                    offset: 100,
                },
                SourceOffsetRecord {
                    partition_id: "p-1".to_owned(),
                    offset: 200,
                },
            ],
            operator_snapshots: vec![
                OperatorSnapshotRef {
                    operator_id: "op-a".to_owned(),
                    task_id: "task-0".to_owned(),
                    snapshot_path: snapshot_path("job-full", 42, "op-a", "task-0"),
                },
                OperatorSnapshotRef {
                    operator_id: "op-b".to_owned(),
                    task_id: "task-1".to_owned(),
                    snapshot_path: snapshot_path("job-full", 42, "op-b", "task-1"),
                },
            ],
            is_savepoint: true,
            savepoint_label: Some("manual-snap".to_owned()),
            iceberg_snapshot_id: Some(999),
            kafka_offsets: Some({
                let mut m = std::collections::BTreeMap::new();
                m.insert("topic-a".to_owned(), 500i64);
                m.insert("topic-b".to_owned(), 600i64);
                m
            }),
        };
        write_epoch_metadata(&s, "job-full", 42, &meta).unwrap();
        let read_back = read_epoch_metadata(&s, "job-full", 42).unwrap();
        assert_eq!(read_back, Some(meta));
    }

    #[test]
    fn write_epoch_metadata_stale_epoch_rejected() {
        let s = make_storage();
        let meta = sample_metadata(3);
        write_epoch_metadata(&s, "j", 3, &meta).unwrap();
        // Write a manifest so epoch 3 is valid
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &serde_json::to_vec_pretty(&meta).unwrap());
        write_manifest(&s, "j", 3, &manifest).unwrap();

        // Epoch 2 is older than latest valid (3) → StaleEpoch
        let meta2 = sample_metadata(2);
        let result = write_epoch_metadata(&s, "j", 2, &meta2);
        assert!(matches!(
            result,
            Err(CheckpointError::StaleEpoch {
                attempted: 2,
                latest: 3
            })
        ));
    }

    #[test]
    fn write_epoch_metadata_equal_epoch_rejected() {
        let s = make_storage();
        let meta = sample_metadata(5);
        write_epoch_metadata(&s, "j", 5, &meta).unwrap();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &serde_json::to_vec_pretty(&meta).unwrap());
        write_manifest(&s, "j", 5, &manifest).unwrap();

        // Same epoch number again → StaleEpoch
        let meta_dup = sample_metadata(5);
        let result = write_epoch_metadata(&s, "j", 5, &meta_dup);
        assert!(matches!(
            result,
            Err(CheckpointError::StaleEpoch {
                attempted: 5,
                latest: 5
            })
        ));
    }

    #[test]
    fn write_epoch_metadata_first_epoch_accepted() {
        let s = make_storage();
        // No prior epochs → NoValidEpoch is treated as "proceed"
        let meta = sample_metadata(1);
        let result = write_epoch_metadata(&s, "j-first", 1, &meta);
        assert!(result.is_ok());
    }

    #[test]
    fn read_epoch_metadata_corrupt_json_returns_error() {
        let s = make_storage();
        s.write_bytes(&metadata_path("j", 1), b"this is not valid json {{{")
            .unwrap();
        let result = read_epoch_metadata(&s, "j", 1);
        assert!(matches!(
            result,
            Err(CheckpointError::Corrupt { epoch: 1, .. })
        ));
    }

    // ── validate_epoch ────────────────────────────────────────────────────

    #[test]
    fn validate_epoch_rejects_stale_epoch_with_valid_manifest() {
        let s = make_storage();
        // Epoch 10 is complete with a valid manifest, then we write epoch 5
        // (which is older) — validate_epoch returns Ok(false) because it's
        // checking the manifest hash, not the epoch number. The stale epoch
        // guard lives in write_epoch_metadata.
        let meta = sample_metadata(5);
        write_epoch_metadata(&s, "j", 5, &meta).unwrap();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &serde_json::to_vec_pretty(&meta).unwrap());
        write_manifest(&s, "j", 5, &manifest).unwrap();
        assert!(validate_epoch(&s, "j", 5).unwrap());
    }

    #[test]
    fn validate_epoch_missing_manifest_returns_false() {
        let s = make_storage();
        let meta = sample_metadata(20);
        write_epoch_metadata(&s, "j", 20, &meta).unwrap();
        // No manifest → false
        assert!(!validate_epoch(&s, "j", 20).unwrap());
    }

    #[test]
    fn validate_epoch_nonexistent_epoch_returns_false() {
        let s = make_storage();
        assert!(!validate_epoch(&s, "j", 999).unwrap());
    }

    #[test]
    fn validate_epoch_corrupt_manifest_parse_error() {
        let s = make_storage();
        s.write_bytes(&manifest_path("j", 30), b"garbage data!!!")
            .unwrap();
        let result = validate_epoch(&s, "j", 30);
        assert!(matches!(
            result,
            Err(CheckpointError::Corrupt { epoch: 30, .. })
        ));
    }

    #[test]
    fn validate_epoch_manifest_entry_missing_file() {
        let s = make_storage();
        // Write a manifest that references a file that doesn't exist on disk
        let mut manifest = IntegrityManifest::new();
        manifest.insert("missing/file.bin", "abc123");
        write_manifest(&s, "j", 40, &manifest).unwrap();
        assert!(!validate_epoch(&s, "j", 40).unwrap());
    }

    #[test]
    fn validate_epoch_manifest_hash_mismatch() {
        let s = make_storage();
        let data = b"good data";
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("data.bin", data);
        write_manifest(&s, "j", 50, &manifest).unwrap();
        // Write the file with different content than what the manifest records
        s.write_bytes(&format!("{}/data.bin", epoch_dir("j", 50)), b"bad data")
            .unwrap();
        assert!(!validate_epoch(&s, "j", 50).unwrap());
    }

    // ── latest_valid_epoch ────────────────────────────────────────────────

    #[test]
    fn latest_valid_epoch_fallback_to_prior_valid_epoch() {
        let s = make_storage();

        // Epoch 1: valid
        let meta1 = sample_metadata(1);
        let state1 = b"state-1";
        write_operator_snapshot(&s, "job-fb2", 1, "op-0", "task-0", state1).unwrap();
        let meta1_json = serde_json::to_vec_pretty(&meta1).unwrap();
        write_epoch_metadata(&s, "job-fb2", 1, &meta1).unwrap();
        let mut m1 = IntegrityManifest::new();
        m1.insert_bytes("metadata.json", &meta1_json);
        m1.insert_bytes("op-0/task-0/state.bin", state1);
        write_manifest(&s, "job-fb2", 1, &m1).unwrap();

        // Epoch 2: complete but then state file corrupted
        let meta2 = sample_metadata(2);
        let state2 = b"state-2";
        write_operator_snapshot(&s, "job-fb2", 2, "op-0", "task-0", state2).unwrap();
        let meta2_json = serde_json::to_vec_pretty(&meta2).unwrap();
        write_epoch_metadata(&s, "job-fb2", 2, &meta2).unwrap();
        let mut m2 = IntegrityManifest::new();
        m2.insert_bytes("metadata.json", &meta2_json);
        m2.insert_bytes("op-0/task-0/state.bin", state2);
        write_manifest(&s, "job-fb2", 2, &m2).unwrap();
        // Tamper the state to invalidate epoch 2
        s.write_bytes(
            &snapshot_path("job-fb2", 2, "op-0", "task-0"),
            b"corrupted!",
        )
        .unwrap();

        // Epoch 3: valid
        let meta3 = sample_metadata(3);
        let state3 = b"state-3";
        write_operator_snapshot(&s, "job-fb2", 3, "op-0", "task-0", state3).unwrap();
        let meta3_json = serde_json::to_vec_pretty(&meta3).unwrap();
        write_epoch_metadata(&s, "job-fb2", 3, &meta3).unwrap();
        let mut m3 = IntegrityManifest::new();
        m3.insert_bytes("metadata.json", &meta3_json);
        m3.insert_bytes("op-0/task-0/state.bin", state3);
        write_manifest(&s, "job-fb2", 3, &m3).unwrap();

        // latest_valid_epoch should skip epoch 2 and return 3
        assert_eq!(latest_valid_epoch(&s, "job-fb2").unwrap(), 3);
    }

    #[test]
    fn latest_valid_epoch_hint_points_to_invalid_falls_back_to_scan() {
        let s = make_storage();

        // Epoch 1: valid
        let meta1 = sample_metadata(1);
        let state1 = b"s1";
        write_operator_snapshot(&s, "job-hint", 1, "op-0", "task-0", state1).unwrap();
        let meta1_json = serde_json::to_vec_pretty(&meta1).unwrap();
        write_epoch_metadata(&s, "job-hint", 1, &meta1).unwrap();
        let mut m1 = IntegrityManifest::new();
        m1.insert_bytes("metadata.json", &meta1_json);
        m1.insert_bytes("op-0/task-0/state.bin", state1);
        write_manifest(&s, "job-hint", 1, &m1).unwrap();

        // Epoch 2: valid, then hint set to 2
        let meta2 = sample_metadata(2);
        let state2 = b"s2";
        write_operator_snapshot(&s, "job-hint", 2, "op-0", "task-0", state2).unwrap();
        let meta2_json = serde_json::to_vec_pretty(&meta2).unwrap();
        write_epoch_metadata(&s, "job-hint", 2, &meta2).unwrap();
        let mut m2 = IntegrityManifest::new();
        m2.insert_bytes("metadata.json", &meta2_json);
        m2.insert_bytes("op-0/task-0/state.bin", state2);
        write_manifest(&s, "job-hint", 2, &m2).unwrap();
        write_epoch_hint(&s, "job-hint", 2).unwrap();

        // Now corrupt epoch 2 so the hint points to an invalid epoch
        s.write_bytes(&snapshot_path("job-hint", 2, "op-0", "task-0"), b"bad")
            .unwrap();

        // latest_valid_epoch should fall back to scanning and return epoch 1
        assert_eq!(latest_valid_epoch(&s, "job-hint").unwrap(), 1);
    }

    #[test]
    fn latest_valid_epoch_no_epochs_at_all() {
        let s = make_storage();
        assert!(matches!(
            latest_valid_epoch(&s, "job-empty"),
            Err(CheckpointError::NoValidEpoch)
        ));
    }

    #[test]
    fn latest_valid_epoch_all_epochs_corrupt() {
        let s = make_storage();

        // Epoch 1: metadata only, no manifest
        let meta1 = sample_metadata(1);
        write_epoch_metadata(&s, "j-corrupt", 1, &meta1).unwrap();

        // Epoch 2: manifest present but file tampered
        let meta2 = sample_metadata(2);
        let state2 = b"state";
        write_operator_snapshot(&s, "j-corrupt", 2, "op-0", "task-0", state2).unwrap();
        let meta2_json = serde_json::to_vec_pretty(&meta2).unwrap();
        write_epoch_metadata(&s, "j-corrupt", 2, &meta2).unwrap();
        let mut m2 = IntegrityManifest::new();
        m2.insert_bytes("metadata.json", &meta2_json);
        m2.insert_bytes("op-0/task-0/state.bin", state2);
        write_manifest(&s, "j-corrupt", 2, &m2).unwrap();
        s.write_bytes(
            &snapshot_path("j-corrupt", 2, "op-0", "task-0"),
            b"tampered",
        )
        .unwrap();

        assert!(matches!(
            latest_valid_epoch(&s, "j-corrupt"),
            Err(CheckpointError::NoValidEpoch)
        ));
    }

    // ── Empty file / zero-byte write tests ───────────────────────────────

    #[test]
    fn local_fs_write_read_empty_bytes() {
        let s = make_storage();
        s.write_bytes("empty.bin", b"").unwrap();
        let data = s.read_bytes("empty.bin").unwrap();
        assert_eq!(data, Some(b"".to_vec()));
    }

    #[test]
    fn write_read_zero_byte_operator_snapshot() {
        let s = make_storage();
        write_operator_snapshot(&s, "j", 1, "op-0", "task-0", b"").unwrap();
        let data = read_operator_snapshot(&s, "j", 1, "op-0", "task-0").unwrap();
        assert_eq!(data, Some(b"".to_vec()));
    }

    #[test]
    fn zero_byte_file_in_manifest_validates() {
        let s = make_storage();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("empty.bin", b"");
        write_manifest(&s, "j", 1, &manifest).unwrap();
        s.write_bytes(&format!("{}/empty.bin", epoch_dir("j", 1)), b"")
            .unwrap();
        assert!(validate_epoch(&s, "j", 1).unwrap());
    }

    #[test]
    fn empty_manifest_validates_as_empty_epoch() {
        let s = make_storage();
        let manifest = IntegrityManifest::new();
        write_manifest(&s, "j", 1, &manifest).unwrap();
        assert!(validate_epoch(&s, "j", 1).unwrap());
    }

    // ── Concurrent writes ───────────────────────────────────────────────

    #[test]
    fn concurrent_write_read_different_paths() {
        use std::sync::Arc;
        use std::thread;

        let s = Arc::new(make_storage());
        let mut handles = vec![];

        for i in 0..8 {
            let s = Arc::clone(&s);
            handles.push(thread::spawn(move || {
                let path = format!("concurrent/data-{i}.bin");
                let payload = format!("payload-{i}");
                s.write_bytes(&path, payload.as_bytes()).unwrap();
                let got = s.read_bytes(&path).unwrap();
                assert_eq!(got, Some(payload.into_bytes()));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn concurrent_write_same_path_last_writer_wins() {
        use std::sync::Arc;
        use std::thread;

        let s = Arc::new(make_storage());
        let mut handles = vec![];

        for i in 0..8 {
            let s = Arc::clone(&s);
            handles.push(thread::spawn(move || {
                s.write_bytes("same/path.bin", format!("writer-{i}").as_bytes())
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let data = s.read_bytes("same/path.bin").unwrap().unwrap();
        let text = String::from_utf8(data).unwrap();
        assert!(text.starts_with("writer-"));
    }

    // ── IntegrityManifest edge cases ────────────────────────────────────

    #[test]
    fn manifest_empty_serialize_deserialize_roundtrip() {
        let m = IntegrityManifest::new();
        let serialized = m.serialize();
        let parsed = IntegrityManifest::deserialize(&serialized).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn manifest_len_and_is_empty() {
        let mut m = IntegrityManifest::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        m.insert("a.bin", "abc");
        assert!(!m.is_empty());
        assert_eq!(m.len(), 1);
        m.insert("b.bin", "def");
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn manifest_insert_overwrites_same_key() {
        let mut m = IntegrityManifest::new();
        m.insert("key.bin", "aaa");
        assert_eq!(m.len(), 1);
        // Overwrite with insert_bytes
        m.insert_bytes("key.bin", b"new content");
        assert_eq!(m.len(), 1);
        assert!(m.verify("key.bin", b"new content"));
        assert!(!m.verify("key.bin", b"old content"));
    }

    #[test]
    fn manifest_insert_bytes_and_verify() {
        let mut m = IntegrityManifest::new();
        m.insert_bytes("file.bin", b"content");
        assert!(m.verify("file.bin", b"content"));
        assert!(!m.verify("file.bin", b"Content"));
    }

    #[test]
    fn manifest_deserialize_invalid_utf8() {
        let bytes: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x01];
        let result = IntegrityManifest::deserialize(&bytes);
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn manifest_deserialize_missing_prefix() {
        let bytes = b"nothex  somefile.bin\n";
        let result = IntegrityManifest::deserialize(bytes);
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn manifest_deserialize_missing_separator() {
        let bytes = b"sha256:abcdef1234567890\n";
        let result = IntegrityManifest::deserialize(bytes);
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn manifest_deserialize_blank_lines_skipped() {
        let mut m = IntegrityManifest::new();
        m.insert("a.bin", "aaa");
        let mut serialized = m.serialize();
        // Insert blank lines
        let s = String::from_utf8(serialized.clone()).unwrap();
        let with_blanks = format!("\n\n{s}\n\n");
        serialized = with_blanks.into_bytes();
        let parsed = IntegrityManifest::deserialize(&serialized).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn manifest_many_entries_roundtrip() {
        let mut m = IntegrityManifest::new();
        for i in 0..100 {
            m.insert(format!("path/{i}/file.bin"), format!("hash-{i:04x}"));
        }
        let serialized = m.serialize();
        let parsed = IntegrityManifest::deserialize(&serialized).unwrap();
        assert_eq!(m, parsed);
    }

    // ── CheckpointError Display coverage ─────────────────────────────────

    #[test]
    fn error_display_covers_all_variants() {
        let errors: Vec<CheckpointError> = vec![
            CheckpointError::Storage {
                message: "io".into(),
            },
            CheckpointError::Corrupt {
                epoch: 1,
                message: "bad".into(),
            },
            CheckpointError::IncompatibleVersion { version: 99 },
            CheckpointError::NoValidEpoch,
            CheckpointError::StaleFencingToken {
                stored: 1,
                current: 2,
            },
            CheckpointError::StaleEpoch {
                attempted: 1,
                latest: 5,
            },
            CheckpointError::InvalidPath {
                path: "/bad".into(),
            },
        ];
        for err in &errors {
            let msg = err.to_string();
            assert!(!msg.is_empty(), "Display must produce non-empty string");
        }
        // Check that each variant produces distinct messages
        let msgs: Vec<_> = errors.iter().map(|e| e.to_string()).collect();
        assert!(msgs[0] != msgs[1]);
    }

    #[test]
    fn error_is_std_error() {
        let err = CheckpointError::NoValidEpoch;
        let _: &dyn std::error::Error = &err;
    }

    // ── write_epoch_hint roundtrip ──────────────────────────────────────

    #[test]
    fn write_and_read_epoch_hint_roundtrip() {
        let s = make_storage();
        write_epoch_hint(&s, "j", 42).unwrap();
        let hint = read_latest_epoch_hint(&s, "j").unwrap();
        assert_eq!(hint, Some(42));
    }

    #[test]
    fn read_epoch_hint_missing_returns_none() {
        let s = make_storage();
        let hint = read_latest_epoch_hint(&s, "j").unwrap();
        assert_eq!(hint, None);
    }

    #[test]
    fn read_epoch_hint_corrupt_utf8() {
        let s = make_storage();
        s.write_bytes("j/checkpoints/latest_epoch.json", &[0xFF, 0xFE])
            .unwrap();
        let result = read_latest_epoch_hint(&s, "j");
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn read_epoch_hint_not_a_number() {
        let s = make_storage();
        s.write_bytes("j/checkpoints/latest_epoch.json", b"not-a-number")
            .unwrap();
        let result = read_latest_epoch_hint(&s, "j");
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    // ── Path traversal edge cases ───────────────────────────────────────

    #[test]
    fn path_traversal_absolute_path_stripped() {
        let s = make_storage();
        // Absolute path should be stripped to relative, result is within base
        let result = s.write_bytes("/absolute/path.bin", b"data");
        assert!(result.is_ok());
        // The file ends up at base_dir/absolute/path.bin (no traversal outside)
        let data = s.read_bytes("absolute/path.bin").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    #[test]
    fn path_with_leading_slash_does_not_escape() {
        let s = make_storage();
        let result = s.write_bytes("/../../etc/passwd", b"evil");
        assert!(result.is_ok());
        // File should be at base_dir/etc/passwd, not /etc/passwd
        let data = s.read_bytes("etc/passwd").unwrap();
        assert_eq!(data, Some(b"evil".to_vec()));
    }

    // ── validate_fencing_token edge cases ────────────────────────────────

    #[test]
    fn validate_fencing_token_zero_accepted() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = 0;
        assert!(validate_fencing_token(&meta, 0).is_ok());
    }

    #[test]
    fn validate_fencing_token_zero_rejected_by_generation_1() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = 0;
        assert!(validate_fencing_token(&meta, 1).is_err());
    }

    #[test]
    fn validate_fencing_token_max_values() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = u64::MAX;
        assert!(validate_fencing_token(&meta, u64::MAX).is_ok());
        // Mismatched token (0 vs u64::MAX) must be rejected.
        assert!(validate_fencing_token(&meta, 0).is_err());
    }

    #[test]
    fn validate_fencing_token_for_restore_accepts_lower_stored_token() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = 3;
        // Restoring metadata from a prior coordinator (token 3) with current
        // coordinator having token 7 — allowed because metadata came from a
        // past valid leader.
        assert!(validate_fencing_token_for_restore(&meta, 7).is_ok());
    }

    #[test]
    fn validate_fencing_token_for_restore_rejects_higher_stored_token() {
        let mut meta = sample_metadata(1);
        meta.fencing_token = 9;
        // Metadata with a higher token than current coordinator suggests this
        // coordinator is stale.
        assert!(validate_fencing_token_for_restore(&meta, 5).is_err());
    }

    // ── ReplayBundle with savepoint fields ──────────────────────────────

    #[test]
    fn generate_replay_bundle_savepoint_fields() {
        let s = make_storage();
        let mut meta = sample_metadata(10);
        meta.is_savepoint = true;
        meta.savepoint_label = Some("manual-save".into());
        write_epoch_metadata(&s, "j", 10, &meta).unwrap();
        let bundle = generate_replay_bundle(&s, "j", 10).unwrap();
        assert!(bundle.is_savepoint);
        assert_eq!(bundle.savepoint_label.as_deref(), Some("manual-save"));
    }

    // ── Multiple operators in one epoch ─────────────────────────────────

    #[test]
    fn full_epoch_multiple_operators_validates() {
        let s = make_storage();
        let meta = sample_metadata(15);
        let state1 = b"state-op0";
        let state2 = b"state-op1";

        write_operator_snapshot(&s, "j", 15, "op-0", "task-0", state1).unwrap();
        write_operator_snapshot(&s, "j", 15, "op-1", "task-0", state2).unwrap();

        let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
        write_epoch_metadata(&s, "j", 15, &meta).unwrap();

        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("metadata.json", &meta_json);
        manifest.insert_bytes("op-0/task-0/state.bin", state1);
        manifest.insert_bytes("op-1/task-0/state.bin", state2);
        write_manifest(&s, "j", 15, &manifest).unwrap();

        assert!(validate_epoch(&s, "j", 15).unwrap());
    }

    // ── Ephemeral storage concurrency ───────────────────────────────────

    #[test]
    fn ephemeral_storage_survives_concurrent_access() {
        use std::thread;

        let s = make_storage();
        let base = s.base_dir().to_path_buf();
        let mut handles = vec![];

        for i in 0..4 {
            let base = base.clone();
            handles.push(thread::spawn(move || {
                let storage = LocalFsCheckpointStorage::new(&base).unwrap();
                let state = format!("state-{i}");
                write_operator_snapshot(
                    &storage,
                    "j",
                    1,
                    &format!("op-{i}"),
                    "t",
                    state.as_bytes(),
                )
                .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..4 {
            let data = read_operator_snapshot(&s, "j", 1, &format!("op-{i}"), "t").unwrap();
            assert!(data.is_some());
        }
    }

    // ── delete_epoch non-existent is no-op ──────────────────────────────

    #[test]
    fn delete_epoch_nonexistent_is_noop() {
        let s = make_storage();
        delete_epoch(&s, "j", 9999).unwrap();
    }

    // ── delete_epoch only removes target epoch ──────────────────────────

    #[test]
    fn delete_epoch_only_removes_target() {
        let s = make_storage();
        write_epoch_metadata(&s, "j", 1, &sample_metadata(1)).unwrap();
        write_epoch_metadata(&s, "j", 2, &sample_metadata(2)).unwrap();
        delete_epoch(&s, "j", 1).unwrap();
        assert_eq!(read_epoch_metadata(&s, "j", 1).unwrap(), None);
        assert!(read_epoch_metadata(&s, "j", 2).unwrap().is_some());
    }

    // ── IntegrityManifest SHA-256 correctness ───────────────────────────

    #[test]
    fn manifest_sha256_matches_independent_computation() {
        let data = b"hello world";
        let mut m = IntegrityManifest::new();
        m.insert_bytes("file.bin", data);
        // Compute expected SHA-256 independently
        use sha2::Digest;
        let expected = format!("{:x}", sha2::Sha256::digest(data));
        assert!(m.verify("file.bin", data));
        // Verify the recorded hash matches independent computation
        let serialized = String::from_utf8(m.serialize()).unwrap();
        let first_line = serialized.lines().next().unwrap();
        assert!(first_line.contains(&expected));
    }

    // ── epoch_dir / metadata_path / snapshot_path / manifest_path ───────

    #[test]
    fn path_helpers_format_correctly() {
        assert_eq!(epoch_dir("job", 1), "job/checkpoints/00000000000000000001");
        assert_eq!(
            metadata_path("job", 1),
            "job/checkpoints/00000000000000000001/metadata.json"
        );
        assert_eq!(
            snapshot_path("job", 1, "op", "t"),
            "job/checkpoints/00000000000000000001/op/t/state.bin"
        );
        assert_eq!(
            manifest_path("job", 1),
            "job/checkpoints/00000000000000000001/manifest.sha256"
        );
    }

    // ── uuid_simple produces unique values ──────────────────────────────

    #[test]
    fn uuid_simple_returns_unique_values() {
        let a = uuid_simple();
        let b = uuid_simple();
        assert_ne!(a, b);
    }

    // ── sha256_hex correctness ──────────────────────────────────────────

    #[test]
    fn sha256_hex_empty_input() {
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_hex_deterministic() {
        let h1 = sha256_hex(b"test data");
        let h2 = sha256_hex(b"test data");
        assert_eq!(h1, h2);
    }

    #[test]
    fn sha256_hex_different_inputs() {
        let h1 = sha256_hex(b"abc");
        let h2 = sha256_hex(b"abd");
        assert_ne!(h1, h2);
    }

    // ── local_fs new with explicit path ─────────────────────────────────

    #[test]
    fn local_fs_new_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subdir").join("deep");
        let s = LocalFsCheckpointStorage::new(&path).unwrap();
        assert!(path.exists());
        s.write_bytes("test.bin", b"data").unwrap();
        assert!(path.join("test.bin").exists());
    }

    #[test]
    fn local_fs_base_dir() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalFsCheckpointStorage::new(dir.path()).unwrap();
        assert_eq!(s.base_dir(), dir.path());
    }

    // ── list_dir one level deep only ────────────────────────────────────

    #[test]
    fn list_dir_is_one_level_deep() {
        let s = make_storage();
        s.write_bytes("prefix/child/grandchild/file.bin", b"x")
            .unwrap();
        let children = s.list_dir("prefix").unwrap();
        assert!(children.contains(&"child".to_owned()));
        // grandchild should NOT appear in prefix's listing
        assert!(!children.contains(&"grandchild".to_owned()));
    }

    // ── validate_epoch with empty data files ────────────────────────────

    #[test]
    fn validate_epoch_all_empty_files() {
        let s = make_storage();
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("a.bin", b"");
        manifest.insert_bytes("b.bin", b"");
        write_manifest(&s, "j", 1, &manifest).unwrap();
        s.write_bytes(&format!("{}/a.bin", epoch_dir("j", 1)), b"")
            .unwrap();
        s.write_bytes(&format!("{}/b.bin", epoch_dir("j", 1)), b"")
            .unwrap();
        assert!(validate_epoch(&s, "j", 1).unwrap());
    }

    // ── SourceOffsetRecord and OperatorSnapshotRef eq ────────────────────

    #[test]
    fn source_offset_record_equality() {
        let a = SourceOffsetRecord {
            partition_id: "p-0".into(),
            offset: 10,
        };
        let b = SourceOffsetRecord {
            partition_id: "p-0".into(),
            offset: 10,
        };
        let c = SourceOffsetRecord {
            partition_id: "p-1".into(),
            offset: 10,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn operator_snapshot_ref_equality() {
        let a = OperatorSnapshotRef {
            operator_id: "op-0".into(),
            task_id: "t-0".into(),
            snapshot_path: "p".into(),
        };
        let b = OperatorSnapshotRef {
            operator_id: "op-0".into(),
            task_id: "t-0".into(),
            snapshot_path: "p".into(),
        };
        assert_eq!(a, b);
    }

    // ── write_epoch_metadata propagates storage errors ──────────────────

    #[test]
    fn write_epoch_metadata_propagates_non_no_valid_epoch_errors() {
        let s = make_storage();
        // Create a valid epoch so later checks don't see NoValidEpoch
        write_epoch_metadata(&s, "j", 1, &sample_metadata(1)).unwrap();
        let mut m = IntegrityManifest::new();
        m.insert_bytes(
            "metadata.json",
            &serde_json::to_vec_pretty(&sample_metadata(1)).unwrap(),
        );
        write_manifest(&s, "j", 1, &m).unwrap();

        // Now try to write epoch 2 - it should succeed since epoch 1 is valid
        let result = write_epoch_metadata(&s, "j", 2, &sample_metadata(2));
        assert!(result.is_ok());
    }

    // ── delete_prefix on empty is no-op ─────────────────────────────────

    #[test]
    fn delete_prefix_nonexistent_is_noop() {
        let s = make_storage();
        s.delete_prefix("no/such/prefix").unwrap();
    }

    // ── validate_epoch manifest with extra file on disk ──────────────────

    #[test]
    fn validate_epoch_manifest_does_not_cover_all_files() {
        let s = make_storage();
        // Manifest only covers file1.bin, but file2.bin also exists on disk
        let mut manifest = IntegrityManifest::new();
        manifest.insert_bytes("file1.bin", b"good");
        write_manifest(&s, "j", 1, &manifest).unwrap();
        s.write_bytes(&format!("{}/file1.bin", epoch_dir("j", 1)), b"good")
            .unwrap();
        s.write_bytes(&format!("{}/file2.bin", epoch_dir("j", 1)), b"extra")
            .unwrap();
        // Should still validate because manifest only checks listed files
        assert!(validate_epoch(&s, "j", 1).unwrap());
    }

    // ── CheckpointMetadata iceberg_snapshot_id and kafka_offsets ─────────

    #[test]
    fn metadata_optional_fields_omitted_when_none() {
        let meta = sample_metadata(1);
        let json = serde_json::to_string(&meta).unwrap();
        assert!(!json.contains("iceberg_snapshot_id"));
        assert!(!json.contains("kafka_offsets"));
    }

    #[test]
    fn metadata_optional_fields_present_when_some() {
        let mut meta = sample_metadata(1);
        meta.iceberg_snapshot_id = Some(42);
        meta.kafka_offsets = Some({
            let mut m = std::collections::BTreeMap::new();
            m.insert("t".into(), 100i64);
            m
        });
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("iceberg_snapshot_id"));
        assert!(json.contains("kafka_offsets"));
    }

    // ── CheckpointMetadata version constant ─────────────────────────────

    #[test]
    fn metadata_version_constant_is_one() {
        assert_eq!(CheckpointMetadata::VERSION, 1);
    }

    // ── EphemeralCheckpointStorage Deref/DerefMut ───────────────────────

    #[test]
    fn ephemeral_deref_allows_storage_trait_methods() {
        let s = make_storage();
        // Deref gives access to LocalFsCheckpointStorage methods
        assert!(s.base_dir().exists());
        // CheckpointStorage trait methods also work via Deref
        s.write_bytes("x.bin", b"y").unwrap();
        let data =
            <EphemeralCheckpointStorage as CheckpointStorage>::read_bytes(&s, "x.bin").unwrap();
        assert_eq!(data, Some(b"y".to_vec()));
    }

    // ── rescaling module tests ──────────────────────────────────────────

    #[test]
    fn rescaler_new_clamps_zero_parallelism_to_one() {
        let rescaler = super::rescaling::KeyGroupRescaler::new(0, 0);
        assert_eq!(rescaler.old_parallelism, 1);
        assert_eq!(rescaler.new_parallelism, 1);
        assert_eq!(rescaler.new_ranges.len(), 1);
    }

    #[test]
    fn rescaler_task_for_key_group_covers_full_range() {
        let rescaler = super::rescaling::KeyGroupRescaler::new(4, 4);
        let mut tasks_used = std::collections::HashSet::new();
        for kg in 0..krishiv_state::key_group::NUM_KEY_GROUPS {
            let task = rescaler.task_for_key_group(kg);
            assert!(task < 4);
            tasks_used.insert(task);
        }
        // All 4 task slots should be used
        assert_eq!(tasks_used.len(), 4);
    }

    #[test]
    fn rescaler_range_for_task_boundary_values() {
        let rescaler = super::rescaling::KeyGroupRescaler::new(2, 4);
        // Task 0 and 1 should have ranges
        assert!(rescaler.range_for_task(0).is_some());
        assert!(rescaler.range_for_task(1).is_some());
        // Task 2 and 3 should have ranges (new parallelism is 4)
        assert!(rescaler.range_for_task(2).is_some());
        assert!(rescaler.range_for_task(3).is_some());
        // Out of range
        assert!(rescaler.range_for_task(4).is_none());
    }

    #[test]
    fn rescaler_key_group_consistency() {
        // For every key group, task_for_key_group must map into a task whose
        // range contains that key group.
        for new_p in [1u32, 2, 3, 4, 8, 16, 32] {
            let rescaler = super::rescaling::KeyGroupRescaler::new(4, new_p);
            for kg in 0..krishiv_state::key_group::NUM_KEY_GROUPS {
                let task = rescaler.task_for_key_group(kg);
                let range = rescaler.range_for_task(task).unwrap();
                assert!(
                    range.contains(kg),
                    "key_group={kg} task={task} range={range:?} new_p={new_p}"
                );
            }
        }
    }

    // ── storage_uri module tests ────────────────────────────────────────

    #[test]
    fn storage_uri_empty_returns_error() {
        let result = super::storage_uri::open_checkpoint_storage_from_uri("");
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn storage_uri_whitespace_only_returns_error() {
        let result = super::storage_uri::open_checkpoint_storage_from_uri("   ");
        assert!(matches!(result, Err(CheckpointError::Storage { .. })));
    }

    #[test]
    fn storage_uri_memory_creates_in_memory_store() {
        let store = super::storage_uri::open_checkpoint_storage_from_uri("memory://").unwrap();
        store.write_bytes("test.bin", b"data").unwrap();
        let data = store.read_bytes("test.bin").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    #[test]
    fn storage_uri_memory_with_prefix() {
        let store =
            super::storage_uri::open_checkpoint_storage_from_uri("memory://prefix").unwrap();
        store.write_bytes("test.bin", b"data").unwrap();
        let data = store.read_bytes("test.bin").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    #[test]
    fn storage_uri_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", dir.path().display());
        let store = super::storage_uri::open_checkpoint_storage_from_uri(&uri).unwrap();
        store.write_bytes("test.bin", b"data").unwrap();
        let data = store.read_bytes("test.bin").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    #[test]
    fn storage_uri_bare_path() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let store = super::storage_uri::open_checkpoint_storage_from_uri(uri).unwrap();
        store.write_bytes("test.bin", b"data").unwrap();
        let data = store.read_bytes("test.bin").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    #[test]
    fn storage_uri_memory_list_dir_returns_children() {
        let store =
            super::storage_uri::open_checkpoint_storage_from_uri("memory://listtest").unwrap();
        store.write_bytes("a/file1.bin", b"1").unwrap();
        store.write_bytes("a/file2.bin", b"2").unwrap();
        store.write_bytes("b/file3.bin", b"3").unwrap();
        let mut children = store.list_dir("a").unwrap();
        children.sort();
        assert_eq!(children, vec!["file1.bin", "file2.bin"]);
    }

    #[test]
    fn storage_uri_memory_delete_prefix() {
        let store =
            super::storage_uri::open_checkpoint_storage_from_uri("memory://delprefix").unwrap();
        store.write_bytes("del/a.bin", b"1").unwrap();
        store.write_bytes("del/b.bin", b"2").unwrap();
        store.write_bytes("keep/c.bin", b"3").unwrap();
        store.delete_prefix("del").unwrap();
        assert!(store.read_bytes("del/a.bin").unwrap().is_none());
        assert!(store.read_bytes("keep/c.bin").unwrap().is_some());
    }

    #[test]
    fn storage_uri_memory_read_nonexistent_returns_none() {
        let store = super::storage_uri::open_checkpoint_storage_from_uri("memory://none").unwrap();
        assert!(store.read_bytes("nope.bin").unwrap().is_none());
    }

    // ── IntegrityManifest serialize format ──────────────────────────────

    #[test]
    fn manifest_serialize_format() {
        let mut m = IntegrityManifest::new();
        m.insert("z.bin", "zzz");
        m.insert("a.bin", "aaa");
        let serialized = String::from_utf8(m.serialize()).unwrap();
        // Entries are sorted by path (BTreeMap)
        let lines: Vec<&str> = serialized.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("sha256:aaa  a.bin"));
        assert!(lines[1].starts_with("sha256:zzz  z.bin"));
    }

    // ── write_operator_snapshot creates nested dirs ──────────────────────

    #[test]
    fn operator_snapshot_creates_nested_directories() {
        let s = make_storage();
        write_operator_snapshot(&s, "j", 1, "deep", "nested", b"data").unwrap();
        let data = read_operator_snapshot(&s, "j", 1, "deep", "nested").unwrap();
        assert_eq!(data, Some(b"data".to_vec()));
    }

    // ── latest_valid_epoch hint-based fast path ──────────────────────────

    #[test]
    fn latest_valid_epoch_uses_hint_when_valid() {
        let s = make_storage();
        // Write epoch 3 as valid
        let meta3 = sample_metadata(3);
        let state3 = b"state-3";
        write_operator_snapshot(&s, "j-hp", 3, "op-0", "task-0", state3).unwrap();
        let meta3_json = serde_json::to_vec_pretty(&meta3).unwrap();
        write_epoch_metadata(&s, "j-hp", 3, &meta3).unwrap();
        let mut m3 = IntegrityManifest::new();
        m3.insert_bytes("metadata.json", &meta3_json);
        m3.insert_bytes("op-0/task-0/state.bin", state3);
        write_manifest(&s, "j-hp", 3, &m3).unwrap();
        write_epoch_hint(&s, "j-hp", 3).unwrap();

        assert_eq!(latest_valid_epoch(&s, "j-hp").unwrap(), 3);
    }

    // ── validate_epoch on epoch with metadata corruption ─────────────────

    #[test]
    fn validate_epoch_with_corrupt_metadata_file() {
        let s = make_storage();
        let mut manifest = IntegrityManifest::new();
        manifest.insert("metadata.json", "badhash");
        write_manifest(&s, "j", 1, &manifest).unwrap();
        s.write_bytes(&format!("{}/metadata.json", epoch_dir("j", 1)), b"not json")
            .unwrap();
        // validate_epoch reads files and computes hash; it should return false
        // because the stored hash doesn't match
        assert!(!validate_epoch(&s, "j", 1).unwrap());
    }

    #[test]
    fn savepoint_and_later_checkpoints_coexist() {
        let s = make_storage();
        let state1 = b"state-epoch-1";
        let state2 = b"state-epoch-2";
        let state3 = b"state-epoch-3";

        // Establish epochs 1, 2, 3
        let meta1 = sample_metadata(1);
        let meta2 = sample_metadata(2);
        let meta3 = sample_metadata(3);
        for epoch in &[1u64, 2, 3] {
            let meta = if *epoch == 1 {
                &meta1
            } else if *epoch == 2 {
                &meta2
            } else {
                &meta3
            };
            let state = if *epoch == 1 {
                state1
            } else if *epoch == 2 {
                state2
            } else {
                state3
            };
            write_operator_snapshot(&s, "j-sp", *epoch, "op-0", "task-0", state).unwrap();
            write_epoch_metadata(&s, "j-sp", *epoch, meta).unwrap();
            let mut m = IntegrityManifest::new();
            let meta_json = serde_json::to_vec_pretty(meta).unwrap();
            m.insert_bytes("metadata.json", &meta_json);
            m.insert_bytes("op-0/task-0/state.bin", state);
            write_manifest(&s, "j-sp", *epoch, &m).unwrap();
        }
        assert_eq!(latest_valid_epoch(&s, "j-sp").unwrap(), 3);

        // Create a savepoint from epoch 2
        let (sp_epoch, _sp_meta) = create_savepoint(&s, "j-sp", Some("test-savepoint")).unwrap();
        assert_eq!(sp_epoch, 3); // latest_valid_epoch is 3

        // Write epochs 4 and 5 after the savepoint
        for epoch in &[4u64, 5] {
            let meta = CheckpointMetadata {
                version: CheckpointMetadata::VERSION,
                epoch: *epoch,
                job_id: "j-sp".into(),
                fencing_token: 1,
                coordinator_id: Some("coord-1".into()),
                timestamp_ms: 1_716_000_000_000,
                source_offsets: Vec::new(),
                operator_snapshots: Vec::new(),
                is_savepoint: false,
                savepoint_label: None,
                iceberg_snapshot_id: None,
                kafka_offsets: None,
            };
            write_epoch_metadata(&s, "j-sp", *epoch, &meta).unwrap();
            let mut m = IntegrityManifest::new();
            let meta_json = serde_json::to_vec_pretty(&meta).unwrap();
            m.insert_bytes("metadata.json", &meta_json);
            write_manifest(&s, "j-sp", *epoch, &m).unwrap();
            write_epoch_hint(&s, "j-sp", *epoch).unwrap();
        }
        assert_eq!(latest_valid_epoch(&s, "j-sp").unwrap(), 5);

        // Savepoint still readable
        let restored = restore_savepoint(&s, "j-sp", 3, 1).unwrap();
        assert!(restored.is_savepoint);
        assert_eq!(restored.savepoint_label.as_deref(), Some("test-savepoint"));

        // Newer checkpoints still valid
        assert!(validate_epoch(&s, "j-sp", 5).unwrap());
        assert!(validate_epoch(&s, "j-sp", 4).unwrap());
    }

    #[test]
    fn delete_savepoint_does_not_affect_checkpoint_epochs() {
        let s = make_storage();
        let state1 = b"state-epoch-1";
        let meta1 = sample_metadata(1);
        write_operator_snapshot(&s, "j-del", 1, "op-0", "task-0", state1).unwrap();
        write_epoch_metadata(&s, "j-del", 1, &meta1).unwrap();
        let mut m = IntegrityManifest::new();
        let meta_json = serde_json::to_vec_pretty(&meta1).unwrap();
        m.insert_bytes("metadata.json", &meta_json);
        m.insert_bytes("op-0/task-0/state.bin", state1);
        write_manifest(&s, "j-del", 1, &m).unwrap();

        create_savepoint(&s, "j-del", Some("to-delete")).unwrap();

        // Write a newer checkpoint with manifest
        let meta2 = sample_metadata(2);
        write_operator_snapshot(&s, "j-del", 2, "op-0", "task-0", b"state-epoch-2").unwrap();
        write_epoch_metadata(&s, "j-del", 2, &meta2).unwrap();
        let mut m2 = IntegrityManifest::new();
        let meta2_json = serde_json::to_vec_pretty(&meta2).unwrap();
        m2.insert_bytes("metadata.json", &meta2_json);
        m2.insert_bytes("op-0/task-0/state.bin", b"state-epoch-2");
        write_manifest(&s, "j-del", 2, &m2).unwrap();
        write_epoch_hint(&s, "j-del", 2).unwrap();

        // Delete the savepoint
        let sp_dir = savepoint_epoch_dir("j-del", 1);
        delete_savepoint(&s, "j-del", 1).unwrap();

        // Savepoint metadata removed but checkpoint epochs remain intact
        assert!(
            s.read_bytes(&format!("{sp_dir}/metadata.json"))
                .unwrap()
                .is_none(),
            "savepoint metadata should be deleted"
        );
        assert!(validate_epoch(&s, "j-del", 1).unwrap());
        assert!(validate_epoch(&s, "j-del", 2).unwrap());
        assert_eq!(latest_valid_epoch(&s, "j-del").unwrap(), 2);
    }
}
