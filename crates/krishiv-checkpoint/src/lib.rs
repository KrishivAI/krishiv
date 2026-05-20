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

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Error / Result ────────────────────────────────────────────────────────────

/// Errors from checkpoint storage operations.
#[derive(Debug)]
pub enum CheckpointError {
    /// Underlying storage I/O failed.
    Storage { message: String },
    /// Epoch data failed integrity validation.
    Corrupt { epoch: u64, message: String },
    /// Checkpoint metadata uses an unsupported format version.
    IncompatibleVersion { version: u32 },
    /// No valid committed epoch exists to restore from.
    NoValidEpoch,
    /// The checkpoint's fencing token predates the current coordinator generation.
    ///
    /// A stale coordinator must not commit checkpoint epochs — reject the write.
    StaleFencingToken { stored: u64, current: u64 },
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage { message } => write!(f, "checkpoint storage error: {message}"),
            Self::Corrupt { epoch, message } => {
                write!(f, "checkpoint epoch {epoch} is corrupt: {message}")
            }
            Self::IncompatibleVersion { version } => {
                write!(f, "unsupported checkpoint metadata version {version}")
            }
            Self::NoValidEpoch => write!(f, "no valid committed checkpoint epoch found"),
            Self::StaleFencingToken { stored, current } => write!(
                f,
                "stale fencing token: metadata token {stored} < current coordinator token {current}"
            ),
        }
    }
}

impl std::error::Error for CheckpointError {}

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
            writeln!(out, "sha256:{hex}  {path}").unwrap();
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
    let hash = Sha256::digest(data);
    format!("{hash:x}")
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

// ── CheckpointStorage trait ───────────────────────────────────────────────────

/// Storage backend for checkpoint data.
///
/// All methods are synchronous; callers in async contexts must use
/// `spawn_blocking` (same contract as [`krishiv_state::RocksDbStateBackend`]).
///
/// [`LocalFsCheckpointStorage`] implements this trait using `std::fs` for
/// unit tests.  A production object-store backend wraps `object_store::ObjectStore`
/// behind this trait in a later release.
pub trait CheckpointStorage: Send + Sync {
    /// Write `data` to `path`.  Overwrites if it already exists.
    ///
    /// Implementations should write atomically (temp-file + rename) to prevent
    /// partial reads of in-progress writes.
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()>;

    /// Read the bytes stored at `path`.  Returns `None` if the path does not exist.
    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>>;

    /// List immediate children of `prefix` (directory listing one level deep).
    ///
    /// Returns relative names (not full paths).  Returns an empty `Vec` if the
    /// prefix does not exist.
    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>>;

    /// Recursively delete everything under `prefix`.  No-op if `prefix` does
    /// not exist.
    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()>;
}

// ── High-level helpers ────────────────────────────────────────────────────────

/// Write serialized `metadata` to `{epoch_dir}/metadata.json`.
pub fn write_epoch_metadata(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    metadata: &CheckpointMetadata,
) -> CheckpointResult<()> {
    let json = serde_json::to_vec_pretty(metadata).map_err(|e| CheckpointError::Storage {
        message: format!("metadata serialize: {e}"),
    })?;
    storage.write_bytes(&metadata_path(job_id, epoch), &json)
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
                if sha256_hex(&data) != *expected_hex {
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
        if let Ok(epoch) = name.parse::<u64>() {
            if validate_epoch(storage, job_id, epoch).unwrap_or(false) {
                valid.push(epoch);
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

/// Find the most recent valid epoch.  Returns `Err(NoValidEpoch)` if none.
pub fn latest_valid_epoch(storage: &dyn CheckpointStorage, job_id: &str) -> CheckpointResult<u64> {
    let epochs = list_valid_epochs(storage, job_id)?;
    epochs
        .into_iter()
        .last()
        .ok_or(CheckpointError::NoValidEpoch)
}

// ── Fencing token enforcement ─────────────────────────────────────────────────

/// Validate that `metadata.fencing_token` is not older than `current_token`.
///
/// Call this before writing a new checkpoint epoch. A stale fencing token means
/// the coordinator that built this metadata is no longer the active leader —
/// committing would risk a split-brain write.
pub fn validate_fencing_token(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token < current_token {
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

// ── LocalFsCheckpointStorage ──────────────────────────────────────────────────

/// Filesystem-backed checkpoint storage for tests.
///
/// All writes use temp-file + rename for atomicity, matching the pattern used
/// by `RocksDbStateBackend`.  Production object-store storage wraps
/// `object_store::ObjectStore` behind the [`CheckpointStorage`] trait.
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

    /// Create storage in a uniquely-named temporary subdirectory under `std::env::temp_dir()`.
    pub fn ephemeral() -> CheckpointResult<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        let name = format!(
            "krishiv-ckpt-{}-{}",
            std::process::id(),
            CTR.fetch_add(1, Ordering::Relaxed)
        );
        Self::new(std::env::temp_dir().join(name))
    }

    fn full_path(&self, path: &str) -> PathBuf {
        // Prevent path traversal: strip any leading '/' or '..' components.
        let clean: PathBuf = path
            .split('/')
            .filter(|c| !c.is_empty() && *c != "..")
            .collect();
        self.base_dir.join(clean)
    }
}

impl CheckpointStorage for LocalFsCheckpointStorage {
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let full = self.full_path(path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CheckpointError::Storage {
                message: format!("mkdir {}: {e}", parent.display()),
            })?;
        }
        let tmp = full.with_extension("tmp");
        std::fs::write(&tmp, data).map_err(|e| CheckpointError::Storage {
            message: format!("write {}: {e}", tmp.display()),
        })?;
        std::fs::rename(&tmp, &full).map_err(|e| CheckpointError::Storage {
            message: format!("rename to {}: {e}", full.display()),
        })?;
        Ok(())
    }

    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let full = self.full_path(path);
        match std::fs::read(&full) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CheckpointError::Storage {
                message: format!("read {}: {e}", full.display()),
            }),
        }
    }

    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let full = self.full_path(prefix);
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
        let full = self.full_path(prefix);
        match std::fs::remove_dir_all(&full) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CheckpointError::Storage {
                message: format!("delete_prefix {}: {e}", full.display()),
            }),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage() -> LocalFsCheckpointStorage {
        LocalFsCheckpointStorage::ephemeral().unwrap()
    }

    fn sample_metadata(epoch: u64) -> CheckpointMetadata {
        CheckpointMetadata {
            version: CheckpointMetadata::VERSION,
            epoch,
            job_id: "job-test".to_owned(),
            fencing_token: 1,
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

        for epoch in [3u64, 1, 5] {
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
    fn validate_fencing_token_future_token_accepted() {
        let meta = sample_metadata(1); // fencing_token = 1
        // If the coordinator's current token is 1, a meta with token=1 is fine
        // A meta with token=2 is also fine (coordinator upgraded its token)
        let mut meta2 = meta.clone();
        meta2.fencing_token = 2;
        assert!(validate_fencing_token(&meta2, 1).is_ok());
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
}
