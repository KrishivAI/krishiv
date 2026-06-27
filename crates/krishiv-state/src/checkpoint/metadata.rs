use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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
/// Versions 1 and 2 are restore-compatible. Version 2 adds coordinator identity;
/// version 3 adds unaligned buffer refs, durable sink transactions, and per-epoch
/// runtime profile. Restore rejects versions outside the published compatibility
/// window.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointMetadata {
    /// Format version. New checkpoints use [`CheckpointMetadata::VERSION`].
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
    /// In-flight buffer references for unaligned checkpoints (v3+).
    ///
    /// Each entry describes a buffer of post-barrier records that was captured
    /// during an unaligned checkpoint and must be replayed on restore.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unaligned_buffer_refs: Vec<UnalignedBufferRef>,
    /// Durable prepared-sink transaction references (v3+).
    ///
    /// Each entry records a sink that prepared a write during this epoch.
    /// On restore, these are re-committed or aborted depending on whether the
    /// epoch was successfully committed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sink_transactions: Vec<SinkTransactionRef>,
    /// Streaming execution profile used for this epoch (v3+).
    ///
    /// Recorded so that restore can verify the runtime was configured consistently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streaming_profile: Option<String>,
}

impl CheckpointMetadata {
    /// Oldest metadata format accepted for restore.
    pub const MIN_SUPPORTED_VERSION: u32 = 1;
    /// Current metadata format written by the engine.
    pub const VERSION: u32 = 3;

    /// Validate that this metadata can be used for restore.
    pub fn validate(&self) -> CheckpointResult<()> {
        if !(Self::MIN_SUPPORTED_VERSION..=Self::VERSION).contains(&self.version) {
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
    /// Legacy numeric offset retained for existing metadata and status tools.
    pub offset: i64,
    /// Connector-encoded exact offset bytes for restore.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encoded_offset: Vec<u8>,
}

/// Reference to the state snapshot file for one operator instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorSnapshotRef {
    pub operator_id: String,
    pub task_id: String,
    /// Path to `state.bin` relative to the checkpoint storage base directory.
    pub snapshot_path: String,
}

/// Reference to an in-flight buffer captured during an unaligned checkpoint.
///
/// When a checkpoint barrier overtakes in-flight data (unaligned mode),
/// the post-barrier records are buffered and their reference is stored in
/// checkpoint metadata. On restore, these buffers must be replayed before
/// the operator processes new data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnalignedBufferRef {
    /// Operator that owns this buffer.
    pub operator_id: String,
    /// Input channel index that received the buffered records.
    pub channel_index: u32,
    /// Number of records in the buffer.
    pub record_count: u64,
    /// Path to the serialized buffer data relative to the checkpoint base.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub buffer_path: String,
}

/// Reference to a durable prepared-sink transaction.
///
/// When a two-phase sink prepares a write during an epoch, the prepare record
/// is durably stored so that the transaction can be committed or aborted
/// depending on whether the checkpoint epoch is successfully committed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SinkTransactionRef {
    /// Sink identifier.
    pub sink_id: String,
    /// Epoch in which the sink was prepared.
    pub epoch: u64,
    /// Path to the prepared transaction data relative to the checkpoint base.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prepare_path: String,
    /// Whether this transaction has been committed.
    pub committed: bool,
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

    /// Whether `relative_path` is recorded in this manifest.
    pub fn contains(&self, relative_path: &str) -> bool {
        self.entries.contains_key(relative_path)
    }

    /// Iterate manifest entries as `(relative_path, sha256_hex)` pairs.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(path, hex)| (path.as_str(), hex.as_str()))
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

pub(super) fn sha256_hex(data: &[u8]) -> String {
    krishiv_common::hash::sha256_hex(data)
}
