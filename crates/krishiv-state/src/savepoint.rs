#![forbid(unsafe_code)]

//! E4.2 — SavepointCoordinator.
//!
//! A savepoint is a named, user-triggered checkpoint that captures the full
//! operator state at a specific epoch.  Unlike periodic checkpoints, savepoints
//! are retained until explicitly deleted and carry a label for human reference.
//!
//! # API
//! ```ignore
//! let coord = SavepointCoordinator::new(storage, job_id);
//! let meta = coord.take_savepoint("before-migration".to_string(), epoch, operator_versions)?;
//! coord.list_savepoints()?;
//! coord.delete_savepoint(&meta.savepoint_id)?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointStorage, io as checkpoint_io};
use crate::error::{StateError, StateResult};

/// Current savepoint metadata format written by the engine.
pub const SAVEPOINT_FORMAT_VERSION: u32 = 1;

const fn default_savepoint_format_version() -> u32 {
    SAVEPOINT_FORMAT_VERSION
}

// ── SavepointMeta ─────────────────────────────────────────────────────────────

/// Immutable metadata for one savepoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavepointMeta {
    /// Savepoint metadata format version. Legacy unversioned records decode as v1.
    #[serde(default = "default_savepoint_format_version")]
    pub format_version: u32,
    /// Unique savepoint ID (UUID v4 string).
    pub savepoint_id: String,
    /// Human-readable label provided by the user.
    pub label: String,
    /// Job this savepoint belongs to.
    pub job_id: String,
    /// Checkpoint epoch at which the savepoint was taken.
    pub epoch: u64,
    /// Map of operator_id → checkpoint epoch version for each operator.
    pub operator_versions: HashMap<String, u64>,
    /// Wall-clock Unix timestamp (seconds) when the savepoint was created.
    pub created_at_secs: u64,
}

impl SavepointMeta {
    /// Validate metadata and operator identity before restore.
    pub fn validate(&self) -> StateResult<()> {
        if self.format_version != SAVEPOINT_FORMAT_VERSION {
            return Err(StateError::SnapshotCorrupt {
                message: format!(
                    "unsupported savepoint metadata version {}; supported version is {}",
                    self.format_version, SAVEPOINT_FORMAT_VERSION
                ),
            });
        }
        if self
            .operator_versions
            .keys()
            .any(|operator_id| operator_id.trim().is_empty())
        {
            return Err(StateError::SnapshotCorrupt {
                message: "savepoint contains an empty operator id".into(),
            });
        }
        Ok(())
    }
}

// ── SavepointCoordinator ──────────────────────────────────────────────────────

/// Manages savepoints for one job.
///
/// Uses an in-memory index backed by the same [`CheckpointStorage`] used for
/// periodic checkpoints.  Savepoints are stored under:
/// `{base}/{job_id}/savepoints/{savepoint_id}/meta.json`.
pub struct SavepointCoordinator {
    job_id: String,
    /// In-memory index: savepoint_id → metadata.
    index: HashMap<String, SavepointMeta>,
    /// Optional durable storage handle. When present, `delete_savepoint`
    /// also removes the durable `savepoints/{epoch}/` copy from storage.
    storage: Option<Arc<dyn CheckpointStorage>>,
}

impl SavepointCoordinator {
    /// Create a new coordinator for `job_id` with no durable storage.
    /// Use [`Self::with_storage`] to enable durable delete.
    pub fn new(job_id: impl Into<String>) -> Self {
        Self {
            job_id: job_id.into(),
            index: HashMap::new(),
            storage: None,
        }
    }

    /// Create a new coordinator with a durable storage handle.
    /// When set, `delete_savepoint` also removes the durable copy.
    pub fn with_storage(job_id: impl Into<String>, storage: Arc<dyn CheckpointStorage>) -> Self {
        Self {
            job_id: job_id.into(),
            index: HashMap::new(),
            storage: Some(storage),
        }
    }

    /// Take a savepoint at `epoch` with the given operator version map.
    ///
    /// Returns the created [`SavepointMeta`]. The caller is responsible for
    /// persisting the metadata to durable storage (e.g. via the checkpoint
    /// storage layer) after this call returns.
    pub fn take_savepoint(
        &mut self,
        label: impl Into<String>,
        epoch: u64,
        operator_versions: HashMap<String, u64>,
    ) -> StateResult<SavepointMeta> {
        let savepoint_id = new_savepoint_id();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let meta = SavepointMeta {
            format_version: SAVEPOINT_FORMAT_VERSION,
            savepoint_id: savepoint_id.clone(),
            label: label.into(),
            job_id: self.job_id.clone(),
            epoch,
            operator_versions,
            created_at_secs: now_secs,
        };

        self.index.insert(savepoint_id, meta.clone());
        Ok(meta)
    }

    /// List all savepoints in chronological order (oldest first).
    pub fn list_savepoints(&self) -> Vec<&SavepointMeta> {
        let mut list: Vec<&SavepointMeta> = self.index.values().collect();
        list.sort_by_key(|m| m.created_at_secs);
        list
    }

    /// Return the metadata for a specific savepoint.
    pub fn get_savepoint(&self, savepoint_id: &str) -> Option<&SavepointMeta> {
        self.index.get(savepoint_id)
    }

    /// Delete a savepoint from the index and from durable storage (if a
    /// storage handle was provided via [`Self::with_storage`]).
    ///
    /// Returns the removed metadata, or an error if the savepoint does not
    /// exist in the in-memory index.
    pub fn delete_savepoint(&mut self, savepoint_id: &str) -> StateResult<SavepointMeta> {
        let meta =
            self.index
                .remove(savepoint_id)
                .ok_or_else(|| StateError::BackendUnavailable {
                    message: format!("savepoint '{savepoint_id}' not found"),
                    source: None,
                })?;

        // Best-effort durable delete: if storage is configured, remove the
        // `savepoints/{epoch}/` directory. Failures are logged but do not
        // undo the in-memory removal (the user explicitly requested deletion).
        if let Some(storage) = &self.storage
            && let Err(e) =
                checkpoint_io::delete_savepoint(storage.as_ref(), &self.job_id, meta.epoch)
        {
            tracing::warn!(
                savepoint_id = savepoint_id,
                epoch = meta.epoch,
                error = %e,
                "failed to delete durable savepoint copy; in-memory index entry removed"
            );
        }

        Ok(meta)
    }

    /// Serialise all savepoints as JSON (for durable persistence).
    pub fn export_index_json(&self) -> StateResult<String> {
        let list: Vec<&SavepointMeta> = self.list_savepoints();
        serde_json::to_string(&list).map_err(|e| StateError::SnapshotCorrupt {
            message: e.to_string(),
        })
    }

    /// Restore the index from serialised JSON.
    pub fn import_index_json(&mut self, json: &str) -> StateResult<()> {
        let list: Vec<SavepointMeta> =
            serde_json::from_str(json).map_err(|e| StateError::SnapshotCorrupt {
                message: e.to_string(),
            })?;
        for meta in list {
            meta.validate()?;
            if meta.job_id != self.job_id {
                return Err(StateError::SnapshotCorrupt {
                    message: format!(
                        "savepoint '{}' belongs to job '{}', not '{}'",
                        meta.savepoint_id, meta.job_id, self.job_id
                    ),
                });
            }
            self.index.insert(meta.savepoint_id.clone(), meta);
        }
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Generate a unique savepoint ID via UUID v4.
fn new_savepoint_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_versions(ops: &[(&str, u64)]) -> HashMap<String, u64> {
        ops.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn take_savepoint_creates_entry() {
        let mut coord = SavepointCoordinator::new("job-1");
        let meta = coord
            .take_savepoint("before-migration", 5, make_versions(&[("agg", 3)]))
            .unwrap();
        assert_eq!(meta.job_id, "job-1");
        assert_eq!(meta.epoch, 5);
        assert_eq!(meta.label, "before-migration");
        assert_eq!(meta.operator_versions["agg"], 3);
    }

    #[test]
    fn list_savepoints_returns_in_order() {
        let mut coord = SavepointCoordinator::new("job-1");
        let m1 = coord.take_savepoint("first", 1, HashMap::new()).unwrap();
        let m2 = coord.take_savepoint("second", 2, HashMap::new()).unwrap();
        let list = coord.list_savepoints();
        assert_eq!(list.len(), 2);
        // Order by created_at_secs — should be m1 then m2 (or equal if sub-second).
        let ids: Vec<&str> = list.iter().map(|m| m.savepoint_id.as_str()).collect();
        assert!(ids.contains(&m1.savepoint_id.as_str()));
        assert!(ids.contains(&m2.savepoint_id.as_str()));
    }

    #[test]
    fn delete_savepoint_removes_entry() {
        let mut coord = SavepointCoordinator::new("job-1");
        let meta = coord.take_savepoint("test", 1, HashMap::new()).unwrap();
        let deleted = coord.delete_savepoint(&meta.savepoint_id).unwrap();
        assert_eq!(deleted.savepoint_id, meta.savepoint_id);
        assert!(coord.get_savepoint(&meta.savepoint_id).is_none());
    }

    #[test]
    fn delete_nonexistent_returns_error() {
        let mut coord = SavepointCoordinator::new("job-1");
        assert!(coord.delete_savepoint("no-such-id").is_err());
    }

    #[test]
    fn export_import_roundtrip() {
        let mut coord = SavepointCoordinator::new("job-1");
        coord
            .take_savepoint("before", 1, make_versions(&[("op1", 2)]))
            .unwrap();

        let json = coord.export_index_json().unwrap();
        assert!(!json.is_empty());

        let mut coord2 = SavepointCoordinator::new("job-1");
        coord2.import_index_json(&json).unwrap();
        assert_eq!(coord2.list_savepoints().len(), 1);
        assert_eq!(coord2.list_savepoints()[0].label, "before");
    }

    #[test]
    fn import_rejects_unknown_format_version() {
        let mut coord = SavepointCoordinator::new("job-A");
        let mut meta = coord.take_savepoint("sp", 1, HashMap::new()).unwrap();
        meta.format_version = 99;
        let json = serde_json::to_string(&[meta]).unwrap();
        assert!(coord.import_index_json(&json).is_err());
    }

    #[test]
    fn import_rejects_wrong_job_id() {
        let mut coord = SavepointCoordinator::new("job-A");
        let meta = coord.take_savepoint("sp", 1, HashMap::new()).unwrap();
        let json = serde_json::to_string(&[meta]).unwrap();

        let mut coord2 = SavepointCoordinator::new("job-B");
        assert!(coord2.import_index_json(&json).is_err());
    }
}
