use std::path::PathBuf;

use crate::checkpoint::metadata::{CheckpointError, CheckpointResult};

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

    pub(crate) fn full_path(&self, path: &str) -> CheckpointResult<PathBuf> {
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
