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
        // Phase 1: strip known-dangerous components before joining so that
        // the candidate path does not escape the base directory via `..` or
        // absolute paths.
        let clean: PathBuf = path
            .split('/')
            .filter(|c| !c.is_empty() && *c != "..")
            .collect();
        let candidate = self.base_dir.join(clean);

        // Phase 2: resolve symlinks so that a symlink pointing outside
        // base_dir cannot bypass the prefix check above.
        // We call `canonicalize` on the *parent* directory rather than the
        // candidate itself so that the operation succeeds even when the file
        // does not yet exist (e.g. before a write creates it).
        let canonical_base =
            self.base_dir
                .canonicalize()
                .map_err(|e| CheckpointError::InvalidPath {
                    path: format!(
                        "cannot canonicalize base_dir {}: {e}",
                        self.base_dir.display()
                    ),
                })?;

        let parent = candidate.parent().unwrap_or(&candidate);
        let canonical_parent = if parent.exists() {
            parent
                .canonicalize()
                .map_err(|e| CheckpointError::InvalidPath {
                    path: format!("cannot canonicalize parent {}: {e}", parent.display()),
                })?
        } else {
            parent.to_path_buf()
        };

        // Phase 3: verify the resolved parent (and therefore the file) stays
        // within the canonical base directory.
        if !canonical_parent.starts_with(&canonical_base) {
            return Err(CheckpointError::InvalidPath {
                path: candidate.display().to_string(),
            });
        }

        // Reconstruct the full path using the canonical parent so that the
        // returned path is consistent with the filesystem view.
        Ok(canonical_parent.join(candidate.file_name().unwrap_or_default()))
    }
}
