#![forbid(unsafe_code)]

//! E4.1 — Incremental RocksDB checkpoint via the rocksdb::Checkpoint API.
//!
//! Wire-or-delete disposition (Phase 51): **keep, currently unwired** — the
//! live-caller wiring (SST-delta checkpoints) is claimed by Phase 56
//! (state v2). See `docs/implementation/wire-or-delete-2026-07.md`.
//!
//! Each epoch creates a hard-linked local snapshot, lists SST files, and
//! uploads only the files not present in the previous epoch. Non-SST metadata
//! files (MANIFEST-*, CURRENT, OPTIONS-*) are small and always uploaded.
//!
//! # Storage layout
//! ```text
//! {prefix}/sst/{filename}                       — shared SST blobs (deduplicated)
//! {prefix}/epochs/{epoch:020}/{meta-filename}   — per-epoch metadata files
//! {prefix}/epochs/{epoch:020}/sst_manifest.json — epoch manifest
//! ```

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointError, CheckpointResult, CheckpointStorage};
use crate::rocksdb_backend::RocksDbStateBackend;

// ── SstFileRef ────────────────────────────────────────────────────────────────

/// Reference to one SST file stored in `CheckpointStorage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstFileRef {
    /// RocksDB SST filename, e.g. `000042.sst`.
    pub filename: String,
    /// Byte size on disk.
    pub size_bytes: u64,
    /// SHA-256 (lowercase hex) of the file content.
    pub sha256_hex: String,
    /// Storage path under the `CheckpointStorage` base.
    pub storage_path: String,
}

// ── EpochMetaFile ─────────────────────────────────────────────────────────────

/// A non-SST metadata file belonging to a specific epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochMetaFile {
    /// Original filename (e.g. `CURRENT`, `MANIFEST-000001`).
    pub filename: String,
    /// Storage path where this file is persisted.
    pub storage_path: String,
}

// ── SstEpochManifest ──────────────────────────────────────────────────────────

/// Complete manifest for one RocksDB checkpoint epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstEpochManifest {
    /// Checkpoint epoch.
    pub epoch: u64,
    /// All SST files required to restore this epoch.
    pub sst_files: Vec<SstFileRef>,
    /// Per-epoch metadata files (MANIFEST-*, CURRENT, OPTIONS-*).
    pub meta_files: Vec<EpochMetaFile>,
    /// Storage path of this manifest's JSON blob.
    pub manifest_storage_path: String,
}

impl SstEpochManifest {
    /// Number of SST files in this epoch.
    pub fn sst_count(&self) -> usize {
        self.sst_files.len()
    }

    /// Total bytes of all SST files.
    pub fn total_sst_bytes(&self) -> u64 {
        self.sst_files.iter().map(|f| f.size_bytes).sum()
    }
}

// ── RocksDbIncrementalCheckpointer ────────────────────────────────────────────

/// Creates incremental RocksDB checkpoints per epoch, uploading only new SST files.
///
/// Maintains an in-memory set of filenames known to be in storage so repeated
/// epochs within the same process run never re-upload unchanged SST files.
/// After a restart, the previous epoch's manifest is consulted to seed the set.
#[derive(Debug, Clone)]
pub struct RocksDbIncrementalCheckpointer {
    work_dir: PathBuf,
    /// Filenames already uploaded to storage during this run.
    uploaded_filenames: HashSet<String>,
}

impl RocksDbIncrementalCheckpointer {
    /// Create a checkpointer using `work_dir` as scratch space for local snapshots.
    pub fn new(work_dir: impl Into<PathBuf>) -> CheckpointResult<Self> {
        let work_dir = work_dir.into();
        std::fs::create_dir_all(&work_dir).map_err(|e| CheckpointError::Storage {
            message: format!("create checkpointer work_dir {}: {e}", work_dir.display()),
        })?;
        Ok(Self {
            work_dir,
            uploaded_filenames: HashSet::new(),
        })
    }

    /// Take an incremental checkpoint of `backend` at `epoch`.
    ///
    /// 1. Creates a hard-linked local snapshot via `rocksdb::Checkpoint`.
    /// 2. Seeds the dedup set from the previous epoch's manifest in storage.
    /// 3. Uploads only SST files not previously uploaded or listed in the prior manifest.
    /// 4. Always uploads per-epoch metadata files (MANIFEST-*, CURRENT, OPTIONS-*).
    /// 5. Writes and returns the new `SstEpochManifest`.
    pub fn take_checkpoint(
        &mut self,
        backend: &RocksDbStateBackend,
        epoch: u64,
        storage: &dyn CheckpointStorage,
        storage_prefix: &str,
    ) -> CheckpointResult<SstEpochManifest> {
        let local_dir = self.work_dir.join(format!("epoch_{epoch:020}"));
        if local_dir.exists() {
            std::fs::remove_dir_all(&local_dir).map_err(|e| CheckpointError::Storage {
                message: format!("clear local checkpoint dir: {e}"),
            })?;
        }

        backend
            .create_rocksdb_checkpoint(&local_dir)
            .map_err(|e| CheckpointError::Storage {
                message: e.to_string(),
            })?;

        self.seed_from_prev_manifest(storage, storage_prefix, epoch)?;

        let (sst_files, meta_files) =
            self.upload_files(&local_dir, epoch, storage, storage_prefix)?;

        let manifest_path = manifest_json_path(storage_prefix, epoch);
        let manifest = SstEpochManifest {
            epoch,
            sst_files,
            meta_files,
            manifest_storage_path: manifest_path.clone(),
        };
        let json = serde_json::to_vec_pretty(&manifest).map_err(|e| CheckpointError::Storage {
            message: format!("manifest serialize: {e}"),
        })?;
        storage.write_bytes(&manifest_path, &json)?;

        let _ = std::fs::remove_dir_all(&local_dir);
        Ok(manifest)
    }

    /// Load the manifest for `epoch` from storage.
    pub fn load_manifest(
        storage: &dyn CheckpointStorage,
        storage_prefix: &str,
        epoch: u64,
    ) -> CheckpointResult<SstEpochManifest> {
        let path = manifest_json_path(storage_prefix, epoch);
        let bytes = storage
            .read_bytes(&path)?
            .ok_or(CheckpointError::NoValidEpoch)?;
        serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Storage {
            message: format!("parse sst manifest: {e}"),
        })
    }

    /// Restore a checkpoint: download all files in `manifest` to `target_dir`.
    ///
    /// After this call `target_dir` contains a complete RocksDB directory that
    /// can be opened with [`RocksDbStateBackend::open`].
    pub fn restore_checkpoint(
        manifest: &SstEpochManifest,
        storage: &dyn CheckpointStorage,
        target_dir: &Path,
    ) -> CheckpointResult<()> {
        std::fs::create_dir_all(target_dir).map_err(|e| CheckpointError::Storage {
            message: format!("create restore dir {}: {e}", target_dir.display()),
        })?;

        for sst in &manifest.sst_files {
            let data =
                storage
                    .read_bytes(&sst.storage_path)?
                    .ok_or_else(|| CheckpointError::Corrupt {
                        epoch: manifest.epoch,
                        message: format!("SST file {} missing from storage", sst.filename),
                    })?;
            verify_sha256(&data, &sst.sha256_hex, &sst.filename, manifest.epoch)?;
            let dest = target_dir.join(&sst.filename);
            std::fs::write(&dest, &data).map_err(|e| CheckpointError::Storage {
                message: format!("write restored SST {}: {e}", dest.display()),
            })?;
        }

        for meta in &manifest.meta_files {
            let data = storage.read_bytes(&meta.storage_path)?.ok_or_else(|| {
                CheckpointError::Corrupt {
                    epoch: manifest.epoch,
                    message: format!("metadata file {} missing", meta.filename),
                }
            })?;
            let dest = target_dir.join(&meta.filename);
            std::fs::write(&dest, &data).map_err(|e| CheckpointError::Storage {
                message: format!("write restored meta {}: {e}", dest.display()),
            })?;
        }

        Ok(())
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn seed_from_prev_manifest(
        &mut self,
        storage: &dyn CheckpointStorage,
        prefix: &str,
        epoch: u64,
    ) -> CheckpointResult<()> {
        if epoch == 0 {
            return Ok(());
        }
        let prev_path = manifest_json_path(prefix, epoch - 1);
        let Some(bytes) = storage.read_bytes(&prev_path)? else {
            return Ok(());
        };
        let prev: SstEpochManifest =
            serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Storage {
                message: format!("parse prev sst manifest: {e}"),
            })?;
        for sst in prev.sst_files {
            self.uploaded_filenames.insert(sst.filename);
        }
        Ok(())
    }

    fn upload_files(
        &mut self,
        local_dir: &Path,
        epoch: u64,
        storage: &dyn CheckpointStorage,
        prefix: &str,
    ) -> CheckpointResult<(Vec<SstFileRef>, Vec<EpochMetaFile>)> {
        let mut sst_files = Vec::new();
        let mut meta_files = Vec::new();

        let entries = std::fs::read_dir(local_dir).map_err(|e| CheckpointError::Storage {
            message: format!("read local checkpoint dir: {e}"),
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| CheckpointError::Storage {
                message: format!("readdir entry: {e}"),
            })?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let filename = entry.file_name().to_string_lossy().into_owned();
            let data = std::fs::read(&path).map_err(|e| CheckpointError::Storage {
                message: format!("read local file {}: {e}", path.display()),
            })?;

            if filename.ends_with(".sst") {
                let sha256 = krishiv_common::hash::sha256_hex(&data);
                let storage_path = format!("{prefix}/sst/{filename}");

                if !self.uploaded_filenames.contains(&filename) {
                    storage.write_bytes(&storage_path, &data)?;
                    self.uploaded_filenames.insert(filename.clone());
                }

                sst_files.push(SstFileRef {
                    filename,
                    size_bytes: data.len() as u64,
                    sha256_hex: sha256,
                    storage_path,
                });
            } else {
                let storage_path = format!("{prefix}/epochs/{epoch:020}/{filename}");
                storage.write_bytes(&storage_path, &data)?;
                meta_files.push(EpochMetaFile {
                    filename,
                    storage_path,
                });
            }
        }

        sst_files.sort_by(|a, b| a.filename.cmp(&b.filename));
        Ok((sst_files, meta_files))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn manifest_json_path(prefix: &str, epoch: u64) -> String {
    format!("{prefix}/epochs/{epoch:020}/sst_manifest.json")
}

fn verify_sha256(data: &[u8], expected: &str, filename: &str, epoch: u64) -> CheckpointResult<()> {
    let actual = krishiv_common::hash::sha256_hex(data);
    if actual != expected {
        return Err(CheckpointError::Corrupt {
            epoch,
            message: format!("SST file {filename}: expected sha256 {expected}, got {actual}"),
        });
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::StateBackend;
    use crate::checkpoint::LocalFsCheckpointStorage;
    use crate::namespace::Namespace;
    use crate::rocksdb_backend::RocksDbStateBackend;

    fn make_storage() -> crate::checkpoint::EphemeralCheckpointStorage {
        LocalFsCheckpointStorage::ephemeral().unwrap()
    }

    fn make_backend() -> (RocksDbStateBackend, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut b = RocksDbStateBackend::open(dir.path()).unwrap();
        let ns = Namespace::new("op1", "counts");
        b.put(&ns, b"key1".to_vec(), b"val1".to_vec()).unwrap();
        b.put(&ns, b"key2".to_vec(), b"val2".to_vec()).unwrap();
        (b, dir)
    }

    #[test]
    fn take_checkpoint_creates_manifest() {
        let storage = make_storage();
        let work = tempfile::tempdir().unwrap();
        let (backend, _db) = make_backend();

        let mut ckpt = RocksDbIncrementalCheckpointer::new(work.path()).unwrap();
        let manifest = ckpt
            .take_checkpoint(&backend, 1, &*storage, "job-1/rocksdb/op-1")
            .unwrap();

        assert_eq!(manifest.epoch, 1);
        assert!(
            !manifest.meta_files.is_empty(),
            "should have CURRENT/MANIFEST meta files"
        );
        assert!(
            storage
                .read_bytes(&manifest.manifest_storage_path)
                .unwrap()
                .is_some(),
            "manifest JSON not written"
        );
    }

    #[test]
    fn second_epoch_includes_all_sst_files() {
        let storage = make_storage();
        let work = tempfile::tempdir().unwrap();
        let (mut backend, _db) = make_backend();

        let mut ckpt = RocksDbIncrementalCheckpointer::new(work.path()).unwrap();
        let m1 = ckpt
            .take_checkpoint(&backend, 1, &*storage, "job-1/rocksdb/op-1")
            .unwrap();

        let ns = Namespace::new("op1", "counts");
        backend
            .put(&ns, b"key3".to_vec(), b"val3".to_vec())
            .unwrap();
        let m2 = ckpt
            .take_checkpoint(&backend, 2, &*storage, "job-1/rocksdb/op-1")
            .unwrap();

        assert!(m2.sst_count() >= m1.sst_count());
    }

    #[test]
    fn restore_checkpoint_roundtrip() {
        let storage = make_storage();
        let work = tempfile::tempdir().unwrap();
        let (backend, _db) = make_backend();

        let mut ckpt = RocksDbIncrementalCheckpointer::new(work.path()).unwrap();
        let manifest = ckpt
            .take_checkpoint(&backend, 1, &*storage, "job-1/rocksdb/op-1")
            .unwrap();

        let restore_dir = tempfile::tempdir().unwrap();
        RocksDbIncrementalCheckpointer::restore_checkpoint(
            &manifest,
            &*storage,
            restore_dir.path(),
        )
        .unwrap();

        for sst in &manifest.sst_files {
            assert!(restore_dir.path().join(&sst.filename).exists());
        }
        for meta in &manifest.meta_files {
            assert!(restore_dir.path().join(&meta.filename).exists());
        }
    }

    #[test]
    fn load_manifest_roundtrip() {
        let storage = make_storage();
        let work = tempfile::tempdir().unwrap();
        let (backend, _db) = make_backend();

        let mut ckpt = RocksDbIncrementalCheckpointer::new(work.path()).unwrap();
        let manifest = ckpt
            .take_checkpoint(&backend, 5, &*storage, "job-2/rocksdb/op-1")
            .unwrap();

        let loaded =
            RocksDbIncrementalCheckpointer::load_manifest(&*storage, "job-2/rocksdb/op-1", 5)
                .unwrap();
        assert_eq!(loaded.epoch, manifest.epoch);
        assert_eq!(loaded.sst_count(), manifest.sst_count());
    }

    #[test]
    fn restore_detects_sst_corruption() {
        let storage = make_storage();
        let work = tempfile::tempdir().unwrap();
        let (backend, _db) = make_backend();

        let mut ckpt = RocksDbIncrementalCheckpointer::new(work.path()).unwrap();
        let mut manifest = ckpt
            .take_checkpoint(&backend, 1, &*storage, "job-3/rocksdb/op-1")
            .unwrap();

        if let Some(sst) = manifest.sst_files.first_mut() {
            sst.sha256_hex = "deadbeef".repeat(8);
        } else {
            return;
        }

        let restore_dir = tempfile::tempdir().unwrap();
        let result = RocksDbIncrementalCheckpointer::restore_checkpoint(
            &manifest,
            &*storage,
            restore_dir.path(),
        );
        assert!(result.is_err(), "restore should fail on hash mismatch");
    }
}
