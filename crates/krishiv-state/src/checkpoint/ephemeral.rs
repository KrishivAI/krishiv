use std::path::PathBuf;

use crate::checkpoint::local_fs::LocalFsCheckpointStorage;
use crate::checkpoint::metadata::{CheckpointError, CheckpointResult};
use crate::checkpoint::storage_trait::{CheckpointStorage, uuid_simple};

impl LocalFsCheckpointStorage {
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
        // Best-effort cleanup; ignore errors (e.g. already removed) but log
        // unexpected failures so a half-cleaned test scratch dir is visible
        // to the operator instead of silently leaking.
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            tracing::debug!(
                path = %self.path.display(),
                error = %error,
                "ephemeral checkpoint storage cleanup failed (best-effort)",
            );
        }
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
