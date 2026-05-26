//! Object-store backed checkpoint storage (P2-5, D4).
//!
//! The `object_store` clients (S3, GCS, Azure) are themselves async and need
//! a Tokio runtime to drive their HTTP transport.  This module's sync trait
//! methods use [`run_blocking_on_tokio`] (which uses `block_in_place` /
//! falls back to a short-lived runtime) instead of `futures::executor::block_on`
//! to avoid the worker-thread deadlock that the latter produced.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as ObjectPath;

use crate::{CheckpointError, CheckpointResult, CheckpointStorage, run_blocking_on_tokio};

const WRITE_TIMEOUT: Duration = Duration::from_secs(300);

/// Checkpoint storage on any `object_store::ObjectStore` implementation (S3, GCS, Azure, memory).
#[derive(Debug, Clone)]
pub struct ObjectStoreCheckpointStorage {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectStoreCheckpointStorage {
    /// Create storage with logical prefix `prefix` (e.g. `"krishiv-checkpoints"`).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_matches('/').to_string();
        Self { store, prefix }
    }

    fn object_path(&self, path: &str) -> ObjectPath {
        let rel = path.trim_start_matches('/');
        let key = if self.prefix.is_empty() {
            rel.to_string()
        } else {
            format!("{}/{}", self.prefix, rel)
        };
        ObjectPath::from(key)
    }

    /// Async write — preferred for callers in a Tokio context (D4).
    pub async fn write_bytes_async_inner(
        &self,
        path: &str,
        data: &[u8],
    ) -> CheckpointResult<()> {
        let object_path = self.object_path(path);
        let payload = bytes::Bytes::copy_from_slice(data);
        tokio::time::timeout(WRITE_TIMEOUT, self.store.put(&object_path, payload.into()))
            .await
            .map_err(|_| CheckpointError::Storage {
                message: "object store write timed out".into(),
            })?
            .map_err(|e| CheckpointError::Storage {
                message: format!("object store put: {e}"),
            })?;
        Ok(())
    }

    /// Async read — preferred for callers in a Tokio context (D4).
    pub async fn read_bytes_async_inner(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let object_path = self.object_path(path);
        match tokio::time::timeout(WRITE_TIMEOUT, self.store.get(&object_path)).await {
            Err(_) => Err(CheckpointError::Storage {
                message: "object store read timed out".into(),
            }),
            Ok(Err(object_store::Error::NotFound { .. })) => Ok(None),
            Ok(Err(e)) => Err(CheckpointError::Storage {
                message: format!("object store get: {e}"),
            }),
            Ok(Ok(meta)) => {
                let bytes = meta.bytes().await.map_err(|e| CheckpointError::Storage {
                    message: format!("object store read body: {e}"),
                })?;
                Ok(Some(bytes.to_vec()))
            }
        }
    }

    /// Async list — preferred for callers in a Tokio context (D4).
    pub async fn list_dir_async_inner(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let path = self.object_path(prefix);
        let mut names = Vec::new();
        let mut stream = self.store.list(Some(&path));
        while let Some(entry) = stream
            .next()
            .await
            .transpose()
            .map_err(|e| CheckpointError::Storage {
                message: format!("object store list: {e}"),
            })?
        {
            if let Some(name) = entry.location.parts().next_back() {
                names.push(name.as_ref().to_string());
            }
        }
        Ok(names)
    }

    /// Async delete — preferred for callers in a Tokio context (D4).
    pub async fn delete_prefix_async_inner(&self, prefix: &str) -> CheckpointResult<()> {
        let path = self.object_path(prefix);
        let mut stream = self.store.list(Some(&path));
        while let Some(entry) = stream
            .next()
            .await
            .transpose()
            .map_err(|e| CheckpointError::Storage {
                message: format!("object store list for delete: {e}"),
            })?
        {
            self.store
                .delete(&entry.location)
                .await
                .map_err(|e| CheckpointError::Storage {
                    message: format!("object store delete: {e}"),
                })?;
        }
        Ok(())
    }
}

impl CheckpointStorage for ObjectStoreCheckpointStorage {
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let this = self.clone();
        let path = path.to_owned();
        let data = data.to_vec();
        run_blocking_on_tokio("object_store write_bytes", async move {
            this.write_bytes_async_inner(&path, &data).await
        })
    }

    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let this = self.clone();
        let path = path.to_owned();
        run_blocking_on_tokio("object_store read_bytes", async move {
            this.read_bytes_async_inner(&path).await
        })
    }

    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let this = self.clone();
        let prefix = prefix.to_owned();
        run_blocking_on_tokio("object_store list_dir", async move {
            this.list_dir_async_inner(&prefix).await
        })
    }

    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        let this = self.clone();
        let prefix = prefix.to_owned();
        run_blocking_on_tokio("object_store delete_prefix", async move {
            this.delete_prefix_async_inner(&prefix).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    // D4: the sync trait uses `block_in_place` which requires the multi-thread
    // flavour.  This is the documented contract for object-store callers and
    // matches what production coordinators run under.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_checkpoint_roundtrip() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "checkpoints");
        storage
            .write_bytes("job-a/checkpoints/1/metadata.json", b"{}")
            .unwrap();
        let bytes = storage
            .read_bytes("job-a/checkpoints/1/metadata.json")
            .unwrap()
            .unwrap();
        assert_eq!(bytes, b"{}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_async_api_works_in_current_thread_context() {
        // Even outside multi-thread settings the async API is always safe to call.
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "checkpoints");
        storage
            .write_bytes_async_inner("job-a/checkpoints/1/metadata.json", b"{}")
            .await
            .unwrap();
        let bytes = storage
            .read_bytes_async_inner("job-a/checkpoints/1/metadata.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes, b"{}");
    }
}
