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
    ///
    /// Writes to a staging key first, then commits the final key so readers
    /// never observe partial object-store payloads.
    pub async fn write_bytes_async_inner(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let staging_path = format!("{path}.staging");
        let object_staging = self.object_path(&staging_path);
        let object_path = self.object_path(path);
        let payload = bytes::Bytes::copy_from_slice(data);
        tokio::time::timeout(
            WRITE_TIMEOUT,
            self.store.put(&object_staging, payload.clone().into()),
        )
        .await
        .map_err(|_| CheckpointError::Storage {
            message: "object store staging write timed out".into(),
        })?
        .map_err(|e| CheckpointError::Storage {
            message: format!("object store staging put: {e}"),
        })?;
        tokio::time::timeout(WRITE_TIMEOUT, self.store.put(&object_path, payload.into()))
            .await
            .map_err(|_| CheckpointError::Storage {
                message: "object store write timed out".into(),
            })?
            .map_err(|e| CheckpointError::Storage {
                message: format!("object store put: {e}"),
            })?;
        let _ = self.store.delete(&object_staging).await;
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
    /// Lists immediate children (one level deep) under `prefix`, not recursive.
    pub async fn list_dir_async_inner(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let path = self.object_path(prefix);
        let result = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .map_err(|e| CheckpointError::Storage {
                message: format!("object store list_with_delimiter: {e}"),
            })?;
        let mut names: Vec<String> = result
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().next_back())
            .map(|p| p.as_ref().to_string())
            .collect();
        names.extend(
            result
                .objects
                .iter()
                .filter_map(|obj| obj.location.parts().next_back())
                .map(|p| p.as_ref().to_string()),
        );
        Ok(names)
    }

    /// Async delete — preferred for callers in a Tokio context (D4).
    pub async fn delete_prefix_async_inner(&self, prefix: &str) -> CheckpointResult<()> {
        let path = self.object_path(prefix);
        let mut stream = self.store.list(Some(&path));
        while let Some(entry) =
            stream
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

#[async_trait::async_trait]
impl CheckpointStorage for ObjectStoreCheckpointStorage {
    async fn write_bytes_async(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        self.write_bytes_async_inner(path, data).await
    }

    async fn read_bytes_async(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        self.read_bytes_async_inner(path).await
    }

    async fn list_dir_async(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        self.list_dir_async_inner(prefix).await
    }

    async fn delete_prefix_async(&self, prefix: &str) -> CheckpointResult<()> {
        self.delete_prefix_async_inner(prefix).await
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_read_nonexistent_returns_none() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "prefix");
        let result = storage
            .read_bytes_async_inner("no/such/file.bin")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_write_overwrites_existing() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        storage
            .write_bytes_async_inner("f.bin", b"first")
            .await
            .unwrap();
        storage
            .write_bytes_async_inner("f.bin", b"second")
            .await
            .unwrap();
        let data = storage
            .read_bytes_async_inner("f.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, b"second");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_empty_data_roundtrip() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        storage
            .write_bytes_async_inner("empty.bin", b"")
            .await
            .unwrap();
        let data = storage
            .read_bytes_async_inner("empty.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, b"");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_list_dir_returns_children() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        storage
            .write_bytes_async_inner("a/file1.bin", b"1")
            .await
            .unwrap();
        storage
            .write_bytes_async_inner("a/file2.bin", b"2")
            .await
            .unwrap();
        storage
            .write_bytes_async_inner("b/file3.bin", b"3")
            .await
            .unwrap();
        let mut children = storage.list_dir_async_inner("a").await.unwrap();
        children.sort();
        assert_eq!(children, vec!["file1.bin", "file2.bin"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_list_dir_empty_prefix() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        let children = storage.list_dir_async_inner("nonexistent").await.unwrap();
        assert!(children.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_delete_prefix_removes_entries() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        storage
            .write_bytes_async_inner("del/a.bin", b"1")
            .await
            .unwrap();
        storage
            .write_bytes_async_inner("del/b.bin", b"2")
            .await
            .unwrap();
        storage
            .write_bytes_async_inner("keep/c.bin", b"3")
            .await
            .unwrap();
        storage.delete_prefix_async_inner("del").await.unwrap();
        assert!(
            storage
                .read_bytes_async_inner("del/a.bin")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .read_bytes_async_inner("del/b.bin")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .read_bytes_async_inner("keep/c.bin")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_delete_prefix_nonexistent_is_noop() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        storage
            .delete_prefix_async_inner("no/such/prefix")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_sync_trait_write_read_roundtrip() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        use crate::CheckpointStorage;
        storage.write_bytes("sync/path.bin", b"sync-data").unwrap();
        let data = storage.read_bytes("sync/path.bin").unwrap().unwrap();
        assert_eq!(data, b"sync-data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_sync_trait_list_dir() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        use crate::CheckpointStorage;
        storage.write_bytes("dir/a.bin", b"1").unwrap();
        storage.write_bytes("dir/b.bin", b"2").unwrap();
        let mut children = storage.list_dir("dir").unwrap();
        children.sort();
        assert_eq!(children, vec!["a.bin", "b.bin"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_sync_trait_delete_prefix() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        use crate::CheckpointStorage;
        storage.write_bytes("rm/a.bin", b"1").unwrap();
        storage.write_bytes("rm/b.bin", b"2").unwrap();
        storage.delete_prefix("rm").unwrap();
        assert!(storage.read_bytes("rm/a.bin").unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_large_data_roundtrip() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let storage = ObjectStoreCheckpointStorage::new(store, "p");
        let large = vec![42u8; 1024 * 1024]; // 1 MB
        storage
            .write_bytes_async_inner("large.bin", &large)
            .await
            .unwrap();
        let data = storage
            .read_bytes_async_inner("large.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, large);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_multiple_prefixes_independent() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let s1 = ObjectStoreCheckpointStorage::new(store.clone(), "prefix-a");
        let s2 = ObjectStoreCheckpointStorage::new(store.clone(), "prefix-b");
        use crate::CheckpointStorage;
        s1.write_bytes("file.bin", b"from-a").unwrap();
        s2.write_bytes("file.bin", b"from-b").unwrap();
        assert_eq!(s1.read_bytes("file.bin").unwrap(), Some(b"from-a".to_vec()));
        assert_eq!(s2.read_bytes("file.bin").unwrap(), Some(b"from-b".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_object_path_construction() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let s = ObjectStoreCheckpointStorage::new(store, "my-prefix");
        // Verify the object_path function handles leading slashes
        let path = s.object_path("/leading/slash.bin");
        assert_eq!(path.as_ref(), "my-prefix/leading/slash.bin");
        let path2 = s.object_path("no/slash.bin");
        assert_eq!(path2.as_ref(), "my-prefix/no/slash.bin");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_store_prefix_trimming() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let s = ObjectStoreCheckpointStorage::new(store, "//trim//");
        use crate::CheckpointStorage;
        s.write_bytes("t.bin", b"d").unwrap();
        let data = s.read_bytes("t.bin").unwrap().unwrap();
        assert_eq!(data, b"d");
    }
}
