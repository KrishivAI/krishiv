//! Object-store backed checkpoint storage (P2-5).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as ObjectPath;

use crate::{CheckpointError, CheckpointResult, CheckpointStorage};

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
}

impl CheckpointStorage for ObjectStoreCheckpointStorage {
    fn write_bytes(&self, path: &str, data: &[u8]) -> CheckpointResult<()> {
        let store = self.store.clone();
        let object_path = self.object_path(path);
        let payload = data.to_vec();
        futures::executor::block_on(async move {
            tokio::time::timeout(WRITE_TIMEOUT, store.put(&object_path, payload.into()))
                .await
                .map_err(|_| CheckpointError::Storage {
                    message: "object store write timed out".into(),
                })?
                .map_err(|e| CheckpointError::Storage {
                    message: format!("object store put: {e}"),
                })?;
            Ok(())
        })
    }

    fn read_bytes(&self, path: &str) -> CheckpointResult<Option<Vec<u8>>> {
        let store = self.store.clone();
        let object_path = self.object_path(path);
        futures::executor::block_on(async move {
            match tokio::time::timeout(WRITE_TIMEOUT, store.get(&object_path)).await {
                Err(_) => Err(CheckpointError::Storage {
                    message: "object store read timed out".into(),
                }),
                Ok(Err(object_store::Error::NotFound { .. })) => Ok(None),
                Ok(Err(e)) => Err(CheckpointError::Storage {
                    message: format!("object store get: {e}"),
                }),
                Ok(Ok(meta)) => {
                    let bytes = meta
                        .bytes()
                        .await
                        .map_err(|e| CheckpointError::Storage {
                            message: format!("object store read body: {e}"),
                        })?;
                    Ok(Some(bytes.to_vec()))
                }
            }
        })
    }

    fn list_dir(&self, prefix: &str) -> CheckpointResult<Vec<String>> {
        let store = self.store.clone();
        let path = self.object_path(prefix);
        futures::executor::block_on(async move {
            let mut names = Vec::new();
            let mut stream = store.list(Some(&path));
            while let Some(entry) = stream.next().await.transpose().map_err(|e| {
                CheckpointError::Storage {
                    message: format!("object store list: {e}"),
                }
            })? {
                if let Some(name) = entry.location.parts().last() {
                    names.push(name.as_ref().to_string());
                }
            }
            Ok(names)
        })
    }

    fn delete_prefix(&self, prefix: &str) -> CheckpointResult<()> {
        let store = self.store.clone();
        let path = self.object_path(prefix);
        futures::executor::block_on(async move {
            let mut stream = store.list(Some(&path));
            while let Some(entry) = stream.next().await.transpose().map_err(|e| {
                CheckpointError::Storage {
                    message: format!("object store list for delete: {e}"),
                }
            })? {
                store.delete(&entry.location).await.map_err(|e| CheckpointError::Storage {
                    message: format!("object store delete: {e}"),
                })?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
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
}
