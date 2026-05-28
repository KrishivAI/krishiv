//! Open checkpoint storage from production URIs (`file://`, `s3://`, `memory://`).

use std::sync::Arc;

use object_store::ObjectStore;

use crate::ObjectStoreCheckpointStorage;
use crate::{CheckpointError, CheckpointResult, CheckpointStorage, LocalFsCheckpointStorage};

/// Open checkpoint storage for a configured path or URI.
///
/// - `memory://` — in-memory object store (tests)
/// - `s3://bucket/prefix` — S3-compatible object store (production)
/// - `file://path` or bare filesystem path — local FS with fsync
pub fn open_checkpoint_storage_from_uri(uri: &str) -> CheckpointResult<Arc<dyn CheckpointStorage>> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return Err(CheckpointError::Storage {
            message: "checkpoint storage URI is empty".into(),
        });
    }
    if trimmed == "memory://" || trimmed.starts_with("memory://") {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let prefix = trimmed
            .strip_prefix("memory://")
            .unwrap_or("")
            .trim_matches('/');
        return Ok(Arc::new(ObjectStoreCheckpointStorage::new(store, prefix)));
    }
    if let Some(rest) = trimmed.strip_prefix("s3://") {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p.trim_matches('/')),
            None => (rest, ""),
        };
        let url = if prefix.is_empty() {
            format!("s3://{bucket}")
        } else {
            format!("s3://{bucket}/{prefix}")
        };
        let store = object_store::aws::AmazonS3Builder::from_env()
            .with_url(&url)
            .build()
            .map_err(|e| CheckpointError::Storage {
                message: format!("s3 checkpoint store {url}: {e}"),
            })?;
        // Use the parsed URI path as the storage prefix, falling back to
        // "checkpoints" when no path component was provided.
        let storage_prefix = if prefix.is_empty() {
            "checkpoints".to_owned()
        } else {
            prefix.to_owned()
        };
        return Ok(Arc::new(ObjectStoreCheckpointStorage::new(
            Arc::new(store),
            storage_prefix,
        )));
    }
    let path = trimmed.strip_prefix("file://").unwrap_or(trimmed);
    Ok(Arc::new(LocalFsCheckpointStorage::new(path).map_err(
        |e| CheckpointError::Storage {
            message: format!("local checkpoint storage at {path}: {e}"),
        },
    )?))
}
