//! Open shuffle storage from production URIs (`file://`, `s3://`).

use std::sync::Arc;

use krishiv_common::DurabilityProfile;

use crate::{
    LocalDiskShuffleStore, ObjectStoreShuffleStore, ShuffleBackend, ShuffleError, ShuffleResult,
};

/// Open a shuffle backend for the configured URI and durability profile.
///
/// - `file://path` or bare path — local disk shuffle store
/// - `s3://bucket/prefix` — object-store shuffle (distributed durable)
pub fn open_shuffle_backend_from_uri(
    uri: &str,
    profile: DurabilityProfile,
) -> ShuffleResult<Arc<ShuffleBackend>> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return Err(ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "shuffle URI is empty",
        )));
    }

    if trimmed == "memory://" || trimmed.starts_with("memory://") {
        if !krishiv_common::allows_unbounded_shuffle_store(profile) {
            return Err(ShuffleError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("memory:// shuffle URIs are forbidden for durability profile '{profile}'"),
            )));
        }
        return Ok(Arc::new(ShuffleBackend::InMemory(Arc::new(
            crate::InMemoryShuffleStore::new(),
        ))));
    }

    if let Some(rest) = trimmed.strip_prefix("s3://") {
        if profile != DurabilityProfile::DistributedDurable
            && profile != DurabilityProfile::DevLocal
        {
            return Err(ShuffleError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "s3:// shuffle requires distributed-durable or dev-local profile",
            )));
        }
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
            .map_err(|e| {
                ShuffleError::Io(std::io::Error::other(format!(
                    "s3 shuffle store {url}: {e}"
                )))
            })?;
        let storage_prefix = if prefix.is_empty() {
            "shuffle".to_owned()
        } else {
            prefix.to_owned()
        };
        let object = Arc::new(ObjectStoreShuffleStore::new(
            Arc::new(store),
            storage_prefix,
        ));
        return Ok(Arc::new(ShuffleBackend::Object(object)));
    }

    let path = trimmed.strip_prefix("file://").unwrap_or(trimmed);
    let disk = Arc::new(LocalDiskShuffleStore::new(path)?);
    Ok(Arc::new(ShuffleBackend::Local(disk)))
}
