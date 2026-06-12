//! Open shuffle storage from production URIs (`file://`, `s3://`).

use std::sync::Arc;

use krishiv_common::DurabilityProfile;

use crate::{
    LocalDiskShuffleStore, ObjectStoreShuffleStore, ShuffleBackend, ShuffleError, ShuffleResult,
    tiered_store::TieredShuffleStore,
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

/// Build a `Tiered` shuffle backend: local-disk for fast same-host P2P reads,
/// object-store for cross-host durability.
///
/// `local_dir` is the local cache directory (created if absent).
/// `s3_uri` must be an `s3://bucket[/prefix]` URI.
///
/// Only valid for `DistributedDurable` (or `DevLocal` for testing).
pub fn open_tiered_shuffle_backend(
    local_dir: &std::path::Path,
    s3_uri: &str,
) -> ShuffleResult<Arc<ShuffleBackend>> {
    let local = Arc::new(LocalDiskShuffleStore::new(local_dir)?);

    let rest = s3_uri.strip_prefix("s3://").ok_or_else(|| {
        ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("tiered shuffle remote URI must be s3://, got: {s3_uri}"),
        ))
    })?;
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
                "tiered shuffle s3 store {url}: {e}"
            )))
        })?;
    let storage_prefix = if prefix.is_empty() {
        "shuffle".to_owned()
    } else {
        prefix.to_owned()
    };
    let remote = Arc::new(ObjectStoreShuffleStore::new(Arc::new(store), storage_prefix));

    Ok(Arc::new(ShuffleBackend::Tiered(Arc::new(
        TieredShuffleStore::new(local, remote),
    ))))
}
