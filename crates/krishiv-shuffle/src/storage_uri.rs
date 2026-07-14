//! Open shuffle storage from production URIs (`file://`, `s3://`).

use std::sync::Arc;

use krishiv_common::DurabilityProfile;

use crate::{
    LocalDiskShuffleStore, ObjectStoreShuffleStore, ShuffleBackend, ShuffleError, ShuffleResult,
    tiered_store::TieredShuffleStore,
};

/// Build an S3 object store the same way the streaming sink does
/// (`krishiv_connectors::lakehouse::object_store_io::build_s3_object_store`):
/// `with_bucket_name` + explicit endpoint/creds/region, path-style over plain
/// HTTP. `AmazonS3Builder::from_env` honours only `AWS_ENDPOINT`, not the
/// AWS-SDK `AWS_ENDPOINT_URL` convention prod sets for MinIO; and `with_url`
/// was observed to intermittently fail MinIO writes ("error sending request")
/// where this builder succeeded from the same pod.
fn build_s3_store(bucket: &str) -> Result<object_store::aws::AmazonS3, object_store::Error> {
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL")
        && !endpoint.is_empty()
    {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }
    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(key);
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(secret);
    }
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    builder = builder.with_region(region);
    // Evict pooled HTTP connections after 1s idle so spaced writers don't reuse
    // a socket kube-proxy/MinIO silently closed (which hangs the 30s request
    // timeout). See the note in krishiv-state's checkpoint storage_uri.
    builder = builder.with_client_options(
        object_store::ClientOptions::new().with_pool_idle_timeout(std::time::Duration::from_secs(1)),
    );
    builder.build()
}

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
        // Construct the S3 client like the proven streaming-sink builder:
        // with_bucket_name + explicit endpoint/creds/region, path-style over HTTP.
        // AmazonS3Builder::from_env honours only AWS_ENDPOINT, not AWS_ENDPOINT_URL;
        // and `with_url` was observed to intermittently fail MinIO writes where the
        // identically configured sink client (with_bucket_name) succeeded.
        let store = build_s3_store(bucket)
            .map_err(|e| ShuffleError::Io(std::io::Error::other(format!("s3 shuffle store {url}: {e}"))))?;
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
    // See the note in `open_shuffle_backend`: build the S3 client like the sink
    // (with_bucket_name + endpoint/creds/region), path-style over HTTP.
    let store = build_s3_store(bucket).map_err(|e| {
        ShuffleError::Io(std::io::Error::other(format!(
            "tiered shuffle s3 store {url}: {e}"
        )))
    })?;
    let storage_prefix = if prefix.is_empty() {
        "shuffle".to_owned()
    } else {
        prefix.to_owned()
    };
    let remote = Arc::new(ObjectStoreShuffleStore::new(
        Arc::new(store),
        storage_prefix,
    ));

    Ok(Arc::new(ShuffleBackend::Tiered(Arc::new(
        TieredShuffleStore::new(local, remote),
    ))))
}
