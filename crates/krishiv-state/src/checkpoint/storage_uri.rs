//! Open checkpoint storage from production URIs (`file://`, `s3://`, `memory://`).

use std::sync::Arc;

use object_store::ObjectStore;

use crate::checkpoint::ObjectStoreCheckpointStorage;
use crate::checkpoint::{
    CheckpointError, CheckpointResult, CheckpointStorage, LocalFsCheckpointStorage,
};

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
        if !krishiv_common::allows_memory_checkpoint_uri(
            krishiv_common::resolve_durability_profile(),
        ) {
            return Err(CheckpointError::Storage {
                message: format!(
                    "memory:// checkpoint URIs are forbidden for durability profile '{}'",
                    krishiv_common::resolve_durability_profile()
                ),
            });
        }
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
        // Construct the S3 client byte-for-byte like the proven streaming-sink
        // builder (krishiv_connectors::lakehouse::object_store_io::build_s3_object_store):
        // with_bucket_name + explicit endpoint/creds/region, path-style over HTTP.
        // AmazonS3Builder::from_env honours only AWS_ENDPOINT, not the AWS-SDK
        // AWS_ENDPOINT_URL convention prod sets for MinIO — without the override the
        // store silently targets real AWS and every write times out. (Using
        // `with_url` instead of `with_bucket_name` was observed to intermittently
        // fail MinIO writes with "error sending request" where the identically
        // configured sink client succeeded from the same pod.)
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
        // Evict pooled HTTP connections after 1s idle. Behind kube-proxy/MinIO an
        // idle keep-alive socket is silently dropped within seconds; hyper's
        // default 90s pool reuses the dead socket and the write hangs the full
        // 30s request timeout — which exceeds the barrier deadline so the
        // checkpoint never commits. Barrier checkpoints are exactly such spaced
        // writers, so force a fresh connection per cycle.
        builder = builder.with_client_options(
            object_store::ClientOptions::new()
                .with_pool_idle_timeout(std::time::Duration::from_secs(1)),
        );
        let store = builder.build().map_err(|e| CheckpointError::Storage {
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
