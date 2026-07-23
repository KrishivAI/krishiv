//! Object-store-backed Iceberg [`Storage`] for shared warehouses (S3 / MinIO).
//!
//! The vendored `iceberg 0.9.1` ships only local-filesystem and in-memory
//! storage backends — there is no S3 implementation in the library. This module
//! bridges Iceberg's [`Storage`] / [`StorageFactory`] traits to
//! [`object_store`] so the engine can read and write Iceberg metadata and data
//! on a shared S3-compatible object store. That is what makes a **cross-pod**
//! path work: the batch/IVM legs (coordinator vs platformd in different pods)
//! *and* the streaming Iceberg sink's DUR-2 recover-commit (a subtask that
//! restores on a different executor than the one that staged the parquet).
//!
//! A single [`KrishivStorage`] instance handles *every* path: each call
//! dispatches on the URI scheme — `s3://` / `s3a://` go to the object store, and
//! anything else (`file://`, `file:/`, bare paths) is delegated to Iceberg's own
//! [`LocalFsStorage`]. A warehouse may therefore even mix schemes.
//!
//! This type is the single, canonical bridge for the whole engine:
//! `krishiv-sql`'s `catalog::object_store_io` re-exports it (rather than
//! defining its own) so there is exactly one `#[typetag::serde]` registration —
//! two identically-tagged `Storage` impls linked into one binary would collide
//! at typetag registration time.
//!
//! S3 configuration is read from the ambient AWS environment **at store-build
//! time** and is deliberately never stored on the struct, so credentials are
//! not serialized into distributed FileIO specs:
//!   * `AWS_ENDPOINT_URL` — S3 endpoint; its presence marks a MinIO-style store
//!     (path-style access + plain HTTP allowed).
//!   * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — credentials.
//!   * `AWS_REGION` / `AWS_DEFAULT_REGION` — region (default `us-east-1`; MinIO
//!     ignores it but the AWS signer requires a non-empty value).
//!
//! The S3 leg requires the `cloud` feature (which enables `object_store/aws`);
//! without it, `s3://` paths return a clear error and only local paths work.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
// `head`/`get`/`put`/`delete` are convenience methods on `ObjectStoreExt` in
// object_store 0.12; `get_range`/`list` are on the base trait.
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

/// Map an `object_store` error into an Iceberg error.
fn os_err(e: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("object store: {e}"))
}

/// `true` for the object-store schemes this bridge handles directly.
fn is_object_store_uri(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://") || path.starts_with("memory://")
}

/// Process-global in-memory object store per bucket, backing the `memory://`
/// scheme. Deterministic and dependency-free — used for tests and ephemeral
/// in-process warehouses. Like S3 (and unlike a per-instance store), it is
/// shared across `KrishivStorage` instances, so a "restarted" sink sees the
/// same objects a prior instance wrote — which is what lets the DUR-2
/// cross-instance recover-commit path be exercised without a live endpoint.
fn memory_store_for_bucket(bucket: &str) -> Arc<dyn ObjectStore> {
    static MEMORY_STORES: std::sync::OnceLock<Mutex<HashMap<String, Arc<InMemory>>>> =
        std::sync::OnceLock::new();
    let map = MEMORY_STORES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard
        .entry(bucket.to_string())
        .or_insert_with(|| Arc::new(InMemory::new()))
        .clone()
}

/// Split an `s3://bucket/key` (or `s3a://…`) URI into `(bucket, key)`.
fn parse_s3_uri(path: &str) -> Result<(String, ObjectPath)> {
    let rest = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, format!("not an s3 uri: {path}")))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, format!("s3 uri has no key: {path}")))?;
    Ok((bucket.to_string(), ObjectPath::from(key)))
}

/// Build an S3/MinIO object store for `bucket` from the ambient AWS environment.
///
/// Reading `AWS_ENDPOINT_URL` here — the AWS-SDK convention prod sets — is what
/// makes MinIO reachable; `AmazonS3Builder::from_env` alone honours only
/// `AWS_ENDPOINT` and would silently target real AWS.
#[cfg(feature = "cloud")]
pub(crate) fn build_s3_object_store(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    // Evict pooled HTTP connections after 1s idle. Behind k8s kube-proxy /
    // MinIO, an idle keep-alive connection is silently dropped server-side
    // within a few seconds; hyper's default 90s pool then reuses the dead
    // socket and the request hangs the full 30s request timeout. Spaced
    // writers (barrier checkpoints every few seconds) hit this every cycle —
    // exceeding the coordinator's barrier deadline so checkpoints never
    // commit. A sub-idle pool timeout forces a fresh connection per spaced
    // request while still reusing within a burst. NOTE: allow_http must live on
    // this ClientOptions — with_client_options REPLACES the builder's options,
    // so a separate builder.with_allow_http(true) would be silently discarded.
    let mut client_opts = object_store::ClientOptions::new()
        .with_pool_idle_timeout(std::time::Duration::from_secs(1));
    let mut has_endpoint = false;
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL")
        && !endpoint.is_empty()
    {
        // MinIO / S3-compatible: path-style access over plain HTTP.
        builder = builder.with_endpoint(endpoint);
        client_opts = client_opts.with_allow_http(true);
        has_endpoint = true;
    }
    builder = builder.with_client_options(client_opts);
    let has_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map(|k| !k.is_empty())
        .unwrap_or(false);
    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID")
        && !key.is_empty()
    {
        builder = builder.with_access_key_id(key);
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY")
        && !secret.is_empty()
    {
        builder = builder.with_secret_access_key(secret);
    }
    // Custom endpoint + no credentials => anonymous MinIO/S3-compatible access,
    // not EC2. Skip signing so reads don't block ~180s on the IMDS credential
    // endpoint (unreachable off-EC2). Real AWS (no endpoint) keeps the default
    // credential chain intact.
    if has_endpoint && !has_key {
        builder = builder.with_skip_signature(true);
    }
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    builder = builder.with_region(region);
    Ok(Arc::new(builder.build().map_err(os_err)?))
}

/// Without the `cloud` feature there is no `object_store` S3 backend compiled
/// in, so `s3://` paths cannot be served — return a clear, actionable error.
#[cfg(not(feature = "cloud"))]
pub(crate) fn build_s3_object_store(_bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    Err(Error::new(
        ErrorKind::FeatureUnsupported,
        "s3:// object storage requires the connectors `cloud` feature \
         (enables object_store/aws); this build has only local-fs Iceberg storage",
    ))
}

/// Iceberg [`Storage`] that serves object-store schemes via [`object_store`] and
/// delegates local-filesystem paths to [`LocalFsStorage`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KrishivStorage {
    /// Delegate for `file://` and bare paths.
    local: LocalFsStorage,
    /// Per-bucket object-store cache. Skipped during serialization so no
    /// credentials or live clients travel in a serialized FileIO; it is rebuilt
    /// lazily from the environment on first use after deserialization.
    #[serde(skip)]
    stores: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
}

impl KrishivStorage {
    /// Get or build the object store for `bucket`, configured from the ambient
    /// AWS environment (MinIO endpoint / credentials / region).
    fn store_for_bucket(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut stores = self
            .stores
            .lock()
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("store cache poisoned: {e}")))?;
        if let Some(store) = stores.get(bucket) {
            return Ok(store.clone());
        }

        let store = build_s3_object_store(bucket)?;
        stores.insert(bucket.to_string(), store.clone());
        Ok(store)
    }

    /// Resolve a path to its object store and key, dispatching on scheme.
    fn resolve(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        if let Some(rest) = path.strip_prefix("memory://") {
            let (bucket, key) = rest.split_once('/').ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("memory uri has no key: {path}"),
                )
            })?;
            Ok((memory_store_for_bucket(bucket), ObjectPath::from(key)))
        } else {
            let (bucket, key) = parse_s3_uri(path)?;
            Ok((self.store_for_bucket(&bucket)?, key))
        }
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for KrishivStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            match store.head(&key).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(os_err(e)),
            }
        } else {
            self.local.exists(path).await
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            let meta = store.head(&key).await.map_err(os_err)?;
            Ok(FileMetadata { size: meta.size })
        } else {
            self.local.metadata(path).await
        }
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            let result = store.get(&key).await.map_err(os_err)?;
            result.bytes().await.map_err(os_err)
        } else {
            self.local.read(path).await
        }
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            Ok(Box::new(ObjectStoreFileRead { store, key }))
        } else {
            self.local.reader(path).await
        }
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            store
                .put(&key, PutPayload::from_bytes(bs))
                .await
                .map_err(os_err)?;
            Ok(())
        } else {
            self.local.write(path, bs).await
        }
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            Ok(Box::new(ObjectStoreFileWrite {
                store,
                key,
                buffer: Vec::new(),
                closed: false,
            }))
        } else {
            self.local.writer(path).await
        }
    }

    async fn delete(&self, path: &str) -> Result<()> {
        if is_object_store_uri(path) {
            let (store, key) = self.resolve(path)?;
            match store.delete(&key).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(os_err(e)),
            }
        } else {
            self.local.delete(path).await
        }
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        if is_object_store_uri(path) {
            let (store, prefix) = self.resolve(path)?;
            let mut listing = store.list(Some(&prefix));
            let mut locations = Vec::new();
            while let Some(meta) = listing.next().await {
                locations.push(meta.map_err(os_err)?.location);
            }
            for location in locations {
                store.delete(&location).await.map_err(os_err)?;
            }
            Ok(())
        } else {
            self.local.delete_prefix(path).await
        }
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Ranged reader over an object-store key.
#[derive(Debug)]
struct ObjectStoreFileRead {
    store: Arc<dyn ObjectStore>,
    key: ObjectPath,
}

#[async_trait]
impl FileRead for ObjectStoreFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store.get_range(&self.key, range).await.map_err(os_err)
    }
}

/// Buffered writer over an object-store key. Iceberg writes each file once and
/// calls [`FileWrite::close`] to finalize it, so buffering the whole payload and
/// issuing a single `put` on close is correct; a multipart upload for very large
/// data files is a future optimization.
#[derive(Debug)]
struct ObjectStoreFileWrite {
    store: Arc<dyn ObjectStore>,
    key: ObjectPath,
    buffer: Vec<u8>,
    closed: bool,
}

#[async_trait]
impl FileWrite for ObjectStoreFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "cannot write to a closed file",
            ));
        }
        self.buffer.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::DataInvalid, "file already closed"));
        }
        self.closed = true;
        let payload = PutPayload::from(std::mem::take(&mut self.buffer));
        self.store.put(&self.key, payload).await.map_err(os_err)?;
        Ok(())
    }
}

/// [`StorageFactory`] that builds a scheme-dispatching [`KrishivStorage`].
///
/// Use this in place of `LocalFsStorageFactory` so a catalog can serve both
/// `file://` and `s3://` warehouses.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KrishivStorageFactory;

#[typetag::serde]
impl StorageFactory for KrishivStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(KrishivStorage::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        let (bucket, key) = parse_s3_uri("s3://warehouse/ns/table/metadata/x.json").unwrap();
        assert_eq!(bucket, "warehouse");
        assert_eq!(key.as_ref(), "ns/table/metadata/x.json");
    }

    #[test]
    fn parse_s3_uri_accepts_s3a() {
        let (bucket, key) = parse_s3_uri("s3a://bkt/a/b").unwrap();
        assert_eq!(bucket, "bkt");
        assert_eq!(key.as_ref(), "a/b");
    }

    #[test]
    fn parse_s3_uri_rejects_non_s3() {
        assert!(parse_s3_uri("file:///tmp/x").is_err());
    }

    #[test]
    fn is_object_store_uri_discriminates() {
        assert!(is_object_store_uri("s3://b/k"));
        assert!(is_object_store_uri("s3a://b/k"));
        assert!(!is_object_store_uri("file:///tmp/x"));
        assert!(!is_object_store_uri("/tmp/x"));
    }

    #[test]
    fn factory_builds_dispatching_storage() {
        let storage = KrishivStorageFactory.build(&StorageConfig::new()).unwrap();
        assert!(format!("{storage:?}").contains("KrishivStorage"));
    }

    /// Without the `cloud` feature, `s3://` paths must fail with a clear feature
    /// error rather than a confusing miss — the local delegate never sees them.
    #[cfg(not(feature = "cloud"))]
    #[tokio::test]
    async fn s3_path_without_cloud_errors_clearly() {
        let storage = KrishivStorage::default();
        let err = storage.exists("s3://bucket/key").await.unwrap_err();
        assert!(
            format!("{err}").contains("cloud"),
            "expected a cloud-feature error, got: {err}"
        );
    }

    /// Live round-trip against a MinIO/S3 endpoint. Ignored by default; run with:
    /// ```bash
    /// AWS_ENDPOINT_URL=http://localhost:9100 AWS_ACCESS_KEY_ID=minio \
    ///   AWS_SECRET_ACCESS_KEY=minio12345 KRISHIV_TEST_S3_BUCKET=warehouse \
    ///   cargo test -p krishiv-connectors --features iceberg,cloud -- --ignored s3_round_trip
    /// ```
    #[cfg(feature = "cloud")]
    #[tokio::test]
    #[ignore = "requires a live S3/MinIO endpoint (KRISHIV_TEST_S3_BUCKET)"]
    async fn s3_round_trip_write_read_list_delete() {
        let bucket = std::env::var("KRISHIV_TEST_S3_BUCKET").expect("KRISHIV_TEST_S3_BUCKET");
        let storage = KrishivStorage::default();
        let base = format!("s3://{bucket}/it/object_store_io");
        let file = format!("{base}/hello.txt");
        let payload = Bytes::from_static(b"hello minio");

        // write + exists + metadata + read
        storage.write(&file, payload.clone()).await.unwrap();
        assert!(storage.exists(&file).await.unwrap());
        assert_eq!(
            storage.metadata(&file).await.unwrap().size,
            payload.len() as u64
        );
        assert_eq!(storage.read(&file).await.unwrap(), payload);

        // ranged reader
        let reader = storage.reader(&file).await.unwrap();
        assert_eq!(
            reader.read(0..5).await.unwrap(),
            Bytes::from_static(b"hello")
        );

        // streaming writer
        let file2 = format!("{base}/streamed.txt");
        let mut w = storage.writer(&file2).await.unwrap();
        w.write(Bytes::from_static(b"ab")).await.unwrap();
        w.write(Bytes::from_static(b"cd")).await.unwrap();
        w.close().await.unwrap();
        assert_eq!(
            storage.read(&file2).await.unwrap(),
            Bytes::from_static(b"abcd")
        );

        // delete_prefix removes both, exists() then false
        storage.delete_prefix(&base).await.unwrap();
        assert!(!storage.exists(&file).await.unwrap());
        assert!(!storage.exists(&file2).await.unwrap());
    }
}
