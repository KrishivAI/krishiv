//! Object-store-backed Iceberg [`Storage`] for shared warehouses (S3 / MinIO).
//!
//! The vendored `iceberg 0.9.1` ships only local-filesystem and in-memory
//! storage backends — there is no S3 implementation in the library. This module
//! bridges Iceberg's [`Storage`] / [`StorageFactory`] traits to
//! [`object_store`] so the engine can read and write Iceberg metadata and data
//! on a shared S3-compatible object store, which is what makes batch / IVM
//! pipeline legs work when the coordinator and platformd run in different pods.
//!
//! A single [`KrishivStorage`] instance handles *every* path: each call
//! dispatches on the URI scheme — `s3://` / `s3a://` go to the object store, and
//! anything else (`file://`, `file:/`, bare paths) is delegated to Iceberg's own
//! [`LocalFsStorage`]. A warehouse may therefore even mix schemes.
//!
//! S3 configuration is read from the ambient AWS environment **at store-build
//! time** and is deliberately never stored on the struct, so credentials are
//! not serialized into distributed FileIO specs:
//!   * `AWS_ENDPOINT_URL` — S3 endpoint; its presence marks a MinIO-style store
//!     (path-style access + plain HTTP allowed).
//!   * `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — credentials.
//!   * `AWS_REGION` / `AWS_DEFAULT_REGION` — region (default `us-east-1`; MinIO
//!     ignores it but the AWS signer requires a non-empty value).

#![cfg(any(
    feature = "iceberg",
    feature = "local-catalog",
    feature = "postgres-catalog",
    feature = "rest-catalog",
    feature = "iceberg-datafusion"
))]

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
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

/// Map an `object_store` error into an Iceberg error.
fn os_err(e: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("object store: {e}"))
}

/// `true` for the object-store schemes this bridge handles directly.
fn is_object_store_uri(path: &str) -> bool {
    path.starts_with("s3://") || path.starts_with("s3a://")
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

        let store = crate::build_s3_object_store(bucket).map_err(os_err)?;
        stores.insert(bucket.to_string(), store.clone());
        Ok(store)
    }

    /// Resolve a path to its object store and key.
    fn resolve(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let (bucket, key) = parse_s3_uri(path)?;
        Ok((self.store_for_bucket(&bucket)?, key))
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

    /// Live round-trip against a MinIO/S3 endpoint. Ignored by default; run with:
    /// ```bash
    /// AWS_ENDPOINT_URL=http://localhost:9100 AWS_ACCESS_KEY_ID=minio \
    ///   AWS_SECRET_ACCESS_KEY=minio12345 KRISHIV_TEST_S3_BUCKET=warehouse \
    ///   cargo test -p krishiv-sql --features rest-catalog -- --ignored s3_round_trip
    /// ```
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
