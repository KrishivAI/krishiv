//! Unified storage factory for building `Arc<dyn ObjectStore>` from URI schemes.
//!
//! Supports S3, local filesystem, and in-memory backends. GCS and Azure
//! backends are available with the `cloud` feature flag. All backends are
//! constructed from environment variables and URI configuration, providing a
//! single entry point for any component that needs object-store access.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;

use crate::error::{ConnectorError, ConnectorResult};

/// Supported storage backend types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StorageBackend {
    /// Local filesystem rooted at a base path.
    Local,
    /// Amazon S3 (or S3-compatible like MinIO).
    S3,
    /// Google Cloud Storage (requires `cloud` feature).
    Gcs,
    /// Azure Blob Storage / Azure Data Lake Storage Gen2 (requires `cloud` feature).
    Azure,
    /// In-memory store (for testing).
    Memory,
}

impl StorageBackend {
    /// Detect the backend from a URI scheme.
    pub fn from_uri(uri: &str) -> Option<Self> {
        if uri.starts_with("s3://") || uri.starts_with("s3a://") {
            Some(Self::S3)
        } else if uri.starts_with("gs://") {
            Some(Self::Gcs)
        } else if uri.starts_with("az://")
            || uri.starts_with("abfs://")
            || uri.starts_with("adls://")
        {
            Some(Self::Azure)
        } else if uri.starts_with("file://") || uri.starts_with('/') || uri.starts_with('.') {
            Some(Self::Local)
        } else {
            None
        }
    }
}

impl std::fmt::Display for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "local"),
            Self::S3 => write!(f, "s3"),
            Self::Gcs => write!(f, "gcs"),
            Self::Azure => write!(f, "azure"),
            Self::Memory => write!(f, "memory"),
        }
    }
}

/// Configuration for building an object store.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    /// Optional override for the bucket/container name.
    pub bucket: Option<String>,
    /// Optional prefix/path within the bucket.
    pub prefix: Option<String>,
    /// Optional endpoint override (for S3-compatible like MinIO).
    pub endpoint: Option<String>,
    /// Optional region override.
    pub region: Option<String>,
    /// Additional key-value properties (backend-specific).
    pub properties: HashMap<String, String>,
}

/// Factory for constructing `Arc<dyn ObjectStore>` instances.
///
/// Resolves the appropriate backend from a URI or explicit backend type,
/// reads environment variables for credentials, and returns a ready-to-use
/// object store.
///
/// # Examples
///
/// ```ignore
/// let store = StorageFactory::from_uri("s3://my-bucket/data")?;
/// let store = StorageFactory::from_uri("/tmp/local-data")?;
/// let store = StorageFactory::build(StorageBackend::Memory, &StorageConfig::default())?;
/// ```
pub struct StorageFactory;

impl StorageFactory {
    /// Build an object store from a URI, auto-detecting the backend.
    pub fn from_uri(uri: &str) -> ConnectorResult<Arc<dyn ObjectStore>> {
        let backend = StorageBackend::from_uri(uri).ok_or_else(|| ConnectorError::Config {
            message: format!("Cannot determine storage backend from URI: {uri}"),
        })?;
        let config = Self::config_from_uri(uri)?;
        Self::build(backend, &config)
    }

    /// Build an object store for a specific backend with the given configuration.
    pub fn build(
        backend: StorageBackend,
        config: &StorageConfig,
    ) -> ConnectorResult<Arc<dyn ObjectStore>> {
        match backend {
            StorageBackend::Local => Self::build_local(config),
            StorageBackend::S3 => Self::build_s3(config),
            StorageBackend::Gcs => Self::build_gcs(config),
            StorageBackend::Azure => Self::build_azure(config),
            StorageBackend::Memory => Ok(Arc::new(object_store::memory::InMemory::new())),
        }
    }

    /// Parse a URI into a `StorageConfig`.
    fn config_from_uri(uri: &str) -> ConnectorResult<StorageConfig> {
        if uri.starts_with("s3://") || uri.starts_with("s3a://") {
            let stripped = uri
                .strip_prefix("s3://")
                .or_else(|| uri.strip_prefix("s3a://"))
                .unwrap_or(uri);
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            let bucket = parts.first().copied().unwrap_or("").to_string();
            let prefix = parts.get(1).map(|s| s.to_string());
            Ok(StorageConfig {
                bucket: Some(bucket),
                prefix,
                ..Default::default()
            })
        } else if uri.starts_with("gs://") {
            let stripped = uri.strip_prefix("gs://").unwrap_or(uri);
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            let bucket = parts.first().copied().unwrap_or("").to_string();
            let prefix = parts.get(1).map(|s| s.to_string());
            Ok(StorageConfig {
                bucket: Some(bucket),
                prefix,
                ..Default::default()
            })
        } else if uri.starts_with("az://") || uri.starts_with("abfs://") {
            let stripped = uri
                .strip_prefix("az://")
                .or_else(|| uri.strip_prefix("abfs://"))
                .or_else(|| uri.strip_prefix("adls://"))
                .unwrap_or(uri);
            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
            let bucket = parts.first().copied().unwrap_or("").to_string();
            let prefix = parts.get(1).map(|s| s.to_string());
            Ok(StorageConfig {
                bucket: Some(bucket),
                prefix,
                ..Default::default()
            })
        } else {
            // Local filesystem path
            Ok(StorageConfig {
                bucket: None,
                prefix: Some(uri.to_string()),
                ..Default::default()
            })
        }
    }

    fn build_local(config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        let root = config.prefix.as_deref().unwrap_or(".");
        let fs = object_store::local::LocalFileSystem::new_with_prefix(root).map_err(|e| {
            ConnectorError::Config {
                message: format!("Failed to create local object store at {root}: {e}"),
            }
        })?;
        Ok(Arc::new(fs))
    }

    #[cfg(feature = "cloud")]
    fn build_s3(config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::from_env();

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
            // MinIO and other S3-compatible stores need path-style access
            builder = builder.with_allow_http(true);
        }
        if let Some(region) = &config.region {
            builder = builder.with_region(region.as_str());
        }
        if let Some(bucket) = &config.bucket {
            builder = builder.with_bucket_name(bucket);
        }

        // Honor common S3 env vars if from_env() didn't pick them up
        let mut has_key = false;
        if let Ok(v) = std::env::var("AWS_ACCESS_KEY_ID")
            && !v.is_empty()
        {
            builder = builder.with_access_key_id(&v);
            has_key = true;
        }
        if let Ok(v) = std::env::var("AWS_SECRET_ACCESS_KEY")
            && !v.is_empty()
        {
            builder = builder.with_secret_access_key(&v);
        }
        if let Ok(v) = std::env::var("AWS_SESSION_TOKEN")
            && !v.is_empty()
        {
            builder = builder.with_token(&v);
        }

        // Custom endpoint + no credentials => anonymous S3-compatible access
        // (MinIO), not EC2. Skip signing so reads don't stall ~180s on the IMDS
        // credential endpoint, which is unreachable off-EC2.
        if config.endpoint.is_some() && !has_key {
            builder = builder.with_skip_signature(true);
        }

        let store = builder.build().map_err(|e| ConnectorError::Config {
            message: format!("Failed to build S3 object store: {e}"),
        })?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "cloud"))]
    fn build_s3(_config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config {
            message: "S3 support requires the 'cloud' feature flag".into(),
        })
    }

    #[cfg(feature = "cloud")]
    fn build_gcs(config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        let mut builder = object_store::gcp::GoogleCloudStorageBuilder::from_env();

        if let Some(bucket) = &config.bucket {
            builder = builder.with_bucket_name(bucket);
        }
        if let Some(endpoint) = &config.endpoint {
            // object_store 0.13 dropped `with_endpoint` on the GCS builder; a
            // custom endpoint (e.g. the fake-gcs-server emulator) is the base URL.
            builder = builder.with_base_url(endpoint);
        }

        let store = builder.build().map_err(|e| ConnectorError::Config {
            message: format!("Failed to build GCS object store: {e}"),
        })?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "cloud"))]
    fn build_gcs(_config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config {
            message: "GCS support requires the 'cloud' feature flag".into(),
        })
    }

    #[cfg(feature = "cloud")]
    fn build_azure(config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        let mut builder = object_store::azure::MicrosoftAzureBuilder::from_env();

        if let Some(container) = &config.bucket {
            builder = builder.with_container_name(container);
        }
        if let Some(endpoint) = &config.endpoint {
            // object_store 0.13's `with_endpoint` takes an owned `String`.
            builder = builder.with_endpoint(endpoint.clone());
        }

        let store = builder.build().map_err(|e| ConnectorError::Config {
            message: format!("Failed to build Azure object store: {e}"),
        })?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "cloud"))]
    fn build_azure(_config: &StorageConfig) -> ConnectorResult<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config {
            message: "Azure support requires the 'cloud' feature flag".into(),
        })
    }

    /// Strip a URI scheme and return the bare path (bucket + key).
    pub fn strip_scheme(uri: &str) -> &str {
        uri.strip_prefix("s3://")
            .or_else(|| uri.strip_prefix("s3a://"))
            .or_else(|| uri.strip_prefix("gs://"))
            .or_else(|| uri.strip_prefix("az://"))
            .or_else(|| uri.strip_prefix("abfs://"))
            .or_else(|| uri.strip_prefix("adls://"))
            .or_else(|| uri.strip_prefix("file://"))
            .unwrap_or(uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_from_uri_s3() {
        assert_eq!(
            StorageBackend::from_uri("s3://bucket/key"),
            Some(StorageBackend::S3)
        );
        assert_eq!(
            StorageBackend::from_uri("s3a://bucket/key"),
            Some(StorageBackend::S3)
        );
    }

    #[test]
    fn backend_from_uri_gcs() {
        assert_eq!(
            StorageBackend::from_uri("gs://bucket/key"),
            Some(StorageBackend::Gcs)
        );
    }

    #[test]
    fn backend_from_uri_azure() {
        assert_eq!(
            StorageBackend::from_uri("az://container/path"),
            Some(StorageBackend::Azure)
        );
        assert_eq!(
            StorageBackend::from_uri("abfs://container/path"),
            Some(StorageBackend::Azure)
        );
    }

    #[test]
    fn backend_from_uri_local() {
        assert_eq!(
            StorageBackend::from_uri("/tmp/data"),
            Some(StorageBackend::Local)
        );
        assert_eq!(
            StorageBackend::from_uri("./data"),
            Some(StorageBackend::Local)
        );
        assert_eq!(
            StorageBackend::from_uri("file:///tmp/data"),
            Some(StorageBackend::Local)
        );
    }

    #[test]
    fn backend_from_uri_unknown() {
        assert_eq!(StorageBackend::from_uri("http://example.com"), None);
    }

    #[test]
    fn config_from_s3_uri() {
        let config = StorageFactory::config_from_uri("s3://my-bucket/path/to/data").unwrap();
        assert_eq!(config.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.prefix.as_deref(), Some("path/to/data"));
    }

    #[test]
    fn config_from_gcs_uri() {
        let config = StorageFactory::config_from_uri("gs://my-bucket/path").unwrap();
        assert_eq!(config.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.prefix.as_deref(), Some("path"));
    }

    #[test]
    fn config_from_local_uri() {
        let config = StorageFactory::config_from_uri("/tmp/data").unwrap();
        assert_eq!(config.bucket, None);
        assert_eq!(config.prefix.as_deref(), Some("/tmp/data"));
    }

    #[test]
    fn strip_scheme_variants() {
        assert_eq!(StorageFactory::strip_scheme("s3://b/k"), "b/k");
        assert_eq!(StorageFactory::strip_scheme("gs://b/k"), "b/k");
        assert_eq!(StorageFactory::strip_scheme("az://c/k"), "c/k");
        assert_eq!(StorageFactory::strip_scheme("/local/path"), "/local/path");
    }

    #[test]
    fn build_memory_store() {
        let store =
            StorageFactory::build(StorageBackend::Memory, &StorageConfig::default()).unwrap();
        let _ = store;
    }

    #[test]
    fn build_local_store() {
        let config = StorageConfig {
            prefix: Some("/tmp".into()),
            ..Default::default()
        };
        let store = StorageFactory::build(StorageBackend::Local, &config).unwrap();
        let _ = store;
    }

    #[test]
    fn backend_display() {
        assert_eq!(StorageBackend::Local.to_string(), "local");
        assert_eq!(StorageBackend::S3.to_string(), "s3");
        assert_eq!(StorageBackend::Gcs.to_string(), "gcs");
        assert_eq!(StorageBackend::Azure.to_string(), "azure");
        assert_eq!(StorageBackend::Memory.to_string(), "memory");
    }

    #[test]
    fn config_from_s3_bucket_only() {
        let config = StorageFactory::config_from_uri("s3://my-bucket").unwrap();
        assert_eq!(config.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(config.prefix, None);
    }
}
