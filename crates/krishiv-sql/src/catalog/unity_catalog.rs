#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Unity Catalog backend (feature = "unity-catalog").
//!
//! Connects Krishiv to a [Unity Catalog](https://www.unitycatalog.io/) server
//! by wrapping its built-in Iceberg REST endpoint.  Unity Catalog exposes a
//! standard Iceberg REST catalog at:
//!
//! ```text
//! <host>/api/2.1/unity-catalog/iceberg/
//! ```
//!
//! This module layers UC-specific auth (Personal Access Token or OAuth2) and
//! passes the configured URL + token to [`KrishivRestCatalog`], which already
//! handles the full Iceberg REST protocol (list namespaces, load tables, commit
//! snapshots, etc.).
//!
//! # Configuration
//!
//! | Env var                      | Description                                          |
//! |------------------------------|------------------------------------------------------|
//! | `KRISHIV_UNITY_HOST`         | Base URL of the Unity Catalog server (required)      |
//! | `KRISHIV_UNITY_TOKEN`        | Personal Access Token (PAT) or OAuth2 bearer token   |
//! | `KRISHIV_UNITY_CATALOG_NAME` | UC catalog name mapped to the Iceberg warehouse      |
//!
//! # Example
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use krishiv_sql::catalog::unity_catalog::UnityCatalog;
//! let catalog = UnityCatalog::from_env().await.unwrap();
//! # });
//! ```

#![cfg(feature = "unity-catalog")]

use crate::catalog::CatalogError;
use crate::catalog::rest_catalog_wrapper::KrishivRestCatalog;

/// Unity Catalog client backed by UC's Iceberg REST endpoint.
///
/// `UnityCatalog` is a thin newtype around [`KrishivRestCatalog`] that knows
/// how to derive the correct Iceberg REST URL and auth headers from UC
/// configuration.
#[derive(Debug, Clone)]
pub struct UnityCatalog {
    inner: KrishivRestCatalog,
    /// The UC catalog name that was used to build this client.
    catalog_name: String,
}

impl UnityCatalog {
    /// Connect to a Unity Catalog server.
    ///
    /// # Arguments
    ///
    /// - `host`: Base URL of the UC server (e.g. `https://adb-xyz.azuredatabricks.net`).
    ///   Must not include a trailing slash.
    /// - `catalog_name`: The Unity Catalog catalog name (e.g. `"main"`).
    ///   Used as the Iceberg warehouse identifier.
    /// - `token`: Optional bearer token (PAT or OAuth2 access token).  When
    ///   `None`, the client sends unauthenticated requests — appropriate only
    ///   for OSS Unity Catalog running without auth.
    pub async fn new(
        host: &str,
        catalog_name: &str,
        token: Option<&str>,
    ) -> Result<Self, CatalogError> {
        let iceberg_uri = format!(
            "{}/api/2.1/unity-catalog/iceberg/",
            host.trim_end_matches('/')
        );
        let inner = KrishivRestCatalog::new(&iceberg_uri, catalog_name, token).await?;
        Ok(Self {
            inner,
            catalog_name: catalog_name.to_owned(),
        })
    }

    /// Build a `UnityCatalog` from environment variables.
    ///
    /// Required: `KRISHIV_UNITY_HOST`
    /// Optional: `KRISHIV_UNITY_TOKEN`, `KRISHIV_UNITY_CATALOG_NAME` (default `"main"`)
    pub async fn from_env() -> Result<Self, CatalogError> {
        let host = std::env::var("KRISHIV_UNITY_HOST").map_err(|_| {
            CatalogError::InvalidConfiguration {
                message: "KRISHIV_UNITY_HOST is required for the Unity Catalog backend".into(),
            }
        })?;
        let catalog_name =
            std::env::var("KRISHIV_UNITY_CATALOG_NAME").unwrap_or_else(|_| "main".into());
        let token = std::env::var("KRISHIV_UNITY_TOKEN").ok();
        Self::new(&host, &catalog_name, token.as_deref()).await
    }

    /// The UC catalog name this client is connected to.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// The underlying [`KrishivRestCatalog`] (exposes the full `iceberg::Catalog` API).
    pub fn as_rest_catalog(&self) -> &KrishivRestCatalog {
        &self.inner
    }
}

// Delegate iceberg::Catalog to the inner KrishivRestCatalog.
// Unity Catalog's Iceberg REST endpoint is fully spec-compliant, so no
// adaptation is needed beyond the URL construction above.
impl std::ops::Deref for UnityCatalog {
    type Target = KrishivRestCatalog;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_fails_without_host() {
        // Remove the env var to ensure the "missing host" error path is taken.
        // Safety: test-only single-threaded context.
        unsafe {
            std::env::remove_var("KRISHIV_UNITY_HOST");
        }
        // Run the future synchronously with a blocking executor.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(UnityCatalog::from_env());
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            CatalogError::InvalidConfiguration { message } => {
                assert!(message.contains("KRISHIV_UNITY_HOST"), "{message}");
            }
            other => panic!("expected InvalidConfiguration, got: {other}"),
        }
    }

    #[test]
    fn catalog_name_default_is_main() {
        unsafe {
            std::env::set_var("KRISHIV_UNITY_HOST", "https://test.unitycatalog.invalid");
            std::env::remove_var("KRISHIV_UNITY_CATALOG_NAME");
            std::env::remove_var("KRISHIV_UNITY_TOKEN");
        }
        // We only test the env-var parsing logic, not actual network connectivity.
        // The `new()` call creates a KrishivRestCatalog which doesn't connect eagerly.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let catalog = rt
            .block_on(UnityCatalog::from_env())
            .expect("construction must succeed even without network");
        assert_eq!(catalog.catalog_name(), "main");
        unsafe {
            std::env::remove_var("KRISHIV_UNITY_HOST");
        }
    }
}
