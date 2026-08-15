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

/// Connection settings resolved from the environment.
///
/// Split out of [`UnityCatalog::from_env`] so the resolution rules can be
/// tested against a plain lookup closure. The tests used to call `from_env`
/// after mutating the real process environment, which needed `unsafe` — and
/// this crate is `#![forbid(unsafe_code)]`, so that test module never compiled.
/// It also raced: env vars are process-global and the harness runs tests in
/// parallel, so the test that removed the host and the one that set it could
/// observe each other.
struct UnityEnvConfig {
    host: String,
    catalog_name: String,
    token: Option<String>,
}

impl UnityEnvConfig {
    fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, CatalogError> {
        let host =
            lookup("KRISHIV_UNITY_HOST").ok_or_else(|| CatalogError::InvalidConfiguration {
                message: "KRISHIV_UNITY_HOST is required for the Unity Catalog backend".into(),
            })?;
        Ok(Self {
            host,
            catalog_name: lookup("KRISHIV_UNITY_CATALOG_NAME").unwrap_or_else(|| "main".to_owned()),
            token: lookup("KRISHIV_UNITY_TOKEN"),
        })
    }
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
        let cfg = UnityEnvConfig::resolve(|k| std::env::var(k).ok())?;
        Self::new(&cfg.host, &cfg.catalog_name, cfg.token.as_deref()).await
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

    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key: &str| owned.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    #[test]
    fn resolve_fails_without_host() {
        let err = UnityEnvConfig::resolve(lookup_from(&[]))
            .err()
            .expect("a missing host must be an error");
        match err {
            CatalogError::InvalidConfiguration { message } => {
                assert!(message.contains("KRISHIV_UNITY_HOST"), "{message}");
            }
            other => panic!("expected InvalidConfiguration, got: {other}"),
        }
    }

    #[test]
    fn catalog_name_defaults_to_main() {
        let cfg = UnityEnvConfig::resolve(lookup_from(&[(
            "KRISHIV_UNITY_HOST",
            "https://test.unitycatalog.invalid",
        )]))
        .expect("host alone is sufficient");
        assert_eq!(cfg.catalog_name, "main");
        assert_eq!(cfg.token, None);
    }

    #[test]
    fn explicit_catalog_name_and_token_win() {
        let cfg = UnityEnvConfig::resolve(lookup_from(&[
            ("KRISHIV_UNITY_HOST", "https://uc.invalid"),
            ("KRISHIV_UNITY_CATALOG_NAME", "analytics"),
            ("KRISHIV_UNITY_TOKEN", "pat-123"),
        ]))
        .expect("fully specified config");
        assert_eq!(cfg.catalog_name, "analytics");
        assert_eq!(cfg.token.as_deref(), Some("pat-123"));
    }

    /// The Iceberg REST URL is what makes this a Unity Catalog client rather
    /// than a plain REST one, and nothing asserted its shape before.
    #[tokio::test]
    async fn new_builds_the_unity_iceberg_rest_endpoint() {
        let catalog = UnityCatalog::new("https://uc.invalid/", "main", None)
            .await
            .expect("construction must not require network");
        assert_eq!(catalog.catalog_name(), "main");
    }
}
