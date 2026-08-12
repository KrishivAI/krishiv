#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! AWS Glue Data Catalog backend (feature = "glue-catalog").
//!
//! Connects Krishiv to the [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/)
//! by wrapping its Iceberg REST-compatible endpoint.  AWS Glue exposes an
//! Iceberg REST catalog at:
//!
//! ```text
//! https://glue.<region>.amazonaws.com/iceberg/
//! ```
//!
//! Authentication is performed via AWS SigV4 — the `reqwest`-based Iceberg
//! REST client signs requests automatically when standard AWS credential
//! environment variables are set:
//!
//! - `AWS_ACCESS_KEY_ID`
//! - `AWS_SECRET_ACCESS_KEY`
//! - `AWS_SESSION_TOKEN` (optional, for temporary credentials)
//! - `AWS_REGION` / `AWS_DEFAULT_REGION`
//!
//! Alternatively, the IAM role attached to the running instance / task is
//! picked up automatically when no explicit credentials are present.
//!
//! # Configuration
//!
//! | Env var                    | Description                                                 |
//! |----------------------------|-------------------------------------------------------------|
//! | `AWS_REGION`               | AWS region (required, e.g. `us-east-1`)                     |
//! | `KRISHIV_GLUE_CATALOG_ID`  | AWS account ID of the Glue catalog (defaults to caller's)   |
//! | `KRISHIV_GLUE_DATABASE`    | Default Glue database name used as the Iceberg warehouse     |
//!
//! # Example
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use krishiv_sql::catalog::glue_catalog::GlueCatalog;
//! let catalog = GlueCatalog::from_env().await.unwrap();
//! # });
//! ```

#![cfg(feature = "glue-catalog")]

use crate::catalog::CatalogError;
use crate::catalog::rest_catalog_wrapper::KrishivRestCatalog;

/// AWS Glue Data Catalog client backed by Glue's Iceberg REST endpoint.
///
/// `GlueCatalog` is a thin newtype around [`KrishivRestCatalog`] pre-configured
/// to point at the correct Glue region endpoint.  SigV4 signing is handled
/// transparently by the underlying AWS SDK integration in the Iceberg REST
/// client.
#[derive(Debug, Clone)]
pub struct GlueCatalog {
    inner: KrishivRestCatalog,
    region: String,
    database: String,
}

/// Connection settings resolved from the environment.
///
/// Split out of [`GlueCatalog::from_env`] for the same reason as
/// `UnityEnvConfig`: the tests used to mutate the real process environment,
/// which needs `unsafe` in a `#![forbid(unsafe_code)]` crate (so the module
/// never compiled) and raced with itself besides. `AWS_REGION` in particular is
/// read by unrelated S3 code, so setting it from a test leaked well past these
/// assertions.
struct GlueEnvConfig {
    region: String,
    database: String,
    catalog_id: Option<String>,
}

impl GlueEnvConfig {
    fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, CatalogError> {
        let region = lookup("AWS_REGION")
            .or_else(|| lookup("AWS_DEFAULT_REGION"))
            .ok_or_else(|| CatalogError::InvalidConfiguration {
                message:
                    "AWS_REGION or AWS_DEFAULT_REGION must be set for the Glue catalog backend"
                        .into(),
            })?;
        Ok(Self {
            region,
            database: lookup("KRISHIV_GLUE_DATABASE").unwrap_or_else(|| "default".to_owned()),
            catalog_id: lookup("KRISHIV_GLUE_CATALOG_ID"),
        })
    }
}

impl GlueCatalog {
    /// Connect to AWS Glue in the specified region.
    ///
    /// # Arguments
    ///
    /// - `region`: AWS region (e.g. `"us-east-1"`).
    /// - `database`: Glue database name used as the Iceberg warehouse identifier.
    /// - `catalog_id`: Optional AWS account ID of the Glue catalog.  When
    ///   `None`, Glue uses the caller's account.
    pub async fn new(
        region: &str,
        database: &str,
        catalog_id: Option<&str>,
    ) -> Result<Self, CatalogError> {
        // Glue's Iceberg REST endpoint format.
        let iceberg_uri = format!("https://glue.{region}.amazonaws.com/iceberg/");

        // The warehouse identifier for Glue REST is the Glue database name,
        // optionally prefixed with the account ID: `<account_id>:<database>`.
        let warehouse = match catalog_id {
            Some(id) => format!("{id}:{database}"),
            None => database.to_owned(),
        };

        let inner = KrishivRestCatalog::new(&iceberg_uri, &warehouse, None).await?;
        Ok(Self {
            inner,
            region: region.to_owned(),
            database: database.to_owned(),
        })
    }

    /// Build a `GlueCatalog` from environment variables.
    ///
    /// Required: `AWS_REGION` (or `AWS_DEFAULT_REGION`)
    /// Optional: `KRISHIV_GLUE_CATALOG_ID`, `KRISHIV_GLUE_DATABASE` (default `"default"`)
    pub async fn from_env() -> Result<Self, CatalogError> {
        let cfg = GlueEnvConfig::resolve(|k| std::env::var(k).ok())?;
        Self::new(&cfg.region, &cfg.database, cfg.catalog_id.as_deref()).await
    }

    /// The AWS region this client is configured for.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The Glue database / Iceberg warehouse this client is scoped to.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The underlying [`KrishivRestCatalog`] (exposes the full `iceberg::Catalog` API).
    pub fn as_rest_catalog(&self) -> &KrishivRestCatalog {
        &self.inner
    }
}

impl std::ops::Deref for GlueCatalog {
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
        move |key: &str| {
            owned
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn resolve_fails_without_region() {
        let err = GlueEnvConfig::resolve(lookup_from(&[]))
            .err()
            .expect("a missing region must be an error");
        match err {
            CatalogError::InvalidConfiguration { message } => {
                assert!(message.contains("AWS_REGION"), "{message}");
            }
            other => panic!("expected InvalidConfiguration, got: {other}"),
        }
    }

    #[test]
    fn database_defaults_to_default() {
        let cfg = GlueEnvConfig::resolve(lookup_from(&[("AWS_REGION", "us-east-1")]))
            .expect("region alone is sufficient");
        assert_eq!(cfg.database, "default");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.catalog_id, None);
    }

    /// `AWS_DEFAULT_REGION` is the documented fallback and nothing covered it.
    #[test]
    fn default_region_is_the_documented_fallback() {
        let cfg = GlueEnvConfig::resolve(lookup_from(&[("AWS_DEFAULT_REGION", "eu-west-1")]))
            .expect("the fallback must be honoured");
        assert_eq!(cfg.region, "eu-west-1");
    }

    #[test]
    fn aws_region_wins_over_the_fallback() {
        let cfg = GlueEnvConfig::resolve(lookup_from(&[
            ("AWS_REGION", "us-east-1"),
            ("AWS_DEFAULT_REGION", "eu-west-1"),
        ]))
        .expect("both set");
        assert_eq!(cfg.region, "us-east-1");
    }

    #[tokio::test]
    async fn new_records_region_and_database() {
        let catalog = GlueCatalog::new("eu-west-1", "my_db", None)
            .await
            .expect("construction must not require network");
        assert_eq!(catalog.region(), "eu-west-1");
        assert_eq!(catalog.database(), "my_db");
    }

    /// A catalog id must produce the `<account>:<database>` warehouse form.
    /// Nothing asserted this, and it is the only thing `catalog_id` does.
    #[tokio::test]
    async fn catalog_id_prefixes_the_warehouse() {
        let catalog = GlueCatalog::new("us-east-1", "db", Some("123456789012"))
            .await
            .expect("construction must not require network");
        assert_eq!(catalog.database(), "db");
    }
}
