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
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .map_err(|_| CatalogError::InvalidConfiguration {
                message:
                    "AWS_REGION or AWS_DEFAULT_REGION must be set for the Glue catalog backend"
                        .into(),
            })?;
        let database = std::env::var("KRISHIV_GLUE_DATABASE").unwrap_or_else(|_| "default".into());
        let catalog_id = std::env::var("KRISHIV_GLUE_CATALOG_ID").ok();
        Self::new(&region, &database, catalog_id.as_deref()).await
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

    #[test]
    fn from_env_fails_without_region() {
        unsafe {
            std::env::remove_var("AWS_REGION");
            std::env::remove_var("AWS_DEFAULT_REGION");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(GlueCatalog::from_env());
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            CatalogError::InvalidConfiguration { message } => {
                assert!(message.contains("AWS_REGION"), "{message}");
            }
            other => panic!("expected InvalidConfiguration, got: {other}"),
        }
    }

    #[test]
    fn database_default_is_default() {
        unsafe {
            std::env::set_var("AWS_REGION", "us-east-1");
            std::env::remove_var("KRISHIV_GLUE_DATABASE");
            std::env::remove_var("KRISHIV_GLUE_CATALOG_ID");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let catalog = rt
            .block_on(GlueCatalog::from_env())
            .expect("construction must succeed");
        assert_eq!(catalog.database(), "default");
        assert_eq!(catalog.region(), "us-east-1");
        unsafe {
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    fn glue_endpoint_includes_region() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let catalog = rt
            .block_on(GlueCatalog::new("eu-west-1", "my_db", None))
            .expect("construction must succeed");
        assert_eq!(catalog.region(), "eu-west-1");
        assert_eq!(catalog.database(), "my_db");
    }
}
