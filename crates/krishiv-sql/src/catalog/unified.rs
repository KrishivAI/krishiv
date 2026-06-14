//! Unified Iceberg catalog facade (Phase J1).
//!
//! [`KrishivCatalog`] is a thin enum over the concrete catalog backends Krishiv
//! supports.  Every variant wraps a type that implements [`iceberg::Catalog`],
//! and [`KrishivCatalog::as_iceberg`] hands the inner catalog back as an
//! `Arc<dyn iceberg::Catalog>` so it can be passed directly to
//! `iceberg-datafusion`'s `IcebergTableProvider` / `IcebergCatalogProvider`
//! without any adapter layer.
//!
//! The high-level helpers (`list_namespaces`, `list_tables`, `load_table`,
//! `create_table`) operate in terms of plain strings so callers outside the
//! Iceberg type universe (SQL DDL, the REST/Flight surface) do not need to
//! construct `NamespaceIdent` / `TableIdent` values themselves.

#![cfg(feature = "local-catalog")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::Schema;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

use crate::catalog::CatalogError;
#[cfg(feature = "local-catalog")]
use crate::catalog::local_catalog::LocalCatalog;
#[cfg(feature = "postgres-catalog")]
use crate::catalog::postgres_catalog::PostgresCatalog;
#[cfg(feature = "rest-catalog")]
use crate::catalog::rest_catalog_wrapper::KrishivRestCatalog;

/// The set of concrete catalog backends Krishiv can talk to.
///
/// Each variant wraps an `Arc` of a type that implements [`iceberg::Catalog`].
#[derive(Clone)]
pub enum KrishivCatalog {
    /// In-memory Iceberg catalog (the iceberg-rust built-in). Tests / ephemeral.
    Memory(Arc<MemoryCatalog>),
    /// File-system (Hadoop-style) catalog. Dev / embedded single-process.
    Local(Arc<LocalCatalog>),
    /// Postgres-backed catalog. Production single-node / small clusters.
    #[cfg(feature = "postgres-catalog")]
    Postgres(Arc<PostgresCatalog>),
    /// REST catalog. Distributed deployments behind a catalog service.
    #[cfg(feature = "rest-catalog")]
    Rest(Arc<KrishivRestCatalog>),
}

impl std::fmt::Debug for KrishivCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KrishivCatalog::Memory(_) => f.write_str("KrishivCatalog::Memory"),
            KrishivCatalog::Local(_) => f.write_str("KrishivCatalog::Local"),
            #[cfg(feature = "postgres-catalog")]
            KrishivCatalog::Postgres(_) => f.write_str("KrishivCatalog::Postgres"),
            #[cfg(feature = "rest-catalog")]
            KrishivCatalog::Rest(_) => f.write_str("KrishivCatalog::Rest"),
        }
    }
}

impl KrishivCatalog {
    /// Build an in-memory catalog backed by the local filesystem at `warehouse`.
    ///
    /// Metadata is written in real Iceberg-spec format but the namespace/table
    /// registry is *not* persisted across process restarts. Use
    /// [`KrishivCatalog::local`] for a restart-durable file-system catalog.
    pub async fn memory(warehouse: &str) -> Result<Self, CatalogError> {
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string())]),
            )
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
        Ok(KrishivCatalog::Memory(Arc::new(catalog)))
    }

    /// Build a restart-durable file-system catalog rooted at `warehouse`.
    pub async fn local(warehouse: &Path) -> Result<Self, CatalogError> {
        let catalog = LocalCatalog::new(warehouse).await?;
        Ok(KrishivCatalog::Local(Arc::new(catalog)))
    }

    /// Build a Postgres-backed catalog.
    #[cfg(feature = "postgres-catalog")]
    pub async fn postgres(database_url: &str, warehouse: &str) -> Result<Self, CatalogError> {
        let catalog = PostgresCatalog::new(database_url, warehouse).await?;
        Ok(KrishivCatalog::Postgres(Arc::new(catalog)))
    }

    /// Build a REST-catalog-backed catalog.
    #[cfg(feature = "rest-catalog")]
    pub async fn rest(
        url: &str,
        warehouse: &str,
        token: Option<&str>,
    ) -> Result<Self, CatalogError> {
        let catalog = KrishivRestCatalog::new(url, warehouse, token).await?;
        Ok(KrishivCatalog::Rest(Arc::new(catalog)))
    }

    /// Return the inner catalog as a trait object for use with
    /// `iceberg-datafusion` and other Iceberg-native consumers.
    pub fn as_iceberg(&self) -> Arc<dyn Catalog + Send + Sync> {
        match self {
            KrishivCatalog::Memory(c) => c.clone() as Arc<dyn Catalog + Send + Sync>,
            KrishivCatalog::Local(c) => c.clone() as Arc<dyn Catalog + Send + Sync>,
            #[cfg(feature = "postgres-catalog")]
            KrishivCatalog::Postgres(c) => c.clone() as Arc<dyn Catalog + Send + Sync>,
            #[cfg(feature = "rest-catalog")]
            KrishivCatalog::Rest(c) => c.clone() as Arc<dyn Catalog + Send + Sync>,
        }
    }

    /// List all top-level namespaces as dotted strings (e.g. `"sales"`).
    pub async fn list_namespaces(&self) -> Result<Vec<String>, CatalogError> {
        let catalog = self.as_iceberg();
        let namespaces = catalog
            .list_namespaces(None)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
        Ok(namespaces
            .into_iter()
            .map(|n| n.inner().join("."))
            .collect())
    }

    /// List the tables in `namespace`.
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<String>, CatalogError> {
        let catalog = self.as_iceberg();
        let ns = parse_namespace(namespace)?;
        let tables = catalog
            .list_tables(&ns)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
        Ok(tables.into_iter().map(|t| t.name().to_string()).collect())
    }

    /// Load a table by `namespace` and `table` name.
    pub async fn load_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<Table, CatalogError> {
        let catalog = self.as_iceberg();
        let ident = TableIdent::new(parse_namespace(namespace)?, table.to_string());
        catalog
            .load_table(&ident)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    }

    /// Create a new table in `namespace`. Creates the namespace if needed.
    pub async fn create_table(
        &self,
        namespace: &str,
        name: &str,
        schema: Schema,
        location: &str,
    ) -> Result<Table, CatalogError> {
        let catalog = self.as_iceberg();
        let ns = parse_namespace(namespace)?;
        // Create the namespace idempotently.
        if !catalog
            .namespace_exists(&ns)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?
        {
            catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
        }
        let creation = if location.is_empty() {
            TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .build()
        } else {
            TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .location(location.to_string())
                .build()
        };
        catalog
            .create_table(&ns, creation)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    }

    /// Drop a table.
    pub async fn drop_table(&self, namespace: &str, table: &str) -> Result<(), CatalogError> {
        let catalog = self.as_iceberg();
        let ident = TableIdent::new(parse_namespace(namespace)?, table.to_string());
        catalog
            .drop_table(&ident)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    }
}

/// Parse a dotted namespace string into a [`NamespaceIdent`].
fn parse_namespace(namespace: &str) -> Result<NamespaceIdent, CatalogError> {
    if namespace.is_empty() {
        return Err(CatalogError::InvalidConfiguration {
            message: "namespace must not be empty".to_string(),
        });
    }
    let parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();
    NamespaceIdent::from_vec(parts).map_err(|e| CatalogError::Iceberg(e.to_string()))
}

// ── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    fn sample_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn unified_local_create_list_load() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = KrishivCatalog::local(dir.path()).await.unwrap();

        catalog
            .create_table("sales", "orders", sample_schema(), "")
            .await
            .unwrap();

        let namespaces = catalog.list_namespaces().await.unwrap();
        assert!(namespaces.contains(&"sales".to_string()));

        let tables = catalog.list_tables("sales").await.unwrap();
        assert_eq!(tables, vec!["orders"]);

        let table = catalog.load_table("sales", "orders").await.unwrap();
        assert_eq!(table.identifier().name(), "orders");
    }

    #[tokio::test]
    async fn unified_as_iceberg_returns_usable_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = KrishivCatalog::local(dir.path()).await.unwrap();
        let iceberg = catalog.as_iceberg();
        // The trait object must be usable directly.
        let namespaces = iceberg.list_namespaces(None).await.unwrap();
        assert!(namespaces.is_empty());
    }

    #[tokio::test]
    async fn unified_memory_catalog_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = KrishivCatalog::memory(&warehouse).await.unwrap();
        catalog
            .create_table("ns", "t", sample_schema(), "")
            .await
            .unwrap();
        let tables = catalog.list_tables("ns").await.unwrap();
        assert_eq!(tables, vec!["t"]);
    }

    #[tokio::test]
    async fn unified_drop_table() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = KrishivCatalog::local(dir.path()).await.unwrap();
        catalog
            .create_table("ns", "t", sample_schema(), "")
            .await
            .unwrap();
        catalog.drop_table("ns", "t").await.unwrap();
        let tables = catalog.list_tables("ns").await.unwrap();
        assert!(tables.is_empty());
    }
}
