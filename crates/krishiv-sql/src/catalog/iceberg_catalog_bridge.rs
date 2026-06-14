//! DataFusion `CatalogProvider` backed by a [`KrishivCatalog`] (Phase J2).
//!
//! Unlike [`super::datafusion_bridge::DataFusionCatalogBridge`] (which is backed
//! by the in-memory [`super::InMemoryCatalog`] and serves `MemTable`s), this
//! bridge resolves tables through `iceberg-datafusion`, giving DataFusion a real
//! file-scan with projection / filter / partition pushdown.
//!
//! Each Iceberg namespace is exposed as a DataFusion schema; each `table()` call
//! loads the Iceberg table from the catalog and wraps it in an
//! `IcebergTableProvider`.

#![cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;

use crate::catalog::iceberg_table_provider::iceberg_scan::iceberg_table_provider;
use crate::catalog::unified::KrishivCatalog;

/// DataFusion [`CatalogProvider`] that resolves Iceberg tables through a
/// [`KrishivCatalog`].
#[derive(Clone)]
pub struct IcebergCatalogBridge {
    catalog: Arc<KrishivCatalog>,
    catalog_name: String,
}

impl IcebergCatalogBridge {
    /// Wrap a [`KrishivCatalog`] under the DataFusion catalog name `catalog_name`.
    pub fn new(catalog: Arc<KrishivCatalog>, catalog_name: impl Into<String>) -> Self {
        Self {
            catalog,
            catalog_name: catalog_name.into(),
        }
    }

    /// The DataFusion catalog name this bridge is registered under.
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Block on `fut` from a synchronous DataFusion trait method.
    ///
    /// DataFusion's `CatalogProvider::schema_names` is synchronous but the
    /// Iceberg catalog is async. We bridge with the current Tokio runtime via
    /// `block_in_place` (multi-thread runtime) and fall back to a private
    /// current-thread runtime when not inside a runtime worker.
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build fallback Tokio runtime")
                .block_on(fut),
        }
    }
}

impl fmt::Debug for IcebergCatalogBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergCatalogBridge")
            .field("catalog_name", &self.catalog_name)
            .finish()
    }
}

impl CatalogProvider for IcebergCatalogBridge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        Self::block_on(self.catalog.list_namespaces()).unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Expose the namespace as a schema if it exists.
        let namespaces = Self::block_on(self.catalog.list_namespaces()).ok()?;
        if namespaces.iter().any(|n| n == name) {
            Some(Arc::new(IcebergSchemaBridge {
                catalog: self.catalog.clone(),
                namespace: name.to_string(),
            }))
        } else {
            None
        }
    }
}

/// DataFusion [`SchemaProvider`] for a single Iceberg namespace.
struct IcebergSchemaBridge {
    catalog: Arc<KrishivCatalog>,
    namespace: String,
}

impl fmt::Debug for IcebergSchemaBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergSchemaBridge")
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait::async_trait]
impl SchemaProvider for IcebergSchemaBridge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        IcebergCatalogBridge::block_on(self.catalog.list_tables(&self.namespace))
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        match self.catalog.load_table(&self.namespace, name).await {
            Ok(table) => {
                let provider = iceberg_table_provider(table).await?;
                Ok(Some(provider))
            }
            // A missing table is not an error for DataFusion resolution.
            Err(_) => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        IcebergCatalogBridge::block_on(self.catalog.list_tables(&self.namespace))
            .map(|tables| tables.iter().any(|t| t == name))
            .unwrap_or(false)
    }
}

// ── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::catalog::unified::KrishivCatalog;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iceberg_catalog_bridge_lists_namespaces_as_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        catalog
            .create_table("sales", "orders", sample_schema(), "")
            .await
            .unwrap();

        let bridge = IcebergCatalogBridge::new(catalog, "iceberg");
        let schemas = bridge.schema_names();
        assert!(schemas.contains(&"sales".to_string()));
        assert!(bridge.schema("sales").is_some());
        assert!(bridge.schema("does_not_exist").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iceberg_catalog_bridge_table_provider_returns_iceberg_provider() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        catalog
            .create_table("sales", "orders", sample_schema(), "")
            .await
            .unwrap();

        let bridge = IcebergCatalogBridge::new(catalog, "iceberg");
        let schema = bridge.schema("sales").expect("namespace schema");
        assert!(schema.table_exist("orders"));
        let provider = schema.table("orders").await.unwrap();
        assert!(provider.is_some(), "iceberg table provider expected");
        let provider = provider.unwrap();
        let arrow_schema = TableProvider::schema(&*provider);
        assert!(arrow_schema.field_with_name("id").is_ok());
    }
}
