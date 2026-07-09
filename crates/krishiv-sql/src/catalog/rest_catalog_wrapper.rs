//! REST catalog wrapper (Phase J4).
//!
//! [`KrishivRestCatalog`] wraps the official `iceberg-catalog-rest` [`RestCatalog`]
//! and implements [`iceberg::Catalog`] by delegation.  This gives Krishiv access
//! to any Iceberg REST-compatible catalog server — Nessie, Apache Polaris,
//! Tabular, AWS Glue, or any custom implementation — via a single URL.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::CatalogBuilder as _;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog, RestCatalogBuilder,
};

use crate::catalog::CatalogError;

/// Thin wrapper over [`RestCatalog`] that implements [`iceberg::Catalog`].
#[derive(Debug, Clone)]
pub struct KrishivRestCatalog {
    inner: Arc<RestCatalog>,
}

impl KrishivRestCatalog {
    /// Connect to the Iceberg REST catalog at `uri`.
    ///
    /// `warehouse` identifies the default warehouse within the catalog server
    /// (empty string = server default). `token` is an optional Bearer token
    /// for servers that require authentication.
    pub async fn new(
        uri: &str,
        warehouse: &str,
        token: Option<&str>,
    ) -> Result<Self, CatalogError> {
        let mut props: HashMap<String, String> = HashMap::new();
        props.insert(REST_CATALOG_PROP_URI.to_string(), uri.to_string());
        if !warehouse.is_empty() {
            props.insert(
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                warehouse.to_string(),
            );
        }
        if let Some(t) = token {
            props.insert(String::from("token"), t.to_string());
        }
        // Dispatch storage by URI scheme so the catalog serves both `file://`
        // (laptop) and `s3://` (shared object-store) warehouses. S3 config is
        // read from the AWS environment by KrishivStorage.
        let inner = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(
                crate::catalog::object_store_io::KrishivStorageFactory,
            ))
            .load("rest", props)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The underlying [`RestCatalog`].
    pub fn inner(&self) -> &RestCatalog {
        &self.inner
    }
}

#[async_trait]
impl Catalog for KrishivRestCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
}
