//! File-system (Hadoop-style) Iceberg catalog (Phase J1).
//!
//! [`LocalCatalog`] implements [`iceberg::Catalog`] over a local warehouse
//! directory.  It is intended for development and embedded single-process use
//! where a network catalog (REST / Postgres) is not warranted but real
//! Iceberg-spec metadata on disk is still required.
//!
//! # Layout
//!
//! ```text
//! warehouse/
//!   {namespace}/
//!     {table}/
//!       metadata/
//!         00000-<uuid>.metadata.json
//!         version-hint.text       # absolute metadata-location of the latest commit
//!       data/
//!         *.parquet
//! ```
//!
//! All Iceberg-spec metadata writes (manifests, manifest lists, table-metadata
//! JSON) are delegated to an inner [`iceberg::MemoryCatalog`] backed by
//! [`LocalFsStorageFactory`], which already produces correct on-disk Iceberg
//! files.  The only thing the memory catalog does *not* do is survive a process
//! restart: it keeps the namespace → table → metadata-location registry in RAM.
//!
//! `LocalCatalog` adds durability on top of that by writing a
//! `version-hint.text` next to every table's metadata and, on construction,
//! scanning the warehouse and re-registering every table it finds back into the
//! in-memory registry.  This mirrors the recovery logic already used by
//! `krishiv_connectors::lakehouse::IcebergNativeTwoPhaseCommit`.

#![cfg(feature = "local-catalog")]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};

use crate::catalog::LakehouseError;

const VERSION_HINT: &str = "version-hint.text";
const METADATA_DIR: &str = "metadata";

/// File-system backed Iceberg catalog rooted at a local warehouse directory.
#[derive(Debug)]
pub struct LocalCatalog {
    inner: Arc<MemoryCatalog>,
    warehouse: PathBuf,
}

impl LocalCatalog {
    /// Open (or initialise) a file-system catalog rooted at `warehouse`.
    ///
    /// The directory is created if it does not exist.  Any tables already
    /// present under the warehouse (detected via their `version-hint.text`
    /// files) are re-registered so the catalog is usable immediately after a
    /// restart.
    pub async fn new(warehouse: &Path) -> Result<Self, LakehouseError> {
        fs::create_dir_all(warehouse).map_err(|e| LakehouseError::Io(e.to_string()))?;
        let warehouse = warehouse
            .canonicalize()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;

        let warehouse_uri = path_to_uri(&warehouse)?;

        let inner = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "local",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_uri)]),
            )
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let inner = Arc::new(inner);

        let catalog = Self {
            inner,
            warehouse: warehouse.clone(),
        };
        catalog.recover_from_disk().await?;
        Ok(catalog)
    }

    /// The warehouse root directory.
    pub fn warehouse(&self) -> &Path {
        &self.warehouse
    }

    /// Default table location (`file://` URI) for `{namespace}/{table}`.
    fn table_location_uri(&self, namespace: &NamespaceIdent, table: &str) -> Result<String, LakehouseError> {
        let mut dir = self.warehouse.clone();
        for part in namespace.clone().inner() {
            dir.push(part);
        }
        dir.push(table);
        // The directory must exist for canonicalize-free URI formatting; create it.
        fs::create_dir_all(&dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
        path_to_uri(&dir)
    }

    /// Local metadata directory for `{namespace}/{table}`.
    fn table_metadata_dir(&self, namespace: &NamespaceIdent, table: &str) -> PathBuf {
        let mut dir = self.warehouse.clone();
        for part in namespace.clone().inner() {
            dir.push(part);
        }
        dir.push(table);
        dir.push(METADATA_DIR);
        dir
    }

    /// Persist the latest metadata location for a table to `version-hint.text`.
    fn write_version_hint(
        &self,
        namespace: &NamespaceIdent,
        table: &str,
        metadata_location: &str,
    ) -> Result<(), LakehouseError> {
        let dir = self.table_metadata_dir(namespace, table);
        fs::create_dir_all(&dir).map_err(|e| LakehouseError::Io(e.to_string()))?;
        fs::write(dir.join(VERSION_HINT), metadata_location)
            .map_err(|e| LakehouseError::Io(e.to_string()))
    }

    /// Scan the warehouse directory and re-register every table found.
    ///
    /// A directory is treated as a table when it contains
    /// `metadata/version-hint.text`.  Its parent path (relative to the
    /// warehouse) is the namespace.  Namespaces are created idempotently before
    /// their tables are registered.
    async fn recover_from_disk(&self) -> Result<(), LakehouseError> {
        let mut discovered: Vec<(NamespaceIdent, String, String)> = Vec::new();
        discover_tables(&self.warehouse, &self.warehouse, &mut discovered)?;

        for (namespace, table_name, metadata_location) in discovered {
            // Create namespace chain (idempotent).
            let _ = self.inner.create_namespace(&namespace, HashMap::new()).await;
            let ident = TableIdent::new(namespace, table_name);
            // Register only if not already known (defensive against double scan).
            if !self.inner.table_exists(&ident).await.unwrap_or(false) {
                self.inner
                    .register_table(&ident, metadata_location)
                    .await
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Catalog for LocalCatalog {
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
        // Materialise the namespace directory so the layout is observable on disk
        // even before any table is created.
        let mut dir = self.warehouse.clone();
        for part in namespace.clone().inner() {
            dir.push(part);
        }
        let _ = fs::create_dir_all(&dir);
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
        // Ensure an explicit location under the warehouse so on-disk layout is
        // deterministic (namespace/table) rather than catalog-default.
        let creation = if creation.location.is_some() {
            creation
        } else {
            let location = self
                .table_location_uri(namespace, &creation.name)
                .map_err(to_iceberg_err)?;
            TableCreation {
                location: Some(location),
                ..creation
            }
        };
        let table_name = creation.name.clone();
        let table = self.inner.create_table(namespace, creation).await?;
        if let Some(loc) = table.metadata_location() {
            self.write_version_hint(namespace, &table_name, loc)
                .map_err(to_iceberg_err)?;
        }
        Ok(table)
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let dir = self.table_metadata_dir(table.namespace(), table.name());
        self.inner.drop_table(table).await?;
        // Remove the version hint so a later recovery does not resurrect the table.
        let _ = fs::remove_file(dir.join(VERSION_HINT));
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await?;
        // Mirror the version hint to the destination metadata dir for recovery.
        if let Ok(table) = self.inner.load_table(dest).await
            && let Some(loc) = table.metadata_location()
        {
            let _ = self.write_version_hint(dest.namespace(), dest.name(), loc);
            let src_hint = self
                .table_metadata_dir(src.namespace(), src.name())
                .join(VERSION_HINT);
            let _ = fs::remove_file(src_hint);
        }
        Ok(())
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        let registered = self
            .inner
            .register_table(table, metadata_location.clone())
            .await?;
        self.write_version_hint(table.namespace(), table.name(), &metadata_location)
            .map_err(to_iceberg_err)?;
        Ok(registered)
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        let ident = commit.identifier().clone();
        let updated = self.inner.update_table(commit).await?;
        if let Some(loc) = updated.metadata_location() {
            self.write_version_hint(ident.namespace(), ident.name(), loc)
                .map_err(to_iceberg_err)?;
        }
        Ok(updated)
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn to_iceberg_err(e: LakehouseError) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string())
}

/// Convert an absolute local path to a `file://` URI.
fn path_to_uri(path: &Path) -> Result<String, LakehouseError> {
    url::Url::from_file_path(path)
        .map(|u| u.to_string())
        .map_err(|()| LakehouseError::Io(format!("cannot convert path to URI: {}", path.display())))
}

/// Recursively walk `dir`, collecting `(namespace, table_name, metadata_location)`
/// for every directory that contains `metadata/version-hint.text`.
fn discover_tables(
    warehouse_root: &Path,
    dir: &Path,
    out: &mut Vec<(NamespaceIdent, String, String)>,
) -> Result<(), LakehouseError> {
    let hint = dir.join(METADATA_DIR).join(VERSION_HINT);
    if hint.is_file() {
        // `dir` is a table directory. Its path relative to the warehouse root is
        // {namespace parts...}/{table}.
        let rel = dir
            .strip_prefix(warehouse_root)
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let parts: Vec<String> = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        if let Some((table_name, ns_parts)) = parts.split_last()
            && !ns_parts.is_empty()
        {
            let namespace = NamespaceIdent::from_vec(ns_parts.to_vec())
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
            let metadata_location = fs::read_to_string(&hint)
                .map_err(|e| LakehouseError::Io(e.to_string()))?
                .trim()
                .to_string();
            if !metadata_location.is_empty() {
                out.push((namespace, table_name.clone(), metadata_location));
            }
        }
        // A table directory is a leaf for discovery purposes.
        return Ok(());
    }

    // Otherwise recurse into subdirectories.
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // Skip the conventional data/metadata dirs at non-table levels.
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name == METADATA_DIR || name == "data" {
                continue;
            }
            discover_tables(warehouse_root, &path, out)?;
        }
    }
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    fn sample_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    async fn create_table(
        catalog: &LocalCatalog,
        ns: &str,
        table: &str,
    ) -> Table {
        let namespace = NamespaceIdent::new(ns.to_string());
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        let creation = TableCreation::builder()
            .name(table.to_string())
            .schema(sample_schema())
            .build();
        catalog.create_table(&namespace, creation).await.unwrap()
    }

    #[tokio::test]
    async fn local_catalog_create_and_load_table() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = LocalCatalog::new(dir.path()).await.unwrap();

        let created = create_table(&catalog, "sales", "orders").await;
        assert_eq!(created.identifier().name(), "orders");

        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "orders".to_string());
        let loaded = catalog.load_table(&ident).await.unwrap();
        assert_eq!(loaded.metadata().current_schema().as_ref().field_id_by_name("id"), Some(1));
        assert_eq!(
            loaded.metadata().current_schema().as_ref().field_id_by_name("name"),
            Some(2)
        );

        // version-hint.text must have been written.
        let hint = catalog
            .table_metadata_dir(&NamespaceIdent::new("sales".to_string()), "orders")
            .join(VERSION_HINT);
        assert!(hint.is_file(), "version-hint.text should be persisted");
    }

    #[tokio::test]
    async fn local_catalog_list_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = LocalCatalog::new(dir.path()).await.unwrap();

        catalog
            .create_namespace(&NamespaceIdent::new("alpha".to_string()), HashMap::new())
            .await
            .unwrap();
        catalog
            .create_namespace(&NamespaceIdent::new("beta".to_string()), HashMap::new())
            .await
            .unwrap();

        let mut names: Vec<String> = catalog
            .list_namespaces(None)
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.inner().join("."))
            .collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn local_catalog_list_tables() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = LocalCatalog::new(dir.path()).await.unwrap();

        create_table(&catalog, "sales", "orders").await;
        create_table(&catalog, "sales", "customers").await;

        let namespace = NamespaceIdent::new("sales".to_string());
        let mut tables: Vec<String> = catalog
            .list_tables(&namespace)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.name().to_string())
            .collect();
        tables.sort();
        assert_eq!(tables, vec!["customers", "orders"]);
    }

    #[tokio::test]
    async fn local_catalog_recovers_tables_after_restart() {
        let dir = tempfile::tempdir().unwrap();

        // Session 1: create a table.
        {
            let catalog = LocalCatalog::new(dir.path()).await.unwrap();
            create_table(&catalog, "sales", "orders").await;
        }

        // Session 2: a fresh catalog over the same warehouse must rediscover it.
        {
            let catalog = LocalCatalog::new(dir.path()).await.unwrap();
            let namespace = NamespaceIdent::new("sales".to_string());
            let tables = catalog.list_tables(&namespace).await.unwrap();
            assert_eq!(tables.len(), 1, "table should survive a restart");
            assert_eq!(tables[0].name(), "orders");
            // And it must be loadable.
            let loaded = catalog.load_table(&tables[0]).await.unwrap();
            assert!(loaded.metadata().current_schema().as_ref().field_id_by_name("id").is_some());
        }
    }

    #[tokio::test]
    async fn local_catalog_drop_table_removes_it() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = LocalCatalog::new(dir.path()).await.unwrap();
        create_table(&catalog, "sales", "orders").await;

        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "orders".to_string());
        assert!(catalog.table_exists(&ident).await.unwrap());
        catalog.drop_table(&ident).await.unwrap();
        assert!(!catalog.table_exists(&ident).await.unwrap());
    }
}
