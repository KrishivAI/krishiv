//! Postgres-backed Iceberg catalog (Phase J3).
//!
//! [`PostgresCatalog`] implements [`iceberg::Catalog`] using two plain SQL
//! tables (`krishiv_namespaces` and `krishiv_tables`) stored in a Postgres
//! database. Atomic metadata-pointer updates use an optimistic compare-and-swap
//! `UPDATE … WHERE metadata_location = $expected` so no advisory locks or
//! external coordinators are required.
//!
//! # Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS krishiv_namespaces (
//!     namespace_name TEXT PRIMARY KEY,
//!     properties     JSONB NOT NULL DEFAULT '{}'
//! );
//!
//! CREATE TABLE IF NOT EXISTS krishiv_tables (
//!     namespace         TEXT NOT NULL,
//!     table_name        TEXT NOT NULL,
//!     metadata_location TEXT NOT NULL,
//!     properties        JSONB NOT NULL DEFAULT '{}',
//!     created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//!     PRIMARY KEY (namespace, table_name)
//! );
//! ```

#![cfg(feature = "postgres-catalog")]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation,
    TableIdent,
};
use sqlx::PgPool;

use crate::catalog::CatalogError;

/// Postgres-backed Iceberg catalog.
///
/// Each table's actual data and Iceberg-spec metadata files (manifests, etc.)
/// live in the `warehouse` location; Postgres only stores the pointer
/// (`metadata_location`) to the current `table-metadata.json`.
#[derive(Debug)]
pub struct PostgresCatalog {
    pool: PgPool,
    /// Base warehouse URI (e.g. `file:///var/krishiv/warehouse` or `s3://bucket/prefix`).
    warehouse: String,
    file_io: FileIO,
}

impl PostgresCatalog {
    /// Connect to `database_url` and initialise the catalog schema.
    pub async fn new(database_url: &str, warehouse: &str) -> Result<Self, CatalogError> {
        let pool = PgPool::connect(database_url)
            .await
            .map_err(|e| CatalogError::Transport {
                operation: "connect".into(),
                message: e.to_string(),
            })?;
        let file_io = build_file_io(warehouse)?;
        let catalog = Self {
            pool,
            warehouse: warehouse.to_string(),
            file_io,
        };
        catalog.migrate().await?;
        Ok(catalog)
    }

    /// Create catalog tables if they do not exist.
    pub async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS krishiv_namespaces (
                 namespace_name TEXT PRIMARY KEY,
                 properties     JSONB NOT NULL DEFAULT '{}'
             )",
        )
        .execute(&self.pool)
        .await
        .map_err(pg_err("migrate namespaces"))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS krishiv_tables (
                 namespace         TEXT NOT NULL,
                 table_name        TEXT NOT NULL,
                 metadata_location TEXT NOT NULL,
                 properties        JSONB NOT NULL DEFAULT '{}',
                 created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 PRIMARY KEY (namespace, table_name)
             )",
        )
        .execute(&self.pool)
        .await
        .map_err(pg_err("migrate tables"))?;

        Ok(())
    }

    /// Default table location URI for `{namespace}/{table_name}`.
    fn table_location(&self, namespace: &NamespaceIdent, table_name: &str) -> String {
        let ns = namespace.inner().join("/");
        // Strip trailing slash from warehouse for clean joins.
        let base = self.warehouse.trim_end_matches('/');
        format!("{base}/{ns}/{table_name}")
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    // ── Namespaces ────────────────────────────────────────────────────────────

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT namespace_name FROM krishiv_namespaces ORDER BY namespace_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("list_namespaces: {e}")))?;

        rows.into_iter()
            .map(|name| {
                NamespaceIdent::from_vec(name.split('.').map(str::to_string).collect())
                    .map_err(|e| iceberg_err(e.to_string()))
            })
            .collect()
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        let name = namespace.inner().join(".");
        let props = serde_json::to_value(&properties)
            .map_err(|e| iceberg_err(format!("serialize properties: {e}")))?;
        sqlx::query(
            "INSERT INTO krishiv_namespaces (namespace_name, properties)
             VALUES ($1, $2)
             ON CONFLICT (namespace_name) DO NOTHING",
        )
        .bind(&name)
        .bind(&props)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("create_namespace: {e}")))?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        let name = namespace.inner().join(".");
        let props_json: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT properties FROM krishiv_namespaces WHERE namespace_name = $1",
        )
        .bind(&name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("get_namespace: {e}")))?;

        match props_json {
            None => Err(iceberg_err(format!("namespace not found: {name}"))),
            Some(v) => {
                let props: HashMap<String, String> = serde_json::from_value(v)
                    .map_err(|e| iceberg_err(format!("deserialize properties: {e}")))?;
                Ok(Namespace::with_properties(namespace.clone(), props))
            }
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let name = namespace.inner().join(".");
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM krishiv_namespaces WHERE namespace_name = $1)",
        )
        .bind(&name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("namespace_exists: {e}")))?;
        Ok(exists)
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        let name = namespace.inner().join(".");
        let props = serde_json::to_value(&properties)
            .map_err(|e| iceberg_err(format!("serialize: {e}")))?;
        sqlx::query(
            "UPDATE krishiv_namespaces SET properties = $2 WHERE namespace_name = $1",
        )
        .bind(&name)
        .bind(&props)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("update_namespace: {e}")))?;
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let name = namespace.inner().join(".");
        sqlx::query("DELETE FROM krishiv_namespaces WHERE namespace_name = $1")
            .bind(&name)
            .execute(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("drop_namespace: {e}")))?;
        Ok(())
    }

    // ── Tables ────────────────────────────────────────────────────────────────

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let ns = namespace.inner().join(".");
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT table_name FROM krishiv_tables WHERE namespace = $1 ORDER BY table_name",
        )
        .bind(&ns)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("list_tables: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|t| TableIdent::new(namespace.clone(), t))
            .collect())
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let ns = namespace.inner().join(".");
        let table_name = creation.name.clone();
        let location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_location(namespace, &table_name));

        // Build initial Iceberg table metadata.
        let metadata = TableMetadataBuilder::from_table_creation(creation)
            .map_err(|e| iceberg_err(e.to_string()))?
            .build()
            .map_err(|e| iceberg_err(e.to_string()))?
            .metadata;

        // Serialise and write metadata.json to the warehouse.
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| iceberg_err(format!("serialize metadata: {e}")))?;
        let metadata_location = format!("{}/metadata/00000-{}.metadata.json", location, uuid::Uuid::new_v4());

        let output = self
            .file_io
            .new_output(&metadata_location)
            .map_err(|e| iceberg_err(e.to_string()))?;
        {
            use iceberg::io::OutputFile;
            let mut writer = output
                .writer()
                .await
                .map_err(|e| iceberg_err(e.to_string()))?;
            use tokio::io::AsyncWriteExt;
            writer
                .write_all(metadata_json.as_bytes())
                .await
                .map_err(|e| iceberg_err(format!("write metadata: {e}")))?;
            writer
                .shutdown()
                .await
                .map_err(|e| iceberg_err(format!("flush metadata: {e}")))?;
        }

        // Insert pointer into Postgres.
        sqlx::query(
            "INSERT INTO krishiv_tables (namespace, table_name, metadata_location)
             VALUES ($1, $2, $3)",
        )
        .bind(&ns)
        .bind(&table_name)
        .bind(&metadata_location)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("insert table row: {e}")))?;

        // Load and return the Table.
        let ident = TableIdent::new(namespace.clone(), table_name);
        self.load_table(&ident).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let ns = table.namespace().inner().join(".");
        let metadata_location: Option<String> = sqlx::query_scalar(
            "SELECT metadata_location FROM krishiv_tables
              WHERE namespace = $1 AND table_name = $2",
        )
        .bind(&ns)
        .bind(table.name())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("load_table query: {e}")))?;

        let metadata_location =
            metadata_location.ok_or_else(|| iceberg_err(format!("table not found: {}", table.name())))?;

        // Read the metadata JSON from the warehouse.
        let input = self
            .file_io
            .new_input(&metadata_location)
            .map_err(|e| iceberg_err(e.to_string()))?;
        let bytes = {
            use iceberg::io::InputFile;
            use tokio::io::AsyncReadExt;
            let mut reader = input.reader().await.map_err(|e| iceberg_err(e.to_string()))?;
            let mut buf = Vec::new();
            reader
                .read_to_end(&mut buf)
                .await
                .map_err(|e| iceberg_err(format!("read metadata: {e}")))?;
            buf
        };

        let metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&bytes)
            .map_err(|e| iceberg_err(format!("deserialize metadata: {e}")))?;

        Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            .build()
            .map_err(|e| iceberg_err(e.to_string()))
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let ns = table.namespace().inner().join(".");
        sqlx::query(
            "DELETE FROM krishiv_tables WHERE namespace = $1 AND table_name = $2",
        )
        .bind(&ns)
        .bind(table.name())
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("drop_table: {e}")))?;
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let ns = table.namespace().inner().join(".");
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM krishiv_tables
                 WHERE namespace = $1 AND table_name = $2
             )",
        )
        .bind(&ns)
        .bind(table.name())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("table_exists: {e}")))?;
        Ok(exists)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        let src_ns = src.namespace().inner().join(".");
        let dest_ns = dest.namespace().inner().join(".");
        sqlx::query(
            "UPDATE krishiv_tables
                SET namespace = $3, table_name = $4, updated_at = NOW()
              WHERE namespace = $1 AND table_name = $2",
        )
        .bind(&src_ns)
        .bind(src.name())
        .bind(&dest_ns)
        .bind(dest.name())
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("rename_table: {e}")))?;
        Ok(())
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        let ns = table.namespace().inner().join(".");
        sqlx::query(
            "INSERT INTO krishiv_tables (namespace, table_name, metadata_location)
             VALUES ($1, $2, $3)
             ON CONFLICT (namespace, table_name)
             DO UPDATE SET metadata_location = EXCLUDED.metadata_location, updated_at = NOW()",
        )
        .bind(&ns)
        .bind(table.name())
        .bind(&metadata_location)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("register_table: {e}")))?;
        self.load_table(table).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        let ident = commit.identifier().clone();
        let ns = ident.namespace().inner().join(".");

        // Read current metadata_location to verify we're updating the right version.
        let current_location: Option<String> = sqlx::query_scalar(
            "SELECT metadata_location FROM krishiv_tables
              WHERE namespace = $1 AND table_name = $2",
        )
        .bind(&ns)
        .bind(ident.name())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("update_table read: {e}")))?;

        let current_location =
            current_location.ok_or_else(|| iceberg_err(format!("table not found: {}", ident.name())))?;

        // Load current table, apply commit requirements & updates.
        let table = self.load_table(&ident).await?;
        let (requirements, updates) = commit.into_parts();

        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for req in requirements {
            req.check(Some(table.metadata()))
                .map_err(|e| iceberg_err(format!("commit requirement: {e}")))?;
        }
        for update in updates {
            metadata_builder = update
                .apply(metadata_builder)
                .map_err(|e| iceberg_err(format!("apply update: {e}")))?;
        }
        let new_metadata = metadata_builder
            .build()
            .map_err(|e| iceberg_err(format!("build metadata: {e}")))?
            .metadata;

        // Write new metadata.json.
        let table_location = table.metadata().location();
        let new_metadata_json = serde_json::to_string_pretty(&new_metadata)
            .map_err(|e| iceberg_err(format!("serialize: {e}")))?;
        let version = new_metadata.last_sequence_number();
        let new_location = format!(
            "{}/metadata/{:05}-{}.metadata.json",
            table_location,
            version,
            uuid::Uuid::new_v4()
        );
        let output = self
            .file_io
            .new_output(&new_location)
            .map_err(|e| iceberg_err(e.to_string()))?;
        {
            use iceberg::io::OutputFile;
            use tokio::io::AsyncWriteExt;
            let mut writer = output.writer().await.map_err(|e| iceberg_err(e.to_string()))?;
            writer
                .write_all(new_metadata_json.as_bytes())
                .await
                .map_err(|e| iceberg_err(format!("write: {e}")))?;
            writer.shutdown().await.map_err(|e| iceberg_err(e.to_string()))?;
        }

        // Atomic CAS update — if another writer updated concurrently, this returns 0 rows.
        let rows_updated: u64 = sqlx::query(
            "UPDATE krishiv_tables
                SET metadata_location = $3, updated_at = NOW()
              WHERE namespace = $1 AND table_name = $2
                AND metadata_location = $4",
        )
        .bind(&ns)
        .bind(ident.name())
        .bind(&new_location)
        .bind(&current_location)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("CAS update: {e}")))?
        .rows_affected();

        if rows_updated == 0 {
            // Clean up the orphaned metadata file we just wrote.
            let _ = self.file_io.delete(&new_location).await;
            return Err(iceberg_err(
                "concurrent write conflict — retry the commit".to_string(),
            ));
        }

        self.load_table(&ident).await
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn iceberg_err(msg: impl Into<String>) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, msg.into())
}

fn pg_err(op: &'static str) -> impl Fn(sqlx::Error) -> iceberg::Error {
    move |e| iceberg_err(format!("{op}: {e}"))
}

fn build_file_io(warehouse: &str) -> Result<FileIO, CatalogError> {
    if warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") {
        FileIOBuilder::new("s3")
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    } else if warehouse.starts_with("abfs://") || warehouse.starts_with("abfss://") {
        FileIOBuilder::new("abfs")
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    } else if warehouse.starts_with("gs://") || warehouse.starts_with("gcs://") {
        FileIOBuilder::new("gcs")
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    } else {
        FileIOBuilder::new("file")
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    /// Postgres integration tests require a `KRISHIV_TEST_DATABASE_URL` env var.
    /// They are marked `#[ignore]` so the default `cargo test` run skips them.
    ///
    /// Run with:
    /// ```bash
    /// KRISHIV_TEST_DATABASE_URL=postgres://user:pass@localhost/test \
    ///   cargo test -p krishiv-sql --features postgres-catalog -- \
    ///   --ignored postgres_catalog
    /// ```
    use super::*;

    fn test_db_url() -> Option<String> {
        std::env::var("KRISHIV_TEST_DATABASE_URL").ok()
    }

    fn sample_schema() -> iceberg::spec::Schema {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        iceberg::spec::Schema::builder()
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
    #[ignore = "requires KRISHIV_TEST_DATABASE_URL"]
    async fn postgres_catalog_create_and_load() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        let ns = NamespaceIdent::new("sales".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let creation = TableCreation::builder()
            .name("orders".to_string())
            .schema(sample_schema())
            .build();
        let created = catalog.create_table(&ns, creation).await.unwrap();
        assert_eq!(created.identifier().name(), "orders");

        let ident = TableIdent::new(ns.clone(), "orders".to_string());
        let loaded = catalog.load_table(&ident).await.unwrap();
        assert!(
            loaded.metadata().current_schema().as_ref().field_id_by_name("id").is_some()
        );
        assert!(catalog.table_exists(&ident).await.unwrap());

        catalog.drop_table(&ident).await.unwrap();
        assert!(!catalog.table_exists(&ident).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires KRISHIV_TEST_DATABASE_URL"]
    async fn postgres_catalog_concurrent_commit_conflict() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();

        let c1 = PostgresCatalog::new(&url, &warehouse).await.unwrap();
        let c2 = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        let ns = NamespaceIdent::new("conflict_test".to_string());
        let _ = c1.create_namespace(&ns, HashMap::new()).await;
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(sample_schema())
            .build();
        c1.create_table(&ns, creation).await.unwrap();

        let ident = TableIdent::new(ns, "t".to_string());

        // Both catalogs load the same table at version 0.
        let t1 = c1.load_table(&ident).await.unwrap();
        let t2 = c2.load_table(&ident).await.unwrap();

        // c1 commits first — should succeed.
        let commit1 = TableCommit::builder()
            .ident(ident.clone())
            .updates(vec![])
            .requirements(vec![])
            .build();
        c1.update_table(commit1).await.expect("first commit should succeed");

        // c2 now tries to commit on stale version — should fail with conflict.
        let commit2 = TableCommit::builder()
            .ident(ident.clone())
            .updates(vec![])
            .requirements(vec![])
            .build();
        let result = c2.update_table(commit2).await;
        assert!(
            result.is_err(),
            "concurrent commit should fail with conflict error"
        );
    }
}
