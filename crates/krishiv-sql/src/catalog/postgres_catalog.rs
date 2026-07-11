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

// Feature-gated at the module declaration in `catalog/mod.rs`
// (`#[cfg(feature = "postgres-catalog")]`).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;
use iceberg::{
    Catalog, MetadataLocation, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
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
    ///
    /// Runs inside a transaction holding an advisory lock: `CREATE TABLE IF
    /// NOT EXISTS` is not concurrency-safe in Postgres (two sessions creating
    /// the same table race on the `pg_type` catalog and one fails with a
    /// `pg_type_typname_nsp_index` duplicate-key error), so two engine nodes
    /// booting against the same catalog database must serialize here.
    pub async fn migrate(&self) -> Result<(), CatalogError> {
        /// Arbitrary constant identifying "krishiv catalog migration"
        /// (ASCII "krishiv" as an integer).
        const MIGRATION_LOCK_KEY: i64 = 0x006b_7269_7368_6976;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(migrate_err("migrate begin"))?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(migrate_err("migrate lock"))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS krishiv_namespaces (
                 namespace_name TEXT PRIMARY KEY,
                 properties     JSONB NOT NULL DEFAULT '{}'
             )",
        )
        .execute(&mut *tx)
        .await
        .map_err(migrate_err("migrate namespaces"))?;

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
        .execute(&mut *tx)
        .await
        .map_err(migrate_err("migrate tables"))?;

        tx.commit().await.map_err(migrate_err("migrate commit"))?;
        Ok(())
    }

    /// Default table location URI for `{namespace}/{table_name}`.
    fn table_location(&self, namespace: &NamespaceIdent, table_name: &str) -> String {
        let ns = namespace.as_ref().join("/");
        // Strip trailing slash from warehouse for clean joins.
        let base = self.warehouse.trim_end_matches('/');
        format!("{base}/{ns}/{table_name}")
    }
}

/// Dotted namespace key used as the Postgres primary-key component.
fn ns_key(namespace: &NamespaceIdent) -> String {
    namespace.as_ref().join(".")
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
        let name = ns_key(namespace);
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
        let name = ns_key(namespace);
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
        let name = ns_key(namespace);
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
        let name = ns_key(namespace);
        let props = serde_json::to_value(&properties)
            .map_err(|e| iceberg_err(format!("serialize: {e}")))?;
        sqlx::query("UPDATE krishiv_namespaces SET properties = $2 WHERE namespace_name = $1")
            .bind(&name)
            .bind(&props)
            .execute(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("update_namespace: {e}")))?;
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let name = ns_key(namespace);
        sqlx::query("DELETE FROM krishiv_namespaces WHERE namespace_name = $1")
            .bind(&name)
            .execute(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("drop_namespace: {e}")))?;
        Ok(())
    }

    // ── Tables ────────────────────────────────────────────────────────────────

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let ns = ns_key(namespace);
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
        let ns = ns_key(namespace);
        let table_name = creation.name.clone();
        let location = creation
            .location
            .clone()
            .unwrap_or_else(|| self.table_location(namespace, &table_name));

        // Build initial Iceberg table metadata. `from_table_creation` rejects
        // a creation without a location, so inject the computed default.
        let mut creation = creation;
        creation.location = Some(location.clone());
        let metadata = TableMetadataBuilder::from_table_creation(creation)
            .map_err(|e| iceberg_err(e.to_string()))?
            .build()
            .map_err(|e| iceberg_err(e.to_string()))?
            .metadata;

        // Serialise and write metadata.json to the warehouse.
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .map_err(|e| iceberg_err(format!("serialize metadata: {e}")))?;
        let metadata_location = MetadataLocation::new_with_table_location(&location).to_string();

        self.file_io
            .new_output(&metadata_location)
            .map_err(|e| iceberg_err(e.to_string()))?
            .write(Bytes::from(metadata_json))
            .await
            .map_err(|e| iceberg_err(format!("write metadata: {e}")))?;

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
        let ns = ns_key(table.namespace());
        let metadata_location: Option<String> = sqlx::query_scalar(
            "SELECT metadata_location FROM krishiv_tables
              WHERE namespace = $1 AND table_name = $2",
        )
        .bind(&ns)
        .bind(table.name())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("load_table query: {e}")))?;

        let metadata_location = metadata_location
            .ok_or_else(|| iceberg_err(format!("table not found: {}", table.name())))?;

        // Read the metadata JSON from the warehouse.
        let bytes = self
            .file_io
            .new_input(&metadata_location)
            .map_err(|e| iceberg_err(e.to_string()))?
            .read()
            .await
            .map_err(|e| iceberg_err(format!("read metadata: {e}")))?;

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
        let ns = ns_key(table.namespace());
        sqlx::query("DELETE FROM krishiv_tables WHERE namespace = $1 AND table_name = $2")
            .bind(&ns)
            .bind(table.name())
            .execute(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("drop_table: {e}")))?;
        Ok(())
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let ns = ns_key(table.namespace());
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
        let src_ns = ns_key(src.namespace());
        let dest_ns = ns_key(dest.namespace());
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
        let ns = ns_key(table.namespace());
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
        let ns = ns_key(ident.namespace());

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

        let current_location = current_location
            .ok_or_else(|| iceberg_err(format!("table not found: {}", ident.name())))?;

        // Load the current table, then let the commit validate its
        // requirements and apply its updates; `TableCommit::apply` also
        // computes the next versioned metadata location.
        let table = self.load_table(&ident).await?;
        let updated = commit.apply(table)?;
        let new_location = updated
            .metadata_location()
            .ok_or_else(|| iceberg_err("updated table has no metadata location"))?
            .to_string();

        // Write new metadata.json.
        let new_metadata_json = serde_json::to_string_pretty(updated.metadata())
            .map_err(|e| iceberg_err(format!("serialize: {e}")))?;
        self.file_io
            .new_output(&new_location)
            .map_err(|e| iceberg_err(e.to_string()))?
            .write(Bytes::from(new_metadata_json))
            .await
            .map_err(|e| iceberg_err(format!("write: {e}")))?;

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

fn migrate_err(op: &'static str) -> impl Fn(sqlx::Error) -> CatalogError {
    move |e| CatalogError::Transport {
        operation: op.into(),
        message: e.to_string(),
    }
}

fn build_file_io(warehouse: &str) -> Result<FileIO, CatalogError> {
    // KrishivStorage dispatches `file://`/bare paths and `s3://`/`s3a://`
    // (env-configured object_store); other schemes are not wired up.
    if ["abfs://", "abfss://", "gs://", "gcs://"]
        .iter()
        .any(|scheme| warehouse.starts_with(scheme))
    {
        return Err(CatalogError::Iceberg(format!(
            "unsupported warehouse scheme for the postgres catalog: {warehouse} \
             (supported: file://, s3://)"
        )));
    }
    Ok(FileIOBuilder::new(Arc::new(
        crate::catalog::object_store_io::KrishivStorageFactory,
    ))
    .build())
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
            loaded
                .metadata()
                .current_schema()
                .as_ref()
                .field_id_by_name("id")
                .is_some()
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
        // The catalog database persists across runs — clear any leftover row.
        let stale = TableIdent::new(ns.clone(), "t".to_string());
        let _ = c1.drop_table(&stale).await;
        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(sample_schema())
            .build();
        c1.create_table(&ns, creation).await.unwrap();

        let ident = TableIdent::new(ns, "t".to_string());

        // Both catalogs load the same table at version 0. (`TableCommit` is
        // no longer publicly constructible — commits go through
        // `Transaction`, which drives `Catalog::update_table` internally.)
        use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
        let t1 = c1.load_table(&ident).await.unwrap();
        let t2 = c2.load_table(&ident).await.unwrap();

        // c1 commits first — should succeed.
        let tx1 = Transaction::new(&t1);
        let tx1 = tx1
            .update_table_properties()
            .set("writer-c1".to_string(), "yes".to_string())
            .apply(tx1)
            .unwrap();
        tx1.commit(&c1).await.expect("first commit should succeed");

        // c2 commits from its stale snapshot. The catalog's CAS pointer
        // update rejects the stale attempt; `Transaction::commit` then
        // retries against refreshed metadata and re-applies the action on
        // top of c1's commit. The property under test is **no lost update**:
        // c1's change must survive c2's retried commit. (A broken CAS would
        // let c2's stale metadata clobber c1's.)
        let tx2 = Transaction::new(&t2);
        let tx2 = tx2
            .update_table_properties()
            .set("writer-c2".to_string(), "yes".to_string())
            .apply(tx2)
            .unwrap();
        tx2.commit(&c2)
            .await
            .expect("retried commit should succeed on refreshed metadata");

        let final_table = c1.load_table(&ident).await.unwrap();
        let props = final_table.metadata().properties();
        assert_eq!(
            props.get("writer-c1").map(String::as_str),
            Some("yes"),
            "c1's committed change was lost to c2's stale commit — CAS conflict handling is broken"
        );
        assert_eq!(
            props.get("writer-c2").map(String::as_str),
            Some("yes"),
            "c2's retried commit did not apply"
        );
    }
}
