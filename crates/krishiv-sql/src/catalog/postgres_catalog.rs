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
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // `parent` used to be `_parent` — discarded, so listing the children of
        // `a` returned every namespace in the catalog.
        //
        // `starts_with` rather than LIKE on purpose: namespace names may
        // contain `_`, which LIKE treats as a single-character wildcard, so a
        // LIKE pattern would over-match sibling namespaces.
        //
        // The `None` case deliberately returns *all* namespaces flattened
        // rather than only top-level ones: this catalog is surfaced through
        // DataFusion, whose schema space is flat, so a nested `a.b` has to be
        // visible as its own schema or it cannot be queried at all.
        let rows = match parent {
            Some(parent) => {
                let prefix = ns_key(parent);
                sqlx::query_scalar::<_, String>(
                    "SELECT namespace_name FROM krishiv_namespaces
                      WHERE starts_with(namespace_name, $1 || '.')
                        AND strpos(substr(namespace_name, length($1) + 2), '.') = 0
                      ORDER BY namespace_name",
                )
                .bind(&prefix)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query_scalar::<_, String>(
                    "SELECT namespace_name FROM krishiv_namespaces ORDER BY namespace_name",
                )
                .fetch_all(&self.pool)
                .await
            }
        }
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
        // `DO NOTHING` keeps this idempotent, which callers rely on
        // (`KrishivCatalog::create_table` and `LocalCatalog::recover_from_disk`
        // both create namespaces speculatively). But it also means an existing
        // row keeps its *old* properties — so returning `properties` here
        // reported the caller's values as though they had been applied. The
        // `RETURNING` clause yields a row only when the insert actually
        // happened; when it did not, read back what is really stored.
        let inserted: Option<serde_json::Value> = sqlx::query_scalar(
            "INSERT INTO krishiv_namespaces (namespace_name, properties)
             VALUES ($1, $2)
             ON CONFLICT (namespace_name) DO NOTHING
             RETURNING properties",
        )
        .bind(&name)
        .bind(&props)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("create_namespace: {e}")))?;

        let stored = match inserted {
            Some(value) => value,
            None => sqlx::query_scalar(
                "SELECT properties FROM krishiv_namespaces WHERE namespace_name = $1",
            )
            .bind(&name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("create_namespace read-back: {e}")))?,
        };

        let stored: HashMap<String, String> = serde_json::from_value(stored)
            .map_err(|e| iceberg_err(format!("deserialize properties: {e}")))?;
        Ok(Namespace::with_properties(namespace.clone(), stored))
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
        // `krishiv_tables` has no foreign key onto `krishiv_namespaces`, so a
        // bare DELETE left every table row in place: the namespace vanished
        // from `list_namespaces` while `list_tables` and `load_table` kept
        // serving its tables. Refuse a non-empty namespace, as the Iceberg
        // contract requires, rather than orphaning them.
        let dropped = sqlx::query(
            "DELETE FROM krishiv_namespaces
              WHERE namespace_name = $1
                AND NOT EXISTS (SELECT 1 FROM krishiv_tables WHERE namespace = $1)",
        )
        .bind(&name)
        .execute(&self.pool)
        .await
        .map_err(|e| iceberg_err(format!("drop_namespace: {e}")))?
        .rows_affected();

        if dropped == 0 {
            // Distinguish "no such namespace" from "namespace not empty".
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM krishiv_namespaces WHERE namespace_name = $1)",
            )
            .bind(&name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| iceberg_err(format!("drop_namespace check: {e}")))?;
            return Err(iceberg_err(if exists {
                format!("namespace not empty: {name}")
            } else {
                format!("namespace not found: {name}")
            }));
        }
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
        let metadata_location =
            MetadataLocation::new_with_metadata(&location, &metadata).to_string();

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

        // The runtime is captured HERE, at load time, not at catalog build:
        // the runtime that loads a table is the runtime that will scan it. A
        // build-time capture would hand every table a handle to whatever
        // runtime happened to exist at startup — the exact shape the
        // empty-plan tripwire in krishiv-connectors exists to catch.
        Table::builder()
            .metadata(metadata)
            .metadata_location(metadata_location)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            .runtime(iceberg::Runtime::try_current().map_err(|e| iceberg_err(e.to_string()))?)
            .build()
            .map_err(|e| iceberg_err(e.to_string()))
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let ns = ns_key(table.namespace());
        let dropped =
            sqlx::query("DELETE FROM krishiv_tables WHERE namespace = $1 AND table_name = $2")
                .bind(&ns)
                .bind(table.name())
                .execute(&self.pool)
                .await
                .map_err(|e| iceberg_err(format!("drop_table: {e}")))?
                .rows_affected();
        if dropped == 0 {
            // A DELETE that matches nothing is a successful statement, not a
            // successful drop. Returning Ok here told the caller a table it
            // never had was gone.
            return Err(iceberg_err(format!(
                "table not found: {}.{}",
                ns,
                table.name()
            )));
        }
        Ok(())
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        // Load BEFORE dropping: the catalog row is the only pointer to the
        // metadata, and files can only be found through the loaded table.
        let loaded = self.load_table(table).await?;
        self.drop_table(table).await?;
        // Every table this catalog creates owns its directory (the location
        // is minted under the warehouse per table), so removing the location
        // prefix is exactly the table's own data + metadata and nothing else.
        loaded
            .file_io()
            .delete_prefix(loaded.metadata().location())
            .await
            .map_err(|e| iceberg_err(format!("purge_table file cleanup: {e}")))?;
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
        let renamed = sqlx::query(
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
        .map_err(|e| iceberg_err(format!("rename_table: {e}")))?
        .rows_affected();
        if renamed == 0 {
            // Renaming a table that does not exist matched no rows and
            // reported success. (A rename onto an *existing* destination is
            // already caught: it violates the primary key and surfaces as an
            // error from the statement above.)
            return Err(iceberg_err(format!(
                "table not found: {}.{}",
                src_ns,
                src.name()
            )));
        }
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
    async fn postgres_catalog_purge_deletes_the_files_drop_leaves_behind() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        let ns = NamespaceIdent::new("purge_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let creation = TableCreation::builder()
            .name("victims".to_string())
            .schema(sample_schema())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();
        let ident = TableIdent::new(ns.clone(), "victims".to_string());

        // Creation wrote at least metadata.json under the table's directory.
        let table_dir = dir.path().join("purge_ns").join("victims");
        let files_before = walkdir_count(&table_dir);
        assert!(files_before > 0, "creation must have written metadata");

        catalog.purge_table(&ident).await.unwrap();
        assert!(!catalog.table_exists(&ident).await.unwrap());
        assert_eq!(
            walkdir_count(&table_dir),
            0,
            "purge must remove the table's files, not just the catalog row"
        );
    }

    fn walkdir_count(dir: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(|e| e.ok())
            .map(|e| {
                let p = e.path();
                if p.is_dir() { walkdir_count(&p) } else { 1 }
            })
            .sum()
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

    /// Every assertion below covers a case where this backend used to report
    /// success for work it had not done — a DELETE or UPDATE that matched no
    /// rows is a successful *statement*, not a successful operation.
    #[tokio::test]
    #[ignore = "requires KRISHIV_TEST_DATABASE_URL"]
    async fn absent_targets_are_errors_not_silent_successes() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        let ns = NamespaceIdent::new("absent_targets".to_string());
        let missing = TableIdent::new(ns.clone(), "never_created".to_string());

        assert!(
            catalog.drop_table(&missing).await.is_err(),
            "dropping a table that does not exist must not report success"
        );
        assert!(
            catalog
                .rename_table(&missing, &TableIdent::new(ns.clone(), "dest".to_string()))
                .await
                .is_err(),
            "renaming a table that does not exist must not report success"
        );
        assert!(
            catalog
                .drop_namespace(&NamespaceIdent::new("no_such_namespace".to_string()))
                .await
                .is_err(),
            "dropping a namespace that does not exist must not report success"
        );
    }

    /// `krishiv_tables` has no foreign key onto `krishiv_namespaces`, so a bare
    /// DELETE orphaned every table in the namespace: gone from
    /// `list_namespaces`, still served by `list_tables` and `load_table`.
    #[tokio::test]
    #[ignore = "requires KRISHIV_TEST_DATABASE_URL"]
    async fn drop_namespace_refuses_to_orphan_tables() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        let ns = NamespaceIdent::new("orphan_check".to_string());
        let ident = TableIdent::new(ns.clone(), "t".to_string());
        let _ = catalog.drop_table(&ident).await;
        let _ = catalog.drop_namespace(&ns).await;

        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(sample_schema())
                    .build(),
            )
            .await
            .unwrap();

        let err = catalog
            .drop_namespace(&ns)
            .await
            .expect_err("a non-empty namespace must not be droppable");
        assert!(
            err.to_string().contains("not empty"),
            "expected a not-empty error, got: {err}"
        );
        assert!(
            catalog.table_exists(&ident).await.unwrap(),
            "the refused drop must leave the table reachable"
        );

        catalog.drop_table(&ident).await.unwrap();
        catalog
            .drop_namespace(&ns)
            .await
            .expect("an emptied namespace drops cleanly");
    }

    /// `list_namespaces` took a `parent` argument and discarded it, so asking
    /// for the children of `a` returned every namespace in the catalog.
    #[tokio::test]
    #[ignore = "requires KRISHIV_TEST_DATABASE_URL"]
    async fn list_namespaces_honours_its_parent_argument() {
        let url = test_db_url().expect("KRISHIV_TEST_DATABASE_URL not set");
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = PostgresCatalog::new(&url, &warehouse).await.unwrap();

        // `parent_probe_x` is a sibling whose name shares the `parent_probe`
        // prefix — it must NOT be reported as a child, which a naive
        // `starts_with(name, parent)` would get wrong. `_` is also a LIKE
        // wildcard, which is why the query avoids LIKE.
        for name in [
            "parent_probe",
            "parent_probe.child_a",
            "parent_probe.child_b",
            "parent_probe.child_a.grandchild",
            "parent_probe_x",
        ] {
            let _ = catalog
                .create_namespace(
                    &NamespaceIdent::from_vec(name.split('.').map(str::to_string).collect())
                        .unwrap(),
                    HashMap::new(),
                )
                .await;
        }

        let parent = NamespaceIdent::new("parent_probe".to_string());
        let mut children: Vec<String> = catalog
            .list_namespaces(Some(&parent))
            .await
            .unwrap()
            .into_iter()
            .map(|n| n.inner().join("."))
            .collect();
        children.sort();
        assert_eq!(
            children,
            vec!["parent_probe.child_a", "parent_probe.child_b"],
            "only immediate children — not grandchildren, not prefix-sharing siblings"
        );
    }
}
