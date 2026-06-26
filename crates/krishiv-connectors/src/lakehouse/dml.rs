//! Iceberg DML operations via copy-on-write (Phase J5).
//!
//! All mutations (DELETE, UPDATE, MERGE) use the **copy-on-write** strategy:
//! surviving rows are rewritten to new Parquet files, and the old files are
//! replaced atomically via drop+recreate (iceberg-rust 0.9.1 has no public
//! overwrite snapshot action).
//!
//! # Design note
//!
//! `iceberg-datafusion 0.9.1` depends on DataFusion 52.x while the workspace
//! uses DataFusion 53.x, so `IcebergStaticTableProvider` cannot be registered
//! with the workspace `SessionContext`. Instead we use iceberg's `plan_files()`
//! to get the underlying Parquet paths, then read them via
//! `SessionContext::read_parquet` (DataFusion 53.x + arrow 58.x native).
//! The transaction side only sees `DataFile` (path + size), never arrow types.
//!
//! # Operation summary
//!
//! | Operation | Strategy |
//! |-----------|----------|
//! | `DELETE WHERE predicate` | Scan → filter out matching rows → overwrite |
//! | `UPDATE SET … WHERE predicate` | Scan → apply column updates → overwrite |
//! | `MERGE INTO target USING source ON keys` | Full outer-join → upsert → overwrite |
//!
//! All three functions return `(rows_affected, new_snapshot_id)`.

use std::sync::Arc;

#[cfg(feature = "iceberg")]
use arrow::array::RecordBatch;
use bytes::Bytes;
use datafusion::datasource::MemTable;
use datafusion::execution::options::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableCreation, TableIdent};
use parquet::arrow::ArrowWriter;
use tokio::task;

use crate::lakehouse::LakehouseError;

// ── internal scan helper ──────────────────────────────────────────────────────

/// Read all Parquet files from an iceberg table snapshot using DataFusion 53's
/// native Parquet reader (avoids iceberg-datafusion version conflict).
async fn scan_iceberg_table(
    table: &iceberg::table::Table,
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>, LakehouseError> {
    let scan = table
        .scan()
        .build()
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let tasks: Vec<iceberg::scan::FileScanTask> = task_stream
        .try_collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    if tasks.is_empty() {
        return Ok(vec![]);
    }

    // Collect local file paths; strip file:// prefix for DataFusion.
    let file_paths: Vec<String> = tasks
        .iter()
        .map(|t| {
            let p = t.data_file_path();
            if let Some(local) = p.strip_prefix("file://") {
                local.to_string()
            } else {
                p.to_string()
            }
        })
        .collect();

    let df = ctx
        .read_parquet(file_paths, ParquetReadOptions::default())
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    df.collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))
}

// ── DELETE ────────────────────────────────────────────────────────────────────

/// Delete rows matching `predicate_sql` from an Iceberg table.
///
/// Returns `(rows_deleted, new_snapshot_id)`.
pub async fn iceberg_delete_where(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    predicate_sql: &str,
    ctx: &SessionContext,
) -> Result<(u64, i64), LakehouseError> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let all_batches = scan_iceberg_table(&table, ctx).await?;
    if all_batches.is_empty() {
        return Ok((0, -1)); // -1: no snapshot needed (table empty, no-op)
    }

    let total_rows: i64 = all_batches.iter().map(|b| b.num_rows() as i64).sum();
    let schema = all_batches.first().ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?.schema();

    // Register as MemTable to run SQL against it.
    let tmp_name = format!("__krishiv_dml_{}", uuid::Uuid::new_v4().simple());
    let mem = MemTable::try_new(schema, vec![all_batches])
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    ctx.register_table(&tmp_name, Arc::new(mem))
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let survive_sql = format!("SELECT * FROM \"{tmp_name}\" WHERE NOT ({predicate_sql})");
    let surviving_batches = ctx
        .sql(&survive_sql)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
        .collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let _ = ctx.deregister_table(&tmp_name);

    let surviving_rows: i64 = surviving_batches.iter().map(|b| b.num_rows() as i64).sum();
    let deleted = (total_rows - surviving_rows).max(0) as u64;

    let snapshot_id = overwrite_table_pub(catalog, table_ident, surviving_batches).await?;
    Ok((deleted, snapshot_id))
}

// ── UPDATE ────────────────────────────────────────────────────────────────────

/// Update columns for rows matching `predicate_sql`.
///
/// `set_expressions` is a list of `(column_name, sql_expression)` pairs.
/// Returns `(rows_updated, new_snapshot_id)`.
pub async fn iceberg_update_where(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    set_expressions: &[(&str, &str)],
    predicate_sql: Option<&str>,
    ctx: &SessionContext,
) -> Result<(u64, i64), LakehouseError> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let all_batches = scan_iceberg_table(&table, ctx).await?;
    if all_batches.is_empty() {
        return Ok((0, -1));
    }

    let schema = all_batches.first().ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?.schema();
    let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let tmp_name = format!("__krishiv_upd_{}", uuid::Uuid::new_v4().simple());
    let mem = MemTable::try_new(schema, vec![all_batches])
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    ctx.register_table(&tmp_name, Arc::new(mem))
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Build CASE WHEN per updated column.
    let select_cols: Vec<String> = field_names
        .iter()
        .map(|col_name| {
            if let Some((_, expr)) = set_expressions.iter().find(|(c, _)| c == col_name) {
                let pred = predicate_sql.unwrap_or("TRUE");
                format!(
                    "CASE WHEN ({pred}) THEN ({expr}) ELSE \"{col_name}\" END AS \"{col_name}\""
                )
            } else {
                format!("\"{col_name}\"")
            }
        })
        .collect();

    let pred_clause = predicate_sql.map_or("TRUE".to_string(), |p| p.to_string());
    let count_sql = format!("SELECT COUNT(*) FROM \"{tmp_name}\" WHERE {pred_clause}");
    let count_batches = ctx
        .sql(&count_sql)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
        .collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let updated = extract_count(&count_batches).max(0) as u64;

    let rewrite_sql = format!("SELECT {} FROM \"{}\"", select_cols.join(", "), tmp_name);
    let batches = ctx
        .sql(&rewrite_sql)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
        .collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let _ = ctx.deregister_table(&tmp_name);

    let snapshot_id = overwrite_table_pub(catalog, table_ident, batches).await?;
    Ok((updated, snapshot_id))
}

// ── MERGE ─────────────────────────────────────────────────────────────────────

/// Upsert `source_batches` into the target Iceberg table on `merge_keys`.
///
/// Returns `(rows_affected, new_snapshot_id)`.
pub async fn iceberg_merge_into(
    catalog: Arc<dyn Catalog + Send + Sync>,
    target_ident: &TableIdent,
    source_batches: Vec<RecordBatch>,
    merge_keys: &[&str],
    ctx: &SessionContext,
) -> Result<(u64, i64), LakehouseError> {
    if source_batches.is_empty() {
        return Ok((0, -1));
    }

    let table = catalog
        .load_table(target_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let target_batches = scan_iceberg_table(&table, ctx).await?;

    let target_name = format!("__krishiv_merge_tgt_{}", uuid::Uuid::new_v4().simple());
    if !target_batches.is_empty() {
        let schema = target_batches.first().ok_or_else(|| LakehouseError::Iceberg("empty target batches".to_string()))?.schema();
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let mem = MemTable::try_new(schema.clone(), vec![target_batches])
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        ctx.register_table(&target_name, Arc::new(mem))
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        let source_schema = source_batches.first().ok_or_else(|| LakehouseError::Iceberg("empty source batches".to_string()))?.schema();
        let source_mem = MemTable::try_new(source_schema, vec![source_batches])
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let source_name = format!("__krishiv_merge_src_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(&source_name, Arc::new(source_mem))
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        let join_cond = merge_keys
            .iter()
            .map(|k| format!("t.\"{k}\" = s.\"{k}\""))
            .collect::<Vec<_>>()
            .join(" AND ");

        let select_cols: Vec<String> = field_names
            .iter()
            .map(|col| format!("COALESCE(s.\"{col}\", t.\"{col}\") AS \"{col}\""))
            .collect();

        let merge_sql = format!(
            "SELECT {cols} FROM \"{target_name}\" t FULL OUTER JOIN \"{source_name}\" s ON {join_cond}",
            cols = select_cols.join(", "),
        );

        let merged_batches = ctx
            .sql(&merge_sql)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
            .collect()
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let rows_affected: u64 = merged_batches.iter().map(|b| b.num_rows() as u64).sum();

        let _ = ctx.deregister_table(&target_name);
        let _ = ctx.deregister_table(&source_name);

        let snapshot_id = overwrite_table_pub(catalog, target_ident, merged_batches).await?;
        Ok((rows_affected, snapshot_id))
    } else {
        // Empty target: just insert all source rows.
        let rows_affected: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();
        let snapshot_id = overwrite_table_pub(catalog, target_ident, source_batches).await?;
        Ok((rows_affected, snapshot_id))
    }
}

// ── shared overwrite helper ───────────────────────────────────────────────────

/// Write `batches` to a Parquet file then commit an Iceberg overwrite via
/// drop+recreate (iceberg-rust 0.9.1 has no public overwrite snapshot action).
///
/// Returns the new snapshot id, or -1 if the table was truncated to empty.
pub async fn overwrite_table_pub(
    catalog: Arc<dyn Catalog + Send + Sync>,
    ident: &TableIdent,
    batches: Vec<RecordBatch>,
) -> Result<i64, LakehouseError> {
    // Load current table to capture schema and location before any mutation.
    let table = catalog
        .load_table(ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let iceberg_schema = table.metadata().current_schema().clone();
    let table_location = table.metadata().location().to_string();
    let file_io = table.file_io().clone();

    let has_rows = batches.iter().any(|b| b.num_rows() > 0);

    if !has_rows {
        // Truncate: drop + recreate with no files.
        catalog
            .drop_table(ident)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let creation = TableCreation::builder()
            .name(ident.name().to_string())
            .schema((*iceberg_schema).clone())
            .location(table_location)
            .build();
        let new_table = catalog
            .create_table(ident.namespace(), creation)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        // -1 sentinel: iceberg-rust 0.9.1 returns no snapshot for empty tables;
        // callers must treat -1 as "no snapshot"
        return Ok(new_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(-1));
    }

    // Write surviving rows to a local Parquet file (blocking), then read bytes.
    let arrow_schema = batches.first().ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?.schema();
    let (file_bytes, file_size, record_count) = task::spawn_blocking({
        let arrow_schema = arrow_schema.clone();
        let batches = batches.clone();
        move || -> Result<(Vec<u8>, u64, u64), LakehouseError> {
            let tmp =
                tempfile::NamedTempFile::new().map_err(|e| LakehouseError::Io(e.to_string()))?;
            let tmp_path = tmp.path().to_path_buf();
            let file =
                std::fs::File::create(&tmp_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
            let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let mut row_count = 0u64;
            for batch in &batches {
                row_count += batch.num_rows() as u64;
                writer
                    .write(batch)
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
            }
            let inner = writer
                .into_inner()
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let size = inner
                .metadata()
                .map_err(|e| LakehouseError::Io(e.to_string()))?
                .len();
            drop(inner);

            let bytes = std::fs::read(&tmp_path).map_err(|e| LakehouseError::Io(e.to_string()))?;
            // tmp drops here, auto-deleting the temp file
            Ok((bytes, size, row_count))
        }
    })
    .await
    .map_err(|e| LakehouseError::Io(e.to_string()))??;

    // Upload via iceberg FileIO.
    let dest = format!(
        "{}/data/{}.parquet",
        table_location.trim_end_matches('/'),
        uuid::Uuid::new_v4()
    );
    let output = file_io
        .new_output(&dest)
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    output
        .write(Bytes::from(file_bytes))
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Build iceberg DataFile descriptor.
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(dest)
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(record_count)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Drop old table and recreate so the new snapshot references ONLY our file.
    catalog
        .drop_table(ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let create_result = catalog
        .create_table(
            ident.namespace(),
            TableCreation::builder()
                .name(ident.name().to_string())
                .schema((*iceberg_schema).clone())
                .location(table_location.clone())
                .build(),
        )
        .await;

    let new_table = match create_result {
        Ok(t) => t,
        Err(create_err) => {
            // Recreate failed — try to restore the original table so we don't leave it invisible
            // (best-effort: if this also fails, log and propagate the original error)
            if let Err(restore_err) = catalog
                .create_table(
                    ident.namespace(),
                    TableCreation::builder()
                        .name(ident.name().to_string())
                        .schema((*iceberg_schema).clone())
                        .location(table_location.clone())
                        .build(),
                )
                .await
            {
                tracing::error!(
                    table = %ident,
                    create_error = %create_err,
                    restore_error = %restore_err,
                    "CRITICAL: table is invisible after failed overwrite and restore attempt; manual intervention required"
                );
            }
            return Err(LakehouseError::Iceberg(create_err.to_string()));
        }
    };

    // Commit the single data file via fast_append.
    let tx = Transaction::new(&new_table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
    let tx = action
        .apply(tx)
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let committed = tx
        .commit(&*catalog)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let snapshot_id = committed
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .unwrap_or(-1);
    Ok(snapshot_id)
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn extract_count(batches: &[RecordBatch]) -> i64 {
    for batch in batches {
        if batch.num_rows() > 0 && batch.num_columns() > 0 {
            let col = batch.column(0);
            use arrow::array::Int64Array;
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                return arr.value(0);
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};
    use std::collections::HashMap;

    async fn make_catalog_with_table() -> (
        Arc<dyn Catalog + Send + Sync>,
        TableIdent,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "mem",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );

        let ns = NamespaceIdent::new("sales".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = iceberg::spec::Schema::builder()
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
            .unwrap();

        let ident = TableIdent::new(ns, "orders".to_string());
        catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();

        (catalog as Arc<dyn Catalog + Send + Sync>, ident, dir)
    }

    #[tokio::test]
    async fn delete_where_on_empty_table_returns_zero() {
        let (catalog, ident, _dir) = make_catalog_with_table().await;
        let ctx = SessionContext::new();
        let (deleted, _snap) = iceberg_delete_where(catalog, &ident, "id > 0", &ctx)
            .await
            .unwrap();
        assert_eq!(deleted, 0, "empty table: no rows to delete");
    }
}
