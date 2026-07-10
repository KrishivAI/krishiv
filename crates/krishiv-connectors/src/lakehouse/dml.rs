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
use krishiv_common::sql_util::quote_identifier;

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
    let schema = all_batches
        .first()
        .ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?
        .schema();

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

    let schema = all_batches
        .first()
        .ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?
        .schema();
    let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let tmp_name = format!("__krishiv_upd_{}", uuid::Uuid::new_v4().simple());
    let mem = MemTable::try_new(schema, vec![all_batches])
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    ctx.register_table(&tmp_name, Arc::new(mem))
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Build CASE WHEN per updated column. Use quote_identifier so column names
    // with special characters (including embedded '"') are properly escaped.
    let select_cols: Vec<String> = field_names
        .iter()
        .map(|col_name| {
            let qc = quote_identifier(col_name);
            if let Some((_, expr)) = set_expressions.iter().find(|(c, _)| c == col_name) {
                let pred = predicate_sql.unwrap_or("TRUE");
                format!("CASE WHEN ({pred}) THEN ({expr}) ELSE {qc} END AS {qc}")
            } else {
                qc
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
        let schema = target_batches
            .first()
            .ok_or_else(|| LakehouseError::Iceberg("empty target batches".to_string()))?
            .schema();
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let mem = MemTable::try_new(schema.clone(), vec![target_batches])
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        ctx.register_table(&target_name, Arc::new(mem))
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        let source_schema = source_batches
            .first()
            .ok_or_else(|| LakehouseError::Iceberg("empty source batches".to_string()))?
            .schema();
        let source_mem = MemTable::try_new(source_schema, vec![source_batches])
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let source_name = format!("__krishiv_merge_src_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(&source_name, Arc::new(source_mem))
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        let join_cond = merge_keys
            .iter()
            .map(|k| {
                let qk = quote_identifier(k);
                format!("t.{qk} = s.{qk}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        let select_cols: Vec<String> = field_names
            .iter()
            .map(|col| {
                let qc = quote_identifier(col);
                format!("COALESCE(s.{qc}, t.{qc}) AS {qc}")
            })
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
    let arrow_schema = batches
        .first()
        .ok_or_else(|| LakehouseError::Iceberg("empty batches".to_string()))?
        .schema();
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

    // CONN-4: Update the version-hint after commit so DML changes survive
    // restart. Without this, the hint still points at the old (dropped)
    // table's metadata and all DML changes are silently rolled back on restart.
    if let Some(loc) = committed.metadata_location() {
        let table_root = std::path::Path::new(table_location.trim_start_matches("file://"));
        if let Err(e) = super::iceberg_native::native::write_version_hint(table_root, loc) {
            tracing::warn!(
                table = %ident,
                location = loc,
                error = %e,
                "version hint update failed after DML commit; hint may be stale"
            );
        }
    }

    let snapshot_id = committed
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .unwrap_or(-1);
    Ok(snapshot_id)
}

// ── CTAS landing (durable CREATE [OR REPLACE] TABLE … AS SELECT) ─────────────

/// Outcome of a durable CTAS landing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CtasLandingReport {
    /// Rows written into the new snapshot.
    pub rows: u64,
    /// Total bytes of the Parquet data files written.
    pub bytes: u64,
    /// Number of data files in the new snapshot.
    pub data_files: usize,
    /// New snapshot id, or -1 when the table is empty (no snapshot).
    pub snapshot_id: i64,
}

/// Default per-data-file roll threshold for CTAS landing, measured against
/// the *in-memory* Arrow size of the buffered batches (Parquet output is
/// typically 2-4× smaller). Override via `KRISHIV_CTAS_TARGET_FILE_BYTES`.
const CTAS_TARGET_FILE_BYTES_DEFAULT: usize = 512 * 1024 * 1024;

fn ctas_target_file_bytes() -> usize {
    std::env::var("KRISHIV_CTAS_TARGET_FILE_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(CTAS_TARGET_FILE_BYTES_DEFAULT)
}

/// Convert a workspace-arrow schema to an Iceberg schema with fresh field ids.
///
/// Hand-rolled (inverse of the read-side map in
/// `krishiv-sql/src/catalog/iceberg_table_provider.rs`) because iceberg-rust
/// 0.9.1 pins arrow 57 while the workspace is on arrow 58, so
/// `iceberg::arrow::arrow_schema_to_schema` cannot accept our types. Flat
/// primitive columns only — nested/list/struct results must be flattened in
/// SQL before a durable CTAS.
pub fn arrow_schema_to_iceberg_schema(
    schema: &arrow::datatypes::Schema,
) -> Result<iceberg::spec::Schema, LakehouseError> {
    use arrow::datatypes::{DataType, TimeUnit};
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    let mut fields: Vec<Arc<NestedField>> = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let prim = match field.data_type() {
            DataType::Boolean => PrimitiveType::Boolean,
            DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
            DataType::Int64 => PrimitiveType::Long,
            // Unsigned 8/16/32 fit losslessly in a signed long.
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => PrimitiveType::Long,
            DataType::Float32 => PrimitiveType::Float,
            DataType::Float64 => PrimitiveType::Double,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PrimitiveType::String,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                PrimitiveType::Binary
            }
            DataType::Date32 | DataType::Date64 => PrimitiveType::Date,
            DataType::Time64(TimeUnit::Microsecond) => PrimitiveType::Time,
            DataType::Timestamp(_, None) => PrimitiveType::Timestamp,
            DataType::Timestamp(_, Some(_)) => PrimitiveType::Timestamptz,
            DataType::Decimal128(precision, scale) if *scale >= 0 && *precision <= 38 => {
                PrimitiveType::Decimal {
                    precision: u32::from(*precision),
                    scale: *scale as u32,
                }
            }
            other => {
                return Err(LakehouseError::Iceberg(format!(
                    "durable CTAS cannot map result column '{}' of type {other} to an \
                     Iceberg type; cast or flatten it in the SELECT",
                    field.name()
                )));
            }
        };
        let iceberg_field = if field.is_nullable() {
            NestedField::optional(idx as i32 + 1, field.name(), Type::Primitive(prim))
        } else {
            NestedField::required(idx as i32 + 1, field.name(), Type::Primitive(prim))
        };
        fields.push(Arc::new(iceberg_field));
    }
    iceberg::spec::Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))
}

/// Normalize a batch so its physical Parquet encoding matches the Iceberg
/// schema produced by [`arrow_schema_to_iceberg_schema`]: timestamps become
/// microsecond precision (Iceberg v2 has no other unit) and Date64 becomes
/// Date32. Other columns pass through untouched.
fn normalize_batch_for_iceberg(batch: &RecordBatch) -> Result<RecordBatch, LakehouseError> {
    use arrow::datatypes::{DataType, Field, TimeUnit};

    let needs_cast = |dt: &DataType| {
        matches!(
            dt,
            DataType::Timestamp(unit, _) if *unit != TimeUnit::Microsecond
        ) || matches!(dt, DataType::Date64)
    };
    if !batch.schema().fields().iter().any(|f| needs_cast(f.data_type())) {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let target = match field.data_type() {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
            DataType::Date64 => Some(DataType::Date32),
            _ => None,
        };
        match target {
            Some(target) => {
                let cast = arrow::compute::cast(column, &target)
                    .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
                fields.push(Arc::new(Field::new(
                    field.name(),
                    target,
                    field.is_nullable(),
                )));
                columns.push(cast);
            }
            None => {
                fields.push(Arc::clone(field));
                columns.push(Arc::clone(column));
            }
        }
    }
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))
}

/// Write one Parquet part file from buffered batches and upload it via the
/// table's FileIO. Returns the Iceberg `DataFile` descriptor.
async fn write_ctas_part(
    file_io: &iceberg::io::FileIO,
    table_location: &str,
    batches: Vec<RecordBatch>,
) -> Result<iceberg::spec::DataFile, LakehouseError> {
    let arrow_schema = batches
        .first()
        .ok_or_else(|| LakehouseError::Iceberg("empty part".to_string()))?
        .schema();
    let (file_bytes, file_size, record_count) =
        task::spawn_blocking(move || -> Result<(Vec<u8>, u64, u64), LakehouseError> {
            let tmp =
                tempfile::NamedTempFile::new().map_err(|e| LakehouseError::Io(e.to_string()))?;
            let file = std::fs::File::create(tmp.path())
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let mut rows = 0u64;
            for batch in &batches {
                rows += batch.num_rows() as u64;
                writer
                    .write(batch)
                    .map_err(|e| LakehouseError::Io(e.to_string()))?;
            }
            writer
                .close()
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let bytes =
                std::fs::read(tmp.path()).map_err(|e| LakehouseError::Io(e.to_string()))?;
            let size = bytes.len() as u64;
            Ok((bytes, size, rows))
        })
        .await
        .map_err(|e| LakehouseError::Io(e.to_string()))??;

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

    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(dest)
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(record_count)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))
}

/// Durably land a `CREATE [OR REPLACE] TABLE … AS SELECT` result stream in an
/// Iceberg table with bounded memory.
///
/// The result stream is consumed incrementally into rolling Parquet part
/// files (roll threshold [`ctas_target_file_bytes`], measured on in-memory
/// Arrow size), each uploaded through the table's FileIO before the next
/// part buffers — peak memory is one part, independent of result size. This
/// is the engine-side fix for pipeline batch refreshes that previously
/// streamed the whole result out over Flight SQL and back (gap G17).
///
/// Replace semantics are drop+recreate (iceberg-rust 0.9.1 has no public
/// overwrite snapshot action), matching [`overwrite_table_pub`]:
///
/// 1. All part files are written and durable *before* the old table is
///    dropped — the destructive window covers metadata operations only.
/// 2. If the recreate fails, a restore of the old table is attempted; if
///    that fails too, a CRITICAL log directs manual intervention.
///
/// For a replace the parts are written under the existing table's location
/// (fresh UUID names cannot collide); for a new table it is created first so
/// the catalog assigns its location. The final `fast_append` commit makes
/// the new snapshot visible atomically.
pub async fn land_ctas(
    catalog: Arc<dyn Catalog + Send + Sync>,
    ident: &TableIdent,
    or_replace: bool,
    stream: datafusion::execution::SendableRecordBatchStream,
) -> Result<CtasLandingReport, LakehouseError> {
    land_ctas_with_target(catalog, ident, or_replace, stream, ctas_target_file_bytes()).await
}

/// [`land_ctas`] with an explicit per-part roll threshold (bytes of buffered
/// in-memory Arrow data). Exposed for callers and tests that need to control
/// file sizing directly instead of via `KRISHIV_CTAS_TARGET_FILE_BYTES`.
pub async fn land_ctas_with_target(
    catalog: Arc<dyn Catalog + Send + Sync>,
    ident: &TableIdent,
    or_replace: bool,
    mut stream: datafusion::execution::SendableRecordBatchStream,
    target_bytes: usize,
) -> Result<CtasLandingReport, LakehouseError> {
    use futures::StreamExt as _;

    // Namespace first: some catalogs error (rather than answer false) on
    // existence probes inside a namespace they have never seen.
    let ns = ident.namespace();
    let ns_exists = catalog
        .namespace_exists(ns)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    if !ns_exists {
        // Tolerate a concurrent create racing us.
        if let Err(e) = catalog.create_namespace(ns, Default::default()).await
            && !catalog
                .namespace_exists(ns)
                .await
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
        {
            return Err(LakehouseError::Iceberg(e.to_string()));
        }
    }

    let exists = catalog
        .table_exists(ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    if exists && !or_replace {
        return Err(LakehouseError::Iceberg(format!(
            "table {ident} already exists; use CREATE OR REPLACE TABLE to replace it"
        )));
    }

    let iceberg_schema = arrow_schema_to_iceberg_schema(stream.schema().as_ref())?;

    // Where the part files go, and the FileIO to write them with. For a
    // replace we write under the existing location before touching metadata;
    // for a new table we create it first so the catalog assigns a location.
    // The old snapshot's data files are collected up front: drop+recreate is
    // metadata-only, so without explicit cleanup every replace would orphan
    // the previous snapshot's Parquet in the object store (a 15-minute batch
    // refresh of a multi-GB table fills a small warehouse within hours).
    let mut replaced_files: Vec<String> = Vec::new();
    let (table_location, file_io, created_fresh) = if exists {
        let old = catalog
            .load_table(ident)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        if old.metadata().current_snapshot().is_some() {
            match old.scan().build() {
                Ok(scan) => match scan.plan_files().await {
                    Ok(stream) => {
                        let tasks: Vec<iceberg::scan::FileScanTask> =
                            stream.try_collect().await.unwrap_or_default();
                        replaced_files = tasks
                            .iter()
                            .map(|t| t.data_file_path().to_string())
                            .collect();
                    }
                    Err(e) => {
                        tracing::warn!(table = %ident, error = %e,
                            "cannot enumerate replaced data files; they will be orphaned");
                    }
                },
                Err(e) => {
                    tracing::warn!(table = %ident, error = %e,
                        "cannot plan replaced table scan; old data files will be orphaned");
                }
            }
        }
        (
            old.metadata().location().to_string(),
            old.file_io().clone(),
            false,
        )
    } else {
        let table = catalog
            .create_table(
                ident.namespace(),
                TableCreation::builder()
                    .name(ident.name().to_string())
                    .schema(iceberg_schema.clone())
                    .build(),
            )
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        (
            table.metadata().location().to_string(),
            table.file_io().clone(),
            true,
        )
    };

    // Consume the stream into rolling part files.
    let mut buffered: Vec<RecordBatch> = Vec::new();
    let mut buffered_bytes = 0usize;
    let mut data_files: Vec<iceberg::spec::DataFile> = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    while let Some(next) = stream.next().await {
        let batch = next.map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = normalize_batch_for_iceberg(&batch)?;
        total_rows += batch.num_rows() as u64;
        buffered_bytes += batch.get_array_memory_size();
        buffered.push(batch);
        if buffered_bytes >= target_bytes {
            let part =
                write_ctas_part(&file_io, &table_location, std::mem::take(&mut buffered)).await?;
            buffered_bytes = 0;
            total_bytes += part.file_size_in_bytes();
            data_files.push(part);
        }
    }
    if !buffered.is_empty() {
        let part = write_ctas_part(&file_io, &table_location, std::mem::take(&mut buffered)).await?;
        total_bytes += part.file_size_in_bytes();
        data_files.push(part);
    }

    // Metadata swap: for a replace, drop + recreate at the same location so
    // the new snapshot references only our files (and picks up the new
    // schema). All data files above are already durable.
    let table = if created_fresh {
        catalog
            .load_table(ident)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
    } else {
        catalog
            .drop_table(ident)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let creation = || {
            TableCreation::builder()
                .name(ident.name().to_string())
                .schema(iceberg_schema.clone())
                .location(table_location.clone())
                .build()
        };
        match catalog.create_table(ident.namespace(), creation()).await {
            Ok(t) => t,
            Err(create_err) => {
                if let Err(restore_err) =
                    catalog.create_table(ident.namespace(), creation()).await
                {
                    tracing::error!(
                        table = %ident,
                        create_error = %create_err,
                        restore_error = %restore_err,
                        "CRITICAL: table is invisible after failed CTAS replace and \
                         restore attempt; manual intervention required"
                    );
                }
                return Err(LakehouseError::Iceberg(create_err.to_string()));
            }
        }
    };

    let files_count = data_files.len();
    let snapshot_id = if data_files.is_empty() {
        // Empty result: the (re)created table with no snapshot is the answer.
        -1
    } else {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let committed = tx
            .commit(&*catalog)
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        // Local-FS native tables track the current metadata via a version
        // hint; object-store tables (REST catalog) do not use one.
        if table_location.starts_with("file://")
            && let Some(loc) = committed.metadata_location()
        {
            let table_root = std::path::Path::new(table_location.trim_start_matches("file://"));
            if let Err(e) = super::iceberg_native::native::write_version_hint(table_root, loc) {
                tracing::warn!(
                    table = %ident,
                    location = loc,
                    error = %e,
                    "version hint update failed after CTAS commit; hint may be stale"
                );
            }
        }
        committed
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(-1)
    };

    // The new snapshot is committed and visible: remove the replaced
    // snapshot's data files (best-effort — a failed delete only leaves an
    // orphan, never corrupts the new table). The list was captured from the
    // old snapshot before any new part existed, so it cannot name new files.
    if !replaced_files.is_empty() {
        let mut removed = 0usize;
        for path in &replaced_files {
            match file_io.delete(path).await {
                Ok(()) => removed += 1,
                Err(e) => {
                    tracing::warn!(table = %ident, path, error = %e,
                        "failed to delete replaced data file (orphaned)");
                }
            }
        }
        tracing::info!(table = %ident, removed, total = replaced_files.len(),
            "removed replaced snapshot's data files");
    }

    Ok(CtasLandingReport {
        rows: total_rows,
        bytes: total_bytes,
        data_files: files_count,
        snapshot_id,
    })
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

    // ── land_ctas ─────────────────────────────────────────────────────────────

    async fn make_empty_catalog() -> (Arc<dyn Catalog + Send + Sync>, tempfile::TempDir) {
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
        (catalog as Arc<dyn Catalog + Send + Sync>, dir)
    }

    async fn stream_of(
        ctx: &SessionContext,
        sql: &str,
    ) -> datafusion::execution::SendableRecordBatchStream {
        ctx.sql(sql).await.unwrap().execute_stream().await.unwrap()
    }

    async fn table_rows(
        catalog: &Arc<dyn Catalog + Send + Sync>,
        ident: &TableIdent,
        ctx: &SessionContext,
    ) -> Vec<RecordBatch> {
        let table = catalog.load_table(ident).await.unwrap();
        scan_iceberg_table(&table, ctx).await.unwrap()
    }

    #[tokio::test]
    async fn land_ctas_creates_new_table_with_rows() {
        let (catalog, _dir) = make_empty_catalog().await;
        let ctx = SessionContext::new();
        let ident = TableIdent::new(NamespaceIdent::new("pipe".into()), "out".into());

        let stream = stream_of(
            &ctx,
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)",
        )
        .await;
        let report = land_ctas(Arc::clone(&catalog), &ident, false, stream)
            .await
            .unwrap();
        assert_eq!(report.rows, 3);
        assert_eq!(report.data_files, 1);
        assert!(report.snapshot_id > 0, "commit must produce a snapshot");
        assert!(report.bytes > 0);

        let rows: usize = table_rows(&catalog, &ident, &ctx)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 3, "read back all landed rows");
    }

    #[tokio::test]
    async fn land_ctas_replace_swaps_contents_and_schema() {
        let (catalog, _dir) = make_empty_catalog().await;
        let ctx = SessionContext::new();
        let ident = TableIdent::new(NamespaceIdent::new("pipe".into()), "out".into());

        let first = stream_of(&ctx, "SELECT * FROM (VALUES (1), (2)) AS t(id)").await;
        land_ctas(Arc::clone(&catalog), &ident, false, first)
            .await
            .unwrap();

        // CREATE without OR REPLACE on an existing table must fail.
        let dup = stream_of(&ctx, "SELECT * FROM (VALUES (9)) AS t(id)").await;
        let err = land_ctas(Arc::clone(&catalog), &ident, false, dup)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "got: {err}"
        );

        // Replace with a different schema and contents.
        let second = stream_of(
            &ctx,
            "SELECT * FROM (VALUES (10, 'x'), (20, 'y'), (30, 'z')) AS t(id, tag)",
        )
        .await;
        let report = land_ctas(Arc::clone(&catalog), &ident, true, second)
            .await
            .unwrap();
        assert_eq!(report.rows, 3);

        let batches = table_rows(&catalog, &ident, &ctx).await;
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3, "replace must not append to the old contents");
        assert_eq!(
            batches[0].schema().fields().len(),
            2,
            "replace must adopt the new schema"
        );
    }

    #[tokio::test]
    async fn land_ctas_rolls_multiple_data_files() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let (catalog, _dir) = make_empty_catalog().await;
        let ctx = SessionContext::new();
        let ident = TableIdent::new(NamespaceIdent::new("pipe".into()), "big".into());

        // Ten explicit source batches; a 1-byte roll threshold rolls a part
        // per streamed batch.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batches: Vec<RecordBatch> = (0..10)
            .map(|part| {
                let start = part * 1000;
                let values: Vec<i64> = (start..start + 1000).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .unwrap()
            })
            .collect();
        let mem = MemTable::try_new(Arc::clone(&schema), vec![batches]).unwrap();
        ctx.register_table("src", Arc::new(mem)).unwrap();
        let stream = stream_of(&ctx, "SELECT id FROM src").await;
        let report = land_ctas_with_target(Arc::clone(&catalog), &ident, false, stream, 1)
            .await
            .unwrap();

        assert_eq!(report.rows, 10_000);
        assert!(
            report.data_files > 1,
            "1-byte threshold must roll multiple parts, got {}",
            report.data_files
        );
        let rows: usize = table_rows(&catalog, &ident, &ctx)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 10_000, "all parts must be committed and readable");
    }

    #[tokio::test]
    async fn land_ctas_replace_deletes_old_data_files() {
        let (catalog, dir) = make_empty_catalog().await;
        let ctx = SessionContext::new();
        let ident = TableIdent::new(NamespaceIdent::new("pipe".into()), "cycled".into());

        let parquet_count = |root: &std::path::Path| -> usize {
            walkdir(root)
                .iter()
                .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
                .count()
        };
        fn walkdir(root: &std::path::Path) -> Vec<std::path::PathBuf> {
            let mut out = Vec::new();
            if let Ok(rd) = std::fs::read_dir(root) {
                for entry in rd.flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        out.extend(walkdir(&p));
                    } else {
                        out.push(p);
                    }
                }
            }
            out
        }

        let first = stream_of(&ctx, "SELECT * FROM (VALUES (1), (2)) AS t(id)").await;
        land_ctas(Arc::clone(&catalog), &ident, false, first)
            .await
            .unwrap();
        assert_eq!(parquet_count(dir.path()), 1);

        // Each replace must leave exactly the new snapshot's files on disk —
        // no orphan accumulation across refresh cycles.
        for round in 0..3 {
            let stream = stream_of(&ctx, "SELECT * FROM (VALUES (10), (20)) AS t(id)").await;
            land_ctas(Arc::clone(&catalog), &ident, true, stream)
                .await
                .unwrap();
            assert_eq!(
                parquet_count(dir.path()),
                1,
                "round {round}: replaced data files must be deleted"
            );
        }

        let rows: usize = table_rows(&catalog, &ident, &ctx)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 2, "table still reads correctly after cleanup");
    }

    #[tokio::test]
    async fn land_ctas_empty_result_creates_empty_table() {
        let (catalog, _dir) = make_empty_catalog().await;
        let ctx = SessionContext::new();
        let ident = TableIdent::new(NamespaceIdent::new("pipe".into()), "empty".into());

        let stream = stream_of(&ctx, "SELECT 1 AS id WHERE FALSE").await;
        let report = land_ctas(Arc::clone(&catalog), &ident, false, stream)
            .await
            .unwrap();
        assert_eq!(report.rows, 0);
        assert_eq!(report.data_files, 0);
        assert_eq!(report.snapshot_id, -1, "empty table has no snapshot");
        assert!(
            catalog.table_exists(&ident).await.unwrap(),
            "empty CTAS must still create the table"
        );
    }

    #[test]
    fn arrow_schema_conversion_rejects_nested_types() {
        use arrow::datatypes::{DataType, Field, Schema};
        let nested = Schema::new(vec![Field::new(
            "xs",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]);
        let err = arrow_schema_to_iceberg_schema(&nested).unwrap_err();
        assert!(err.to_string().contains("cannot map"), "got: {err}");
    }
}
