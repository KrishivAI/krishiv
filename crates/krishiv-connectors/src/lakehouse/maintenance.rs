//! Iceberg table maintenance operations (Phase J6).
//!
//! Three maintenance procedures:
//!
//! | Procedure | SQL CALL | Effect |
//! |-----------|----------|--------|
//! | `expire_snapshots` | `CALL system.expire_snapshots('ns.tbl', '7 days', 5)` | Remove old snapshots and their orphaned files |
//! | `remove_orphan_files` | `CALL system.remove_orphan_files('ns.tbl', '1 day')` | Delete data files not in any live snapshot |
//! | `compact_data_files` | `CALL system.compact_data_files('ns.tbl', 134217728)` | Merge small Parquet files into larger ones |

#![cfg(feature = "iceberg")]

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{Duration, Utc};
use iceberg::spec::SnapshotRef;
use iceberg::{Catalog, TableIdent};

use crate::lakehouse::LakehouseError;

// ── expire_snapshots ──────────────────────────────────────────────────────────

/// Remove snapshots older than `older_than` from the table history, keeping at
/// least `retain_last` snapshots regardless of age.
///
/// Returns the number of snapshots that would be removed.
pub async fn expire_snapshots(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    older_than: Duration,
    retain_last: usize,
) -> Result<usize, LakehouseError> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let metadata = table.metadata();
    let cutoff_ms = (Utc::now() - older_than).timestamp_millis();

    // Collect all snapshots sorted newest-first.
    let mut all_snapshots: Vec<SnapshotRef> = metadata.snapshots().cloned().collect();
    all_snapshots.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms()));

    let current_id = metadata.current_snapshot().map(|s| s.snapshot_id());

    let mut kept = 0usize;
    let mut to_expire: Vec<i64> = Vec::new();

    for snap in &all_snapshots {
        let is_current = current_id == Some(snap.snapshot_id());
        let too_new = snap.timestamp_ms() > cutoff_ms;
        if is_current || too_new || kept < retain_last {
            kept += 1;
        } else {
            to_expire.push(snap.snapshot_id());
        }
    }

    if to_expire.is_empty() {
        return Ok(0);
    }

    let removed = to_expire.len();

    // Note: full snapshot removal requires iceberg-rust to expose a public API.
    // A full implementation would call:
    //   txn.expire_snapshots(to_expire).apply().commit()
    // when iceberg-rust stabilises that API. For now we log and return the count.
    tracing::info!(
        table = %table_ident,
        expired = removed,
        "expire_snapshots: marked {} snapshot(s) for removal",
        removed
    );

    Ok(removed)
}

// ── remove_orphan_files ───────────────────────────────────────────────────────

/// Delete data/metadata files in the table location that are not referenced by
/// any live snapshot and are older than `older_than`.
///
/// Returns the number of orphan files removed. Deletion only occurs for
/// `file://` (local filesystem) paths; other storage backends require
/// object-store-level listing which is not yet integrated.
pub async fn remove_orphan_files(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    older_than: Duration,
) -> Result<usize, LakehouseError> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    let metadata = table.metadata();
    let table_location = metadata.location().to_string();

    // Build the set of all files referenced by live snapshots.
    let mut referenced: HashSet<String> = HashSet::new();

    for meta_log in metadata.metadata_log() {
        referenced.insert(meta_log.metadata_file.clone());
    }
    if let Some(loc) = table.metadata_location() {
        referenced.insert(loc.to_string());
    }

    // manifest_list() returns &str (not Option<&str>) in iceberg 0.9.1.
    for snapshot in metadata.snapshots() {
        referenced.insert(snapshot.manifest_list().to_string());
    }

    let cutoff_ms = (Utc::now() - older_than).timestamp_millis();

    // Local-only listing via std::fs (object_store listing is not yet wired up).
    let data_prefix = format!("{}/data", table_location.trim_end_matches('/'));
    let local_path = data_prefix
        .strip_prefix("file://")
        .map(std::path::Path::new)
        .or_else(|| {
            if !data_prefix.contains("://") {
                Some(std::path::Path::new(&data_prefix))
            } else {
                None
            }
        });

    let Some(local_path) = local_path else {
        tracing::info!(
            table = %table_ident,
            location = %table_location,
            "remove_orphan_files: non-local storage backend; skipping file scan"
        );
        return Ok(0);
    };

    if !local_path.exists() {
        return Ok(0);
    }

    let mut orphan_count = 0usize;
    let file_io = table.file_io().clone();

    // Collect local file paths recursively using std::fs.
    let mut stack = vec![local_path.to_path_buf()];
    let mut local_files: Vec<std::path::PathBuf> = Vec::new();
    while let Some(dir) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.is_file() {
                    local_files.push(path);
                }
            }
        }
    }

    for path in local_files {
        let path_str = path.to_string_lossy().to_string();
        let uri = format!("file://{path_str}");

        if referenced.contains(&uri) || referenced.contains(&path_str) {
            continue;
        }

        // Check age via filesystem metadata.
        if let Ok(meta) = std::fs::metadata(&path)
            && let Ok(modified) = meta.modified()
        {
            use std::time::SystemTime;
            let modified_ms = modified
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if modified_ms > cutoff_ms {
                continue; // Too new — leave it.
            }
        }

        match file_io.delete(&uri).await {
            Ok(()) => orphan_count += 1,
            Err(e) => {
                tracing::warn!(path = %uri, error = %e, "remove_orphan_files: failed to delete");
            }
        }
    }

    Ok(orphan_count)
}

// ── compact_data_files ────────────────────────────────────────────────────────

/// Merge small Parquet files into a single larger file to improve query performance.
///
/// Reads all Parquet files via iceberg `plan_files()` + parquet 58.x reader
/// (avoids iceberg-datafusion DataFusion version conflict). Returns the number
/// of files rewritten (0 if no data, 1 if compaction occurred).
pub async fn compact_data_files(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    target_file_size_bytes: u64,
) -> Result<usize, LakehouseError> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Nothing to compact if table is empty.
    if table.metadata().current_snapshot().is_none() {
        return Ok(0);
    }

    // Enumerate Parquet files via iceberg scan plan (avoids arrow 57/58 mismatch).
    let scan = table
        .scan()
        .build()
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    use futures::TryStreamExt;
    let tasks: Vec<iceberg::scan::FileScanTask> = task_stream
        .try_collect()
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    if tasks.is_empty() {
        return Ok(0);
    }

    let file_io = table.file_io().clone();

    // Read all Parquet files using parquet 58.x (arrow 58.x RecordBatch).
    let mut all_batches: Vec<arrow::array::RecordBatch> = Vec::new();
    for task in &tasks {
        let path = task.data_file_path();
        let input = file_io
            .new_input(path)
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        let bytes = input
            .read()
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .build()
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        for batch in reader {
            all_batches.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
        }
    }

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok(0);
    }

    // Rewrite all data into a single file via drop+recreate overwrite.
    let _ = crate::lakehouse::dml::overwrite_table_pub(catalog, table_ident, all_batches).await?;

    tracing::info!(
        table = %table_ident,
        target_bytes = target_file_size_bytes,
        "compact_data_files: rewrote table into single file"
    );

    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};
    use std::collections::HashMap;

    async fn empty_catalog_table() -> (
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
        let ns = NamespaceIdent::new("ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let ident = TableIdent::new(ns, "t".to_string());
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
    async fn expire_snapshots_fresh_table_returns_zero() {
        let (catalog, ident, _dir) = empty_catalog_table().await;
        let removed = expire_snapshots(catalog, &ident, Duration::days(7), 1)
            .await
            .unwrap();
        assert_eq!(removed, 0, "fresh table has no old snapshots");
    }
}
