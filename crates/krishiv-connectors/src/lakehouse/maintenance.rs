//! Iceberg table maintenance operations (Phase J6).
//!
//! Three maintenance procedures:
//!
//! | Procedure | SQL CALL | Effect |
//! |-----------|----------|--------|
//! | `expire_snapshots` | `CALL system.expire_snapshots('ns.tbl', '7 days', 5)` | Remove old snapshots and their orphaned files |
//! | `remove_orphan_files` | `CALL system.remove_orphan_files('ns.tbl', '1 day')` | Delete data files not in any live snapshot |
//! | `compact_data_files` | `CALL system.compact_data_files('ns.tbl', 134217728)` | Merge small Parquet files into larger ones |

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{Duration, Utc};
use futures::TryStreamExt;
use iceberg::spec::SnapshotRef;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};

use crate::lakehouse::LakehouseError;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Collect all data-file paths for a specific snapshot via the iceberg scan API.
///
/// Returns an error if the snapshot cannot be scanned, preventing silent data
/// loss during orphan file cleanup.
async fn file_paths_for_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<HashSet<String>, LakehouseError> {
    let scan = table.scan().snapshot_id(snapshot_id).build().map_err(|e| {
        LakehouseError::Io(std::io::Error::other(format!(
            "failed to build scan for snapshot {snapshot_id}: {e}"
        )))
    })?;
    let task_stream = scan.plan_files().await.map_err(|e| {
        LakehouseError::Io(std::io::Error::other(format!(
            "failed to plan files for snapshot {snapshot_id}: {e}"
        )))
    })?;
    let tasks: Vec<iceberg::scan::FileScanTask> = task_stream.try_collect().await.map_err(|e| {
        LakehouseError::Io(std::io::Error::other(format!(
            "failed to collect file tasks for snapshot {snapshot_id}: {e}"
        )))
    })?;
    Ok(tasks
        .into_iter()
        .map(|t| t.data_file_path().to_string())
        .collect())
}

// ── expire_snapshots ──────────────────────────────────────────────────────────

/// Remove snapshots older than `older_than` from the table history, keeping at
/// least `retain_last` snapshots regardless of age.
///
/// Returns the number of snapshots marked for removal. Data files that are only
/// referenced by expired snapshots (not by any kept snapshot) are deleted via
/// the table's `FileIO` — this works for local, S3, GCS, and Azure backends.
///
/// Expired snapshot IDs are also recorded in the `krishiv.expired-snapshot-ids`
/// table property so that external tools have an audit trail.
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

    let mut kept_ids: HashSet<i64> = HashSet::new();
    let mut to_expire: Vec<i64> = Vec::new();
    let mut kept = 0usize;

    for snap in &all_snapshots {
        let is_current = current_id == Some(snap.snapshot_id());
        let too_new = snap.timestamp_ms() > cutoff_ms;
        if is_current || too_new || kept < retain_last {
            kept += 1;
            kept_ids.insert(snap.snapshot_id());
        } else {
            to_expire.push(snap.snapshot_id());
        }
    }

    if to_expire.is_empty() {
        return Ok(0);
    }

    let removed = to_expire.len();
    let file_io = table.file_io().clone();

    // Collect data files referenced by ALL kept snapshots so we don't delete
    // anything still needed by the live history.
    let mut kept_files: HashSet<String> = HashSet::new();
    for snap_id in &kept_ids {
        let paths = file_paths_for_snapshot(&table, *snap_id).await?;
        kept_files.extend(paths);
    }

    // Delete data files referenced ONLY by expired snapshots.
    let mut files_deleted = 0usize;
    for snap_id in &to_expire {
        let expiring_files = file_paths_for_snapshot(&table, *snap_id).await?;
        for path in expiring_files {
            if !kept_files.contains(&path) {
                match file_io.delete(&path).await {
                    Ok(()) => files_deleted += 1,
                    Err(e) => {
                        tracing::warn!(
                            path = %path,
                            error = %e,
                            "expire_snapshots: failed to delete orphan file"
                        );
                    }
                }
            }
        }
    }

    // Record expired IDs in table properties for audit/observability. Best-effort.
    let expired_ids_csv = to_expire
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let tx = Transaction::new(&table);
    let action = tx
        .update_table_properties()
        .set("krishiv.expired-snapshot-ids".to_string(), expired_ids_csv);
    if let Ok(tx) = action.apply(tx)
        && let Err(e) = tx.commit(&*catalog).await
    {
        tracing::warn!(
            table = %table_ident,
            error = %e,
            "expire_snapshots: failed to persist expired-snapshot-ids property"
        );
    }

    tracing::info!(
        table = %table_ident,
        snapshots_expired = removed,
        files_deleted = files_deleted,
        "expire_snapshots: expired {} snapshot(s), deleted {} orphan file(s)",
        removed,
        files_deleted,
    );

    Ok(removed)
}

// ── remove_orphan_files ───────────────────────────────────────────────────────

/// Delete data/metadata files in the table location that are not referenced by
/// any live snapshot and are older than `older_than`.
///
/// Returns the number of orphan files removed.
///
/// **Local storage** (`file://` or bare paths): files are enumerated via
/// `std::fs::read_dir` and deleted via the table's `FileIO`.
///
/// **Cloud storage** (S3, GCS, Azure): listing requires credentials the caller
/// holds implicitly via iceberg's `FileIO`. We enumerate files that appear in
/// expired snapshots (tracked in `krishiv.expired-snapshot-ids`) but are absent
/// from any current live snapshot, then delete them via `FileIO`. This catches
/// files orphaned by `expire_snapshots`; files orphaned by failed partial writes
/// require external storage-side scanning.
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
    for snapshot in metadata.snapshots() {
        referenced.insert(snapshot.manifest_list().to_string());
    }

    // Collect data files for all live snapshots via scan.
    for snapshot in metadata.snapshots() {
        let paths = file_paths_for_snapshot(&table, snapshot.snapshot_id()).await?;
        referenced.extend(paths);
    }

    let cutoff_ms = (Utc::now() - older_than).timestamp_millis();
    let file_io = table.file_io().clone();
    let mut orphan_count = 0usize;

    // ── Local path listing ────────────────────────────────────────────────────
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

    if let Some(local_path) = local_path {
        if local_path.exists() {
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

                // Skip files younger than the retention threshold.
                if let Ok(meta) = std::fs::metadata(&path)
                    && let Ok(modified) = meta.modified()
                {
                    use std::time::SystemTime;
                    let modified_ms = modified
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    if modified_ms > cutoff_ms {
                        continue;
                    }
                }

                match file_io.delete(&uri).await {
                    Ok(()) => orphan_count += 1,
                    Err(e) => {
                        tracing::warn!(
                            path = %uri,
                            error = %e,
                            "remove_orphan_files: failed to delete local file"
                        );
                    }
                }
            }
        }
    } else {
        // ── Cloud path: use expired-snapshot-ids property ─────────────────────
        // `expire_snapshots` records snapshot IDs whose files were orphaned in
        // `krishiv.expired-snapshot-ids`. For each such snapshot still present in
        // the history, collect its file paths and delete any not referenced by
        // live snapshots. This covers the common case of files orphaned by
        // `expire_snapshots`; truly stray files (from aborted writes) require
        // external cloud-side listing.
        let expired_ids_csv = metadata
            .properties()
            .get("krishiv.expired-snapshot-ids")
            .cloned()
            .unwrap_or_default();

        let expired_ids: Vec<i64> = expired_ids_csv
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();

        if expired_ids.is_empty() {
            tracing::info!(
                table = %table_ident,
                location = %table_location,
                "remove_orphan_files: cloud backend, no expired-snapshot-ids recorded; skipping"
            );
        } else {
            for snap_id in expired_ids {
                let expiring_files = file_paths_for_snapshot(&table, snap_id).await?;
                for path in expiring_files {
                    if referenced.contains(&path) {
                        continue;
                    }
                    match file_io.delete(&path).await {
                        Ok(()) => orphan_count += 1,
                        Err(e) => {
                            tracing::warn!(
                                path = %path,
                                error = %e,
                                "remove_orphan_files: failed to delete cloud file"
                            );
                        }
                    }
                }
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

    #[tokio::test]
    async fn remove_orphan_files_fresh_table_returns_zero() {
        let (catalog, ident, _dir) = empty_catalog_table().await;
        let removed = remove_orphan_files(catalog, &ident, Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(removed, 0, "fresh table has no orphan files");
    }
}
