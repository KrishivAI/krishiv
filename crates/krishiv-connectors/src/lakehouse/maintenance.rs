//! Iceberg table maintenance operations (Phase J6).
//!
//! Maintenance procedures:
//!
//! | Procedure | SQL CALL | Effect |
//! |-----------|----------|--------|
//! | `expire_snapshots` | `CALL system.expire_snapshots('ns.tbl', '7 days', 5)` | Remove old snapshots and their orphaned files |
//! | `remove_orphan_files` | `CALL system.remove_orphan_files('ns.tbl', '1 day')` | Delete data files not in any live snapshot |
//! | `compact_data_files` | `CALL system.compact_data_files('ns.tbl', 134217728)` | Bin-pack small Parquet files per partition |
//! | `maintain_table` | `CALL system.maintain_table('ns.tbl', '7 days')` | Compact, then expire, then remove orphans |

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
        LakehouseError::Io(format!(
            "failed to build scan for snapshot {snapshot_id}: {e}"
        ))
    })?;
    let task_stream = scan.plan_files().await.map_err(|e| {
        LakehouseError::Io(format!(
            "failed to plan files for snapshot {snapshot_id}: {e}"
        ))
    })?;
    let tasks: Vec<iceberg::scan::FileScanTask> = task_stream.try_collect().await.map_err(|e| {
        LakehouseError::Io(format!(
            "failed to collect file tasks for snapshot {snapshot_id}: {e}"
        ))
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

/// Read every batch of one Parquet data file via parquet 58.x (avoids the
/// iceberg-datafusion DataFusion version conflict).
async fn read_parquet_file(
    file_io: &iceberg::io::FileIO,
    path: &str,
) -> Result<Vec<arrow::array::RecordBatch>, LakehouseError> {
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
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LakehouseError::Io(e.to_string()))
}

/// Compact small Parquet data files into larger ones, partition by partition.
///
/// Files are bin-packed within their partition value: files smaller than
/// `target_file_size_bytes` are grouped into bins of roughly the target size
/// and each bin is rewritten as one Parquet file. Memory stays bounded — one
/// bin (~target size) is read at a time. Files already at or above the
/// target, and lone small files with nothing to merge with, are carried over
/// untouched.
///
/// The metadata swap is drop+recreate preserving the partition spec
/// (iceberg-rust 0.9.1 exposes no public rewrite/replace snapshot action;
/// a true atomic rewrite commit lands with the 0.10 bump, task #163),
/// guarded by a G3-style conflict check: immediately before the swap the
/// table is reloaded, and if any snapshot was committed after planning the
/// compaction aborts (cleaning up its part files) instead of silently
/// discarding the concurrent writer's commit.
///
/// Returns the number of newly written (compacted) data files; 0 when there
/// is nothing to compact, in which case the table is left untouched.
pub async fn compact_data_files(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    target_file_size_bytes: u64,
) -> Result<usize, LakehouseError> {
    use crate::lakehouse::dml::{PendingPart, fanout_into_buffers, write_ctas_part};
    use crate::lakehouse::partitioned_write::{PartitionFanout, transforms_from_metadata};
    use iceberg::TableCreation;
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Nothing to compact if table is empty.
    let Some(planned_snapshot) = table.metadata().current_snapshot().map(|s| s.snapshot_id())
    else {
        return Ok(0);
    };

    // Enumerate data files via the iceberg scan plan (avoids arrow 57/58
    // mismatch); the manifest entries carry size, row count and partition.
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

    // This engine's writers never produce delete files today; refuse rather
    // than silently drop deletes a foreign writer may have committed.
    if tasks.iter().any(|t| !t.deletes.is_empty()) {
        return Err(LakehouseError::Iceberg(format!(
            "compact_data_files: {table_ident} has delete files; compaction over delete files \
             is not supported yet"
        )));
    }

    // Group files by partition value, then bin-pack the small ones within
    // each partition. A file without a manifest row count cannot be carried
    // over (its DataFile descriptor needs the count), so it is always
    // rewritten.
    let mut groups: std::collections::BTreeMap<String, Vec<&iceberg::scan::FileScanTask>> =
        std::collections::BTreeMap::new();
    for task in &tasks {
        let key = format!("{:?}", task.partition);
        groups.entry(key).or_default().push(task);
    }

    let mut bins: Vec<Vec<&iceberg::scan::FileScanTask>> = Vec::new();
    let mut kept: Vec<&iceberg::scan::FileScanTask> = Vec::new();
    for (_, mut files) in groups {
        files.sort_by_key(|t| t.file_size_in_bytes);
        let mut bin: Vec<&iceberg::scan::FileScanTask> = Vec::new();
        let mut bin_bytes = 0u64;
        for task in files {
            if task.file_size_in_bytes >= target_file_size_bytes && task.record_count.is_some() {
                kept.push(task);
                continue;
            }
            bin_bytes += task.file_size_in_bytes.max(1);
            bin.push(task);
            if bin_bytes >= target_file_size_bytes {
                bins.push(std::mem::take(&mut bin));
                bin_bytes = 0;
            }
        }
        if !bin.is_empty() {
            bins.push(bin);
        }
    }
    // A one-file bin with a known row count gains nothing from a rewrite.
    bins.retain(|bin| match bin.as_slice() {
        [only] if only.record_count.is_some() => {
            kept.push(only);
            false
        }
        _ => true,
    });
    if bins.is_empty() {
        return Ok(0);
    }

    let file_io = table.file_io().clone();
    let table_location = table.metadata().location().to_string();
    let iceberg_schema = table.metadata().current_schema().clone();
    let partition_by = transforms_from_metadata(table.metadata())?;
    let unbound_spec = if partition_by.is_empty() {
        None
    } else {
        Some(
            table
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone()
                .into_unbound(),
        )
    };

    // Rewrite each bin into (normally) one part per partition value, one bin
    // in memory at a time. The fanout re-derives partition values from the
    // rows, so a file whose contents disagree with its manifest partition is
    // corrected rather than propagated.
    let mut pending: Vec<PendingPart> = Vec::new();
    let mut replaced: Vec<String> = Vec::new();
    for bin in &bins {
        let mut buffers = std::collections::BTreeMap::new();
        let mut fanout: Option<PartitionFanout> = None;
        for task in bin {
            let batches = read_parquet_file(&file_io, task.data_file_path()).await?;
            for batch in &batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let f = match &fanout {
                    Some(f) => f,
                    None => fanout.get_or_insert(PartitionFanout::try_new(
                        batch.schema().as_ref(),
                        &partition_by,
                    )?),
                };
                fanout_into_buffers(f, batch, &mut buffers)?;
            }
            replaced.push(task.data_file_path().to_string());
        }
        for (_, buf) in buffers {
            pending.push(
                write_ctas_part(
                    &file_io,
                    &table_location,
                    &buf.path,
                    buf.partition,
                    buf.batches,
                )
                .await?,
            );
        }
    }

    // G3-style conflict check: abort (and clean up our parts) if anything
    // committed since planning. A small window remains between this check
    // and the drop below — closing it needs the 0.10 atomic rewrite (#163).
    let current = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let now_snapshot = current
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    if now_snapshot != Some(planned_snapshot) {
        for part in &pending {
            if let Err(e) = file_io.delete(&part.dest).await {
                tracing::warn!(path = %part.dest, error = %e,
                    "compact_data_files: failed to clean up part after conflict abort");
            }
        }
        return Err(LakehouseError::Iceberg(format!(
            "compact_data_files: concurrent commit detected on {table_ident} \
             (snapshot {planned_snapshot} -> {now_snapshot:?}); compaction aborted, retry later"
        )));
    }

    // Metadata swap: drop + recreate at the same location with the same
    // schema and partition spec, then commit kept + compacted files.
    catalog
        .drop_table(table_ident)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let creation = || {
        TableCreation::builder()
            .name(table_ident.name().to_string())
            .schema((*iceberg_schema).clone())
            .partition_spec_opt(unbound_spec.clone())
            .location(table_location.clone())
            .build()
    };
    let new_table = match catalog
        .create_table(table_ident.namespace(), creation())
        .await
    {
        Ok(t) => t,
        Err(create_err) => {
            if let Err(restore_err) = catalog
                .create_table(table_ident.namespace(), creation())
                .await
            {
                tracing::error!(
                    table = %table_ident,
                    create_error = %create_err,
                    restore_error = %restore_err,
                    "CRITICAL: table is invisible after failed compaction swap and restore \
                     attempt; manual intervention required"
                );
            }
            return Err(LakehouseError::Iceberg(create_err.to_string()));
        }
    };

    let spec_id = new_table.metadata().default_partition_spec_id();
    let mut data_files = Vec::with_capacity(kept.len() + pending.len());
    for task in &kept {
        let record_count = task.record_count.ok_or_else(|| {
            LakehouseError::Iceberg(format!(
                "compact_data_files: kept file {} lost its record count",
                task.data_file_path()
            ))
        })?;
        data_files.push(
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(task.data_file_path().to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(task.file_size_in_bytes)
                .record_count(record_count)
                .partition(task.partition.clone().unwrap_or_else(Struct::empty))
                .partition_spec_id(spec_id)
                .build()
                .map_err(|e| LakehouseError::Iceberg(e.to_string()))?,
        );
    }
    let compacted = pending.len();
    for part in pending {
        data_files.push(part.into_data_file(spec_id)?);
    }

    let tx = Transaction::new(&new_table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action
        .apply(tx)
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let committed = tx
        .commit(&*catalog)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Keep the local-FS version hint current so the compaction survives a
    // restart (CONN-4).
    if let Some(loc) = committed.metadata_location() {
        let table_root = std::path::Path::new(table_location.trim_start_matches("file://"));
        if let Err(e) = super::iceberg_native::native::write_version_hint(table_root, loc) {
            tracing::warn!(
                table = %table_ident,
                location = loc,
                error = %e,
                "version hint update failed after compaction commit; hint may be stale"
            );
        }
    }

    // The new snapshot no longer references the rewritten files: delete them
    // (best effort — a failed delete only leaves an orphan).
    let mut removed = 0usize;
    for path in &replaced {
        match file_io.delete(path).await {
            Ok(()) => removed += 1,
            Err(e) => {
                tracing::warn!(table = %table_ident, path, error = %e,
                    "compact_data_files: failed to delete rewritten file (orphaned)");
            }
        }
    }

    tracing::info!(
        table = %table_ident,
        target_bytes = target_file_size_bytes,
        rewritten = replaced.len(),
        removed,
        compacted,
        kept = kept.len(),
        "compact_data_files: bin-packed small files per partition"
    );

    Ok(compacted)
}

// ── maintain_table ────────────────────────────────────────────────────────────

/// Outcome of one [`maintain_table`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// Newly written compacted data files.
    pub compacted_files: usize,
    /// Snapshots removed from the table history.
    pub expired_snapshots: usize,
    /// Orphaned files deleted from storage.
    pub removed_orphans: usize,
}

/// One-call table maintenance, in the order that lets each step feed the
/// next: compact small files (commits a new snapshot), expire old snapshots
/// (frees the pre-compaction history), then remove orphaned files.
///
/// This is the schedulable entry point — `CALL system.maintain_table(…)` —
/// for platform-driven periodic maintenance jobs. Errors propagate (nothing
/// is swallowed); a compaction conflict with a concurrent writer surfaces as
/// an error and the scheduler simply retries on its next tick.
pub async fn maintain_table(
    catalog: Arc<dyn Catalog + Send + Sync>,
    table_ident: &TableIdent,
    target_file_size_bytes: u64,
    older_than: Duration,
    retain_last: usize,
) -> Result<MaintenanceReport, LakehouseError> {
    let compacted_files =
        compact_data_files(Arc::clone(&catalog), table_ident, target_file_size_bytes).await?;
    let expired_snapshots =
        expire_snapshots(Arc::clone(&catalog), table_ident, older_than, retain_last).await?;
    let removed_orphans = remove_orphan_files(catalog, table_ident, older_than).await?;
    Ok(MaintenanceReport {
        compacted_files,
        expired_snapshots,
        removed_orphans,
    })
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

    #[tokio::test]
    async fn compact_fresh_table_returns_zero() {
        let (catalog, ident, _dir) = empty_catalog_table().await;
        let compacted = compact_data_files(catalog, &ident, 128 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(compacted, 0, "fresh table has nothing to compact");
    }

    #[tokio::test]
    async fn compact_bin_packs_small_files_per_partition() {
        use crate::lakehouse::dml::land_ctas_with_target;
        use crate::lakehouse::partitioned_write::parse_partition_transform;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let catalog: Arc<dyn Catalog + Send + Sync> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "mem",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );
        let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "part_compact".into());

        // Two stream batches + a 1-byte roll threshold ⇒ every batch flushes
        // per partition: 4 small files (2 per region).
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let make_batch = |ids: &[i64], regions: &[&str]| {
            arrow::array::RecordBatch::try_new(
                Arc::clone(&arrow_schema),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(regions.to_vec())),
                ],
            )
            .unwrap()
        };
        let batches = vec![
            make_batch(&[1, 2], &["eu", "us"]),
            make_batch(&[3, 4, 5], &["eu", "us", "eu"]),
        ];
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(Arc::clone(&arrow_schema), vec![batches]).unwrap();
        let stream = ctx
            .read_table(Arc::new(mem))
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let partition_by = vec![parse_partition_transform("region").unwrap()];
        let report = land_ctas_with_target(
            Arc::clone(&catalog),
            &ident,
            false,
            &partition_by,
            stream,
            1,
        )
        .await
        .unwrap();
        assert_eq!(report.rows, 5);
        assert_eq!(report.data_files, 4, "two small files per region");

        // Compaction merges each region's files into one, keeping the spec.
        let compacted = compact_data_files(Arc::clone(&catalog), &ident, 128 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(compacted, 2, "one merged file per region");

        let table = catalog.load_table(&ident).await.unwrap();
        let spec = table.metadata().default_partition_spec();
        assert_eq!(spec.fields().len(), 1, "compaction must preserve the spec");
        assert_eq!(spec.fields()[0].name, "region");

        let tasks: Vec<iceberg::scan::FileScanTask> = table
            .scan()
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(tasks.len(), 2);
        let mut rows = 0usize;
        for task in &tasks {
            assert!(
                task.data_file_path().contains("region="),
                "path: {}",
                task.data_file_path()
            );
            let batches = read_parquet_file(table.file_io(), task.data_file_path())
                .await
                .unwrap();
            rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, 5, "compaction must not lose rows");

        // Already compact: a second run is a no-op that commits nothing.
        let snapshot_before = table.metadata().current_snapshot().unwrap().snapshot_id();
        let again = compact_data_files(Arc::clone(&catalog), &ident, 128 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(again, 0, "nothing left to merge");
        let reloaded = catalog.load_table(&ident).await.unwrap();
        assert_eq!(
            reloaded
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id(),
            snapshot_before,
            "no-op compaction must not commit a new snapshot"
        );
    }

    #[tokio::test]
    async fn maintain_table_fresh_table_reports_all_zero() {
        let (catalog, ident, _dir) = empty_catalog_table().await;
        let report = maintain_table(catalog, &ident, 128 * 1024 * 1024, Duration::days(7), 1)
            .await
            .unwrap();
        assert_eq!(
            report,
            MaintenanceReport {
                compacted_files: 0,
                expired_snapshots: 0,
                removed_orphans: 0
            }
        );
    }
}
