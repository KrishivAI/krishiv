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
    // An empty file set here means "nothing is referenced", and both callers
    // turn that into deletions. Prove it before believing it.
    crate::lakehouse::empty_plan_guard::guard_empty_plan(
        table,
        tasks.len(),
        &format!("file_paths_for_snapshot(snapshot {snapshot_id})"),
    )?;
    // Delete files (equality/position deletes) are live table state too: a
    // task's `deletes` are read on every scan, so treating them as
    // unreferenced would resurrect the rows they delete.
    let mut paths = HashSet::new();
    for task in tasks {
        for delete in &task.deletes {
            paths.insert(delete.file_path.clone());
        }
        paths.insert(task.data_file_path().to_string());
    }
    Ok(paths)
}

// ── expire_snapshots ──────────────────────────────────────────────────────────

/// Remove snapshots older than `older_than` from the table history, keeping at
/// least `retain_last` snapshots regardless of age.
///
/// Returns the number of snapshots removed. The expired snapshots are removed
/// from the table metadata via `Transaction::expire_snapshots`, then data
/// files that were only referenced by them (not by any kept snapshot) are
/// deleted via the table's `FileIO` — this works for local, S3, GCS, and
/// Azure backends. Idempotent: a second run selects nothing and returns 0.
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

    // Collect the expiring snapshots' file sets BEFORE the metadata commit —
    // once a snapshot is removed from the metadata it can no longer be scanned.
    let mut expiring_files: HashSet<String> = HashSet::new();
    for snap_id in &to_expire {
        expiring_files.extend(file_paths_for_snapshot(&table, *snap_id).await?);
    }

    // Remove the expired snapshots from the table metadata. This is the real
    // expiry: after this commit, time travel to them fails loudly instead of
    // silently reading files deleted below, and a re-run sees them gone
    // (idempotent — the second run selects nothing and returns 0). The age
    // cutoff is pinned to 0 so only the explicitly selected ids expire; the
    // selection above already applied `older_than` / `retain_last`.
    let tx = Transaction::new(&table);
    let action = tx
        .expire_snapshots()
        .expire_snapshot_ids(to_expire.iter().copied())
        .expire_older_than_ms(0);
    let tx = action
        .apply(tx)
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
    let committed = tx
        .commit(&*catalog)
        .await
        .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;

    // Keep the local-FS version hint pointing at the post-expiry metadata so a
    // reopen does not resurrect the expired history (best effort, like
    // compaction: the commit above is already durable in the catalog).
    if let Some(loc) = committed.metadata_location() {
        let table_root =
            std::path::Path::new(table.metadata().location().trim_start_matches("file://"));
        if table_root.join("metadata").is_dir()
            && let Err(e) = super::iceberg_native::native::write_version_hint(table_root, loc)
        {
            tracing::warn!(
                table = %table_ident,
                location = loc,
                error = %e,
                "version hint update failed after expire_snapshots commit; hint may be stale"
            );
        }
    }

    // Delete data files referenced ONLY by expired snapshots (now that the
    // metadata no longer references them).
    let mut files_deleted = 0usize;
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
/// snapshots recorded by a legacy `krishiv.expired-snapshot-ids` property (a
/// pre-metadata-expiry audit trail) but are absent from any current live
/// snapshot, then delete them via `FileIO`. Files orphaned by failed partial
/// writes require external storage-side scanning.
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
                // A snapshot already removed from the metadata (real expiry)
                // cannot be scanned; nothing to enumerate for it here.
                if metadata.snapshot_by_id(snap_id).is_none() {
                    continue;
                }
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
/// The metadata swap is drop+recreate preserving the partition spec.
/// Checked against iceberg-rust 0.10.1 at the bump (2026-08-14): its
/// `Transaction` still exposes only fast_append / expire_snapshots /
/// metadata updates — no public rewrite/replace-files action — so the
/// earlier claim that "a true atomic rewrite commit lands with the 0.10
/// bump" was wrong. Task #163 stays open, re-check on the next release.
/// The swap is
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

    // A table with delete files cannot be bin-packed file-by-file: carrying
    // any data file over while dropping its delete files would RESURRECT the
    // deleted rows, and concatenating raw parquet would do the same inside a
    // bin. So deletes force the whole table through the delete-applying
    // arrow reader (new in iceberg 0.10) and a full rewrite — the result is
    // a delete-free table, which is exactly what compacting deletes means.
    let has_deletes = tasks.iter().any(|t| !t.deletes.is_empty());

    // Group files by partition value, then bin-pack the small ones within
    // each partition. A file without a manifest row count cannot be carried
    // over (its DataFile descriptor needs the count), so it is always
    // rewritten. (Skipped entirely under `has_deletes`: nothing is carried
    // over there, every file is rewritten.)
    let mut bins: Vec<Vec<&iceberg::scan::FileScanTask>> = Vec::new();
    let mut kept: Vec<&iceberg::scan::FileScanTask> = Vec::new();
    if !has_deletes {
        let mut groups: std::collections::BTreeMap<String, Vec<&iceberg::scan::FileScanTask>> =
            std::collections::BTreeMap::new();
        for task in &tasks {
            let key = format!("{:?}", task.partition);
            groups.entry(key).or_default().push(task);
        }

        for (_, mut files) in groups {
            files.sort_by_key(|t| t.file_size_in_bytes);
            let mut bin: Vec<&iceberg::scan::FileScanTask> = Vec::new();
            let mut bin_bytes = 0u64;
            for task in files {
                if task.file_size_in_bytes >= target_file_size_bytes && task.record_count.is_some()
                {
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
    if has_deletes {
        // Full rewrite through the delete-applying reader: stream the scan
        // (deletes applied by iceberg's arrow reader), fan out into
        // per-partition buffers, and flush any buffer that reaches the
        // target size — memory stays bounded by (partitions × target size),
        // the same bound as the bin path. Every data file AND every delete
        // file is replaced; nothing is carried over.
        use futures::TryStreamExt as _;
        let mut buffers = std::collections::BTreeMap::new();
        let mut fanout: Option<PartitionFanout> = None;
        let mut stream = scan
            .to_arrow()
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))?
        {
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
            fanout_into_buffers(f, &batch, &mut buffers)?;
            let full: Vec<String> = buffers
                .iter()
                .filter(|(_, b)| b.bytes as u64 >= target_file_size_bytes)
                .map(|(k, _)| k.clone())
                .collect();
            for key in full {
                if let Some(buf) = buffers.remove(&key) {
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
        for task in &tasks {
            replaced.push(task.data_file_path().to_string());
            for delete in &task.deletes {
                let path = delete.file_path.clone();
                if !replaced.contains(&path) {
                    replaced.push(path);
                }
            }
        }
    }
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

    // The recreated table is EMPTY until this commit lands, so a commit
    // failure must not be surfaced without a restore attempt — mirroring the
    // create-failure fallback above. The full file set (kept + compacted) is
    // retried on a freshly loaded table; if that also fails the table is left
    // empty and needs manual intervention.
    let append_files = |files: Vec<iceberg::spec::DataFile>, table: &iceberg::table::Table| {
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(files);
        action
            .apply(tx)
            .map_err(|e| LakehouseError::Iceberg(e.to_string()))
    };
    let committed = match append_files(data_files.clone(), &new_table)?
        .commit(&*catalog)
        .await
    {
        Ok(t) => t,
        Err(commit_err) => {
            let restored = match catalog.load_table(table_ident).await {
                Ok(reloaded) => match append_files(data_files, &reloaded) {
                    Ok(tx) => tx.commit(&*catalog).await.map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                },
                Err(e) => Err(e.to_string()),
            };
            match restored {
                Ok(t) => t,
                Err(restore_err) => {
                    tracing::error!(
                        table = %table_ident,
                        commit_error = %commit_err,
                        restore_error = %restore_err,
                        "CRITICAL: table is EMPTY after failed compaction commit and restore \
                         attempt; manual intervention required (re-append its data files)"
                    );
                    return Err(LakehouseError::Iceberg(commit_err.to_string()));
                }
            }
        }
    };

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
pub(crate) mod tests {
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

    /// The empty-plan tripwire, against a REAL table rather than its decision
    /// table.
    ///
    /// The unit tests in `empty_plan_guard` cover the branching on plain
    /// inputs. What they cannot cover is the assumption the guard rests on:
    /// that a krishiv-written snapshot actually carries a parseable
    /// `total-data-files` in `summary().additional_properties`. If iceberg
    /// stopped writing that key, the guard would silently degrade to its
    /// "unprovable" branch — still safe for destructive callers, but no longer
    /// the check it claims to be. This pins it.
    #[tokio::test]
    async fn the_empty_plan_tripwire_reads_a_real_snapshot_summary() {
        use crate::lakehouse::dml::land_ctas_with_target;
        use crate::lakehouse::empty_plan_guard::guard_empty_plan;
        use arrow::array::Int64Array;
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
        let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "tripwire".into());

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(Arc::clone(&arrow_schema), vec![vec![batch]]).unwrap();
        ctx.register_table("src", Arc::new(mem)).unwrap();
        let stream = ctx
            .sql("SELECT * FROM src")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        land_ctas_with_target(Arc::clone(&catalog), &ident, false, &[], stream, 1)
            .await
            .expect("land table");

        let table = catalog.load_table(&ident).await.unwrap();

        // The assumption, asserted: the snapshot records a file count > 0.
        let recorded = table
            .metadata()
            .current_snapshot()
            .expect("a snapshot")
            .summary()
            .additional_properties
            .get("total-data-files")
            .cloned();
        let recorded = recorded.expect(
            "iceberg must record `total-data-files`; without it the tripwire degrades to \
             its unprovable branch and stops being the check it claims to be",
        );
        assert!(
            recorded.trim().parse::<u64>().unwrap() > 0,
            "expected a non-empty table, got total-data-files={recorded}"
        );

        // A real plan passes.
        guard_empty_plan(&table, 2, "test").expect("a plan with files must pass");

        // The failure this exists for: zero planned against a snapshot that
        // says otherwise.
        let err = guard_empty_plan(&table, 0, "test")
            .expect_err("an empty plan contradicting the snapshot must be refused");
        let msg = err.to_string();
        assert!(msg.contains("planned 0 files"), "{msg}");
        assert!(msg.contains("NOT been modified"), "{msg}");
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

    /// A REAL merge-on-read table — data files plus a live equality-delete
    /// file — built through iceberg's public spec APIs, because this engine
    /// (and iceberg-rust 0.10.1 itself) has no transaction that can commit
    /// one: `fast_append` refuses non-Data content. Foreign writers (Spark,
    /// Flink) produce exactly this shape, and until the 0.10 bump every read
    /// path here would have silently RESURRECTED the deleted row.
    ///
    /// Table: ids 1..=4 in one data file; an equality delete on id=2.
    pub(crate) async fn mor_table_with_live_equality_delete() -> (
        Arc<dyn Catalog + Send + Sync>,
        TableIdent,
        tempfile::TempDir,
    ) {
        use crate::lakehouse::dml::land_ctas_with_target;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
            ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention,
            Struct, Summary, TableMetadataBuilder,
        };

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
        let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "mor".into());

        // Snapshot 1: a normal krishiv landing, ids 1..=4.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]))],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(Arc::clone(&arrow_schema), vec![vec![batch]]).unwrap();
        ctx.register_table("src", Arc::new(mem)).unwrap();
        let stream = ctx
            .sql("SELECT * FROM src")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        land_ctas_with_target(Arc::clone(&catalog), &ident, false, &[], stream, usize::MAX)
            .await
            .expect("land base table");

        let table = catalog.load_table(&ident).await.unwrap();
        let metadata = table.metadata();
        let file_io = table.file_io().clone();
        let location = metadata.location().to_string();
        let snap1 = metadata.current_snapshot().expect("base snapshot").clone();

        // The equality-delete parquet: one row, id=2, with the iceberg field
        // id so the delete-file reader can map it back to column 1.
        let delete_field =
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )]));
        let delete_schema = Arc::new(ArrowSchema::new(vec![delete_field]));
        let delete_batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&delete_schema),
            vec![Arc::new(Int64Array::from(vec![2i64]))],
        )
        .unwrap();
        let mut delete_bytes = Vec::new();
        {
            let mut w = parquet::arrow::ArrowWriter::try_new(
                &mut delete_bytes,
                Arc::clone(&delete_schema),
                None,
            )
            .unwrap();
            w.write(&delete_batch).unwrap();
            w.close().unwrap();
        }
        let delete_path = format!("{location}/data/eq-delete-00000.parquet");
        file_io
            .new_output(&delete_path)
            .unwrap()
            .write(delete_bytes.clone().into())
            .await
            .unwrap();

        let snap2_id = snap1.snapshot_id() + 1;
        let seq2 = snap1.sequence_number() + 1;

        let delete_data_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(delete_path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(delete_bytes.len() as u64)
            .record_count(1)
            .partition(Struct::empty())
            .partition_spec_id(metadata.default_partition_spec_id())
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap();

        let delete_manifest_path = format!("{location}/metadata/eq-delete-m0.avro");
        let mut mw = ManifestWriterBuilder::new(
            file_io.new_output(&delete_manifest_path).unwrap(),
            Some(snap2_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_deletes();
        mw.add_file(delete_data_file, seq2).unwrap();
        let delete_manifest = mw.write_manifest_file().await.unwrap();

        // New manifest list: everything snapshot 1 tracked, plus the deletes.
        let old_entries = table.manifest_list_reader(&snap1).load().await.unwrap();
        let list_path = format!("{location}/metadata/snap-{snap2_id}-manifest-list.avro");
        let mut lw = ManifestListWriter::v2(
            file_io
                .new_output(&list_path)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snap2_id,
            Some(snap1.snapshot_id()),
            seq2,
        );
        lw.add_manifests(
            old_entries
                .entries()
                .iter()
                .cloned()
                .chain(std::iter::once(delete_manifest)),
        )
        .unwrap();
        lw.close().await.unwrap();

        // Snapshot 2 keeps `total-data-files` honest so the empty-plan
        // tripwire judges this table the same way it judges krishiv's own.
        let snap2 = Snapshot::builder()
            .with_snapshot_id(snap2_id)
            .with_parent_snapshot_id(Some(snap1.snapshot_id()))
            .with_sequence_number(seq2)
            .with_timestamp_ms(snap1.timestamp_ms() + 1)
            .with_manifest_list(list_path)
            .with_schema_id(snap1.schema_id().unwrap())
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::from([
                    ("total-data-files".to_string(), "1".to_string()),
                    ("total-delete-files".to_string(), "1".to_string()),
                ]),
            })
            .build();

        let new_metadata = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            table.metadata_location().map(str::to_string),
        )
        .add_snapshot(snap2)
        .unwrap()
        .set_ref(
            "main",
            SnapshotReference {
                snapshot_id: snap2_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        // `<version>-<uuid>.metadata.json`: catalogs parse the uuid segment of
        // the current metadata filename when deriving the next one, so it must
        // be a real uuid.
        let metadata_path =
            format!("{location}/metadata/00002-8c05c396-9e02-4b0d-9a7a-6f1c7a4d3ab1.metadata.json");
        file_io
            .new_output(&metadata_path)
            .unwrap()
            .write(serde_json::to_vec(&new_metadata).unwrap().into())
            .await
            .unwrap();
        catalog.drop_table(&ident).await.unwrap();
        catalog.register_table(&ident, metadata_path).await.unwrap();

        (catalog, ident, dir)
    }

    /// The fixture itself proves the hazard: the scan plans a task whose
    /// deletes are non-empty, and a raw parquet read of the data files
    /// returns the deleted row. If this ever stops holding, the two tests
    /// below are testing nothing.
    #[tokio::test]
    async fn the_mor_fixture_really_carries_a_live_delete() {
        use futures::TryStreamExt as _;
        let (catalog, ident, _dir) = mor_table_with_live_equality_delete().await;
        let table = catalog.load_table(&ident).await.unwrap();
        let scan = table.scan().build().unwrap();
        let tasks: Vec<iceberg::scan::FileScanTask> = scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            tasks.iter().any(|t| !t.deletes.is_empty()),
            "fixture must plan tasks with delete files"
        );
        let mut raw_rows = 0usize;
        for t in &tasks {
            for b in read_parquet_file(table.file_io(), t.data_file_path())
                .await
                .unwrap()
            {
                raw_rows += b.num_rows();
            }
        }
        assert_eq!(
            raw_rows, 4,
            "raw parquet must still hold the deleted row — that is the trap"
        );
    }

    /// Live delete files are table state, not orphans: removing them would
    /// resurrect the rows they delete. `older_than` is set in the future so
    /// every unreferenced file is age-eligible — only the referenced set
    /// protects the equality-delete file.
    #[tokio::test]
    async fn remove_orphan_files_keeps_live_delete_files() {
        use futures::TryStreamExt as _;
        let (catalog, ident, _dir) = mor_table_with_live_equality_delete().await;

        remove_orphan_files(Arc::clone(&catalog), &ident, Duration::hours(-1))
            .await
            .unwrap();

        let table = catalog.load_table(&ident).await.unwrap();
        let scan = table.scan().build().unwrap();
        let batches: Vec<arrow::array::RecordBatch> = scan
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .expect("the equality-delete file must survive orphan removal");
        let mut ids: Vec<i64> = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            ids.extend(col.iter().flatten());
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 3, 4],
            "id=2 must stay deleted — deleting the live delete file resurrects it"
        );
    }

    /// Real expiry: the expired snapshot leaves the table metadata (time
    /// travel to it fails loudly instead of silently reading deleted files),
    /// and a second run finds nothing to expire.
    #[tokio::test]
    async fn expire_snapshots_removes_snapshots_from_metadata_and_is_idempotent() {
        let (catalog, ident, _dir) = mor_table_with_live_equality_delete().await;
        let before = catalog.load_table(&ident).await.unwrap();
        let old_id = before
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .expect("fixture has a two-snapshot history");

        // Let the snapshots age past a zero-duration cutoff.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let removed = expire_snapshots(Arc::clone(&catalog), &ident, Duration::zero(), 1)
            .await
            .unwrap();
        assert_eq!(removed, 1, "the non-current snapshot must expire");

        let after = catalog.load_table(&ident).await.unwrap();
        assert!(
            after.metadata().snapshot_by_id(old_id).is_none(),
            "expired snapshot must be removed from the table metadata"
        );
        assert!(
            after.metadata().current_snapshot().is_some(),
            "the current snapshot must survive"
        );

        let again = expire_snapshots(Arc::clone(&catalog), &ident, Duration::zero(), 1)
            .await
            .unwrap();
        assert_eq!(again, 0, "a second run must find nothing to expire");
    }

    /// Delegating catalog that fails the first `update_table` (the final
    /// compaction commit for a `MemoryCatalog` table) and then recovers —
    /// modelling a transient commit failure after the drop+recreate swap.
    #[derive(Debug)]
    struct FailFirstUpdateCatalog {
        inner: Arc<dyn Catalog + Send + Sync>,
        remaining_failures: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Catalog for FailFirstUpdateCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }
        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }
        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.get_namespace(namespace).await
        }
        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }
        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }
        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }
        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }
        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.create_table(namespace, creation).await
        }
        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
            self.inner.load_table(table).await
        }
        async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.drop_table(table).await
        }
        async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.purge_table(table).await
        }
        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }
        async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }
        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.register_table(table, metadata_location).await
        }
        async fn update_table(
            &self,
            commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            use std::sync::atomic::Ordering;
            if self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "injected commit failure",
                ));
            }
            self.inner.update_table(commit).await
        }
    }

    /// A transient failure of the post-swap commit must not leave the table
    /// EMPTY: the restore fallback re-appends the full file set.
    #[tokio::test]
    async fn compact_commit_failure_restores_table_contents() {
        use crate::lakehouse::dml::land_ctas_with_target;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let dir = tempfile::tempdir().unwrap();
        let warehouse = url::Url::from_file_path(dir.path()).unwrap().to_string();
        let inner: Arc<dyn Catalog + Send + Sync> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "mem",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
                )
                .await
                .unwrap(),
        );
        let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "flaky".into());

        // Two small files (roll threshold 1 flushes each batch) so compaction
        // has work to do.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let make_batch = |ids: &[i64]| {
            arrow::array::RecordBatch::try_new(
                Arc::clone(&arrow_schema),
                vec![Arc::new(Int64Array::from(ids.to_vec()))],
            )
            .unwrap()
        };
        let batches = vec![make_batch(&[1, 2]), make_batch(&[3])];
        let ctx = SessionContext::new();
        let mem = MemTable::try_new(Arc::clone(&arrow_schema), vec![batches]).unwrap();
        let stream = ctx
            .read_table(Arc::new(mem))
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        land_ctas_with_target(Arc::clone(&inner), &ident, false, &[], stream, 1)
            .await
            .unwrap();

        let flaky: Arc<dyn Catalog + Send + Sync> = Arc::new(FailFirstUpdateCatalog {
            inner: Arc::clone(&inner),
            remaining_failures: std::sync::atomic::AtomicUsize::new(1),
        });
        compact_data_files(flaky, &ident, 128 * 1024 * 1024)
            .await
            .expect("the restore fallback must recover from a transient commit failure");

        let table = inner.load_table(&ident).await.unwrap();
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
        let mut rows = 0usize;
        for task in &tasks {
            let batches = read_parquet_file(table.file_io(), task.data_file_path())
                .await
                .unwrap();
            rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(
            rows, 3,
            "the table must not be left empty after the failed commit"
        );
    }

    /// Compaction over a merge-on-read table: reads THROUGH the deletes,
    /// rewrites, and the result is a delete-free table without the dead row.
    #[tokio::test]
    async fn compact_applies_equality_deletes_and_lands_a_delete_free_table() {
        use futures::TryStreamExt as _;
        let (catalog, ident, _dir) = mor_table_with_live_equality_delete().await;

        let compacted = compact_data_files(Arc::clone(&catalog), &ident, 128 * 1024 * 1024)
            .await
            .expect("compaction over delete files must succeed since iceberg 0.10");
        assert!(compacted >= 1, "a rewrite must have produced files");

        let table = catalog.load_table(&ident).await.unwrap();
        let scan = table.scan().build().unwrap();
        let tasks: Vec<iceberg::scan::FileScanTask> = scan
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            tasks.iter().all(|t| t.deletes.is_empty()),
            "the compacted table must carry no delete files"
        );
        let mut ids = Vec::new();
        for t in &tasks {
            for b in read_parquet_file(table.file_io(), t.data_file_path())
                .await
                .unwrap()
            {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                ids.extend(col.iter().flatten());
            }
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 3, 4],
            "id=2 must stay deleted after the rewrite"
        );
    }
}
