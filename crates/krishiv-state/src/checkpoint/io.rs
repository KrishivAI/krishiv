use crate::checkpoint::metadata::{
    CheckpointError, CheckpointMetadata, CheckpointResult, IntegrityManifest, OperatorSnapshotRef,
};
use crate::checkpoint::paths::{
    epoch_dir, latest_epoch_hint_path, manifest_path, metadata_path, snapshot_path,
};
use crate::checkpoint::storage_trait::CheckpointStorage;
use sha2::{Digest, Sha256};

// ── High-level helpers ────────────────────────────────────────────────────────

/// Write serialized `metadata` to `{epoch_dir}/metadata.json`.
///
/// **Does not update the epoch hint file.** Callers must call [`write_epoch_hint`]
/// *after* [`write_manifest`] so the hint only ever points to a fully sealed epoch.
/// Updating the hint before the manifest is written can cause `latest_valid_epoch`
/// to return an epoch whose manifest has not yet been written — resulting in an
/// apparent "no valid epoch" on the next restart even though a newer epoch exists.
pub fn write_epoch_metadata(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    metadata: &CheckpointMetadata,
) -> CheckpointResult<()> {
    validate_metadata_identity(metadata, job_id, epoch)?;
    // Propagate real storage errors; only `NoValidEpoch` (first checkpoint)
    // is benign and should be treated as "no prior epoch, proceed".
    // Using `if let Ok(...)` would silently swallow non-`NoValidEpoch` errors
    // and bypass the monotonicity guard, so we use an explicit `match`.
    match latest_valid_epoch(storage, job_id) {
        Ok(latest) if epoch <= latest => {
            return Err(CheckpointError::StaleEpoch {
                attempted: epoch,
                latest,
            });
        }
        Ok(_) => {}                              // newer epoch — proceed
        Err(CheckpointError::NoValidEpoch) => {} // no prior epoch — proceed
        Err(e) => return Err(e),                 // real storage error — propagate
    }
    let json = serde_json::to_vec_pretty(metadata).map_err(|e| CheckpointError::Storage {
        message: format!("metadata serialize: {e}"),
    })?;
    storage.write_bytes(&metadata_path(job_id, epoch), &json)
    // NOTE: epoch hint is NOT written here — callers must call write_epoch_hint()
    // after write_manifest() succeeds to guarantee the hint only points to sealed epochs.
}

/// Async variant of [`write_epoch_metadata`].
pub async fn write_epoch_metadata_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    metadata: &CheckpointMetadata,
) -> CheckpointResult<()> {
    validate_metadata_identity(metadata, job_id, epoch)?;
    match latest_valid_epoch_async(storage, job_id).await {
        Ok(latest) if epoch <= latest => {
            return Err(CheckpointError::StaleEpoch {
                attempted: epoch,
                latest,
            });
        }
        Ok(_) => {}
        Err(CheckpointError::NoValidEpoch) => {}
        Err(e) => return Err(e),
    }
    let json = serde_json::to_vec_pretty(metadata).map_err(|e| CheckpointError::Storage {
        message: format!("metadata serialize: {e}"),
    })?;
    storage
        .write_bytes_async(&metadata_path(job_id, epoch), &json)
        .await
}

/// Update the fast-path epoch hint to `epoch`.
///
/// This must be called **last** — after both [`write_epoch_metadata`] and
/// [`write_manifest`] have succeeded.  Writing the hint before the manifest is
/// present can cause `latest_valid_epoch` to return an epoch that fails
/// `validate_epoch` on the next restart, forcing a full directory scan.
///
/// In the worst case (crash between writing the manifest and writing the hint)
/// `latest_valid_epoch` simply falls back to scanning `list_valid_epochs`, so
/// the epoch is not lost — the hint is purely a read-path optimisation.
pub fn write_epoch_hint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.write_bytes(
        &latest_epoch_hint_path(job_id),
        epoch.to_string().as_bytes(),
    )
}

/// Async variant of [`write_epoch_hint`].
pub async fn write_epoch_hint_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(
            &latest_epoch_hint_path(job_id),
            epoch.to_string().as_bytes(),
        )
        .await
}

/// Read and deserialize `metadata.json` for `epoch`.  Returns `None` if absent.
pub fn read_epoch_metadata(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<Option<CheckpointMetadata>> {
    match storage.read_bytes(&metadata_path(job_id, epoch))? {
        None => Ok(None),
        Some(bytes) => {
            let meta: CheckpointMetadata =
                serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Corrupt {
                    epoch,
                    message: format!("metadata JSON parse: {e}"),
                })?;
            Ok(Some(meta))
        }
    }
}

/// Async variant of [`read_epoch_metadata`].
pub async fn read_epoch_metadata_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<Option<CheckpointMetadata>> {
    match storage
        .read_bytes_async(&metadata_path(job_id, epoch))
        .await?
    {
        None => Ok(None),
        Some(bytes) => {
            let meta: CheckpointMetadata =
                serde_json::from_slice(&bytes).map_err(|e| CheckpointError::Corrupt {
                    epoch,
                    message: format!("metadata JSON parse: {e}"),
                })?;
            Ok(Some(meta))
        }
    }
}

/// Write an operator state snapshot to `{epoch_dir}/{op_id}/{task_id}/state.bin`.
pub fn write_operator_snapshot(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
    bytes: &[u8],
) -> CheckpointResult<()> {
    storage.write_bytes(&snapshot_path(job_id, epoch, op_id, task_id), bytes)
}

/// Async variant of [`write_operator_snapshot`].
pub async fn write_operator_snapshot_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
    bytes: &[u8],
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(&snapshot_path(job_id, epoch, op_id, task_id), bytes)
        .await
}

/// Read an operator state snapshot.  Returns `None` if absent.
pub fn read_operator_snapshot(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
) -> CheckpointResult<Option<Vec<u8>>> {
    storage.read_bytes(&snapshot_path(job_id, epoch, op_id, task_id))
}

/// Async variant of [`read_operator_snapshot`].
pub async fn read_operator_snapshot_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    op_id: &str,
    task_id: &str,
) -> CheckpointResult<Option<Vec<u8>>> {
    storage
        .read_bytes_async(&snapshot_path(job_id, epoch, op_id, task_id))
        .await
}

/// Write the integrity manifest for `epoch`.
///
/// This must be called **last** — after all state snapshots and metadata are
/// written.  A present and valid manifest is the signal that an epoch is
/// complete and safe to restore from.
pub fn write_manifest(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<()> {
    storage.write_bytes(&manifest_path(job_id, epoch), &manifest.serialize())
}

/// Async variant of [`write_manifest`].
pub async fn write_manifest_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<()> {
    storage
        .write_bytes_async(&manifest_path(job_id, epoch), &manifest.serialize())
        .await
}

/// Validate the integrity manifest for `epoch`.
///
/// Returns `true` if the manifest exists, covers `metadata.json`, the metadata
/// identity matches `job_id`/`epoch`, every metadata-declared snapshot is
/// covered by the manifest, and every listed file's SHA-256 matches the
/// manifest entry.  Returns `false` if the manifest is absent, omits required
/// files, or any hash fails.
pub fn validate_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<bool> {
    let Some(manifest) = read_optional_manifest(storage, &manifest_path(job_id, epoch), epoch)?
    else {
        return Ok(false);
    };
    validate_checkpoint_manifest(storage, job_id, epoch, &manifest)
}

fn read_optional_manifest(
    storage: &dyn CheckpointStorage,
    path: &str,
    epoch: u64,
) -> CheckpointResult<Option<IntegrityManifest>> {
    let Some(manifest_bytes) = storage.read_bytes(path)? else {
        return Ok(None);
    };
    IntegrityManifest::deserialize(&manifest_bytes)
        .map(Some)
        .map_err(|e| CheckpointError::Corrupt {
            epoch,
            message: format!("manifest parse: {e}"),
        })
}

fn read_required_manifest(
    storage: &dyn CheckpointStorage,
    path: &str,
    epoch: u64,
) -> CheckpointResult<IntegrityManifest> {
    read_optional_manifest(storage, path, epoch)?.ok_or(CheckpointError::NoValidEpoch)
}

fn validate_manifest_at_prefix(
    storage: &dyn CheckpointStorage,
    base_prefix: &str,
    manifest_path: &str,
    epoch: u64,
) -> CheckpointResult<bool> {
    let Some(manifest) = read_optional_manifest(storage, manifest_path, epoch)? else {
        return Ok(false);
    };
    validate_manifest_entries(storage, base_prefix, epoch, &manifest)
}

fn validate_checkpoint_manifest(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<bool> {
    if !validate_manifest_entries(storage, &epoch_dir(job_id, epoch), epoch, manifest)? {
        return Ok(false);
    }

    let Some(metadata_bytes) = storage.read_bytes(&metadata_path(job_id, epoch))? else {
        return Ok(false);
    };
    validate_checkpoint_metadata_contract(&metadata_bytes, job_id, epoch, manifest)
}

async fn validate_checkpoint_manifest_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<bool> {
    if !validate_manifest_entries_async(storage, &epoch_dir(job_id, epoch), epoch, manifest).await?
    {
        return Ok(false);
    }

    let Some(metadata_bytes) = storage
        .read_bytes_async(&metadata_path(job_id, epoch))
        .await?
    else {
        return Ok(false);
    };
    validate_checkpoint_metadata_contract(&metadata_bytes, job_id, epoch, manifest)
}

fn validate_checkpoint_metadata_contract(
    metadata_bytes: &[u8],
    job_id: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<bool> {
    let metadata = serde_json::from_slice::<CheckpointMetadata>(metadata_bytes).map_err(|e| {
        CheckpointError::Corrupt {
            epoch,
            message: format!("metadata JSON parse: {e}"),
        }
    })?;
    validate_metadata_identity(&metadata, job_id, epoch)?;
    for snapshot in &metadata.operator_snapshots {
        let relative_path = snapshot_relative_path(job_id, epoch, snapshot)?;
        validate_manifest_relative_path(relative_path, epoch)?;
        if !manifest.contains(relative_path) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_manifest_entries(
    storage: &dyn CheckpointStorage,
    base_prefix: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<bool> {
    if !manifest.contains("metadata.json") {
        return Ok(false);
    }
    for (path, expected_hex) in manifest.entries() {
        validate_manifest_relative_path(path, epoch)?;
        let full = format!("{base_prefix}/{path}");
        match storage.read_bytes(&full)? {
            None => return Ok(false),
            Some(data) => {
                // Stream-hash via BufReader to avoid loading large files into
                // memory twice (once for read, once for digest).
                use std::io::Read as _;
                let mut reader = std::io::BufReader::new(data.as_slice());
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 8192];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| CheckpointError::Storage {
                            message: format!("reading {full} for hash: {e}"),
                        })?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                let hash = format!("{:x}", hasher.finalize());
                if hash != *expected_hex {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

async fn validate_manifest_entries_async(
    storage: &dyn CheckpointStorage,
    base_prefix: &str,
    epoch: u64,
    manifest: &IntegrityManifest,
) -> CheckpointResult<bool> {
    if !manifest.contains("metadata.json") {
        return Ok(false);
    }
    for (path, expected_hex) in manifest.entries() {
        validate_manifest_relative_path(path, epoch)?;
        let full = format!("{base_prefix}/{path}");
        match storage.read_bytes_async(&full).await? {
            None => return Ok(false),
            Some(data) => {
                use std::io::Read as _;
                let mut reader = std::io::BufReader::new(data.as_slice());
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 8192];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| CheckpointError::Storage {
                            message: format!("reading {full} for hash: {e}"),
                        })?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                let hash = format!("{:x}", hasher.finalize());
                if hash != *expected_hex {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn validate_manifest_relative_path(path: &str, epoch: u64) -> CheckpointResult<()> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') {
        return Err(CheckpointError::Corrupt {
            epoch,
            message: format!("manifest path {path:?} is not a relative checkpoint path"),
        });
    }
    if path == "manifest.sha256" {
        return Err(CheckpointError::Corrupt {
            epoch,
            message: "manifest must not include itself".to_owned(),
        });
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(CheckpointError::Corrupt {
                epoch,
                message: format!("manifest path {path:?} contains an invalid component"),
            });
        }
    }
    Ok(())
}

fn validate_metadata_identity(
    metadata: &CheckpointMetadata,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    metadata.validate()?;
    if metadata.job_id != job_id {
        return Err(CheckpointError::Corrupt {
            epoch,
            message: format!(
                "metadata job_id {} does not match requested job_id {job_id}",
                metadata.job_id
            ),
        });
    }
    if metadata.epoch != epoch {
        return Err(CheckpointError::Corrupt {
            epoch,
            message: format!(
                "metadata epoch {} does not match requested epoch {epoch}",
                metadata.epoch
            ),
        });
    }
    Ok(())
}

fn snapshot_relative_path<'a>(
    job_id: &str,
    epoch: u64,
    snapshot: &'a OperatorSnapshotRef,
) -> CheckpointResult<&'a str> {
    let prefix = format!("{}/", epoch_dir(job_id, epoch));
    snapshot
        .snapshot_path
        .strip_prefix(&prefix)
        .ok_or_else(|| CheckpointError::Corrupt {
            epoch,
            message: format!(
                "snapshot path {} is not under checkpoint epoch {}",
                snapshot.snapshot_path,
                epoch_dir(job_id, epoch)
            ),
        })
}

/// Async variant of [`validate_epoch`].
pub async fn validate_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<bool> {
    let manifest_bytes = match storage
        .read_bytes_async(&manifest_path(job_id, epoch))
        .await?
    {
        None => return Ok(false),
        Some(b) => b,
    };
    let manifest =
        IntegrityManifest::deserialize(&manifest_bytes).map_err(|e| CheckpointError::Corrupt {
            epoch,
            message: format!("manifest parse: {e}"),
        })?;
    validate_checkpoint_manifest_async(storage, job_id, epoch, &manifest).await
}

/// Return all epoch numbers that have a valid integrity manifest, in ascending order.
///
/// Epochs with missing or corrupt manifests are silently excluded.
pub fn list_valid_epochs(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let checkpoint_prefix = format!("{job_id}/checkpoints");
    let epoch_dirs = storage.list_dir(&checkpoint_prefix)?;
    let mut valid = Vec::new();
    for name in epoch_dirs {
        let Ok(epoch) = name.parse::<u64>() else {
            tracing::warn!(epoch_dir = %name, "skipping non-numeric checkpoint epoch directory");
            continue;
        };
        match validate_epoch(storage, job_id, epoch) {
            Ok(true) => valid.push(epoch),
            Ok(false) => tracing::warn!(job_id, epoch, "excluding invalid checkpoint epoch"),
            Err(e) => {
                tracing::warn!(job_id, epoch, error = %e, "checkpoint epoch validation failed");
                continue;
            }
        }
    }
    valid.sort_unstable();
    Ok(valid)
}

/// Async variant of [`list_valid_epochs`].
pub async fn list_valid_epochs_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let checkpoint_prefix = format!("{job_id}/checkpoints");
    let epoch_dirs = storage.list_dir_async(&checkpoint_prefix).await?;
    let mut valid = Vec::new();
    for name in epoch_dirs {
        let Ok(epoch) = name.parse::<u64>() else {
            tracing::warn!(epoch_dir = %name, "skipping non-numeric checkpoint epoch directory");
            continue;
        };
        match validate_epoch_async(storage, job_id, epoch).await {
            Ok(true) => valid.push(epoch),
            Ok(false) => tracing::warn!(job_id, epoch, "excluding invalid checkpoint epoch"),
            Err(e) => {
                tracing::warn!(job_id, epoch, error = %e, "checkpoint epoch validation failed");
                continue;
            }
        }
    }
    valid.sort_unstable();
    Ok(valid)
}

/// Delete all data for `epoch` from storage.
pub fn delete_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.delete_prefix(&epoch_dir(job_id, epoch))
}

/// Async variant of [`delete_epoch`].
pub async fn delete_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
) -> CheckpointResult<()> {
    storage.delete_prefix_async(&epoch_dir(job_id, epoch)).await
}

/// Find the most recent valid epoch.  Returns `Err(NoValidEpoch)` if none.
pub fn latest_valid_epoch(storage: &dyn CheckpointStorage, job_id: &str) -> CheckpointResult<u64> {
    if let Some(hinted) = read_latest_epoch_hint(storage, job_id)?
        && validate_epoch(storage, job_id, hinted)?
    {
        return Ok(hinted);
    }

    let epochs = list_valid_epochs(storage, job_id)?;
    epochs
        .into_iter()
        .last()
        .ok_or(CheckpointError::NoValidEpoch)
}

/// Async variant of [`latest_valid_epoch`].
pub async fn latest_valid_epoch_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<u64> {
    if let Some(hinted) = read_latest_epoch_hint_async(storage, job_id).await?
        && validate_epoch_async(storage, job_id, hinted).await?
    {
        return Ok(hinted);
    }

    let epochs = list_valid_epochs_async(storage, job_id).await?;
    epochs
        .into_iter()
        .last()
        .ok_or(CheckpointError::NoValidEpoch)
}

fn read_latest_epoch_hint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Option<u64>> {
    let Some(bytes) = storage.read_bytes(&latest_epoch_hint_path(job_id))? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| CheckpointError::Storage {
        message: format!("latest epoch hint is not valid UTF-8: {error}"),
    })?;
    text.trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|error| CheckpointError::Storage {
            message: format!("latest epoch hint is not a valid u64: {error}"),
        })
}

async fn read_latest_epoch_hint_async(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Option<u64>> {
    let Some(bytes) = storage
        .read_bytes_async(&latest_epoch_hint_path(job_id))
        .await?
    else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| CheckpointError::Storage {
        message: format!("latest epoch hint is not valid UTF-8: {error}"),
    })?;
    text.trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|error| CheckpointError::Storage {
            message: format!("latest epoch hint is not a valid u64: {error}"),
        })
}

// ── Fencing token enforcement ─────────────────────────────────────────────────

/// Validate that `metadata.fencing_token` is not older than `current_token`.
///
/// Call this before writing a new checkpoint epoch or savepoint. Rejects metadata
/// whose fencing token does not match the current coordinator's token — this
/// prevents split-brain commits by stale coordinators.
///
/// **Important**: fencing tokens are per-coordinator-instance and are not
/// comparable across different coordinator instances.  This function should be
/// used only when the caller is the current active coordinator doing a write.
/// For restore operations, use [`validate_fencing_token_for_restore`] instead.
pub fn validate_fencing_token(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token != current_token {
        return Err(CheckpointError::StaleFencingToken {
            stored: metadata.fencing_token,
            current: current_token,
        });
    }
    Ok(())
}

/// Validate fencing token for a checkpoint restore operation.
///
/// Unlike [`validate_fencing_token`], this function accepts metadata written by
/// a prior coordinator instance (whose fencing token may differ from the current
/// leader's token because fencing tokens are per-coordinator-instance, not
/// globally monotonic).  The restore path relies on the leader-election
/// mechanism to guarantee that only one coordinator is actively mutating job
/// state; the fencing token in the metadata is recorded for audit purposes.
///
/// The check rejects only the pathological case where the metadata token is
/// strictly greater than the current token — which would indicate the metadata
/// was written by a coordinator that came *after* this one in the leadership
/// sequence, meaning this coordinator is stale.
pub fn validate_fencing_token_for_restore(
    metadata: &CheckpointMetadata,
    current_token: u64,
) -> CheckpointResult<()> {
    if metadata.fencing_token > current_token {
        return Err(CheckpointError::StaleFencingToken {
            stored: metadata.fencing_token,
            current: current_token,
        });
    }
    Ok(())
}

/// Path to the immutable savepoint prefix for a job.
fn savepoint_prefix(job_id: &str) -> String {
    format!("{job_id}/savepoints")
}

/// Path to a specific savepoint epoch directory.
fn savepoint_epoch_dir(job_id: &str, savepoint_epoch: u64) -> String {
    format!("{}/{:020}", savepoint_prefix(job_id), savepoint_epoch)
}

/// C11: Create an immutable savepoint from the latest committed checkpoint.
///
/// Copies all checkpoint files (metadata, state snapshots, manifest) to a
/// separate `savepoints/` prefix that is excluded from normal checkpoint
/// garbage collection.  The savepoint persists until explicitly deleted by
/// the administrator.
///
/// Returns the savepoint epoch (same as the source checkpoint epoch) and
/// the serialized metadata.
pub fn create_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    label: Option<&str>,
) -> CheckpointResult<(u64, CheckpointMetadata)> {
    let epoch = latest_valid_epoch(storage, job_id)?;
    create_savepoint_at_epoch(storage, job_id, epoch, label)
}

/// Create an immutable savepoint from a specific committed checkpoint epoch.
///
/// Same contract as [`create_savepoint`] but pinned to `epoch` rather than the
/// latest valid epoch, so callers that just committed a savepoint-flagged
/// epoch cannot race with a concurrently committing newer epoch.
pub fn create_savepoint_at_epoch(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    epoch: u64,
    label: Option<&str>,
) -> CheckpointResult<(u64, CheckpointMetadata)> {
    let source_manifest = read_required_manifest(storage, &manifest_path(job_id, epoch), epoch)?;
    let mut metadata =
        read_epoch_metadata(storage, job_id, epoch)?.ok_or(CheckpointError::NoValidEpoch)?;
    validate_metadata_identity(&metadata, job_id, epoch)?;
    metadata.is_savepoint = true;
    metadata.savepoint_label = label.map(str::to_string);

    let savepoint_dir = savepoint_epoch_dir(job_id, epoch);
    let mut savepoint_manifest = IntegrityManifest::new();

    let metadata_json =
        serde_json::to_vec_pretty(&metadata).map_err(|e| CheckpointError::Storage {
            message: format!("savepoint metadata serialize: {e}"),
        })?;
    savepoint_manifest.insert_bytes("metadata.json", &metadata_json);

    let mut snapshot_files = Vec::with_capacity(metadata.operator_snapshots.len());
    for snap in &metadata.operator_snapshots {
        let rel = snapshot_relative_path(job_id, epoch, snap)?;
        let data =
            storage
                .read_bytes(&snap.snapshot_path)?
                .ok_or_else(|| CheckpointError::Corrupt {
                    epoch,
                    message: format!(
                        "snapshot {} referenced by metadata is missing",
                        snap.snapshot_path
                    ),
                })?;
        if !source_manifest.verify(rel, &data) {
            return Err(CheckpointError::Corrupt {
                epoch,
                message: format!(
                    "snapshot {} is not covered by the source checkpoint manifest",
                    snap.snapshot_path
                ),
            });
        }
        savepoint_manifest.insert_bytes(rel, &data);
        snapshot_files.push((rel.to_owned(), data));
    }

    let write_result = (|| -> CheckpointResult<()> {
        storage.write_bytes(&format!("{savepoint_dir}/metadata.json"), &metadata_json)?;
        for (rel, data) in &snapshot_files {
            storage.write_bytes(&format!("{savepoint_dir}/{rel}"), data)?;
        }
        storage.write_bytes(
            &format!("{savepoint_dir}/manifest.sha256"),
            &savepoint_manifest.serialize(),
        )?;
        Ok(())
    })();
    if let Err(error) = write_result {
        cleanup_partial_prefix(storage, &savepoint_dir);
        return Err(error);
    }

    Ok((epoch, metadata))
}

/// C11: Restore from an immutable savepoint.
///
/// Reads savepoint metadata and validates that the current coordinator's
/// fencing token is equal to or greater than the savepoint's fencing token.
/// Returns the savepoint metadata and the list of valid savepoint epochs
/// available for this job.
pub fn restore_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    savepoint_epoch: u64,
    current_fencing_token: u64,
) -> CheckpointResult<CheckpointMetadata> {
    let savepoint_dir = savepoint_epoch_dir(job_id, savepoint_epoch);
    if !validate_manifest_at_prefix(
        storage,
        &savepoint_dir,
        &format!("{savepoint_dir}/manifest.sha256"),
        savepoint_epoch,
    )? {
        return Err(CheckpointError::Corrupt {
            epoch: savepoint_epoch,
            message: "savepoint manifest validation failed".to_owned(),
        });
    }

    let meta_path = format!("{savepoint_dir}/metadata.json");
    let metadata_bytes = storage
        .read_bytes(&meta_path)?
        .ok_or(CheckpointError::NoValidEpoch)?;
    let metadata = serde_json::from_slice::<CheckpointMetadata>(&metadata_bytes).map_err(|e| {
        CheckpointError::Corrupt {
            epoch: savepoint_epoch,
            message: format!("savepoint metadata JSON parse: {e}"),
        }
    })?;

    validate_metadata_identity(&metadata, job_id, savepoint_epoch)?;
    validate_fencing_token_for_restore(&metadata, current_fencing_token)?;

    // Copy savepoint files back into the active checkpoints directory for restore.
    let epoch_dir = epoch_dir(job_id, savepoint_epoch);
    let mut restored_manifest = IntegrityManifest::new();
    restored_manifest.insert_bytes("metadata.json", &metadata_bytes);

    let mut snapshot_files = Vec::with_capacity(metadata.operator_snapshots.len());
    for snap in &metadata.operator_snapshots {
        let rel = snapshot_relative_path(job_id, savepoint_epoch, snap)?;
        let savepoint_snap = format!("{savepoint_dir}/{rel}");
        let data =
            storage
                .read_bytes(&savepoint_snap)?
                .ok_or_else(|| CheckpointError::Corrupt {
                    epoch: savepoint_epoch,
                    message: format!("savepoint snapshot {savepoint_snap} is missing"),
                })?;
        restored_manifest.insert_bytes(rel, &data);
        snapshot_files.push((rel.to_owned(), data));
    }

    storage.write_bytes(&format!("{epoch_dir}/metadata.json"), &metadata_bytes)?;
    for (rel, data) in &snapshot_files {
        storage.write_bytes(&format!("{epoch_dir}/{rel}"), data)?;
    }
    storage.write_bytes(
        &manifest_path(job_id, savepoint_epoch),
        &restored_manifest.serialize(),
    )?;

    Ok(metadata)
}

/// List all savepoint epochs for a job.
pub fn list_savepoints(
    storage: &dyn CheckpointStorage,
    job_id: &str,
) -> CheckpointResult<Vec<u64>> {
    let prefix = savepoint_prefix(job_id);
    let names = storage.list_dir(&prefix)?;
    let mut epochs: Vec<u64> = names
        .into_iter()
        .filter_map(|n| n.parse::<u64>().ok())
        .collect();
    epochs.sort_unstable();
    Ok(epochs)
}

/// Delete a savepoint (no-op if the savepoint does not exist).
pub fn delete_savepoint(
    storage: &dyn CheckpointStorage,
    job_id: &str,
    savepoint_epoch: u64,
) -> CheckpointResult<()> {
    let dir = savepoint_epoch_dir(job_id, savepoint_epoch);
    storage.delete_prefix(&dir)
}

fn cleanup_partial_prefix(storage: &dyn CheckpointStorage, prefix: &str) {
    if let Err(error) = storage.delete_prefix(prefix) {
        tracing::warn!(
            prefix,
            error = %error,
            "failed to clean up partial checkpoint storage prefix"
        );
    }
}
