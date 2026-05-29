use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore,
    compression::{ShuffleCompression, parquet_writer_properties},
    error::{io_err, shuffle_write_lock},
    store::LeaseMap,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// A local-disk shuffle store that serialises partitions to Parquet files.
///
/// Each partition is written to `{base_dir}/{job_id}/{stage_id}/{partition}.parquet`.
/// Lease tokens are tracked in memory; they survive the process only as long as
/// the store object is alive.
pub struct LocalDiskShuffleStore {
    base_dir: PathBuf,
    lease_tokens: LeaseMap,
    compression: ShuffleCompression,
}

impl LocalDiskShuffleStore {
    /// Create a new store rooted at `base_dir`, creating the directory if needed.
    pub fn new(base_dir: impl AsRef<Path>) -> ShuffleResult<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            io_err(format!(
                "failed to create shuffle base dir '{}': {e}",
                base_dir.display()
            ))
        })?;
        Ok(Self {
            base_dir,
            lease_tokens: Arc::new(RwLock::new(BTreeMap::new())),
            compression: ShuffleCompression::None,
        })
    }

    /// Set the Parquet compression codec for partition writes.
    #[must_use]
    pub fn with_compression(mut self, compression: ShuffleCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Return the configured Parquet compression codec.
    pub fn compression(&self) -> ShuffleCompression {
        self.compression
    }

    fn partition_path(&self, id: &PartitionId) -> ShuffleResult<PathBuf> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        Ok(self
            .base_dir
            .join(&id.job_id)
            .join(&id.stage_id)
            .join(format!("{}.parquet", id.partition)))
    }
}

impl ShuffleStore for LocalDiskShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        let key = (id.job_id, id.stage_id, id.partition);
        let mut leases = shuffle_write_lock(&self.lease_tokens)?;
        if let Some(&expected) = leases.get(&key)
            && lease_token < expected
        {
            return Err(ShuffleError::StaleLeaseToken {
                expected,
                actual: lease_token,
            });
        }
        leases.insert(key, lease_token);
        Ok(())
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (
            partition.id.job_id.clone(),
            partition.id.stage_id.clone(),
            partition.id.partition,
        );

        // BUG-4: Two-phase token validation with temp-file + rename atomicity.
        //
        // The previous single-phase approach acquired the write lock, validated
        // the token, advanced it, released the lock, and then wrote the file.
        // Two concurrent writers with tokens T1 < T2 could both pass validation
        // (sequentially), then race to write the file — with T1's stale data
        // potentially overwriting T2's newer data if T1's spawn_blocking started
        // later.
        //
        // Fix: Write to a temp file WITHOUT holding the lock (phase 1), then
        // re-acquire the lock and atomically rename the temp file to the final
        // path only if the token in the map still matches (phase 2).  If a newer
        // writer has meanwhile advanced the token past ours, we discard the temp.
        //
        // Phase 1: validate initial token and advance it.
        {
            let mut tokens = shuffle_write_lock(&self.lease_tokens)?;
            if let Some(&expected) = tokens.get(&key) {
                // P1.25: use `<` (monotonic-token semantics) — reject stale writes,
                // accept equal or newer tokens.
                if lease_token < expected {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
                // Advance the stored token so a zombie with the previous token
                // cannot win a race by writing before the replacement arrives.
                tokens.insert(key.clone(), lease_token);
            } else {
                // Compatibility path for direct single-attempt writes: the first
                // writer establishes the expected token for this partition.
                tokens.insert(key.clone(), lease_token);
            }
        }

        let final_path = self.partition_path(&partition.id)?;
        let writer_props = parquet_writer_properties(self.compression);
        let lease_tokens = Arc::clone(&self.lease_tokens);
        let parent_dir = final_path.parent().map(PathBuf::from);

        // P0.4: Wrap all blocking filesystem I/O in spawn_blocking so the
        // async executor thread is never stalled by synchronous disk calls.
        tokio::task::spawn_blocking(move || {
            use parquet::arrow::ArrowWriter;
            use std::sync::atomic::{AtomicU64, Ordering};

            // Use a process-local counter for unique temp file names.
            static TMP_CTR: AtomicU64 = AtomicU64::new(1);
            let tmp_suffix = TMP_CTR.fetch_add(1, Ordering::Relaxed);

            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| io_err(format!("failed to create partition dir: {e}")))?;
            }

            // Phase 1 (continued): Write to a temp file alongside the final path.
            let tmp_path = final_path.with_extension(format!("tmp.{tmp_suffix}"));
            {
                let tmp_file = std::fs::File::create(&tmp_path).map_err(|e| {
                    io_err(format!(
                        "failed to create temp partition file '{}': {e}",
                        tmp_path.display()
                    ))
                })?;
                let schema = partition.schema.clone();
                let mut writer = ArrowWriter::try_new(tmp_file, schema, Some(writer_props))
                    .map_err(|e| io_err(format!("failed to create Parquet writer: {e}")))?;
                for batch in &partition.batches {
                    writer
                        .write(batch)
                        .map_err(|e| io_err(format!("failed to write Parquet batch: {e}")))?;
                }
                // S4: Sync temp file to durable storage before commit.
                let mut tmp_file = writer
                    .into_inner()
                    .map_err(|e| io_err(format!("failed to finalize Parquet writer: {e}")))?;
                tmp_file
                    .sync_all()
                    .map_err(|e| io_err(format!("failed to fsync temp file: {e}")))?;
            }

            // Phase 2: Re-acquire the lock and commit via rename only if our token
            // is still the current winner.  If a newer writer advanced the token
            // past ours since phase 1, discard the temp file.
            let commit = {
                let tokens = lease_tokens
                    .read()
                    .map_err(|_| io_err("lease token lock poisoned"))?;
                tokens
                    .get(&key)
                    .copied()
                    .map_or(false, |t| t == lease_token)
            };

            if commit {
                std::fs::rename(&tmp_path, &final_path).map_err(|e| {
                    io_err(format!(
                        "failed to rename temp partition '{}' → '{}': {e}",
                        tmp_path.display(),
                        final_path.display()
                    ))
                })?;
                // S4: Fsync the parent directory so the rename is durable.
                if let Some(ref parent) = parent_dir {
                    if let Ok(dir) = std::fs::File::open(parent) {
                        dir.sync_all().ok();
                    }
                }
            } else {
                // Newer writer won — silently discard this temp file.
                let _ = std::fs::remove_file(&tmp_path);
                return Err(ShuffleError::StaleLeaseToken {
                    expected: lease_token.saturating_add(1),
                    actual: lease_token,
                });
            }
            Ok(())
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))?
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        let path = self.partition_path(id)?;
        let id = id.clone();

        // P0.4: Wrap all blocking filesystem I/O in spawn_blocking so the
        // async executor thread is never stalled by synchronous disk calls.
        tokio::task::spawn_blocking(move || {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            use std::fs::File;

            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(io_err(format!(
                        "failed to open partition file '{}': {e}",
                        path.display()
                    )));
                }
            };
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| io_err(format!("failed to build Parquet reader: {e}")))?;
            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| io_err(format!("failed to build Parquet batch reader: {e}")))?;
            let mut batches = Vec::new();
            for result in reader {
                let batch =
                    result.map_err(|e| io_err(format!("error reading Parquet batch: {e}")))?;
                batches.push(batch);
            }
            Ok(Some(ShufflePartition {
                id,
                schema,
                batches,
            }))
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))?
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        crate::validate_safe_id(job_id, "job_id")?;
        let dir = self.base_dir.join(job_id);
        let job_id_owned = job_id.to_owned();

        // P0.4: Wrap blocking filesystem removal in spawn_blocking.
        tokio::task::spawn_blocking(move || {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(io_err(format!("failed to delete job partitions: {e}")));
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))??;

        // Clean up in-memory lease tokens for this job (in-memory, safe outside spawn_blocking).
        let mut tokens = shuffle_write_lock(&self.lease_tokens)?;
        tokens.retain(|(jid, _, _), _| jid != &job_id_owned);
        Ok(())
    }
}
