use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore,
    compression::{ShuffleCompression, parquet_writer_properties},
    error::shuffle_write_lock,
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
            ShuffleError::Io(format!(
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

    fn partition_path(&self, id: &PartitionId) -> PathBuf {
        self.base_dir
            .join(&id.job_id)
            .join(&id.stage_id)
            .join(format!("{}.parquet", id.partition))
    }
}

impl ShuffleStore for LocalDiskShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        let mut leases = shuffle_write_lock(&self.lease_tokens)?;
        if let Some(&expected) = leases.get(&key)
            && lease_token != expected
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

        // Validate/update the lease token.
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
            } else {
                // Compatibility path for direct single-attempt writes: the first
                // writer establishes the expected token for this partition.
                tokens.insert(key, lease_token);
            }
        }

        let path = self.partition_path(&partition.id);
        let writer_props = parquet_writer_properties(self.compression);

        // P0.4: Wrap all blocking filesystem I/O in spawn_blocking so the
        // async executor thread is never stalled by synchronous disk calls.
        tokio::task::spawn_blocking(move || {
            use parquet::arrow::ArrowWriter;

            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    ShuffleError::Io(format!("failed to create partition dir: {e}"))
                })?;
            }

            let file = std::fs::File::create(&path).map_err(|e| {
                ShuffleError::Io(format!(
                    "failed to create partition file '{}': {e}",
                    path.display()
                ))
            })?;

            let schema = partition.schema.clone();
            let mut writer = ArrowWriter::try_new(file, schema, Some(writer_props))
                .map_err(|e| ShuffleError::Io(format!("failed to create Parquet writer: {e}")))?;

            for batch in &partition.batches {
                writer
                    .write(batch)
                    .map_err(|e| ShuffleError::Io(format!("failed to write Parquet batch: {e}")))?;
            }
            writer
                .close()
                .map_err(|e| ShuffleError::Io(format!("failed to close Parquet writer: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| ShuffleError::Io(format!("spawn_blocking join error: {e}")))?
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        let path = self.partition_path(id);
        let id = id.clone();

        // P0.4: Wrap all blocking filesystem I/O in spawn_blocking so the
        // async executor thread is never stalled by synchronous disk calls.
        tokio::task::spawn_blocking(move || {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            use std::fs::File;

            if !path.exists() {
                return Ok(None);
            }
            let file = File::open(&path).map_err(|e| {
                ShuffleError::Io(format!(
                    "failed to open partition file '{}': {e}",
                    path.display()
                ))
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| ShuffleError::Io(format!("failed to build Parquet reader: {e}")))?;
            let schema = builder.schema().clone();
            let reader = builder.build().map_err(|e| {
                ShuffleError::Io(format!("failed to build Parquet batch reader: {e}"))
            })?;
            let mut batches = Vec::new();
            for result in reader {
                let batch = result
                    .map_err(|e| ShuffleError::Io(format!("error reading Parquet batch: {e}")))?;
                batches.push(batch);
            }
            Ok(Some(ShufflePartition {
                id,
                schema,
                batches,
            }))
        })
        .await
        .map_err(|e| ShuffleError::Io(format!("spawn_blocking join error: {e}")))?
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        let dir = self.base_dir.join(job_id);
        let job_id_owned = job_id.to_owned();

        // P0.4: Wrap blocking filesystem removal in spawn_blocking.
        tokio::task::spawn_blocking(move || {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(ShuffleError::Io(format!(
                        "failed to delete job partitions: {e}"
                    )));
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| ShuffleError::Io(format!("spawn_blocking join error: {e}")))??;

        // Clean up in-memory lease tokens for this job (in-memory, safe outside spawn_blocking).
        let mut tokens = shuffle_write_lock(&self.lease_tokens)?;
        tokens.retain(|(jid, _, _), _| jid != &job_id_owned);
        Ok(())
    }
}
