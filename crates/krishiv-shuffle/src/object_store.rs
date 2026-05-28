use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore,
    compression::ShuffleCompression, store::PartitionKey,
};
use dashmap::DashMap;
use object_store::{ObjectStore, ObjectStoreExt as _};
use std::sync::Arc;

/// An object-store backed shuffle store.
///
/// Partitions are stored as Arrow IPC stream files at paths:
///   `<prefix>/<job_id>/<stage_id>/<partition>.ipc`
///
/// Assignment lease tokens are tracked in memory so zombie writers cannot
/// overwrite committed partitions after a task retry.
pub struct ObjectStoreShuffleStore {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: object_store::path::Path,
    lease_tokens: Arc<DashMap<PartitionKey, u64>>,
    compression: ShuffleCompression,
}

impl ObjectStoreShuffleStore {
    /// Create a new store backed by `store` rooted at `prefix`.
    pub fn new(store: Arc<dyn object_store::ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix_str = prefix.into();
        let prefix = if prefix_str.is_empty() {
            object_store::path::Path::default()
        } else {
            object_store::path::Path::from(prefix_str.as_str())
        };
        Self {
            store,
            prefix,
            lease_tokens: Arc::new(DashMap::new()),
            compression: ShuffleCompression::None,
        }
    }

    /// Set the IPC compression codec for written partitions.
    pub fn with_compression(mut self, compression: ShuffleCompression) -> Self {
        self.compression = compression;
        self
    }

    fn object_path(&self, id: &PartitionId) -> object_store::path::Path {
        let key = format!("{}/{}/{}.ipc", id.job_id, id.stage_id, id.partition);
        if self.prefix.as_ref().is_empty() {
            object_store::path::Path::from(key.as_str())
        } else {
            object_store::path::Path::from(format!("{}/{key}", self.prefix).as_str())
        }
    }

    fn job_prefix(&self, job_id: &str) -> object_store::path::Path {
        if self.prefix.as_ref().is_empty() {
            object_store::path::Path::from(job_id)
        } else {
            object_store::path::Path::from(format!("{}/{job_id}", self.prefix).as_str())
        }
    }
}

impl ShuffleStore for ObjectStoreShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        // Atomically compare-and-swap via DashMap entry API to close the
        // TOCTOU gap between read and insert.
        match self.lease_tokens.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut o) => {
                let expected = *o.get();
                if lease_token < expected {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
                o.insert(lease_token);
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(lease_token);
            }
        }
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
        match self.lease_tokens.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut o) => {
                let expected = *o.get();
                if lease_token < expected {
                    return Err(ShuffleError::StaleLeaseToken {
                        expected,
                        actual: lease_token,
                    });
                }
                // Advance the stored token so a zombie with the previous token
                // cannot win a race by writing before the replacement arrives.
                o.insert(lease_token);
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(lease_token);
            }
        }

        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

        let ipc_compression = match self.compression {
            ShuffleCompression::None => None,
            ShuffleCompression::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
            ShuffleCompression::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
        };
        let write_options = IpcWriteOptions::default()
            .try_with_compression(ipc_compression)
            .map_err(|e| ShuffleError::Io(e.to_string()))?;

        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new_with_options(&mut buf, &partition.schema, write_options)
            .map_err(|e| ShuffleError::Io(e.to_string()))?;
        for batch in &partition.batches {
            writer
                .write(batch)
                .map_err(|e| ShuffleError::Io(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| ShuffleError::Io(e.to_string()))?;

        self.store
            .put(
                &self.object_path(&partition.id),
                bytes::Bytes::from(buf).into(),
            )
            .await
            .map_err(|e| ShuffleError::Io(e.to_string()))?;
        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        use arrow::ipc::reader::StreamReader;

        let path = self.object_path(id);
        let result = self.store.get(&path).await;
        match result {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(ShuffleError::Io(e.to_string())),
            Ok(obj) => {
                let data = obj
                    .bytes()
                    .await
                    .map_err(|e| ShuffleError::Io(e.to_string()))?;
                let cursor = std::io::Cursor::new(data.as_ref());
                let mut reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| ShuffleError::Io(e.to_string()))?;
                let schema = reader.schema();
                let mut batches = Vec::new();
                for batch_result in &mut reader {
                    let batch = batch_result.map_err(|e| ShuffleError::Io(e.to_string()))?;
                    batches.push(batch);
                }
                Ok(Some(ShufflePartition {
                    id: id.clone(),
                    schema,
                    batches,
                }))
            }
        }
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        use futures::StreamExt as _;
        use futures::TryStreamExt;

        self.lease_tokens.retain(|(jid, _, _), _| jid != job_id);

        // P2.9: collect all object paths, then issue a single batch-delete stream
        // rather than O(N) serial round-trips.
        let prefix = self.job_prefix(job_id);
        let paths: Vec<object_store::path::Path> = self
            .store
            .list(Some(&prefix))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .map_err(|e| ShuffleError::Io(e.to_string()))?;

        self.store
            .delete_stream(
                futures::stream::iter(paths.into_iter().map(Ok::<_, object_store::Error>)).boxed(),
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| ShuffleError::Io(e.to_string()))?;

        Ok(())
    }
}
