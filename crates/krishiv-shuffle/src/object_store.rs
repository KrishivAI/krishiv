use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore, store::PartitionKey,
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
        }
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
        if let Some(expected) = self.lease_tokens.get(&key).map(|entry| *entry)
            && lease_token != expected
        {
            return Err(ShuffleError::StaleLeaseToken {
                expected,
                actual: lease_token,
            });
        }
        self.lease_tokens.insert(key, lease_token);
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
        if let Some(expected) = self.lease_tokens.get(&key).map(|entry| *entry) {
            if lease_token < expected {
                return Err(ShuffleError::StaleLeaseToken {
                    expected,
                    actual: lease_token,
                });
            }
        } else {
            self.lease_tokens.insert(key, lease_token);
        }

        use arrow::ipc::writer::StreamWriter;

        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &partition.schema)
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
