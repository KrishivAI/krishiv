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
    content_hashes: Arc<DashMap<PartitionKey, [u8; 32]>>,
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
            content_hashes: Arc::new(DashMap::new()),
            compression: ShuffleCompression::None,
        }
    }

    fn compute_content_hash(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }

    fn encode_content_hash(hash: &[u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in hash {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn decode_content_hash(encoded: &[u8]) -> Option<[u8; 32]> {
        fn nibble(byte: u8) -> Option<u8> {
            match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            }
        }

        let encoded = match encoded {
            [body @ .., b'\n'] | [body @ .., b'\r'] => body,
            body => body,
        };
        if encoded.len() != 64 {
            return None;
        }

        let mut hash = [0u8; 32];
        for (idx, chunk) in encoded.chunks_exact(2).enumerate() {
            let high = nibble(chunk[0])?;
            let low = nibble(chunk[1])?;
            hash[idx] = (high << 4) | low;
        }
        Some(hash)
    }

    /// Set the IPC compression codec for written partitions.
    pub fn with_compression(mut self, compression: ShuffleCompression) -> Self {
        self.compression = compression;
        self
    }

    fn object_path(&self, id: &PartitionId) -> ShuffleResult<object_store::path::Path> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        let key = format!("{}/{}/{}.ipc", id.job_id, id.stage_id, id.partition);
        if self.prefix.as_ref().is_empty() {
            Ok(object_store::path::Path::from(key.as_str()))
        } else {
            Ok(object_store::path::Path::from(
                format!("{}/{key}", self.prefix).as_str(),
            ))
        }
    }

    fn hash_object_path(&self, id: &PartitionId) -> ShuffleResult<object_store::path::Path> {
        let ipc_path = self.object_path(id)?;
        Ok(object_store::path::Path::from(
            format!("{ipc_path}.blake3").as_str(),
        ))
    }

    fn lease_object_path(&self, id: &PartitionId) -> ShuffleResult<object_store::path::Path> {
        let ipc_path = self.object_path(id)?;
        Ok(object_store::path::Path::from(
            format!("{ipc_path}.lease").as_str(),
        ))
    }

    async fn load_persisted_lease(&self, id: &PartitionId) -> ShuffleResult<Option<u64>> {
        let path = self.lease_object_path(id)?;
        match self.store.get(&path).await {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(crate::error::io_err(e.to_string())),
            Ok(obj) => {
                let bytes = obj
                    .bytes()
                    .await
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
                crate::lease_persistence::decode_lease_token(bytes.as_ref())
                    .ok_or_else(|| {
                        ShuffleError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid shuffle lease sidecar",
                        ))
                    })
                    .map(Some)
            }
        }
    }

    async fn persist_lease(&self, id: &PartitionId, token: u64) -> ShuffleResult<()> {
        self.store
            .put(
                &self.lease_object_path(id)?,
                bytes::Bytes::from(crate::lease_persistence::encode_lease_token(token)).into(),
            )
            .await
            .map_err(|e| crate::error::io_err(e.to_string()))?;
        Ok(())
    }

    async fn resolve_lease_token(
        &self,
        key: &PartitionKey,
        id: &PartitionId,
        incoming: u64,
    ) -> ShuffleResult<u64> {
        // B3: Use DashMap::entry to atomically read-validate-write the in-memory
        // token under a shard lock, preventing two concurrent writers from both
        // passing the monotonic check.
        //
        // We must release the DashMap shard lock before performing the async
        // object-store persist, so we compute the new token under the lock and
        // then persist it after releasing.
        let memory = self.lease_tokens.get(key).map(|entry| *entry);

        // If we have an in-memory token and it is higher than `incoming`, reject
        // immediately without an object-store read.
        if let Some(mem_token) = memory
            && incoming < mem_token
        {
            return Err(crate::ShuffleError::StaleLeaseToken {
                expected: mem_token,
                actual: incoming,
            });
        }

        let persisted = if memory.is_none() {
            self.load_persisted_lease(id).await?
        } else {
            None
        };
        let current = memory.or(persisted);
        let next = crate::lease_persistence::enforce_monotonic_lease(current, incoming)?;

        // Atomic update: use entry() to hold the shard lock for the full
        // read-modify-write cycle, preventing another concurrent writer from
        // sneaking in between our validate and our insert.
        use dashmap::mapref::entry::Entry;
        match self.lease_tokens.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                let current_in_map = *e.get();
                if incoming < current_in_map {
                    return Err(crate::ShuffleError::StaleLeaseToken {
                        expected: current_in_map,
                        actual: incoming,
                    });
                }
                e.insert(next);
            }
            Entry::Vacant(e) => {
                e.insert(next);
            }
        }

        self.persist_lease(id, next).await?;
        Ok(next)
    }

    fn job_prefix(&self, job_id: &str) -> ShuffleResult<object_store::path::Path> {
        crate::validate_safe_id(job_id, "job_id")?;
        if self.prefix.as_ref().is_empty() {
            Ok(object_store::path::Path::from(job_id))
        } else {
            Ok(object_store::path::Path::from(
                format!("{}/{job_id}", self.prefix).as_str(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl ShuffleStore for ObjectStoreShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        let _ = self.resolve_lease_token(&key, &id, lease_token).await?;
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
        let _ = self
            .resolve_lease_token(&key, &partition.id, lease_token)
            .await?;

        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

        let ipc_compression = match self.compression {
            ShuffleCompression::None => None,
            ShuffleCompression::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
            ShuffleCompression::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
        };
        let write_options = IpcWriteOptions::default()
            .try_with_compression(ipc_compression)
            .map_err(|e| crate::error::io_err(e.to_string()))?;

        let mut buf = Vec::new();
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, &partition.schema, write_options)
                .map_err(|e| crate::error::io_err(e.to_string()))?;
        for batch in &partition.batches {
            writer
                .write(batch)
                .map_err(|e| crate::error::io_err(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| crate::error::io_err(e.to_string()))?;

        let hash = Self::compute_content_hash(&buf);

        self.store
            .put(
                &self.object_path(&partition.id)?,
                bytes::Bytes::from(buf).into(),
            )
            .await
            .map_err(|e| crate::error::io_err(e.to_string()))?;
        self.store
            .put(
                &self.hash_object_path(&partition.id)?,
                bytes::Bytes::from(Self::encode_content_hash(&hash)).into(),
            )
            .await
            .map_err(|e| crate::error::io_err(e.to_string()))?;

        // Keep an in-process cache, but persisted sidecars are the source of
        // truth after a reader restart.
        self.content_hashes.insert(key, hash);
        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShufflePartition>> {
        use arrow::ipc::reader::StreamReader;

        let path = self.object_path(id)?;
        let result = self.store.get(&path).await;
        match result {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(crate::error::io_err(e.to_string())),
            Ok(obj) => {
                let data = obj
                    .bytes()
                    .await
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
                let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
                let persisted_hash_path = self.hash_object_path(id)?;
                let persisted_hash = match self.store.get(&persisted_hash_path).await {
                    Err(object_store::Error::NotFound { .. }) => {
                        return Err(ShuffleError::ContentHashMismatch {
                            partition: format!("{:?}", key),
                            expected: "persisted blake3 sidecar".to_string(),
                            actual: "missing".to_string(),
                        });
                    }
                    Err(e) => return Err(crate::error::io_err(e.to_string())),
                    Ok(hash_obj) => {
                        let hash_bytes = hash_obj
                            .bytes()
                            .await
                            .map_err(|e| crate::error::io_err(e.to_string()))?;
                        Self::decode_content_hash(hash_bytes.as_ref()).ok_or_else(|| {
                            ShuffleError::ContentHashMismatch {
                                partition: format!("{:?}", key),
                                expected: "64 lowercase hex blake3 digest".to_string(),
                                actual: String::from_utf8_lossy(hash_bytes.as_ref()).into_owned(),
                            }
                        })?
                    }
                };

                if let Some(stored_ref) = self.content_hashes.get(&key)
                    && *stored_ref != persisted_hash
                {
                    return Err(ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: Self::encode_content_hash(stored_ref.value()),
                        actual: Self::encode_content_hash(&persisted_hash),
                    });
                }

                let computed = Self::compute_content_hash(data.as_ref());
                if computed != persisted_hash {
                    return Err(ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: Self::encode_content_hash(&persisted_hash),
                        actual: Self::encode_content_hash(&computed),
                    });
                }

                let cursor = std::io::Cursor::new(data.as_ref());
                let mut reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
                let schema = reader.schema();
                let mut batches = Vec::new();
                for batch_result in &mut reader {
                    let batch = batch_result.map_err(|e| crate::error::io_err(e.to_string()))?;
                    batches.push(batch);
                }
                let partition = ShufflePartition {
                    id: id.clone(),
                    schema,
                    batches,
                };

                Ok(Some(partition))
            }
        }
    }

    /// Stream IPC batches lazily instead of materialising all into a Vec.
    ///
    /// The full object must still be fetched upfront for content-hash
    /// verification, but individual `RecordBatch`es are yielded one at a time
    /// via `spawn_blocking` rather than being pre-collected.
    async fn stream_partition(
        &self,
        id: &PartitionId,
    ) -> ShuffleResult<Option<crate::ShuffleStream>> {
        use arrow::ipc::reader::StreamReader;

        let path = self.object_path(id)?;
        let data = match self.store.get(&path).await {
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(crate::error::io_err(e.to_string())),
            Ok(obj) => obj
                .bytes()
                .await
                .map_err(|e| crate::error::io_err(e.to_string()))?,
        };

        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        let id_clone = id.clone();

        let persisted_hash_path = self.hash_object_path(id)?;
        let persisted_hash = match self.store.get(&persisted_hash_path).await {
            Err(object_store::Error::NotFound { .. }) => {
                return Err(ShuffleError::ContentHashMismatch {
                    partition: format!("{:?}", key),
                    expected: "persisted blake3 sidecar".to_string(),
                    actual: "missing".to_string(),
                });
            }
            Err(e) => return Err(crate::error::io_err(e.to_string())),
            Ok(hash_obj) => {
                let hash_bytes = hash_obj
                    .bytes()
                    .await
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
                Self::decode_content_hash(hash_bytes.as_ref()).ok_or_else(|| {
                    ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: "64 lowercase hex blake3 digest".to_string(),
                        actual: String::from_utf8_lossy(hash_bytes.as_ref()).into_owned(),
                    }
                })?
            }
        };

        if let Some(stored_ref) = self.content_hashes.get(&key)
            && *stored_ref != persisted_hash
        {
            return Err(ShuffleError::ContentHashMismatch {
                partition: format!("{:?}", key),
                expected: Self::encode_content_hash(stored_ref.value()),
                actual: Self::encode_content_hash(&persisted_hash),
            });
        }
        let computed = Self::compute_content_hash(data.as_ref());
        if computed != persisted_hash {
            return Err(ShuffleError::ContentHashMismatch {
                partition: format!("{:?}", key),
                expected: Self::encode_content_hash(&persisted_hash),
                actual: Self::encode_content_hash(&computed),
            });
        }

        // B2: The IPC bytes from the object store are NOT outer-compressed —
        // compression is embedded at the IPC record-batch level. Pass `data`
        // directly to StreamReader without a double-decompress step.
        // Convert to Vec<u8> so the owned bytes can be moved into spawn_blocking.
        let ipc_bytes: Vec<u8> = data.to_vec();
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader =
            StreamReader::try_new(cursor, None).map_err(|e| crate::error::io_err(e.to_string()))?;
        let schema = reader.schema();

        let batch_stream = futures::stream::unfold(Some(reader), |reader_opt| async move {
            let mut reader = reader_opt?;
            let res = tokio::task::spawn_blocking(move || {
                reader.next().map(|batch_res| (batch_res, reader))
            })
            .await;
            match res {
                Ok(Some((Ok(batch), reader))) => Some((Ok(batch), Some(reader))),
                Ok(Some((Err(e), reader))) => Some((
                    Err(crate::error::io_err(format!("IPC batch read error: {e}"))),
                    Some(reader),
                )),
                Ok(None) => None,
                Err(e) => Some((
                    Err(crate::error::io_err(format!(
                        "spawn_blocking join error: {e}"
                    ))),
                    None,
                )),
            }
        });

        Ok(Some(crate::ShuffleStream {
            id: id_clone,
            schema,
            batches: Box::pin(batch_stream),
        }))
    }

    async fn delete_job_partitions(&self, job_id: &str) -> ShuffleResult<()> {
        use futures::StreamExt as _;
        use futures::TryStreamExt;

        self.lease_tokens.retain(|(jid, _, _), _| jid != job_id);
        self.content_hashes.retain(|(jid, _, _), _| jid != job_id);

        // P2.9: collect all object paths, then issue a single batch-delete stream
        // rather than O(N) serial round-trips.
        let prefix = self.job_prefix(job_id)?;
        let paths: Vec<object_store::path::Path> = self
            .store
            .list(Some(&prefix))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .map_err(|e| crate::error::io_err(e.to_string()))?;

        self.store
            .delete_stream(
                futures::stream::iter(paths.into_iter().map(Ok::<_, object_store::Error>)).boxed(),
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| crate::error::io_err(e.to_string()))?;

        Ok(())
    }
}
