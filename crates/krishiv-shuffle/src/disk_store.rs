use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore, ShuffleStream,
    compression::{ShuffleCompression, parquet_writer_properties},
    error::{io_err, shuffle_write_lock},
    store::LeaseMap,
};
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// A local-disk shuffle store that serialises partitions to Parquet files.
///
/// Each partition is written to `{base_dir}/{job_id}/{stage_id}/{partition}.parquet`.
/// Lease tokens are persisted to `{partition}.lease` sidecars so zombie writers
/// are rejected after executor restart.
pub struct LocalDiskShuffleStore {
    base_dir: PathBuf,
    lease_tokens: LeaseMap,
    compression: ShuffleCompression,
    // In-memory hash tracking for strict verification on read (DashMap matches object_store.rs pattern)
    content_hashes: Arc<DashMap<crate::store::PartitionKey, [u8; 32]>>,
}

/// Compute BLAKE3 hash over raw bytes (Parquet file content or similar).
fn compute_hash_bytes(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

fn encode_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hash(encoded: &[u8]) -> Option<[u8; 32]> {
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
        Self::cleanup_temp_files(&base_dir)?;
        Ok(Self {
            base_dir,
            lease_tokens: Arc::new(RwLock::new(BTreeMap::new())),
            compression: ShuffleCompression::None,
            content_hashes: Arc::new(DashMap::new()),
        })
    }

    fn cleanup_temp_files(dir: &Path) -> ShuffleResult<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir).map_err(|e| io_err(e.to_string()))? {
            let entry = entry.map_err(|e| io_err(e.to_string()))?;
            let ft = entry.file_type().map_err(|e| io_err(e.to_string()))?;
            let path = entry.path();
            if ft.is_dir() {
                Self::cleanup_temp_files(&path)?;
            } else if ft.is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.contains(".tmp.")
            {
                let _ = std::fs::remove_file(&path);
            }
        }
        Ok(())
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

    fn partition_hash_path(&self, id: &PartitionId) -> ShuffleResult<PathBuf> {
        let partition_path = self.partition_path(id)?;
        let mut path = partition_path.into_os_string();
        path.push(".blake3");
        Ok(PathBuf::from(path))
    }

    fn partition_lease_path(&self, id: &PartitionId) -> ShuffleResult<PathBuf> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        Ok(self
            .base_dir
            .join(&id.job_id)
            .join(&id.stage_id)
            .join(format!("{}.lease", id.partition)))
    }

    fn load_persisted_lease(&self, id: &PartitionId) -> ShuffleResult<Option<u64>> {
        let path = self.partition_lease_path(id)?;
        match std::fs::read(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(format!(
                "failed to read shuffle lease file '{}': {e}",
                path.display()
            ))),
            Ok(bytes) => crate::lease_persistence::decode_lease_token(&bytes)
                .ok_or_else(|| {
                    io_err(format!(
                        "invalid shuffle lease file '{}'",
                        path.display()
                    ))
                })
                .map(Some),
        }
    }

    fn persist_lease(&self, id: &PartitionId, token: u64) -> ShuffleResult<()> {
        let path = self.partition_lease_path(id)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                io_err(format!(
                    "failed to create shuffle lease dir '{}': {e}",
                    parent.display()
                ))
            })?;
        }
        std::fs::write(&path, crate::lease_persistence::encode_lease_token(token)).map_err(|e| {
            io_err(format!(
                "failed to write shuffle lease file '{}': {e}",
                path.display()
            ))
        })
    }

    fn resolve_lease_token(
        &self,
        id: &PartitionId,
        incoming: u64,
    ) -> ShuffleResult<u64> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        let memory = shuffle_write_lock(&self.lease_tokens)?
            .get(&key)
            .copied();
        let persisted = self.load_persisted_lease(id)?;
        let current = memory.or(persisted);
        let next = crate::lease_persistence::enforce_monotonic_lease(current, incoming)?;
        shuffle_write_lock(&self.lease_tokens)?.insert(key, next);
        self.persist_lease(id, next)?;
        Ok(next)
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
        let _ = self.resolve_lease_token(&id, lease_token)?;
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
        // Phase 1: validate initial token and advance it (persisted + in-memory).
        {
            let _ = self.resolve_lease_token(&partition.id, lease_token)?;
        }

        let final_path = self.partition_path(&partition.id)?;
        let final_hash_path = self.partition_hash_path(&partition.id)?;
        let writer_props = parquet_writer_properties(self.compression);
        let lease_tokens = Arc::clone(&self.lease_tokens);
        let content_hashes = Arc::clone(&self.content_hashes);
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
                let tmp_file = writer
                    .into_inner()
                    .map_err(|e| io_err(format!("failed to finalize Parquet writer: {e}")))?;
                tmp_file
                    .sync_all()
                    .map_err(|e| io_err(format!("failed to fsync temp file: {e}")))?;
            }

            // Compute BLAKE3 hash over the written Parquet bytes so write-time
            // and read-time hashes use the same encoding.
            let parquet_bytes = std::fs::read(&tmp_path)
                .map_err(|e| io_err(format!("failed to read temp file for hashing: {e}")))?;
            let hash = compute_hash_bytes(&parquet_bytes);
            let tmp_hash_path = final_hash_path.with_extension(format!("blake3.tmp.{tmp_suffix}"));
            {
                let mut hash_file = std::fs::File::create(&tmp_hash_path).map_err(|e| {
                    io_err(format!(
                        "failed to create temp partition hash file '{}': {e}",
                        tmp_hash_path.display()
                    ))
                })?;
                use std::io::Write;
                hash_file
                    .write_all(encode_hash(&hash).as_bytes())
                    .map_err(|e| io_err(format!("failed to write temp partition hash: {e}")))?;
                hash_file
                    .sync_all()
                    .map_err(|e| io_err(format!("failed to fsync temp partition hash: {e}")))?;
            }

            // Phase 2: Re-acquire the lock and commit via rename only if our token
            // is still the current winner.  If a newer writer advanced the token
            // past ours since phase 1, discard the temp file.
            let commit = {
                let tokens = lease_tokens
                    .read()
                    .map_err(|_| io_err("lease token lock poisoned"))?;
                tokens.get(&key).copied() == Some(lease_token)
            };

            if commit {
                std::fs::rename(&tmp_path, &final_path).map_err(|e| {
                    io_err(format!(
                        "failed to rename temp partition '{}' → '{}': {e}",
                        tmp_path.display(),
                        final_path.display()
                    ))
                })?;
                std::fs::rename(&tmp_hash_path, &final_hash_path).map_err(|e| {
                    io_err(format!(
                        "failed to rename temp partition hash '{}' → '{}': {e}",
                        tmp_hash_path.display(),
                        final_hash_path.display()
                    ))
                })?;
                // S4: Fsync the parent directory so the rename is durable.
                if let Some(ref parent) = parent_dir
                    && let Ok(dir) = std::fs::File::open(parent)
                {
                    dir.sync_all().ok();
                }

                // Store hash for strict read verification (DashMap — no lock management needed)
                content_hashes.insert(key.clone(), hash);
            } else {
                // Newer writer won — silently discard this temp file.
                let _ = std::fs::remove_file(&tmp_path);
                let _ = std::fs::remove_file(&tmp_hash_path);
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
        let id = id.clone();
        let stream_opt = self.stream_partition(&id).await?;
        let Some(mut stream) = stream_opt else {
            return Ok(None);
        };
        let mut batches = Vec::new();
        use futures::StreamExt;
        while let Some(batch_res) = stream.batches.next().await {
            batches.push(batch_res?);
        }
        Ok(Some(ShufflePartition {
            id,
            schema: stream.schema,
            batches,
        }))
    }

    async fn stream_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShuffleStream>> {
        let path = self.partition_path(id)?;
        let hash_path = self.partition_hash_path(id)?;
        let id = id.clone();
        let id_clone = id.clone();
        let content_hashes = Arc::clone(&self.content_hashes);

        let result = tokio::task::spawn_blocking(move || {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

            let raw_bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(io_err(format!(
                        "failed to read partition file '{}': {e}",
                        path.display()
                    )));
                }
            };

            let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
            let persisted_hash_bytes = std::fs::read(&hash_path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: "persisted blake3 sidecar".to_string(),
                        actual: "missing".to_string(),
                    }
                } else {
                    io_err(format!(
                        "failed to read partition hash file '{}': {e}",
                        hash_path.display()
                    ))
                }
            })?;
            let persisted_hash = decode_hash(&persisted_hash_bytes).ok_or_else(|| {
                ShuffleError::ContentHashMismatch {
                    partition: format!("{:?}", key),
                    expected: "64 lowercase hex blake3 digest".to_string(),
                    actual: String::from_utf8_lossy(&persisted_hash_bytes).into_owned(),
                }
            })?;

            if let Some(stored_ref) = content_hashes.get(&key)
                && *stored_ref != persisted_hash
            {
                return Err(ShuffleError::ContentHashMismatch {
                    partition: format!("{:?}", key),
                    expected: encode_hash(stored_ref.value()),
                    actual: encode_hash(&persisted_hash),
                });
            }

            let computed = compute_hash_bytes(&raw_bytes);
            if computed != persisted_hash {
                return Err(ShuffleError::ContentHashMismatch {
                    partition: format!("{:?}", key),
                    expected: encode_hash(&persisted_hash),
                    actual: encode_hash(&computed),
                });
            }

            let parquet_bytes_frozen = bytes::Bytes::from(raw_bytes);
            let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes_frozen)
                .map_err(|e| io_err(format!("failed to build Parquet reader: {e}")))?;
            let schema = builder.schema().clone();
            let reader = builder
                .build()
                .map_err(|e| io_err(format!("failed to build Parquet batch reader: {e}")))?;

            Ok::<_, ShuffleError>(Some((schema, reader)))
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))?;

        let Some((schema, reader)) = result? else {
            return Ok(None);
        };

        let stream = futures::stream::unfold(Some(reader), move |reader_opt| async move {
            let mut reader = reader_opt?;
            let res = tokio::task::spawn_blocking(move || {
                reader.next().map(|batch_res| (batch_res, reader))
            })
            .await;

            match res {
                Ok(Some((Ok(batch), reader))) => Some((Ok(batch), Some(reader))),
                Ok(Some((Err(e), reader))) => Some((
                    Err(io_err(format!("error reading Parquet batch: {e}"))),
                    Some(reader),
                )),
                Ok(None) => None,
                Err(e) => Some((Err(io_err(format!("spawn_blocking error: {e}"))), None)),
            }
        });

        Ok(Some(ShuffleStream {
            id: id_clone,
            schema,
            batches: Box::pin(stream),
        }))
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
        // Clean up content hashes for this job (DashMap — no lock management needed).
        self.content_hashes
            .retain(|(jid, _, _), _| jid != &job_id_owned);
        Ok(())
    }
}
