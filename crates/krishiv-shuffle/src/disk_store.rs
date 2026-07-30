use crate::{
    PartitionId, ShuffleError, ShufflePartition, ShuffleResult, ShuffleStore, ShuffleStream,
    compression::{ShuffleCompression, parquet_writer_properties},
    error::{io_err, shuffle_write_lock},
    store::LeaseMap,
};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// A local-disk shuffle store that serialises partitions to Parquet files.
///
/// Each partition is written to `{base_dir}/{job_id}/{stage_id}/{partition}.parquet`.
/// Lease tokens are persisted to `{partition}.lease` sidecars so zombie writers
/// are rejected after executor restart.
#[derive(Clone)]
pub struct LocalDiskShuffleStore {
    base_dir: PathBuf,
    lease_tokens: LeaseMap,
    compression: ShuffleCompression,
    // In-memory hash tracking for strict verification on read (DashMap matches object_store.rs pattern)
    content_hashes: Arc<DashMap<crate::store::PartitionKey, [u8; 32]>>,
    // Ceiling on page cache held by committed-but-unconsumed partitions. Shared
    // across every store clone so the bound is per-process, not per-handle —
    // the kernel charges the container once, so there is only one budget to
    // spend.
    page_cache_budget: Arc<krishiv_common::page_cache::ShufflePageCacheBudget>,
}

/// Compute BLAKE3 hash over raw bytes already held in memory.
///
/// Used by the read path when the partition was small enough to read in one
/// pass, which costs nothing extra — the bytes are already there. The *write*
/// path deliberately does not use this: it hashes through [`HashingWriter`] as
/// bytes reach the disk, so it never needs a second copy of what it just wrote.
pub(crate) fn blake3_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Largest partition the read path will hold in memory to serve in one pass.
///
/// Above this it streams instead, so a pathological partition cannot exhaust
/// the executor. Peak read-side cost is this times the number of concurrent
/// partition reads. 32 MiB is roughly ten times the observed SF100 average
/// (~3.5 MB), so the streaming fallback is genuinely exceptional rather than
/// the case that quietly dominates.
pub(crate) const INLINE_READ_LIMIT: u64 = 32 * 1024 * 1024;

/// How much of a file to hash per `read` call when verifying on the read path.
///
/// Large enough that syscall overhead is irrelevant against a 270 MB/s disk,
/// small enough that verification costs a fixed 256 KiB regardless of partition
/// size. The previous read path allocated a `Vec<u8>` the size of the whole
/// partition purely so it could hash it.
const HASH_CHUNK_BYTES: usize = 256 * 1024;

/// Hash the file at `path` without materialising it.
///
/// Returns `Ok(None)` if the file does not exist, matching the read path's
/// treatment of a missing partition as "not present" rather than an error.
fn blake3_hash_file(path: &Path) -> std::io::Result<Option<[u8; 32]>> {
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; HASH_CHUNK_BYTES];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        // `Read` promises `n <= buf.len()`; a reader that breaks that promise
        // would silently corrupt the digest, so refuse rather than panic.
        let chunk = buf
            .get(..n)
            .ok_or_else(|| std::io::Error::other("reader reported more bytes than requested"))?;
        hasher.update(chunk);
    }
    Ok(Some(*hasher.finalize().as_bytes()))
}

/// A `Write` adapter that BLAKE3-hashes bytes as they pass through to `inner`.
///
/// This exists so the shuffle write path can produce its content hash without a
/// second pass over the data. The digest covers exactly the bytes that reach the
/// file, which is what the read path re-computes, so the two remain comparable.
struct HashingWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
}

impl<W: std::io::Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
        }
    }

    /// Consume the adapter, returning the wrapped writer and the digest of
    /// everything written through it.
    fn finish(self) -> (W, [u8; 32]) {
        let digest = *self.hasher.finalize().as_bytes();
        (self.inner, digest)
    }
}

impl<W: std::io::Write> std::io::Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Hash only what the inner writer accepted, so a short write cannot
        // desynchronise the digest from the file's contents.
        let n = self.inner.write(buf)?;
        // `Write` promises `n <= buf.len()`; hashing outside the slice the
        // writer actually accepted would desynchronise the digest, so a writer
        // that breaks that promise is an error, not a panic.
        let accepted = buf
            .get(..n)
            .ok_or_else(|| std::io::Error::other("writer accepted more bytes than offered"))?;
        self.hasher.update(accepted);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// A partition's identity and schema, without its data.
///
/// The streaming write path needs both before the first batch arrives (the
/// paths come from the id, `ArrowWriter` needs the schema), but must not hold
/// the batches — that is the whole point. Keeping them in one struct means the
/// blocking writer captures exactly this and nothing else.
struct PartitionMeta {
    id: PartitionId,
    schema: arrow::datatypes::SchemaRef,
}

/// Removes the staging files unless the write committed.
///
/// Before this, any failure after `File::create` — a Parquet write error, a
/// source-stream error, a poisoned lease lock — left `*.tmp.N` behind, and the
/// only thing that ever removed those was `cleanup_temp_files` at store
/// construction, i.e. at executor boot. On a node that filled its disk, that
/// boot is exactly what cannot happen. Same failure mode as the spill files in
/// `krishiv_shuffle::orphan`, and the same fix: reclaim at the point of
/// failure, not at the next start.
struct StagingFiles {
    data: PathBuf,
    hash: PathBuf,
    committed: bool,
}

impl Drop for StagingFiles {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _ = std::fs::remove_file(&self.data);
        let _ = std::fs::remove_file(&self.hash);
    }
}

fn encode_hash(hash: &[u8; 32]) -> String {
    hash.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
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
        let high = nibble(*chunk.first()?)?;
        let low = nibble(*chunk.get(1)?)?;
        *hash.get_mut(idx)? = (high << 4) | low;
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
            lease_tokens: Arc::new(RwLock::new(ahash::AHashMap::default())),
            compression: crate::compression::default_storage_compression(),
            content_hashes: Arc::new(DashMap::new()),
            page_cache_budget: Arc::new(
                krishiv_common::page_cache::ShufflePageCacheBudget::from_capacity(
                    &krishiv_common::executor_capacity::ExecutorCapacity::detect(),
                ),
            ),
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
                .ok_or_else(|| io_err(format!("invalid shuffle lease file '{}'", path.display())))
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

    /// Offloads blocking filesystem
    /// operations (`std::fs::read`, `create_dir_all`, `write`) to
    /// `spawn_blocking` so the async executor thread is not stalled.
    ///
    /// The in-memory lease token map is still updated synchronously under the
    /// `lease_tokens` lock; only the FS read/persist happens in the blocking
    /// pool. This satisfies the AGENTS.md rule: "do not hide blocking filesystem
    /// or database work inside async tasks."
    async fn resolve_lease_token_async(
        &self,
        id: &PartitionId,
        incoming: u64,
    ) -> ShuffleResult<u64> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);

        // Check the in-memory map for the current token.
        let memory = shuffle_write_lock(&self.lease_tokens)?.get(&key).copied();

        // Fast path: if the in-memory map already has a token, check
        // monotonicity without an FS read. A stale incoming token is rejected
        // here (same as enforce_monotonic_lease would do).
        if let Some(mem_token) = memory
            && incoming < mem_token
        {
            return Err(crate::ShuffleError::StaleLeaseToken {
                expected: mem_token,
                actual: incoming,
            });
        }

        // Phase 1: read persisted lease (if needed) in spawn_blocking.
        let persisted: Option<u64> = if memory.is_some() {
            None
        } else {
            let id_clone = id.clone();
            let this = self.clone();
            let result = tokio::task::spawn_blocking(move || this.load_persisted_lease(&id_clone))
                .await
                .map_err(|e| io_err(format!("lease read task panicked: {e}")))?;
            result?
        };

        let current = memory.or(persisted);
        let next = crate::lease_persistence::enforce_monotonic_lease(current, incoming)?;

        // Re-acquire the write lock and verify the in-memory token hasn't been
        // updated by a concurrent task while we were reading from disk.
        {
            let mut tokens = shuffle_write_lock(&self.lease_tokens)
                .map_err(|e| io_err(format!("lease token lock poisoned: {e}")))?;
            if let Some(&current_in_mem) = tokens.get(&key)
                && current_in_mem > next
            {
                return Err(crate::ShuffleError::StaleLeaseToken {
                    expected: current_in_mem,
                    actual: next,
                });
            }
            tokens.insert(key.clone(), next);
        }

        // Phase 2: persist the new lease token in spawn_blocking.
        // Skip the FS write if the token is unchanged.
        if Some(next) != current {
            let id_clone = id.clone();
            let this = self.clone();
            tokio::task::spawn_blocking(move || this.persist_lease(&id_clone, next))
                .await
                .map_err(|e| io_err(format!("lease persist task panicked: {e}")))??;
        }

        Ok(next)
    }
}

#[async_trait::async_trait]
impl ShuffleStore for LocalDiskShuffleStore {
    async fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        crate::validate_safe_id(&id.job_id, "job_id")?;
        crate::validate_safe_id(&id.stage_id, "stage_id")?;
        self.resolve_lease_token_async(&id, lease_token).await?;
        Ok(())
    }

    /// Write a fully-materialised partition.
    ///
    /// Delegates to [`Self::write_partition_stream`] so both entry points share
    /// one implementation: there is exactly one place that opens the temp file,
    /// hashes, and commits, and no way for the two to drift apart on the lease
    /// protocol or the rename order.
    async fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let ShufflePartition { id, schema, batches } = partition;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        self.write_partition_stream(id, schema, Box::pin(stream), lease_token)
            .await
    }

    /// Write a partition without ever holding it whole.
    ///
    /// Batches are pulled from `batches` on the async side and handed to a
    /// blocking `ArrowWriter` over a depth-2 channel, so at most a couple of
    /// batches are in flight and each is dropped as soon as it has been
    /// serialised. Peak write-side memory is therefore one batch, not one
    /// partition — see [`ShuffleStore::write_partition_stream`] for the pool
    /// starvation that made this necessary.
    async fn write_partition_stream(
        &self,
        id: PartitionId,
        schema: arrow::datatypes::SchemaRef,
        batches: crate::store::ShuffleBatchStream,
        lease_token: u64,
    ) -> ShuffleResult<()> {
        let partition = PartitionMeta { id, schema };
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
        // Use the async variant so blocking FS operations (lease read/persist)
        // are offloaded to spawn_blocking.
        {
            let _ = self
                .resolve_lease_token_async(&partition.id, lease_token)
                .await?;
        }

        let final_path = self.partition_path(&partition.id)?;
        let final_hash_path = self.partition_hash_path(&partition.id)?;
        let writer_props = parquet_writer_properties(self.compression);
        let lease_tokens = Arc::clone(&self.lease_tokens);
        let content_hashes = Arc::clone(&self.content_hashes);
        let page_cache_budget = Arc::clone(&self.page_cache_budget);
        let parent_dir = final_path.parent().map(PathBuf::from);

        // Depth 2: enough that the producer is never the reason the writer
        // stalls, small enough that "in flight" is a couple of batches rather
        // than a partition. This bound is the whole point of the streaming
        // path, so it must stay small.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ShuffleResult<arrow::record_batch::RecordBatch>>(2);

        // P0.4: Wrap all blocking filesystem I/O in spawn_blocking so the
        // async executor thread is never stalled by synchronous disk calls.
        let writer_task = tokio::task::spawn_blocking(move || {
            use parquet::arrow::ArrowWriter;
            use std::sync::atomic::{AtomicU64, Ordering};

            // ENOSPC (errno 28) or StorageFull — surface as DiskFull so callers
            // know not to retry the write indefinitely.
            fn wrap_io_err(e: std::io::Error, path: &std::path::Path) -> ShuffleError {
                if e.kind() == std::io::ErrorKind::StorageFull || e.raw_os_error() == Some(28) {
                    ShuffleError::DiskFull {
                        path: path.to_string_lossy().into_owned(),
                        source: e,
                    }
                } else {
                    io_err(e.to_string())
                }
            }

            // Use a process-local counter for unique temp file names.
            static TMP_CTR: AtomicU64 = AtomicU64::new(1);
            let tmp_suffix = TMP_CTR.fetch_add(1, Ordering::Relaxed);

            if let Some(parent) = final_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| wrap_io_err(e, parent))?;
            }

            // Phase 1 (continued): Write to a temp file alongside the final path.
            let tmp_path = final_path.with_extension(format!("tmp.{tmp_suffix}"));
            let tmp_hash_path = final_hash_path.with_extension(format!("blake3.tmp.{tmp_suffix}"));
            // Every `?` from here on unlinks both staging files on the way out.
            let mut staging = StagingFiles {
                data: tmp_path.clone(),
                hash: tmp_hash_path.clone(),
                committed: false,
            };
            // The BLAKE3 hash is computed as the Parquet bytes flow to disk.
            //
            // This used to write the file, then `std::fs::read` the whole thing
            // back to hash it — a second, full-size copy of every shuffle
            // partition in anonymous memory, accounted by no budget, once per
            // partition per task. At SF100 that is the difference between a
            // bounded writer and an unbounded one, and it is invisible to the
            // DataFusion pool, so it never showed up as a spill. Hashing in-line
            // is equivalent: the digest still covers exactly the bytes in the
            // file, which is what the read path re-computes and compares.
            let hash = {
                let tmp_file =
                    std::fs::File::create(&tmp_path).map_err(|e| wrap_io_err(e, &tmp_path))?;
                let schema = partition.schema.clone();
                let mut writer =
                    ArrowWriter::try_new(HashingWriter::new(tmp_file), schema, Some(writer_props))
                        .map_err(|e| io_err(format!("failed to create Parquet writer: {e}")))?;
                // Pull batches as the producer yields them and drop each one as
                // soon as it is serialised, so the writer's residency is a batch
                // rather than a partition.
                while let Some(batch) = rx.blocking_recv() {
                    let batch = batch?;
                    writer
                        .write(&batch)
                        .map_err(|e| io_err(format!("failed to write Parquet batch: {e}")))?;
                }
                let (tmp_file, hash) = writer
                    .into_inner()
                    .map_err(|e| io_err(format!("failed to finalize Parquet writer: {e}")))?
                    .finish();
                // S4: Sync temp file to durable storage before commit.
                tmp_file.sync_all().map_err(|e| wrap_io_err(e, &tmp_path))?;
                hash
            };
            {
                let mut hash_file = std::fs::File::create(&tmp_hash_path)
                    .map_err(|e| wrap_io_err(e, &tmp_hash_path))?;
                use std::io::Write;
                hash_file
                    .write_all(encode_hash(&hash).as_bytes())
                    .map_err(|e| wrap_io_err(e, &tmp_hash_path))?;
                hash_file
                    .sync_all()
                    .map_err(|e| wrap_io_err(e, &tmp_hash_path))?;
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
                // SH5: rename the primary data file first, fsync it, then
                // rename the hash sidecar. The gap analysis identified the
                // previous order (hash first, then data) as the bug: a
                // crash between the two renames left a hash sidecar that
                // pointed at a non-existent data file, and the read path
                // tripped `ContentHashMismatch` on a partition that was
                // actually uncorrupted.
                //
                // Crash-safety:
                // - Crash before any rename: both files are still temp (safe).
                // - Crash after data rename only: the data file exists
                //   without its hash. The read path treats the missing
                //   sidecar as "no verification" (warn + skip) so the
                //   partition is still readable.
                // - Crash after both renames: fully committed.
                // From here the staging files are being *moved*, not abandoned:
                // a rename leaves nothing at the old path, and a failure
                // between the two renames is handled by the read path's
                // missing-sidecar tolerance rather than by deleting committed
                // data. Disarm the guard before the first rename so it can
                // never unlink a file that is now the live partition.
                staging.committed = true;
                std::fs::rename(&tmp_path, &final_path).map_err(|e| {
                    io_err(format!(
                        "failed to rename temp partition '{}' → '{}': {e}",
                        tmp_path.display(),
                        final_path.display()
                    ))
                })?;
                // fsync the data file before publishing its hash sidecar.
                // The kernel may otherwise reorder the sidecar rename
                // ahead of the data's metadata flush, leaving a window
                // where the data is on disk in name only.
                if let Ok(f) = std::fs::File::open(&final_path)
                    && let Err(e) = f.sync_all()
                {
                    tracing::warn!(
                        path = %final_path.display(),
                        error = %e,
                        "failed to fsync data file; durability may be compromised"
                    );
                }
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

                // Register the committed partition against the page-cache
                // ceiling. This does NOT evict it: a reduce task on this node
                // may read it back shortly, and serving that from RAM instead of
                // a 270 MB/s disk is exactly what the cache is for. What it does
                // is bound the total, so the oldest partitions are dropped once
                // the working set exceeds the budget. The unbounded growth — a
                // partition from the first stage still resident ten stages later
                // — was the actual cause of the OOM kills, not the caching
                // itself. Safe here because the data was fsynced above.
                let cached_bytes = std::fs::metadata(&final_path).map(|m| m.len()).unwrap_or(0);
                page_cache_budget.record_written(final_path.clone(), cached_bytes);
            } else {
                // Newer writer won. `staging` is still armed, so returning
                // here unlinks both temp files.
                // B4: Report the actual current token as `expected`, not
                // `lease_token + 1` (which was wrong when tokens advance by more than 1).
                let current = {
                    let tokens = lease_tokens
                        .read()
                        .map_err(|_| io_err("lease token lock poisoned"))?;
                    tokens.get(&key).copied().unwrap_or(0)
                };
                return Err(ShuffleError::StaleLeaseToken {
                    expected: current,
                    actual: lease_token,
                });
            }
            Ok(())
        });

        // Feed the writer. A send failure means the writer already returned, so
        // its error — not a channel-closed message — is what the caller needs;
        // stop feeding and let the join below report it.
        {
            use futures::StreamExt as _;
            let mut batches = batches;
            while let Some(batch) = batches.next().await {
                if tx.send(batch).await.is_err() {
                    break;
                }
            }
        }
        // Closing the channel is what ends the writer's receive loop. It must
        // happen before the await or the two deadlock: the writer would block
        // on `blocking_recv` forever while this task blocks on the join.
        drop(tx);

        writer_task
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

    /// One `stat`, no read — see [`ShuffleStore::partition_bytes`].
    async fn partition_bytes(&self, id: &PartitionId) -> ShuffleResult<Option<u64>> {
        let path = self.partition_path(id)?;
        let result = tokio::task::spawn_blocking(move || match std::fs::metadata(&path) {
            Ok(m) => Ok(Some(m.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            // A stat that fails for any other reason is not worth failing the
            // fetch over: the caller only uses this to size an admission
            // charge, and "unknown" already has a safe meaning there.
            Err(_) => Ok(None),
        })
        .await
        .map_err(|e| io_err(format!("partition stat join: {e}")))?;
        result
    }

    async fn stream_partition(&self, id: &PartitionId) -> ShuffleResult<Option<ShuffleStream>> {
        let path = self.partition_path(id)?;
        let hash_path = self.partition_hash_path(id)?;
        let evict_path = path.clone();
        let id = id.clone();
        let id_clone = id.clone();
        let content_hashes = Arc::clone(&self.content_hashes);
        let page_cache_budget = Arc::clone(&self.page_cache_budget);

        let result = tokio::task::spawn_blocking(move || {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

            // Read the partition in ONE pass when it is small enough to hold,
            // which is the overwhelmingly common case.
            //
            // The original code slurped every partition into a `Vec<u8>` with no
            // ceiling — fast, but one oversized partition could exhaust the
            // executor. Replacing that with "always stream" traded an unbounded
            // buffer for a doubled read: a full pass to verify the hash, then the
            // Parquet reader issuing positioned reads for row groups, both
            // against a 270 MB/s disk. That is the wrong trade at the sizes that
            // actually occur — SF100 shuffle partitions here average ~3.5 MB
            // (8.6 GB across ~2500 files), so the buffer was never the problem;
            // its unboundedness was.
            //
            // Bound it instead: below the threshold, one sequential read serves
            // both the hash check and the Parquet reader from memory. Above it,
            // fall back to streaming so a pathological partition still cannot
            // blow the executor. Peak cost is therefore
            // `INLINE_READ_LIMIT × concurrent reads`, which is accountable.
            let data_len = match std::fs::metadata(&path) {
                Ok(m) => m.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(io_err(format!(
                        "failed to stat partition file '{}': {e}",
                        path.display()
                    )));
                }
            };
            let inline_bytes: Option<Vec<u8>> = if data_len <= INLINE_READ_LIMIT {
                match std::fs::read(&path) {
                    Ok(b) => Some(b),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => {
                        return Err(io_err(format!(
                            "failed to read partition file '{}': {e}",
                            path.display()
                        )));
                    }
                }
            } else {
                None
            };

            let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
            // SH5: a missing hash sidecar is not a corruption signal —
            // it means the data file was committed but the sidecar
            // rename was lost (or the sidecar was never written in a
            // pre-sidecar build). Log a warning and skip the verification
            // rather than failing the read with `ContentHashMismatch`.
            let persisted_hash: Option<[u8; 32]> = match std::fs::read(&hash_path) {
                Ok(bytes) => match decode_hash(&bytes) {
                    Some(h) => Some(h),
                    None => {
                        return Err(ShuffleError::ContentHashMismatch {
                            partition: format!("{:?}", key),
                            expected: "64 lowercase hex blake3 digest".to_string(),
                            actual: "unparseable sidecar".to_string(),
                        });
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!(
                        partition = ?key,
                        data_path = %path.display(),
                        "shuffle partition has no hash sidecar; skipping verification"
                    );
                    None
                }
                Err(e) => {
                    return Err(io_err(format!(
                        "failed to read partition hash file '{}': {e}",
                        hash_path.display()
                    )));
                }
            };

            if let Some(persisted_hash) = persisted_hash {
                if let Some(stored_ref) = content_hashes.get(&key)
                    && *stored_ref != persisted_hash
                {
                    return Err(ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: encode_hash(stored_ref.value()),
                        actual: encode_hash(&persisted_hash),
                    });
                }
                // Hash the buffer we already hold when we have one — no second
                // trip to disk. Only the oversized fallback re-reads, and it
                // streams in fixed chunks so verification stays O(1) in memory.
                let computed = match &inline_bytes {
                    Some(bytes) => blake3_hash(bytes),
                    None => match blake3_hash_file(&path) {
                        Ok(Some(h)) => h,
                        // The file vanished between stat and verify: a
                        // concurrent cleanup won, which the caller treats as
                        // "not present".
                        Ok(None) => return Ok(None),
                        Err(e) => {
                            return Err(io_err(format!(
                                "failed to hash partition file '{}': {e}",
                                path.display()
                            )));
                        }
                    },
                };
                if computed != persisted_hash {
                    return Err(ShuffleError::ContentHashMismatch {
                        partition: format!("{:?}", key),
                        expected: encode_hash(&persisted_hash),
                        actual: encode_hash(&computed),
                    });
                }
            }

            // Serve Parquet from the buffer we already read; only the oversized
            // path opens the file and lets the reader fetch row groups on demand.
            // The two builders are different types (`Bytes` vs `File` readers),
            // so each branch finishes its own build and they meet at the reader,
            // which is the same type either way.
            let (schema, reader) = match inline_bytes {
                Some(bytes) => {
                    let builder =
                        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                            .map_err(|e| io_err(format!("failed to build Parquet reader: {e}")))?;
                    let schema = builder.schema().clone();
                    let reader = builder.build().map_err(|e| {
                        io_err(format!("failed to build Parquet batch reader: {e}"))
                    })?;
                    (schema, reader)
                }
                None => {
                    let data_file = match std::fs::File::open(&path) {
                        Ok(f) => f,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                        Err(e) => {
                            return Err(io_err(format!(
                                "failed to open partition file '{}': {e}",
                                path.display()
                            )));
                        }
                    };
                    let builder = ParquetRecordBatchReaderBuilder::try_new(data_file)
                        .map_err(|e| io_err(format!("failed to build Parquet reader: {e}")))?;
                    let schema = builder.schema().clone();
                    let reader = builder.build().map_err(|e| {
                        io_err(format!("failed to build Parquet batch reader: {e}"))
                    })?;
                    (schema, reader)
                }
            };

            Ok::<_, ShuffleError>(Some((schema, reader)))
        })
        .await
        .map_err(|e| io_err(format!("spawn_blocking join error: {e}")))?;

        let Some((schema, reader)) = result? else {
            return Ok(None);
        };

        // Shuffle output is read once. After a partition has been served its
        // cache has no remaining value, so release it and return the budget —
        // this is what keeps the ceiling from being reached by live data in the
        // first place. Doing it on drop covers both the stream running to
        // completion and a fetch abandoned part-way.
        struct ReleaseOnDrop(
            PathBuf,
            Arc<krishiv_common::page_cache::ShufflePageCacheBudget>,
        );
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.1.record_consumed(&self.0);
            }
        }

        let stream = futures::stream::unfold(
            Some((reader, ReleaseOnDrop(evict_path, page_cache_budget))),
            move |state| async move {
                let (mut reader, guard) = state?;
                let res = tokio::task::spawn_blocking(move || {
                    reader.next().map(|batch_res| (batch_res, reader))
                })
                .await;

                match res {
                    Ok(Some((Ok(batch), reader))) => Some((Ok(batch), Some((reader, guard)))),
                    Ok(Some((Err(e), reader))) => Some((
                        Err(io_err(format!("error reading Parquet batch: {e}"))),
                        Some((reader, guard)),
                    )),
                    // Exhausted: `guard` drops here and the cache is released.
                    Ok(None) => None,
                    Err(e) => Some((Err(io_err(format!("spawn_blocking error: {e}"))), None)),
                }
            },
        );

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec())) as _],
        )
        .unwrap()
    }

    fn id(job: &str, stage: &str, part: u32) -> PartitionId {
        PartitionId {
            job_id: job.to_string(),
            stage_id: stage.to_string(),
            partition: part,
        }
    }

    /// The write path hashes as it streams to disk; the read path hashes by
    /// streaming the file back. Those two must agree, or every read fails
    /// `ContentHashMismatch`.
    ///
    /// This is the invariant that let the full-file `std::fs::read` on the
    /// write path be removed: the digest is defined as "BLAKE3 of the bytes in
    /// the file", and both sides now compute that without materialising it. The
    /// partition here spans several `HASH_CHUNK_BYTES` chunks so the read side's
    /// chunk loop is genuinely exercised rather than completing in one pass.
    #[tokio::test]
    async fn streamed_write_hash_matches_streamed_read_hash_across_chunks() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let partition = id("job-chunked", "stage-1", 0);

        // ~400k rows of poorly-compressible values: comfortably more than one
        // 256 KiB hash chunk once written as Parquet.
        let values: Vec<i64> = (0..400_000i64)
            .map(|i| i.wrapping_mul(2_654_435_761))
            .collect();
        let sp = ShufflePartition {
            id: partition.clone(),
            schema: make_batch(&values).schema(),
            batches: vec![make_batch(&values)],
        };
        store.write_partition(sp, 1).await.expect("write");

        let path = store.partition_path(&partition).expect("path");
        assert!(
            std::fs::metadata(&path).unwrap().len() > HASH_CHUNK_BYTES as u64,
            "test needs a partition larger than one hash chunk to be meaningful"
        );

        // Reading verifies the sidecar against a freshly streamed digest.
        let read = store
            .read_partition(&partition)
            .await
            .expect("read must verify")
            .expect("partition present");
        let total: usize = read.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, values.len());

        // And the streaming digest agrees with the whole-buffer one, which is
        // what the sidecar was compared against before this path was changed.
        let streamed = blake3_hash_file(&path).unwrap().expect("file present");
        let whole_buffer = blake3_hash(&std::fs::read(&path).unwrap());
        assert_eq!(
            streamed, whole_buffer,
            "chunked hashing must equal whole-buffer hashing"
        );
    }

    /// Verification must still *catch* corruption. Removing the read-back is
    /// only safe if the hash is genuinely being checked — a fix that silently
    /// stopped verifying would also make this suite pass.
    #[tokio::test]
    async fn a_corrupted_partition_is_still_rejected() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let partition = id("job-corrupt", "stage-1", 0);
        let sp = ShufflePartition {
            id: partition.clone(),
            schema: make_batch(&[1, 2, 3]).schema(),
            batches: vec![make_batch(&[1, 2, 3])],
        };
        store.write_partition(sp, 1).await.expect("write");

        // Flip bytes in the middle of the committed file, leaving its length
        // and its sidecar untouched.
        let path = store.partition_path(&partition).expect("path");
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        // Drop the in-memory hash so the sidecar comparison is what runs.
        store.content_hashes.clear();

        let result = store.read_partition(&partition).await;
        assert!(
            matches!(result, Err(ShuffleError::ContentHashMismatch { .. })),
            "corrupted partition must be rejected, got {result:?}"
        );
    }

    /// SH5: a shuffle write must produce a data file *and* a hash
    /// sidecar; the read path must accept the data when the sidecar is
    /// present and verify it.
    #[tokio::test]
    async fn write_then_read_round_trips_with_hash() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let partition = id("job-1", "stage-1", 0);
        let sp = ShufflePartition {
            id: partition.clone(),
            schema: make_batch(&[1, 2, 3]).schema(),
            batches: vec![make_batch(&[1, 2, 3])],
        };
        store.write_partition(sp, 1).await.expect("write");
        let read = store
            .read_partition(&partition)
            .await
            .expect("read")
            .expect("partition present");
        assert_eq!(read.batches.len(), 1);
        assert_eq!(read.batches[0].num_rows(), 3);
    }

    /// The streaming write must produce a file the read path accepts, with the
    /// same rows in the same order as the collecting write.
    ///
    /// Checked against the collecting path directly rather than against a
    /// hard-coded expectation, because the whole claim of
    /// `write_partition_stream` is that it is the *same write* with a different
    /// residency — an assertion that could pass while the two diverged would
    /// prove nothing.
    #[tokio::test]
    async fn streaming_and_collecting_writes_produce_the_same_partition() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let batches: Vec<RecordBatch> = (0..8)
            .map(|g: i64| make_batch(&[g * 10, g * 10 + 1, g * 10 + 2]))
            .collect();
        let schema = batches[0].schema();

        let collected_id = id("job-collect", "stage-1", 0);
        store
            .write_partition(
                ShufflePartition {
                    id: collected_id.clone(),
                    schema: schema.clone(),
                    batches: batches.clone(),
                },
                1,
            )
            .await
            .expect("collecting write");

        let streamed_id = id("job-stream", "stage-1", 0);
        store
            .write_partition_stream(
                streamed_id.clone(),
                schema.clone(),
                Box::pin(futures::stream::iter(batches.clone().into_iter().map(Ok))),
                1,
            )
            .await
            .expect("streaming write");

        let values = |p: &ShufflePartition| -> Vec<i64> {
            p.batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("int column")
                        .values()
                        .to_vec()
                })
                .collect()
        };
        let a = store
            .read_partition(&collected_id)
            .await
            .unwrap()
            .expect("collected present");
        let b = store
            .read_partition(&streamed_id)
            .await
            .unwrap()
            .expect("streamed present");
        let expected: Vec<i64> = (0..8).flat_map(|g: i64| [g * 10, g * 10 + 1, g * 10 + 2]).collect();
        assert_eq!(values(&a), expected, "collecting write lost or reordered rows");
        assert_eq!(values(&b), expected, "streaming write lost or reordered rows");
    }

    /// A partition whose source stream fails part-way must not commit, and must
    /// not leave staging files behind.
    ///
    /// Before the `StagingFiles` guard, every failure after `File::create` left
    /// a `*.tmp.N`, and the only thing that ever removed those was
    /// `cleanup_temp_files` at store construction — i.e. at executor boot,
    /// which is exactly what a node that filled its disk cannot do.
    #[tokio::test]
    async fn a_failed_stream_commits_nothing_and_leaves_no_staging_files() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let partition = id("job-fail", "stage-1", 0);
        let good = make_batch(&[1, 2, 3]);
        let schema = good.schema();

        let items: Vec<ShuffleResult<RecordBatch>> = vec![
            Ok(good),
            Err(ShuffleError::Io(std::io::Error::other("upstream exploded"))),
        ];
        let err = store
            .write_partition_stream(
                partition.clone(),
                schema,
                Box::pin(futures::stream::iter(items)),
                1,
            )
            .await
            .expect_err("a failing source must fail the write");
        assert!(
            err.to_string().contains("upstream exploded"),
            "the source's error must reach the caller, got: {err}"
        );

        assert!(
            store.read_partition(&partition).await.unwrap().is_none(),
            "a failed write must not publish a partition"
        );
        let stage_dir = dir.path().join("job-fail").join("stage-1");
        let leftovers: Vec<String> = std::fs::read_dir(&stage_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.contains(".tmp."))
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            leftovers.is_empty(),
            "staging files must be reclaimed at the point of failure, found {leftovers:?}"
        );
    }

    /// SH5: a shuffle partition whose hash sidecar is missing must
    /// still be readable (with a warning, not a hard `ContentHashMismatch`
    /// error). This exercises the new data-first-then-hash rename order
    /// and the read-path softening.
    #[tokio::test]
    async fn missing_hash_sidecar_is_warned_not_failed() {
        let dir = tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).expect("store");
        let partition = id("job-1", "stage-1", 0);
        let sp = ShufflePartition {
            id: partition.clone(),
            schema: make_batch(&[1, 2, 3]).schema(),
            batches: vec![make_batch(&[1, 2, 3])],
        };
        store.write_partition(sp, 1).await.expect("write");
        // Remove the hash sidecar to simulate the "data committed, hash
        // rename lost" crash window.
        let partition_path = dir.path().join("job-1").join("stage-1").join("0.parquet");
        let hash_path = partition_path.with_extension("parquet.blake3");
        std::fs::remove_file(&hash_path).expect("remove hash");
        let read = store
            .read_partition(&partition)
            .await
            .expect("read must succeed even without hash sidecar")
            .expect("partition present");
        assert_eq!(read.batches.len(), 1);
        assert_eq!(read.batches[0].num_rows(), 3);
    }
}
