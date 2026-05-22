#![forbid(unsafe_code)]

//! Shuffle store and hash partitioner for Krishiv.
//!
//! Provides local-disk shuffle write/read paths, an Arrow-based hash
//! partitioner, compression codec metadata, and orphan artifact detection.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use object_store::ObjectStoreExt as _;

use arrow::array::{
    Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt as _;

// ── ShufflePath ───────────────────────────────────────────────────────────────

/// Identifies a shuffle partition on disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShufflePath {
    /// Job identifier.
    pub job_id: String,
    /// Stage identifier.
    pub stage_id: String,
    /// Partition index within the stage.
    pub partition_id: u32,
}

impl ShufflePath {
    pub fn new(job_id: impl Into<String>, stage_id: impl Into<String>, partition_id: u32) -> Self {
        Self {
            job_id: job_id.into(),
            stage_id: stage_id.into(),
            partition_id,
        }
    }

    /// Returns the staging path: `{job_id}/{stage_id}/{partition_id}.tmp`
    pub fn staging_name(&self) -> String {
        format!(
            "{}/{}/{}.tmp",
            self.job_id, self.stage_id, self.partition_id
        )
    }

    /// Returns the final path: `{job_id}/{stage_id}/{partition_id}.ipc`
    pub fn final_name(&self) -> String {
        format!(
            "{}/{}/{}.ipc",
            self.job_id, self.stage_id, self.partition_id
        )
    }
}

// ── PartitionState ────────────────────────────────────────────────────────────

/// Lifecycle state of a single shuffle partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionState {
    /// Write has been started but not yet completed.
    Pending,
    /// Write completed and the partition is ready to be read.
    Available,
    /// Write failed; the error reason is captured.
    Failed {
        /// Human-readable failure reason.
        reason: String,
    },
}

// ── ShuffleMetadata ───────────────────────────────────────────────────────────

/// In-memory registry tracking the state of shuffle partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffleMetadata {
    states: HashMap<ShufflePath, PartitionState>,
    max_partitions: usize,
}

impl Default for ShuffleMetadata {
    fn default() -> Self {
        Self {
            states: HashMap::new(),
            max_partitions: 65_536,
        }
    }
}

impl ShuffleMetadata {
    /// Create an empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of tracked partitions (default 65536).
    #[must_use]
    pub fn with_max_partitions(mut self, n: usize) -> Self {
        self.max_partitions = n;
        self
    }

    /// Record that a partition write has been started.
    ///
    /// Returns `TooManyPartitions` when the cap is already reached.
    pub fn mark_pending(&mut self, path: &ShufflePath) -> ShuffleResult<()> {
        if self.states.len() >= self.max_partitions && !self.states.contains_key(path) {
            return Err(ShuffleError::TooManyPartitions {
                limit: self.max_partitions,
            });
        }
        self.states.insert(path.clone(), PartitionState::Pending);
        Ok(())
    }

    /// Record that a partition is fully written and available.
    pub fn mark_available(&mut self, path: &ShufflePath) {
        self.states.insert(path.clone(), PartitionState::Available);
    }

    /// Record that a partition write failed with the given reason.
    pub fn mark_failed(&mut self, path: &ShufflePath, reason: String) {
        self.states
            .insert(path.clone(), PartitionState::Failed { reason });
    }

    /// Return the current state for a partition, if known.
    pub fn state(&self, path: &ShufflePath) -> Option<&PartitionState> {
        self.states.get(path)
    }

    /// Return `true` only when every path in the slice is `Available`.
    pub fn all_available(&self, paths: &[ShufflePath]) -> bool {
        paths
            .iter()
            .all(|p| matches!(self.states.get(p), Some(PartitionState::Available)))
    }

    /// Number of partitions currently in the `Available` state.
    pub fn available_count(&self) -> usize {
        self.states
            .values()
            .filter(|s| **s == PartitionState::Available)
            .count()
    }

    /// Number of partitions currently tracked (any state).
    pub fn total_count(&self) -> usize {
        self.states.len()
    }
}

// ── ShuffleError ──────────────────────────────────────────────────────────────

/// Errors that can occur in shuffle operations.
#[derive(Debug)]
pub enum ShuffleError {
    /// I/O failure, wrapping the original error message.
    Io(String),
    /// The requested partition path does not exist on disk.
    PartitionNotFound {
        /// String representation of the path.
        path: String,
    },
    /// The partition exists in the metadata registry but is not yet available.
    PartitionNotAvailable {
        /// String representation of the path.
        path: String,
    },
    /// A stale lease token was used; the write was rejected.
    StaleLeaseToken {
        /// The expected (current) lease token.
        expected: u64,
        /// The token actually presented by the caller.
        actual: u64,
    },
    /// An object-store or generic path was not found.
    ///
    /// Used as the `StoreError::PartitionNotFound` alias when the partition key
    /// has already been formatted into a path string.
    NotFound {
        /// String representation of the missing path.
        path: String,
    },
    /// The shuffle partition cap was exceeded; no new partitions may be registered.
    TooManyPartitions {
        /// The configured partition limit.
        limit: usize,
    },
}

impl std::fmt::Display for ShuffleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "shuffle I/O error: {msg}"),
            Self::PartitionNotFound { path } => {
                write!(f, "shuffle partition not found: {path}")
            }
            Self::PartitionNotAvailable { path } => {
                write!(f, "shuffle partition not available: {path}")
            }
            Self::StaleLeaseToken { expected, actual } => write!(
                f,
                "stale shuffle lease token: expected {expected}, actual {actual}"
            ),
            Self::NotFound { path } => write!(f, "shuffle path not found: {path}"),
            Self::TooManyPartitions { limit } => {
                write!(
                    f,
                    "shuffle partition limit exceeded: max {limit} partitions"
                )
            }
        }
    }
}

impl std::error::Error for ShuffleError {}

impl From<std::io::Error> for ShuffleError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

/// Convenience alias for `Result<T, ShuffleError>`.
pub type ShuffleResult<T> = Result<T, ShuffleError>;

// ── ShuffleCompression / CompressionCodec ────────────────────────────────────

/// Compression algorithm for shuffle block data.
///
/// Used in [`ShuffleWriteConfig`] and [`ShuffleReadConfig`] to specify
/// how shuffle blocks are compressed on write and decompressed on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShuffleCompression {
    /// No compression (default).
    #[default]
    None,
    /// LZ4 frame compression via `lz4_flex`.
    Lz4,
    /// Zstandard compression via `zstd`.
    Zstd,
}

/// Compression codec — type alias for [`ShuffleCompression`].
pub type CompressionCodec = ShuffleCompression;

impl ShuffleCompression {
    /// Compress `data` using this codec. Returns the compressed bytes.
    pub fn compress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            ShuffleCompression::Zstd => {
                zstd::encode_all(data, 0).map_err(|e| ShuffleError::Io(e.to_string()))
            }
        }
    }

    /// Decompress `data` using this codec. Returns the original bytes.
    pub fn decompress(self, data: &[u8]) -> ShuffleResult<Vec<u8>> {
        match self {
            ShuffleCompression::None => Ok(data.to_vec()),
            ShuffleCompression::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| ShuffleError::Io(e.to_string())),
            ShuffleCompression::Zstd => {
                zstd::decode_all(data).map_err(|e| ShuffleError::Io(e.to_string()))
            }
        }
    }
}

// ── LocalShuffleStore ─────────────────────────────────────────────────────────

/// Local-disk shuffle store.
///
/// Writes each partition to a `.tmp` staging file and then atomically renames
/// it to the final `.ipc` path, matching the invariant from the shuffle
/// deployment model: a partition is either fully available or absent.
#[derive(Debug, Clone)]
pub struct LocalShuffleStore {
    base_dir: PathBuf,
    compression: CompressionCodec,
}

impl LocalShuffleStore {
    /// Create a new store rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            compression: CompressionCodec::None,
        }
    }

    /// Set the compression codec for this store.
    #[must_use]
    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    /// Return the compression codec in use.
    pub fn compression(&self) -> CompressionCodec {
        self.compression
    }

    /// Write `data` to disk for the given partition, applying the configured
    /// compression codec before writing.
    ///
    /// 1. Compresses `data` with the configured codec.
    /// 2. Creates `{base_dir}/{staging_name}` (including parent dirs).
    /// 3. Writes the compressed bytes.
    /// 4. Atomically renames staging path → final path.
    pub async fn write_partition(&self, path: &ShufflePath, data: &[u8]) -> ShuffleResult<()> {
        let compressed = self.compression.compress(data)?;
        let staging = self.base_dir.join(path.staging_name());
        let final_path = self.base_dir.join(path.final_name());

        // Create parent directories.
        if let Some(parent) = staging.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&staging, &compressed).await?;
        tokio::fs::rename(&staging, &final_path).await?;
        Ok(())
    }

    /// Read the bytes for a partition, decompressing with the configured codec.
    ///
    /// Returns `PartitionNotFound` if the final path does not exist.
    pub async fn read_partition(&self, path: &ShufflePath) -> ShuffleResult<Vec<u8>> {
        let final_path = self.base_dir.join(path.final_name());
        match tokio::fs::read(&final_path).await {
            Ok(bytes) => self.compression.decompress(&bytes),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ShuffleError::PartitionNotFound {
                    path: final_path.display().to_string(),
                })
            }
            Err(e) => Err(ShuffleError::Io(e.to_string())),
        }
    }

    /// Delete the entire directory for `job_id`.
    ///
    /// No-ops if the directory does not exist.
    pub async fn delete_job(&self, job_id: &str) -> ShuffleResult<()> {
        let dir = self.base_dir.join(job_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ShuffleError::Io(e.to_string())),
        }
    }
}

// ── Orphan detection ──────────────────────────────────────────────────────────

/// Scan `base_dir` for `.ipc` files whose job directory is not in `active_job_ids`.
///
/// Returns a list of orphan file paths (absolute paths under `base_dir`).
pub fn scan_orphans(
    base_dir: &std::path::Path,
    active_job_ids: &std::collections::HashSet<String>,
) -> ShuffleResult<Vec<std::path::PathBuf>> {
    if !base_dir.exists() {
        return Ok(Vec::new());
    }

    let mut orphans = Vec::new();

    for entry in std::fs::read_dir(base_dir)? {
        let entry = entry?;
        // P2.16: use DirEntry::file_type() to avoid an extra stat syscall per entry.
        if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let path = entry.path();
        let job_id = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };

        if !active_job_ids.contains(&job_id) {
            // Recursively collect all .ipc files in this job directory.
            collect_ipc_files(&path, &mut orphans)?;
        }
    }

    Ok(orphans)
}

/// Recursively collect all `.ipc` files under `dir`.
fn collect_ipc_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> ShuffleResult<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // P2.16: use DirEntry::file_type() to avoid an extra stat syscall per entry.
        let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
        let path = entry.path();
        if is_dir {
            collect_ipc_files(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("ipc") {
            out.push(path);
        }
    }
    Ok(())
}

/// Delete all orphan artifacts found by `scan_orphans`.
///
/// Returns the number of files deleted.
pub fn cleanup_orphans(
    base_dir: &std::path::Path,
    active_job_ids: &std::collections::HashSet<String>,
) -> ShuffleResult<usize> {
    let orphans = scan_orphans(base_dir, active_job_ids)?;
    let count = orphans.len();
    for path in &orphans {
        std::fs::remove_file(path)?;
    }
    Ok(count)
}

// ── HashPartitioner ───────────────────────────────────────────────────────────

/// Splits an Arrow `RecordBatch` into N buckets by hashing one key column.
///
/// Supported key column types: `Int32`, `Int64`, `Utf8`.
#[derive(Debug, Clone)]
pub struct HashPartitioner {
    key_column: String,
    buckets: u32,
}

impl HashPartitioner {
    /// Create a partitioner that splits on `key_column` into `buckets` buckets.
    pub fn new(key_column: impl Into<String>, buckets: u32) -> Self {
        Self {
            key_column: key_column.into(),
            buckets,
        }
    }

    /// Partition `batch` into `self.buckets` sub-batches.
    ///
    /// The returned `Vec` always has exactly `self.buckets` entries.  Empty
    /// buckets are represented as zero-row `RecordBatch` values with the same
    /// schema as the input.
    pub fn partition(&self, batch: &RecordBatch) -> ShuffleResult<Vec<RecordBatch>> {
        let schema = batch.schema();
        let col_idx = schema
            .index_of(&self.key_column)
            .map_err(|e| ShuffleError::Io(e.to_string()))?;
        let key_col = batch.column(col_idx);

        let n = self.buckets as usize;
        let num_rows = batch.num_rows();

        // Collect row indices per bucket.
        // P3.8: use a generic helper closure to avoid five near-identical loop bodies.
        let mut bucket_indices: Vec<Vec<u32>> = vec![Vec::new(); n];

        match key_col.data_type() {
            DataType::Int32 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("data type is Int32");
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    hash_i64(arr.value(row) as i64, self.buckets)
                });
            }
            DataType::Int64 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("data type is Int64");
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    hash_i64(arr.value(row), self.buckets)
                });
            }
            DataType::Utf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("data type is Utf8");
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    hash_str(arr.value(row), self.buckets)
                });
            }
            DataType::Utf8View => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("data type is Utf8View");
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    hash_str(arr.value(row), self.buckets)
                });
            }
            DataType::LargeUtf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("data type is LargeUtf8");
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    hash_str(arr.value(row), self.buckets)
                });
            }
            other => {
                return Err(ShuffleError::Io(format!("unsupported key type: {other}")));
            }
        }

        // Build one RecordBatch per bucket.
        let mut result = Vec::with_capacity(n);
        for indices in &bucket_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(schema.clone()));
            } else {
                let index_arr = UInt32Array::from_iter_values(indices.iter().copied());
                let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &index_arr, None)
                            .map_err(|e| ShuffleError::Io(e.to_string()))
                    })
                    .collect::<ShuffleResult<_>>()?;
                let partition_batch = RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|e| ShuffleError::Io(e.to_string()))?;
                result.push(partition_batch);
            }
        }

        Ok(result)
    }
}

// ── Hashing helpers ───────────────────────────────────────────────────────────

fn hash_i64(value: i64, buckets: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % buckets as u64) as u32
}

fn hash_str(value: &str, buckets: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    value.as_bytes().hash(&mut hasher);
    (hasher.finish() % buckets as u64) as u32
}

/// P3.8: Generic bucket-fill helper shared by all `HashPartitioner::partition` arms.
///
/// Iterates `num_rows` rows, calls `bucket_fn(row_index)` to determine the
/// target bucket, and appends the row index to the corresponding bucket vec.
/// Avoids code duplication across the five supported Arrow column types.
fn fill_buckets<F>(
    num_rows: usize,
    _num_partitions: u32,
    bucket_indices: &mut [Vec<u32>],
    bucket_fn: F,
) where
    F: Fn(usize) -> u32,
{
    for row in 0..num_rows {
        let bucket = bucket_fn(row) as usize;
        bucket_indices[bucket].push(row as u32);
    }
}

// ── ShuffleStore trait + implementations ──────────────────────────────────────

/// Identifies a shuffle partition uniquely within a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    pub job_id: String,
    pub stage_id: String,
    pub partition: u32,
}

/// A single shuffle partition: schema + ordered record batches.
#[derive(Debug, Clone)]
pub struct ShufflePartition {
    pub id: PartitionId,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Unified error type for [`ShuffleStore`] operations.
///
/// Previously a separate `StoreError` enum; now a type alias for [`ShuffleError`]
/// so callers only need to handle one error type across all shuffle APIs.
/// External code that imports `StoreError` continues to compile unchanged.
pub type StoreError = ShuffleError;

/// Convenience result alias for [`ShuffleStore`] operations.
///
/// Equivalent to [`ShuffleResult`]; kept for backward compatibility.
pub type StoreResult<T> = ShuffleResult<T>;

/// An async shuffle store that persists inter-stage partition data.
///
/// Implementations must be `Send + Sync` so they can be shared across async
/// task boundaries inside the executor runtime.
pub trait ShuffleStore: Send + Sync {
    /// Register the currently valid assignment lease token for a partition.
    ///
    /// Executors should call this when a task assignment is launched so a
    /// zombie attempt cannot win a race by writing before the replacement
    /// attempt commits data. Subsequent writes for the partition must present
    /// exactly this token until a newer assignment registers a replacement.
    fn register_partition_lease(
        &self,
        id: PartitionId,
        lease_token: u64,
    ) -> impl Future<Output = StoreResult<()>> + Send;

    /// Write a partition. `lease_token` must match the current assignment
    /// token for this partition; stale tokens are rejected.
    fn write_partition(
        &self,
        partition: ShufflePartition,
        lease_token: u64,
    ) -> impl Future<Output = StoreResult<()>> + Send;

    /// Read a partition. Returns `None` if not yet written.
    fn read_partition(
        &self,
        id: &PartitionId,
    ) -> impl Future<Output = StoreResult<Option<ShufflePartition>>> + Send;

    /// Delete all partitions for a job (called on job completion or cancellation).
    fn delete_job_partitions(&self, job_id: &str) -> impl Future<Output = StoreResult<()>> + Send;
}

// ── Shared type aliases ───────────────────────────────────────────────────────

/// Compound key used for both `InMemoryShuffleStore` and `LocalDiskShuffleStore`
/// lease maps: `(job_id, stage_id, partition_index)`.
type PartitionKey = (String, String, u32);

/// Shared lease-token map type used by both in-memory and disk-backed stores.
type LeaseMap = Arc<RwLock<BTreeMap<PartitionKey, u64>>>;

// ── InMemoryShuffleStore ──────────────────────────────────────────────────────

/// An in-memory shuffle store backed by a `BTreeMap` under an `RwLock`.
///
/// Used for testing and single-node deployments where shuffle data does
/// not need to survive process restarts.
#[derive(Default)]
pub struct InMemoryShuffleStore {
    // key: (job_id, stage_id, partition) → latest accepted partition
    partitions: Arc<RwLock<BTreeMap<PartitionKey, ShufflePartition>>>,
    // key: (job_id, stage_id, partition) → current assignment lease token
    lease_tokens: LeaseMap,
}

impl InMemoryShuffleStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ShuffleStore for InMemoryShuffleStore {
    async fn register_partition_lease(&self, id: PartitionId, lease_token: u64) -> StoreResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        let mut leases = self.lease_tokens.write().unwrap();
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
    ) -> StoreResult<()> {
        let key = (
            partition.id.job_id.clone(),
            partition.id.stage_id.clone(),
            partition.id.partition,
        );
        {
            let mut leases = self.lease_tokens.write().unwrap();
            if let Some(&expected) = leases.get(&key) {
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
                leases.insert(key.clone(), lease_token);
            }
        }
        self.partitions.write().unwrap().insert(key, partition);
        Ok(())
    }

    async fn read_partition(&self, id: &PartitionId) -> StoreResult<Option<ShufflePartition>> {
        let key = (id.job_id.clone(), id.stage_id.clone(), id.partition);
        let guard = self.partitions.read().unwrap();
        Ok(guard.get(&key).cloned())
    }

    async fn delete_job_partitions(&self, job_id: &str) -> StoreResult<()> {
        self.partitions
            .write()
            .unwrap()
            .retain(|(jid, _, _), _| jid != job_id);
        self.lease_tokens
            .write()
            .unwrap()
            .retain(|(jid, _, _), _| jid != job_id);
        Ok(())
    }
}

// ── LocalDiskShuffleStore ─────────────────────────────────────────────────────

/// A local-disk shuffle store that serialises partitions to Parquet files.
///
/// Each partition is written to `{base_dir}/{job_id}/{stage_id}/{partition}.parquet`.
/// Lease tokens are tracked in memory; they survive the process only as long as
/// the store object is alive.
pub struct LocalDiskShuffleStore {
    base_dir: PathBuf,
    lease_tokens: LeaseMap,
}

impl LocalDiskShuffleStore {
    /// Create a new store rooted at `base_dir`, creating the directory if needed.
    pub fn new(base_dir: impl AsRef<Path>) -> StoreResult<Self> {
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
        })
    }

    fn partition_path(&self, id: &PartitionId) -> PathBuf {
        self.base_dir
            .join(&id.job_id)
            .join(&id.stage_id)
            .join(format!("{}.parquet", id.partition))
    }
}

impl ShuffleStore for LocalDiskShuffleStore {
    async fn register_partition_lease(&self, id: PartitionId, lease_token: u64) -> StoreResult<()> {
        let key = (id.job_id, id.stage_id, id.partition);
        let mut leases = self.lease_tokens.write().unwrap();
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
    ) -> StoreResult<()> {
        let key = (
            partition.id.job_id.clone(),
            partition.id.stage_id.clone(),
            partition.id.partition,
        );

        // Validate/update the lease token.
        {
            let mut tokens = self.lease_tokens.write().unwrap();
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
            let mut writer = ArrowWriter::try_new(file, schema, None)
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

    async fn read_partition(&self, id: &PartitionId) -> StoreResult<Option<ShufflePartition>> {
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

    async fn delete_job_partitions(&self, job_id: &str) -> StoreResult<()> {
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
        let mut tokens = self.lease_tokens.write().unwrap();
        tokens.retain(|(jid, _, _), _| jid != &job_id_owned);
        Ok(())
    }
}

// ── ObjectStoreShuffleStore ───────────────────────────────────────────────────

/// An object-store backed shuffle store.
///
/// Partitions are stored as Arrow IPC stream files at paths:
///   `<prefix>/<job_id>/<stage_id>/<partition>.ipc`
///
/// This store has no lease mechanism — it is intended for batch jobs where
/// task retries use overwrite semantics on the same object key.
pub struct ObjectStoreShuffleStore {
    store: Arc<dyn object_store::ObjectStore>,
    prefix: object_store::path::Path,
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
        Self { store, prefix }
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
        _id: PartitionId,
        _lease_token: u64,
    ) -> StoreResult<()> {
        Ok(())
    }

    async fn write_partition(
        &self,
        partition: ShufflePartition,
        _lease_token: u64,
    ) -> StoreResult<()> {
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

    async fn read_partition(&self, id: &PartitionId) -> StoreResult<Option<ShufflePartition>> {
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

    async fn delete_job_partitions(&self, job_id: &str) -> StoreResult<()> {
        use futures::TryStreamExt;

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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        CompressionCodec, HashPartitioner, LocalShuffleStore, PartitionState,
        ShuffleError, ShuffleMetadata, ShufflePath, cleanup_orphans, scan_orphans,
    };

    // ── ShufflePath ───────────────────────────────────────────────────────

    #[test]
    fn shuffle_path_staging_name() {
        let path = ShufflePath {
            job_id: "job1".into(),
            stage_id: "s0".into(),
            partition_id: 3,
        };
        assert_eq!(path.staging_name(), "job1/s0/3.tmp");
    }

    #[test]
    fn shuffle_path_final_name() {
        let path = ShufflePath {
            job_id: "job1".into(),
            stage_id: "s0".into(),
            partition_id: 3,
        };
        assert_eq!(path.final_name(), "job1/s0/3.ipc");
    }

    // ── ShuffleMetadata ───────────────────────────────────────────────────

    fn make_path(partition_id: u32) -> ShufflePath {
        ShufflePath {
            job_id: "j".into(),
            stage_id: "s".into(),
            partition_id,
        }
    }

    #[test]
    fn metadata_pending_to_available() {
        let mut meta = ShuffleMetadata::new();
        let p = make_path(0);
        meta.mark_pending(&p).unwrap();
        assert_eq!(meta.state(&p), Some(&PartitionState::Pending));
        meta.mark_available(&p);
        assert_eq!(meta.state(&p), Some(&PartitionState::Available));
    }

    #[test]
    fn metadata_pending_to_failed() {
        let mut meta = ShuffleMetadata::new();
        let p = make_path(1);
        meta.mark_pending(&p).unwrap();
        meta.mark_failed(&p, "disk full".into());
        assert_eq!(
            meta.state(&p),
            Some(&PartitionState::Failed {
                reason: "disk full".into()
            })
        );
    }

    #[test]
    fn metadata_all_available_requires_every_path() {
        let mut meta = ShuffleMetadata::new();
        let p0 = make_path(0);
        let p1 = make_path(1);
        meta.mark_available(&p0);
        meta.mark_pending(&p1).unwrap();

        assert!(!meta.all_available(&[p0.clone(), p1.clone()]));

        meta.mark_available(&p1);
        assert!(meta.all_available(&[p0, p1]));
    }

    #[test]
    fn metadata_all_available_empty_slice() {
        let meta = ShuffleMetadata::new();
        assert!(meta.all_available(&[]));
    }

    #[test]
    fn metadata_partition_cap_enforced() {
        let mut meta = ShuffleMetadata::new().with_max_partitions(2);
        meta.mark_pending(&make_path(0)).unwrap();
        meta.mark_pending(&make_path(1)).unwrap();
        let err = meta.mark_pending(&make_path(2)).unwrap_err();
        assert!(
            matches!(err, ShuffleError::TooManyPartitions { limit: 2 }),
            "expected TooManyPartitions(2), got: {err}"
        );
    }

    #[test]
    fn metadata_cap_allows_update_of_existing_partition() {
        let mut meta = ShuffleMetadata::new().with_max_partitions(1);
        let p = make_path(0);
        meta.mark_pending(&p).unwrap();
        // Re-marking an existing key must succeed even at cap.
        meta.mark_pending(&p).unwrap();
    }

    // ── LocalShuffleStore ─────────────────────────────────────────────────

    #[tokio::test]
    async fn local_store_write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        let path = ShufflePath {
            job_id: "job-rw".into(),
            stage_id: "s1".into(),
            partition_id: 0,
        };
        let data = b"hello shuffle".as_slice();
        store.write_partition(&path, data).await.unwrap();
        let read = store.read_partition(&path).await.unwrap();
        assert_eq!(read, data);
    }

    #[tokio::test]
    async fn local_store_read_missing_returns_partition_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        let path = ShufflePath {
            job_id: "ghost".into(),
            stage_id: "s0".into(),
            partition_id: 0,
        };
        let err = store.read_partition(&path).await.unwrap_err();
        assert!(
            matches!(err, ShuffleError::PartitionNotFound { .. }),
            "expected PartitionNotFound, got {err}"
        );
    }

    #[tokio::test]
    async fn local_store_delete_job_removes_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        let path = ShufflePath {
            job_id: "deljob".into(),
            stage_id: "s0".into(),
            partition_id: 0,
        };
        store.write_partition(&path, b"data").await.unwrap();
        let job_dir = dir.path().join("deljob");
        assert!(job_dir.exists());

        store.delete_job("deljob").await.unwrap();
        assert!(!job_dir.exists());
    }

    #[tokio::test]
    async fn local_store_delete_job_noop_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        // Should not return an error.
        store.delete_job("nonexistent-job").await.unwrap();
    }

    // ── CompressionCodec ──────────────────────────────────────────────────

    #[test]
    fn compression_codec_default_is_none() {
        assert_eq!(CompressionCodec::default(), CompressionCodec::None);
    }

    #[test]
    fn local_store_default_compression_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path());
        assert_eq!(store.compression(), CompressionCodec::None);
    }

    #[test]
    fn local_store_with_compression_lz4() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
        assert_eq!(store.compression(), CompressionCodec::Lz4);
    }

    // ── Compression round-trip tests ──────────────────────────────────────

    #[test]
    fn compression_codec_none_round_trip() {
        let data = b"hello shuffle world";
        let compressed = CompressionCodec::None.compress(data).unwrap();
        let decompressed = CompressionCodec::None.decompress(&compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn compression_codec_lz4_round_trip() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let compressed = CompressionCodec::Lz4.compress(&data).unwrap();
        let decompressed = CompressionCodec::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data, "LZ4 round-trip must be byte-exact");
    }

    #[test]
    fn compression_codec_zstd_round_trip() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        let compressed = CompressionCodec::Zstd.compress(&data).unwrap();
        let decompressed = CompressionCodec::Zstd.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data, "Zstd round-trip must be byte-exact");
    }

    #[tokio::test]
    async fn local_store_lz4_write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Lz4);
        let path = ShufflePath::new("job-1", "stage-1", 0);
        let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        store.write_partition(&path, &data).await.unwrap();
        let read_back = store.read_partition(&path).await.unwrap();
        assert_eq!(read_back, data, "LZ4 write/read round-trip must be byte-exact");
    }

    #[tokio::test]
    async fn local_store_zstd_write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalShuffleStore::new(dir.path()).with_compression(CompressionCodec::Zstd);
        let path = ShufflePath::new("job-1", "stage-1", 0);
        let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        store.write_partition(&path, &data).await.unwrap();
        let read_back = store.read_partition(&path).await.unwrap();
        assert_eq!(read_back, data, "Zstd write/read round-trip must be byte-exact");
    }

    // ── Orphan detection ──────────────────────────────────────────────────

    fn write_ipc_file(base: &std::path::Path, job_id: &str, stage_id: &str, partition_id: u32) {
        let dir = base.join(job_id).join(stage_id);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join(format!("{partition_id}.ipc"));
        std::fs::write(file, b"dummy").unwrap();
    }

    #[test]
    fn scan_orphans_empty_base_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let active: HashSet<String> = HashSet::new();
        let result = scan_orphans(dir.path(), &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_nonexistent_base_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does_not_exist");
        let active: HashSet<String> = HashSet::new();
        let result = scan_orphans(&missing, &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_all_active_no_orphans() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "job1", "s0", 0);
        write_ipc_file(dir.path(), "job1", "s0", 1);

        let mut active: HashSet<String> = HashSet::new();
        active.insert("job1".into());

        let result = scan_orphans(dir.path(), &active).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn scan_orphans_inactive_job_returns_ipc_files() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 1);

        let active: HashSet<String> = HashSet::new();
        let mut result = scan_orphans(dir.path(), &active).unwrap();
        result.sort();

        assert_eq!(result.len(), 2);
        for path in &result {
            assert!(
                path.extension().and_then(|e| e.to_str()) == Some("ipc"),
                "expected .ipc extension"
            );
        }
    }

    #[test]
    fn scan_orphans_mixed_active_and_inactive() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "active_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s1", 0);

        let mut active: HashSet<String> = HashSet::new();
        active.insert("active_job".into());

        let result = scan_orphans(dir.path(), &active).unwrap();
        assert_eq!(result.len(), 2);
        // None of the orphans should be under active_job.
        for path in &result {
            assert!(
                !path.to_string_lossy().contains("active_job"),
                "active job files should not be orphans"
            );
        }
    }

    #[test]
    fn cleanup_orphans_deletes_files_and_returns_count() {
        let dir = tempfile::tempdir().unwrap();
        write_ipc_file(dir.path(), "dead_job", "s0", 0);
        write_ipc_file(dir.path(), "dead_job", "s0", 1);

        let active: HashSet<String> = HashSet::new();
        let count = cleanup_orphans(dir.path(), &active).unwrap();
        assert_eq!(count, 2);

        // Files should be gone.
        let remaining = scan_orphans(dir.path(), &active).unwrap();
        assert!(remaining.is_empty());
    }

    // ── HashPartitioner ───────────────────────────────────────────────────

    fn make_int32_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn make_utf8_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let arr = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn partitioner_int32_preserves_total_rows() {
        let batch = make_int32_batch(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let partitioner = HashPartitioner::new("key", 4);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 4);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn partitioner_int32_each_row_in_correct_bucket() {
        let values = vec![10i32, 20, 30, 40, 50];
        let batch = make_int32_batch(values.clone());
        let buckets = 3u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        // Verify each row ends up in the expected bucket.
        for &v in &values {
            let mut hasher = DefaultHasher::new();
            (v as i64).hash(&mut hasher);
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_utf8_preserves_total_rows() {
        let batch = make_utf8_batch(vec!["alpha", "beta", "gamma", "delta"]);
        let partitioner = HashPartitioner::new("key", 2);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 2);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn partitioner_utf8_each_row_in_correct_bucket() {
        let values = vec!["hello", "world", "foo", "bar"];
        let batch = make_utf8_batch(values.clone());
        let buckets = 3u32;
        let partitioner = HashPartitioner::new("key", buckets);
        let partitions = partitioner.partition(&batch).unwrap();

        for &v in &values {
            let mut hasher = DefaultHasher::new();
            v.as_bytes().hash(&mut hasher);
            let expected_bucket = (hasher.finish() % buckets as u64) as usize;
            let arr = partitions[expected_bucket]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let found = (0..arr.len()).any(|i| arr.value(i) == v);
            assert!(
                found,
                "value {v} not found in expected bucket {expected_bucket}"
            );
        }
    }

    #[test]
    fn partitioner_unsupported_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Float64,
            false,
        )]));
        let arr = Arc::new(arrow::array::Float64Array::from(vec![1.0f64]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let partitioner = HashPartitioner::new("key", 4);
        let err = partitioner.partition(&batch).unwrap_err();
        assert!(
            matches!(err, ShuffleError::Io(_)),
            "expected Io error for unsupported type"
        );
    }

    #[test]
    fn partitioner_empty_batch_produces_empty_buckets() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let partitioner = HashPartitioner::new("key", 3);
        let partitions = partitioner.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 3);
        assert!(partitions.iter().all(|p| p.num_rows() == 0));
    }

    // ── ShuffleStore tests ────────────────────────────────────────────────

    use super::{
        InMemoryShuffleStore, LocalDiskShuffleStore, PartitionId, ShufflePartition, ShuffleStore,
        StoreError,
    };

    fn make_store_partition(job_id: &str, stage_id: &str, partition: u32) -> ShufflePartition {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ShufflePartition {
            id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id: stage_id.to_owned(),
                partition,
            },
            schema,
            batches: vec![batch],
        }
    }

    #[tokio::test]
    async fn in_memory_shuffle_write_and_read_roundtrip() {
        let store = InMemoryShuffleStore::new();
        let partition = make_store_partition("job-1", "stage-1", 0);
        let id = partition.id.clone();
        store.write_partition(partition, 1).await.unwrap();
        let read_back = store.read_partition(&id).await.unwrap();
        assert!(read_back.is_some());
        let read_back = read_back.unwrap();
        assert_eq!(read_back.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn in_memory_shuffle_read_missing_returns_none() {
        let store = InMemoryShuffleStore::new();
        let id = PartitionId {
            job_id: "ghost-job".to_owned(),
            stage_id: "s0".to_owned(),
            partition: 0,
        };
        let result = store.read_partition(&id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn in_memory_shuffle_delete_job_partitions() {
        let store = InMemoryShuffleStore::new();
        let p0 = make_store_partition("job-del", "s0", 0);
        let p1 = make_store_partition("job-del", "s0", 1);
        let id0 = p0.id.clone();
        let id1 = p1.id.clone();
        store.write_partition(p0, 1).await.unwrap();
        store.write_partition(p1, 1).await.unwrap();

        store.delete_job_partitions("job-del").await.unwrap();

        assert!(store.read_partition(&id0).await.unwrap().is_none());
        assert!(store.read_partition(&id1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_memory_shuffle_stale_lease_token_rejected() {
        let store = InMemoryShuffleStore::new();
        let partition = make_store_partition("job-stale", "s0", 0);
        // Write with token=5.
        store.write_partition(partition.clone(), 5).await.unwrap();
        // Try to overwrite with a lower token — should be rejected.
        let err = store.write_partition(partition, 3).await.unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 5,
                    actual: 3
                }
            ),
            "expected StaleLeaseToken(expected=5, actual=3), got {err}"
        );
    }

    #[tokio::test]
    async fn in_memory_shuffle_equal_lease_token_overwrites() {
        let store = InMemoryShuffleStore::new();
        let partition = make_store_partition("job-eq", "s0", 0);
        let id = partition.id.clone();
        store.write_partition(partition.clone(), 2).await.unwrap();
        // Same token is allowed — overwrites with the new data.
        store.write_partition(partition, 2).await.unwrap();
        assert!(store.read_partition(&id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn in_memory_registered_fresh_lease_rejects_stale_registration() {
        let store = InMemoryShuffleStore::new();
        let id = make_store_partition("job-zombie-register", "s0", 0).id;

        store.register_partition_lease(id.clone(), 8).await.unwrap();
        let err = store.register_partition_lease(id, 7).await.unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 8,
                    actual: 7
                }
            ),
            "expected StaleLeaseToken(expected=8, actual=7), got {err}"
        );
    }

    #[tokio::test]
    async fn in_memory_registered_fresh_lease_rejects_stale_write_before_commit() {
        let store = InMemoryShuffleStore::new();
        let partition = make_store_partition("job-zombie", "s0", 0);
        let id = partition.id.clone();

        store.register_partition_lease(id.clone(), 8).await.unwrap();

        let err = store
            .write_partition(partition.clone(), 7)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 8,
                    actual: 7
                }
            ),
            "expected StaleLeaseToken(expected=8, actual=7), got {err}"
        );
        assert!(store.read_partition(&id).await.unwrap().is_none());

        store.write_partition(partition, 8).await.unwrap();
        assert!(store.read_partition(&id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn local_disk_shuffle_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        let partition = make_store_partition("job-disk-1", "stage-1", 0);
        let id = partition.id.clone();
        store.write_partition(partition, 1).await.unwrap();
        let read_back = store.read_partition(&id).await.unwrap();
        assert!(read_back.is_some());
        let read_back = read_back.unwrap();
        assert_eq!(read_back.batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn local_disk_shuffle_delete_job_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        let p0 = make_store_partition("job-disk-del", "s0", 0);
        let id0 = p0.id.clone();
        store.write_partition(p0, 1).await.unwrap();

        store.delete_job_partitions("job-disk-del").await.unwrap();

        // The file should be gone so read returns None.
        assert!(store.read_partition(&id0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_disk_shuffle_stale_token_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        let partition = make_store_partition("job-disk-stale", "s0", 0);
        store.write_partition(partition.clone(), 10).await.unwrap();
        let err = store.write_partition(partition, 7).await.unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 10,
                    actual: 7
                }
            ),
            "expected StaleLeaseToken(expected=10, actual=7), got {err}"
        );
    }

    #[tokio::test]
    async fn local_disk_registered_fresh_lease_rejects_stale_registration() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        let id = make_store_partition("job-disk-zombie-register", "s0", 0).id;

        store
            .register_partition_lease(id.clone(), 11)
            .await
            .unwrap();
        let err = store.register_partition_lease(id, 10).await.unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 11,
                    actual: 10
                }
            ),
            "expected StaleLeaseToken(expected=11, actual=10), got {err}"
        );
    }

    #[tokio::test]
    async fn local_disk_registered_fresh_lease_rejects_stale_write_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalDiskShuffleStore::new(dir.path()).unwrap();
        let partition = make_store_partition("job-disk-zombie", "s0", 0);
        let id = partition.id.clone();

        store
            .register_partition_lease(id.clone(), 11)
            .await
            .unwrap();

        let err = store
            .write_partition(partition.clone(), 10)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::StaleLeaseToken {
                    expected: 11,
                    actual: 10
                }
            ),
            "expected StaleLeaseToken(expected=11, actual=10), got {err}"
        );
        assert!(store.read_partition(&id).await.unwrap().is_none());

        store.write_partition(partition, 11).await.unwrap();
        assert!(store.read_partition(&id).await.unwrap().is_some());
    }

    // ── ObjectStoreShuffleStore ───────────────────────────────────────────

    use crate::ObjectStoreShuffleStore;
    use object_store::memory::InMemory;

    fn make_object_store_partition(
        job_id: &str,
        stage_id: &str,
        partition: u32,
    ) -> ShufflePartition {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![partition as i32]))],
        )
        .unwrap();
        ShufflePartition {
            id: PartitionId {
                job_id: job_id.to_owned(),
                stage_id: stage_id.to_owned(),
                partition,
            },
            schema,
            batches: vec![batch],
        }
    }

    #[tokio::test]
    async fn object_store_shuffle_write_and_read_round_trip() {
        let inner = Arc::new(InMemory::new());
        let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");

        let partition = make_object_store_partition("job-os-1", "s0", 0);
        let id = partition.id.clone();
        store.write_partition(partition, 0).await.unwrap();

        let read = store.read_partition(&id).await.unwrap().unwrap();
        assert_eq!(read.batches.len(), 1);
        assert_eq!(read.batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn object_store_shuffle_read_missing_returns_none() {
        let inner = Arc::new(InMemory::new());
        let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");
        let id = PartitionId {
            job_id: "missing".into(),
            stage_id: "s0".into(),
            partition: 0,
        };
        let result = store.read_partition(&id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn object_store_shuffle_delete_job_removes_all_partitions() {
        let inner = Arc::new(InMemory::new());
        let store = ObjectStoreShuffleStore::new(inner, "shuffle-test");

        store
            .write_partition(make_object_store_partition("job-del-os", "s0", 0), 0)
            .await
            .unwrap();
        store
            .write_partition(make_object_store_partition("job-del-os", "s0", 1), 0)
            .await
            .unwrap();

        store.delete_job_partitions("job-del-os").await.unwrap();

        let id0 = PartitionId {
            job_id: "job-del-os".into(),
            stage_id: "s0".into(),
            partition: 0,
        };
        let id1 = PartitionId {
            job_id: "job-del-os".into(),
            stage_id: "s0".into(),
            partition: 1,
        };
        assert!(store.read_partition(&id0).await.unwrap().is_none());
        assert!(store.read_partition(&id1).await.unwrap().is_none());
    }
}

// ── Shuffle partition transport (Arrow IPC over TCP) ─────────────────────────
//
// Krishiv uses Arrow IPC framing over a simple TCP connection for shuffle reads.
// The protocol is intentionally minimal:
//   client → server: "<job_id>/<stage_id>/<partition_id>\n" (UTF-8 ticket)
//   server → client: 4-byte big-endian u32 payload length, then raw Arrow IPC bytes
//                    A length of 0 means "partition not found".
//
// This achieves the same data transport as Arrow Flight DoGet without requiring
// the full gRPC/Flight service trait implementation. The `arrow-flight` crate is
// retained as a dependency for future upgrade to the full Flight protocol.

pub mod flight {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::ipc::reader::StreamReader;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use crate::{LocalDiskShuffleStore, PartitionId, ShuffleStore};

    fn parse_ticket(ticket: &str) -> Option<(String, String, u32)> {
        let parts: Vec<&str> = ticket.trim().splitn(3, '/').collect();
        if parts.len() != 3 {
            return None;
        }
        let partition_id = parts[2].parse::<u32>().ok()?;
        Some((parts[0].to_owned(), parts[1].to_owned(), partition_id))
    }

    /// Serialize `batches` to Arrow IPC stream format. Returns `None` on any
    /// serialization error so callers can fall back to sending an empty response
    /// rather than partial / corrupted bytes.
    fn serialize_ipc_partition(
        schema: &arrow::datatypes::Schema,
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Option<Vec<u8>> {
        use arrow::ipc::writer::StreamWriter;
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, schema).ok()?;
        for batch in batches {
            writer.write(batch).ok()?;
        }
        writer.finish().ok()?;
        Some(buf)
    }

    async fn handle_connection(mut stream: TcpStream, store: Arc<LocalDiskShuffleStore>) {
        // Read ticket line terminated by '\n'.
        let mut buf = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            match stream.read_exact(&mut byte).await {
                Ok(_) => {
                    if byte[0] == b'\n' {
                        break;
                    }
                    buf.push(byte[0]);
                }
                Err(_) => return,
            }
        }

        let ticket = match std::str::from_utf8(&buf) {
            Ok(s) => s.to_owned(),
            Err(_) => {
                let _ = stream.write_all(&0u32.to_be_bytes()).await;
                return;
            }
        };

        let Some((job_id, stage_id, partition_id)) = parse_ticket(&ticket) else {
            let _ = stream.write_all(&0u32.to_be_bytes()).await;
            return;
        };

        let id = PartitionId {
            job_id,
            stage_id,
            partition: partition_id,
        };
        let result = store.read_partition(&id).await;

        match result {
            Ok(Some(partition)) => {
                // Serialize to a local buffer first; send len=0 if serialization
                // fails so the client gets a clean "not found" rather than
                // partial / corrupted Arrow IPC bytes.
                let buf = serialize_ipc_partition(&partition.schema, &partition.batches)
                    .unwrap_or_default();
                let len = buf.len() as u32;
                let _ = stream.write_all(&len.to_be_bytes()).await;
                let _ = stream.write_all(&buf).await;
            }
            _ => {
                let _ = stream.write_all(&0u32.to_be_bytes()).await;
            }
        }
    }

    /// Start the shuffle IPC server on `addr` backed by `store`.
    ///
    /// Returns the local address and a join handle. Call `abort()` on the handle
    /// to shut down the server.
    pub async fn serve(
        addr: SocketAddr,
        store: Arc<LocalDiskShuffleStore>,
    ) -> io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let store = Arc::clone(&store);
                tokio::spawn(handle_connection(stream, store));
            }
        });
        Ok((local_addr, handle))
    }

    /// Fetch all [`RecordBatch`]es for one shuffle partition from a remote server.
    ///
    /// `endpoint` format: `<host>:<port>` (e.g. `"10.0.0.5:50051"`)
    pub struct FlightShuffleClient;

    impl FlightShuffleClient {
        pub async fn fetch(
            endpoint: impl Into<String>,
            job_id: &str,
            stage_id: &str,
            partition_id: u32,
        ) -> io::Result<Vec<RecordBatch>> {
            let endpoint = endpoint.into();
            let mut stream = TcpStream::connect(&endpoint).await?;

            let ticket = format!("{job_id}/{stage_id}/{partition_id}\n");
            stream.write_all(ticket.as_bytes()).await?;

            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            if len == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("partition {job_id}/{stage_id}/{partition_id} not found"),
                ));
            }

            // Guard against a server sending a maliciously large length that
            // would cause an OOM allocation on the client side.
            const MAX_PARTITION_BYTES: usize = 256 * 1024 * 1024; // 256 MiB
            if len > MAX_PARTITION_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("partition length {len} exceeds maximum {MAX_PARTITION_BYTES} bytes"),
                ));
            }

            let mut data = vec![0u8; len];
            stream.read_exact(&mut data).await?;

            let cursor = std::io::Cursor::new(data);
            let reader = StreamReader::try_new(cursor, None)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut batches = Vec::new();
            for batch in reader {
                let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                batches.push(batch);
            }
            Ok(batches)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use super::*;
        use crate::{LocalDiskShuffleStore, PartitionId, ShufflePartition};

        fn make_test_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap()
        }

        #[tokio::test]
        async fn flight_server_serves_partition_and_client_reads_it() {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

            let batch = make_test_batch();

            let id = PartitionId {
                job_id: "job-flight-1".to_owned(),
                stage_id: "s0".to_owned(),
                partition: 0,
            };
            let partition = ShufflePartition {
                id: id.clone(),
                schema: batch.schema(),
                batches: vec![batch.clone()],
            };
            store.register_partition_lease(id.clone(), 1).await.unwrap();
            store.write_partition(partition, 1).await.unwrap();

            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();

            let endpoint = local_addr.to_string();
            let result = FlightShuffleClient::fetch(&endpoint, "job-flight-1", "s0", 0)
                .await
                .unwrap();

            server_handle.abort();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 3);
            assert_eq!(result[0].num_columns(), 2);
        }

        #[tokio::test]
        async fn flight_client_returns_error_for_missing_partition() {
            let dir = tempfile::tempdir().unwrap();
            let store = Arc::new(LocalDiskShuffleStore::new(dir.path()).unwrap());

            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (local_addr, server_handle) = serve(addr, Arc::clone(&store)).await.unwrap();
            let endpoint = local_addr.to_string();

            let result = FlightShuffleClient::fetch(&endpoint, "missing", "s0", 0).await;
            server_handle.abort();

            assert!(
                matches!(result, Err(ref e) if e.kind() == std::io::ErrorKind::NotFound),
                "expected NotFound, got: {result:?}"
            );
        }
    }
}
