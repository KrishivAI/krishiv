#![forbid(unsafe_code)]

//! Shuffle store and hash partitioner for Krishiv R4.
//!
//! Provides local-disk shuffle write/read paths and an Arrow-based hash
//! partitioner that splits a `RecordBatch` by a key column into N buckets.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

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
    /// Returns the staging path: `{job_id}/{stage_id}/{partition_id}.tmp`
    pub fn staging_name(&self) -> String {
        format!("{}/{}/{}.tmp", self.job_id, self.stage_id, self.partition_id)
    }

    /// Returns the final path: `{job_id}/{stage_id}/{partition_id}.ipc`
    pub fn final_name(&self) -> String {
        format!("{}/{}/{}.ipc", self.job_id, self.stage_id, self.partition_id)
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
#[derive(Debug, Default)]
pub struct ShuffleMetadata {
    states: HashMap<ShufflePath, PartitionState>,
}

impl ShuffleMetadata {
    /// Create an empty metadata store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a partition write has been started.
    pub fn mark_pending(&mut self, path: &ShufflePath) {
        self.states.insert(path.clone(), PartitionState::Pending);
    }

    /// Record that a partition is fully written and available.
    pub fn mark_available(&mut self, path: &ShufflePath) {
        self.states
            .insert(path.clone(), PartitionState::Available);
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
        paths.iter().all(|p| {
            matches!(self.states.get(p), Some(PartitionState::Available))
        })
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

// ── LocalShuffleStore ─────────────────────────────────────────────────────────

/// Local-disk shuffle store.
///
/// Writes each partition to a `.tmp` staging file and then atomically renames
/// it to the final `.ipc` path, matching the invariant from the shuffle
/// deployment model: a partition is either fully available or absent.
#[derive(Debug, Clone)]
pub struct LocalShuffleStore {
    base_dir: PathBuf,
}

impl LocalShuffleStore {
    /// Create a new store rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Write `data` to disk for the given partition.
    ///
    /// 1. Creates `{base_dir}/{staging_name}` (including parent dirs).
    /// 2. Writes `data`.
    /// 3. Atomically renames staging path → final path.
    pub async fn write_partition(
        &self,
        path: &ShufflePath,
        data: &[u8],
    ) -> ShuffleResult<()> {
        let staging = self.base_dir.join(path.staging_name());
        let final_path = self.base_dir.join(path.final_name());

        // Create parent directories.
        if let Some(parent) = staging.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&staging, data).await?;
        tokio::fs::rename(&staging, &final_path).await?;
        Ok(())
    }

    /// Read the bytes for a partition.
    ///
    /// Returns `PartitionNotFound` if the final path does not exist.
    pub async fn read_partition(&self, path: &ShufflePath) -> ShuffleResult<Vec<u8>> {
        let final_path = self.base_dir.join(path.final_name());
        match tokio::fs::read(&final_path).await {
            Ok(bytes) => Ok(bytes),
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
        let mut bucket_indices: Vec<Vec<u32>> = vec![Vec::new(); n];

        match key_col.data_type() {
            DataType::Int32 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("data type is Int32");
                for row in 0..num_rows {
                    let bucket = hash_i64(arr.value(row) as i64, self.buckets);
                    bucket_indices[bucket as usize].push(row as u32);
                }
            }
            DataType::Int64 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("data type is Int64");
                for row in 0..num_rows {
                    let bucket = hash_i64(arr.value(row), self.buckets);
                    bucket_indices[bucket as usize].push(row as u32);
                }
            }
            DataType::Utf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("data type is Utf8");
                for row in 0..num_rows {
                    let bucket = hash_str(arr.value(row), self.buckets);
                    bucket_indices[bucket as usize].push(row as u32);
                }
            }
            other => {
                return Err(ShuffleError::Io(format!(
                    "unsupported key type: {other}"
                )));
            }
        }

        // Build one RecordBatch per bucket.
        let mut result = Vec::with_capacity(n);
        for indices in &bucket_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(schema.clone()));
            } else {
                let index_arr = UInt32Array::from(indices.clone());
                let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &index_arr, None)
                            .map_err(|e| ShuffleError::Io(e.to_string()))
                    })
                    .collect::<ShuffleResult<_>>()?;
                let partition_batch =
                    RecordBatch::try_new(schema.clone(), columns)
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        HashPartitioner, LocalShuffleStore, PartitionState, ShuffleError, ShufflePath,
        ShuffleMetadata,
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
        meta.mark_pending(&p);
        assert_eq!(meta.state(&p), Some(&PartitionState::Pending));
        meta.mark_available(&p);
        assert_eq!(meta.state(&p), Some(&PartitionState::Available));
    }

    #[test]
    fn metadata_pending_to_failed() {
        let mut meta = ShuffleMetadata::new();
        let p = make_path(1);
        meta.mark_pending(&p);
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
        meta.mark_pending(&p1);

        assert!(!meta.all_available(&[p0.clone(), p1.clone()]));

        meta.mark_available(&p1);
        assert!(meta.all_available(&[p0, p1]));
    }

    #[test]
    fn metadata_all_available_empty_slice() {
        let meta = ShuffleMetadata::new();
        assert!(meta.all_available(&[]));
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

    // ── HashPartitioner ───────────────────────────────────────────────────

    fn make_int32_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let arr = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    fn make_utf8_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Utf8,
            false,
        )]));
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
            assert!(found, "value {v} not found in expected bucket {expected_bucket}");
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
            assert!(found, "value {v} not found in expected bucket {expected_bucket}");
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
}
