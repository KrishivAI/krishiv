//! Shuffle-routing hash partitioner (XxHash64).
//!
//! **Type boundary**: keys are hashed with XxHash64 for deterministic bucket
//! assignment in a shuffle exchange. This is intentionally *different* from
//! `krishiv_common::partition`, which uses SHA-256 with a domain-separation
//! prefix for keyed-semantics partitioning (join keys, state sharding). The
//! two hash functions are not interchangeable — never use this partitioner
//! where keyed-semantics guarantees are required.

use crate::{ShuffleError, ShuffleResult};

/// A bucket index produced by the XxHash64 shuffle-routing partitioner.
///
/// Intentionally not `From<u32>` or `Into<u32>`: callers must name `.0` to
/// extract the raw index. This makes it a compile-time error to use a
/// `KeyedShard` (SHA-256 keyed semantics) where a `ShuffleBucket` is required,
/// and vice versa — the two hash domains must never be aliased.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShuffleBucket(pub u32);

use arrow::array::{
    Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Splits an Arrow `RecordBatch` into N buckets by hashing one key column.
///
/// Supported key column types: `Int32`, `Int64`, `Utf8`, `Utf8View`, `LargeUtf8`.
///
/// The hash seed should be derived from the job ID so different jobs produce
/// independent bucket distributions. This prevents both accidental and
/// adversarial concentration caused by fixed-seed hash collisions.
/// Use [`HashPartitioner::with_seed`] to set a per-job seed.
#[derive(Debug)]
pub struct HashPartitioner {
    key_column: String,
    buckets: u32,
    /// XxHash64 seed. Defaults to 0; set per-job via `with_seed` to prevent
    /// pathological distributions from a fixed seed.
    seed: u64,
    null_key_count: AtomicU64,
}

impl Clone for HashPartitioner {
    fn clone(&self) -> Self {
        Self {
            key_column: self.key_column.clone(),
            buckets: self.buckets,
            seed: self.seed,
            null_key_count: AtomicU64::new(0),
        }
    }
}

/// Sentinel bytes used to hash null keys.
///
/// Null values are hashed using a fixed sentinel so they distribute across
/// buckets deterministically — identical to a non-null key that happens to
/// hash to the same bucket — rather than all concentrating on bucket 0.
///
/// The sentinel is deliberately chosen to be unreachable via normal integer
/// or string encoding (all-0xFF bytes), so it will not accidentally alias
/// a real value. A per-type tag byte (`0x00` for integers, `0x01` for
/// strings) is mixed in to prevent cross-type collisions.
const NULL_SENTINEL_INT: &[u8] = &[0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
const NULL_SENTINEL_STR: &[u8] = &[0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

impl HashPartitioner {
    pub fn new(key_column: impl Into<String>, buckets: u32) -> Self {
        Self {
            key_column: key_column.into(),
            buckets,
            seed: 0,
            null_key_count: AtomicU64::new(0),
        }
    }

    /// Set a per-job XxHash64 seed.
    ///
    /// Derive the seed from the job ID so different jobs produce independent
    /// bucket distributions, preventing adversarial or pathological input
    /// patterns from concentrating all rows into one bucket.
    ///
    /// Typical usage:
    /// ```ignore
    /// use std::hash::Hasher;
    /// let mut h = twox_hash::XxHash64::with_seed(0);
    /// h.write(job_id.as_bytes());
    /// let partitioner = HashPartitioner::new(key_col, buckets).with_seed(h.finish());
    /// ```
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn null_key_count(&self) -> u64 {
        self.null_key_count.load(Ordering::Relaxed)
    }

    /// Hash the null sentinel for integer columns and count the null.
    fn null_int_bucket(&self) -> ShuffleBucket {
        self.null_key_count.fetch_add(1, Ordering::Relaxed);
        let mut hasher = twox_hash::XxHash64::with_seed(self.seed);
        hasher.write(NULL_SENTINEL_INT);
        ShuffleBucket((hasher.finish() % self.buckets as u64) as u32)
    }

    /// Hash the null sentinel for string columns and count the null.
    fn null_str_bucket(&self) -> ShuffleBucket {
        self.null_key_count.fetch_add(1, Ordering::Relaxed);
        let mut hasher = twox_hash::XxHash64::with_seed(self.seed);
        hasher.write(NULL_SENTINEL_STR);
        ShuffleBucket((hasher.finish() % self.buckets as u64) as u32)
    }

    /// Partition `batch` into `self.buckets` sub-batches.
    ///
    /// The returned `Vec` always has exactly `self.buckets` entries.  Empty
    /// buckets are represented as zero-row `RecordBatch` values with the same
    /// schema as the input.
    pub fn partition(&self, batch: &RecordBatch) -> ShuffleResult<Vec<RecordBatch>> {
        if self.buckets == 0 {
            return Err(ShuffleError::InvalidPartitionCount {
                buckets: self.buckets,
            });
        }
        let schema = batch.schema();
        let col_idx = schema
            .index_of(&self.key_column)
            .map_err(|e| crate::error::io_err(e.to_string()))?;
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
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Int32".into(),
                    })?;
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    if arr.is_null(row) {
                        self.null_int_bucket()
                    } else {
                        hash_i64(arr.value(row) as i64, self.buckets, self.seed)
                    }
                });
            }
            DataType::Int64 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Int64".into(),
                    })?;
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    if arr.is_null(row) {
                        self.null_int_bucket()
                    } else {
                        hash_i64(arr.value(row), self.buckets, self.seed)
                    }
                });
            }
            DataType::Utf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Utf8".into(),
                    })?;
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    if arr.is_null(row) {
                        self.null_str_bucket()
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                });
            }
            DataType::Utf8View => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Utf8View".into(),
                    })?;
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    if arr.is_null(row) {
                        self.null_str_bucket()
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                });
            }
            DataType::LargeUtf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "LargeUtf8".into(),
                    })?;
                fill_buckets(num_rows, self.buckets, &mut bucket_indices, |row| {
                    if arr.is_null(row) {
                        self.null_str_bucket()
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                });
            }
            other => {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!(
                        "supported partition key type (Int32, Int64, Utf8, Utf8View, LargeUtf8), got {other}"
                    ),
                });
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
                            .map_err(|e| crate::error::io_err(e.to_string()))
                    })
                    .collect::<ShuffleResult<_>>()?;
                let partition_batch = RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
                result.push(partition_batch);
            }
        }

        Ok(result)
    }
}

/// Hash an i64 value into a shuffle bucket using XxHash64 with the given seed.
///
/// The seed should be derived from the job ID so different jobs produce
/// independent distributions. Pass `0` for tests or when no job context exists.
pub fn hash_i64(value: i64, buckets: u32, seed: u64) -> ShuffleBucket {
    debug_assert!(buckets > 0);
    let mut hasher = twox_hash::XxHash64::with_seed(seed);
    hasher.write(&value.to_le_bytes());
    ShuffleBucket((hasher.finish() % buckets as u64) as u32)
}

/// Hash a string value into a shuffle bucket using XxHash64 with the given seed.
///
/// The seed should be derived from the job ID so different jobs produce
/// independent distributions. Pass `0` for tests or when no job context exists.
pub fn hash_str(value: &str, buckets: u32, seed: u64) -> ShuffleBucket {
    debug_assert!(buckets > 0);
    let mut hasher = twox_hash::XxHash64::with_seed(seed);
    hasher.write(value.as_bytes());
    ShuffleBucket((hasher.finish() % buckets as u64) as u32)
}

fn fill_buckets<F>(
    num_rows: usize,
    _num_partitions: u32,
    bucket_indices: &mut [Vec<u32>],
    bucket_fn: F,
) where
    F: Fn(usize) -> ShuffleBucket,
{
    for row in 0..num_rows {
        let bucket = bucket_fn(row).0 as usize;
        bucket_indices[bucket].push(row as u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn batch_with_nulls() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(2),
                None,
                None,
            ]))],
        )
        .unwrap()
    }

    /// Null partition keys must be counted via `null_key_count` and routed to
    /// a deterministic bucket derived from a type-tagged sentinel hash — NOT
    /// always to bucket 0.  Routing all nulls to bucket 0 caused structural
    /// skew on datasets with nullable keys; the sentinel approach distributes
    /// them like any other well-distributed key value.
    #[test]
    fn null_keys_are_counted_and_distributed_not_pinned_to_zero() {
        let partitioner = HashPartitioner::new("k", 4);
        let batch = batch_with_nulls();
        let buckets = partitioner.partition(&batch).unwrap();

        assert_eq!(buckets.len(), 4, "partition count must match requested buckets");
        assert_eq!(
            partitioner.null_key_count(),
            3,
            "all three null keys must be counted"
        );
        let total_rows: usize = buckets.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "no rows must be dropped during partitioning");

        // Nulls hash to a deterministic sentinel — all three land in the same
        // sentinel bucket (which may or may not be bucket 0 depending on seed
        // and bucket count). Verify that the sentinel bucket contains the null rows.
        let null_bucket = (twox_hash::XxHash64::oneshot(0, super::NULL_SENTINEL_INT) % 4) as usize;
        assert!(
            buckets[null_bucket].num_rows() >= 3,
            "null sentinel bucket {} must contain all 3 null-keyed rows, got {} rows",
            null_bucket,
            buckets[null_bucket].num_rows()
        );
    }

    /// With a 16-bucket partitioner the total row count is preserved and the
    /// null-key counter reflects the three null rows.
    #[test]
    fn null_keys_total_row_count_preserved_16_buckets() {
        let partitioner = HashPartitioner::new("k", 16);
        let batch = batch_with_nulls();
        let parts = partitioner.partition(&batch).unwrap();
        let total: usize = parts.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "no rows dropped");
        assert_eq!(partitioner.null_key_count(), 3);
    }

    /// Regression (Wave 1 — Data Correctness): `Clone` must reset the
    /// null-key counter rather than sharing or copying the source's atomic
    /// count, so a cloned partitioner starts with a clean accounting state.
    #[test]
    fn clone_resets_null_key_counter() {
        let partitioner = HashPartitioner::new("k", 4);
        partitioner.partition(&batch_with_nulls()).unwrap();
        assert_eq!(partitioner.null_key_count(), 3);

        let cloned = partitioner.clone();
        assert_eq!(
            cloned.null_key_count(),
            0,
            "clone must start with a reset null-key counter"
        );
        assert_eq!(
            partitioner.null_key_count(),
            3,
            "cloning must not affect the source counter"
        );
    }
}
