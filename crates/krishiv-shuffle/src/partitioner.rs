//! Shuffle-routing hash partitioner (XxHash64).
//!
//! **Type boundary**: keys are hashed with XxHash64 for deterministic bucket
//! assignment in a shuffle exchange. This is intentionally *different* from
//! `krishiv_common::partition`, which uses SHA-256 with a domain-separation
//! prefix for keyed-semantics partitioning (join keys, state sharding). The
//! two hash functions are not interchangeable — never use this partitioner
//! where keyed-semantics guarantees are required.

use crate::{ShuffleError, ShuffleResult};
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
/// Supported key column types: `Int32`, `Int64`, `Utf8`.
#[derive(Debug)]
pub struct HashPartitioner {
    key_column: String,
    buckets: u32,
    null_key_count: AtomicU64,
}

impl Clone for HashPartitioner {
    fn clone(&self) -> Self {
        Self {
            key_column: self.key_column.clone(),
            buckets: self.buckets,
            null_key_count: AtomicU64::new(0),
        }
    }
}

impl HashPartitioner {
    pub fn new(key_column: impl Into<String>, buckets: u32) -> Self {
        Self {
            key_column: key_column.into(),
            buckets,
            null_key_count: AtomicU64::new(0),
        }
    }

    pub fn null_key_count(&self) -> u64 {
        self.null_key_count.load(Ordering::Relaxed)
    }

    fn inc_null(&self) -> u32 {
        self.null_key_count.fetch_add(1, Ordering::Relaxed);
        0
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
                        self.inc_null()
                    } else {
                        hash_i64(arr.value(row) as i64, self.buckets)
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
                        self.inc_null()
                    } else {
                        hash_i64(arr.value(row), self.buckets)
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
                        self.inc_null()
                    } else {
                        hash_str(arr.value(row), self.buckets)
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
                        self.inc_null()
                    } else {
                        hash_str(arr.value(row), self.buckets)
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
                        self.inc_null()
                    } else {
                        hash_str(arr.value(row), self.buckets)
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

pub fn hash_i64(value: i64, buckets: u32) -> u32 {
    debug_assert!(buckets > 0);
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(&value.to_le_bytes());
    (hasher.finish() % buckets as u64) as u32
}

pub fn hash_str(value: &str, buckets: u32) -> u32 {
    debug_assert!(buckets > 0);
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(value.as_bytes());
    (hasher.finish() % buckets as u64) as u32
}

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

    /// Regression (Wave 1 — Data Correctness): null partition keys must be
    /// routed to a deterministic bucket (bucket 0) and counted via
    /// `null_key_count`, rather than silently dropped or panicking on the
    /// missing hash input.
    #[test]
    fn null_keys_route_to_bucket_zero_and_are_counted() {
        let partitioner = HashPartitioner::new("k", 4);
        let batch = batch_with_nulls();
        let buckets = partitioner.partition(&batch).unwrap();

        assert_eq!(buckets.len(), 4);
        assert_eq!(
            partitioner.null_key_count(),
            3,
            "all three null keys must be counted"
        );
        // Every null-keyed row lands in bucket 0 (non-null keys may also hash
        // there, so bucket 0 holds at least the null rows).
        assert!(
            buckets[0].num_rows() >= 3,
            "bucket 0 must contain all null-keyed rows, got {} rows",
            buckets[0].num_rows()
        );
        let total_rows: usize = buckets.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "no rows must be dropped during partitioning");
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
