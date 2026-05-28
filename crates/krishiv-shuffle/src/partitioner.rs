use crate::{ShuffleError, ShuffleResult};
use arrow::array::{
    Array, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::hash::Hasher;
use std::sync::Arc;

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
                        0
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
                        0
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
                        0
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
                        0
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
                        0
                    } else {
                        hash_str(arr.value(row), self.buckets)
                    }
                });
            }
            other => {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!("supported partition key type (Int32, Int64, Utf8, Utf8View, LargeUtf8), got {other}"),
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
