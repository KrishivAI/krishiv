//! E2.4 — Range-based shuffle partitioner.
//!
//! Range partitioning assigns rows to buckets based on sorted key boundaries
//! rather than hash values.  This preserves key order across partitions, which
//! is required for the GlobalSort merge phase.
//!
//! # Workflow
//! 1. Call [`RangeSampler::sample`] on each input batch to collect a random
//!    sample of key values.
//! 2. Call [`RangeSampler::build_boundaries`] to sort the sample and pick
//!    evenly-spaced boundary points that split the key space into N buckets.
//! 3. Create a [`RangePartitioner`] with those boundaries and call
//!    [`RangePartitioner::partition`] on each batch to route rows to buckets.
//!
//! Rows with the same key always land in the same bucket.  The final sort
//! merge step assumes each bucket is handed to a single sort-merge executor.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::{ShuffleError, ShuffleResult};

// ── Boundary value ────────────────────────────────────────────────────────────

/// A comparable key boundary used to assign rows to range buckets.
///
/// Boundaries define the *upper bound* of each bucket except the last.
/// Bucket 0 receives keys < boundary[0]; bucket i receives
/// boundary[i-1] ≤ key < boundary[i]; the last bucket receives all remaining keys.
#[derive(Debug, Clone, PartialEq)]
pub enum RangeBound {
    Int32(i32),
    Int64(i64),
    Utf8(String),
}

impl RangeBound {
    fn from_i32(v: i32) -> Self {
        Self::Int32(v)
    }
    fn from_i64(v: i64) -> Self {
        Self::Int64(v)
    }
}

// ── Sampler ───────────────────────────────────────────────────────────────────

/// Collects an unbiased reservoir sample of key values from input batches.
///
/// Uses Vitter's Algorithm R for reservoir sampling: every row in the input
/// has an equal probability of appearing in the final sample regardless of
/// input order, batch size, or total count. This produces unbiased range
/// boundaries even for skewed distributions.
///
/// Call [`sample`][Self::sample] for each input batch, then
/// [`build_boundaries`][Self::build_boundaries] once all input is consumed.
///
/// G5: Replaced deterministic stride sampling with proper reservoir sampling
/// so that `build_boundaries` produces unbiased quantile estimates.
#[derive(Debug)]
pub struct RangeSampler {
    i32_samples: Vec<i32>,
    i64_samples: Vec<i64>,
    str_samples: Vec<String>,
    /// Maximum number of samples to retain (reservoir capacity).
    reservoir_size: usize,
    /// Total number of non-null rows seen so far (for Vitter's Algorithm R).
    rows_seen: usize,
    /// Deterministic seed for the PRNG used in reservoir sampling.
    seed: u64,
}

impl Default for RangeSampler {
    fn default() -> Self {
        Self {
            i32_samples: Vec::new(),
            i64_samples: Vec::new(),
            str_samples: Vec::new(),
            reservoir_size: 10_000,
            rows_seen: 0,
            seed: 0,
        }
    }
}

/// Simple deterministic LCG PRNG for reservoir sampling (no external deps).
/// Returns a value in `[0, n)`.
fn lcg_rand(state: &mut u64, n: usize) -> usize {
    // LCG parameters from Knuth.
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    // Use the upper 32 bits for better quality.
    ((*state >> 33) as usize) % n
}

impl RangeSampler {
    /// Create a sampler with a reservoir of `reservoir_size` samples.
    ///
    /// The `sample_fraction` parameter is kept for API compatibility but is
    /// unused — the reservoir size governs how many samples are retained.
    pub fn new(sample_fraction: f64) -> Self {
        // Convert fraction to a reservoir size: at 1.0 fraction keep up to
        // 10_000 samples; at 0.01 keep 100. The actual per-row probability
        // of inclusion is determined by Algorithm R at `build_boundaries` time.
        let reservoir_size = ((sample_fraction.clamp(0.01, 1.0) * 10_000.0) as usize).max(1);
        Self {
            reservoir_size,
            ..Default::default()
        }
    }

    /// Collect key values from `batch` column `key_column` using reservoir sampling.
    pub fn sample(&mut self, batch: &RecordBatch, key_column: &str) -> ShuffleResult<()> {
        let idx = batch
            .schema()
            .index_of(key_column)
            .map_err(|_e| ShuffleError::InvalidPartitionCount { buckets: 0 })?;
        let col = batch.column(idx);
        let k = self.reservoir_size;

        match col.data_type() {
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Int32 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    let i = self.rows_seen;
                    self.rows_seen += 1;
                    if i < k {
                        self.i32_samples.push(v);
                    } else {
                        let j = lcg_rand(&mut self.seed, i + 1);
                        if j < k && let Some(s) = self.i32_samples.get_mut(j) { *s = v; }
                    }
                }
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Int64 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    let i = self.rows_seen;
                    self.rows_seen += 1;
                    if i < k {
                        self.i64_samples.push(v);
                    } else {
                        let j = lcg_rand(&mut self.seed, i + 1);
                        if j < k && let Some(s) = self.i64_samples.get_mut(j) { *s = v; }
                    }
                }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Utf8 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row).to_owned();
                    let i = self.rows_seen;
                    self.rows_seen += 1;
                    if i < k {
                        self.str_samples.push(v);
                    } else {
                        let j = lcg_rand(&mut self.seed, i + 1);
                        if j < k && let Some(s) = self.str_samples.get_mut(j) { *s = v; }
                    }
                }
            }
            other => {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!("range partition key must be Int32/Int64/Utf8, got {other}"),
                });
            }
        }
        Ok(())
    }

    /// Build `buckets - 1` boundary values from the collected sample.
    ///
    /// Returns the boundary list sorted in ascending order. The caller passes
    /// this to [`RangePartitioner::new`].
    pub fn build_boundaries(mut self, buckets: u32) -> ShuffleResult<Vec<RangeBound>> {
        if buckets == 0 {
            return Err(ShuffleError::InvalidPartitionCount { buckets: 0 });
        }
        let num_boundaries = (buckets - 1) as usize;
        if num_boundaries == 0 {
            return Ok(vec![]);
        }

        if !self.i32_samples.is_empty() {
            self.i32_samples.sort_unstable();
            return Ok(pick_boundaries(
                &self.i32_samples,
                num_boundaries,
                RangeBound::from_i32,
            ));
        }
        if !self.i64_samples.is_empty() {
            self.i64_samples.sort_unstable();
            return Ok(pick_boundaries(
                &self.i64_samples,
                num_boundaries,
                RangeBound::from_i64,
            ));
        }
        if !self.str_samples.is_empty() {
            self.str_samples.sort_unstable();
            return Ok(pick_boundaries_str(&self.str_samples, num_boundaries));
        }

        // No samples — all data went to bucket 0.
        Ok(vec![])
    }
}

fn pick_boundaries<T: Clone, F: Fn(T) -> RangeBound>(
    sorted: &[T],
    num_boundaries: usize,
    f: F,
) -> Vec<RangeBound> {
    let n = sorted.len();
    if n == 0 {
        return vec![];
    }
    let mut boundaries = Vec::with_capacity(num_boundaries);
    for i in 1..=num_boundaries {
        let idx = (i * n / (num_boundaries + 1)).min(n - 1);
        if let Some(item) = sorted.get(idx) { boundaries.push(f(item.clone())); }
    }
    boundaries
}

fn pick_boundaries_str(sorted: &[String], num_boundaries: usize) -> Vec<RangeBound> {
    pick_boundaries(sorted, num_boundaries, RangeBound::Utf8)
}

// ── RangePartitioner ─────────────────────────────────────────────────────────

/// Assigns rows to buckets based on pre-computed range boundaries.
///
/// All rows are assigned to `buckets` buckets.  Rows with keys less than
/// `boundaries[0]` go to bucket 0; rows with keys in [`boundaries[i-1]`,
/// `boundaries[i]`) go to bucket i; rows ≥ last boundary go to `buckets-1`.
#[derive(Debug, Clone)]
pub struct RangePartitioner {
    key_column: String,
    boundaries: Vec<RangeBound>,
    buckets: u32,
}

impl RangePartitioner {
    /// Create a range partitioner with pre-built boundaries.
    pub fn new(key_column: impl Into<String>, boundaries: Vec<RangeBound>, buckets: u32) -> Self {
        Self {
            key_column: key_column.into(),
            boundaries,
            buckets,
        }
    }

    /// Number of output buckets.
    pub fn buckets(&self) -> u32 {
        self.buckets
    }

    /// Partition `batch` into `self.buckets` sub-batches in range order.
    pub fn partition(&self, batch: &RecordBatch) -> ShuffleResult<Vec<RecordBatch>> {
        if self.buckets == 0 {
            return Err(ShuffleError::InvalidPartitionCount { buckets: 0 });
        }
        let schema = batch.schema();
        let col_idx = schema
            .index_of(&self.key_column)
            .map_err(|e| crate::error::io_err(e.to_string()))?;
        let col = batch.column(col_idx);
        let n = self.buckets as usize;
        let mut bucket_indices: Vec<Vec<u32>> = vec![Vec::new(); n];

        // B5: Validate that the boundary type matches the column type.
        // If boundaries were built for Int64 but the column is Int32, silently
        // routing all rows to the last bucket was a data-correctness bug.
        if let Some(first_bound) = self.boundaries.first() {
            let col_type = col.data_type();
            let bound_type_name = match first_bound {
                RangeBound::Int32(_) => "Int32",
                RangeBound::Int64(_) => "Int64",
                RangeBound::Utf8(_) => "Utf8",
            };
            let col_type_name = match col_type {
                DataType::Int32 => "Int32",
                DataType::Int64 => "Int64",
                DataType::Utf8 => "Utf8",
                other => {
                    return Err(ShuffleError::TypeMismatch {
                        expected: format!(
                            "range partitioner: unsupported key type {other}, expected Int32/Int64/Utf8"
                        ),
                    });
                }
            };
            if bound_type_name != col_type_name {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!(
                        "range partition boundaries are {bound_type_name} but column '{}' is {col_type_name}",
                        self.key_column
                    ),
                });
            }
        }

        match col.data_type() {
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Int32 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    let bucket = if arr.is_null(row) {
                        0
                    } else {
                        self.bucket_for_i32(arr.value(row))
                    };
                    if let Some(v) = bucket_indices.get_mut(bucket) { v.push(row as u32); }
                }
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Int64 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    let bucket = if arr.is_null(row) {
                        0
                    } else {
                        self.bucket_for_i64(arr.value(row))
                    };
                    if let Some(v) = bucket_indices.get_mut(bucket) { v.push(row as u32); }
                }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ShuffleError::TypeMismatch {
                        expected: "Utf8 downcast failed".into(),
                    }
                })?;
                for row in 0..batch.num_rows() {
                    let bucket = if arr.is_null(row) {
                        0
                    } else {
                        self.bucket_for_str(arr.value(row))
                    };
                    if let Some(v) = bucket_indices.get_mut(bucket) { v.push(row as u32); }
                }
            }
            other => {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!(
                        "range partitioner: unsupported key type {other}, expected Int32/Int64/Utf8"
                    ),
                });
            }
        }

        // Materialise per-bucket RecordBatches.
        let mut result = Vec::with_capacity(n);
        for indices in &bucket_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(schema.clone()));
            } else {
                let idx_arr = UInt32Array::from_iter_values(indices.iter().copied());
                let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &idx_arr, None)
                            .map_err(|e| crate::error::io_err(e.to_string()))
                    })
                    .collect::<ShuffleResult<_>>()?;
                result.push(
                    RecordBatch::try_new(schema.clone(), columns)
                        .map_err(|e| crate::error::io_err(e.to_string()))?,
                );
            }
        }
        Ok(result)
    }

    fn bucket_for_i32(&self, v: i32) -> usize {
        for (i, b) in self.boundaries.iter().enumerate() {
            if let RangeBound::Int32(bound) = b
                && v < *bound
            {
                return i;
            }
        }
        (self.buckets - 1) as usize
    }

    fn bucket_for_i64(&self, v: i64) -> usize {
        for (i, b) in self.boundaries.iter().enumerate() {
            if let RangeBound::Int64(bound) = b
                && v < *bound
            {
                return i;
            }
        }
        (self.buckets - 1) as usize
    }

    fn bucket_for_str(&self, v: &str) -> usize {
        for (i, b) in self.boundaries.iter().enumerate() {
            if let RangeBound::Utf8(bound) = b
                && v < bound.as_str()
            {
                return i;
            }
        }
        (self.buckets - 1) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    fn int_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    #[test]
    fn range_partitioner_three_buckets_splits_by_boundary() {
        // Boundaries: [33, 66] → buckets [<33, 33-65, ≥66]
        let boundaries = vec![RangeBound::Int32(33), RangeBound::Int32(66)];
        let p = RangePartitioner::new("id", boundaries, 3);
        let batch = int_batch(&[10, 20, 33, 50, 66, 80, 99]);
        let partitions = p.partition(&batch).unwrap();

        assert_eq!(partitions.len(), 3);
        // bucket 0: id < 33  → 10, 20
        assert_eq!(partitions[0].num_rows(), 2);
        // bucket 1: 33 ≤ id < 66  → 33, 50
        assert_eq!(partitions[1].num_rows(), 2);
        // bucket 2: id ≥ 66  → 66, 80, 99
        assert_eq!(partitions[2].num_rows(), 3);
    }

    #[test]
    fn range_partitioner_total_rows_preserved() {
        let boundaries = vec![RangeBound::Int32(50)];
        let p = RangePartitioner::new("id", boundaries, 2);
        let batch = int_batch(&[1, 10, 50, 70, 100]);
        let partitions = p.partition(&batch).unwrap();
        let total: usize = partitions.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn range_partitioner_no_boundaries_all_in_bucket_zero() {
        let p = RangePartitioner::new("id", vec![], 1);
        let batch = int_batch(&[5, 10, 15]);
        let partitions = p.partition(&batch).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].num_rows(), 3);
    }

    #[test]
    fn sampler_builds_two_boundaries_for_three_buckets() {
        let batch = int_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let mut sampler = RangeSampler::new(1.0);
        sampler.sample(&batch, "id").unwrap();
        let boundaries = sampler.build_boundaries(3).unwrap();
        assert_eq!(boundaries.len(), 2, "3 buckets → 2 boundaries");
        // Boundaries should be roughly at 1/3 and 2/3 of the sorted sample.
        if let (RangeBound::Int32(lo), RangeBound::Int32(hi)) = (&boundaries[0], &boundaries[1]) {
            assert!(*lo < *hi, "boundaries must be ordered");
            assert!(*lo >= 1 && *lo <= 10);
        } else {
            panic!("expected Int32 boundaries");
        }
    }

    #[test]
    fn sampler_single_bucket_returns_no_boundaries() {
        let batch = int_batch(&[1, 2, 3]);
        let mut sampler = RangeSampler::new(1.0);
        sampler.sample(&batch, "id").unwrap();
        let boundaries = sampler.build_boundaries(1).unwrap();
        assert!(boundaries.is_empty());
    }
}
