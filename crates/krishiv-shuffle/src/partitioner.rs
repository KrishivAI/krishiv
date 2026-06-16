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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
        ShuffleBucket(
            (twox_hash::XxHash64::oneshot(self.seed, NULL_SENTINEL_INT) % self.buckets as u64)
                as u32,
        )
    }

    /// Hash the null sentinel for string columns and count the null.
    fn null_str_bucket(&self) -> ShuffleBucket {
        self.null_key_count.fetch_add(1, Ordering::Relaxed);
        ShuffleBucket(
            (twox_hash::XxHash64::oneshot(self.seed, NULL_SENTINEL_STR) % self.buckets as u64)
                as u32,
        )
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

// ── Salted hash partitioning (Phase 2.8 skew mitigation) ─────────────────────

/// Instruction to split one hot hash bucket into `salt_factor` sub-partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaltSpec {
    /// The hot bucket to split. Must be `< base_buckets`.
    pub partition_id: u32,
    /// Number of sub-partitions the hot bucket's rows are spread across.
    /// Must be `>= 2` (a factor of 1 is a no-op and is rejected).
    pub salt_factor: u32,
}

/// Hash partitioner that splits designated hot buckets into sub-partitions.
///
/// Rows are first routed by the wrapped [`HashPartitioner`]. Rows landing in
/// a salted (hot) bucket `b` with factor `k` are then spread round-robin over
/// `k` sub-partitions: the original ID `b` plus `k - 1` extra partitions
/// appended after the base bucket range. The extra IDs are deterministic:
///
/// ```text
/// total_partitions = base_buckets + Σ (salt_factor_i − 1)
/// sub_ids(b_i)     = { b_i } ∪ { base_buckets + offset_i + t : t ∈ 0..k_i−1 }
/// offset_i         = Σ_{j<i} (salt_factor_j − 1)        (salts sorted by id)
/// ```
///
/// **Correctness scope**: salting breaks key→partition affinity for the hot
/// bucket, so it is only safe when the consumer merges sub-partitions before
/// keyed processing — partial-aggregate shuffles and sort-merge runs qualify;
/// keyed streaming state does NOT (the scheduler never applies salt overrides
/// to streaming jobs).
#[derive(Debug, Clone)]
pub struct SaltedHashPartitioner {
    inner: HashPartitioner,
    base_buckets: u32,
    /// Sorted by `partition_id`; validated unique and in-range.
    salts: Vec<SaltSpec>,
}

impl SaltedHashPartitioner {
    /// Wrap `inner` (with `base_buckets` output partitions) and split each
    /// bucket named in `salts` into its `salt_factor` sub-partitions.
    pub fn new(
        inner: HashPartitioner,
        base_buckets: u32,
        mut salts: Vec<SaltSpec>,
    ) -> ShuffleResult<Self> {
        if base_buckets == 0 {
            return Err(ShuffleError::InvalidPartitionCount {
                buckets: base_buckets,
            });
        }
        salts.sort_by_key(|s| s.partition_id);
        for pair in salts.windows(2) {
            if pair[0].partition_id == pair[1].partition_id {
                return Err(crate::error::io_err(format!(
                    "duplicate salt spec for partition {}",
                    pair[0].partition_id
                )));
            }
        }
        for salt in &salts {
            if salt.partition_id >= base_buckets {
                return Err(crate::error::io_err(format!(
                    "salt partition {} out of range (base buckets {})",
                    salt.partition_id, base_buckets
                )));
            }
            if salt.salt_factor < 2 {
                return Err(crate::error::io_err(format!(
                    "salt factor {} for partition {} must be >= 2",
                    salt.salt_factor, salt.partition_id
                )));
            }
        }
        Ok(Self {
            inner,
            base_buckets,
            salts,
        })
    }

    /// Total output partitions including the appended sub-partitions.
    pub fn total_partitions(&self) -> u32 {
        let extra: u32 = self.salts.iter().map(|s| s.salt_factor - 1).sum();
        self.base_buckets + extra
    }

    /// All partition IDs that carry rows of hot bucket `partition_id`
    /// (the bucket itself first, then its extra sub-partitions). Returns a
    /// single-element vector for non-salted buckets.
    pub fn sub_partition_ids(&self, partition_id: u32) -> Vec<u32> {
        let mut offset = 0u32;
        for salt in &self.salts {
            if salt.partition_id == partition_id {
                let mut ids = Vec::with_capacity(salt.salt_factor as usize);
                ids.push(partition_id);
                for t in 0..salt.salt_factor - 1 {
                    ids.push(self.base_buckets + offset + t);
                }
                return ids;
            }
            offset += salt.salt_factor - 1;
        }
        vec![partition_id]
    }

    /// The base bucket whose rows a given output partition carries.
    ///
    /// Identity for IDs below `base_buckets`; extra sub-partitions map back
    /// to the hot bucket they were split from. Returns `None` for IDs at or
    /// beyond `total_partitions()`.
    pub fn parent_of(&self, partition_id: u32) -> Option<u32> {
        if partition_id < self.base_buckets {
            return Some(partition_id);
        }
        let mut offset = 0u32;
        for salt in &self.salts {
            let extra = salt.salt_factor - 1;
            if partition_id < self.base_buckets + offset + extra {
                return Some(salt.partition_id);
            }
            offset += extra;
        }
        None
    }

    /// Partition `batch` into `total_partitions()` sub-batches.
    ///
    /// Rows in salted buckets are spread round-robin (by row position within
    /// the bucket) across the bucket's sub-partitions.
    pub fn partition(&self, batch: &RecordBatch) -> ShuffleResult<Vec<RecordBatch>> {
        let base = self.inner.partition(batch)?;
        let total = self.total_partitions() as usize;
        let schema = batch.schema();
        let mut result: Vec<RecordBatch> = Vec::with_capacity(total);
        // Start with the base buckets; hot buckets are replaced below.
        result.extend(base.iter().cloned());
        result.resize_with(total, || RecordBatch::new_empty(schema.clone()));

        for salt in &self.salts {
            let hot = &base[salt.partition_id as usize];
            if hot.num_rows() == 0 {
                continue;
            }
            let sub_ids = self.sub_partition_ids(salt.partition_id);
            let k = sub_ids.len();
            // Round-robin row assignment across the k sub-partitions.
            let mut per_sub: Vec<Vec<u32>> = vec![Vec::new(); k];
            for row in 0..hot.num_rows() {
                per_sub[row % k].push(row as u32);
            }
            for (sub_idx, rows) in per_sub.iter().enumerate() {
                let target = sub_ids[sub_idx] as usize;
                if rows.is_empty() {
                    result[target] = RecordBatch::new_empty(schema.clone());
                    continue;
                }
                let index_arr = UInt32Array::from_iter_values(rows.iter().copied());
                let columns: Vec<Arc<dyn arrow::array::Array>> = hot
                    .columns()
                    .iter()
                    .map(|col| {
                        take(col.as_ref(), &index_arr, None)
                            .map_err(|e| crate::error::io_err(e.to_string()))
                    })
                    .collect::<ShuffleResult<_>>()?;
                result[target] = RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|e| crate::error::io_err(e.to_string()))?;
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
    ShuffleBucket(
        (twox_hash::XxHash64::oneshot(seed, &value.to_le_bytes()) % buckets as u64) as u32,
    )
}

/// Hash a string value into a shuffle bucket using XxHash64 with the given seed.
///
/// The seed should be derived from the job ID so different jobs produce
/// independent distributions. Pass `0` for tests or when no job context exists.
pub fn hash_str(value: &str, buckets: u32, seed: u64) -> ShuffleBucket {
    debug_assert!(buckets > 0);
    ShuffleBucket(
        (twox_hash::XxHash64::oneshot(seed, value.as_bytes()) % buckets as u64) as u32,
    )
}

fn fill_buckets<F>(
    num_rows: usize,
    num_partitions: u32,
    bucket_indices: &mut [Vec<u32>],
    bucket_fn: F,
) where
    F: Fn(usize) -> ShuffleBucket,
{
    let hint = num_rows / (num_partitions as usize).max(1);
    for v in bucket_indices.iter_mut() {
        v.reserve(hint);
    }
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

        assert_eq!(
            buckets.len(),
            4,
            "partition count must match requested buckets"
        );
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

    // ── SaltedHashPartitioner (Phase 2.8) ────────────────────────────────

    fn skewed_batch(rows: i64) -> RecordBatch {
        // All rows share the same key → all land in one hot bucket.
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                42i64,
                rows as usize,
            )))],
        )
        .unwrap()
    }

    #[test]
    fn salted_partitioner_layout_math() {
        let p = SaltedHashPartitioner::new(
            HashPartitioner::new("k", 4),
            4,
            vec![
                SaltSpec {
                    partition_id: 1,
                    salt_factor: 3,
                },
                SaltSpec {
                    partition_id: 3,
                    salt_factor: 2,
                },
            ],
        )
        .unwrap();

        // total = 4 + (3-1) + (2-1) = 7
        assert_eq!(p.total_partitions(), 7);
        // bucket 1 → {1, 4, 5}; bucket 3 → {3, 6}; bucket 0 → {0}
        assert_eq!(p.sub_partition_ids(1), vec![1, 4, 5]);
        assert_eq!(p.sub_partition_ids(3), vec![3, 6]);
        assert_eq!(p.sub_partition_ids(0), vec![0]);
        // Parent mapping is the inverse.
        assert_eq!(p.parent_of(4), Some(1));
        assert_eq!(p.parent_of(5), Some(1));
        assert_eq!(p.parent_of(6), Some(3));
        assert_eq!(p.parent_of(2), Some(2));
        assert_eq!(p.parent_of(7), None);
    }

    #[test]
    fn salted_partitioner_splits_hot_bucket_rows() {
        let inner = HashPartitioner::new("k", 4);
        let batch = skewed_batch(90);
        // Find the hot bucket first with a plain partitioner.
        let plain = inner.partition(&batch).unwrap();
        let hot = plain
            .iter()
            .position(|b| b.num_rows() > 0)
            .expect("one bucket holds all rows") as u32;

        let salted = SaltedHashPartitioner::new(
            HashPartitioner::new("k", 4),
            4,
            vec![SaltSpec {
                partition_id: hot,
                salt_factor: 3,
            }],
        )
        .unwrap();

        let parts = salted.partition(&batch).unwrap();
        assert_eq!(parts.len(), 6, "4 base + 2 extra sub-partitions");
        let total: usize = parts.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 90, "no rows lost");
        // The 90 hot rows are spread evenly across the 3 sub-partitions.
        for id in salted.sub_partition_ids(hot) {
            assert_eq!(
                parts[id as usize].num_rows(),
                30,
                "round-robin spreads hot rows evenly (sub-partition {id})"
            );
        }
        // Non-hot base buckets are empty.
        for b in 0..4u32 {
            if b != hot {
                assert_eq!(parts[b as usize].num_rows(), 0);
            }
        }
    }

    #[test]
    fn salted_partitioner_no_salt_for_cold_buckets() {
        // Salting bucket X while data lands in bucket Y must leave Y intact.
        let batch = skewed_batch(10);
        let plain = HashPartitioner::new("k", 4).partition(&batch).unwrap();
        let hot = plain.iter().position(|b| b.num_rows() > 0).unwrap() as u32;
        let cold = (0..4u32).find(|b| *b != hot).unwrap();

        let salted = SaltedHashPartitioner::new(
            HashPartitioner::new("k", 4),
            4,
            vec![SaltSpec {
                partition_id: cold,
                salt_factor: 2,
            }],
        )
        .unwrap();
        let parts = salted.partition(&batch).unwrap();
        assert_eq!(parts[hot as usize].num_rows(), 10, "hot bucket untouched");
        assert_eq!(parts[4].num_rows(), 0, "cold sub-partition stays empty");
    }

    #[test]
    fn salted_partitioner_rejects_invalid_specs() {
        // Factor < 2.
        assert!(
            SaltedHashPartitioner::new(
                HashPartitioner::new("k", 4),
                4,
                vec![SaltSpec {
                    partition_id: 0,
                    salt_factor: 1
                }],
            )
            .is_err()
        );
        // Out-of-range partition.
        assert!(
            SaltedHashPartitioner::new(
                HashPartitioner::new("k", 4),
                4,
                vec![SaltSpec {
                    partition_id: 4,
                    salt_factor: 2
                }],
            )
            .is_err()
        );
        // Duplicate partition.
        assert!(
            SaltedHashPartitioner::new(
                HashPartitioner::new("k", 4),
                4,
                vec![
                    SaltSpec {
                        partition_id: 2,
                        salt_factor: 2
                    },
                    SaltSpec {
                        partition_id: 2,
                        salt_factor: 3
                    }
                ],
            )
            .is_err()
        );
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
