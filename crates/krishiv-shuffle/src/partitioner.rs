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

/// Splits an Arrow `RecordBatch` into N buckets by hashing its key columns.
///
/// Supported key column types: `Int32`, `Int64`, `Utf8`, `Utf8View`, `LargeUtf8`.
///
/// The hash seed should be derived from the job ID so different jobs produce
/// independent bucket distributions. This prevents both accidental and
/// adversarial concentration caused by fixed-seed hash collisions.
/// Use [`HashPartitioner::with_seed`] to set a per-job seed.
///
/// # Multi-column keys
///
/// A shuffle whose key is `(a, b)` must hash **both** columns. Hashing only the
/// first is co-location-correct — equal keys still meet, because equal `(a, b)`
/// implies equal `a` — so it produces right answers, which is why it survived
/// unnoticed. What it costs is parallelism: a key like
/// `(o_orderstatus, l_orderkey)` has three distinct values in its leading
/// column, so every row lands in one of three buckets and the other fifteen
/// reduce tasks get nothing.
///
/// TPC-H at SF100 does not expose it — a partial aggregate sits in front of the
/// low-cardinality cases, so only a handful of pre-aggregated rows cross the
/// wire and the bucket count is irrelevant. Do **not** attribute q5/q7/q16 to
/// this.
#[derive(Debug)]
pub struct HashPartitioner {
    /// Key columns in declaration order. Order is part of the contract: both
    /// sides of a join must hash the same columns in the same sequence, or equal
    /// keys land in different buckets and rows silently fail to meet.
    key_columns: Vec<String>,
    buckets: u32,
    /// XxHash64 seed. Defaults to 0; set per-job via `with_seed` to prevent
    /// pathological distributions from a fixed seed.
    seed: u64,
    null_key_count: AtomicU64,
}

impl Clone for HashPartitioner {
    fn clone(&self) -> Self {
        Self {
            key_columns: self.key_columns.clone(),
            buckets: self.buckets,
            seed: self.seed,
            null_key_count: AtomicU64::new(0),
        }
    }
}

/// Odd 64-bit constant (the golden-ratio prime) used to mix a second and
/// subsequent key column's hash into the running per-row accumulator.
const KEY_MIX_PRIME: u64 = 0x9E37_79B1_85EB_CA87;

/// Fold one more key column's hash into a running per-row accumulator.
///
/// Not commutative — `mix(mix(0,a),b) != mix(mix(0,b),a)` — which is what makes
/// column *order* part of the partitioning contract rather than an accident.
fn mix_key_hash(acc: u64, next: u64) -> u64 {
    acc.rotate_left(31) ^ next.wrapping_mul(KEY_MIX_PRIME)
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
        Self::new_multi(vec![key_column.into()], buckets)
    }

    /// A partitioner over a composite key.
    ///
    /// `key_columns` order is significant — see [`HashPartitioner`]. An empty
    /// list is accepted and routes every row to bucket 0, matching what callers
    /// already do when a stage declares no key (they skip partitioning
    /// entirely); erroring here would turn a keyless exchange into a failure.
    pub fn new_multi(key_columns: Vec<String>, buckets: u32) -> Self {
        Self {
            key_columns,
            buckets,
            seed: 0,
            null_key_count: AtomicU64::new(0),
        }
    }

    /// The key columns, in hashing order.
    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
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

    /// The number of output partitions this partitioner produces.
    pub fn partition_count(&self) -> u32 {
        self.buckets
    }

    /// Bucket for a null key of the given sentinel domain.
    ///
    /// Pure: the sentinel and the seed are both constant for the life of the
    /// partitioner, so this value is invariant across every row of a batch.
    /// It used to be recomputed — hash and all — once per null row, alongside
    /// an atomic increment per null row. On a nullable key column at SF100
    /// that is millions of redundant hashes and millions of contended atomic
    /// read-modify-writes to compute a constant. Callers now hoist it out of
    /// the row loop and account the nulls in one `null_count()` add.
    fn null_bucket(&self, sentinel: &[u8]) -> ShuffleBucket {
        ShuffleBucket((twox_hash::XxHash64::oneshot(self.seed, sentinel) % self.buckets as u64) as u32)
    }

    /// Record `count` null keys against the null-key counter in one add.
    fn count_nulls(&self, count: usize) {
        self.null_key_count
            .fetch_add(count as u64, Ordering::Relaxed);
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
        let n = self.buckets as usize;
        let num_rows = batch.num_rows();

        // Collect row indices per bucket.
        // P3.8: use a generic helper closure to avoid five near-identical loop bodies.
        let mut bucket_indices: Vec<Vec<u32>> = vec![Vec::new(); n];

        // A composite key needs each row's hashes from every key column combined
        // before the modulo, which the single-column typed closures below cannot
        // express. Kept as a separate branch on purpose: the one-column case is
        // the hot path over hundreds of millions of rows at SF100, and it stays
        // exactly as it was — same closures, same buckets, no per-row `Vec<u64>`.
        if self.key_columns.len() != 1 {
            let hashes = self.composite_row_hashes(batch)?;
            fill_buckets(&mut bucket_indices, num_rows, |row| {
                let hash = hashes.get(row).copied().unwrap_or(0);
                ShuffleBucket((hash % self.buckets as u64) as u32)
            })?;
            return gather_buckets(batch, &bucket_indices);
        }

        let key_column = self
            .key_columns
            .first()
            .ok_or_else(|| crate::error::io_err("partitioner has no key column".to_string()))?;
        let col_idx = schema
            .index_of(key_column)
            .map_err(|e| crate::error::io_err(e.to_string()))?;
        let key_col = batch.column(col_idx);

        match key_col.data_type() {
            DataType::Int32 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Int32".into(),
                    })?;
                let null_bucket = self.null_bucket(NULL_SENTINEL_INT);
                self.count_nulls(arr.null_count());
                fill_buckets(&mut bucket_indices, num_rows, |row| {
                    if arr.is_null(row) {
                        null_bucket
                    } else {
                        hash_i64(arr.value(row) as i64, self.buckets, self.seed)
                    }
                })?;
            }
            DataType::Int64 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Int64".into(),
                    })?;
                let null_bucket = self.null_bucket(NULL_SENTINEL_INT);
                self.count_nulls(arr.null_count());
                fill_buckets(&mut bucket_indices, num_rows, |row| {
                    if arr.is_null(row) {
                        null_bucket
                    } else {
                        hash_i64(arr.value(row), self.buckets, self.seed)
                    }
                })?;
            }
            DataType::Utf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Utf8".into(),
                    })?;
                let null_bucket = self.null_bucket(NULL_SENTINEL_STR);
                self.count_nulls(arr.null_count());
                fill_buckets(&mut bucket_indices, num_rows, |row| {
                    if arr.is_null(row) {
                        null_bucket
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                })?;
            }
            DataType::Utf8View => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "Utf8View".into(),
                    })?;
                let null_bucket = self.null_bucket(NULL_SENTINEL_STR);
                self.count_nulls(arr.null_count());
                fill_buckets(&mut bucket_indices, num_rows, |row| {
                    if arr.is_null(row) {
                        null_bucket
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                })?;
            }
            DataType::LargeUtf8 => {
                let arr = key_col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: "LargeUtf8".into(),
                    })?;
                let null_bucket = self.null_bucket(NULL_SENTINEL_STR);
                self.count_nulls(arr.null_count());
                fill_buckets(&mut bucket_indices, num_rows, |row| {
                    if arr.is_null(row) {
                        null_bucket
                    } else {
                        hash_str(arr.value(row), self.buckets, self.seed)
                    }
                })?;
            }
            other => {
                return Err(ShuffleError::TypeMismatch {
                    expected: format!(
                        "supported partition key type (Int32, Int64, Utf8, Utf8View, LargeUtf8), got {other}"
                    ),
                });
            }
        }

        gather_buckets(batch, &bucket_indices)
    }

    /// One combined hash per row across every key column, in declaration order.
    ///
    /// Nulls use the same type-tagged sentinels as the single-column path, so a
    /// null in any key position still distributes rather than pinning to a
    /// bucket. The null counter is incremented per null *value*, so a row with
    /// two null key columns counts twice — the counter measures null keys seen,
    /// not rows.
    fn composite_row_hashes(&self, batch: &RecordBatch) -> ShuffleResult<Vec<u64>> {
        let schema = batch.schema();
        let mut acc = vec![0u64; batch.num_rows()];
        for (position, name) in self.key_columns.iter().enumerate() {
            let col_idx = schema
                .index_of(name)
                .map_err(|e| crate::error::io_err(e.to_string()))?;
            let column = batch.column(col_idx);
            let nulls = self.fold_column_into(&mut acc, column.as_ref(), position == 0)?;
            self.count_nulls(nulls);
        }
        Ok(acc)
    }

    /// Fold one column's per-row hashes into `acc`, returning its null count.
    ///
    /// `is_first` writes the hash directly instead of mixing, which is what
    /// keeps a one-column composite key bucket-identical to the single-column
    /// fast path — the two must agree or the same logical shuffle key would
    /// route differently depending on which branch ran.
    fn fold_column_into(
        &self,
        acc: &mut [u64],
        column: &dyn Array,
        is_first: bool,
    ) -> ShuffleResult<usize> {
        let seed = self.seed;
        let mut apply = |row: usize, hash: u64| {
            if let Some(slot) = acc.get_mut(row) {
                *slot = if is_first {
                    hash
                } else {
                    mix_key_hash(*slot, hash)
                };
            }
        };
        macro_rules! fold {
            ($ty:ty, $expected:literal, $sentinel:expr, $bytes:expr) => {{
                let arr = column
                    .as_any()
                    .downcast_ref::<$ty>()
                    .ok_or_else(|| ShuffleError::TypeMismatch {
                        expected: $expected.into(),
                    })?;
                let null_hash = twox_hash::XxHash64::oneshot(seed, $sentinel);
                for row in 0..arr.len() {
                    let hash = if arr.is_null(row) {
                        null_hash
                    } else {
                        let value = arr.value(row);
                        twox_hash::XxHash64::oneshot(seed, $bytes(value).as_ref())
                    };
                    apply(row, hash);
                }
                Ok(arr.null_count())
            }};
        }
        match column.data_type() {
            DataType::Int32 => fold!(Int32Array, "Int32", NULL_SENTINEL_INT, |v: i32| (v as i64)
                .to_le_bytes()),
            DataType::Int64 => {
                fold!(Int64Array, "Int64", NULL_SENTINEL_INT, |v: i64| v.to_le_bytes())
            }
            DataType::Utf8 => fold!(StringArray, "Utf8", NULL_SENTINEL_STR, |v: &str| v
                .as_bytes()
                .to_vec()),
            DataType::Utf8View => fold!(StringViewArray, "Utf8View", NULL_SENTINEL_STR, |v: &str| v
                .as_bytes()
                .to_vec()),
            DataType::LargeUtf8 => fold!(
                LargeStringArray,
                "LargeUtf8",
                NULL_SENTINEL_STR,
                |v: &str| v.as_bytes().to_vec()
            ),
            other => Err(ShuffleError::TypeMismatch {
                expected: format!(
                    "supported partition key type (Int32, Int64, Utf8, Utf8View, LargeUtf8), got {other}"
                ),
            }),
        }
    }
}

/// Materialise one `RecordBatch` per bucket from per-bucket row indices.
///
/// Empty buckets become zero-row batches with the input schema, so the result
/// always has exactly `bucket_indices.len()` entries and downstream index
/// arithmetic stays valid.
fn gather_buckets(
    batch: &RecordBatch,
    bucket_indices: &[Vec<u32>],
) -> ShuffleResult<Vec<RecordBatch>> {
    let schema = batch.schema();
    let mut result = Vec::with_capacity(bucket_indices.len());
    for indices in bucket_indices {
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

/// Hash an i64 value into a shuffle bucket using XxHash64 with the given seed.
///
/// The seed should be derived from the job ID so different jobs produce
/// independent distributions. Pass `0` for tests or when no job context exists.
/// # Panics
///
/// Panics if `buckets` is zero. This was a `debug_assert!`, which is compiled
/// out of release builds — the exact builds that run shuffles — leaving `% 0`
/// to panic anyway, but as an unattributed "attempt to calculate the remainder
/// with a divisor of zero" from inside a hash function. A guard that only
/// guards in debug is not a guard; name the contract in both profiles. The
/// branch is perfectly predicted and disappears next to hashing eight bytes.
pub fn hash_i64(value: i64, buckets: u32, seed: u64) -> ShuffleBucket {
    assert!(buckets > 0, "hash_i64 requires at least one bucket");
    ShuffleBucket(
        (twox_hash::XxHash64::oneshot(seed, &value.to_le_bytes()) % buckets as u64) as u32,
    )
}

/// Hash a string value into a shuffle bucket using XxHash64 with the given seed.
///
/// The seed should be derived from the job ID so different jobs produce
/// independent distributions. Pass `0` for tests or when no job context exists.
/// # Panics
///
/// Panics if `buckets` is zero — see [`hash_i64`] for why this is a real
/// `assert!` rather than a `debug_assert!`.
pub fn hash_str(value: &str, buckets: u32, seed: u64) -> ShuffleBucket {
    assert!(buckets > 0, "hash_str requires at least one bucket");
    ShuffleBucket((twox_hash::XxHash64::oneshot(seed, value.as_bytes()) % buckets as u64) as u32)
}

/// Route each row of `0..num_rows` into `bucket_indices` by `bucket_fn`.
///
/// A bucket index outside `bucket_indices` is an error, not a skipped row.
/// This used to be `if let Some(v) = bucket_indices.get_mut(bucket)`, which
/// **silently dropped** any row whose bucket fell out of range — shuffle rows
/// vanishing with no error anywhere, which surfaces as a wrong query answer
/// rather than a failure. The invariant that makes it unreachable (every
/// `bucket_fn` here returns `hash % self.buckets`, and `bucket_indices` is
/// built with exactly `self.buckets` entries) was unstated and unenforced, so
/// only a reviewer holding both halves in mind could see it held. Losing rows
/// is never the right response to a broken invariant; say so instead.
///
/// The old `num_partitions` argument is gone: it was only ever used to size a
/// capacity hint, while the real bucket count came from `bucket_indices.len()`.
/// Two sources for one number is how they drift apart.
fn fill_buckets<F>(
    bucket_indices: &mut [Vec<u32>],
    num_rows: usize,
    bucket_fn: F,
) -> ShuffleResult<()>
where
    F: Fn(usize) -> ShuffleBucket,
{
    let num_partitions = bucket_indices.len();
    let hint = num_rows / num_partitions.max(1);
    for v in bucket_indices.iter_mut() {
        v.reserve(hint);
    }
    for row in 0..num_rows {
        let bucket = bucket_fn(row).0 as usize;
        let slot = bucket_indices.get_mut(bucket).ok_or_else(|| {
            crate::error::io_err(format!(
                "partitioner routed row {row} to bucket {bucket}, but only \
                 {num_partitions} buckets exist; refusing to drop the row"
            ))
        })?;
        slot.push(row as u32);
    }
    Ok(())
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

    /// A two-column batch whose LEADING key has only two distinct values and
    /// whose trailing key is unique per row.
    fn low_then_high_cardinality(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| i % 2))),
                Arc::new(Int64Array::from_iter_values(0..rows)),
            ],
        )
        .unwrap()
    }

    /// The bottleneck this fixes: hashing only the leading key column collapses
    /// a composite shuffle key onto that column's cardinality.
    ///
    /// With `(status, id)` where `status` has two values, the old single-column
    /// behaviour could occupy at most two of eight buckets, leaving six reduce
    /// tasks with nothing. Hashing both spreads the rows.
    #[test]
    fn a_composite_key_uses_every_bucket_not_just_the_leading_columns() {
        let batch = low_then_high_cardinality(4096);

        let leading_only = HashPartitioner::new("status", 8);
        let occupied = |parts: &[RecordBatch]| parts.iter().filter(|b| b.num_rows() > 0).count();
        let leading_buckets = occupied(&leading_only.partition(&batch).unwrap());
        assert!(
            leading_buckets <= 2,
            "a two-valued key cannot reach more than two buckets; got {leading_buckets}"
        );

        let composite =
            HashPartitioner::new_multi(vec!["status".into(), "id".into()], 8);
        let parts = composite.partition(&batch).unwrap();
        assert_eq!(parts.len(), 8);
        assert_eq!(
            parts.iter().map(|b| b.num_rows()).sum::<usize>(),
            4096,
            "no rows may be dropped"
        );
        assert_eq!(
            occupied(&parts),
            8,
            "hashing both key columns must reach every bucket, not {leading_buckets}"
        );
    }

    /// Co-location is the whole point of a shuffle: equal keys must land in the
    /// same bucket regardless of which batch they arrived in, or a join silently
    /// loses matches.
    #[test]
    fn equal_composite_keys_always_land_in_the_same_bucket() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let make = |a: Vec<i64>, b: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
            )
            .unwrap()
        };
        let partitioner = HashPartitioner::new_multi(vec!["a".into(), "b".into()], 7);
        // The same logical key (5, 9) in two differently-shaped batches.
        let left = partitioner.partition(&make(vec![5], vec![9])).unwrap();
        let right = partitioner
            .partition(&make(vec![1, 5, 3], vec![2, 9, 4]))
            .unwrap();
        let bucket_of = |parts: &[RecordBatch], want: (i64, i64)| -> Option<usize> {
            parts.iter().position(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let bb = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..b.num_rows()).any(|r| (a.value(r), bb.value(r)) == want)
            })
        };
        assert_eq!(
            bucket_of(&left, (5, 9)),
            bucket_of(&right, (5, 9)),
            "equal composite keys must co-locate across batches"
        );
    }

    /// Column order is part of the contract, so `(a, b)` and `(b, a)` are
    /// different partitionings. This pins the mix as non-commutative — if it
    /// ever became commutative, two sides of a join declared in different
    /// orders would appear to agree while hashing different things.
    #[test]
    fn key_column_order_changes_the_partitioning() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..256)),
                Arc::new(Int64Array::from_iter_values((0..256).map(|i| i * 7 + 1))),
            ],
        )
        .unwrap();
        let ab = HashPartitioner::new_multi(vec!["a".into(), "b".into()], 8)
            .partition(&batch)
            .unwrap();
        let ba = HashPartitioner::new_multi(vec!["b".into(), "a".into()], 8)
            .partition(&batch)
            .unwrap();
        let shape = |parts: &[RecordBatch]| parts.iter().map(|b| b.num_rows()).collect::<Vec<_>>();
        assert_ne!(
            shape(&ab),
            shape(&ba),
            "the key mix must not be commutative, or column order would be \
             silently meaningless"
        );
    }

    /// A one-column composite must route exactly like the single-column fast
    /// path. The two are separate branches for performance, and a divergence
    /// would mean the same logical key hashed differently depending on which
    /// branch ran — the co-location bug, introduced by an optimisation.
    #[test]
    fn a_single_element_composite_matches_the_fast_path_exactly() {
        let batch = low_then_high_cardinality(512);
        let fast = HashPartitioner::new("id", 8).partition(&batch).unwrap();
        let composite = HashPartitioner::new_multi(vec!["id".into()], 8)
            .partition(&batch)
            .unwrap();
        let shape = |parts: &[RecordBatch]| parts.iter().map(|b| b.num_rows()).collect::<Vec<_>>();
        assert_eq!(
            shape(&fast),
            shape(&composite),
            "the composite path must agree with the single-column path on one column"
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
