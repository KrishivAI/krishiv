#![forbid(unsafe_code)]

//! Spine-style `Trace` — an append-only accumulation of `DeltaBatch`es.
//!
//! A `Trace` is the durable state an incremental operator maintains across
//! clock ticks. It enables efficient probing by key column(s): given a set of
//! keys in a delta batch, the Trace returns all matching rows with their
//! accumulated weights.
//!
//! Implementation: 8-level Spine. Batches are inserted at level 0. When a
//! level exceeds `MERGE_THRESHOLD` batches, all batches at that level are
//! concatenated + consolidated into one and promoted to level+1. This gives
//! O(log N) amortized merge cost.
//!
//! Each batch carries a lazily-built [`KeyIndex`] so a probe costs
//! O(probe keys · log(batch rows) + matches) rather than O(the entire trace).
//! Before IVM-AUD-PERF-6 the doc above claimed "O(L · hash) probe cost", which
//! was wrong in the way this register keeps finding: `probe_by_keys` extracted
//! a fresh `Vec<String>` key for EVERY row of EVERY batch on EVERY call. The
//! comment described the intended design; the code scanned the whole trace.

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Int64Array, RecordBatch,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::SchemaRef;

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};
use crate::operators::consolidate::consolidate_batch;

/// Number of levels in the Spine.
const NUM_LEVELS: usize = 8;

/// When a level reaches this many batches, they are merged and promoted.
const MERGE_THRESHOLD: usize = 4;

/// Counts per-row key extractions performed inside `probe_by_keys` (NOT those
/// performed while building an index). The asymptotic claim of
/// IVM-AUD-PERF-6 — a probe costs O(matches), not O(the trace) — is otherwise
/// untestable from a unit test: a timing assertion would be flaky and a
/// correctness assertion passes just as well against the O(state) scan it
/// replaced.
#[cfg(test)]
static PROBE_KEY_EXTRACTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Fixed seeds so index-build and probe hash a key identically. `ahash`'s
/// default `RandomState` is seeded per instance, which would make the two
/// disagree and silently return zero matches.
const KEY_HASH_SEEDS: (u64, u64, u64, u64) = (
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
);

fn hash_key(key: &KeyTuple) -> u64 {
    let state = ahash::RandomState::with_seeds(
        KEY_HASH_SEEDS.0,
        KEY_HASH_SEEDS.1,
        KEY_HASH_SEEDS.2,
        KEY_HASH_SEEDS.3,
    );
    state.hash_one(key)
}

/// One batch's rows ordered by key hash, so a probe can jump to the candidate
/// rows for a key instead of walking the batch.
///
/// Two parallel vectors rather than an `AHashMap<KeyTuple, Vec<u32>>`: this is
/// 12 bytes per row flat with no per-distinct-key heap allocation. Memory is
/// the other half of this crate's scale problem (task #166 measures ~1.2 KB
/// resident per seeded row), and a map keyed by the owned key tuple would add
/// ~60 bytes and two allocations per distinct key — trading a time defect for
/// a memory one.
struct KeyIndex {
    /// Key hashes in ascending order. Parallel to `rows`.
    hashes: Vec<u64>,
    /// Row index within the batch for the hash at the same position.
    rows: Vec<u32>,
}

impl KeyIndex {
    fn build(data: &RecordBatch, key_indices: &[usize]) -> DeltaResult<Self> {
        let n = data.num_rows();
        let mut pairs: Vec<(u64, u32)> = Vec::with_capacity(n);
        for row in 0..n {
            let key = extract_key(data, key_indices, row)?;
            // A batch above u32::MAX rows cannot be addressed by this index.
            // Arrow batches are far smaller, but saturating here would alias
            // row 0, so refuse instead of indexing the wrong row.
            let as_u32 = u32::try_from(row)
                .map_err(|_| DeltaError::Operator("batch exceeds u32 rows".into()))?;
            pairs.push((hash_key(&key), as_u32));
        }
        pairs.sort_unstable();
        let mut hashes = Vec::with_capacity(n);
        let mut rows = Vec::with_capacity(n);
        for (h, r) in pairs {
            hashes.push(h);
            rows.push(r);
        }
        Ok(Self { hashes, rows })
    }

    /// Rows whose key hash equals `h`. These are CANDIDATES: a hash collision
    /// puts a non-matching row in this slice, so every caller must still
    /// compare the actual key.
    fn candidates(&self, h: u64) -> &[u32] {
        let lo = self.hashes.partition_point(|x| *x < h);
        let hi = self.hashes.partition_point(|x| *x <= h);
        self.rows.get(lo..hi).unwrap_or(&[])
    }
}

/// A trace batch plus its key index. The index is built on first probe, so a
/// trace that is only ever snapshotted or checkpointed never pays for one.
struct IndexedBatch {
    batch: DeltaBatch,
    index: std::sync::OnceLock<KeyIndex>,
}

impl IndexedBatch {
    fn new(batch: DeltaBatch) -> Self {
        Self {
            batch,
            index: std::sync::OnceLock::new(),
        }
    }

    fn index(&self, key_indices: &[usize]) -> DeltaResult<&KeyIndex> {
        if let Some(existing) = self.index.get() {
            return Ok(existing);
        }
        let built = KeyIndex::build(&self.batch.data_batch(), key_indices)?;
        // A racing thread may have won the set; both built the same index from
        // the same immutable batch, so either is correct.
        let _ = self.index.set(built);
        self.index
            .get()
            .ok_or_else(|| DeltaError::Operator("key index initialisation".into()))
    }
}

/// Accumulated state for one incremental operator.
///
/// All rows across all levels together form the Z-set representing the
/// operator's current accumulated view of the data.
pub struct Trace {
    /// key_columns[i] = column index in the data schema for join/group keys.
    key_col_indices: Vec<usize>,
    /// key_col_names[i] = column name for the join key.
    key_col_names: Vec<String>,
    /// Data schema (without `_weight`).
    data_schema: SchemaRef,
    /// Levels[0] holds recent small batches; levels[7] holds large merged batches.
    levels: [Vec<IndexedBatch>; NUM_LEVELS],
    /// Total rows across all levels (approximate; includes zero-weight rows until GC).
    total_rows: usize,
    /// Optional lateness column index for GC.
    lateness_col_idx: Option<usize>,
}

impl Trace {
    /// Create an empty Trace for a given data schema and set of key column names.
    pub fn new(data_schema: SchemaRef, key_columns: &[&str]) -> DeltaResult<Self> {
        let key_col_indices = key_columns
            .iter()
            .map(|name| {
                data_schema
                    .index_of(name)
                    .map_err(|_| DeltaError::ColumnNotFound((*name).to_string()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let key_col_names = key_columns.iter().map(|s| s.to_string()).collect();
        Ok(Self {
            key_col_indices,
            key_col_names,
            data_schema,
            levels: Default::default(),
            total_rows: 0,
            lateness_col_idx: None,
        })
    }

    pub fn with_lateness_column(mut self, col_name: &str) -> DeltaResult<Self> {
        let idx = self
            .data_schema
            .index_of(col_name)
            .map_err(|_| DeltaError::ColumnNotFound(col_name.to_string()))?;
        self.lateness_col_idx = Some(idx);
        Ok(self)
    }

    pub fn data_schema(&self) -> &SchemaRef {
        &self.data_schema
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn key_column_names(&self) -> &[String] {
        &self.key_col_names
    }

    // ── Insert ───────────────────────────────────────────────────────────────

    /// Append a new `DeltaBatch` to the Trace.
    /// Triggers background merge if the level overflows `MERGE_THRESHOLD`.
    pub fn insert(&mut self, batch: DeltaBatch) {
        if batch.is_empty() {
            return;
        }
        self.total_rows += batch.num_rows();
        if let Some(l) = self.levels.get_mut(0) {
            l.push(IndexedBatch::new(batch));
        }
        self.cascade_merge(0);
    }

    fn cascade_merge(&mut self, level: usize) {
        // Bounds guard: level must be a valid index.
        let lvl_len = match self.levels.get(level) {
            Some(l) => l.len(),
            None => return,
        };
        if lvl_len < MERGE_THRESHOLD {
            return;
        }
        // Take the current level's batches (leaves an empty Vec in place).
        let batches = match self.levels.get_mut(level) {
            Some(l) => std::mem::take(l),
            None => return,
        };
        // Row count before consolidation — used to keep `total_rows` honest
        // (AUD-10: consolidation drops cancelled/zero-weight rows, so the metric
        // must shrink by however many rows the merge removed).
        let before_rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();
        let deltas: Vec<DeltaBatch> = batches.iter().map(|b| b.batch.clone()).collect();
        // If we're at the top level, consolidate in place instead of discarding.
        // Without this, the top level grows without bound and probe latency
        // degrades linearly with total history.
        if level + 1 >= NUM_LEVELS {
            if let Ok(merged) = DeltaBatch::concat(&deltas) {
                if let Ok(consolidated) = consolidate_batch(merged, &[], &self.data_schema) {
                    self.total_rows = self
                        .total_rows
                        .saturating_sub(before_rows.saturating_sub(consolidated.num_rows()));
                    if let Some(l) = self.levels.get_mut(level) {
                        l.push(IndexedBatch::new(consolidated));
                    }
                } else if let Some(l) = self.levels.get_mut(level) {
                    *l = batches;
                }
            } else if let Some(l) = self.levels.get_mut(level) {
                *l = batches;
            }
            return;
        }
        if let Ok(merged) = DeltaBatch::concat(&deltas)
            && let Ok(consolidated) = consolidate_batch(merged, &[], &self.data_schema)
            && let Some(next) = self.levels.get_mut(level + 1)
        {
            self.total_rows = self
                .total_rows
                .saturating_sub(before_rows.saturating_sub(consolidated.num_rows()));
            next.push(IndexedBatch::new(consolidated));
            self.cascade_merge(level + 1);
            return;
        }
        // On error, restore the batches to the current level so no data is lost.
        if let Some(l) = self.levels.get_mut(level) {
            *l = batches;
        }
    }

    // ── Probe ────────────────────────────────────────────────────────────────

    /// Given a `keys` RecordBatch (data schema, no `_weight`), return a
    /// `DeltaBatch` of all Trace rows that join with at least one key row,
    /// preserving their accumulated weights.
    ///
    /// The output schema is the Trace's data schema + `_weight`.
    /// If a Trace row has accumulated weight 0, it is excluded (dropped zeros).
    pub fn probe_by_keys(&self, keys: &RecordBatch) -> DeltaResult<DeltaBatch> {
        if keys.num_rows() == 0 {
            return DeltaBatch::empty(self.data_schema.clone());
        }

        // The `keys` batch has exactly N key columns in the same order as
        // `self.key_col_names` (it was projected to contain only key columns).
        // Use sequential indices [0..N] to extract tuples from the probe batch,
        // while using `self.key_col_indices` to index into the trace's own batches.
        let probe_indices: Vec<usize> = (0..self.key_col_names.len()).collect();
        let key_set = build_key_set(keys, &probe_indices)?;
        // Distinct probe hashes. `key_set` is already deduplicated by key, but
        // two distinct keys can collide, so the candidate lists are unioned and
        // every candidate is confirmed against `key_set` below.
        let mut probe_hashes: Vec<u64> = key_set.iter().map(hash_key).collect();
        probe_hashes.sort_unstable();
        probe_hashes.dedup();

        let mut result_batches = Vec::new();
        for level in &self.levels {
            for slot in level {
                let index = slot.index(&self.key_col_indices)?;
                let mut candidates: Vec<u32> = Vec::new();
                for &h in &probe_hashes {
                    candidates.extend_from_slice(index.candidates(h));
                }
                if candidates.is_empty() {
                    continue;
                }
                // Ascending + deduplicated so `take` reproduces the row order
                // `filter_record_batch` produced before this was indexed, and
                // so a hash collision between two probe keys cannot emit the
                // same trace row twice.
                candidates.sort_unstable();
                candidates.dedup();
                // `data_batch` is a validating `RecordBatch::try_new`, and the
                // pre-fix probe built one for EVERY batch of EVERY level on
                // EVERY call, then allocated a full-length mask and ran the
                // filter kernel over it — work proportional to the trace and
                // done whether or not anything matched. Measured on TPC-H q21
                // at seed 800k that fixed per-call overhead, times 390k calls,
                // was ~80% of a 9.5 s probe bill. A batch with no candidate
                // now costs two binary searches and nothing else.
                let data = slot.batch.data_batch();
                let mut matched: Vec<u32> = Vec::with_capacity(candidates.len());
                #[cfg(test)]
                PROBE_KEY_EXTRACTS
                    .fetch_add(candidates.len(), std::sync::atomic::Ordering::Relaxed);
                for &row in &candidates {
                    let key = extract_key(&data, &self.key_col_indices, row as usize)?;
                    if key_set.contains(&key) {
                        matched.push(row);
                    }
                }
                if matched.is_empty() {
                    continue;
                }
                let take_idx = arrow::array::UInt32Array::from(matched);
                let filtered = arrow::compute::take_record_batch(slot.batch.inner(), &take_idx)?;
                if filtered.num_rows() > 0 {
                    result_batches.push(
                        DeltaBatch::from_weighted(filtered)
                            .map_err(|e| DeltaError::Operator(e.to_string()))?,
                    );
                }
            }
        }

        if result_batches.is_empty() {
            return DeltaBatch::empty(self.data_schema.clone());
        }
        let merged = DeltaBatch::concat(&result_batches)?;
        merged.drop_zeros()
    }

    // ── Force consolidation ──────────────────────────────────────────────────

    /// Force-consolidate all levels into a single batch. Useful before
    /// checkpointing or when join probe latency matters.
    pub fn consolidate(&mut self) -> DeltaResult<()> {
        let mut all: Vec<DeltaBatch> = Vec::new();
        for level in &mut self.levels {
            all.extend(std::mem::take(level).into_iter().map(|s| s.batch));
        }
        if all.is_empty() {
            return Ok(());
        }
        let merged = DeltaBatch::concat(&all)?;
        let consolidated = consolidate_batch(merged, &[], &self.data_schema)?;
        self.total_rows = consolidated.num_rows();
        self.levels[NUM_LEVELS - 1].push(IndexedBatch::new(consolidated));
        Ok(())
    }

    // ── Watermark GC ─────────────────────────────────────────────────────────

    /// Remove all Trace entries where the lateness column value < `watermark_ms`.
    /// No-op if no lateness column was configured.
    pub fn gc_below_watermark(&mut self, watermark_ms: i64) -> DeltaResult<usize> {
        let Some(ts_idx) = self.lateness_col_idx else {
            return Ok(0);
        };
        let mut removed = 0usize;
        for level in &mut self.levels {
            for slot in level.iter_mut() {
                let batch = &mut slot.batch;
                let data = batch.data_batch();
                if ts_idx >= data.num_columns() {
                    continue;
                }
                let ts_col = data.column(ts_idx);
                // IVM-4: try all common temporal/integer types for the lateness
                // column.  Previously only Int64 was handled, so a Timestamp
                // lateness column (the natural event-time type) hit `continue`
                // and skipped every batch, making GC a universal no-op.
                //
                // AUD-2: this is a *keep* mask. `filter_mask` retains rows whose
                // mask entry is `true`, so a row is kept iff its lateness value
                // (normalized to epoch-ms) is `>= watermark_ms` — i.e. still
                // live. Rows strictly below the watermark are expired and
                // dropped. A null lateness value is always kept (never silently
                // GC'd). The previous mask compared `< watermark_ms` and thus
                // deleted every live row while retaining expired state.
                let mask: BooleanArray = {
                    // Build a keep mask from a per-row extractor that returns the
                    // value normalized to epoch-ms (None for null → keep).
                    let keep_ge = |to_ms: &dyn Fn(usize) -> Option<i64>| -> BooleanArray {
                        (0..data.num_rows())
                            .map(|r| Some(to_ms(r).map(|ms| ms >= watermark_ms).unwrap_or(true)))
                            .collect()
                    };
                    if let Some(arr) = ts_col.as_any().downcast_ref::<Int64Array>() {
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r)))
                    } else if let Some(arr) =
                        ts_col.as_any().downcast_ref::<TimestampMillisecondArray>()
                    {
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r)))
                    } else if let Some(arr) =
                        ts_col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                    {
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r) / 1_000))
                    } else if let Some(arr) = ts_col.as_any().downcast_ref::<TimestampSecondArray>()
                    {
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r).saturating_mul(1_000)))
                    } else if let Some(arr) =
                        ts_col.as_any().downcast_ref::<TimestampNanosecondArray>()
                    {
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r) / 1_000_000))
                    } else if let Some(arr) = ts_col.as_any().downcast_ref::<Date32Array>() {
                        // Date32 = days since epoch.
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r) as i64 * 86_400_000))
                    } else if let Some(arr) = ts_col.as_any().downcast_ref::<Date64Array>() {
                        // Date64 = milliseconds since epoch already.
                        keep_ge(&|r| (!arr.is_null(r)).then(|| arr.value(r)))
                    } else {
                        continue;
                    }
                };
                let before = batch.num_rows();
                *batch = batch.filter_mask(&mask)?;
                removed += before - batch.num_rows();
                // GC renumbers the batch's rows, so any index built against the
                // pre-GC row order now points at the wrong rows. Drop it and
                // let the next probe rebuild.
                slot.index = std::sync::OnceLock::new();
            }
        }
        self.total_rows = self.total_rows.saturating_sub(removed);
        Ok(removed)
    }

    // ── Checkpoint serialization ─────────────────────────────────────────────

    /// Serialize the Trace's accumulated Z-set losslessly.
    ///
    /// Format: `u32 n_batches || (u64 len || serialized DeltaBatch)*` over all
    /// levels, flattened — the level layout is an internal merge optimization,
    /// not state; the union of batches (with weights) is the state. Structural
    /// configuration (schema, key columns, lateness) is *not* serialized: the
    /// caller restores into a Trace rebuilt with the same constructor arguments.
    pub fn state_bytes(&self) -> DeltaResult<Vec<u8>> {
        let batches: Vec<&DeltaBatch> = self.levels.iter().flatten().map(|s| &s.batch).collect();
        let mut out = Vec::new();
        let n = u32::try_from(batches.len())
            .map_err(|_| DeltaError::Serialization("trace batch count overflows u32".into()))?;
        out.extend_from_slice(&n.to_le_bytes());
        for batch in batches {
            let bytes = crate::delta_batch::serialize_delta_batch(batch)?;
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        Ok(out)
    }

    /// Decode a [`state_bytes`](Self::state_bytes) payload into its batches
    /// without touching any Trace. Split from [`restore_state_bytes`] so a
    /// caller restoring *several* traces (the join operator) can decode
    /// everything first and mutate only when the whole checkpoint is valid.
    pub fn decode_state(bytes: &[u8]) -> DeltaResult<Vec<DeltaBatch>> {
        let truncated = || DeltaError::Serialization("trace state truncated".into());
        let mut pos = 0usize;
        let n = {
            let raw = bytes.get(pos..pos + 4).ok_or_else(truncated)?;
            pos += 4;
            u32::from_le_bytes(raw.try_into().map_err(|_| truncated())?) as usize
        };
        let mut restored: Vec<DeltaBatch> = Vec::with_capacity(n);
        for _ in 0..n {
            let raw = bytes.get(pos..pos + 8).ok_or_else(truncated)?;
            pos += 8;
            let len = u64::from_le_bytes(raw.try_into().map_err(|_| truncated())?) as usize;
            let payload = bytes.get(pos..pos + len).ok_or_else(truncated)?;
            pos += len;
            restored.push(crate::delta_batch::deserialize_delta_batch(payload)?);
        }
        Ok(restored)
    }

    /// Replace the Trace's accumulated Z-set with the given batches (from
    /// [`decode_state`](Self::decode_state)).
    pub fn replace_batches(&mut self, batches: Vec<DeltaBatch>) {
        self.levels = Default::default();
        self.total_rows = 0;
        for batch in batches {
            self.insert(batch);
        }
    }

    /// Replace the Trace's accumulated Z-set with one produced by
    /// [`state_bytes`](Self::state_bytes). Weights (row multiplicities) are
    /// preserved exactly — unlike seeding from a materialized snapshot, which
    /// collapses duplicates to weight 1. Mutates only after the whole payload
    /// decoded, so a truncated checkpoint cannot leave the trace half-replaced.
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<()> {
        let batches = Self::decode_state(bytes)?;
        self.replace_batches(batches);
        Ok(())
    }

    // ── Collect all rows ─────────────────────────────────────────────────────

    /// Collect all rows with positive net weight (the "current snapshot").
    pub fn snapshot(&self) -> DeltaResult<RecordBatch> {
        let mut all = Vec::new();
        for level in &self.levels {
            all.extend(level.iter().map(|s| s.batch.clone()));
        }
        if all.is_empty() {
            let empty = arrow::array::RecordBatch::new_empty(self.data_schema.clone());
            return Ok(empty);
        }
        let merged = DeltaBatch::concat(&all)?;
        let consolidated = consolidate_batch(merged, &[], &self.data_schema)?;
        // Multiset semantics: a weight-k row appears k times, so replaying a
        // trace snapshot as unit inserts reconstructs the multiplicities.
        consolidated.filter_positive_expanded()
    }
}

impl std::fmt::Debug for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Trace(keys={:?}, rows={})",
            self.key_col_names, self.total_rows
        )
    }
}

// ── Key matching helpers ───────────────────────────────────────────────────────

type KeyTuple = Vec<String>;

fn extract_key(batch: &RecordBatch, key_indices: &[usize], row: usize) -> DeltaResult<KeyTuple> {
    key_indices
        .iter()
        .map(|&idx| {
            let col = batch.column(idx);
            array_scalar_to_string(col, row)
        })
        .collect()
}

/// Crate-13 audit: this used to be a private stringifier with a `"NULL"`
/// sentinel (colliding with a real `"NULL"` string) and no coverage for
/// Utf8View / LargeUtf8 / temporal / binary types, which all collapsed into
/// one `<unsupported:…>` bucket and falsely matched each other in probes.
/// Now delegates to the shared null-unambiguous helper.
fn array_scalar_to_string(arr: &dyn Array, row: usize) -> DeltaResult<String> {
    crate::operators::key_util::scalar_to_group_key(arr, row)
}

fn build_key_set(
    keys: &RecordBatch,
    key_indices: &[usize],
) -> DeltaResult<ahash::AHashSet<KeyTuple>> {
    let mut set = ahash::AHashSet::new();
    for row in 0..keys.num_rows() {
        set.insert(extract_key(keys, key_indices, row)?);
    }
    Ok(set)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn id_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    fn id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    #[test]
    fn trace_insert_and_probe_matches() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        let cb = DeltaBatch::from_inserts(id_batch(&[1, 2, 3])).unwrap();
        trace.insert(cb);

        let keys = id_batch(&[2]);
        let result = trace.probe_by_keys(&keys).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.weights().value(0), 1);
    }

    #[test]
    fn trace_probe_no_match_returns_empty() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        let cb = DeltaBatch::from_inserts(id_batch(&[1, 2])).unwrap();
        trace.insert(cb);
        let keys = id_batch(&[99]);
        let result = trace.probe_by_keys(&keys).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn trace_insert_and_delete_cancels_in_snapshot() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(id_batch(&[5])).unwrap());
        trace.insert(DeltaBatch::from_deletes(id_batch(&[5])).unwrap());
        trace.consolidate().unwrap();
        let snap = trace.snapshot().unwrap();
        assert_eq!(snap.num_rows(), 0);
    }

    /// Regression (crate-13 audit, A-class): Utf8View key columns previously
    /// stringified to the shared `<unsupported:…>` bucket, so *every* row of
    /// that type matched every probe key.
    #[test]
    fn trace_probe_utf8view_keys_match_exactly() {
        use arrow::array::StringViewArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Utf8View,
            false,
        )]));
        let batch = |vals: &[&str]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringViewArray::from(vals.to_vec()))],
            )
            .unwrap()
        };
        let mut trace = Trace::new(schema.clone(), &["k"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(batch(&["a", "b"])).unwrap());
        let hit = trace.probe_by_keys(&batch(&["a"])).unwrap();
        assert_eq!(hit.num_rows(), 1, "probe must match exactly one row");
    }

    /// Regression (crate-13 audit, A-class): a SQL null key and the string
    /// "NULL" previously produced the same probe key and falsely matched.
    #[test]
    fn trace_probe_null_does_not_match_null_string() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, true)]));
        let mut trace = Trace::new(schema.clone(), &["k"]).unwrap();
        trace.insert(
            DeltaBatch::from_inserts(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![Some("NULL")]))],
                )
                .unwrap(),
            )
            .unwrap(),
        );
        let null_probe = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        let hit = trace.probe_by_keys(&null_probe).unwrap();
        assert!(
            hit.is_empty(),
            "SQL-null probe key must not match the string \"NULL\" row"
        );
    }

    /// IVM-AUD-PERF-6. The defect: `probe_by_keys` extracted a `Vec<String>`
    /// key for EVERY row of EVERY batch on EVERY call, so a point probe against
    /// a 50k-row trace touched 50k rows. TPC-H q21 issues thousands of point
    /// probes per tick (the semi/anti ΔB branch probes both traces once per
    /// distinct right-delta key), which is what made its tick grow with
    /// accumulated state while the delta was pinned.
    ///
    /// The first probe still pays a full pass — that is the index build — so
    /// this measures the SECOND probe, which is the steady state.
    #[test]
    fn a_probe_does_not_scan_the_whole_trace() {
        use std::sync::atomic::Ordering::Relaxed;
        const ROWS: i32 = 50_000;
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        let all: Vec<i32> = (0..ROWS).collect();
        trace.insert(DeltaBatch::from_inserts(id_batch(&all)).unwrap());

        // First probe builds the index; its cost is amortised over the batch's
        // life, so it is deliberately not the thing under test.
        let _ = trace.probe_by_keys(&id_batch(&[7])).unwrap();

        PROBE_KEY_EXTRACTS.store(0, Relaxed);
        let hit = trace.probe_by_keys(&id_batch(&[7])).unwrap();
        let touched = PROBE_KEY_EXTRACTS.load(Relaxed);

        assert_eq!(hit.num_rows(), 1, "the probe must still find its row");
        assert!(
            touched < 100,
            "a one-key probe against {ROWS} rows touched {touched} of them; \
             the index is not being used (pre-fix this was {ROWS})"
        );
    }

    /// The same claim across a Spine merge: `MERGE_THRESHOLD` inserts cascade
    /// into a consolidated batch, and the consolidated batch needs its own
    /// index or the probe silently reverts to scanning.
    #[test]
    fn the_index_survives_a_spine_merge() {
        use std::sync::atomic::Ordering::Relaxed;
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        for chunk in 0..(MERGE_THRESHOLD + 1) {
            let base = (chunk as i32) * 10_000;
            let rows: Vec<i32> = (base..base + 10_000).collect();
            trace.insert(DeltaBatch::from_inserts(id_batch(&rows)).unwrap());
        }
        let _ = trace.probe_by_keys(&id_batch(&[25_000])).unwrap();

        PROBE_KEY_EXTRACTS.store(0, Relaxed);
        let hit = trace.probe_by_keys(&id_batch(&[25_000])).unwrap();
        let touched = PROBE_KEY_EXTRACTS.load(Relaxed);

        assert_eq!(
            hit.num_rows(),
            1,
            "the merged batch must still be probeable"
        );
        assert!(
            touched < 100,
            "after a merge a one-key probe touched {touched} rows"
        );
    }

    /// GC renumbers a batch's rows. An index built before the GC maps key
    /// hashes to the PRE-GC row positions, so reusing it makes `take` return
    /// whatever row now sits at that offset — a silent wrong answer, not an
    /// error. This is the test that fails if the invalidation in
    /// `gc_below_watermark` is removed.
    #[test]
    fn gc_invalidates_the_index_so_probes_do_not_read_stale_rows() {
        use arrow::array::{Int32Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let rows = |ids: &[i32], ts: &[i64]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids.to_vec())),
                    Arc::new(Int64Array::from(ts.to_vec())),
                ],
            )
            .unwrap()
        };
        let mut trace = Trace::new(schema.clone(), &["id"])
            .unwrap()
            .with_lateness_column("ts")
            .unwrap();
        // ids 1..=4; only id=4 is at or above the watermark.
        trace.insert(DeltaBatch::from_inserts(rows(&[1, 2, 3, 4], &[10, 20, 30, 400])).unwrap());

        // Build the index against the pre-GC row order.
        let before = trace.probe_by_keys(&rows(&[1], &[0])).unwrap();
        assert_eq!(before.num_rows(), 1, "id=1 is present before GC");

        let removed = trace.gc_below_watermark(100).unwrap();
        assert_eq!(removed, 3, "ids 1..3 are below the watermark");

        // id=1 is gone. With a stale index its hash still resolves to row 0,
        // which after GC holds id=4 — so a stale index answers this probe with
        // the WRONG row rather than with nothing.
        let after = trace.probe_by_keys(&rows(&[1], &[0])).unwrap();
        assert!(
            after.is_empty(),
            "GC'd key must not match; got {} row(s)",
            after.num_rows()
        );
        let survivor = trace.probe_by_keys(&rows(&[4], &[0])).unwrap();
        assert_eq!(
            survivor.num_rows(),
            1,
            "the surviving row is still probeable"
        );
        assert_eq!(
            survivor
                .data_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            4,
            "the probe must return id=4 itself, not whatever row 0 used to be"
        );
    }

    /// #160: state round-trip preserves accumulated weights exactly (a row
    /// inserted twice restores with weight 2, not 1).
    #[test]
    fn trace_state_round_trip_preserves_weights() {
        let mut trace = Trace::new(id_schema(), &["id"]).unwrap();
        trace.insert(DeltaBatch::from_inserts(id_batch(&[7])).unwrap());
        trace.insert(DeltaBatch::from_inserts(id_batch(&[7, 8])).unwrap());

        let bytes = trace.state_bytes().unwrap();
        let mut restored = Trace::new(id_schema(), &["id"]).unwrap();
        restored.restore_state_bytes(&bytes).unwrap();

        let probe = restored.probe_by_keys(&id_batch(&[7, 8])).unwrap();
        let mut weights: Vec<(String, i64)> = (0..probe.num_rows())
            .map(|i| {
                let id = probe
                    .data_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(i);
                (id.to_string(), probe.weights().value(i))
            })
            .collect();
        weights.sort();
        // Row 7 may appear as one weight-2 row (consolidated) or two weight-1
        // rows; either way the accumulated weight must be 2.
        let total_7: i64 = weights
            .iter()
            .filter(|(id, _)| id == "7")
            .map(|(_, w)| w)
            .sum();
        let total_8: i64 = weights
            .iter()
            .filter(|(id, _)| id == "8")
            .map(|(_, w)| w)
            .sum();
        assert_eq!(total_7, 2, "duplicate multiplicity must survive restore");
        assert_eq!(total_8, 1);
        // Empty trace round-trips too.
        let empty = Trace::new(id_schema(), &["id"]).unwrap();
        let empty_bytes = empty.state_bytes().unwrap();
        let mut restored_empty = Trace::new(id_schema(), &["id"]).unwrap();
        restored_empty.insert(DeltaBatch::from_inserts(id_batch(&[1])).unwrap());
        restored_empty.restore_state_bytes(&empty_bytes).unwrap();
        assert_eq!(restored_empty.total_rows(), 0, "restore replaces state");
    }
}
