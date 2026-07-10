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
//! O(log N) amortized merge cost and O(L · hash) probe cost where L ≤ 8.

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
    levels: [Vec<DeltaBatch>; NUM_LEVELS],
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
            l.push(batch);
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
        let before_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // If we're at the top level, consolidate in place instead of discarding.
        // Without this, the top level grows without bound and probe latency
        // degrades linearly with total history.
        if level + 1 >= NUM_LEVELS {
            if let Ok(merged) = DeltaBatch::concat(&batches) {
                if let Ok(consolidated) = consolidate_batch(merged, &[], &self.data_schema) {
                    self.total_rows = self
                        .total_rows
                        .saturating_sub(before_rows.saturating_sub(consolidated.num_rows()));
                    if let Some(l) = self.levels.get_mut(level) {
                        l.push(consolidated);
                    }
                } else if let Some(l) = self.levels.get_mut(level) {
                    *l = batches;
                }
            } else if let Some(l) = self.levels.get_mut(level) {
                *l = batches;
            }
            return;
        }
        if let Ok(merged) = DeltaBatch::concat(&batches)
            && let Ok(consolidated) = consolidate_batch(merged, &[], &self.data_schema)
            && let Some(next) = self.levels.get_mut(level + 1)
        {
            self.total_rows = self
                .total_rows
                .saturating_sub(before_rows.saturating_sub(consolidated.num_rows()));
            next.push(consolidated);
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
        let key_set = build_key_set(keys, &probe_indices);

        let mut result_batches = Vec::new();
        for level in &self.levels {
            for batch in level {
                let data = batch.data_batch();
                let weights = batch.weights();
                let mask = make_key_match_mask(&data, &self.key_col_indices, &key_set);
                let filtered = arrow::compute::filter_record_batch(batch.inner(), &mask)?;
                if filtered.num_rows() > 0 {
                    result_batches.push(
                        DeltaBatch::from_weighted(filtered)
                            .map_err(|e| DeltaError::Operator(e.to_string()))?,
                    );
                }
                let _ = weights; // consumed implicitly by batch.inner()
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
            all.extend(std::mem::take(level));
        }
        if all.is_empty() {
            return Ok(());
        }
        let merged = DeltaBatch::concat(&all)?;
        let consolidated = consolidate_batch(merged, &[], &self.data_schema)?;
        self.total_rows = consolidated.num_rows();
        self.levels[NUM_LEVELS - 1].push(consolidated);
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
            for batch in level.iter_mut() {
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
        let batches: Vec<&DeltaBatch> = self.levels.iter().flatten().collect();
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
            all.extend(level.iter().cloned());
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

fn extract_key(batch: &RecordBatch, key_indices: &[usize], row: usize) -> KeyTuple {
    key_indices
        .iter()
        .map(|&idx| {
            let col = batch.column(idx);
            array_scalar_to_string(col, row)
        })
        .collect()
}

fn array_scalar_to_string(arr: &dyn Array, row: usize) -> String {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    macro_rules! try_downcast {
        ($t:ty) => {
            if let Some(a) = arr.as_any().downcast_ref::<$t>() {
                return if a.is_null(row) {
                    "NULL".to_string()
                } else {
                    a.value(row).to_string()
                };
            }
        };
    }
    try_downcast!(Int8Array);
    try_downcast!(Int16Array);
    try_downcast!(Int32Array);
    try_downcast!(Int64Array);
    try_downcast!(UInt8Array);
    try_downcast!(UInt16Array);
    try_downcast!(UInt32Array);
    try_downcast!(UInt64Array);
    try_downcast!(Float32Array);
    try_downcast!(Float64Array);
    try_downcast!(BooleanArray);
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) {
            "NULL".to_string()
        } else {
            a.value(row).to_string()
        };
    }
    format!("<unsupported:{}>", arr.data_type())
}

fn build_key_set(keys: &RecordBatch, key_indices: &[usize]) -> ahash::AHashSet<KeyTuple> {
    let mut set = ahash::AHashSet::new();
    for row in 0..keys.num_rows() {
        set.insert(extract_key(keys, key_indices, row));
    }
    set
}

fn make_key_match_mask(
    data: &RecordBatch,
    key_indices: &[usize],
    key_set: &ahash::AHashSet<KeyTuple>,
) -> BooleanArray {
    (0..data.num_rows())
        .map(|row| {
            let key = extract_key(data, key_indices, row);
            Some(key_set.contains(&key))
        })
        .collect()
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
