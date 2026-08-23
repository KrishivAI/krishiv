#![forbid(unsafe_code)]

//! Nonlinear incremental DISTINCT operator.
//!
//! DISTINCT is nonlinear because it cannot be derived by applying the function
//! to the delta alone — it requires tracking the accumulated weight of each
//! row across all ticks to detect when a row crosses the zero/positive threshold.
//!
//! Protocol per (row, weight) in delta:
//!   old_count = count[row]          (default 0)
//!   new_count = old_count + weight
//!   count[row] = new_count
//!   if old_count <= 0 && new_count > 0: emit (row, +1)  — row becomes present
//!   if old_count >  0 && new_count <= 0: emit (row, -1) — row disappears

use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};
use crate::operators::key_util::scalar_to_group_key;

/// Incremental DISTINCT operator with per-row threshold tracking.
pub struct IncrementalDistinctOp {
    /// Accumulated weight per row key (all data columns joined as string).
    /// Only rows with non-zero weight are stored.
    counts: AHashMap<Vec<String>, i64>,
}

impl IncrementalDistinctOp {
    pub fn new() -> Self {
        Self {
            counts: AHashMap::new(),
        }
    }

    /// Evict distinct entries whose event time is below `watermark`.
    ///
    /// Note: the current data model does not carry a per-row event time on
    /// `IncrementalDistinctOp::counts` (the key is a string projection of the
    /// row content, not a typed timestamp). Until that schema is added, the
    /// operator is a no-op here. The interface exists so the `ViewPlan::Distinct`
    /// arm of `gc_watermark` is reached; the eviction is wired to no-op
    /// pending schema work. A long-running incremental DISTINCT over a
    /// high-cardinality source should use `DISTINCT (event_time_col)` in the
    /// view body so the SQL engine can prune older partitions.
    /// Watermark GC is **not applicable** here, for the same reason as the
    /// aggregate operator: DISTINCT state is a per-row-key multiplicity map
    /// with no retained event time, so no watermark can prove an entry is
    /// evictable (IVM-AUD-CORE-6). Returning 0 is the honest answer.
    pub fn gc_watermark(&mut self, _watermark: i64) -> crate::DeltaResult<usize> {
        Ok(0)
    }

    /// Apply one tick of DISTINCT to the incoming delta.
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        if delta.is_empty() {
            let schema = delta.data_schema().clone();
            return DeltaBatch::empty(schema);
        }

        let data_schema = delta.data_schema().clone();
        let data = delta.data_batch();
        let weights = delta.weights();

        // Column indices for row key (all data columns)
        let n_cols = data.num_columns();

        let mut out_rows: Vec<Vec<String>> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for row in 0..data.num_rows() {
            // Null-unambiguous keys (crate-13 audit): a SQL null and the
            // string "NULL" must count as distinct rows.
            let row_key: Vec<String> = (0..n_cols)
                .map(|ci| scalar_to_group_key(data.column(ci), row))
                .collect::<DeltaResult<Vec<_>>>()?;

            let old_count = *self.counts.get(&row_key).unwrap_or(&0);
            let w = weights.value(row);
            let new_count = old_count + w;

            if new_count == 0 {
                self.counts.remove(&row_key);
            } else {
                self.counts.insert(row_key.clone(), new_count);
            }

            if old_count <= 0 && new_count > 0 {
                out_rows.push(row_key);
                out_weights.push(1);
            } else if old_count > 0 && new_count <= 0 {
                out_rows.push(row_key);
                out_weights.push(-1);
            }
            // else: row presence unchanged, no output
        }

        if out_rows.is_empty() {
            return DeltaBatch::empty(data_schema);
        }

        // Reconstruct output RecordBatch from string keys.
        // This is a simplified version that uses StringArray for all columns.
        // A full production implementation would use the original typed columns.
        build_output(out_rows, out_weights, &data, data_schema)
    }

    pub fn count_for_key(&self, key: &[String]) -> i64 {
        *self.counts.get(key).unwrap_or(&0)
    }

    /// Serialize the per-row multiplicity map (the operator's full state) so a
    /// DISTINCT view can be restored losslessly across a coordinator restart —
    /// the materialized snapshot only records *presence*, not the accumulated
    /// weight needed to know when a row crosses the zero threshold (G6/F4).
    ///
    /// Format (little-endian): `u32 n_rows || (u32 n_cols || (u32 len || utf8)*
    /// || i64 count)*`.
    pub fn state_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        // v2 magic (crate-13 audit): the key encoding changed to the
        // null-unambiguous form; pre-v2 blobs must fail restore (→ the caller
        // falls back to seed-from-snapshots) instead of silently installing
        // keys the new encoding can never match.
        out.extend_from_slice(b"DST2");
        out.extend_from_slice(&(self.counts.len() as u32).to_le_bytes());
        for (key, count) in &self.counts {
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            for col in key {
                out.extend_from_slice(&(col.len() as u32).to_le_bytes());
                out.extend_from_slice(col.as_bytes());
            }
            out.extend_from_slice(&count.to_le_bytes());
        }
        out
    }

    /// Replace the multiplicity map with one produced by [`state_bytes`].
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<()> {
        let err = || DeltaError::Operator("distinct state truncated".into());
        let bytes = bytes.strip_prefix(b"DST2".as_slice()).ok_or_else(|| {
            DeltaError::Operator(
                "distinct state blob is not format v2 (null-unambiguous keys); \
                 restore falls back to seed-from-snapshots"
                    .into(),
            )
        })?;
        let mut pos = 0usize;
        let rd_u32 = |pos: &mut usize| -> DeltaResult<u32> {
            let raw = bytes.get(*pos..*pos + 4).ok_or_else(err)?;
            *pos += 4;
            Ok(u32::from_le_bytes(raw.try_into().unwrap_or([0; 4])))
        };
        let n_rows = rd_u32(&mut pos)? as usize;
        let mut counts: AHashMap<Vec<String>, i64> = AHashMap::with_capacity(n_rows);
        for _ in 0..n_rows {
            let n_cols = rd_u32(&mut pos)? as usize;
            let mut key: Vec<String> = Vec::with_capacity(n_cols);
            for _ in 0..n_cols {
                let len = rd_u32(&mut pos)? as usize;
                let raw = bytes.get(pos..pos + len).ok_or_else(err)?;
                key.push(
                    std::str::from_utf8(raw)
                        .map_err(|e| DeltaError::Operator(e.to_string()))?
                        .to_string(),
                );
                pos += len;
            }
            let raw = bytes.get(pos..pos + 8).ok_or_else(err)?;
            pos += 8;
            counts.insert(key, i64::from_le_bytes(raw.try_into().unwrap_or([0; 8])));
        }
        self.counts = counts;
        Ok(())
    }
}

impl Default for IncrementalDistinctOp {
    fn default() -> Self {
        Self::new()
    }
}

fn build_output(
    row_keys: Vec<Vec<String>>,
    weights: Vec<i64>,
    original_data: &RecordBatch,
    data_schema: arrow::datatypes::SchemaRef,
) -> DeltaResult<DeltaBatch> {
    // Use row indices from original batch matching our keys for typed output.
    // This is O(n²) for simplicity; production would use a HashMap from key → row_idx.
    let n_cols = original_data.num_columns();
    let n_out = row_keys.len();

    let mut row_indices: Vec<usize> = Vec::with_capacity(n_out);
    'outer: for key in &row_keys {
        for orig_row in 0..original_data.num_rows() {
            let orig_key: Vec<String> = (0..n_cols)
                .map(|ci| scalar_to_group_key(original_data.column(ci), orig_row))
                .collect::<DeltaResult<Vec<_>>>()?;
            if &orig_key == key {
                row_indices.push(orig_row);
                continue 'outer;
            }
        }
        // Key not found in original batch (can happen for delayed retractions from
        // state that predates this tick's batch). Push a sentinel row index 0.
        row_indices.push(0);
    }

    let take_indices =
        arrow::array::UInt64Array::from(row_indices.iter().map(|&r| r as u64).collect::<Vec<_>>());

    let mut cols: Vec<Arc<dyn Array>> = original_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &take_indices, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<Vec<_>>>()?;

    cols.push(Arc::new(Int64Array::from(weights)));

    let mut full_fields: Vec<_> = data_schema.fields().iter().cloned().collect();
    full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let full_schema = Arc::new(Schema::new(full_fields));

    let inner = RecordBatch::try_new(full_schema, cols)?;
    DeltaBatch::from_weighted(inner)
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

    #[test]
    fn first_occurrence_emits_plus_one() {
        let mut op = IncrementalDistinctOp::new();
        let delta = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        let out = op.apply(delta).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.weights().value(0), 1);
    }

    #[test]
    fn second_occurrence_no_output() {
        let mut op = IncrementalDistinctOp::new();
        let d1 = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        op.apply(d1).unwrap();
        // Insert same row again (weight +2 total) → no new distinct emission
        let d2 = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        let out = op.apply(d2).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn last_deletion_emits_minus_one() {
        let mut op = IncrementalDistinctOp::new();
        let d1 = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        op.apply(d1).unwrap();
        let d2 = DeltaBatch::from_deletes(id_batch(&[1])).unwrap();
        let out = op.apply(d2).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.weights().value(0), -1);
    }

    #[test]
    fn multiset_deletion_does_not_emit_until_count_reaches_zero() {
        let mut op = IncrementalDistinctOp::new();
        // Insert row twice (multiset weight 2)
        let d1 = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        op.apply(d1).unwrap();
        let d2 = DeltaBatch::from_inserts(id_batch(&[1])).unwrap();
        op.apply(d2).unwrap();
        // Delete once (weight 1 → count still > 0, no retraction)
        let d3 = DeltaBatch::from_deletes(id_batch(&[1])).unwrap();
        let out = op.apply(d3).unwrap();
        assert!(out.is_empty(), "count still positive, no retraction");
        // Delete second time (count → 0, emit retraction)
        let d4 = DeltaBatch::from_deletes(id_batch(&[1])).unwrap();
        let out2 = op.apply(d4).unwrap();
        assert_eq!(out2.num_rows(), 1);
        assert_eq!(out2.weights().value(0), -1);
    }

    /// Regression (crate-13 audit, A-class): a SQL null row and the string
    /// "NULL" row are distinct — retracting the null must not consume the
    /// string row's count.
    #[test]
    fn null_and_null_string_are_distinct_rows() {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let string_null = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("NULL")]))],
        )
        .unwrap();
        let real_null = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        let mut op = IncrementalDistinctOp::new();
        op.apply(DeltaBatch::from_inserts(string_null).unwrap())
            .unwrap();
        // Retract the *actual null* row, which was never inserted: the string
        // "NULL" row's presence must be unaffected (its count stays 1); the
        // null row's count goes to -1 and emits its own retraction.
        let out = op
            .apply(DeltaBatch::from_deletes(real_null).unwrap())
            .unwrap();
        for i in 0..out.num_rows() {
            assert!(
                out.data_batch().column(0).is_null(i) || out.weights().value(i) >= 0,
                "the string \"NULL\" row must not be retracted by a SQL-null delete"
            );
        }
        assert_eq!(
            op.count_for_key(&["vNULL".to_string()]),
            1,
            "string \"NULL\" count must remain 1"
        );
    }

    /// Pre-v2 (magic-less) state blobs must fail restore so the caller reseeds
    /// instead of installing keys the new encoding can never match.
    #[test]
    fn restore_rejects_pre_v2_state_blob() {
        let mut op = IncrementalDistinctOp::new();
        // A well-formed *old-format* blob: 0 rows, no magic.
        assert!(op.restore_state_bytes(&0u32.to_le_bytes()).is_err());
        // v2 round-trip still works.
        op.apply(DeltaBatch::from_inserts(id_batch(&[1])).unwrap())
            .unwrap();
        let bytes = op.state_bytes();
        let mut fresh = IncrementalDistinctOp::new();
        fresh.restore_state_bytes(&bytes).unwrap();
        assert_eq!(fresh.count_for_key(&["v1".to_string()]), 1);
    }
}
