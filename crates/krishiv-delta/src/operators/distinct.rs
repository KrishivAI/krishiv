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
            let row_key: Vec<String> = (0..n_cols)
                .map(|ci| scalar_to_string(data.column(ci), row))
                .collect();

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
                .map(|ci| scalar_to_string(original_data.column(ci), orig_row))
                .collect();
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

fn scalar_to_string(arr: &dyn Array, row: usize) -> String {
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) {
            "NULL".into()
        } else {
            a.value(row).to_string()
        };
    }
    "NULL".to_string()
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
}
