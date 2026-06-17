#![forbid(unsafe_code)]

//! Consolidation: sort by key columns, sum weights for matching rows, drop zeros.
//!
//! This is the core Z-set normalization step. After any sequence of inserts and
//! retractions, consolidation produces a canonical minimal representation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};

/// Consolidate a `DeltaBatch`: for each group of rows with identical key
/// column values, sum their weights. Rows with weight == 0 are dropped.
///
/// Key columns drive grouping; non-key columns take the value from the first
/// row in each group (they are assumed to be functionally dependent on the key
/// for meaningful consolidation; see note below).
///
/// Note: for full Z-set correctness, ALL column values must be identical for
/// two rows to be considered equal (not just key columns). Pass an empty
/// `key_columns` slice to group by all data columns.
pub fn consolidate_batch(
    batch: DeltaBatch,
    key_columns: &[String],
    _data_schema: &SchemaRef,
) -> DeltaResult<DeltaBatch> {
    if batch.is_empty() {
        return Ok(batch);
    }

    let data = batch.data_batch();
    let weights = batch.weights();

    // Build string keys for each row using all data columns (full equality).
    // This correctly handles the Z-set case where we group by *all* columns.
    let col_names: Vec<String> = if key_columns.is_empty() {
        data.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        key_columns.to_vec()
    };

    let col_indices: Vec<usize> = col_names
        .iter()
        .map(|name| {
            data.schema()
                .index_of(name)
                .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    // BTreeMap preserves insertion order and sorts keys lexicographically.
    // Key = string repr of key columns, value = (first row idx, accumulated weight).
    let mut groups: BTreeMap<Vec<String>, (usize, i64)> = BTreeMap::new();
    let mut key_order: Vec<Vec<String>> = Vec::new();

    for row in 0..data.num_rows() {
        let row_key: Vec<String> = col_indices
            .iter()
            .map(|&idx| scalar_to_string(data.column(idx), row))
            .collect();
        let w = weights.value(row);
        if let Some(entry) = groups.get_mut(&row_key) {
            entry.1 += w;
        } else {
            key_order.push(row_key.clone());
            groups.insert(row_key, (row, w));
        }
    }

    // Build output arrays: one row per group with non-zero weight.
    let output_rows: Vec<(usize, i64)> = key_order
        .iter()
        .filter_map(|k| {
            let (row_idx, w) = groups[k];
            if w != 0 { Some((row_idx, w)) } else { None }
        })
        .collect();

    if output_rows.is_empty() {
        return DeltaBatch::empty(Arc::new(data.schema().as_ref().clone()));
    }

    // Gather output columns.
    let row_indices: Vec<u64> = output_rows.iter().map(|(i, _)| *i as u64).collect();
    let output_weights: Vec<i64> = output_rows.iter().map(|(_, w)| *w).collect();

    let mut output_cols: Vec<Arc<dyn Array>> = data
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col, &arrow::array::UInt64Array::from(row_indices.clone()), None))
        .collect::<Result<Vec<_>, _>>()?;

    // Append consolidated weight column.
    output_cols.push(Arc::new(Int64Array::from(output_weights)));

    // Build full schema (data + _weight).
    let mut full_fields: Vec<_> = data.schema().fields().iter().cloned().collect();
    full_fields.push(Arc::new(arrow::datatypes::Field::new(
        WEIGHT_COLUMN,
        arrow::datatypes::DataType::Int64,
        false,
    )));
    let full_schema = Arc::new(arrow::datatypes::Schema::new(full_fields));

    let inner = RecordBatch::try_new(full_schema, output_cols)?;
    DeltaBatch::from_weighted(inner)
}

fn scalar_to_string(arr: &dyn Array, row: usize) -> String {
    use arrow::array::{
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    macro_rules! try_fmt {
        ($t:ty) => {
            if let Some(a) = arr.as_any().downcast_ref::<$t>() {
                return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
            }
        };
    }
    try_fmt!(Int8Array); try_fmt!(Int16Array); try_fmt!(Int32Array); try_fmt!(Int64Array);
    try_fmt!(UInt8Array); try_fmt!(UInt16Array); try_fmt!(UInt32Array); try_fmt!(UInt64Array);
    try_fmt!(Float32Array); try_fmt!(Float64Array);
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    format!("<{:?}>", arr.data_type())
}

/// `ConsolidateOp` applies `consolidate_batch` as a streaming operator.
pub struct ConsolidateOp {
    key_columns: Vec<String>,
}

impl ConsolidateOp {
    pub fn new(key_columns: Vec<String>) -> Self {
        Self { key_columns }
    }

    pub fn apply(&self, batch: DeltaBatch, data_schema: &SchemaRef) -> DeltaResult<DeltaBatch> {
        consolidate_batch(batch, &self.key_columns, data_schema)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch_from(ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    #[test]
    fn insert_and_delete_cancel() {
        let ins = DeltaBatch::from_inserts(batch_from(&[1])).unwrap();
        let del = DeltaBatch::from_deletes(batch_from(&[1])).unwrap();
        let merged = DeltaBatch::concat(&[ins, del]).unwrap();
        let result = consolidate_batch(merged, &["id".to_string()], &schema()).unwrap();
        assert!(result.is_empty(), "cancelling +1 and -1 should yield empty");
    }

    #[test]
    fn two_inserts_sum_to_two() {
        let a = DeltaBatch::from_inserts(batch_from(&[1])).unwrap();
        let b = DeltaBatch::from_inserts(batch_from(&[1])).unwrap();
        let merged = DeltaBatch::concat(&[a, b]).unwrap();
        let result = consolidate_batch(merged, &["id".to_string()], &schema()).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.weights().value(0), 2);
    }

    #[test]
    fn different_keys_stay_separate() {
        let a = DeltaBatch::from_inserts(batch_from(&[1, 2])).unwrap();
        let result = consolidate_batch(a, &["id".to_string()], &schema()).unwrap();
        assert_eq!(result.num_rows(), 2);
    }
}
