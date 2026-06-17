#![forbid(unsafe_code)]

//! Stateful incremental aggregate operators.
//!
//! Supports SUM, COUNT, AVG with correct retraction handling.
//! For each delta row (row, weight):
//!   1. Compute old aggregate for the row's group → emit retraction (-1)
//!   2. Apply delta to running state
//!   3. Compute new aggregate for the row's group → emit insertion (+1)
//!
//! MAX and MIN are handled via a BTreeMap multiset per group, which tracks the
//! full distribution of values (needed to correctly handle retractions).

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};

// ── Aggregation specification ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Aggregation {
    Sum { input_col: String, output_col: String },
    Count { output_col: String },
    Avg { input_col: String, output_col: String },
    Min { input_col: String, output_col: String },
    Max { input_col: String, output_col: String },
}

impl Aggregation {
    pub fn output_col(&self) -> &str {
        match self {
            Self::Sum { output_col, .. }
            | Self::Count { output_col }
            | Self::Avg { output_col, .. }
            | Self::Min { output_col, .. }
            | Self::Max { output_col, .. } => output_col,
        }
    }
}

// ── Running state per group ────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
struct GroupState {
    sum: f64,
    count: i64,
    /// For MIN/MAX: multiset of (value_str → cumulative weight).
    /// We use String keys for simplicity; a proper impl would use typed values.
    min_max_set: BTreeMap<String, i64>,
}

impl GroupState {
    fn apply_delta(&mut self, value_str: &str, numeric_val: f64, weight: i64) {
        self.sum += numeric_val * weight as f64;
        self.count += weight;
        let entry = self.min_max_set.entry(value_str.to_string()).or_insert(0);
        *entry += weight;
        if *entry == 0 {
            self.min_max_set.remove(value_str);
        }
    }

    fn current_sum(&self) -> f64 {
        self.sum
    }

    fn current_count(&self) -> i64 {
        self.count
    }

    fn current_avg(&self) -> Option<f64> {
        if self.count == 0 { None } else { Some(self.sum / self.count as f64) }
    }

    fn current_min(&self) -> Option<&str> {
        self.min_max_set.keys().next().map(String::as_str)
    }

    fn current_max(&self) -> Option<&str> {
        self.min_max_set.keys().next_back().map(String::as_str)
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// ── IncrementalAggOp ──────────────────────────────────────────────────────────

/// Stateful incremental aggregate operator.
pub struct IncrementalAggOp {
    group_by: Vec<String>,
    aggregations: Vec<Aggregation>,
    output_schema: SchemaRef,
    /// state[group_key] → running aggregate state per group
    state: AHashMap<Vec<String>, GroupState>,
}

impl IncrementalAggOp {
    pub fn new(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
    ) -> DeltaResult<Self> {
        // Validate group-by columns exist
        for col in &group_by {
            input_schema
                .field_with_name(col)
                .map_err(|_| DeltaError::ColumnNotFound(col.clone()))?;
        }

        // Build output schema: group-by columns + aggregate output columns
        let mut out_fields: Vec<_> = group_by
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        for agg in &aggregations {
            let output_type = match agg {
                Aggregation::Count { .. } => DataType::Int64,
                Aggregation::Sum { .. } | Aggregation::Avg { .. }
                | Aggregation::Min { .. } | Aggregation::Max { .. } => DataType::Float64,
            };
            out_fields.push(Arc::new(Field::new(agg.output_col(), output_type, true)));
        }

        let output_schema = Arc::new(Schema::new(out_fields));

        Ok(Self { group_by, aggregations, output_schema, state: AHashMap::new() })
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Apply one tick of incremental aggregation.
    ///
    /// For each row in `delta`:
    /// 1. Look up the group's current state.
    /// 2. Emit retraction of old aggregate output (if group was non-empty).
    /// 3. Apply delta weight to state.
    /// 4. Emit insertion of new aggregate output (if group is now non-empty).
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        if delta.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        let data = delta.data_batch();
        let weights = delta.weights();

        let group_col_indices: Vec<usize> = self
            .group_by
            .iter()
            .map(|name| {
                data.schema()
                    .index_of(name)
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Track which groups were touched, and their before/after states.
        let mut touched: AHashMap<Vec<String>, (Option<GroupState>, ())> = AHashMap::new();

        for row in 0..data.num_rows() {
            let group_key: Vec<String> = group_col_indices
                .iter()
                .map(|&idx| scalar_to_string(data.column(idx), row))
                .collect();

            // Record state before this row's delta
            if !touched.contains_key(&group_key) {
                let before = self.state.get(&group_key).cloned();
                touched.insert(group_key.clone(), (before, ()));
            }

            let w = weights.value(row);

            // Apply delta to each aggregation's state
            let group_state = self.state.entry(group_key.clone()).or_default();
            for agg in &self.aggregations {
                match agg {
                    Aggregation::Sum { input_col, .. }
                    | Aggregation::Avg { input_col, .. }
                    | Aggregation::Min { input_col, .. }
                    | Aggregation::Max { input_col, .. } => {
                        if let Ok(col_idx) = data.schema().index_of(input_col) {
                            let val_str = scalar_to_string(data.column(col_idx), row);
                            let numeric = val_str.parse::<f64>().unwrap_or(0.0);
                            group_state.apply_delta(&val_str, numeric, w);
                        }
                    }
                    Aggregation::Count { .. } => {
                        group_state.count += w;
                    }
                }
            }

            // GC empty groups
            if self.state.get(&group_key).map(GroupState::is_empty).unwrap_or(false) {
                self.state.remove(&group_key);
            }
        }

        // Build output: retraction of old agg + insertion of new agg for each touched group
        let mut out_group_rows: Vec<Vec<String>> = Vec::new(); // group key values
        let mut out_weights: Vec<i64> = Vec::new();
        let mut agg_values: Vec<Vec<Option<f64>>> = Vec::new(); // [row][agg_idx] → value

        for (group_key, (before_state, ())) in &touched {
            // Retraction: if there was a non-empty state before, emit -1
            if let Some(before) = before_state
                && !before.is_empty()
            {
                let vals = compute_agg_values(before, &self.aggregations);
                out_group_rows.push(group_key.clone());
                out_weights.push(-1);
                agg_values.push(vals);
            }
            // Insertion: if there is now a non-empty state, emit +1
            if let Some(after) = self.state.get(group_key)
                && !after.is_empty()
            {
                let vals = compute_agg_values(after, &self.aggregations);
                out_group_rows.push(group_key.clone());
                out_weights.push(1);
                agg_values.push(vals);
            }
        }

        if out_group_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        // Build output RecordBatch
        build_output_batch(
            &out_group_rows,
            &out_weights,
            &agg_values,
            &self.group_by,
            &self.aggregations,
            &self.output_schema,
        )
    }
}

fn compute_agg_values(state: &GroupState, aggregations: &[Aggregation]) -> Vec<Option<f64>> {
    aggregations
        .iter()
        .map(|agg| match agg {
            Aggregation::Sum { .. } => Some(state.current_sum()),
            Aggregation::Count { .. } => Some(state.current_count() as f64),
            Aggregation::Avg { .. } => state.current_avg(),
            Aggregation::Min { .. } => {
                state.current_min().and_then(|s| s.parse::<f64>().ok())
            }
            Aggregation::Max { .. } => {
                state.current_max().and_then(|s| s.parse::<f64>().ok())
            }
        })
        .collect()
}

fn build_output_batch(
    group_rows: &[Vec<String>],
    weights: &[i64],
    agg_values: &[Vec<Option<f64>>],
    group_by: &[String],
    aggregations: &[Aggregation],
    output_schema: &SchemaRef,
) -> DeltaResult<DeltaBatch> {
    let n_group = group_by.len();

    // Build group-by columns (all as StringArray for now; TODO: typed)
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    for gi in 0..n_group {
        let vals: Vec<Option<&str>> = group_rows.iter().map(|r| Some(r[gi].as_str())).collect();
        cols.push(Arc::new(StringArray::from(vals)) as Arc<dyn Array>);
    }

    // Build aggregate columns — Count is Int64, all others are Float64.
    for (ai, agg) in aggregations.iter().enumerate() {
        match agg {
            Aggregation::Count { .. } => {
                let vals: Int64Array =
                    agg_values.iter().map(|row| row[ai].map(|v| v as i64)).collect();
                cols.push(Arc::new(vals) as Arc<dyn Array>);
            }
            _ => {
                let vals: Float64Array = agg_values.iter().map(|row| row[ai]).collect();
                cols.push(Arc::new(vals) as Arc<dyn Array>);
            }
        }
    }

    // Weight column
    cols.push(Arc::new(Int64Array::from(weights.to_vec())));

    let mut full_fields: Vec<_> = output_schema.fields().iter().cloned().collect();
    full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let full_schema = Arc::new(Schema::new(full_fields));

    // Re-type group-by columns to match output_schema field types
    let inner = RecordBatch::try_new(full_schema, cols)?;
    DeltaBatch::from_weighted(inner)
}

fn scalar_to_string(arr: &dyn Array, row: usize) -> String {
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return if a.is_null(row) { "NULL".into() } else { a.value(row).to_string() };
    }
    "NULL".to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn order_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn order_batch(cids: &[&str], amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(StringArray::from(cids.to_vec())) as Arc<dyn Array>,
                Arc::new(Float64Array::from(amounts.to_vec())) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[test]
    fn sum_increases_on_insert() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        let delta = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(delta).unwrap();
        // Should have one insertion of sum=100
        assert!(!out.is_empty());
        let positive = out.filter_positive().unwrap();
        assert_eq!(positive.num_rows(), 1);
    }

    #[test]
    fn sum_retracts_on_delete() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        // First insert
        let d1 = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        op.apply(d1).unwrap();

        // Then delete → should emit retraction of sum=100 and insertion of sum=0 (empty group GC'd)
        let d2 = DeltaBatch::from_deletes(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(d2).unwrap();
        assert!(!out.is_empty());
        // Retraction should appear
        let retractions = out.filter_negative().unwrap();
        assert_eq!(retractions.num_rows(), 1);
    }

    #[test]
    fn count_increments_correctly() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Count { output_col: "cnt".into() }],
        )
        .unwrap();

        let d1 = DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 20.0])).unwrap();
        op.apply(d1).unwrap();
        // Count for c1 should be 2
        assert_eq!(op.state.get(&vec!["c1".to_string()]).map(|s| s.count), Some(2));
    }
}
