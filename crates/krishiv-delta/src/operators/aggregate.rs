#![forbid(unsafe_code)]

//! Stateful incremental aggregate operators.
//!
//! Supports SUM, COUNT, AVG with correct retraction handling.
//! For each delta row (row, weight):
//!   1. Compute old aggregate for the row's group → emit retraction (-1)
//!   2. Apply delta to running state
//!   3. Compute new aggregate for the row's group → emit insertion (+1)
//!
//! Each aggregation expression has its own state so a `[Count, Sum]` spec
//! does not double-count or cross-contaminate (Sum's `sum` and Count's
//! `count` are distinct fields).

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};
use crate::operators::key_util::{scalar_to_key as scalar_to_group_key, scalar_to_string};

// ── Aggregation specification ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Aggregation {
    Sum {
        input_col: String,
        output_col: String,
    },
    Count {
        output_col: String,
        /// When `Some`, only non-null values of this column are counted
        /// (SQL `COUNT(col)` excludes nulls).  When `None`, counts all rows
        /// (SQL `COUNT(*)`).
        input_col: Option<String>,
    },
    Avg {
        input_col: String,
        output_col: String,
    },
    Min {
        input_col: String,
        output_col: String,
    },
    Max {
        input_col: String,
        output_col: String,
    },
}

impl Aggregation {
    pub fn output_col(&self) -> &str {
        match self {
            Self::Sum { output_col, .. }
            | Self::Count { output_col, .. }
            | Self::Avg { output_col, .. }
            | Self::Min { output_col, .. }
            | Self::Max { output_col, .. } => output_col,
        }
    }

    fn input_col(&self) -> Option<&str> {
        match self {
            Self::Sum { input_col, .. }
            | Self::Avg { input_col, .. }
            | Self::Min { input_col, .. }
            | Self::Max { input_col, .. } => Some(input_col),
            Self::Count { input_col, .. } => input_col.as_deref(),
        }
    }
}

// ── Per-aggregation state ──────────────────────────────────────────────────────

/// Ordered f64 wrapper for MIN/MAX BTreeMap keys.
///
/// `f64` does not implement `Ord` (NaN). `total_cmp` is used so NaN sorts
/// consistently (after all finite values), keeping the BTreeMap invariants valid.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Default for OrdF64 {
    fn default() -> Self {
        Self(0.0)
    }
}

/// Separate running state for ONE aggregation expression.
/// A group's full state is `Vec<AggState>` indexed by position in `aggregations`.
///
/// `sum` is used by SUM. `avg_sum_i64` + `avg_count_i64` are used by AVG:
/// for integer-typed input columns they accumulate exactly in i64, emitting
/// the quotient as f64 only at output time. For float-typed inputs the caller
/// sets `avg_is_integer = false` and falls back to f64 accumulation in `sum`.
#[derive(Debug, Default, Clone)]
struct AggState {
    /// Weighted sum for SUM aggregations (f64 accumulation).
    sum: f64,
    /// Row count for COUNT / empty-group detection. Also used as the non-null
    /// input count for AVG when inputs are float (avg_is_integer == false).
    count: i64,
    /// Integer-precision weighted sum for AVG over integer-typed inputs.
    avg_sum_i64: i64,
    /// Non-null input count for AVG (separately tracked from `count` so
    /// COUNT and AVG can coexist in a multi-aggregation spec).
    avg_count_i64: i64,
    /// True when the AVG input is an integer column — use i64 accumulation.
    avg_is_integer: bool,
    /// For MIN/MAX: multiset of (value → cumulative weight).
    /// Uses OrdF64 keys so float columns (e.g. Float64) are ordered correctly.
    min_max_set: BTreeMap<OrdF64, i64>,
}

impl AggState {
    fn apply_delta_for_agg(&mut self, agg: &Aggregation, input_val_str: &str, weight: i64) {
        match agg {
            Aggregation::Sum { .. } => {
                // SQL: null inputs are excluded from SUM.
                if input_val_str == "NULL" {
                    return;
                }
                let numeric = input_val_str.parse::<f64>().unwrap_or(0.0);
                self.sum += numeric * weight as f64;
                self.count += weight;
            }
            Aggregation::Count { input_col, .. } => {
                // IVM-6: COUNT(col) excludes nulls; COUNT(*) counts all rows.
                // When `input_col` is `Some`, the caller has already converted
                // null values to the "NULL" sentinel via `scalar_to_string`.
                if input_col.is_some() && input_val_str == "NULL" {
                    return;
                }
                self.count += weight;
            }
            Aggregation::Avg { .. } => {
                // SQL: null inputs are excluded from AVG.
                if input_val_str == "NULL" {
                    return;
                }
                // Integer-typed input: accumulate exactly in i64 to avoid float
                // drift from many small increments. Detect by successful i64 parse.
                if let Ok(int_val) = input_val_str.parse::<i64>() {
                    self.avg_is_integer = true;
                    self.avg_sum_i64 = self.avg_sum_i64.saturating_add(int_val * weight);
                    self.avg_count_i64 += weight;
                } else {
                    // Float input: fall back to f64 accumulation.
                    let numeric = input_val_str.parse::<f64>().unwrap_or(0.0);
                    self.sum += numeric * weight as f64;
                    self.avg_count_i64 += weight;
                }
                self.count += weight;
            }
            Aggregation::Min { .. } => {
                // SQL: null inputs do not affect MIN.
                if input_val_str == "NULL" {
                    return;
                }
                let key = OrdF64(input_val_str.parse::<f64>().unwrap_or(0.0));
                let entry = self.min_max_set.entry(key).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    self.min_max_set.remove(&key);
                }
                self.count += weight;
            }
            Aggregation::Max { .. } => {
                // SQL: null inputs do not affect MAX.
                if input_val_str == "NULL" {
                    return;
                }
                let key = OrdF64(input_val_str.parse::<f64>().unwrap_or(0.0));
                let entry = self.min_max_set.entry(key).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    self.min_max_set.remove(&key);
                }
                self.count += weight;
            }
        }
    }

    fn current_value(&self, agg: &Aggregation) -> Option<f64> {
        match agg {
            Aggregation::Sum { .. } => Some(self.sum),
            Aggregation::Count { .. } => Some(self.count as f64),
            Aggregation::Avg { .. } => {
                if self.avg_count_i64 == 0 {
                    None
                } else if self.avg_is_integer {
                    Some(self.avg_sum_i64 as f64 / self.avg_count_i64 as f64)
                } else {
                    Some(self.sum / self.avg_count_i64 as f64)
                }
            }
            Aggregation::Min { .. } => self.min_max_set.keys().next().map(|k| k.0),
            Aggregation::Max { .. } => self.min_max_set.keys().next_back().map(|k| k.0),
        }
    }
}

/// `group_key → per-aggregation running state`.
/// Keys are `Vec<Option<String>>` where `None` represents a SQL null group member.
type GroupStateMap = AHashMap<Vec<Option<String>>, Vec<AggState>>;

/// Before/after snapshot map used within a single `apply` tick.
type TouchedMap = AHashMap<Vec<Option<String>>, (Option<Vec<AggState>>, ())>;

// ── IncrementalAggOp ──────────────────────────────────────────────────────────

/// Stateful incremental aggregate operator.
pub struct IncrementalAggOp {
    group_by: Vec<String>,
    aggregations: Vec<Aggregation>,
    output_schema: SchemaRef,
    /// state[group_key] → per-aggregation running state (one entry per aggregation)
    state: GroupStateMap,
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

        // Validate input columns for each agg
        for agg in &aggregations {
            if let Some(input_col) = agg.input_col() {
                input_schema
                    .field_with_name(input_col)
                    .map_err(|_| DeltaError::ColumnNotFound(input_col.to_string()))?;
            }
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
                Aggregation::Sum { .. }
                | Aggregation::Avg { .. }
                | Aggregation::Min { .. }
                | Aggregation::Max { .. } => DataType::Float64,
            };
            out_fields.push(Arc::new(Field::new(agg.output_col(), output_type, true)));
        }

        let output_schema = Arc::new(Schema::new(out_fields));

        Ok(Self {
            group_by,
            aggregations,
            output_schema,
            state: GroupStateMap::default(),
        })
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Evict aggregate groups whose event time is below `watermark`.
    ///
    /// Note: the current data model does not carry a per-group event time on
    /// `IncrementalAggOp::state` (groups are keyed by value, not by a typed
    /// timestamp). Until that schema is added, the operator is a no-op here.
    /// The interface exists so the `ViewPlan::Aggregate` arm of
    /// `gc_watermark` is reached; the eviction is wired to no-op pending
    /// schema work. A long-running incremental aggregate over an unbounded
    /// source should add a `TUMBLE/HOP/SESSION` window or filter on
    /// `event_time_col` in the view body so the SQL engine can prune older
    /// partitions.
    pub fn gc_watermark(&mut self, _watermark: i64) -> crate::DeltaResult<usize> {
        Ok(0)
    }

    /// Apply one tick of incremental aggregation.
    ///
    /// For each row in `delta`:
    /// 1. Look up the group's current state (per-aggregation).
    /// 2. Emit retraction of old aggregate output (if group was non-empty).
    /// 3. Apply delta weight to each aggregation's state independently.
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
        let mut touched: TouchedMap = AHashMap::new();

        for row in 0..data.num_rows() {
            let group_key: Vec<Option<String>> = group_col_indices
                .iter()
                .map(|&idx| scalar_to_group_key(data.column(idx), row))
                .collect();

            // Record state before this row's delta
            if !touched.contains_key(&group_key) {
                let before = self.state.get(&group_key).cloned();
                touched.insert(group_key.clone(), (before, ()));
            }

            let w = weights.value(row);

            // Apply delta to each aggregation's state independently.
            // Each aggregation has its own AggState, so [Count, Sum] does not
            // double-count and Sum + Min do not cross-contaminate.
            let group_state = self
                .state
                .entry(group_key.clone())
                .or_insert_with(|| vec![AggState::default(); self.aggregations.len()]);

            // Ensure the state vector matches the aggregation count
            // (handles the case where a new aggregation was added after state was created)
            if group_state.len() < self.aggregations.len() {
                group_state.resize(self.aggregations.len(), AggState::default());
            }

            for (state, agg) in group_state.iter_mut().zip(self.aggregations.iter()) {
                let input_val_str = match agg.input_col() {
                    Some(col) => {
                        if let Ok(idx) = data.schema().index_of(col) {
                            scalar_to_string(data.column(idx), row)
                        } else {
                            "NULL".to_string()
                        }
                    }
                    None => "".to_string(),
                };
                state.apply_delta_for_agg(agg, &input_val_str, w);
            }

            // GC empty groups: a group is empty when ALL its per-agg states are empty
            if let Some(states) = self.state.get(&group_key) {
                let all_empty = states.iter().all(|s| s.count == 0);
                if all_empty {
                    self.state.remove(&group_key);
                }
            }
        }

        // Build output: retraction of old agg + insertion of new agg for each touched group
        let mut out_group_rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();
        let mut agg_values: Vec<Vec<Option<f64>>> = Vec::new();

        for (group_key, (before_states, ())) in &touched {
            let has_before = before_states
                .as_ref()
                .map(|s| s.iter().any(|a| a.count != 0))
                .unwrap_or(false);
            let has_after = self
                .state
                .get(group_key)
                .map(|s| s.iter().any(|a| a.count != 0))
                .unwrap_or(false);

            if has_before && let Some(states) = before_states.as_ref() {
                let vals = compute_agg_values(states, &self.aggregations);
                out_group_rows.push(group_key.clone());
                out_weights.push(-1);
                agg_values.push(vals);
            }
            if has_after && let Some(after_states) = self.state.get(group_key) {
                let vals = compute_agg_values(after_states, &self.aggregations);
                out_group_rows.push(group_key.clone());
                out_weights.push(1);
                agg_values.push(vals);
            }
        }

        if out_group_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

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

fn compute_agg_values(states: &[AggState], aggregations: &[Aggregation]) -> Vec<Option<f64>> {
    states
        .iter()
        .zip(aggregations.iter())
        .map(|(state, agg)| state.current_value(agg))
        .collect()
}

fn build_output_batch(
    group_rows: &[Vec<Option<String>>],
    weights: &[i64],
    agg_values: &[Vec<Option<f64>>],
    group_by: &[String],
    aggregations: &[Aggregation],
    output_schema: &SchemaRef,
) -> DeltaResult<DeltaBatch> {
    let n_group = group_by.len();

    // Build group-by columns with their native types.
    // Group keys are stored as Option<String> (None = SQL null); cast to the
    // output schema's declared type so downstream operators see correct types.
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    for gi in 0..n_group {
        let vals: Vec<Option<&str>> = group_rows
            .iter()
            .map(|r| r.get(gi).and_then(|s| s.as_deref()))
            .collect();
        let string_col: Arc<dyn Array> = Arc::new(StringArray::from(vals));
        let target = output_schema.field(gi).data_type();
        if target == &DataType::Utf8 || target == &DataType::LargeUtf8 {
            cols.push(string_col);
        } else {
            cols.push(compute::cast(&string_col, target)?);
        }
    }

    // Build aggregate columns — Count is Int64, all others are Float64.
    for (ai, agg) in aggregations.iter().enumerate() {
        match agg {
            Aggregation::Count { .. } => {
                let vals: Int64Array = agg_values
                    .iter()
                    .map(|row| row.get(ai).copied().flatten().map(|v| v as i64))
                    .collect();
                cols.push(Arc::new(vals) as Arc<dyn Array>);
            }
            _ => {
                let vals: Float64Array = agg_values
                    .iter()
                    .map(|row| row.get(ai).copied().flatten())
                    .collect();
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
            vec![Aggregation::Count {
                output_col: "cnt".into(),
                input_col: None,
            }],
        )
        .unwrap();

        let d1 = DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 20.0])).unwrap();
        op.apply(d1).unwrap();
        // Count for c1 should be 2
        assert_eq!(
            op.state
                .get(&vec![Some("c1".to_string())])
                .map(|s| s[0].count),
            Some(2)
        );
    }

    #[test]
    fn min_float_retract_current_min_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Min = 1.2.
        // Retract 1.2. Min must become 2.7 (not 0.0, which the old i64 parse would give).
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Min {
                input_col: "v".into(),
                output_col: "min_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(insert).unwrap();

        // Current min for "g" should be 1.2
        let group_key = vec![Some("g".to_string())];
        let min_val = op
            .state
            .get(&group_key)
            .and_then(|s| s.first())
            .and_then(|s| s.min_max_set.keys().next())
            .map(|k| k.0);
        assert!(
            (min_val.unwrap_or(f64::NAN) - 1.2).abs() < 1e-9,
            "min before retraction should be 1.2, got {min_val:?}"
        );

        // Retract 1.2
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![1.2])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(retract).unwrap();

        // Min should now be 2.7, not 0.0
        let min_after = op
            .state
            .get(&group_key)
            .and_then(|s| s.first())
            .and_then(|s| s.min_max_set.keys().next())
            .map(|k| k.0);
        assert!(
            (min_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "min after retracting 1.2 should be 2.7, got {min_after:?}"
        );
    }

    #[test]
    fn max_float_retract_current_max_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Max = 3.5.
        // Retract 3.5. Max must become 2.7.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Max {
                input_col: "v".into(),
                output_col: "max_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(insert).unwrap();

        // Retract 3.5
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(retract).unwrap();

        let max_after = op
            .state
            .get(&vec![Some("g".to_string())])
            .and_then(|s| s.first())
            .and_then(|s| s.min_max_set.keys().next_back())
            .map(|k| k.0);
        assert!(
            (max_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "max after retracting 3.5 should be 2.7, got {max_after:?}"
        );
    }
}
