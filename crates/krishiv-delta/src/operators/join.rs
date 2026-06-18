#![forbid(unsafe_code)]

//! Bilinear incremental join operator.
//!
//! The DBSP identity for join is:
//!
//!   Δ(A ⋈ B) = (ΔA ⋈ B_trace) + (A_trace ⋈ ΔB)
//!
//! This operator maintains two `Trace` objects, one per side. Each tick:
//! 1. Probe B_trace with all keys from ΔA → emit output rows with weight = ΔA.weight
//! 2. Probe A_trace with all keys from ΔB → emit output rows with weight = ΔB.weight
//! 3. Insert ΔA into A_trace, ΔB into B_trace
//! 4. Return Union(step1, step2) as the output delta

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};
use crate::trace::Trace;

/// Join type for incremental joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrJoinType {
    Inner,
}

/// Bilinear incremental join operator.
///
/// Maintains two `Trace` objects (one per join side) and performs probe-based
/// hash-join on each tick, processing only the delta input.
pub struct IncrementalJoinOp {
    left_trace: Trace,
    right_trace: Trace,
    left_key_cols: Vec<String>,
    right_key_cols: Vec<String>,
    // Kept for future LEFT/RIGHT/FULL OUTER join support and validation.
    #[allow(dead_code)]
    left_schema: SchemaRef,
    #[allow(dead_code)]
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    // Kept for future join type dispatch (LEFT/RIGHT/FULL OUTER).
    #[allow(dead_code)]
    join_type: IncrJoinType,
}

impl IncrementalJoinOp {
    /// Create a new incremental join operator.
    ///
    /// * `left_schema` / `right_schema` — data schemas (no `_weight`)
    /// * `left_key_cols` / `right_key_cols` — matching join key column names
    /// * `join_type` — inner join only for now
    pub fn new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_key_cols: Vec<String>,
        right_key_cols: Vec<String>,
        join_type: IncrJoinType,
    ) -> DeltaResult<Self> {
        let left_key_refs: Vec<&str> = left_key_cols.iter().map(String::as_str).collect();
        let right_key_refs: Vec<&str> = right_key_cols.iter().map(String::as_str).collect();

        let left_trace = Trace::new(left_schema.clone(), &left_key_refs)?;
        let right_trace = Trace::new(right_schema.clone(), &right_key_refs)?;

        // Output schema: all left data columns + all right non-key data columns.
        let mut out_fields: Vec<_> = left_schema.fields().iter().cloned().collect();
        for field in right_schema.fields().iter() {
            if !right_key_cols.contains(field.name()) {
                out_fields.push(field.clone());
            }
        }
        let output_schema = Arc::new(Schema::new(out_fields));

        Ok(Self {
            left_trace,
            right_trace,
            left_key_cols,
            right_key_cols,
            left_schema,
            right_schema,
            output_schema,
            join_type,
        })
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Apply one tick of the bilinear join.
    ///
    /// * `delta_left` — changes to the left relation this tick (may be empty)
    /// * `delta_right` — changes to the right relation this tick (may be empty)
    ///
    /// Returns the combined output delta: `(ΔA ⋈ B_trace) ∪ (A_trace ⋈ ΔB)`.
    pub fn apply(
        &mut self,
        delta_left: Option<DeltaBatch>,
        delta_right: Option<DeltaBatch>,
    ) -> DeltaResult<DeltaBatch> {
        let mut output_parts: Vec<DeltaBatch> = Vec::new();

        // Step 1: ΔA ⋈ B_trace
        if let Some(dl) = &delta_left
            && !dl.is_empty()
        {
            let probe_result = self.probe_left_against_right_trace(dl)?;
            if !probe_result.is_empty() {
                output_parts.push(probe_result);
            }
        }

        // Step 2: A_trace ⋈ ΔB
        if let Some(dr) = &delta_right
            && !dr.is_empty()
        {
            let probe_result = self.probe_right_against_left_trace(dr)?;
            if !probe_result.is_empty() {
                output_parts.push(probe_result);
            }
        }

        // Step 3: update traces AFTER probe (traces reflect state from previous ticks)
        if let Some(dl) = delta_left {
            self.left_trace.insert(dl);
        }
        if let Some(dr) = delta_right {
            self.right_trace.insert(dr);
        }

        // Step 4: combine output
        if output_parts.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }
        if output_parts.len() == 1 {
            return Ok(output_parts.remove(0));
        }
        DeltaBatch::concat(&output_parts)
    }

    // ── Internal probe methods ─────────────────────────────────────────────────

    fn probe_left_against_right_trace(&self, delta_left: &DeltaBatch) -> DeltaResult<DeltaBatch> {
        // For each row in delta_left, look up matching rows in right_trace.
        // Output row weight = delta_left.weight (the right_trace rows have
        // accumulated weight +1 in standard usage).
        let left_data = delta_left.data_batch();
        let left_weights = delta_left.weights();

        // Extract key values from left delta to probe right trace.
        let left_key_data = project_columns(&left_data, &self.left_key_cols)?;
        let right_matches = self.right_trace.probe_by_keys(&left_key_data)?;

        if right_matches.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_join_output_left_probe(&left_data, left_weights, &right_matches)
    }

    fn probe_right_against_left_trace(&self, delta_right: &DeltaBatch) -> DeltaResult<DeltaBatch> {
        let right_data = delta_right.data_batch();
        let right_weights = delta_right.weights();

        let right_key_data = project_columns(&right_data, &self.right_key_cols)?;
        let left_matches = self.left_trace.probe_by_keys(&right_key_data)?;

        if left_matches.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_join_output_right_probe(&left_matches, &right_data, right_weights)
    }

    /// Build output rows: for each (left_row, right_row) pair where join keys match,
    /// emit one output row with weight = left_delta.weight * right_trace.weight.
    fn build_join_output_left_probe(
        &self,
        left_data: &RecordBatch,
        left_weights: &Int64Array,
        right_matches: &DeltaBatch,
    ) -> DeltaResult<DeltaBatch> {
        let right_data = right_matches.data_batch();
        let right_weights = right_matches.weights();

        let left_key_indices = col_indices(left_data, &self.left_key_cols)?;
        let right_key_indices = col_indices(&right_data, &self.right_key_cols)?;

        let mut out_left_rows: Vec<usize> = Vec::new();
        let mut out_right_rows: Vec<usize> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for li in 0..left_data.num_rows() {
            for ri in 0..right_data.num_rows() {
                if keys_match(
                    left_data,
                    &left_key_indices,
                    li,
                    &right_data,
                    &right_key_indices,
                    ri,
                ) {
                    out_left_rows.push(li);
                    out_right_rows.push(ri);
                    out_weights.push(left_weights.value(li) * right_weights.value(ri));
                }
            }
        }

        if out_left_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        build_join_batch(
            left_data,
            &right_data,
            &self.right_key_cols,
            &out_left_rows,
            &out_right_rows,
            out_weights,
            &self.output_schema,
        )
    }

    fn build_join_output_right_probe(
        &self,
        left_matches: &DeltaBatch,
        right_data: &RecordBatch,
        right_weights: &Int64Array,
    ) -> DeltaResult<DeltaBatch> {
        let left_data = left_matches.data_batch();
        let left_weights = left_matches.weights();

        let left_key_indices = col_indices(&left_data, &self.left_key_cols)?;
        let right_key_indices = col_indices(right_data, &self.right_key_cols)?;

        let mut out_left_rows: Vec<usize> = Vec::new();
        let mut out_right_rows: Vec<usize> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();

        for li in 0..left_data.num_rows() {
            for ri in 0..right_data.num_rows() {
                if keys_match(
                    &left_data,
                    &left_key_indices,
                    li,
                    right_data,
                    &right_key_indices,
                    ri,
                ) {
                    out_left_rows.push(li);
                    out_right_rows.push(ri);
                    out_weights.push(left_weights.value(li) * right_weights.value(ri));
                }
            }
        }

        if out_left_rows.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        build_join_batch(
            &left_data,
            right_data,
            &self.right_key_cols,
            &out_left_rows,
            &out_right_rows,
            out_weights,
            &self.output_schema,
        )
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn col_indices(batch: &RecordBatch, cols: &[String]) -> DeltaResult<Vec<usize>> {
    cols.iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
        })
        .collect()
}

fn project_columns(batch: &RecordBatch, col_names: &[String]) -> DeltaResult<RecordBatch> {
    let indices = col_indices(batch, col_names)?;
    let fields: Vec<_> = indices
        .iter()
        .map(|&i| Arc::new(batch.schema().field(i).clone()))
        .collect();
    let cols: Vec<Arc<dyn Array>> = indices.iter().map(|&i| batch.column(i).clone()).collect();
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

fn keys_match(
    left: &RecordBatch,
    left_indices: &[usize],
    li: usize,
    right: &RecordBatch,
    right_indices: &[usize],
    ri: usize,
) -> bool {
    left_indices
        .iter()
        .zip(right_indices.iter())
        .all(|(&lk, &rk)| {
            let la = left.column(lk);
            let ra = right.column(rk);
            scalar_eq(la, li, ra, ri)
        })
}

fn scalar_eq(a: &dyn Array, ai: usize, b: &dyn Array, bi: usize) -> bool {
    use arrow::array::{Int32Array, Int64Array, StringArray};
    if a.is_null(ai) && b.is_null(bi) {
        return true;
    }
    if a.is_null(ai) || b.is_null(bi) {
        return false;
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<Int64Array>(),
        b.as_any().downcast_ref::<Int64Array>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<Int32Array>(),
        b.as_any().downcast_ref::<Int32Array>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    if let (Some(av), Some(bv)) = (
        a.as_any().downcast_ref::<StringArray>(),
        b.as_any().downcast_ref::<StringArray>(),
    ) {
        return av.value(ai) == bv.value(bi);
    }
    false
}

fn build_join_batch(
    left_data: &RecordBatch,
    right_data: &RecordBatch,
    right_key_cols: &[String],
    left_rows: &[usize],
    right_rows: &[usize],
    weights: Vec<i64>,
    output_schema: &SchemaRef,
) -> DeltaResult<DeltaBatch> {
    let left_indices =
        arrow::array::UInt64Array::from(left_rows.iter().map(|&r| r as u64).collect::<Vec<_>>());
    let right_indices =
        arrow::array::UInt64Array::from(right_rows.iter().map(|&r| r as u64).collect::<Vec<_>>());

    let left_cols: Vec<Arc<dyn Array>> = left_data
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &left_indices, None).map_err(DeltaError::Arrow))
        .collect::<DeltaResult<Vec<_>>>()?;

    let right_non_key_cols: Vec<Arc<dyn Array>> = right_data
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !right_key_cols.contains(f.name()))
        .map(|(i, _)| {
            arrow::compute::take(right_data.column(i), &right_indices, None)
                .map_err(DeltaError::Arrow)
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    let mut all_cols: Vec<Arc<dyn Array>> = left_cols;
    all_cols.extend(right_non_key_cols);
    all_cols.push(Arc::new(Int64Array::from(weights)));

    // Build the full schema (output_schema + _weight).
    let mut full_fields: Vec<_> = output_schema.fields().iter().cloned().collect();
    full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let full_schema = Arc::new(Schema::new(full_fields));

    let inner = RecordBatch::try_new(full_schema, all_cols)?;
    DeltaBatch::from_weighted(inner)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]))
    }

    fn customers_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn orders_batch(order_ids: &[i32], cust_ids: &[i32]) -> RecordBatch {
        RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int32Array::from(order_ids.to_vec())),
                Arc::new(Int32Array::from(cust_ids.to_vec())),
            ],
        )
        .unwrap()
    }

    fn customers_batch(cust_ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            customers_schema(),
            vec![
                Arc::new(Int32Array::from(cust_ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn join_delta_left_against_trace_right() {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        // Tick 1: insert right (customers) only
        let c = DeltaBatch::from_inserts(customers_batch(&[1, 2], &["Alice", "Bob"])).unwrap();
        let out1 = op.apply(None, Some(c)).unwrap();
        assert!(out1.is_empty(), "no left delta → no output yet");

        // Tick 2: insert left (orders) — should join with right trace
        let o = DeltaBatch::from_inserts(orders_batch(&[100, 101], &[1, 2])).unwrap();
        let out2 = op.apply(Some(o), None).unwrap();
        assert_eq!(
            out2.num_rows(),
            2,
            "two orders should join with two customers"
        );
        assert!(out2.weights().iter().all(|w| w == Some(1)));
    }

    #[test]
    fn join_retraction_propagates_negative_weight() {
        let mut op = IncrementalJoinOp::new(
            orders_schema(),
            customers_schema(),
            vec!["customer_id".into()],
            vec!["customer_id".into()],
            IncrJoinType::Inner,
        )
        .unwrap();

        // Build up traces first
        let c = DeltaBatch::from_inserts(customers_batch(&[1], &["Alice"])).unwrap();
        op.apply(None, Some(c)).unwrap();
        let o = DeltaBatch::from_inserts(orders_batch(&[100], &[1])).unwrap();
        op.apply(Some(o), None).unwrap();

        // Delete a customer → should produce retraction in output
        let del_c = DeltaBatch::from_deletes(customers_batch(&[1], &["Alice"])).unwrap();
        let out = op.apply(None, Some(del_c)).unwrap();
        assert!(!out.is_empty());
        assert_eq!(out.weights().value(0), -1);
    }
}
