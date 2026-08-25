#![forbid(unsafe_code)]

//! Linear filter operator.
//!
//! Filter is linear: `filter(ΔA) = Δ(filter(A))`. Applying filter to a delta
//! yields exactly the same result as computing the full filtered view and
//! differencing — so no state is needed, just apply the predicate.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::delta_batch::DeltaBatch;
use crate::error::{DeltaError, DeltaResult};

/// Apply a predicate to the data columns of a `DeltaBatch`.
///
/// `pred` receives the data `RecordBatch` (no `_weight` column) and must
/// return a `BooleanArray` of the same length. Rows where the mask is `false`
/// or `null` are dropped; their weights are discarded.
pub fn filter_batch<F>(batch: DeltaBatch, pred: F) -> DeltaResult<DeltaBatch>
where
    F: FnOnce(&RecordBatch) -> DeltaResult<BooleanArray>,
{
    let data = batch.data_batch();
    let mask = pred(&data)?;

    if mask.len() != data.num_rows() {
        return Err(DeltaError::Operator(format!(
            "filter predicate returned mask length {} but batch has {} rows",
            mask.len(),
            data.num_rows()
        )));
    }

    batch.filter_mask(&mask)
}

/// Replace a delta's data columns, keeping every row's weight untouched.
///
/// This is the kernel behind the O(Δ) map/projection plan (IVM-MAP-1). It is
/// sound for exactly one reason, and the reason is worth stating because it is
/// what makes the operator stateless: **projection is linear over Z-sets.**
/// `map` distributes over multiset addition — `map(A + B) = map(A) + map(B)` —
/// so mapping a delta gives precisely the delta of the mapped relation, with no
/// accumulator and nothing to checkpoint. Aggregation and DISTINCT are *not*
/// linear, which is why those operators must carry state and this one must not.
///
/// `f` must return a batch with the same row count and in the same row order —
/// row `i` of the result is row `i` of the input, so it keeps row `i`'s weight.
/// A row-count change is rejected rather than allowed to misalign weights,
/// which would silently mislabel insertions as retractions.
///
/// Note the mapped rows are deliberately **not** consolidated: a projection may
/// be non-injective (two source rows collapsing to one output row), and SQL
/// projection does not deduplicate. Their weights are summed downstream by
/// whatever consumes the Z-set, which is where `SELECT DISTINCT` would apply.
pub fn map_batch<F>(batch: DeltaBatch, f: F) -> DeltaResult<DeltaBatch>
where
    F: FnOnce(&RecordBatch) -> DeltaResult<RecordBatch>,
{
    let data = batch.data_batch();
    let rows_in = data.num_rows();
    let mapped = f(&data)?;
    if mapped.num_rows() != rows_in {
        return Err(DeltaError::Operator(format!(
            "map produced {} rows from {rows_in} input rows; a map must be \
             row-preserving or the weights it carries no longer line up",
            mapped.num_rows()
        )));
    }
    let mut columns: Vec<ArrayRef> = mapped.columns().to_vec();
    columns.push(Arc::new(batch.weights().clone()));
    let mut fields: Vec<Arc<Field>> = mapped.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        crate::delta_batch::WEIGHT_COLUMN,
        DataType::Int64,
        false,
    )));
    let inner = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| DeltaError::Operator(format!("map rebuild failed: {e}")))?;
    DeltaBatch::from_weighted(inner)
}

/// `FilterOp` holds a static column predicate: keep rows where `column == value`.
/// For richer predicates, use `filter_batch` directly with a closure.
pub struct FilterOp {
    column: String,
    value: FilterValue,
}

#[derive(Clone)]
pub enum FilterValue {
    Int64Gt(i64),
    Int64Ge(i64),
    Int64Lt(i64),
    Int64Le(i64),
    Int64Eq(i64),
    StringEq(String),
}

impl FilterOp {
    pub fn col_gt(column: impl Into<String>, threshold: i64) -> Self {
        Self {
            column: column.into(),
            value: FilterValue::Int64Gt(threshold),
        }
    }
    pub fn col_ge(column: impl Into<String>, threshold: i64) -> Self {
        Self {
            column: column.into(),
            value: FilterValue::Int64Ge(threshold),
        }
    }
    pub fn col_eq_str(column: impl Into<String>, val: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            value: FilterValue::StringEq(val.into()),
        }
    }

    pub fn apply(&self, batch: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let col_name = self.column.clone();
        let val = self.value.clone();
        filter_batch(batch, move |data| {
            let col_idx = data
                .schema()
                .index_of(&col_name)
                .map_err(|_| DeltaError::ColumnNotFound(col_name.clone()))?;
            let col = data.column(col_idx);
            let mask = match &val {
                FilterValue::Int64Gt(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter()
                        .map(|v| Some(v.unwrap_or(i64::MIN) > t))
                        .collect()
                }
                FilterValue::Int64Ge(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter()
                        .map(|v| Some(v.unwrap_or(i64::MIN) >= t))
                        .collect()
                }
                FilterValue::Int64Lt(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter()
                        .map(|v| Some(v.unwrap_or(i64::MIN) < t))
                        .collect()
                }
                FilterValue::Int64Le(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter()
                        .map(|v| Some(v.unwrap_or(i64::MAX) <= t))
                        .collect()
                }
                FilterValue::Int64Eq(expected) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let e = *expected;
                    arr.iter().map(|v| Some(v == Some(e))).collect()
                }
                FilterValue::StringEq(expected) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| DeltaError::Operator("expected String column".into()))?;
                    arr.iter()
                        .map(|v| Some(v == Some(expected.as_str())))
                        .collect()
                }
            };
            Ok(mask)
        })
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn amount_batch(amounts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(amounts.to_vec()))]).unwrap()
    }

    #[test]
    fn filter_gt_keeps_positives() {
        let cb = DeltaBatch::from_inserts(amount_batch(&[-1, 0, 5, 10])).unwrap();
        let op = FilterOp::col_gt("amount", 0);
        let result = op.apply(cb).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn filter_gt_preserves_weights() {
        let cb = DeltaBatch::from_deletes(amount_batch(&[5])).unwrap();
        let op = FilterOp::col_gt("amount", 0);
        let result = op.apply(cb).unwrap();
        assert_eq!(result.weights().value(0), -1);
    }

    #[test]
    fn filter_on_missing_column_errors() {
        let cb = DeltaBatch::from_inserts(amount_batch(&[1])).unwrap();
        let op = FilterOp::col_gt("nonexistent", 0);
        assert!(op.apply(cb).is_err());
    }
}
