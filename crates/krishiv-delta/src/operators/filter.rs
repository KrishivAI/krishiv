#![forbid(unsafe_code)]

//! Linear filter operator.
//!
//! Filter is linear: `filter(ΔA) = Δ(filter(A))`. Applying filter to a delta
//! yields exactly the same result as computing the full filtered view and
//! differencing — so no state is needed, just apply the predicate.

use arrow::array::BooleanArray;
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
        Self { column: column.into(), value: FilterValue::Int64Gt(threshold) }
    }
    pub fn col_ge(column: impl Into<String>, threshold: i64) -> Self {
        Self { column: column.into(), value: FilterValue::Int64Ge(threshold) }
    }
    pub fn col_eq_str(column: impl Into<String>, val: impl Into<String>) -> Self {
        Self { column: column.into(), value: FilterValue::StringEq(val.into()) }
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
                    arr.iter().map(|v| Some(v.unwrap_or(i64::MIN) > t)).collect()
                }
                FilterValue::Int64Ge(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter().map(|v| Some(v.unwrap_or(i64::MIN) >= t)).collect()
                }
                FilterValue::Int64Lt(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter().map(|v| Some(v.unwrap_or(i64::MIN) < t)).collect()
                }
                FilterValue::Int64Le(threshold) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or_else(|| DeltaError::Operator("expected Int64 column".into()))?;
                    let t = *threshold;
                    arr.iter().map(|v| Some(v.unwrap_or(i64::MAX) <= t)).collect()
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
                    arr.iter().map(|v| Some(v == Some(expected.as_str()))).collect()
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Int64, false),
        ]));
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
