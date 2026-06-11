#![forbid(unsafe_code)]

//! E2.2 — GlobalSort physical operators.
//!
//! A distributed global sort runs in two phases:
//! 1. **Local sort** — each partition sorts its input batches individually.
//! 2. **Merge sort** — sorted partitions are merged into one total order.
//!
//! Callers feed sorted batches from multiple partitions to
//! [`SortedBatchMerger`] to obtain the final merged output.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

// ── Sort key descriptor ───────────────────────────────────────────────────────

/// A single column sort key: column name + ascending/descending.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

impl SortKey {
    pub fn asc(column: impl Into<String>) -> Self {
        Self { column: column.into(), ascending: true }
    }
    pub fn desc(column: impl Into<String>) -> Self {
        Self { column: column.into(), ascending: false }
    }
}

// ── Local sort ────────────────────────────────────────────────────────────────

/// Sort a single [`RecordBatch`] by `keys`.
///
/// Uses Arrow's `lexsort_to_indices` so all key types supported by Arrow are
/// handled without a manual dispatch.
pub fn sort_batch(batch: &RecordBatch, keys: &[SortKey]) -> ExecResult<RecordBatch> {
    if keys.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let sort_columns: Vec<SortColumn> = keys
        .iter()
        .map(|k| {
            let idx = batch
                .schema()
                .index_of(&k.column)
                .map_err(|_| ExecError::ColumnNotFound(k.column.clone()))?;
            Ok(SortColumn {
                values: batch.column(idx).clone(),
                options: Some(SortOptions {
                    descending: !k.ascending,
                    nulls_first: true,
                }),
            })
        })
        .collect::<ExecResult<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| ExecError::Arrow(e.to_string()))?;

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(|e| ExecError::Arrow(e.to_string())))
        .collect::<ExecResult<Vec<_>>>()?;

    RecordBatch::try_new(batch.schema(), columns).map_err(|e| ExecError::Arrow(e.to_string()))
}

// ── Sorted batch merger ───────────────────────────────────────────────────────

/// Merge-sort N pre-sorted [`RecordBatch`] slices into a single sorted batch.
///
/// Algorithm: k-way merge using a simple priority selection on the current
/// front row of each input. O(n·k) — suitable for moderate k (< 64 partitions).
/// For large k a heap-based merge should replace this.
pub struct SortedBatchMerger {
    keys: Vec<SortKey>,
}

impl SortedBatchMerger {
    pub fn new(keys: Vec<SortKey>) -> Self {
        Self { keys }
    }

    /// Merge `inputs` (each already sorted by `keys`) into one sorted batch.
    pub fn merge(&self, inputs: &[RecordBatch]) -> ExecResult<RecordBatch> {
        let non_empty: Vec<&RecordBatch> = inputs.iter().filter(|b| b.num_rows() > 0).collect();
        if non_empty.is_empty() {
            // All inputs have zero rows — return an empty batch using the schema of
            // the first input. If there are no inputs at all, we cannot infer a schema.
            let schema = inputs
                .first()
                .map(|b| b.schema())
                .ok_or_else(|| ExecError::InvalidInput("merge: no input batches".into()))?;
            return Ok(RecordBatch::new_empty(schema));
        }
        let inputs = non_empty;
        if inputs.len() == 1 {
            return Ok((*inputs[0]).clone());
        }

        let schema = inputs[0].schema();

        // Pointers: (batch_index, row_index)
        let mut pointers: Vec<(usize, usize)> = (0..inputs.len()).map(|i| (i, 0)).collect();
        let total_rows: usize = inputs.iter().map(|b| b.num_rows()).sum();

        // Output row indices in terms of (batch_index, row_index).
        let mut order: Vec<(usize, usize)> = Vec::with_capacity(total_rows);

        loop {
            // Find the pointer with the smallest current row.
            let mut min_pos: Option<usize> = None;
            for (pos, &(bi, ri)) in pointers.iter().enumerate() {
                if ri >= inputs[bi].num_rows() {
                    continue;
                }
                match min_pos {
                    None => min_pos = Some(pos),
                    Some(mp) => {
                        let (mbi, mri) = pointers[mp];
                        if self.row_less(inputs[bi], ri, inputs[mbi], mri)? {
                            min_pos = Some(pos);
                        }
                    }
                }
            }
            match min_pos {
                None => break,
                Some(pos) => {
                    let (bi, ri) = pointers[pos];
                    order.push((bi, ri));
                    pointers[pos] = (bi, ri + 1);
                }
            }
        }

        // Materialise the merged output.
        // Build per-column arrays from (batch, row) index pairs.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for col_idx in 0..schema.fields().len() {
            let column = materialise_column(inputs.as_slice(), col_idx, &order, schema.as_ref())?;
            columns.push(column);
        }

        RecordBatch::try_new(schema, columns).map_err(|e| ExecError::Arrow(e.to_string()))
    }

    /// Returns `true` if `(lb, lr)` sorts before `(rb, rr)`.
    fn row_less(
        &self,
        lb: &RecordBatch,
        lr: usize,
        rb: &RecordBatch,
        rr: usize,
    ) -> ExecResult<bool> {
        for key in &self.keys {
            let l_idx = lb
                .schema()
                .index_of(&key.column)
                .map_err(|_| ExecError::ColumnNotFound(key.column.clone()))?;
            let r_idx = rb
                .schema()
                .index_of(&key.column)
                .map_err(|_| ExecError::ColumnNotFound(key.column.clone()))?;
            let lv = scalar_at(lb.column(l_idx), lr)?;
            let rv = scalar_at(rb.column(r_idx), rr)?;
            match lv.partial_cmp(&rv) {
                None => continue,
                Some(std::cmp::Ordering::Less) => return Ok(key.ascending),
                Some(std::cmp::Ordering::Greater) => return Ok(!key.ascending),
                Some(std::cmp::Ordering::Equal) => continue,
            }
        }
        Ok(false) // equal — treat as not-less
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Scalar value extracted from an array for comparison purposes.
#[derive(Debug, PartialEq, PartialOrd)]
enum ScalarVal {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    UInt32(u32),
}

fn scalar_at(arr: &dyn arrow::array::Array, row: usize) -> ExecResult<ScalarVal> {
    if arr.is_null(row) {
        return Ok(ScalarVal::Null);
    }
    match arr.data_type() {
        DataType::Int32 => Ok(ScalarVal::Int32(
            arr.as_any().downcast_ref::<Int32Array>().unwrap().value(row),
        )),
        DataType::Int64 => Ok(ScalarVal::Int64(
            arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
        )),
        DataType::Float64 => Ok(ScalarVal::Float64(
            arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        )),
        DataType::Utf8 => Ok(ScalarVal::Utf8(
            arr.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_owned(),
        )),
        DataType::Boolean => Ok(ScalarVal::Bool(
            arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
        )),
        DataType::UInt32 => Ok(ScalarVal::UInt32(
            arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(row),
        )),
        dt => Err(ExecError::UnsupportedType(format!("sort: unsupported type {dt}"))),
    }
}

/// Materialise one column from (batch, row) index pairs into an `ArrayRef`.
fn materialise_column(
    inputs: &[&RecordBatch],
    col_idx: usize,
    order: &[(usize, usize)],
    schema: &Schema,
) -> ExecResult<ArrayRef> {
    // We gather per-row values by iterating the order and using `take` on each
    // source batch.  To avoid per-row allocations, group consecutive rows from
    // the same batch together, then concatenate.
    //
    // Fast path: build a UInt32 index list for each source batch, run `take`,
    // and finally concatenate the pieces in order.

    let n = inputs.len();
    let mut per_batch_indices: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut per_batch_positions: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (out_pos, &(bi, ri)) in order.iter().enumerate() {
        per_batch_indices[bi].push(ri as u32);
        per_batch_positions[bi].push(out_pos);
    }

    // Build one output array of the right length.
    let total = order.len();
    let dt = schema.field(col_idx).data_type();
    let mut out_pieces: Vec<(usize, ArrayRef)> = Vec::new();

    for bi in 0..n {
        if per_batch_indices[bi].is_empty() {
            continue;
        }
        let idx_arr = UInt32Array::from(per_batch_indices[bi].clone());
        let taken =
            take(inputs[bi].column(col_idx), &idx_arr, None)
                .map_err(|e| ExecError::Arrow(e.to_string()))?;
        out_pieces.push((bi, taken));
    }

    // Now assemble the final array in `order` order.
    // We know what output position each (bi,ri) maps to; use scatter.
    // Simple approach: allocate per-type builder and fill.
    scatter_column(inputs, col_idx, order, total, dt)
}

/// Build one output column by iterating `order` and reading each row.
fn scatter_column(
    inputs: &[&RecordBatch],
    col_idx: usize,
    order: &[(usize, usize)],
    total: usize,
    dt: &DataType,
) -> ExecResult<ArrayRef> {
    use arrow::array::*;
    macro_rules! scatter_prim {
        ($ty:ty, $build:ty, $cast:path) => {{
            let mut b = <$build>::with_capacity(total);
            for &(bi, ri) in order {
                let arr = inputs[bi].column(col_idx);
                if arr.is_null(ri) {
                    b.append_null();
                } else {
                    b.append_value($cast(arr.as_any().downcast_ref::<$ty>().unwrap().value(ri)));
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let result: ArrayRef = match dt {
        DataType::Int32 => scatter_prim!(Int32Array, Int32Builder, std::convert::identity),
        DataType::Int64 => scatter_prim!(Int64Array, Int64Builder, std::convert::identity),
        DataType::Float64 => scatter_prim!(Float64Array, Float64Builder, std::convert::identity),
        DataType::UInt32 => scatter_prim!(UInt32Array, UInt32Builder, std::convert::identity),
        DataType::Boolean => scatter_prim!(BooleanArray, BooleanBuilder, std::convert::identity),
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(total, total * 8);
            for &(bi, ri) in order {
                let arr = inputs[bi].column(col_idx);
                if arr.is_null(ri) {
                    b.append_null();
                } else {
                    b.append_value(arr.as_any().downcast_ref::<StringArray>().unwrap().value(ri));
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        other => {
            return Err(ExecError::UnsupportedType(format!(
                "sort merge: unsupported column type {other}"
            )));
        }
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(ids: &[i32], vals: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(vals.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn sort_batch_ascending_by_id() {
        let batch = make_batch(&[3, 1, 2], &["c", "a", "b"]);
        let sorted = sort_batch(&batch, &[SortKey::asc("id")]).unwrap();
        let ids = sorted.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[test]
    fn sort_batch_descending_by_id() {
        let batch = make_batch(&[1, 3, 2], &["a", "c", "b"]);
        let sorted = sort_batch(&batch, &[SortKey::desc("id")]).unwrap();
        let ids = sorted.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[3, 2, 1]);
    }

    #[test]
    fn sort_batch_empty_returns_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema);
        let sorted = sort_batch(&batch, &[SortKey::asc("id")]).unwrap();
        assert_eq!(sorted.num_rows(), 0);
    }

    #[test]
    fn merge_two_sorted_batches() {
        let left = make_batch(&[1, 3, 5], &["a", "c", "e"]);
        let right = make_batch(&[2, 4, 6], &["b", "d", "f"]);
        let merger = SortedBatchMerger::new(vec![SortKey::asc("id")]);
        let merged = merger.merge(&[left, right]).unwrap();
        let ids = merged.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn merge_three_sorted_batches() {
        let b1 = make_batch(&[1, 4], &["a", "d"]);
        let b2 = make_batch(&[2, 5], &["b", "e"]);
        let b3 = make_batch(&[3, 6], &["c", "f"]);
        let merger = SortedBatchMerger::new(vec![SortKey::asc("id")]);
        let merged = merger.merge(&[b1, b2, b3]).unwrap();
        let ids = merged.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn merge_single_batch_returns_clone() {
        let batch = make_batch(&[5, 3, 1], &["e", "c", "a"]);
        let sorted = sort_batch(&batch, &[SortKey::asc("id")]).unwrap();
        let merger = SortedBatchMerger::new(vec![SortKey::asc("id")]);
        let merged = merger.merge(&[sorted.clone()]).unwrap();
        let ids = merged.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 3, 5]);
    }

    #[test]
    fn merge_with_duplicate_keys() {
        let b1 = make_batch(&[1, 2], &["a1", "b1"]);
        let b2 = make_batch(&[1, 3], &["a2", "c1"]);
        let merger = SortedBatchMerger::new(vec![SortKey::asc("id")]);
        let merged = merger.merge(&[b1, b2]).unwrap();
        assert_eq!(merged.num_rows(), 4);
        let ids = merged.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        // Both rows with id=1 appear, in some order before id=2, id=3.
        let sorted_ids: Vec<i32> = (0..merged.num_rows()).map(|i| ids.value(i)).collect();
        assert_eq!(sorted_ids, vec![1, 1, 2, 3]);
    }
}
