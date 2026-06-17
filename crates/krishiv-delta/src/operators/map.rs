#![forbid(unsafe_code)]

//! Linear map and project operators.
//!
//! Linear operators preserve the weight column exactly — they apply only to
//! the data columns. This is the key property that makes them free to
//! incrementalize: `map(ΔA) = Δ(map(A))`.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};

/// Apply a function to the data columns of a `DeltaBatch`, preserving weights.
///
/// `f` receives the data `RecordBatch` (no `_weight` column) and must return
/// a new `RecordBatch` with the same number of rows and a compatible schema.
/// The `_weight` column from the input is re-attached to the output.
pub fn map_batch<F>(batch: DeltaBatch, f: F) -> DeltaResult<DeltaBatch>
where
    F: FnOnce(RecordBatch) -> DeltaResult<RecordBatch>,
{
    let weights = batch.weights().clone();
    let data = batch.data_batch();
    let mapped = f(data)?;

    if mapped.num_rows() != weights.len() {
        return Err(DeltaError::Operator(format!(
            "map function changed row count from {} to {}; map must preserve row count",
            weights.len(),
            mapped.num_rows()
        )));
    }

    let mut fields: Vec<_> = mapped.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    let full_schema = Arc::new(Schema::new(fields));

    let mut cols: Vec<Arc<dyn Array>> = mapped.columns().to_vec();
    cols.push(Arc::new(weights));

    let inner = RecordBatch::try_new(full_schema, cols)?;
    DeltaBatch::from_weighted(inner)
}

/// Project a `DeltaBatch` to a subset of data columns.
///
/// `output_columns` is a list of column names to keep (order preserved).
/// The `_weight` column is always carried through.
pub fn project_batch(batch: DeltaBatch, output_columns: &[&str]) -> DeltaResult<DeltaBatch> {
    let data = batch.data_batch();
    let schema = data.schema();

    let col_indices: Vec<usize> = output_columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| DeltaError::ColumnNotFound((*name).to_string()))
        })
        .collect::<DeltaResult<Vec<_>>>()?;

    let out_fields: Vec<_> = col_indices
        .iter()
        .map(|&i| schema.field(i).clone())
        .collect();
    let out_schema = Arc::new(Schema::new(out_fields));
    let out_cols: Vec<Arc<dyn Array>> =
        col_indices.iter().map(|&i| data.column(i).clone()).collect();

    map_batch(batch, |_| {
        RecordBatch::try_new(out_schema, out_cols).map_err(DeltaError::Arrow)
    })
}

/// `MapOp` wraps a `project` as a reusable streaming operator.
pub struct ProjectOp {
    output_columns: Vec<String>,
    output_schema: SchemaRef,
}

impl ProjectOp {
    pub fn new(input_schema: &SchemaRef, output_columns: Vec<String>) -> DeltaResult<Self> {
        let fields: Vec<_> = output_columns
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let output_schema = Arc::new(Schema::new(fields));
        Ok(Self { output_columns, output_schema })
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn apply(&self, batch: DeltaBatch) -> DeltaResult<DeltaBatch> {
        let col_refs: Vec<&str> = self.output_columns.iter().map(String::as_str).collect();
        project_batch(batch, &col_refs)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn two_col_batch(ids: &[i32], vals: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(Int32Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn project_keeps_one_column() {
        let data = two_col_batch(&[1, 2], &[10, 20]);
        let cb = DeltaBatch::from_inserts(data).unwrap();
        let projected = project_batch(cb, &["id"]).unwrap();
        assert_eq!(projected.data_schema().fields().len(), 1);
        assert_eq!(projected.data_schema().field(0).name(), "id");
        assert_eq!(projected.num_rows(), 2);
    }

    #[test]
    fn map_preserves_weights() {
        let data = two_col_batch(&[1], &[100]);
        let cb = DeltaBatch::from_deletes(data).unwrap();
        let result = map_batch(cb, Ok).unwrap();
        assert_eq!(result.weights().value(0), -1);
    }

    #[test]
    fn project_missing_column_errors() {
        let data = two_col_batch(&[1], &[1]);
        let cb = DeltaBatch::from_inserts(data).unwrap();
        let err = project_batch(cb, &["nonexistent"]);
        assert!(err.is_err());
    }
}
