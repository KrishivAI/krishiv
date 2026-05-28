//! Normalize incoming batches to a target schema (R14 S3.1).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::ExecError;

/// Maps target column names to source column names for rename evolution.
#[derive(Debug, Clone, Default)]
pub struct ColumnRenameMap {
    target_to_source: HashMap<String, String>,
}

impl ColumnRenameMap {
    pub fn new(renames: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            target_to_source: renames.into_iter().collect(),
        }
    }
}

/// Arrow-level schema normalizer inserted before live-table delta writers.
#[derive(Debug, Clone)]
pub struct SchemaNormalizeOperator {
    target: Arc<Schema>,
    renames: ColumnRenameMap,
}

impl SchemaNormalizeOperator {
    pub fn new(target: Arc<Schema>) -> Self {
        Self {
            target,
            renames: ColumnRenameMap::default(),
        }
    }

    pub fn with_renames(mut self, renames: ColumnRenameMap) -> Self {
        self.renames = renames;
        self
    }

    pub fn set_target_schema(&mut self, target: Arc<Schema>) {
        self.target = target;
    }

    fn source_column<'a>(&'a self, target_field: &'a str) -> &'a str {
        self.renames
            .target_to_source
            .get(target_field)
            .map(String::as_str)
            .unwrap_or(target_field)
    }

    pub fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        if batch.schema() == self.target {
            return Ok(batch.clone());
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.target.fields().len());
        for field in self.target.fields() {
            let lookup = self.source_column(field.name());
            let value = if let Ok(idx) = batch.schema().index_of(lookup) {
                let col = batch.column(idx);
                Self::cast_column(col, field)?
            } else {
                Arc::new(arrow::array::new_null_array(
                    field.data_type(),
                    batch.num_rows(),
                ))
            };
            columns.push(value);
        }
        RecordBatch::try_new(self.target.clone(), columns)
            .map_err(|e| ExecError::Arrow(e.to_string()))
    }

    fn cast_column(col: &ArrayRef, target_field: &Field) -> Result<ArrayRef, ExecError> {
        if col.data_type() == target_field.data_type() {
            return Ok(col.clone());
        }
        if Self::is_widen(col.data_type(), target_field.data_type()) {
            return cast(col, target_field.data_type())
                .map_err(|e| ExecError::IncompatibleSchemaEvolution(e.to_string()));
        }
        if target_field.is_nullable() {
            return cast(col, target_field.data_type())
                .map_err(|e| ExecError::IncompatibleSchemaEvolution(e.to_string()));
        }
        Err(ExecError::IncompatibleSchemaEvolution(format!(
            "cannot cast {:?} to {:?} for column {}",
            col.data_type(),
            target_field.data_type(),
            target_field.name()
        )))
    }

    fn is_widen(from: &DataType, to: &DataType) -> bool {
        matches!(
            (from, to),
            (DataType::Int8, DataType::Int16)
                | (DataType::Int8, DataType::Int32)
                | (DataType::Int16, DataType::Int32)
                | (DataType::Int16, DataType::Int64)
                | (DataType::Int32, DataType::Int64)
                | (DataType::Int32, DataType::Float64)
                | (DataType::Int64, DataType::Float64)
                | (DataType::UInt8, DataType::UInt16)
                | (DataType::UInt8, DataType::UInt32)
                | (DataType::UInt16, DataType::UInt32)
                | (DataType::UInt16, DataType::UInt64)
                | (DataType::UInt32, DataType::UInt64)
                | (DataType::Float32, DataType::Float64)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(schema: Arc<Schema>, cols: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn add_nullable_column() {
        let src = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b = batch(src, vec![Arc::new(Int64Array::from(vec![1_i64]))]);
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("discount", DataType::Float64, true),
        ]));
        let out = SchemaNormalizeOperator::new(target).normalize(&b).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(1).null_count(), 1);
    }

    #[test]
    fn widen_int32_to_int64() {
        let src = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let b = batch(src, vec![Arc::new(Int32Array::from(vec![7]))]);
        let target = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let out = SchemaNormalizeOperator::new(target).normalize(&b).unwrap();
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn drop_extra_column() {
        let src = Arc::new(Schema::new(vec![
            Field::new("keep", DataType::Utf8, true),
            Field::new("drop_me", DataType::Utf8, true),
        ]));
        let b = batch(
            src,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(StringArray::from(vec![Some("b")])),
            ],
        );
        let target = Arc::new(Schema::new(vec![Field::new("keep", DataType::Utf8, true)]));
        let out = SchemaNormalizeOperator::new(target).normalize(&b).unwrap();
        assert_eq!(out.num_columns(), 1);
    }

    #[test]
    fn rename_via_map() {
        let src = Arc::new(Schema::new(vec![Field::new("old", DataType::Utf8, true)]));
        let b = batch(src, vec![Arc::new(StringArray::from(vec![Some("v")]))]);
        let target = Arc::new(Schema::new(vec![Field::new("new", DataType::Utf8, true)]));
        let op = SchemaNormalizeOperator::new(target)
            .with_renames(ColumnRenameMap::new([("new".into(), "old".into())]));
        let out = op.normalize(&b).unwrap();
        assert_eq!(out.schema().field(0).name(), "new");
    }

    #[test]
    fn narrowing_rejected() {
        let src = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let b = batch(src, vec![Arc::new(Int64Array::from(vec![1_i64]))]);
        let target = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let err = SchemaNormalizeOperator::new(target)
            .normalize(&b)
            .unwrap_err();
        assert!(matches!(err, ExecError::IncompatibleSchemaEvolution(_)));
    }
}
