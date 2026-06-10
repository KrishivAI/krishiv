//! Schema normalization helpers for connector CDC paths.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{ConnectorError, ConnectorResult};

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

/// Arrow-level schema normalizer for connector CDC pipelines.
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

    fn source_column<'a>(&'a self, target_field: &'a str) -> &'a str {
        self.renames
            .target_to_source
            .get(target_field)
            .map(String::as_str)
            .unwrap_or(target_field)
    }

    pub fn normalize(&self, batch: &RecordBatch) -> ConnectorResult<RecordBatch> {
        let source_schema = batch.schema();
        let mut source_indices = HashMap::with_capacity(source_schema.fields().len());
        for (index, field) in source_schema.fields().iter().enumerate() {
            if source_indices
                .insert(field.name().as_str(), index)
                .is_some()
            {
                return Err(ConnectorError::Schema {
                    message: format!("source schema contains duplicate column {}", field.name()),
                });
            }
        }
        if source_schema == self.target {
            return Ok(batch.clone());
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.target.fields().len());
        for field in self.target.fields() {
            let lookup = self.source_column(field.name());
            let value = if let Some(index) = source_indices.get(lookup) {
                let col = batch.column(*index);
                Self::cast_column(col, field)?
            } else if !field.is_nullable() {
                return Err(ConnectorError::Schema {
                    message: format!("missing non-nullable column {}", field.name()),
                });
            } else {
                arrow::array::new_null_array(field.data_type(), batch.num_rows())
            };
            columns.push(value);
        }
        RecordBatch::try_new(self.target.clone(), columns).map_err(|e| ConnectorError::Schema {
            message: e.to_string(),
        })
    }

    fn cast_column(col: &ArrayRef, target_field: &Field) -> ConnectorResult<ArrayRef> {
        if col.data_type() == target_field.data_type() {
            return Ok(col.clone());
        }
        if col.data_type() == &DataType::Null && target_field.is_nullable() {
            return Ok(arrow::array::new_null_array(
                target_field.data_type(),
                col.len(),
            ));
        }
        if Self::is_widen(col.data_type(), target_field.data_type()) {
            return cast(col, target_field.data_type()).map_err(|e| ConnectorError::Schema {
                message: e.to_string(),
            });
        }
        Err(ConnectorError::Schema {
            message: format!(
                "cannot cast {:?} to {:?} for column {}",
                col.data_type(),
                target_field.data_type(),
                target_field.name()
            ),
        })
    }

    fn is_widen(from: &DataType, to: &DataType) -> bool {
        from != to && Self::widening_target(from, to).as_ref() == Some(to)
    }

    pub fn widening_target(left: &DataType, right: &DataType) -> Option<DataType> {
        if left == right {
            return Some(left.clone());
        }
        match (left, right) {
            (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => {
                Some(DataType::Int16)
            }
            (DataType::Int8 | DataType::Int16, DataType::Int32)
            | (DataType::Int32, DataType::Int8 | DataType::Int16) => Some(DataType::Int32),
            (DataType::Int8 | DataType::Int16 | DataType::Int32, DataType::Int64)
            | (DataType::Int64, DataType::Int8 | DataType::Int16 | DataType::Int32) => {
                Some(DataType::Int64)
            }
            (DataType::Float32, DataType::Float64) | (DataType::Float64, DataType::Float32) => {
                Some(DataType::Float64)
            }
            _ => None,
        }
    }
}
