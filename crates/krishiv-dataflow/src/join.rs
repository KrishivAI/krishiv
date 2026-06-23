use std::fmt;

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Typed group-by / join key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggKey {
    Int32(i32),
    Int64(i64),
    /// `f64` stored as IEEE-754 bits for total-order hashing.
    Float64(u64),
    Utf8(String),
    Bool(bool),
}

impl fmt::Display for AggKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float64(bits) => write!(f, "{}", f64::from_bits(*bits)),
            Self::Utf8(s) => f.write_str(s),
            Self::Bool(v) => write!(f, "{v}"),
        }
    }
}

impl AggKey {
    pub(crate) fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int32(a), Self::Int32(b)) => a.cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.cmp(b),
            (Self::Utf8(a), Self::Utf8(b)) => a.cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (a, b) => a.discriminant().cmp(&b.discriminant()),
        }
    }

    fn discriminant(&self) -> u8 {
        match self {
            Self::Int32(_) => 0,
            Self::Int64(_) => 1,
            Self::Float64(_) => 2,
            Self::Utf8(_) => 3,
            Self::Bool(_) => 4,
        }
    }
}

/// Extract a typed [`AggKey`] from one column at `row`.
///
/// Supported types: `Int32`, `Int64`, `Float64`, `Utf8`, `Bool`.
/// Avoids heap allocation for integer and boolean keys.
pub fn extract_agg_key(batch: &RecordBatch, col_idx: usize, row: usize) -> ExecResult<AggKey> {
    if col_idx >= batch.num_columns() {
        return Err(ExecError::InvalidInput(format!(
            "group key column index {col_idx} is out of bounds for {} columns",
            batch.num_columns()
        )));
    }
    if row >= batch.num_rows() {
        return Err(ExecError::InvalidInput(format!(
            "group key row index {row} is out of bounds for {} rows",
            batch.num_rows()
        )));
    }

    let col = batch.column(col_idx);
    if col.is_null(row) {
        return Err(ExecError::InvalidInput(format!(
            "group key column '{}' contains null at row {row}",
            batch.schema().field(col_idx).name()
        )));
    }

    match col.data_type() {
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int32 key failed downcast".into())
            })?;
            Ok(AggKey::Int32(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Int64 key failed downcast".into())
            })?;
            Ok(AggKey::Int64(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Float64 key failed downcast".into())
            })?;
            Ok(AggKey::Float64(arr.value(row).to_bits()))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Utf8 key failed downcast".into())
            })?;
            Ok(AggKey::Utf8(arr.value(row).to_string()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Bool key failed downcast".into())
            })?;
            Ok(AggKey::Bool(arr.value(row)))
        }
        other => Err(ExecError::UnsupportedType(format!(
            "unsupported group key type: {other}"
        ))),
    }
}

// ── CompositeKey ────────────────────────────────────────────────────────────

/// Composite multi-key for use with join operators.
/// Placeholder for future multi-key join support.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey(Vec<AggKey>);

impl std::fmt::Display for CompositeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self.0.iter().map(|k| k.to_string()).collect();
        write!(f, "{}", parts.join("|"))
    }
}

impl CompositeKey {
    pub fn new(keys: Vec<AggKey>) -> Self {
        Self(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_agg_key_rejects_null_values() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();

        let err = extract_agg_key(&batch, 0, 1).unwrap_err();
        assert!(matches!(err, ExecError::InvalidInput(_)));
        assert!(err.to_string().contains("contains null at row 1"));
    }

    #[test]
    fn extract_agg_key_rejects_out_of_bounds_indices() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();

        let column_err = extract_agg_key(&batch, 1, 0).unwrap_err();
        assert!(matches!(column_err, ExecError::InvalidInput(_)));
        assert!(column_err.to_string().contains("column index 1"));

        let row_err = extract_agg_key(&batch, 0, 1).unwrap_err();
        assert!(matches!(row_err, ExecError::InvalidInput(_)));
        assert!(row_err.to_string().contains("row index 1"));
    }
}
