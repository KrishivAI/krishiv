use std::fmt;

use arrow::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Typed group-by / join key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggKey {
    Int32(i32),
    Int64(i64),
    /// Unsigned integer keys.
    ///
    /// Absent until the NEXMark harness hit it: ids and prices arrive as
    /// `UInt64` from realistic sources, and a `UInt64` grouping key failed with
    /// "unsupported group key type". Widened rather than cast to `Int64` so a
    /// key above `i64::MAX` cannot silently alias onto a negative one.
    UInt64(u64),
    /// `f64` stored as IEEE-754 bits for total-order hashing.
    Float64(u64),
    Utf8(String),
    Bool(bool),
}

impl fmt::Display for AggKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int32(v) => write!(f, "{v}"),
            Self::UInt64(v) => write!(f, "{v}"),
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
            (Self::UInt64(a), Self::UInt64(b)) => a.cmp(b),
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
            Self::UInt64(_) => 5,
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
        // DataFusion 54 emits `Utf8View` as the default representation for
        // string columns (e.g. `CAST(x AS VARCHAR)`), and can also produce
        // `LargeUtf8`; both are the same logical string key.
        DataType::Utf8View => {
            let arr = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared Utf8View key failed downcast".into())
                })?;
            Ok(AggKey::Utf8(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared LargeUtf8 key failed downcast".into())
                })?;
            Ok(AggKey::Utf8(arr.value(row).to_string()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ExecError::UnsupportedType("declared Bool key failed downcast".into())
            })?;
            Ok(AggKey::Bool(arr.value(row)))
        }
        // Direct per-width downcasts, NOT `arrow::compute::cast` to UInt64.
        // This function runs once PER ROW; the cast kernel allocates a fresh
        // array wrapper on every call even on its same-type fast path, and the
        // pinned NEXMark A/B measured that overhead at 2-2.5x whole-query
        // throughput on UInt64-keyed tumbling windows (q7: 4.1M vs 10.4M
        // ev/sec). Widening `as u64` is lossless for every unsigned width.
        DataType::UInt8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared UInt8 key failed downcast".into())
                })?;
            Ok(AggKey::UInt64(u64::from(arr.value(row))))
        }
        DataType::UInt16 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared UInt16 key failed downcast".into())
                })?;
            Ok(AggKey::UInt64(u64::from(arr.value(row))))
        }
        DataType::UInt32 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared UInt32 key failed downcast".into())
                })?;
            Ok(AggKey::UInt64(u64::from(arr.value(row))))
        }
        DataType::UInt64 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| {
                    ExecError::UnsupportedType("declared UInt64 key failed downcast".into())
                })?;
            Ok(AggKey::UInt64(arr.value(row)))
        }
        other => Err(ExecError::UnsupportedType(format!(
            "unsupported group key type: {other}"
        ))),
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

    /// Unsigned grouping keys work, and a key above `i64::MAX` keeps its
    /// identity.
    ///
    /// The NEXMark harness hit "unsupported group key type: UInt64" grouping
    /// bids by `auction`. The obvious repair — cast unsigned to `Int64` — is
    /// what this test rejects: `u64::MAX` and `u64::MAX - 1` both map to
    /// negative `i64`s, and a *distinct* key pair must stay distinct. Hence the
    /// dedicated `AggKey::UInt64` variant.
    #[test]
    fn extract_agg_key_supports_unsigned_keys_without_aliasing() {
        use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let wide = Arc::new(UInt64Array::from(vec![7_u64, u64::MAX, u64::MAX - 1])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "auction",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![wide]).unwrap();

        let small = extract_agg_key(&batch, 0, 0).expect("UInt64 key must extract");
        assert_eq!(small, AggKey::UInt64(7));

        // Above i64::MAX, and distinct from its neighbour. Casting to i64 would
        // make both negative; casting to f64 would make them EQUAL (neither is
        // representable, both round to 2^64), silently merging two groups.
        let top = extract_agg_key(&batch, 0, 1).expect("u64::MAX key must extract");
        let below = extract_agg_key(&batch, 0, 2).expect("u64::MAX-1 key must extract");
        assert_eq!(top, AggKey::UInt64(u64::MAX));
        assert_ne!(
            top, below,
            "distinct u64 keys must not alias onto one group"
        );

        // Every narrow unsigned width widens into the same variant, so
        // UInt8(7)/UInt16(7)/UInt32(7)/UInt64(7) land in one group rather than
        // four. Each width is its own match arm since the per-row cast kernel
        // was removed (it cost 2-2.5x whole-query throughput), so each arm
        // needs its own row here — a broken UInt16 arm must not hide behind a
        // passing UInt32 case.
        use arrow::array::{UInt8Array, UInt16Array};
        let narrow_cases: [(DataType, ArrayRef); 3] = [
            (DataType::UInt8, Arc::new(UInt8Array::from(vec![7_u8]))),
            (DataType::UInt16, Arc::new(UInt16Array::from(vec![7_u16]))),
            (DataType::UInt32, Arc::new(UInt32Array::from(vec![7_u32]))),
        ];
        for (dt, arr) in narrow_cases {
            let narrow_schema =
                Arc::new(Schema::new(vec![Field::new("auction", dt.clone(), false)]));
            let narrow_batch = RecordBatch::try_new(narrow_schema, vec![arr]).unwrap();
            assert_eq!(
                extract_agg_key(&narrow_batch, 0, 0)
                    .unwrap_or_else(|e| panic!("{dt} key must extract: {e}")),
                AggKey::UInt64(7),
                "{dt} must widen into the same key variant"
            );
        }
    }

    #[test]
    fn extract_agg_key_supports_all_string_representations() {
        use arrow::array::{LargeStringArray, StringArray, StringViewArray};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        // Utf8, Utf8View (DataFusion 54's default for CAST(.. AS VARCHAR)), and
        // LargeUtf8 all yield the same logical AggKey::Utf8 — the bounded-window
        // group-by must accept every string representation the planner emits.
        for (dt, arr) in [
            (
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["grp-3"])) as arrow::array::ArrayRef,
            ),
            (
                DataType::Utf8View,
                Arc::new(StringViewArray::from(vec!["grp-3"])) as arrow::array::ArrayRef,
            ),
            (
                DataType::LargeUtf8,
                Arc::new(LargeStringArray::from(vec!["grp-3"])) as arrow::array::ArrayRef,
            ),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("grp", dt.clone(), false)]));
            let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
            let key = extract_agg_key(&batch, 0, 0)
                .unwrap_or_else(|e| panic!("{dt} key must extract: {e}"));
            assert_eq!(key, AggKey::Utf8("grp-3".to_string()), "for {dt}");
        }
    }
}
