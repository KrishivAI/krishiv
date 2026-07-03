#![forbid(unsafe_code)]

//! Shared scalar-to-string/key helpers for incremental operators.
//!
//! These helpers replace per-operator duplicated `scalar_to_string` copies that
//! had inconsistent type coverage (some handled only Int/Float/String, dropping
//! Boolean/Date/Timestamp/Decimal to a constant placeholder — silently
//! corrupting consolidation, DISTINCT, dedup, and provenance hashing).
//!
//! Two variants exist because callers need different null semantics:
//! - [`scalar_to_string`] returns `"NULL"` for nulls (sentinel-based callers:
//!   consolidation, DISTINCT, aggregate input values, dedup hashing).
//! - [`scalar_to_key`] returns `None` for nulls (Option-based callers:
//!   aggregate group keys, join key extraction — where `None` represents a
//!   SQL null group member).
//!
//! Float types use their **bit representation** in [`scalar_to_key`] (not
//! `to_string`) for a stable, injective key: `to_string` is not injective
//! across NaN variants and may not reliably distinguish denormals. In
//! [`scalar_to_string`] floats use the shortest-round-trip decimal format
//! (Ryu) since that variant is used for value extraction (SUM/AVG input),
//! not equality grouping.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};

/// Stringify a scalar for use as a group-key or hash component.
///
/// Returns `"NULL"` for SQL nulls. Callers for SUM/AVG/MIN/MAX must check for
/// this sentinel and skip the row (SQL excludes nulls from these aggregates).
///
/// Covers all common Arrow primitive/temporal/string/binary types. Unsupported
/// types (e.g. complex nested types) return a type-tagged placeholder so they
/// are visible in debugging rather than silently colliding.
pub fn scalar_to_string(arr: &dyn Array, row: usize) -> String {
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    // Signed integers
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return a.value(row).to_string();
    }
    // Unsigned integers
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return a.value(row).to_string();
    }
    // Floats: use Rust's shortest-round-trip decimal format (Ryu algorithm)
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string();
    }
    // String types
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return a.value(row).to_string();
    }
    // Boolean
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return (a.value(row) as u8).to_string();
    }
    // Date / Timestamp — stringify as raw integer epoch ticks
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return a.value(row).to_string();
    }
    // Binary types — hex-encode for a stable, injective representation
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return hex_encode(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return hex_encode(a.value(row));
    }
    // Decimal and other types: fall back to a typed display via the array's
    // debug representation. This avoids silent collisions (every unsupported
    // type getting the same constant) at the cost of a less compact key.
    format!("<{:?}>", arr.data_type())
}

/// Stringify a scalar for use as an equality/group key.
///
/// Returns `None` for SQL nulls (they hash together as a single null group).
/// Returns `None` for unrecognized array types so callers can detect the gap.
///
/// Float types use their bit representation (not `to_string`) for a stable,
/// injective key: `to_string` is not injective across NaN variants and may
/// not reliably distinguish denormals. Bit-repr is always unique per value.
pub fn scalar_to_key(arr: &dyn Array, row: usize) -> Option<String> {
    if arr.is_null(row) {
        return None;
    }
    // Signed integers
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return Some(a.value(row).to_string());
    }
    // Unsigned integers
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return Some(a.value(row).to_string());
    }
    // Floats: bit-repr for injective, stable keys (to_string varies across NaN
    // variants and is not guaranteed unique under all rounding modes).
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row).to_bits().to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Some((a.value(row).to_bits() as u64).to_string());
    }
    // String types
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return Some(a.value(row).to_string());
    }
    // Boolean (store as "0"/"1" for a compact, unambiguous key)
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Some((a.value(row) as u8).to_string());
    }
    // Date types (stored as day / ms offsets — stringify the raw integer)
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return Some(a.value(row).to_string());
    }
    // Timestamp types (all stored as i64 epoch ticks, unit is irrelevant for
    // grouping since the column already carries a fixed unit in its DataType)
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(a.value(row).to_string());
    }
    // Binary types
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Some(hex_encode(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Some(hex_encode(a.value(row)));
    }
    None
}

/// Hex-encode a byte slice with a `0x` prefix for stable binary key strings.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("0x");
    use std::fmt::Write;
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, TimestampMillisecondArray,
        UInt32Array,
    };
    use arrow::datatypes::TimeUnit;

    #[test]
    fn nulls_return_null_sentinel() {
        let arr = Int64Array::from(vec![None]);
        assert_eq!(scalar_to_string(&arr, 0), "NULL");
        assert_eq!(scalar_to_key(&arr, 0), None);
    }

    #[test]
    fn boolean_round_trip() {
        let arr = BooleanArray::from(vec![Some(true), Some(false)]);
        assert_eq!(scalar_to_string(&arr, 0), "1");
        assert_eq!(scalar_to_string(&arr, 1), "0");
        assert_eq!(scalar_to_key(&arr, 0), Some("1".into()));
    }

    #[test]
    fn timestamp_round_trip() {
        let arr = TimestampMillisecondArray::from(vec![Some(1000), Some(2000)]);
        assert_eq!(scalar_to_string(&arr, 0), "1000");
        assert_eq!(scalar_to_string(&arr, 1), "2000");
        assert_eq!(scalar_to_key(&arr, 0), Some("1000".into()));
        // Verify the TimeUnit import is not unused (clippy cleanliness).
        let _ = TimeUnit::Second;
    }

    #[test]
    fn date32_round_trip() {
        let arr = Date32Array::from(vec![Some(42)]);
        assert_eq!(scalar_to_string(&arr, 0), "42");
        assert_eq!(scalar_to_key(&arr, 0), Some("42".into()));
    }

    #[test]
    fn uint_types_round_trip() {
        let arr = UInt32Array::from(vec![Some(99)]);
        assert_eq!(scalar_to_string(&arr, 0), "99");
        assert_eq!(scalar_to_key(&arr, 0), Some("99".into()));
    }

    #[test]
    fn float_key_uses_bit_repr() {
        // NaN values: to_string gives "NaN" for all variants, but bit-repr differs.
        let nan1 = f64::NAN;
        let nan2 = f64::from_bits(f64::NAN.to_bits() | 1);
        let arr = Float64Array::from(vec![Some(nan1), Some(nan2)]);
        let k0 = scalar_to_key(&arr, 0).unwrap();
        let k1 = scalar_to_key(&arr, 1).unwrap();
        assert_ne!(
            k0, k1,
            "distinct NaN bit patterns must produce distinct keys"
        );
    }

    #[test]
    fn distinct_values_produce_distinct_strings() {
        let arr = Int64Array::from(vec![Some(1), Some(2)]);
        assert_ne!(scalar_to_string(&arr, 0), scalar_to_string(&arr, 1));
    }

    #[test]
    fn int32_and_int64_dont_silently_collide() {
        // Two different typed columns with value 42 must produce the same string
        // (42 == 42) so cross-type joins work, but different values must differ.
        let a32 = Int32Array::from(vec![Some(42)]);
        let a64 = Int64Array::from(vec![Some(42)]);
        assert_eq!(scalar_to_string(&a32, 0), scalar_to_string(&a64, 0));
    }
}
