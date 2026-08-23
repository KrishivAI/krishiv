#![forbid(unsafe_code)]

//! Shared scalar-to-string/key helpers for incremental operators.
//!
//! These helpers replace per-operator duplicated `scalar_to_string` copies that
//! had inconsistent type coverage (some handled only Int/Float/String, dropping
//! Boolean/Date/Timestamp/Decimal to a constant placeholder — silently
//! corrupting consolidation, DISTINCT, dedup, and provenance hashing).
//!
//! **The contract these helpers must satisfy is injectivity.** Every caller —
//! Z-set consolidation, DISTINCT, join-key probing, dedup hashing, trace
//! keying — compares the returned strings for *equality* and cancels or
//! matches rows on that basis. Two distinct values that encode to the same
//! string do not merely degrade performance: they annihilate each other's
//! weights and produce a silently wrong view.
//!
//! IVM-AUD-1: the previous fallback for types without an explicit branch was
//! `format!("<{:?}>", arr.data_type())` — the *type name*, identical for every
//! row. Its comment claimed this "avoids silent collisions"; it was itself the
//! collision, and it covered Decimal (`SUM(price)` over a decimal column) and
//! Dictionary (what modern DataFusion emits for string columns) among others.
//! A `+1` on one price cancelled a `-1` on a different price. Every type now
//! has a value-faithful encoding, and anything Arrow itself cannot render is a
//! hard error rather than a collision — a key that cannot be computed is a
//! failure, never a guess.
//!
//! Two null variants exist because callers need different semantics:
//! - [`scalar_to_string`] returns `"NULL"` for nulls (sentinel-based callers).
//! - [`scalar_to_key`] returns `None` for nulls (Option-based callers:
//!   aggregate group keys, join key extraction — where `None` represents a
//!   SQL null group member). `None` is reserved *strictly* for SQL nulls; an
//!   unencodable type is `Err`, so it can never be mistaken for a null group.
//!
//! Float types use their **bit representation** in [`scalar_to_key`] (not
//! `to_string`) for a stable, injective key: `to_string` is not injective
//! across NaN variants and may not reliably distinguish denormals. In
//! [`scalar_to_string`] floats use the shortest-round-trip decimal format
//! (Ryu), which is injective across distinct finite values and deliberately
//! folds NaN variants together — matching SQL/DataFusion GROUP BY semantics.
//!
//! Follow-up (recorded, not a correctness gap): whole-row keying could use
//! Arrow's `RowConverter` — the canonical columnar row encoding already used
//! by `differentiate` — which would be both faster and structurally immune to
//! this defect class. That is a refactor of the four call sites, tracked in
//! `docs/implementation/ivm-audit-register.md`.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};

use crate::{DeltaError, DeltaResult};

/// Whether float values encode as bit patterns (injective across NaN variants)
/// or as shortest-round-trip decimals (folds NaN variants, matching GROUP BY).
#[derive(Clone, Copy, PartialEq, Eq)]
enum FloatEncoding {
    Bits,
    Decimal,
}

/// Stringify a scalar for use as a group-key or hash component.
///
/// Returns `"NULL"` for SQL nulls. Callers for SUM/AVG/MIN/MAX must check for
/// this sentinel and skip the row (SQL excludes nulls from these aggregates).
///
/// Errors when the value's Arrow type has no faithful encoding — never returns
/// a placeholder that would collide with other values of the same type.
pub fn scalar_to_string(arr: &dyn Array, row: usize) -> DeltaResult<String> {
    if arr.is_null(row) {
        return Ok("NULL".to_string());
    }
    encode_value(arr, row, FloatEncoding::Decimal)
}

/// Stringify a scalar for use as an equality/group key.
///
/// Returns `Ok(None)` for SQL nulls (they hash together as a single null
/// group) and `Err` for a type with no faithful encoding — the two cases are
/// deliberately distinct, so an unencodable type can never be silently
/// grouped as "null" (IVM-AUD-1).
pub fn scalar_to_key(arr: &dyn Array, row: usize) -> DeltaResult<Option<String>> {
    if arr.is_null(row) {
        return Ok(None);
    }
    encode_value(arr, row, FloatEncoding::Bits).map(Some)
}

/// Null-unambiguous group-key component for equality/hashing callers.
///
/// `scalar_to_string` returns the sentinel `"NULL"` for SQL nulls, which a
/// Utf8 value `"NULL"` collides with — consolidation would cancel a real
/// `"NULL"` string against a SQL null (crate-13 audit). This variant prefixes
/// every real value with `'v'` and encodes null as `"n"`, so the two can
/// never produce the same key component.
pub fn scalar_to_group_key(arr: &dyn Array, row: usize) -> DeltaResult<String> {
    if arr.is_null(row) {
        return Ok("n".to_string());
    }
    let encoded = encode_value(arr, row, FloatEncoding::Decimal)?;
    let mut s = String::with_capacity(encoded.len() + 1);
    s.push('v');
    s.push_str(&encoded);
    Ok(s)
}

/// The single value-faithful encoder behind all three public helpers.
///
/// Ordering is by expected frequency: the primitive/string fast paths first,
/// then the previously-missing types, then Arrow's own display for the
/// genuinely rare nested shapes, then a hard error.
fn encode_value(arr: &dyn Array, row: usize, floats: FloatEncoding) -> DeltaResult<String> {
    // Signed integers
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return Ok(a.value(row).to_string());
    }
    // Unsigned integers
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return Ok(a.value(row).to_string());
    }
    // Floats
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let v = a.value(row);
        return Ok(match floats {
            FloatEncoding::Bits => v.to_bits().to_string(),
            FloatEncoding::Decimal => v.to_string(),
        });
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        let v = a.value(row);
        return Ok(match floats {
            FloatEncoding::Bits => (v.to_bits() as u64).to_string(),
            FloatEncoding::Decimal => v.to_string(),
        });
    }
    // String types
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return Ok(a.value(row).to_string());
    }
    // Boolean
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok((a.value(row) as u8).to_string());
    }
    // Date / Timestamp — raw integer epoch ticks (the unit is fixed by the
    // column's DataType, so the raw value is injective within a column).
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    // Decimal — IVM-AUD-1's headline victim: `SUM(price)` over a decimal
    // column. The raw i128/i256 mantissa is injective because precision and
    // scale are fixed by the column's DataType.
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal256Array>() {
        return Ok(a.value(row).to_string());
    }
    // Time / Duration / Interval — raw values; units are fixed per column.
    if let Some(a) = arr.as_any().downcast_ref::<Time32SecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time32MillisecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time64MicrosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time64NanosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationSecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationMillisecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationMicrosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<DurationNanosecondArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<IntervalYearMonthArray>() {
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<IntervalDayTimeArray>() {
        let v = a.value(row);
        return Ok(format!("{}d{}ms", v.days, v.milliseconds));
    }
    if let Some(a) = arr.as_any().downcast_ref::<IntervalMonthDayNanoArray>() {
        let v = a.value(row);
        return Ok(format!("{}m{}d{}ns", v.months, v.days, v.nanoseconds));
    }
    // Binary types
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(hex_encode(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(hex_encode(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(hex_encode(a.value(row)));
    }
    // Dictionary — what DataFusion emits for repeated string columns. Resolve
    // to the underlying value so a dictionary-encoded "apple" keys the same as
    // a plain Utf8 "apple" (semantically the same value).
    if let Some(idx) = dictionary_value_index(arr, row) {
        let values = dictionary_values(arr).ok_or_else(|| {
            DeltaError::Operator(format!(
                "dictionary array with unresolvable values for key encoding: {:?}",
                arr.data_type()
            ))
        })?;
        return encode_value(values.as_ref(), idx, floats);
    }
    // Everything else (List, LargeList, FixedSizeList, Struct, Map, Union,
    // RunEndEncoded, …): Arrow's own display is value-faithful for these.
    // Rare enough that constructing a formatter per cell is acceptable; if it
    // ever becomes hot, the RowConverter refactor noted at the top is the fix.
    let options = arrow::util::display::FormatOptions::default();
    match arrow::util::display::ArrayFormatter::try_new(arr, &options) {
        Ok(formatter) => Ok(formatter.value(row).to_string()),
        // Fail closed. A key that cannot be computed is an error, never a
        // placeholder — the placeholder is precisely what IVM-AUD-1 was.
        Err(e) => Err(DeltaError::Operator(format!(
            "no injective key encoding for Arrow type {:?}: {e}; \
             incremental maintenance cannot group or cancel rows of this type",
            arr.data_type()
        ))),
    }
}

/// Resolve a dictionary array's key at `row` to an index into its values.
///
/// Returns `None` when `arr` is not a dictionary (the caller then falls
/// through to the remaining branches).
fn dictionary_value_index(arr: &dyn Array, row: usize) -> Option<usize> {
    macro_rules! try_dict {
        ($($kt:ty),+ $(,)?) => {
            $(
                if let Some(d) = arr.as_any().downcast_ref::<DictionaryArray<$kt>>() {
                    let k = d.keys().value(row);
                    return usize::try_from(k).ok();
                }
            )+
        };
    }
    try_dict!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    );
    None
}

/// The values array behind a dictionary array, for any supported key type.
fn dictionary_values(arr: &dyn Array) -> Option<arrow::array::ArrayRef> {
    macro_rules! try_values {
        ($($kt:ty),+ $(,)?) => {
            $(
                if let Some(d) = arr.as_any().downcast_ref::<DictionaryArray<$kt>>() {
                    return Some(d.values().clone());
                }
            )+
        };
    }
    try_values!(
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    );
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
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMillisecondArray, UInt32Array,
    };
    use arrow::datatypes::TimeUnit;

    #[test]
    fn nulls_return_null_sentinel() {
        let arr = Int64Array::from(vec![None]);
        assert_eq!(scalar_to_string(&arr, 0).unwrap(), "NULL");
        assert_eq!(scalar_to_key(&arr, 0).unwrap(), None);
        assert_eq!(scalar_to_group_key(&arr, 0).unwrap(), "n");
    }

    #[test]
    fn boolean_round_trip() {
        let arr = BooleanArray::from(vec![Some(true), Some(false)]);
        assert_eq!(scalar_to_string(&arr, 0).unwrap(), "1");
        assert_eq!(scalar_to_string(&arr, 1).unwrap(), "0");
        assert_eq!(scalar_to_key(&arr, 0).unwrap(), Some("1".into()));
    }

    #[test]
    fn timestamp_round_trip() {
        let arr = TimestampMillisecondArray::from(vec![Some(1000), Some(2000)]);
        assert_eq!(scalar_to_string(&arr, 0).unwrap(), "1000");
        assert_eq!(scalar_to_string(&arr, 1).unwrap(), "2000");
        assert_eq!(scalar_to_key(&arr, 0).unwrap(), Some("1000".into()));
        let _ = TimeUnit::Second;
    }

    #[test]
    fn date32_round_trip() {
        let arr = Date32Array::from(vec![Some(42)]);
        assert_eq!(scalar_to_string(&arr, 0).unwrap(), "42");
        assert_eq!(scalar_to_key(&arr, 0).unwrap(), Some("42".into()));
    }

    #[test]
    fn uint_types_round_trip() {
        let arr = UInt32Array::from(vec![Some(99)]);
        assert_eq!(scalar_to_string(&arr, 0).unwrap(), "99");
        assert_eq!(scalar_to_key(&arr, 0).unwrap(), Some("99".into()));
    }

    #[test]
    fn float_key_uses_bit_repr() {
        let nan1 = f64::NAN;
        let nan2 = f64::from_bits(f64::NAN.to_bits() | 1);
        let arr = Float64Array::from(vec![Some(nan1), Some(nan2)]);
        let k0 = scalar_to_key(&arr, 0).unwrap().unwrap();
        let k1 = scalar_to_key(&arr, 1).unwrap().unwrap();
        assert_ne!(
            k0, k1,
            "distinct NaN bit patterns must produce distinct keys"
        );
    }

    #[test]
    fn distinct_values_produce_distinct_strings() {
        let arr = Int64Array::from(vec![Some(1), Some(2)]);
        assert_ne!(
            scalar_to_string(&arr, 0).unwrap(),
            scalar_to_string(&arr, 1).unwrap()
        );
    }

    #[test]
    fn int32_and_int64_dont_silently_collide() {
        let a32 = Int32Array::from(vec![Some(42)]);
        let a64 = Int64Array::from(vec![Some(42)]);
        assert_eq!(
            scalar_to_string(&a32, 0).unwrap(),
            scalar_to_string(&a64, 0).unwrap()
        );
    }

    /// IVM-AUD-1 regression. Revert-proof: restore the
    /// `format!("<{:?}>", arr.data_type())` fallback and both assertions fail
    /// — every decimal in the column encodes to `<Decimal128(10, 2)>`, so a
    /// `+1` on 10.00 cancels a `-1` on 99.99 during consolidation.
    #[test]
    fn decimal_values_do_not_collapse_to_one_key() {
        let arr = Decimal128Array::from(vec![Some(1000_i128), Some(9999_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let k0 = scalar_to_group_key(&arr, 0).unwrap();
        let k1 = scalar_to_group_key(&arr, 1).unwrap();
        assert_ne!(k0, k1, "distinct decimals must produce distinct keys");
        assert!(
            !k0.contains("Decimal128"),
            "the key must encode the VALUE, not the type name: {k0}"
        );
    }

    /// IVM-AUD-1 regression for the type DataFusion actually emits for
    /// repeated strings. Revert-proof in the same way as the decimal case.
    #[test]
    fn dictionary_values_do_not_collapse_and_match_plain_utf8() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let dict: DictionaryArray<Int32Type> = vec![Some("apple"), Some("banana"), Some("apple")]
            .into_iter()
            .collect();
        let k0 = scalar_to_group_key(&dict, 0).unwrap();
        let k1 = scalar_to_group_key(&dict, 1).unwrap();
        let k2 = scalar_to_group_key(&dict, 2).unwrap();
        assert_ne!(k0, k1, "distinct dictionary values must differ");
        assert_eq!(k0, k2, "equal dictionary values must match");
        // A dictionary-encoded value and the same plain Utf8 value are the
        // same value and must key identically.
        let plain = StringArray::from(vec![Some("apple")]);
        assert_eq!(k0, scalar_to_group_key(&plain, 0).unwrap());
    }

    /// Nested types encode by value through Arrow's own display rather than
    /// collapsing to the type name.
    #[test]
    fn list_values_do_not_collapse_to_one_key() {
        use arrow::array::ListArray;
        use arrow::datatypes::Int32Type;
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        let k0 = scalar_to_group_key(&list, 0).unwrap();
        let k1 = scalar_to_group_key(&list, 1).unwrap();
        assert_ne!(k0, k1, "distinct lists must produce distinct keys");
    }
}
