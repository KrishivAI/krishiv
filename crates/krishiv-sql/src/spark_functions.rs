#![forbid(unsafe_code)]
//! Spark-reference scalar functions with **exact** semantics (Phase 60).
//!
//! DataFusion already ships exact equivalents for several Spark aliases —
//! `nvl`, `nvl2`, `substring_index` — so those are matrix-only (no code here).
//! This module fills the two gaps where DataFusion's function is *present but
//! semantically different* or *absent*, honouring the phase's exact-or-absent
//! rule:
//!
//! - **`date_format(ts, fmt)`** — DataFusion aliases `date_format` onto
//!   `to_char`, which interprets **chrono/strftime** patterns (`%Y-%m-%d`).
//!   Spark uses Java `DateTimeFormatter` **pattern letters** (`yyyy-MM-dd`), so
//!   `date_format(ts, 'yyyy-MM-dd')` silently emits the literal text
//!   `yyyy-MM-dd` under DataFusion. We register a Spark-pattern `date_format`
//!   that translates the supported pattern letters to chrono and **errors
//!   clearly on unsupported letters** rather than producing wrong output.
//! - **`crc32(expr)`** — absent in DataFusion; the IEEE CRC-32 of the input's
//!   UTF-8 bytes as a `BIGINT`, matching Spark.
//!
//! The remaining Spark hash/generator functions (`xxhash64`, `stack`,
//! `posexplode`, `inline`) require either byte-exact replication of Spark's
//! typed hashing (seed 42) or table-generating/`LATERAL VIEW` machinery and are
//! tracked as `Planned` in the feature matrix — not approximated here.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Builder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::DateTime;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::prelude::SessionContext;

/// Register the Spark-parity scalar UDFs (`date_format`, `crc32`).
pub fn register_spark_scalar_functions(ctx: &SessionContext) -> Result<(), DataFusionError> {
    ctx.register_udf(make_date_format());
    ctx.register_udf(make_crc32());
    Ok(())
}

// ── date_format (Spark pattern letters) ─────────────────────────────────────

fn make_date_format() -> ScalarUDF {
    create_udf(
        "date_format",
        // Declaring the temporal input as Timestamp(ns) lets DataFusion coerce
        // Date/Timestamp/other-precision inputs to a single concrete type; a
        // Date coerces to midnight, matching Spark.
        vec![
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let [ts_arr, fmt_arr] = arrays.as_slice() else {
                return Err(DataFusionError::Internal(
                    "date_format: expected 2 arguments".into(),
                ));
            };
            let ts = ts_arr
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_format: arg 0 must be Timestamp(ns)".into())
                })?;
            let fmt = fmt_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_format: arg 1 (format) must be Utf8".into())
                })?;
            let mut out = StringBuilder::new();
            for i in 0..ts.len() {
                if ts.is_null(i) || fmt.is_null(i) {
                    out.append_null();
                    continue;
                }
                let chrono_fmt = spark_pattern_to_chrono(fmt.value(i))
                    .map_err(DataFusionError::Execution)?;
                let dt = DateTime::from_timestamp_nanos(ts.value(i)).naive_utc();
                out.append_value(dt.format(&chrono_fmt).to_string());
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

/// Translate a Spark/Java `DateTimeFormatter` pattern to a chrono `strftime`
/// pattern. Supported letters are translated **exactly**; an unsupported
/// pattern letter returns `Err` so callers get a clear error rather than
/// silently-wrong output. Text inside single quotes is a literal; `''` is a
/// literal single quote.
fn spark_pattern_to_chrono(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut i = 0;
    while let Some(&c) = chars.get(i) {
        // Quoted literal segment.
        if c == '\'' {
            i += 1;
            if chars.get(i) == Some(&'\'') {
                // '' → literal single quote
                out.push('\'');
                i += 1;
                continue;
            }
            while let Some(&c2) = chars.get(i) {
                if c2 == '\'' {
                    break;
                }
                escape_literal(c2, &mut out);
                i += 1;
            }
            if chars.get(i).is_none() {
                return Err(format!("unterminated quoted literal in pattern '{pattern}'"));
            }
            i += 1; // consume closing quote
            continue;
        }
        // A run of the same pattern letter.
        if c.is_ascii_alphabetic() {
            let mut n = 1;
            while chars.get(i + n) == Some(&c) {
                n += 1;
            }
            out.push_str(&translate_letter(c, n).ok_or_else(|| {
                format!("unsupported Spark datetime pattern letter '{c}' (x{n}) in '{pattern}'")
            })?);
            i += n;
            continue;
        }
        // Non-letter, unquoted: literal.
        escape_literal(c, &mut out);
        i += 1;
    }
    Ok(out)
}

/// A `%` in literal text must be escaped for chrono (`%%`).
fn escape_literal(c: char, out: &mut String) {
    if c == '%' {
        out.push_str("%%");
    } else {
        out.push(c);
    }
}

/// Map one Spark pattern letter repeated `n` times to a chrono directive.
/// Returns `None` for unsupported letters (caller turns this into an error).
fn translate_letter(c: char, n: usize) -> Option<String> {
    let s = match (c, n) {
        // Year
        ('y' | 'u', 2) => "%y",
        ('y' | 'u', _) => "%Y",
        // Month: numeric (M/MM) vs text (MMM/MMMM)
        ('M' | 'L', 1) => "%-m",
        ('M' | 'L', 2) => "%m",
        ('M' | 'L', 3) => "%b",
        ('M' | 'L', _) => "%B",
        // Day of month
        ('d', 1) => "%-d",
        ('d', _) => "%d",
        // Day of year
        ('D', 1) => "%-j",
        ('D', _) => "%j",
        // Day of week name
        ('E', 1..=3) => "%a",
        ('E', _) => "%A",
        // Hour 0-23 / 1-12
        ('H', 1) => "%-H",
        ('H', _) => "%H",
        ('h', 1) => "%-I",
        ('h', _) => "%I",
        // Minute / second
        ('m', 1) => "%-M",
        ('m', _) => "%M",
        ('s', 1) => "%-S",
        ('s', _) => "%S",
        // Fraction of second (Spark SSS = milliseconds)
        ('S', 1..=3) => "%3f",
        ('S', 4..=6) => "%6f",
        ('S', _) => "%9f",
        // AM/PM
        ('a', _) => "%p",
        _ => return None,
    };
    Some(s.to_string())
}

// ── crc32 ───────────────────────────────────────────────────────────────────

fn make_crc32() -> ScalarUDF {
    create_udf(
        "crc32",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let [input_arr] = arrays.as_slice() else {
                return Err(DataFusionError::Internal("crc32: expected 1 argument".into()));
            };
            let input = input_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("crc32: argument must be Utf8".into()))?;
            let mut out = Int64Builder::new();
            for i in 0..input.len() {
                if input.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(crc32_ieee(input.value(i).as_bytes()) as i64);
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

/// IEEE CRC-32 (reflected, polynomial 0xEDB88320) — the same checksum Spark's
/// `crc32` returns, computed over the input bytes.
fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_known_vectors() {
        // Standard CRC-32 test vectors.
        assert_eq!(crc32_ieee(b""), 0x0000_0000);
        assert_eq!(crc32_ieee(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32_ieee(b"abc"), 0x3524_41C2);
        // Spark: SELECT crc32('Spark') = 1557323817
        assert_eq!(crc32_ieee(b"Spark") as i64, 1_557_323_817);
    }

    #[test]
    fn spark_pattern_common_letters() {
        assert_eq!(spark_pattern_to_chrono("yyyy-MM-dd").unwrap(), "%Y-%m-%d");
        assert_eq!(
            spark_pattern_to_chrono("yyyy-MM-dd HH:mm:ss").unwrap(),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(spark_pattern_to_chrono("yy/M/d").unwrap(), "%y/%-m/%-d");
        assert_eq!(spark_pattern_to_chrono("MMM").unwrap(), "%b");
        assert_eq!(spark_pattern_to_chrono("EEEE").unwrap(), "%A");
        assert_eq!(spark_pattern_to_chrono("hh:mm a").unwrap(), "%I:%M %p");
    }

    #[test]
    fn spark_pattern_literals_and_percent() {
        // Quoted literal and a stray percent sign must be escaped for chrono.
        assert_eq!(
            spark_pattern_to_chrono("yyyy 'at' HH").unwrap(),
            "%Y at %H"
        );
        assert_eq!(spark_pattern_to_chrono("HH'%'").unwrap(), "%H%%");
        // Non-letter separators pass through literally.
        assert_eq!(spark_pattern_to_chrono("yyyy/MM").unwrap(), "%Y/%m");
    }

    #[test]
    fn spark_pattern_unsupported_letter_errors() {
        // Timezone / era letters are not supported → clear error, not silent wrong output.
        assert!(spark_pattern_to_chrono("yyyy z").is_err());
        assert!(spark_pattern_to_chrono("G yyyy").is_err());
        assert!(spark_pattern_to_chrono("yyyy'").is_err()); // unterminated literal
    }

    #[tokio::test]
    async fn date_format_and_crc32_via_sql() {
        use arrow::array::{Int64Array, StringArray};
        let engine = crate::SqlEngine::new();
        let batches = engine
            .sql(
                "SELECT date_format(TIMESTAMP '2024-03-07 09:05:00', 'yyyy-MM-dd HH:mm') AS d, \
                        crc32('Spark') AS c",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let b = &batches[0];
        let d = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(d.value(0), "2024-03-07 09:05");
        assert_eq!(c.value(0), 1_557_323_817);
    }
}
