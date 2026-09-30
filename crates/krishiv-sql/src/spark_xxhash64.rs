#![forbid(unsafe_code)]
//! Spark's `xxhash64(expr, …)`: a 64-bit hash of one or more values.
//!
//! Spark hashes with XXH64 and seed 42, feeding each value in by its type and
//! using the running hash as the seed for the next value. Its `hashInt`,
//! `hashLong` and `hashUnsafeBytes` are the standard XXH64 of 4, 8 and `n`
//! little-endian bytes, so this is standard XXH64 driven the same way:
//!
//! | value | hashed as |
//! |---|---|
//! | boolean | 4 bytes, `1` or `0` |
//! | tinyint, smallint, int, date | 4 bytes |
//! | bigint, timestamp (microseconds) | 8 bytes |
//! | float / double | the IEEE bits, with `-0.0` as `0.0` and one NaN |
//! | string, binary | the bytes |
//! | decimal, precision ≤ 18 | the unscaled value, 8 bytes |
//! | decimal, precision > 18 | the unscaled value's big-endian bytes |
//! | array, struct, map | each element / field / key then value, in order |
//! | NULL | nothing: the hash is unchanged |
//!
//! The hash depends on the *type*, so a value must have the type it has in
//! Spark to hash the same: an integer literal is `BIGINT` here and `INT` in
//! Spark, so `xxhash64(1)` needs `CAST(1 AS INT)` to reproduce Spark's number.
//! Types Spark has no counterpart for (unsigned integers, intervals, …) are
//! refused rather than hashed some way Spark would not.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

type DFResult<T> = Result<T, DataFusionError>;

/// Spark's seed for `xxhash64`.
const SEED: u64 = 42;

const PRIME_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME_5: u64 = 0x27D4_EB2F_1656_67C5;

fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME_2))
        .rotate_left(31)
        .wrapping_mul(PRIME_1)
}

fn merge_round(acc: u64, value: u64) -> u64 {
    (acc ^ round(0, value))
        .wrapping_mul(PRIME_1)
        .wrapping_add(PRIME_4)
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut buffer = [0_u8; 8];
    for (slot, byte) in buffer.iter_mut().zip(bytes) {
        *slot = *byte;
    }
    u64::from_le_bytes(buffer)
}

fn read_u32(bytes: &[u8]) -> u64 {
    let mut buffer = [0_u8; 4];
    for (slot, byte) in buffer.iter_mut().zip(bytes) {
        *slot = *byte;
    }
    u64::from(u32::from_le_bytes(buffer))
}

/// Standard XXH64.
fn xxh64(data: &[u8], seed: u64) -> u64 {
    let mut stripes = data.chunks_exact(32);
    let mut hash = if data.len() >= 32 {
        let mut v1 = seed.wrapping_add(PRIME_1).wrapping_add(PRIME_2);
        let mut v2 = seed.wrapping_add(PRIME_2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME_1);
        for stripe in stripes.by_ref() {
            let mut lanes = stripe.chunks_exact(8).map(read_u64);
            v1 = round(v1, lanes.next().unwrap_or(0));
            v2 = round(v2, lanes.next().unwrap_or(0));
            v3 = round(v3, lanes.next().unwrap_or(0));
            v4 = round(v4, lanes.next().unwrap_or(0));
        }
        let mut hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        for lane in [v1, v2, v3, v4] {
            hash = merge_round(hash, lane);
        }
        hash
    } else {
        seed.wrapping_add(PRIME_5)
    };
    hash = hash.wrapping_add(data.len() as u64);

    let mut words = stripes.remainder().chunks_exact(8);
    for word in words.by_ref() {
        hash ^= round(0, read_u64(word));
        hash = hash
            .rotate_left(27)
            .wrapping_mul(PRIME_1)
            .wrapping_add(PRIME_4);
    }
    let mut halves = words.remainder().chunks_exact(4);
    for half in halves.by_ref() {
        hash ^= read_u32(half).wrapping_mul(PRIME_1);
        hash = hash
            .rotate_left(23)
            .wrapping_mul(PRIME_2)
            .wrapping_add(PRIME_3);
    }
    for byte in halves.remainder() {
        hash ^= u64::from(*byte).wrapping_mul(PRIME_5);
        hash = hash.rotate_left(11).wrapping_mul(PRIME_1);
    }

    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME_3);
    hash ^ (hash >> 32)
}

fn hash_int(value: i32, seed: u64) -> u64 {
    xxh64(&value.to_le_bytes(), seed)
}

fn hash_long(value: i64, seed: u64) -> u64 {
    xxh64(&value.to_le_bytes(), seed)
}

/// `BigInteger.toByteArray()`: the shortest big-endian two's-complement form.
fn minimal_be_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while let (Some(first), Some(next)) = (bytes.get(start), bytes.get(start + 1)) {
        let redundant =
            (*first == 0x00 && *next & 0x80 == 0) || (*first == 0xFF && *next & 0x80 != 0);
        if !redundant {
            break;
        }
        start += 1;
    }
    bytes.get(start..).unwrap_or(&[]).to_vec()
}

/// Feed `array[row]` into the running hash.
fn hash_value(array: &dyn Array, row: usize, seed: u64) -> DFResult<u64> {
    if array.is_null(row) {
        return Ok(seed);
    }
    let hash = match array.data_type() {
        DataType::Null => seed,
        DataType::Boolean => hash_int(i32::from(array.as_boolean().value(row)), seed),
        DataType::Int8 => hash_int(i32::from(array.as_primitive::<Int8Type>().value(row)), seed),
        DataType::Int16 => hash_int(
            i32::from(array.as_primitive::<Int16Type>().value(row)),
            seed,
        ),
        DataType::Int32 => hash_int(array.as_primitive::<Int32Type>().value(row), seed),
        DataType::Date32 => hash_int(array.as_primitive::<Date32Type>().value(row), seed),
        DataType::Int64 => hash_long(array.as_primitive::<Int64Type>().value(row), seed),
        // Spark keeps timestamps in microseconds.
        DataType::Timestamp(unit, _) => {
            let micros = match unit {
                TimeUnit::Second => array
                    .as_primitive::<TimestampSecondType>()
                    .value(row)
                    .wrapping_mul(1_000_000),
                TimeUnit::Millisecond => array
                    .as_primitive::<TimestampMillisecondType>()
                    .value(row)
                    .wrapping_mul(1_000),
                TimeUnit::Microsecond => {
                    array.as_primitive::<TimestampMicrosecondType>().value(row)
                }
                TimeUnit::Nanosecond => array
                    .as_primitive::<TimestampNanosecondType>()
                    .value(row)
                    .div_euclid(1_000),
            };
            hash_long(micros, seed)
        }
        DataType::Float32 => {
            let value = array.as_primitive::<Float32Type>().value(row);
            let bits = if value.is_nan() {
                f32::NAN.to_bits()
            } else if value == 0.0 {
                0.0_f32.to_bits()
            } else {
                value.to_bits()
            };
            xxh64(&bits.to_le_bytes(), seed)
        }
        DataType::Float64 => {
            let value = array.as_primitive::<Float64Type>().value(row);
            let bits = if value.is_nan() {
                f64::NAN.to_bits()
            } else if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            };
            xxh64(&bits.to_le_bytes(), seed)
        }
        DataType::Utf8 => xxh64(array.as_string::<i32>().value(row).as_bytes(), seed),
        DataType::LargeUtf8 => xxh64(array.as_string::<i64>().value(row).as_bytes(), seed),
        DataType::Utf8View => xxh64(array.as_string_view().value(row).as_bytes(), seed),
        DataType::Binary => xxh64(array.as_binary::<i32>().value(row), seed),
        DataType::LargeBinary => xxh64(array.as_binary::<i64>().value(row), seed),
        DataType::BinaryView => xxh64(array.as_binary_view().value(row), seed),
        DataType::Decimal128(precision, _) => {
            let unscaled = array.as_primitive::<Decimal128Type>().value(row);
            match i64::try_from(unscaled) {
                Ok(small) if *precision <= 18 => hash_long(small, seed),
                _ => xxh64(&minimal_be_bytes(unscaled), seed),
            }
        }
        DataType::List(_) => hash_all(array.as_list::<i32>().value(row).as_ref(), seed)?,
        DataType::LargeList(_) => hash_all(array.as_list::<i64>().value(row).as_ref(), seed)?,
        DataType::FixedSizeList(_, _) => {
            hash_all(array.as_fixed_size_list().value(row).as_ref(), seed)?
        }
        DataType::Struct(_) => {
            let mut hash = seed;
            for column in array.as_struct().columns() {
                hash = hash_value(column.as_ref(), row, hash)?;
            }
            hash
        }
        DataType::Map(_, _) => {
            let entries = array.as_map().value(row);
            let mut hash = seed;
            if let [keys, values] = entries.columns() {
                for entry in 0..entries.len() {
                    hash = hash_value(keys.as_ref(), entry, hash)?;
                    hash = hash_value(values.as_ref(), entry, hash)?;
                }
            }
            hash
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "xxhash64 does not support {other}: Spark has no such type to hash it as"
            )));
        }
    };
    Ok(hash)
}

/// Feed every element of `array` into the running hash, in order.
fn hash_all(array: &dyn Array, seed: u64) -> DFResult<u64> {
    let mut hash = seed;
    for row in 0..array.len() {
        hash = hash_value(array, row, hash)?;
    }
    Ok(hash)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct XxHash64 {
    signature: Signature,
}

impl ScalarUDFImpl for XxHash64 {
    fn name(&self) -> &str {
        "xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        if arg_types.is_empty() {
            return Err(DataFusionError::Plan(String::from(
                "xxhash64 needs at least one argument",
            )));
        }
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let columns = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<DFResult<Vec<ArrayRef>>>()?;
        let mut hashes = vec![SEED; rows];
        for column in &columns {
            for (row, hash) in hashes.iter_mut().enumerate() {
                *hash = hash_value(column.as_ref(), row, *hash)?;
            }
        }
        // Spark returns the 64 bits as a signed BIGINT.
        let values = hashes
            .into_iter()
            .map(|hash| i64::from_ne_bytes(hash.to_ne_bytes()));
        Ok(ColumnarValue::Array(Arc::new(
            Int64Array::from_iter_values(values),
        )))
    }
}

/// The `xxhash64` scalar function.
pub(crate) fn make_xxhash64() -> ScalarUDF {
    ScalarUDF::new_from_impl(XxHash64 {
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Published XXH64 vectors: the building block is the real algorithm.
    #[test]
    fn xxh64_matches_the_reference_vectors() {
        assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"a", 0), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(xxh64(b"abc", 0), 0x44BC_2CF5_AD77_0999);
        // Longer than one 32-byte stripe, with a tail of every size class.
        assert_eq!(
            xxh64(b"Nobody inspects the spammish repetition", 0),
            0xFBCE_A83C_8A37_8BF1
        );
    }

    #[test]
    fn minimal_bytes_are_javas_biginteger_bytes() {
        assert_eq!(minimal_be_bytes(0), [0x00]);
        assert_eq!(minimal_be_bytes(1), [0x01]);
        assert_eq!(minimal_be_bytes(-1), [0xFF]);
        assert_eq!(minimal_be_bytes(127), [0x7F]);
        assert_eq!(minimal_be_bytes(128), [0x00, 0x80]);
        assert_eq!(minimal_be_bytes(-128), [0x80]);
        assert_eq!(minimal_be_bytes(-129), [0xFF, 0x7F]);
        assert_eq!(minimal_be_bytes(256), [0x01, 0x00]);
    }

    async fn scalar(sql: &str) -> i64 {
        let batches = crate::SqlEngine::new()
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan `{sql}`: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("run `{sql}`: {e}"));
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    }

    /// The example in Spark's own function reference, which exercises a
    /// string, an array of INT, and an INT, chained.
    #[tokio::test]
    async fn reproduces_the_spark_documentation_example() {
        assert_eq!(
            scalar("SELECT xxhash64('Spark', make_array(CAST(123 AS INT)), CAST(2 AS INT))").await,
            5_602_566_077_635_097_486
        );
    }

    #[tokio::test]
    async fn hashes_by_type_and_skips_nulls() {
        // The same number hashes differently as INT and as BIGINT, as in Spark.
        let as_int = scalar("SELECT xxhash64(CAST(1 AS INT))").await;
        let as_long = scalar("SELECT xxhash64(CAST(1 AS BIGINT))").await;
        assert_ne!(as_int, as_long);
        assert_eq!(as_int, i64::from_ne_bytes(hash_int(1, SEED).to_ne_bytes()));
        assert_eq!(
            as_long,
            i64::from_ne_bytes(hash_long(1, SEED).to_ne_bytes())
        );
        // Narrower integers hash as INT.
        assert_eq!(scalar("SELECT xxhash64(CAST(1 AS SMALLINT))").await, as_int);
        assert_eq!(scalar("SELECT xxhash64(CAST(1 AS TINYINT))").await, as_int);
        // A NULL contributes nothing, wherever it sits.
        assert_eq!(
            scalar("SELECT xxhash64(CAST(NULL AS INT), CAST(1 AS INT))").await,
            as_int
        );
        assert_eq!(
            scalar("SELECT xxhash64(CAST(1 AS INT), CAST(NULL AS VARCHAR))").await,
            as_int
        );
        // All NULL is the seed itself.
        assert_eq!(scalar("SELECT xxhash64(CAST(NULL AS INT))").await, 42);
        // Argument order matters.
        assert_ne!(
            scalar("SELECT xxhash64('a', 'b')").await,
            scalar("SELECT xxhash64('b', 'a')").await
        );
    }

    #[tokio::test]
    async fn floating_zeroes_and_nested_values() {
        assert_eq!(
            scalar("SELECT xxhash64(CAST(-0.0 AS DOUBLE))").await,
            scalar("SELECT xxhash64(CAST(0.0 AS DOUBLE))").await
        );
        // A struct hashes as its fields in order; an array as its elements.
        assert_eq!(
            scalar("SELECT xxhash64(named_struct('a', 'x', 'b', CAST(7 AS INT)))").await,
            scalar("SELECT xxhash64('x', CAST(7 AS INT))").await
        );
        assert_eq!(
            scalar("SELECT xxhash64(make_array('x', 'y'))").await,
            scalar("SELECT xxhash64('x', 'y')").await
        );
    }

    #[tokio::test]
    async fn hashes_a_column_row_by_row() {
        let batches = crate::SqlEngine::new()
            .sql("SELECT xxhash64(s) AS h FROM (VALUES ('a'), ('b'), ('a')) AS t(s)")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let hashes: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(hashes.len(), 3);
        assert_eq!(hashes[0], hashes[2]);
        assert_ne!(hashes[0], hashes[1]);
    }

    #[tokio::test]
    async fn refuses_a_type_spark_cannot_hash() {
        let engine = crate::SqlEngine::new();
        let error = match engine.sql("SELECT xxhash64(arrow_cast(1, 'UInt32'))").await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("must fail").to_string(),
        };
        assert!(error.contains("does not support"), "{error}");
    }
}
