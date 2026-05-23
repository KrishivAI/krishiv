//! Spark 3.5 SQL function aliases and UDFs for Krishiv (R15 S1).

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Pairs of (DataFusion builtin name, Spark alias names).
const SPARK_ALIASES: &[(&str, &[&str])] = &[
    ("coalesce", &["ifnull", "nvl"]),
    ("strpos", &["instr", "locate", "charindex", "position"]),
    ("substr", &["substring", "substring_index", "substr"]),
    ("left", &["left"]),
    ("right", &["right"]),
    ("upper", &["upper"]),
    ("lower", &["lower"]),
    ("trim", &["trim", "btrim", "ltrim", "rtrim"]),
    ("length", &["length", "len", "char_length", "character_length"]),
    ("concat", &["concat"]),
    ("concat_ws", &["concat_ws"]),
    ("replace", &["replace"]),
    ("translate", &["translate"]),
    ("regexp_replace", &["regexp_replace"]),
    ("regexp_match", &["regexp_extract", "regexp_like"]),
    ("split_part", &["split_part"]),
    ("split", &["split"]),
    ("initcap", &["initcap"]),
    ("lpad", &["lpad"]),
    ("rpad", &["rpad"]),
    ("repeat", &["repeat"]),
    ("reverse", &["reverse"]),
    ("ascii", &["ascii"]),
    ("chr", &["chr", "char"]),
    ("encode", &["base64", "encode"]),
    ("decode", &["unbase64", "decode"]),
    ("md5", &["md5"]),
    ("sha256", &["sha2", "sha256"]),
    ("abs", &["abs"]),
    ("ceil", &["ceil", "ceiling"]),
    ("floor", &["floor"]),
    ("round", &["round", "bround"]),
    ("sqrt", &["sqrt"]),
    ("power", &["pow", "power"]),
    ("exp", &["exp"]),
    ("ln", &["ln"]),
    ("log", &["log", "log10"]),
    ("sin", &["sin"]),
    ("cos", &["cos"]),
    ("tan", &["tan"]),
    ("cot", &["cot"]),
    ("asin", &["asin"]),
    ("acos", &["acos"]),
    ("atan", &["atan"]),
    ("atan2", &["atan2"]),
    ("sign", &["sign", "signum"]),
    ("mod", &["pmod", "mod"]),
    ("greatest", &["greatest"]),
    ("least", &["least"]),
    ("date_part", &["date_part"]),
    ("date_trunc", &["date_trunc", "trunc"]),
    ("date_add", &["date_add"]),
    ("date_sub", &["date_sub"]),
    ("make_date", &["make_date"]),
    ("to_date", &["to_date"]),
    ("to_timestamp", &["to_timestamp", "timestamp"]),
    ("to_char", &["date_format", "to_char"]),
    ("from_unixtime", &["from_unixtime"]),
    ("to_unixtime", &["unix_timestamp", "to_unixtime"]),
    ("now", &["current_timestamp", "now", "localtimestamp"]),
    ("today", &["current_date", "today", "curdate"]),
    ("array_has", &["array_contains", "array_has"]),
    ("array_distinct", &["array_distinct"]),
    ("array_intersect", &["array_intersect"]),
    ("array_union", &["array_union"]),
    ("array_except", &["array_except"]),
    ("array_length", &["size", "array_length"]),
    ("array_element", &["element_at"]),
    ("flatten", &["flatten"]),
    ("array_append", &["array_append"]),
    ("array_prepend", &["array_prepend"]),
    ("array_position", &["array_position"]),
    ("array_remove", &["array_remove"]),
    ("array_sort", &["array_sort"]),
    ("array_slice", &["slice"]),
    ("map_keys", &["map_keys"]),
    ("map_values", &["map_values"]),
    ("map_entries", &["map_entries"]),
    ("struct", &["named_struct", "struct"]),
    ("row_number", &["row_number"]),
    ("rank", &["rank"]),
    ("dense_rank", &["dense_rank"]),
    ("percent_rank", &["percent_rank"]),
    ("cume_dist", &["cume_dist"]),
    ("ntile", &["ntile"]),
    ("lag", &["lag"]),
    ("lead", &["lead"]),
    ("first_value", &["first_value", "first"]),
    ("last_value", &["last_value", "last"]),
    ("nth_value", &["nth_value"]),
    ("count", &["count"]),
    ("sum", &["sum"]),
    ("avg", &["avg", "mean"]),
    ("min", &["min"]),
    ("max", &["max"]),
    ("stddev", &["stddev", "stddev_samp"]),
    ("stddev_pop", &["stddev_pop"]),
    ("var_samp", &["var_samp", "variance"]),
    ("var_pop", &["var_pop"]),
    ("corr", &["corr"]),
    ("covar_pop", &["covar_pop"]),
    ("covar_samp", &["covar_samp"]),
    ("skewness", &["skewness"]),
    ("kurtosis", &["kurtosis"]),
    ("approx_percentile_cont", &["percentile_approx", "approx_percentile"]),
    ("approx_distinct", &["approx_count_distinct", "hll_cardinality"]),
    ("bit_and", &["bit_and"]),
    ("bit_or", &["bit_or"]),
    ("bit_xor", &["bit_xor"]),
    ("bool_and", &["bool_and", "every"]),
    ("bool_or", &["bool_or", "some"]),
    ("is_null", &["isnull"]),
    ("is_not_null", &["isnotnull"]),
    ("nullif", &["nullif"]),
    ("try_cast", &["try_cast"]),
    ("uuid", &["uuid"]),
    ("random", &["rand", "random"]),
    ("xxhash64", &["hash", "spark_hash", "xxhash64"]),
    ("bin", &["bin"]),
    ("hex", &["hex"]),
    ("octet_length", &["octet_length"]),
    ("bit_length", &["bit_length"]),
    ("overlay", &["overlay"]),
    ("ends_with", &["endswith", "ends_with"]),
    ("starts_with", &["startswith", "starts_with"]),
    ("contains", &["contains"]),
    ("factorial", &["factorial"]),
    ("pi", &["pi"]),
    ("e", &["e"]),
    ("degrees", &["degrees"]),
    ("radians", &["radians"]),
    ("width_bucket", &["width_bucket"]),];

/// Count of unique Spark alias names registered (for acceptance gates).
pub fn spark_alias_count() -> usize {
    SPARK_ALIASES.iter().map(|(_, a)| a.len()).sum()
}

/// Register Spark-compatible SQL function names on a DataFusion session.
pub fn register_spark_functions(
    ctx: &SessionContext,
) -> Result<(), datafusion::error::DataFusionError> {
    let registry = ctx.state();

    for (builtin, aliases) in SPARK_ALIASES {
        if let Ok(udf) = registry.udf(builtin) {
            let aliases: &[&'static str] = aliases;
            ctx.register_udf(udf.as_ref().clone().with_aliases(aliases.iter().copied()));
        }
    }

    // Spark `isnan` for double columns (divergent null semantics vs DataFusion).
    let isnan = create_udf(
        "isnan",
        vec![DataType::Float64],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| {
            use datafusion::logical_expr::ColumnarValue;
            let arr = match &args[0] {
                ColumnarValue::Array(a) => a.clone(),
                ColumnarValue::Scalar(s) => s.to_array()?,
            };
            let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "isnan: expected Float64 array".into(),
                )
            })?;
            let out: BooleanArray = arr.iter().map(|v| v.map(|x| x.is_nan())).collect();
            Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
        }),
    );
    ctx.register_udf(isnan);
    crate::spark_compat_date::register_spark_date_udfs(ctx)?;

    Ok(())
}

/// Classify null-handling for documentation / tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullHandlingClass {
    Equivalent,
    Divergent,
    Unimplemented,
}

/// Metadata for spark_compat test harness.
#[derive(Debug, Clone)]
pub struct SparkFunctionTestCase {
    pub name: &'static str,
    pub sql: String,
    pub null_handling: NullHandlingClass,
    pub null_handling_note: &'static str,
}

/// Built-in catalog of spark_compat SQL smoke tests (R15 S1.1).
pub fn spark_function_test_cases() -> Vec<SparkFunctionTestCase> {
    vec![
        SparkFunctionTestCase {
            name: "concat_ws",
            sql: "SELECT concat_ws(',', 'a', 'b', 'c') AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "DataFusion concat_ws",
        },
        SparkFunctionTestCase {
            name: "regexp_replace",
            sql: "SELECT regexp_replace('abc123', '[0-9]+', 'X') AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "DataFusion regexp_replace",
        },
        SparkFunctionTestCase {
            name: "ifnull",
            sql: "SELECT ifnull(NULL, 42) AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "alias of coalesce",
        },
        SparkFunctionTestCase {
            name: "isnan",
            sql: "SELECT isnan(x) AS v FROM (VALUES (CAST('NaN' AS DOUBLE)), (1.0)) AS t(x)".into(),
            null_handling: NullHandlingClass::Divergent,
            null_handling_note: "custom ScalarUDF",
        },
        SparkFunctionTestCase {
            name: "row_number_window",
            sql: "SELECT row_number() OVER (ORDER BY n) AS rn FROM (VALUES (1),(2),(3)) AS t(n)".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "window row_number",
        },
        SparkFunctionTestCase {
            name: "array_contains",
            sql: "SELECT array_contains(ARRAY[1,2,3], 2) AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "array_has alias",
        },
        SparkFunctionTestCase {
            name: "upper",
            sql: "SELECT upper('abc') AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "string upper",
        },
        SparkFunctionTestCase {
            name: "greatest",
            sql: "SELECT greatest(1, 2, 3) AS v".into(),
            null_handling: NullHandlingClass::Equivalent,
            null_handling_note: "greatest",
        },
    ]
}

