"""PySpark-shaped SQL functions namespace."""

import pytest

import krishiv as ks
import krishiv.functions as KF
from krishiv.sql import DataFrame, Session
from krishiv.sql import functions as F


FUNCTION_NAMES = {
    "abs",
    "acos",
    "aggregate",
    "approx_count_distinct",
    "array",
    "array_agg",
    "array_append",
    "array_contains",
    "array_distinct",
    "array_except",
    "array_intersect",
    "array_length",
    "array_position",
    "array_prepend",
    "array_remove",
    "array_union",
    "asc",
    "ascii",
    "asin",
    "atan",
    "atan2",
    "avg",
    "bit_and",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "btrim",
    "call_function",
    "cardinality",
    "cast",
    "cbrt",
    "ceil",
    "char_length",
    "chr",
    "coalesce",
    "col",
    "collect_list",
    "column",
    "concat",
    "concat_ws",
    "contains",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "count_all",
    "count_distinct",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "current_date",
    "current_timestamp",
    "date_trunc",
    "day",
    "dayofmonth",
    "degrees",
    "dense_rank",
    "desc",
    "ends_with",
    "exists",
    "exp",
    "explode",
    "expr",
    "factorial",
    "filter",
    "first",
    "first_value",
    "flatten",
    "floor",
    "forall",
    "function",
    "gcd",
    "greatest",
    "hour",
    "ifnull",
    "initcap",
    "instr",
    "isnan",
    "isnotnull",
    "isnull",
    "lag",
    "last",
    "last_value",
    "lcm",
    "lead",
    "least",
    "left",
    "length",
    "lit",
    "ln",
    "locate",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "max",
    "md5",
    "mean",
    "median",
    "min",
    "minute",
    "month",
    "nanvl",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nvl",
    "nvl2",
    "octet_length",
    "percent_rank",
    "pi",
    "posexplode",
    "pow",
    "power",
    "quarter",
    "radians",
    "rand",
    "rank",
    "reduce",
    "regexp_like",
    "regexp_replace",
    "repeat",
    "reverse",
    "right",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "second",
    "sha256",
    "sha512",
    "sign",
    "signum",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "substr",
    "substring",
    "sum",
    "sum_distinct",
    "tan",
    "tanh",
    "to_date",
    "to_timestamp",
    "transform",
    "translate",
    "trim",
    "try_cast",
    "upper",
    "uuid",
    "var_pop",
    "var_samp",
    "variance",
    "when",
    "year",
    "zip_with",
}


def _assert_sql_contains(column, *parts):
    sql = column.sql().upper()
    for part in parts:
        assert part.upper() in sql


def _pretty(dataframe):
    result = dataframe.collect()
    assert result.row_count >= 1
    return result.pretty()


def test_all_public_function_callables_are_covered():
    exported_callables = {
        name
        for name in F.__all__
        if callable(getattr(F, name, None)) and not name.startswith("_")
    }
    assert exported_callables == FUNCTION_NAMES


def test_sql_namespace_and_short_aliases_are_available():
    assert Session is ks.Session
    assert DataFrame is ks.DataFrame
    assert F.col("amount").sql() == '"amount"'
    assert F.column("amount").sql() == '"amount"'
    assert KF.col("amount").sql() == '"amount"'
    assert ks.col("amount").sql() == '"amount"'


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (42, "42"),
        (3.5, "3.5"),
        ("O'Reilly", "'O''REILLY'"),
    ],
)
def test_lit_supports_stable_literal_types(value, expected):
    assert expected in F.lit(value).sql().upper()


def test_lit_rejects_unsupported_python_values():
    with pytest.raises(TypeError, match="expected a Column or a literal"):
        F.lit(object())


def test_expr_call_function_and_function_alias_render_sql():
    assert F.expr("amount + 1").sql() == "amount + 1"
    _assert_sql_contains(F.call_function("sqrt", F.col("amount")), "SQRT", "amount")
    assert F.function("sqrt", "amount").sql() == F.call_function("sqrt", "amount").sql()


def test_column_boolean_context_is_rejected():
    with pytest.raises(TypeError, match="lazy expression"):
        bool(F.col("amount"))


def test_aggregate_helpers_render_and_execute():
    dataframe = ks.Session.local().sql(
        "SELECT 1 AS grp, 2 AS amount UNION ALL SELECT 1 AS grp, 4 AS amount"
    )
    grouped = dataframe.group_by_columns([F.col("grp")]).agg_columns(
        [
            F.count("amount").alias("count_amount"),
            F.count("*").alias("count_star"),
            F.count().alias("count_default"),
            F.count(None).alias("count_none"),
            F.count_all().alias("count_all"),
            F.sum("amount").alias("sum_amount"),
            F.avg("amount").alias("avg_amount"),
            F.mean("amount").alias("mean_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
        ]
    )
    text = _pretty(grouped)
    for header in [
        "count_amount",
        "count_star",
        "count_default",
        "count_none",
        "count_all",
        "sum_amount",
        "avg_amount",
        "mean_amount",
        "min_amount",
        "max_amount",
    ]:
        assert header in text


def test_window_functions_render_and_execute():
    dataframe = ks.Session.local().sql(
        "SELECT 1 AS grp, 10 AS amount UNION ALL SELECT 1 AS grp, 20 AS amount "
        "UNION ALL SELECT 1 AS grp, 30 AS amount"
    )
    order = [F.asc(F.col("amount"))]
    windowed = dataframe.select_columns(
        [
            F.col("grp"),
            F.col("amount"),
            F.row_number().over(partition_by=[F.col("grp")], order_by=order).alias("rn"),
            F.rank().over(partition_by=[F.col("grp")], order_by=order).alias("rk"),
            F.dense_rank().over(partition_by=[F.col("grp")], order_by=order).alias("dr"),
            F.percent_rank().over(partition_by=[F.col("grp")], order_by=order).alias("pr"),
            F.cume_dist().over(partition_by=[F.col("grp")], order_by=order).alias("cd"),
            F.ntile(2).over(partition_by=[F.col("grp")], order_by=order).alias("nt"),
            F.lag(F.col("amount"), 1).over(partition_by=[F.col("grp")], order_by=order).alias("lag_amt"),
            F.lead(F.col("amount"), 1).over(partition_by=[F.col("grp")], order_by=order).alias("lead_amt"),
            F.first_value(F.col("amount")).over(partition_by=[F.col("grp")], order_by=order).alias("first_amt"),
            F.last_value(F.col("amount"))
            .over(partition_by=[F.col("grp")], order_by=order)
            .rows_between(None, None)
            .alias("last_amt"),
            F.sum(F.col("amount"))
            .over(partition_by=[F.col("grp")], order_by=order)
            .rows_between(None, 0)
            .alias("running_sum"),
        ]
    )
    result = windowed.collect()
    assert result.row_count == 3
    text = result.pretty()
    for header in [
        "rn",
        "rk",
        "dr",
        "pr",
        "cd",
        "nt",
        "lag_amt",
        "lead_amt",
        "first_amt",
        "last_amt",
        "running_sum",
    ]:
        assert header in text
    assert "6" in text


def test_null_helpers_execute_and_validate_empty_varargs():
    dataframe = ks.Session.local().sql(
        "SELECT CAST(NULL AS BIGINT) AS missing, 7 AS value, CAST('NaN' AS DOUBLE) AS nan_value"
    )
    projected = dataframe.select_columns(
        [
            F.coalesce("missing", F.lit(9)).alias("coalesced"),
            F.ifnull("missing", F.lit(8)).alias("ifnull_value"),
            F.nullif("value", F.lit(7)).alias("nullif_value"),
            F.isnull("missing").alias("missing_is_null"),
            F.isnotnull("value").alias("value_is_not_null"),
            F.isnan("nan_value").alias("nan_check"),
        ]
    )
    text = _pretty(projected)
    for header in [
        "coalesced",
        "ifnull_value",
        "nullif_value",
        "missing_is_null",
        "value_is_not_null",
        "nan_check",
    ]:
        assert header in text
    assert "9" in text
    assert "8" in text
    assert "true" in text.lower()

    with pytest.raises(ValueError, match="coalesce requires"):
        F.coalesce()


def test_string_helpers_execute_and_validate_empty_concat():
    dataframe = ks.Session.local().sql("SELECT '  AbC  ' AS raw, 'x' AS suffix")
    projected = dataframe.select_columns(
        [
            F.upper("raw").alias("upper_value"),
            F.lower("raw").alias("lower_value"),
            F.length("raw").alias("length_value"),
            F.trim("raw").alias("trim_value"),
            F.ltrim("raw").alias("ltrim_value"),
            F.rtrim("raw").alias("rtrim_value"),
            F.concat(F.trim("raw"), F.lit("-"), "suffix").alias("concat_value"),
        ]
    )
    text = _pretty(projected)
    for header in [
        "upper_value",
        "lower_value",
        "length_value",
        "trim_value",
        "ltrim_value",
        "rtrim_value",
        "concat_value",
    ]:
        assert header in text
    assert "ABC" in text
    assert "abc" in text.lower()
    assert "AbC-x" in text

    with pytest.raises(ValueError, match="concat requires"):
        F.concat()


def test_numeric_helpers_execute():
    dataframe = ks.Session.local().sql("SELECT -4.7 AS x, 4.0 AS y, 1.0 AS angle")
    projected = dataframe.select_columns(
        [
            F.abs("x").alias("abs_value"),
            F.round("x").alias("round_value"),
            F.round("x", 1).alias("round_scaled"),
            F.floor("x").alias("floor_value"),
            F.ceil("x").alias("ceil_value"),
            F.sqrt("y").alias("sqrt_value"),
            F.exp("angle").alias("exp_value"),
            F.log("y").alias("log_value"),
            F.sin("angle").alias("sin_value"),
            F.cos("angle").alias("cos_value"),
            F.tan("angle").alias("tan_value"),
        ]
    )
    text = _pretty(projected)
    for header in [
        "abs_value",
        "round_value",
        "round_scaled",
        "floor_value",
        "ceil_value",
        "sqrt_value",
        "exp_value",
        "log_value",
        "sin_value",
        "cos_value",
        "tan_value",
    ]:
        assert header in text
    assert "4.7" in text
    assert "2" in text


def test_date_time_helpers_execute():
    dataframe = ks.Session.local().sql(
        "SELECT CAST('2026-06-19T12:34:56' AS TIMESTAMP) AS ts"
    )
    projected = dataframe.select_columns(
        [
            F.current_date().alias("current_date_value"),
            F.current_timestamp().alias("current_timestamp_value"),
            F.date_trunc("day", "ts").alias("truncated_day"),
        ]
    )
    text = _pretty(projected)
    assert "current_date_value" in text
    assert "current_timestamp_value" in text
    assert "truncated_day" in text
    assert "2026-06-19" in text


def test_ordering_and_cast_helpers_render_and_execute():
    assert "ASC" in F.asc("amount").sql().upper()
    assert "DESC" in F.desc(F.col("amount")).sql().upper()

    dataframe = ks.Session.local().sql("SELECT '42' AS good_value, 'bad' AS bad_value")
    projected = dataframe.select_columns(
        [
            F.cast("good_value", "int").alias("cast_value"),
            F.try_cast("bad_value", "int").alias("try_cast_value"),
        ]
    )
    text = _pretty(projected)
    assert "cast_value" in text
    assert "try_cast_value" in text
    assert "42" in text

    with pytest.raises(ValueError, match="unsupported type"):
        F.cast("good_value", "not_a_type")
