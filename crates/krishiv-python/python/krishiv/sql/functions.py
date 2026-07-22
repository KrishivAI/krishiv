"""Column expression helpers for Krishiv SQL/DataFrame APIs.

The functions here intentionally cover the migration-critical core first:
column/literal construction, SQL expression escape hatches, common aggregates,
null handling, string helpers, numeric helpers, date/time helpers, and a generic
``call_function`` fallback for DataFusion/Krishiv SQL functions that are not yet
wrapped explicitly.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from ..krishiv import (
    Column,
    avg as _avg,
    call_function as _call_function,
    col as _col,
    count as _count,
    count_all as _count_all,
    cume_dist as _cume_dist,
    dense_rank as _dense_rank,
    expr as _expr,
    first_value as _first_value,
    lag as _lag,
    last_value as _last_value,
    lead as _lead,
    lit as _lit,
    max as _max,
    min as _min,
    nth_value as _nth_value,
    ntile as _ntile,
    percent_rank as _percent_rank,
    rank as _rank,
    row_number as _row_number,
    sum as _sum,
    when as _when,
)

ColumnOrName = Union[Column, str]
ColumnLike = Union[Column, str, None, bool, int, float, bytes]


def _to_column(value: ColumnLike) -> Column:
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        return _col(value)
    return _lit(value)


def _to_columns(values: tuple[ColumnLike, ...]) -> list[Column]:
    return [_to_column(value) for value in values]


def col(name: str) -> Column:
    return _col(name)


def column(name: str) -> Column:
    return _col(name)


def lit(value: Any) -> Column:
    return _lit(value)


def expr(sql: str) -> Column:
    return _expr(sql)


def call_function(name: str, *args: ColumnLike) -> Column:
    return _call_function(name, _to_columns(args))


def function(name: str, *args: ColumnLike) -> Column:
    return call_function(name, *args)


def count(column: ColumnLike = "*") -> Column:
    if column is None or (isinstance(column, str) and column == "*"):
        return _count_all()
    return _count(_to_column(column))


def count_all() -> Column:
    return _count_all()


def sum(column: ColumnOrName) -> Column:  # noqa: A001
    return _sum(_to_column(column))


def avg(column: ColumnOrName) -> Column:
    return _avg(_to_column(column))


def mean(column: ColumnOrName) -> Column:
    return avg(column)


def min(column: ColumnOrName) -> Column:  # noqa: A001
    return _min(_to_column(column))


def max(column: ColumnOrName) -> Column:  # noqa: A001
    return _max(_to_column(column))


def coalesce(*columns: ColumnLike) -> Column:
    if not columns:
        raise ValueError("coalesce requires at least one argument")
    return call_function("coalesce", *columns)


def ifnull(value: ColumnLike, replacement: ColumnLike) -> Column:
    return coalesce(value, replacement)


def nullif(left: ColumnLike, right: ColumnLike) -> Column:
    return call_function("nullif", left, right)


def isnull(column: ColumnLike) -> Column:
    return _to_column(column).is_null()


def isnotnull(column: ColumnLike) -> Column:
    return _to_column(column).is_not_null()


def isnan(column: ColumnLike) -> Column:
    return call_function("isnan", column)


def upper(column: ColumnLike) -> Column:
    return call_function("upper", column)


def lower(column: ColumnLike) -> Column:
    return call_function("lower", column)


def length(column: ColumnLike) -> Column:
    return call_function("length", column)


def trim(column: ColumnLike) -> Column:
    return call_function("trim", column)


def ltrim(column: ColumnLike) -> Column:
    return call_function("ltrim", column)


def rtrim(column: ColumnLike) -> Column:
    return call_function("rtrim", column)


def concat(*columns: ColumnLike) -> Column:
    if not columns:
        raise ValueError("concat requires at least one argument")
    return call_function("concat", *columns)


def abs(column: ColumnLike) -> Column:  # noqa: A001
    return call_function("abs", column)


def round(column: ColumnLike, scale: Optional[int] = None) -> Column:  # noqa: A001
    if scale is None:
        return call_function("round", column)
    return call_function("round", column, lit(scale))


def floor(column: ColumnLike) -> Column:
    return call_function("floor", column)


def ceil(column: ColumnLike) -> Column:
    return call_function("ceil", column)


def sqrt(column: ColumnLike) -> Column:
    return call_function("sqrt", column)


def exp(column: ColumnLike) -> Column:
    return call_function("exp", column)


def log(column: ColumnLike) -> Column:
    return call_function("ln", column)


def sin(column: ColumnLike) -> Column:
    return call_function("sin", column)


def cos(column: ColumnLike) -> Column:
    return call_function("cos", column)


def tan(column: ColumnLike) -> Column:
    return call_function("tan", column)


def current_date() -> Column:
    return call_function("current_date")


def current_timestamp() -> Column:
    return call_function("current_timestamp")


def date_trunc(unit: str, timestamp: ColumnLike) -> Column:
    return call_function("date_trunc", lit(unit), timestamp)


def asc(column: ColumnLike) -> Column:
    return _to_column(column).asc()


def desc(column: ColumnLike) -> Column:
    return _to_column(column).desc()


def cast(column: ColumnLike, data_type: str) -> Column:
    return _to_column(column).cast(data_type)


def try_cast(column: ColumnLike, data_type: str) -> Column:
    return _to_column(column).try_cast(data_type)


# ── Window functions ──────────────────────────────────────────────────────────
#
# Chain with `.over(partition_by=[...], order_by=[...])` and optionally
# `.rows_between(start, end)` / `.range_between(start, end)`, e.g.
# `rank().over(partition_by=[col("dept")], order_by=[desc(col("salary"))])`.


def row_number() -> Column:
    return _row_number()


def rank() -> Column:
    return _rank()


def dense_rank() -> Column:
    return _dense_rank()


def percent_rank() -> Column:
    return _percent_rank()


def cume_dist() -> Column:
    return _cume_dist()


def ntile(n: int) -> Column:
    return _ntile(n)


def lag(column: ColumnLike, offset: int = 1, default: ColumnLike = None) -> Column:
    default_column = None if default is None else _to_column(default)
    return _lag(_to_column(column), offset, default_column)


def lead(column: ColumnLike, offset: int = 1, default: ColumnLike = None) -> Column:
    default_column = None if default is None else _to_column(default)
    return _lead(_to_column(column), offset, default_column)


def first_value(column: ColumnLike) -> Column:
    return _first_value(_to_column(column))


def last_value(column: ColumnLike) -> Column:
    return _last_value(_to_column(column))


def nth_value(column: ColumnLike, n: int) -> Column:
    return _nth_value(_to_column(column), n)


# ── Helpers ─────────────────────────────────────────────────────────────────


def _flatten(cols: tuple) -> list:
    """Accept both ``f(a, b, c)`` and ``f([a, b, c])`` call styles (PySpark
    functions accept either a single iterable or varargs)."""
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        return list(cols[0])
    return list(cols)


def _sql(value: ColumnLike) -> str:
    return _to_column(value).sql()


# ── Conditional / null handling ─────────────────────────────────────────────
#
# `when(...).when(...).otherwise(...)` builds a SQL CASE expression. The
# returned value is a real Column, so `F.when(c, v)` on its own is already a
# valid `CASE WHEN c THEN v END` (implicit `ELSE NULL`), exactly like PySpark.


def when(condition: Column, value: Any) -> Column:
    """`CASE WHEN condition THEN value END` — chainable with
    `.when(...)`/`.otherwise(...)` (PySpark `F.when`).

    Like PySpark, a non-``Column`` ``value`` is a **literal** (not a column
    reference), so ``when(cond, "yes")`` yields the string ``'yes'``."""
    if not isinstance(condition, Column):
        raise TypeError("when() condition must be a Column boolean expression")
    return _when(condition, value)


def nvl(column: ColumnLike, replacement: ColumnLike) -> Column:
    return coalesce(column, replacement)


def nvl2(column: ColumnLike, value_if_not_null: ColumnLike, value_if_null: ColumnLike) -> Column:
    return _expr(
        f"CASE WHEN ({_sql(column)}) IS NOT NULL "
        f"THEN ({_sql(value_if_not_null)}) ELSE ({_sql(value_if_null)}) END"
    )


def greatest(*columns: ColumnLike) -> Column:
    cols = _flatten(columns)
    if not cols:
        raise ValueError("greatest requires at least one argument")
    return call_function("greatest", *cols)


def least(*columns: ColumnLike) -> Column:
    cols = _flatten(columns)
    if not cols:
        raise ValueError("least requires at least one argument")
    return call_function("least", *cols)


def nanvl(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("nanvl", col1, col2)


# ── String functions ────────────────────────────────────────────────────────


def ascii(column: ColumnLike) -> Column:  # noqa: A001
    return call_function("ascii", column)


def chr(column: ColumnLike) -> Column:  # noqa: A001
    return call_function("chr", column)


def initcap(column: ColumnLike) -> Column:
    return call_function("initcap", column)


def btrim(column: ColumnLike) -> Column:
    return call_function("btrim", column)


def reverse(column: ColumnLike) -> Column:
    return call_function("reverse", column)


def repeat(column: ColumnLike, n: int) -> Column:
    return call_function("repeat", column, lit(n))


def instr(column: ColumnLike, substr: str) -> Column:
    """1-based position of the first occurrence of ``substr`` (PySpark
    `F.instr`); 0 when absent."""
    return call_function("strpos", column, lit(substr))


def locate(substr: str, column: ColumnLike, pos: int = 1) -> Column:
    """1-based position of ``substr`` in ``column`` (PySpark `F.locate`)."""
    if pos != 1:
        return call_function("strpos", call_function("substr", column, lit(pos)), lit(substr))
    return call_function("strpos", column, lit(substr))


def lpad(column: ColumnLike, length: int, pad: str = " ") -> Column:
    return call_function("lpad", column, lit(length), lit(pad))


def rpad(column: ColumnLike, length: int, pad: str = " ") -> Column:
    return call_function("rpad", column, lit(length), lit(pad))


def split_part(column: ColumnLike, delimiter: str, part: int) -> Column:
    return call_function("split_part", column, lit(delimiter), lit(part))


def translate(column: ColumnLike, matching: str, replacing: str) -> Column:
    return call_function("translate", column, lit(matching), lit(replacing))


def left(column: ColumnLike, n: int) -> Column:
    return call_function("left", column, lit(n))


def right(column: ColumnLike, n: int) -> Column:
    return call_function("right", column, lit(n))


def substring(column: ColumnLike, pos: int, length: int) -> Column:
    """1-based substring (PySpark `F.substring`)."""
    return call_function("substr", column, lit(pos), lit(length))


def substr(column: ColumnLike, pos: ColumnLike, length: Optional[ColumnLike] = None) -> Column:
    if length is None:
        return call_function("substr", column, pos)
    return call_function("substr", column, pos, length)


def concat_ws(sep: str, *columns: ColumnLike) -> Column:
    cols = _flatten(columns)
    return call_function("concat_ws", lit(sep), *cols)


def _search_arg(value: ColumnLike) -> Column:
    """A search term (prefix/suffix/substring): a bare ``str`` is a **literal**,
    not a column reference (matches PySpark `Column.startswith` etc.)."""
    return value if isinstance(value, Column) else lit(value)


def starts_with(column: ColumnLike, prefix: ColumnLike) -> Column:
    return call_function("starts_with", column, _search_arg(prefix))


def ends_with(column: ColumnLike, suffix: ColumnLike) -> Column:
    return call_function("ends_with", column, _search_arg(suffix))


def contains(column: ColumnLike, substr: ColumnLike) -> Column:
    return call_function("strpos", column, _search_arg(substr)) > lit(0)


def char_length(column: ColumnLike) -> Column:
    return call_function("character_length", column)


def octet_length(column: ColumnLike) -> Column:
    return call_function("octet_length", column)


def bit_length(column: ColumnLike) -> Column:
    return call_function("bit_length", column)


def regexp_replace(column: ColumnLike, pattern: str, replacement: str) -> Column:
    """Regex replace. NOTE: the engine uses the Rust ``regex`` dialect, which
    differs from Java/Spark regex for some advanced constructs."""
    return call_function("regexp_replace", column, lit(pattern), lit(replacement))


def regexp_like(column: ColumnLike, pattern: str) -> Column:
    """Regex match test (Rust ``regex`` dialect; see `regexp_replace`)."""
    return call_function("regexp_like", column, lit(pattern))


# ── Math functions ──────────────────────────────────────────────────────────


def acos(column: ColumnLike) -> Column:
    return call_function("acos", column)


def asin(column: ColumnLike) -> Column:
    return call_function("asin", column)


def atan(column: ColumnLike) -> Column:
    return call_function("atan", column)


def atan2(y: ColumnLike, x: ColumnLike) -> Column:
    return call_function("atan2", y, x)


def cosh(column: ColumnLike) -> Column:
    return call_function("cosh", column)


def sinh(column: ColumnLike) -> Column:
    return call_function("sinh", column)


def tanh(column: ColumnLike) -> Column:
    return call_function("tanh", column)


def cot(column: ColumnLike) -> Column:
    return call_function("cot", column)


def degrees(column: ColumnLike) -> Column:
    return call_function("degrees", column)


def radians(column: ColumnLike) -> Column:
    return call_function("radians", column)


def ln(column: ColumnLike) -> Column:
    return call_function("ln", column)


def log2(column: ColumnLike) -> Column:
    return call_function("log2", column)


def log10(column: ColumnLike) -> Column:
    return call_function("log10", column)


def power(base: ColumnLike, exponent: ColumnLike) -> Column:
    return call_function("power", base, exponent)


def pow(base: ColumnLike, exponent: ColumnLike) -> Column:  # noqa: A001
    return power(base, exponent)


def cbrt(column: ColumnLike) -> Column:
    return call_function("cbrt", column)


def signum(column: ColumnLike) -> Column:
    return call_function("signum", column)


def sign(column: ColumnLike) -> Column:
    return signum(column)


def factorial(column: ColumnLike) -> Column:
    return call_function("factorial", column)


def gcd(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("gcd", col1, col2)


def lcm(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("lcm", col1, col2)


def pi() -> Column:
    return call_function("pi")


def rand() -> Column:
    """Uniform random double in [0, 1) (PySpark `F.rand`, unseeded)."""
    return call_function("random")


def uuid() -> Column:
    return call_function("uuid")


# ── Aggregate functions ─────────────────────────────────────────────────────


def stddev(column: ColumnOrName) -> Column:
    return call_function("stddev", column)


def stddev_samp(column: ColumnOrName) -> Column:
    return call_function("stddev_samp", column)


def stddev_pop(column: ColumnOrName) -> Column:
    return call_function("stddev_pop", column)


def variance(column: ColumnOrName) -> Column:
    return call_function("var_samp", column)


def var_samp(column: ColumnOrName) -> Column:
    return call_function("var_samp", column)


def var_pop(column: ColumnOrName) -> Column:
    return call_function("var_pop", column)


def corr(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return call_function("corr", col1, col2)


def covar_samp(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return call_function("covar_samp", col1, col2)


def covar_pop(col1: ColumnOrName, col2: ColumnOrName) -> Column:
    return call_function("covar_pop", col1, col2)


def approx_count_distinct(column: ColumnOrName) -> Column:
    return call_function("approx_distinct", column)


def array_agg(column: ColumnOrName) -> Column:
    return call_function("array_agg", column)


def collect_list(column: ColumnOrName) -> Column:
    """Aggregate values into an array (PySpark `F.collect_list`)."""
    return call_function("array_agg", column)


def bit_and(column: ColumnOrName) -> Column:
    return call_function("bit_and", column)


def bit_or(column: ColumnOrName) -> Column:
    return call_function("bit_or", column)


def bit_xor(column: ColumnOrName) -> Column:
    return call_function("bit_xor", column)


def bool_and(column: ColumnOrName) -> Column:
    return call_function("bool_and", column)


def bool_or(column: ColumnOrName) -> Column:
    return call_function("bool_or", column)


def first(column: ColumnOrName) -> Column:
    return _first_value(_to_column(column))


def last(column: ColumnOrName) -> Column:
    return _last_value(_to_column(column))


def median(column: ColumnOrName) -> Column:
    return call_function("median", column)


def count_distinct(*columns: ColumnLike) -> Column:
    cols = _flatten(columns)
    if not cols:
        raise ValueError("count_distinct requires at least one argument")
    inside = ", ".join(_sql(c) for c in cols)
    return _expr(f"COUNT(DISTINCT {inside})")


def sum_distinct(column: ColumnOrName) -> Column:
    return _expr(f"SUM(DISTINCT {_sql(column)})")


# ── Date / time functions ───────────────────────────────────────────────────
#
# Only extractions whose value matches Spark exactly are exposed. Pattern-based
# formatting (`date_format`, `to_date(fmt)`) is deliberately left to `expr(...)`
# because Spark's Java pattern grammar differs from the engine's chrono grammar.


def now() -> Column:
    return call_function("now")


def to_date(column: ColumnLike) -> Column:
    return call_function("to_date", column)


def to_timestamp(column: ColumnLike) -> Column:
    return call_function("to_timestamp", column)


def _date_part(part: str, column: ColumnLike) -> Column:
    return call_function("date_part", lit(part), column)


def year(column: ColumnLike) -> Column:
    return _date_part("year", column)


def month(column: ColumnLike) -> Column:
    return _date_part("month", column)


def day(column: ColumnLike) -> Column:
    return _date_part("day", column)


def dayofmonth(column: ColumnLike) -> Column:
    return _date_part("day", column)


def hour(column: ColumnLike) -> Column:
    return _date_part("hour", column)


def minute(column: ColumnLike) -> Column:
    return _date_part("minute", column)


def second(column: ColumnLike) -> Column:
    return _date_part("second", column)


def quarter(column: ColumnLike) -> Column:
    return _date_part("quarter", column)


# ── Array / collection functions ────────────────────────────────────────────


def array(*columns: ColumnLike) -> Column:
    return call_function("make_array", *_flatten(columns))


def array_contains(column: ColumnLike, value: ColumnLike) -> Column:
    return call_function("array_contains", column, value)


def array_distinct(column: ColumnLike) -> Column:
    return call_function("array_distinct", column)


def array_position(column: ColumnLike, value: ColumnLike) -> Column:
    return call_function("array_position", column, value)


def array_remove(column: ColumnLike, value: ColumnLike) -> Column:
    return call_function("array_remove", column, value)


def array_append(column: ColumnLike, value: ColumnLike) -> Column:
    return call_function("array_append", column, value)


def array_prepend(value: ColumnLike, column: ColumnLike) -> Column:
    return call_function("array_prepend", value, column)


def array_union(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("array_union", col1, col2)


def array_intersect(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("array_intersect", col1, col2)


def array_except(col1: ColumnLike, col2: ColumnLike) -> Column:
    return call_function("array_except", col1, col2)


def array_length(column: ColumnLike) -> Column:
    return call_function("array_length", column)


def cardinality(column: ColumnLike) -> Column:
    return call_function("cardinality", column)


def flatten(column: ColumnLike) -> Column:
    return call_function("flatten", column)


# ── Hash functions ──────────────────────────────────────────────────────────


def md5(column: ColumnLike) -> Column:
    return call_function("md5", column)


def sha256(column: ColumnLike) -> Column:
    """SHA-256 as a lowercase hex string (PySpark `F.sha2(col, 256)`)."""
    return call_function("encode", call_function("sha256", column), lit("hex"))


def sha512(column: ColumnLike) -> Column:
    """SHA-512 as a lowercase hex string (PySpark `F.sha2(col, 512)`)."""
    return call_function("encode", call_function("sha512", column), lit("hex"))


__all__ = [
    # core / literals
    "col",
    "column",
    "lit",
    "expr",
    "call_function",
    "function",
    "asc",
    "desc",
    "cast",
    "try_cast",
    # conditional / null
    "when",
    "coalesce",
    "ifnull",
    "nullif",
    "nvl",
    "nvl2",
    "nanvl",
    "greatest",
    "least",
    "isnull",
    "isnotnull",
    "isnan",
    # aggregates
    "avg",
    "mean",
    "count",
    "count_all",
    "count_distinct",
    "sum",
    "sum_distinct",
    "min",
    "max",
    "first",
    "last",
    "median",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "corr",
    "covar_samp",
    "covar_pop",
    "approx_count_distinct",
    "array_agg",
    "collect_list",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    # strings
    "upper",
    "lower",
    "length",
    "char_length",
    "octet_length",
    "bit_length",
    "trim",
    "ltrim",
    "rtrim",
    "btrim",
    "concat",
    "concat_ws",
    "ascii",
    "chr",
    "initcap",
    "reverse",
    "repeat",
    "instr",
    "locate",
    "lpad",
    "rpad",
    "left",
    "right",
    "substr",
    "substring",
    "split_part",
    "translate",
    "starts_with",
    "ends_with",
    "contains",
    "regexp_replace",
    "regexp_like",
    # math
    "abs",
    "round",
    "ceil",
    "floor",
    "sqrt",
    "cbrt",
    "exp",
    "log",
    "ln",
    "log2",
    "log10",
    "power",
    "pow",
    "signum",
    "sign",
    "factorial",
    "gcd",
    "lcm",
    "pi",
    "rand",
    "uuid",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "sinh",
    "cosh",
    "tanh",
    "cot",
    "degrees",
    "radians",
    # date / time
    "current_date",
    "current_timestamp",
    "now",
    "to_date",
    "to_timestamp",
    "date_trunc",
    "year",
    "month",
    "day",
    "dayofmonth",
    "hour",
    "minute",
    "second",
    "quarter",
    # arrays / collections
    "array",
    "array_contains",
    "array_distinct",
    "array_position",
    "array_remove",
    "array_append",
    "array_prepend",
    "array_union",
    "array_intersect",
    "array_except",
    "array_length",
    "cardinality",
    "flatten",
    # hashing
    "md5",
    "sha256",
    "sha512",
    # window
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
]
