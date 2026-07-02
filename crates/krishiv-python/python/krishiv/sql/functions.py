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


__all__ = [
    "abs",
    "asc",
    "avg",
    "call_function",
    "cast",
    "ceil",
    "coalesce",
    "col",
    "column",
    "concat",
    "cos",
    "count",
    "count_all",
    "cume_dist",
    "current_date",
    "current_timestamp",
    "date_trunc",
    "dense_rank",
    "desc",
    "exp",
    "expr",
    "first_value",
    "floor",
    "function",
    "ifnull",
    "isnan",
    "isnotnull",
    "isnull",
    "lag",
    "last_value",
    "lead",
    "length",
    "lit",
    "log",
    "lower",
    "ltrim",
    "max",
    "mean",
    "min",
    "nth_value",
    "ntile",
    "nullif",
    "percent_rank",
    "rank",
    "round",
    "row_number",
    "rtrim",
    "sin",
    "sqrt",
    "sum",
    "tan",
    "trim",
    "try_cast",
    "upper",
]
