"""PySpark-compatible convenience layer over Krishiv's native classes.

Krishiv's native ``DataFrame``/``Session`` API is deliberately Pythonic and
snake_case, and in several places (lazy ``QueryResult``, ``group_by`` on raw
SQL, ``with_column`` on SQL text) is a better fit than PySpark's. This module
does **not** replace any of that — it is purely *additive*: it grafts the
PySpark spellings (``groupBy``, ``orderBy``, ``withColumn``, ``createDataFrame``,
``df.write.mode(...).parquet(...)``, ``session.read.format(...).load(...)``,
``df.na``/``df.stat``, ``session.catalog`` …) onto the same native objects so
that code written for ``pyspark.sql`` runs against Krishiv unchanged.

Native methods that PySpark also defines with *different* semantics are wrapped
so the native calling convention still works (e.g. ``select(["a", "b"])`` and
``select("a", col("b"))`` both work; ``sort(["x"], [True])`` still works).
"""

from __future__ import annotations

from typing import Any, Optional

import inspect

from .krishiv import (
    Batch,
    Column,
    DataFrame,
    GroupedDataFrame,
    QueryResult,
    Session,
    call_function,
    col,
    expr,
    lit,
    udf as _native_udf,
)


# ── Row ─────────────────────────────────────────────────────────────────────


class Row(dict):
    """A result row with both ``row['name']`` and ``row.name`` access
    (PySpark ``Row``)."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def asDict(self, recursive: bool = False) -> dict:  # noqa: ARG002
        return dict(self)

    def __repr__(self) -> str:
        return "Row(" + ", ".join(f"{k}={v!r}" for k, v in self.items()) + ")"


# ── helpers ─────────────────────────────────────────────────────────────────


def _flatten(cols: tuple) -> list:
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        return list(cols[0])
    return list(cols)


def _as_column(value: Any) -> Column:
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        return col(value)
    return lit(value)


def _rows(result: QueryResult) -> list:
    """Materialise a QueryResult as a list of :class:`Row` (needs pyarrow)."""
    table = result.to_arrow()
    return [Row(**record) for record in table.to_pylist()]


class _Explode:
    """Marker produced by ``F.explode`` / ``F.posexplode``. It is not a Column;
    the DataFrame ``select``/``withColumn`` adapters recognise it and route
    through the engine's ``unnest`` so the row count expands (a plain Column
    cannot, since explode changes cardinality)."""

    def __init__(self, column, pos: bool = False) -> None:
        self.column = _as_column(column)
        self.pos = pos
        self.col_name = "col"
        self.pos_name = "pos"

    def alias(self, *names: str) -> "_Explode":
        if self.pos and len(names) == 2:
            self.pos_name, self.col_name = names
        elif names:
            self.col_name = names[-1]
        return self


def explode(column) -> _Explode:
    """Explode an array column into one row per element (PySpark `F.explode`)."""
    return _Explode(column, pos=False)


def posexplode(column) -> _Explode:
    """Explode an array into ``(pos, col)`` rows, 0-based (PySpark `F.posexplode`)."""
    return _Explode(column, pos=True)


def _apply_explode(df: DataFrame, normal_cols: list, ex: _Explode) -> DataFrame:
    prefix = [_as_column(c) for c in normal_cols]
    if ex.pos:
        # 0-based positions: generate_series(0, cardinality(arr) - 1) is a
        # list column the same length as the exploded array, so unnesting both
        # together zips element/position row-wise.
        cardinality = call_function("cardinality", [ex.column]).cast("int64")
        positions = call_function("generate_series", [lit(0), cardinality - lit(1)])
        proj = prefix + [positions.alias(ex.pos_name), ex.column.alias(ex.col_name)]
        return df.select_columns(proj).unnest([ex.pos_name, ex.col_name])
    proj = prefix + [ex.column.alias(ex.col_name)]
    return df.select_columns(proj).unnest([ex.col_name])


# Native methods we override but must still be able to call in their original
# form (the native list/string calling convention stays supported).
_native = {
    "select": DataFrame.select,
    "filter": DataFrame.filter,
    "sort": DataFrame.sort,
    "grouped_agg": GroupedDataFrame.agg,
    "unpersist": DataFrame.unpersist,
}


def _df_unpersist(self, blocking: bool = False):  # noqa: ARG001
    # PySpark returns the DataFrame for chaining; the native call returns None.
    _native["unpersist"](self)
    return self


def _agg_columns(*exprs, **named):
    """Normalise PySpark ``agg`` arguments — Column varargs, a single list, or
    ``name=expr`` keywords — into a list of aliased Columns; falls back to the
    native SQL-string path when only strings are given."""
    items = _flatten(exprs) if exprs else []
    strings = [e for e in items if isinstance(e, str)]
    columns = [e for e in items if isinstance(e, Column)]
    for alias, e in named.items():
        columns.append((e if isinstance(e, Column) else _as_column(e)).alias(alias))
    return columns, strings


def _grouped_agg(self, *exprs, **named):
    columns, strings = _agg_columns(*exprs, **named)
    if strings and not columns:
        return _native["grouped_agg"](self, strings)
    columns.extend(expr(s) for s in strings)
    return self.agg_columns(columns)


def _df_agg(self, *exprs, **named):
    return _grouped_agg(self.group_by_columns([]), *exprs, **named)


class _GroupingSet:
    """PySpark ``df.rollup(...)`` / ``df.cube(...)`` — a grouping-set builder
    finalized with ``.agg(...)`` or ``.count()``."""

    def __init__(self, df: DataFrame, cols: list, kind: str) -> None:
        self._df = df
        self._groups = [c if isinstance(c, Column) else col(c) for c in cols]
        self._kind = kind

    def agg(self, *exprs, **named):
        columns, strings = _agg_columns(*exprs, **named)
        columns.extend(expr(s) for s in strings)
        grouped = self._df.group_by_columns([])
        if self._kind == "rollup":
            return grouped.rollup(self._groups, columns)
        return grouped.cube(self._groups, columns)

    def count(self):
        from .sql.functions import count_all  # noqa: PLC0415

        return self.agg(count_all().alias("count"))


def _df_rollup(self, *cols):
    return _GroupingSet(self, _flatten(cols), "rollup")


def _df_cube(self, *cols):
    return _GroupingSet(self, _flatten(cols), "cube")


# ── DataFrame: transforms (PySpark camelCase + varargs + Column) ─────────────


def _df_groupBy(self, *cols):
    cols = _flatten(cols)
    if any(isinstance(c, Column) for c in cols):
        return self.group_by_columns([_as_column(c) for c in cols])
    return self.group_by([str(c) for c in cols])


def _df_select(self, *cols):
    cols = _flatten(cols)
    explodes = [c for c in cols if isinstance(c, _Explode)]
    if explodes:
        if len(explodes) > 1:
            raise ValueError("only one explode/posexplode is allowed per select()")
        normal = [c for c in cols if not isinstance(c, _Explode)]
        return _apply_explode(self, normal, explodes[0])
    if any(isinstance(c, Column) for c in cols):
        return self.select_columns([_as_column(c) for c in cols])
    # all plain names — preserve native name-based projection (validates cols)
    return _native["select"](self, [str(c) for c in cols])


def _df_selectExpr(self, *exprs):
    return self.select_exprs(list(_flatten(exprs)))


def _df_filter(self, condition):
    if isinstance(condition, Column):
        return self.filter_column(condition)
    return _native["filter"](self, condition)


def _df_withColumn(self, name: str, column):
    if isinstance(column, _Explode):
        column.col_name = name
        return _apply_explode(self, [col(c) for c in self.columns()], column)
    return self.with_column(name, column.sql() if isinstance(column, Column) else str(column))


def _df_withColumns(self, columns: dict):
    result = self
    for name, column in columns.items():
        result = _df_withColumn(result, name, column)
    return result


def _df_withColumnRenamed(self, existing: str, new: str):
    return self.rename(existing, new)


def _df_withColumnsRenamed(self, mapping: dict):
    return self.with_columns_renamed(list(mapping.items()))


def _df_drop(self, *cols):
    return self.drop_columns([str(c) if not isinstance(c, Column) else c.sql() for c in _flatten(cols)])


_SORT_SUFFIXES = (
    (" DESC NULLS LAST", False),
    (" DESC NULLS FIRST", False),
    (" ASC NULLS LAST", True),
    (" ASC NULLS FIRST", True),
    (" DESC", False),
    (" ASC", True),
)


def _order_spec(column):
    """Return ``(column_name, ascending)`` for a name or a (possibly ``.desc()``)
    Column. The native ordering takes column names plus a direction flag."""
    if isinstance(column, str):
        return column, True
    sql = column.sql().strip()
    ascending = True
    upper = sql.upper()
    for suffix, is_asc in _SORT_SUFFIXES:
        if upper.endswith(suffix):
            ascending = is_asc
            sql = sql[: -len(suffix)].strip()
            break
    if len(sql) >= 2 and sql[0] == '"' and sql[-1] == '"':
        sql = sql[1:-1].replace('""', '"')
    return sql, ascending


def _df_orderBy(self, *cols, ascending=None):
    specs = [_order_spec(c) for c in _flatten(cols)]
    names = [name for name, _ in specs]
    if ascending is None:
        descending = [not asc for _, asc in specs]
    elif isinstance(ascending, bool):
        descending = [not ascending] * len(names)
    else:
        descending = [not bool(a) for a in ascending]
    return _native["sort"](self, names, descending)


def _df_sort(self, *cols, ascending=None, descending=None):
    # Preserve the native convention: sort(["a", "b"], [True, False]).
    if descending is not None or (
        len(cols) == 2
        and isinstance(cols[0], (list, tuple))
        and isinstance(cols[1], (list, tuple))
    ):
        columns = list(cols[0]) if cols and isinstance(cols[0], (list, tuple)) else [str(c) for c in cols]
        desc = descending if descending is not None else list(cols[1])
        return _native["sort"](self, columns, desc)
    return _df_orderBy(self, *cols, ascending=ascending)


def _df_where(self, condition):
    return _df_filter(self, condition)


def _df_dropDuplicates(self, subset=None):
    if not subset:
        return self.distinct()
    partition = [col(c) for c in subset]
    ranked = self.with_column(
        "__krishiv_rn",
        expr("row_number()").over(partition_by=partition, order_by=partition).sql(),
    )
    return ranked.filter_column(col("__krishiv_rn") == lit(1)).drop_columns(["__krishiv_rn"])


def _df_unionByName(self, other, allowMissingColumns: bool = False):
    if not allowMissingColumns:
        # Native union-by-name aligns `other` to this schema by name (and
        # errors on a column mismatch, matching PySpark).
        return self.union_by_name(other)
    left_cols = self.columns()
    right_cols = other.columns()
    ordered = list(left_cols) + [c for c in right_cols if c not in left_cols]
    left = self.select_columns(
        [col(c) if c in left_cols else lit(None).alias(c) for c in ordered]
    )
    right = other.select_columns(
        [col(c) if c in right_cols else lit(None).alias(c) for c in ordered]
    )
    return left.union(right)


def _df_unionAll(self, other):
    return self.union(other)


def _df_crossJoin(self, other):
    # The engine expresses a cartesian product as an inner join with no keys.
    return self.join(other, [], how="inner")


def _df_subtract(self, other):
    return self.except_distinct(other)


def _df_toDF(self, *names):
    names = _flatten(names)
    current = self.columns()
    if len(names) != len(current):
        raise ValueError(f"toDF expected {len(current)} names, got {len(names)}")
    return self.select_columns([col(old).alias(new) for old, new in zip(current, names)])


# ── DataFrame: actions ──────────────────────────────────────────────────────


def _df_count(self):
    return self.num_rows()


def _df_take(self, num: int):
    return _rows(self.limit(num).collect())


def _df_head(self, n=None):
    if n is None:
        rows = _rows(self.limit(1).collect())
        return rows[0] if rows else None
    return _rows(self.limit(n).collect())


def _df_first(self):
    return _df_head(self, None)


def _df_tail(self, num: int):
    rows = _rows(self.collect())
    return rows[-num:]


def _df_collect_rows(self):
    """PySpark-shaped ``collect()`` result as a list of :class:`Row`.

    ``DataFrame.collect()`` keeps returning Krishiv's richer ``QueryResult``
    (with ``.to_pandas()`` / ``.to_arrow()``); use ``collect_rows()`` /
    ``take()`` / ``head()`` for a PySpark ``List[Row]``."""
    return _rows(self.collect())


def _df_toPandas(self):
    return self.collect().to_pandas()


def _df_toLocalIterator(self):
    return iter(_rows(self.collect()))


def _df_isEmpty(self):
    return self.limit(1).num_rows() == 0


def _df_printSchema(self):
    print("root")
    for name, type_name in self.schema():
        print(f" |-- {name}: {type_name} (nullable = true)")


def _df_dtypes(self):
    return [(name, type_name) for name, type_name in self.schema()]


# ── DataFrame: na / stat accessors ──────────────────────────────────────────


def _na_type_matches(value, type_str: str) -> bool:
    """Whether a ``na.fill`` scalar may fill a column of this engine type
    (PySpark only fills type-compatible columns)."""
    t = type_str.lower()
    if isinstance(value, bool):
        return "bool" in t
    if isinstance(value, (int, float)):
        return any(k in t for k in ("int", "float", "double", "decimal"))
    if isinstance(value, str):
        return "utf8" in t or "string" in t
    return True


class DataFrameNaFunctions:
    """``df.na`` — null handling (PySpark ``DataFrameNaFunctions``)."""

    def __init__(self, df: DataFrame) -> None:
        self._df = df

    def drop(self, how: str = "any", thresh: Optional[int] = None, subset=None):
        if how not in ("any", "all"):
            raise ValueError("how must be 'any' or 'all'")
        if thresh is not None or how == "all":
            raise NotImplementedError(
                "na.drop currently supports how='any' without thresh; "
                "use SQL for how='all'/thresh"
            )
        columns = list(subset) if subset else []
        return self._df.drop_nulls(columns)

    def fill(self, value, subset=None):
        df = self._df
        # ``fill_null`` interpolates the value as raw SQL (COALESCE(col, value)),
        # so a Python value must become a proper SQL literal — ``lit(...).sql()``
        # quotes strings (escaping embedded quotes) and renders numbers/bools.
        if isinstance(value, dict):
            for column, val in value.items():
                df = df.fill_null(column, lit(val).sql())
            return df
        # PySpark fills only columns whose type matches the scalar's type
        # (a numeric value never touches a string column, and vice versa).
        schema = dict(df.schema())
        candidates = list(subset) if subset else list(schema)
        literal = lit(value).sql()
        for column in candidates:
            if _na_type_matches(value, schema.get(column, "")):
                df = df.fill_null(column, literal)
        return df


class DataFrameStatFunctions:
    """``df.stat`` — a composable subset of PySpark ``DataFrameStatFunctions``."""

    def __init__(self, df: DataFrame) -> None:
        self._df = df

    def _global_agg(self, aggregate) -> Any:
        rows = _rows(self._df.group_by_columns([]).agg_columns([aggregate.alias("c")]).collect())
        return rows[0]["c"] if rows else None

    def corr(self, col1: str, col2: str, method: str = "pearson") -> float:
        if method != "pearson":
            raise NotImplementedError("only the 'pearson' correlation is supported")
        from .sql.functions import corr as _corr  # noqa: PLC0415

        return self._global_agg(_corr(col1, col2))

    def cov(self, col1: str, col2: str) -> float:
        from .sql.functions import covar_samp as _covar  # noqa: PLC0415

        return self._global_agg(_covar(col1, col2))


# ── Session: createDataFrame / range / catalog / read ───────────────────────


_TEMP_COUNTER = {"n": 0}


def _fresh_name(prefix: str) -> str:
    _TEMP_COUNTER["n"] += 1
    return f"__krishiv_{prefix}_{_TEMP_COUNTER['n']}"


def _session_createDataFrame(self, data, schema=None):
    """Create a DataFrame from Python data (PySpark ``createDataFrame``).

    ``data`` may be a list of tuples/lists, a list of dicts, or a pandas
    DataFrame. ``schema`` may be a :class:`krishiv.types.StructType`, a list of
    column names, or ``None`` (inferred)."""
    import pyarrow as pa  # noqa: PLC0415

    from . import types as _types  # noqa: PLC0415

    arrow_schema = None
    names = None
    if isinstance(schema, _types.StructType):
        arrow_schema = schema.to_arrow()
    elif isinstance(schema, (list, tuple)) and all(isinstance(s, str) for s in schema):
        names = list(schema)

    if pa is not None and hasattr(data, "to_records"):  # pandas DataFrame
        table = pa.Table.from_pandas(data, preserve_index=False)
    elif data and isinstance(data[0], dict):
        table = pa.Table.from_pylist([dict(r) for r in data], schema=arrow_schema)
    else:
        rows = [tuple(r) for r in data]
        if names is None and arrow_schema is not None:
            names = [f.name for f in arrow_schema]
        if names is None:
            width = len(rows[0]) if rows else 0
            names = [f"_{i + 1}" for i in range(width)]
        columns = list(zip(*rows)) if rows else [[] for _ in names]
        arrays = [pa.array(list(colvals)) for colvals in columns] or [
            pa.array([]) for _ in names
        ]
        table = pa.Table.from_arrays(arrays, names=names)
        if arrow_schema is not None:
            table = table.cast(arrow_schema)

    name = _fresh_name("df")
    self.register_record_batches(name, [Batch(b) for b in table.to_batches()])
    return self.table(name)


def _session_range(self, start, end=None, step=1, numPartitions=None):  # noqa: ARG001
    """PySpark ``SparkSession.range`` — a DataFrame with a single ``id`` column."""
    import pyarrow as pa  # noqa: PLC0415

    if end is None:
        start, end = 0, start
    ids = list(range(start, end, step))
    table = pa.table({"id": pa.array(ids, type=pa.int64())})
    name = _fresh_name("range")
    self.register_record_batches(name, [Batch(b) for b in table.to_batches()])
    return self.table(name)


_UDF_TYPE_ALIASES = {
    "int": "int64",
    "integer": "int64",
    "long": "int64",
    "bigint": "int64",
    "short": "int64",
    "byte": "int64",
    "float": "double",
    "real": "double",
    "str": "string",
    "bool": "boolean",
}


def _udf_type(dtype) -> str:
    """Normalise a UDF type: a `krishiv.types.DataType`, a PySpark type name, or
    an engine type string, to the engine's UDF type grammar."""
    if hasattr(dtype, "cast_string"):
        dtype = dtype.cast_string()
    name = str(dtype).strip().lower()
    return _UDF_TYPE_ALIASES.get(name, name)


class UDFRegistration:
    """``session.udf`` — register Python scalar UDFs (PySpark ``spark.udf``).

    ``register(name, f, returnType)`` wraps a plain Python scalar function
    (called once per row) over the engine's columnar UDF interface and returns
    a callable usable in DataFrame expressions::

        square = spark.udf.register("square", lambda x: x * x, "int")
        df.select(square(col("n")))

    ``returnType`` accepts a ``krishiv.types`` object, a PySpark type name
    (``"int"``, ``"string"``…), or an engine type string. Argument types
    default to ``returnType``; pass ``argTypes=[...]`` for heterogeneous inputs
    (the engine is typed, so an int column bound to a declared string arg is
    fine, but not the reverse)."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def register(self, name: str, f, returnType="string", argTypes=None):  # noqa: N803
        try:
            arity = sum(
                1
                for p in inspect.signature(f).parameters.values()
                if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY)
            )
        except (TypeError, ValueError):
            arity = len(argTypes) if argTypes else 1
        out_type = _udf_type(returnType)
        arg_types = [_udf_type(t) for t in argTypes] if argTypes else [out_type] * arity
        params = [f"a{i}" for i in range(len(arg_types))]
        input_types = dict(zip(params, arg_types))

        def _batch(cols):
            columns = [cols[p] for p in params]
            return [f(*row) for row in zip(*columns)]

        wrapped = _native_udf(
            _batch, name=name, input_types=input_types, output_type=out_type
        )
        self._session.register_udf(wrapped)

        def _invoke(*cols):
            return call_function(name, [_as_column(c) for c in cols])

        _invoke.__name__ = name
        return _invoke


class Catalog:
    """``session.catalog`` — a subset of PySpark's ``Catalog``."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def listTables(self, dbName: Optional[str] = None):  # noqa: ARG002
        return self._session.list_tables()

    def tableExists(self, tableName: str, dbName: Optional[str] = None) -> bool:  # noqa: ARG002
        return self._session.table_exists(tableName)

    def dropTempView(self, viewName: str) -> bool:
        if self._session.table_exists(viewName):
            self._session.deregister_table(viewName)
            return True
        return False

    def dropGlobalTempView(self, viewName: str) -> bool:
        return self.dropTempView(viewName)

    def listColumns(self, tableName: str, dbName: Optional[str] = None):  # noqa: ARG002
        return [name for name, _ in self._session.table(tableName).schema()]


class DataFrameReader:
    """``session.read`` — a fluent, PySpark-shaped reader."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._format = None
        self._options: dict = {}

    def format(self, source: str) -> "DataFrameReader":
        self._format = source.lower()
        return self

    def option(self, key: str, value) -> "DataFrameReader":
        self._options[key] = value
        return self

    def options(self, **kwargs) -> "DataFrameReader":
        self._options.update(kwargs)
        return self

    def schema(self, schema) -> "DataFrameReader":  # noqa: ARG002
        # Schema-on-read is inferred by the engine; accepted for compatibility.
        return self

    def load(self, path: Optional[str] = None, format: Optional[str] = None):  # noqa: A002
        fmt = (format or self._format or "parquet").lower()
        if fmt == "parquet":
            return self._session.read_parquet(path)
        if fmt == "csv":
            header = self._options.get("header", True)
            header = header in (True, "true", "True")
            delimiter = self._options.get("sep", self._options.get("delimiter", ","))
            if not isinstance(delimiter, str):
                delimiter = chr(delimiter)
            return self._session.read_csv_with_options(
                path, has_header=header, delimiter=delimiter
            )
        if fmt in ("json", "ndjson"):
            return self._session.read_json(path)
        raise ValueError(f"unsupported read format: {fmt!r}")

    def parquet(self, path: str):
        return self._session.read_parquet(path)

    def csv(self, path: str, header=None, sep=None, **options):
        if header is not None:
            self._options["header"] = header
        if sep is not None:
            self._options["sep"] = sep
        self._options.update(options)
        return self.load(path, format="csv")

    def json(self, path: str):
        return self._session.read_json(path)

    def table(self, name: str):
        return self._session.table(name)


class DataFrameWriter:
    """``df.write`` — a fluent, PySpark-shaped writer."""

    def __init__(self, df: DataFrame) -> None:
        self._df = df
        self._format = "parquet"
        self._mode = "error"
        self._options: dict = {}
        self._partition_by: list = []

    def format(self, source: str) -> "DataFrameWriter":
        self._format = source.lower()
        return self

    def mode(self, save_mode: str) -> "DataFrameWriter":
        self._mode = save_mode
        return self

    def option(self, key: str, value) -> "DataFrameWriter":
        self._options[key] = value
        return self

    def options(self, **kwargs) -> "DataFrameWriter":
        self._options.update(kwargs)
        return self

    def partitionBy(self, *cols) -> "DataFrameWriter":
        self._partition_by = _flatten(cols)
        return self

    def save(self, path: Optional[str] = None, format: Optional[str] = None, mode: Optional[str] = None):  # noqa: A002
        fmt = (format or self._format or "parquet").lower()
        self._df.write_file(
            path,
            fmt,
            mode=mode or self._mode,
            partition_by=[str(c) for c in self._partition_by],
        )

    def parquet(self, path: str, mode: Optional[str] = None):
        return self.save(path, format="parquet", mode=mode)

    def csv(self, path: str, mode: Optional[str] = None):
        return self.save(path, format="csv", mode=mode)

    def json(self, path: str, mode: Optional[str] = None):
        return self.save(path, format="json", mode=mode)

    def saveAsTable(self, name: str, format: Optional[str] = None, mode: Optional[str] = None):  # noqa: A002, ARG002
        # Register the collected frame as a named table in the session catalog.
        raise NotImplementedError(
            "saveAsTable requires a session handle; register with "
            "session.register_dataframe(name, df) instead"
        )


class _SessionBuilder:
    """``SparkSession.builder`` shim — options are accepted for compatibility;
    ``getOrCreate()`` returns an embedded Krishiv session."""

    def __init__(self) -> None:
        self._config: dict = {}

    def appName(self, name: str) -> "_SessionBuilder":  # noqa: ARG002
        return self

    def master(self, master: str) -> "_SessionBuilder":  # noqa: ARG002
        return self

    def config(self, key=None, value=None, **kwargs) -> "_SessionBuilder":
        if key is not None:
            self._config[key] = value
        self._config.update(kwargs)
        return self

    def remote(self, url: str):
        return Session.connect(url)

    def getOrCreate(self) -> Session:
        return Session.embedded()


def _apply() -> None:
    """Graft every PySpark-shaped name onto the native classes."""
    # DataFrame transforms
    DataFrame.groupBy = _df_groupBy
    DataFrame.groupby = _df_groupBy
    DataFrame.select = _df_select
    DataFrame.selectExpr = _df_selectExpr
    DataFrame.filter = _df_filter
    DataFrame.where = _df_where
    DataFrame.withColumn = _df_withColumn
    DataFrame.withColumns = _df_withColumns
    DataFrame.withColumnRenamed = _df_withColumnRenamed
    DataFrame.withColumnsRenamed = _df_withColumnsRenamed
    DataFrame.drop = _df_drop
    DataFrame.orderBy = _df_orderBy
    DataFrame.sort = _df_sort
    DataFrame.dropDuplicates = _df_dropDuplicates
    DataFrame.drop_duplicates = _df_dropDuplicates
    DataFrame.agg = _df_agg
    DataFrame.rollup = _df_rollup
    DataFrame.cube = _df_cube
    DataFrame.unpersist = _df_unpersist
    GroupedDataFrame.agg = _grouped_agg
    DataFrame.unionByName = _df_unionByName
    DataFrame.unionAll = _df_unionAll
    DataFrame.crossJoin = _df_crossJoin
    DataFrame.subtract = _df_subtract
    DataFrame.toDF = _df_toDF
    # DataFrame actions
    DataFrame.count = _df_count
    DataFrame.take = _df_take
    DataFrame.head = _df_head
    DataFrame.first = _df_first
    DataFrame.tail = _df_tail
    DataFrame.collect_rows = _df_collect_rows
    DataFrame.toPandas = _df_toPandas
    DataFrame.toLocalIterator = _df_toLocalIterator
    DataFrame.isEmpty = _df_isEmpty
    DataFrame.printSchema = _df_printSchema
    DataFrame.dtypes = property(_df_dtypes)
    DataFrame.na = property(DataFrameNaFunctions)
    # PySpark direct aliases for the na functions (df.fillna / df.dropna).
    DataFrame.fillna = lambda self, value, subset=None: DataFrameNaFunctions(self).fill(value, subset=subset)
    DataFrame.dropna = lambda self, how="any", thresh=None, subset=None: DataFrameNaFunctions(self).drop(how=how, thresh=thresh, subset=subset)
    DataFrame.stat = property(DataFrameStatFunctions)
    DataFrame.write = property(DataFrameWriter)
    # Session parity
    Session.createDataFrame = _session_createDataFrame
    Session.range = _session_range
    Session.catalog = property(Catalog)
    Session.udf = property(UDFRegistration)
    Session.read = property(DataFrameReader)
    Session.stop = Session.close
    Session.builder = _SessionBuilder()
    # Structured-streaming name parity (Krishiv keeps its native streaming DSL,
    # which is exposed here under the PySpark spellings).
    Session.readStream = property(lambda self: self.read_stream())
    DataFrame.writeStream = property(lambda self: self.write_stream())
