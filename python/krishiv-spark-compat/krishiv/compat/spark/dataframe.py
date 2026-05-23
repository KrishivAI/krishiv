"""DataFrame shim — builds SQL and executes via Spark Connect or embedded Krishiv."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple, Union

try:
    import pyarrow as pa
except ImportError:  # pragma: no cover
    pa = None


class Column:
    def __init__(self, sql: str) -> None:
        self.sql = sql

    def __gt__(self, other: Any) -> Column:
        rhs = other.sql if isinstance(other, Column) else repr(other)
        return Column(f"({self.sql} > {rhs})")

    def __eq__(self, other: Any) -> Column:
        rhs = other.sql if isinstance(other, Column) else repr(other)
        return Column(f"({self.sql} = {rhs})")


class DataFrame:
    def __init__(self, session: Any, query: str) -> None:
        self._session = session
        self._query = query

    def _wrap(self, query: str) -> DataFrame:
        return DataFrame(self._session, query)

    def filter(self, condition: Union[Column, str]) -> DataFrame:
        cond = condition.sql if isinstance(condition, Column) else condition
        return self._wrap(f"SELECT * FROM ({self._query}) WHERE {cond}")

    def where(self, condition: Union[Column, str]) -> DataFrame:
        return self.filter(condition)

    def select(self, *cols: str) -> DataFrame:
        inner = ", ".join(cols)
        return self._wrap(f"SELECT {inner} FROM ({self._query})")

    def selectExpr(self, *exprs: str) -> DataFrame:
        inner = ", ".join(exprs)
        return self._wrap(f"SELECT {inner} FROM ({self._query})")

    def groupBy(self, *cols: str) -> _GroupedData:
        return _GroupedData(self, cols)

    def orderBy(self, *cols: str) -> DataFrame:
        inner = ", ".join(cols)
        return self._wrap(f"SELECT * FROM ({self._query}) ORDER BY {inner}")

    def sort(self, *cols: str) -> DataFrame:
        return self.orderBy(*cols)

    def limit(self, n: int) -> DataFrame:
        return self._wrap(f"SELECT * FROM ({self._query}) LIMIT {n}")

    def distinct(self) -> DataFrame:
        return self._wrap(f"SELECT DISTINCT * FROM ({self._query})")

    def drop(self, *cols: str) -> DataFrame:
        # Simplified: requires subquery with known schema; use SELECT * for tests.
        return self._wrap(f"SELECT * FROM ({self._query})")

    def join(self, other: DataFrame, on: str, how: str = "inner") -> DataFrame:
        how_sql = how.upper().replace("_", " ")
        return self._wrap(
            f"SELECT * FROM ({self._query}) {how_sql} JOIN ({other._query}) ON {on}"
        )

    def union(self, other: DataFrame) -> DataFrame:
        return self._wrap(f"{self._query} UNION {other._query}")

    def unionAll(self, other: DataFrame) -> DataFrame:
        return self._wrap(f"{self._query} UNION ALL {other._query}")

    def count(self) -> int:
        batches = self._collect_batches()
        if not batches:
            return 0
        return sum(b.num_rows for b in batches)

    def collect(self) -> List[Tuple]:
        batches = self._collect_batches()
        rows: List[Tuple] = []
        for batch in batches:
            cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            for i in range(batch.num_rows):
                rows.append(tuple(c[i] for c in cols))
        return rows

    def show(self, n: int = 20) -> None:
        for row in self.limit(n).collect():
            print(row)

    def printSchema(self) -> None:
        batches = self._collect_batches()
        if batches:
            print(batches[0].schema)
        else:
            print("Schema: unknown (empty result)")

    def toPandas(self):
        import pandas as pd

        return pd.DataFrame(self.collect())

    def _collect_batches(self) -> List[Any]:
        if pa is None:
            raise ImportError("pyarrow is required for DataFrame.collect()")
        raw = self._session._execute_sql(self._query)
        out = []
        import pyarrow.ipc as ipc

        for data in raw:
            reader = ipc.open_stream(data)
            for batch in reader:
                out.append(batch)
        return out


class _GroupedData:
    def __init__(self, df: DataFrame, keys: Sequence[str]) -> None:
        self._df = df
        self._keys = keys

    def agg(self, *exprs: str) -> DataFrame:
        group = ", ".join(self._keys)
        aggs = ", ".join(exprs)
        q = self._df._query
        return self._df._wrap(
            f"SELECT {group}, {aggs} FROM ({q}) GROUP BY {group}"
        )
