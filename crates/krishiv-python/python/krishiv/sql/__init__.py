"""SQL-facing Python API for Krishiv.

This namespace mirrors the familiar shape of ``pyspark.sql`` while delegating
to Krishiv's native Arrow/DataFusion-backed classes.
"""

from ..krishiv import (  # noqa: F401
    Column,
    DataFrame,
    DataStreamReader,
    DataStreamWriter,
    GroupedDataFrame,
    QueryResult,
    Session,
    StreamingDataFrame,
    StreamingQuery,
    StreamingQueryProgress,
)
from . import functions  # noqa: F401

__all__ = [
    "Column",
    "DataFrame",
    "DataStreamReader",
    "DataStreamWriter",
    "GroupedDataFrame",
    "QueryResult",
    "Session",
    "StreamingDataFrame",
    "StreamingQuery",
    "StreamingQueryProgress",
    "functions",
]
