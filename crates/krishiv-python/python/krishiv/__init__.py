"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (  # noqa: F401
    AuthorizationError,
    BlockingSession,
    RustScalarUdf,
    DeltaBatch,
    IvmJob,
    StepSummary,
    ViewError,
    QueryResult,
    JobStatus,
    CheckpointError,
    ConnectorError,
    Column,
    DataFrame,
    DataFrameStream,
    DataStreamReader,
    GroupedDataFrame,
    IcebergSink,
    KafkaSink,
    KrishivError,
    LiveTable,
    ModeError,
    UdfError,
    ParquetSink,
    QueryHandle,
    QueryError,
    Schema,
    SchemaError,
    Session,
    StreamingDataFrame,
    StreamingQuery,
    StreamingQueryManager,
    StreamingQueryProgress,
    DataStreamWriter,
    AggExpr,
    Batch,
    read_parquet,
    read_kafka,
    read_iceberg,
    read_kinesis,
    read_pulsar,
    read_delta,
    write_delta,
    read_hudi,
    write_hudi_append,
    write_hudi_upsert,
    interval_join,
    stream_table_join,
    temporal_join,
    stream_stream_join,
    register_state_migration,
    state_migration,
    apply_state_migration,
    memo_cache_info,
    memo_transform_call,
    make_example_batch,
    call_function,
    col,
    count,
    count_all,
    expr,
    lit,
    max,
    min,
    avg,
    sum,
    when,
    udf,
)

from .krishiv import sinks

from .krishiv import agg
from . import functions
from . import types
from ._pyspark import Row, _apply as _apply_pyspark_compat

# Graft the PySpark-compatible convenience surface onto the native classes.
_apply_pyspark_compat()

# PySpark entry-point alias. `SparkSession.builder.getOrCreate()` yields an
# embedded Krishiv session; `SparkSession(...)` is the native `Session`.
SparkSession = Session

import asyncio as _asyncio
import inspect as _inspect


async def connect_async(url: str) -> Session:
    """Create a remote session from async code."""
    return Session.connect(url)


_native_session_sql = Session.sql
_native_session_sql_async = getattr(Session, "sql_async", None)


async def _session_sql_async(self, query: str):
    """Plan SQL from async code and return a lazy DataFrame.

    Prefers the native ``sql_async`` (a genuine coroutine created via
    ``pyo3-async-runtimes::future_into_py`` — it schedules work on the Tokio
    runtime and suspends this coroutine without blocking the event loop).
    Falls back to running the synchronous ``sql`` on a thread-pool executor
    if the native method is unavailable or is not itself awaitable, so this
    never blocks the calling event loop either way.
    """
    if _native_session_sql_async is not None:
        result = _native_session_sql_async(self, query)
        if _inspect.isawaitable(result):
            return await result
        return result
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, _native_session_sql, self, query)


_native_dataframe_collect_async = DataFrame.collect_async


async def _dataframe_collect_async(self):
    """Collect a DataFrame from async code (runs on a thread pool).

    The native ``collect_async`` blocks the calling thread until the query
    completes (it releases the GIL internally but returns only once the
    result is ready), so it must run on a worker thread rather than be
    called directly on the event loop thread.
    """
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, _native_dataframe_collect_async, self)


_native_dataframe_execute_stream_async = DataFrame.execute_stream_async


async def _dataframe_execute_stream_async(self):
    """Execute a DataFrame as a stream from async code (runs on a thread pool)."""
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, _native_dataframe_execute_stream_async, self)


_native_streaming_dataframe_execute_stream_async = StreamingDataFrame.execute_stream_async


async def _streaming_dataframe_execute_stream_async(self):
    """Execute a streaming DataFrame from async code (runs on a thread pool)."""
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, _native_streaming_dataframe_execute_stream_async, self)


_native_query_handle_collect = QueryHandle.collect
_native_query_handle_collect_async = getattr(QueryHandle, "collect_async", None)


async def _query_handle_collect_async(self):
    """Await a submitted query handle (runs on a thread pool)."""
    if _native_query_handle_collect_async is not None:
        result = _native_query_handle_collect_async(self)
        if _inspect.isawaitable(result):
            return await result
        return result
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, _native_query_handle_collect, self)


Session.sql_async = _session_sql_async
DataFrame.collect_async = _dataframe_collect_async
DataFrame.execute_stream_async = _dataframe_execute_stream_async
StreamingDataFrame.execute_stream_async = _streaming_dataframe_execute_stream_async
QueryHandle.collect_async = _query_handle_collect_async


def _session_is_embedded(self) -> bool:
    """Return True when this session runs in-process embedded mode."""
    return self.mode == "embedded"


def _session_is_single_node(self) -> bool:
    """Return True when this session routes to a single-node daemon."""
    return self.mode == "local"


def _session_is_distributed(self) -> bool:
    """Return True when this session routes to a distributed coordinator."""
    return self.mode == "distributed"


Session.is_embedded = _session_is_embedded
Session.is_single_node = _session_is_single_node
Session.is_distributed = _session_is_distributed


def _register_arrow_stream(self, job_name: str, async_gen):
    """
    Register a Python async generator of PyArrow RecordBatches to continuously feed a running stream job.
    This bridges Python's async ecosystem directly into Rust's continuous stream pipeline.
    """
    from .krishiv import Batch
    async def _pump():
        try:
            async for pyarrow_batch in async_gen:
                self.push_stream_job_input(job_name, [Batch(pyarrow_batch)])
        except Exception as e:
            print(f"Error pumping stream {job_name}: {e}")

    try:
        loop = _asyncio.get_running_loop()
        loop.create_task(_pump())
    except RuntimeError:
        # If no loop is running, this function assumes the caller will run it later
        pass

Session.register_arrow_stream = _register_arrow_stream



__all__ = [
    "DeltaBatch",
    "IvmJob",
    "StepSummary",
    "ViewError",
    "KrishivError",
    "QueryError",
    "SchemaError",
    "ConnectorError",
    "CheckpointError",
    "AuthorizationError",
    "ModeError",
    "UdfError",
    "Session",
    "BlockingSession",
    "RustScalarUdf",
    "Column",
    "DataFrame",
    "DataFrameStream",
    "DataStreamReader",
    "GroupedDataFrame",
    "StreamingDataFrame",
    "StreamingQuery",
    "StreamingQueryManager",
    "StreamingQueryProgress",
    "DataStreamWriter",
    "Schema",
    "LiveTable",
    "AggExpr",
    "Batch",
    "ParquetSink",
    "KafkaSink",
    "IcebergSink",
    "QueryHandle",
    "read_parquet",
    "read_kafka",
    "read_iceberg",
    "read_kinesis",
    "read_pulsar",
    "read_delta",
    "write_delta",
    "read_hudi",
    "write_hudi_append",
    "write_hudi_upsert",
    "interval_join",
    "stream_table_join",
    "temporal_join",
    "stream_stream_join",
    "register_state_migration",
    "state_migration",
    "apply_state_migration",
    "memo_cache_info",
    "memo_transform_call",
    "make_example_batch",
    "call_function",
    "col",
    "count",
    "count_all",
    "expr",
    "lit",
    "max",
    "min",
    "avg",
    "sum",
    "when",
    "connect_async",
    "agg",
    "functions",
    "types",
    "udf",
    "Row",
    "SparkSession",
    "QueryResult",
    "JobStatus",
    "sinks",
]

try:
    from .krishiv import LiveTable
    _orig_live_anext = LiveTable.__anext__
    async def _new_live_anext(self):
        return _orig_live_anext(self)
    LiveTable.__anext__ = _new_live_anext
except (ImportError, AttributeError):
    pass

try:
    from .krishiv import DataFrameStream
    _orig_dfs_anext = DataFrameStream.__anext__
    async def _new_dfs_anext(self):
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(None, _orig_dfs_anext, self)
    DataFrameStream.__anext__ = _new_dfs_anext
except (ImportError, AttributeError):
    pass
