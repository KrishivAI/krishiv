"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (  # noqa: F401
    AuthorizationError,
    BlockingSession,
    RustScalarUdf,
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
    Stream,
    KeyedStream,
    WindowedStream,
    WindowSpec,
    StreamingDataFrame,
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
    udf,
)

from .krishiv import sinks

from .krishiv import agg
from .krishiv import windows
from . import functions

import asyncio as _asyncio
import inspect as _inspect


async def connect_async(url: str) -> Session:
    """Create a remote session from async code."""
    return Session.connect(url)


_native_session_sql = Session.sql


async def _session_sql_async(self, query: str):
    """Plan SQL from async code and return a lazy DataFrame."""
    return _native_session_sql(self, query)


_native_dataframe_collect = DataFrame.collect


async def _dataframe_collect_async(self):
    """Collect a DataFrame from async code."""
    return _native_dataframe_collect(self)


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
    "Schema",
    "Stream",
    "KeyedStream",
    "WindowedStream",
    "WindowSpec",
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
    "connect_async",
    "agg",
    "functions",
    "windows",
    "udf",
    "QueryResult",
    "JobStatus",
    "sinks",
]

# Wrap __anext__ of Rust-defined async iterators to return coroutines
# as required by newer Python versions (Python 3.13+)
try:
    from .krishiv import WindowedStream
    _orig_windowed_anext = WindowedStream.__anext__
    async def _new_windowed_anext(self):
        return _orig_windowed_anext(self)
    WindowedStream.__anext__ = _new_windowed_anext
except (ImportError, AttributeError):
    pass

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
