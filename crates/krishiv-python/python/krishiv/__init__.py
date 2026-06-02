"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (  # noqa: F401
    AuthorizationError,
    Batch,
    QueryResult,
    JobStatus,
    CheckpointError,
    ConnectorError,
    DataFrame,
    DataFrameStream,
    IcebergSink,
    KafkaSink,
    KrishivError,
    ModeError,
    UdfError,
    ParquetSink,
    QueryError,
    Schema,
    SchemaError,
    Session,
    Stream,
    KeyedStream,
    WindowedStream,
    WindowSpec,
    AggExpr,
    read_parquet,
    read_kafka,
    read_iceberg,
    read_delta,
    read_hudi,
    write_hudi_append,
    write_hudi_upsert,
    register_state_migration,
    state_migration,
    apply_state_migration,
    memo_cache_info,
    memo_transform_call,
    make_example_batch,
    udf,
)

from .krishiv import sinks

from .krishiv import agg
from .krishiv import windows

import asyncio as _asyncio


async def connect_async(url: str) -> Session:
    """Connect to a remote coordinator without blocking the event loop."""
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: Session.connect(url))


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
    "DataFrame",
    "DataFrameStream",
    "Schema",
    "Stream",
    "KeyedStream",
    "WindowedStream",
    "WindowSpec",
    "AggExpr",
    "Batch",
    "ParquetSink",
    "KafkaSink",
    "IcebergSink",
    "read_parquet",
    "read_kafka",
    "read_iceberg",
    "read_delta",
    "read_hudi",
    "write_hudi_append",
    "write_hudi_upsert",
    "register_state_migration",
    "state_migration",
    "apply_state_migration",
    "memo_cache_info",
    "memo_transform_call",
    "make_example_batch",
    "connect_async",
    "agg",
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




def arrow_udf(fn):
    """Mark a UDF as Arrow-native.

    An Arrow-native UDF receives and must return a ``pyarrow.RecordBatch``
    instead of a column dict.  The first column of the returned batch is used
    as the scalar output array.  This avoids per-column
    ``Vec<Option<T>> → PyList → Arrow`` marshalling and can be significantly
    faster for large batches.

    Usage::

        @krishiv.arrow_udf
        def double_value(batch):
            import pyarrow.compute as pc
            col = batch.column("value")
            return batch.set_column(0, "value", pc.multiply(col, 2))

    The decorated function is registered with the standard ``session.udf()`` API.
    """
    fn._krishiv_arrow_udf = True
    return fn
