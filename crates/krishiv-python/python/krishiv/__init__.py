"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (  # noqa: F401
    AuthorizationError,
    Batch,
    QueryResult,
    JobStatus,
    CheckpointError,
    ConnectorError,
    DataFrame,
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

