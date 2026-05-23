"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (  # noqa: F401
    AuthorizationError,
    Batch,
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
    register_state_migration,
    state_migration,
    udf,
)

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
    "register_state_migration",
    "state_migration",
    "connect_async",
    "agg",
    "windows",
    "udf",
]
