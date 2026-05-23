"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (
    AuthorizationError,
    Batch,
    ChangeFeedIter,
    CheckpointError,
    ConnectorError,
    DataFrame,
    IcebergSink,
    KafkaSink,
    KrishivError,
    LiveTable,
    ModeError,
    MemoCacheInfo,
    ParquetSink,
    QueryError,
    SchemaError,
    Session,
    Stream,
    WindowedStream,
    memo_cache_info,
    read_kafka,
    read_parquet,
)

import asyncio as _asyncio


async def connect_async(url: str) -> Session:
    """Connect to a remote coordinator without blocking the asyncio loop."""
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: Session.connect(url))


__all__ = [
    "AuthorizationError",
    "Batch",
    "ChangeFeedIter",
    "CheckpointError",
    "ConnectorError",
    "DataFrame",
    "IcebergSink",
    "KafkaSink",
    "KrishivError",
    "LiveTable",
    "ModeError",
    "MemoCacheInfo",
    "ParquetSink",
    "QueryError",
    "SchemaError",
    "Session",
    "Stream",
    "WindowedStream",
    "connect_async",
    "memo_cache_info",
    "read_kafka",
    "read_parquet",
]
