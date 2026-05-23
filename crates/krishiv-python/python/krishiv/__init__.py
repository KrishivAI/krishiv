"""Krishiv — hybrid batch and streaming compute engine.

Pure-Python facade that re-exports the native extension and adds
``connect_async`` for asyncio-first workflows.

Usage::

    import krishiv

    # Embedded SQL
    session = krishiv.Session.embedded()
    df = session.sql("SELECT 1 AS n")
    print(df.collect())

    # Connect to a distributed coordinator
    session = krishiv.Session.connect("http://coordinator:50051")

    # Stream with tumbling windows
    session = krishiv.Session.local()
    stream = session.stream("events", watermark_column="ts", max_lateness_ms=5000)
    windowed = stream.tumbling_window(60)
    async for batch in windowed:
        process(batch)
"""

from .krishiv import (  # noqa: F401 — re-export native extension symbols
    # Exception hierarchy
    KrishivError,
    QueryError,
    SchemaError,
    ConnectorError,
    CheckpointError,
    AuthorizationError,
    ModeError,
    # Core types
    Session,
    DataFrame,
    Stream,
    WindowedStream,
    Batch,
    # Sinks
    ParquetSink,
    KafkaSink,
    IcebergSink,
    # Module-level functions
    read_parquet,
    read_kafka,
)

import asyncio as _asyncio


async def connect_async(url: str) -> "Session":
    """Open a distributed session to ``url`` without blocking the event loop.

    This coroutine runs the session builder on a thread-pool executor so the
    asyncio event loop is never blocked during connection setup.

    Args:
        url: Coordinator Arrow Flight endpoint, e.g. ``http://coordinator:50051``.

    Returns:
        A :class:`Session` in distributed mode.
    """
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: Session.connect(url))


__all__ = [
    # Exceptions
    "KrishivError",
    "QueryError",
    "SchemaError",
    "ConnectorError",
    "CheckpointError",
    "AuthorizationError",
    "ModeError",
    # Core types
    "Session",
    "DataFrame",
    "Stream",
    "WindowedStream",
    "Batch",
    # Sinks
    "ParquetSink",
    "KafkaSink",
    "IcebergSink",
    # Functions
    "read_parquet",
    "read_kafka",
    "connect_async",
]
