"""Krishiv — hybrid batch and streaming compute engine."""

from .krishiv import (
    AuthorizationError,
    Batch,
    CheckpointError,
    ConnectorError,
    DataFrame,
    IcebergSink,
    KafkaSink,
    KeyedStream,
    KrishivError,
    ModeError,
    ParquetSink,
    QueryError,
    Schema,
    SchemaError,
    Session,
    Stream,
    WindowedStream,
    read_kafka,
    read_parquet,
)

import asyncio as _asyncio


class _Windows:
    @staticmethod
    def tumbling(size_ms: int) -> tuple[str, int]:
        return ("tumbling", size_ms)

    @staticmethod
    def sliding(size_ms: int, slide_ms: int) -> tuple[str, int, int]:
        return ("sliding", size_ms, slide_ms)

    @staticmethod
    def session(gap_ms: int) -> tuple[str, int]:
        return ("session", gap_ms)


windows = _Windows()


class _Agg:
    @staticmethod
    def sum(column: str) -> tuple[str, str]:
        return ("sum", column)

    @staticmethod
    def count() -> tuple[str, None]:
        return ("count", None)

    @staticmethod
    def max(column: str) -> tuple[str, str]:
        return ("max", column)

    @staticmethod
    def min(column: str) -> tuple[str, str]:
        return ("min", column)

    @staticmethod
    def mean(column: str) -> tuple[str, str]:
        return ("mean", column)


agg = _Agg()


class _Sinks:
    @staticmethod
    def parquet(path: str, partition_by: str | None = None) -> ParquetSink:
        return ParquetSink(path)

    @staticmethod
    def kafka(bootstrap_servers: str, topic: str) -> KafkaSink:
        return KafkaSink(topic, bootstrap_servers)

    @staticmethod
    def iceberg(catalog_uri: str, table_name: str) -> IcebergSink:
        return IcebergSink(catalog_uri, table_name)


sinks = _Sinks()


async def connect_async(url: str) -> Session:
    """Connect to a remote coordinator without blocking the asyncio loop."""
    loop = _asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: Session.connect(url))


__all__ = [
    "AuthorizationError",
    "Batch",
    "CheckpointError",
    "ConnectorError",
    "DataFrame",
    "IcebergSink",
    "KafkaSink",
    "KeyedStream",
    "KrishivError",
    "ModeError",
    "ParquetSink",
    "QueryError",
    "Schema",
    "SchemaError",
    "Session",
    "Stream",
    "WindowedStream",
    "agg",
    "connect_async",
    "read_kafka",
    "read_parquet",
    "sinks",
    "windows",
]
