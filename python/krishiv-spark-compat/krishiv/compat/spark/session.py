"""SparkSession shim with Spark Connect remote execution."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urlparse

from krishiv.compat.spark.client import SparkConnectClient
from krishiv.compat.spark.dataframe import DataFrame


@dataclass
class _Builder:
    _remote: Optional[str] = None
    _app_name: Optional[str] = None
    _config: dict[str, str] = field(default_factory=dict)

    def remote(self, url: str) -> _Builder:
        self._remote = url
        return self

    def appName(self, name: str) -> _Builder:
        self._app_name = name
        return self

    def config(self, key: str, value: str) -> _Builder:
        self._config[key] = value
        return self

    def getOrCreate(self) -> SparkSession:
        return SparkSession(remote=self._remote, app_name=self._app_name, config=self._config)


class SparkSession:
    """PySpark-compatible session backed by Krishiv Spark Connect or embedded SQL."""

    builder = _Builder()

    def __init__(
        self,
        remote: Optional[str] = None,
        app_name: Optional[str] = None,
        config: Optional[dict[str, str]] = None,
    ) -> None:
        self._remote = remote
        self._app_name = app_name or "krishiv"
        self._config = config or {}
        self._client: Optional[SparkConnectClient] = None
        if remote:
            parsed = urlparse(remote.replace("sc://", "http://", 1))
            host = parsed.hostname or "localhost"
            port = parsed.port or 7070
            self._client = SparkConnectClient(host, port)

    def sql(self, query: str) -> DataFrame:
        return DataFrame(self, query=query)

    def _execute_sql(self, query: str) -> list:
        if self._client is None:
            raise RuntimeError("local SparkSession requires remote('sc://host:7070')")
        return self._client.execute_sql(query, session_id=str(uuid.uuid4()))

    def __repr__(self) -> str:
        return f"SparkSession(remote={self._remote!r})"
