"""Krishiv dbt adapter (Flight SQL transport — ADR-R15.3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence


@dataclass
class KrishivCredentials:
    flight_sql_host: str = "localhost"
    flight_sql_port: int = 31337
    database: str = "default"
    schema: str = "default"

    @property
    def grpc_target(self) -> str:
        return f"grpc://{self.flight_sql_host}:{self.flight_sql_port}"


class KrishivConnection:
    """Flight SQL connection for dbt (uses flightsql-dbapi when installed)."""

    def __init__(self, credentials: KrishivCredentials) -> None:
        self.credentials = credentials
        self._queries: list[str] = []
        self._cursor = None
        try:
            from flightsql import dbapi

            self._conn = dbapi.connect(
                self.credentials.grpc_target,
                db_kwargs={
                    "database": self.credentials.database,
                    "schema": self.credentials.schema,
                },
            )
            self._cursor = self._conn.cursor()
        except ImportError:
            self._conn = None

    def execute(self, sql: str) -> None:
        self._queries.append(sql)
        if self._cursor is not None:
            self._cursor.execute(sql)

    def get_result_from_cursor(self) -> List[tuple]:
        if self._cursor is None:
            return []
        return list(self._cursor.fetchall())

    @property
    def last_query(self) -> Optional[str]:
        return self._queries[-1] if self._queries else None

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()


class KrishivAdapter:
    """dbt adapter entry point."""

    type = "krishiv"

    def __init__(self, config: Any) -> None:
        self.config = config
        creds = config.credentials if hasattr(config, "credentials") else config
        self.credentials = KrishivCredentials(
            flight_sql_host=getattr(creds, "flight_sql_host", "localhost"),
            flight_sql_port=int(getattr(creds, "flight_sql_port", 31337)),
            database=getattr(creds, "database", "default"),
            schema=getattr(creds, "schema", "default"),
        )

    def open(self) -> KrishivConnection:
        return KrishivConnection(self.credentials)

    def compile_model(self, model: dict[str, Any]) -> str:
        sql = model.get("compiled_code") or model.get("raw_code") or ""
        mat = model.get("config", {}).get("materialized", "view")
        name = model["name"]
        if mat == "table":
            return f"CREATE TABLE {name} AS {sql}"
        if mat == "incremental":
            return f"INSERT INTO {name} {sql}"
        return f"CREATE OR REPLACE VIEW {name} AS {sql}"

    def list_relations_without_caching(self) -> Sequence[str]:
        return []

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[str]:
        return f"{database}.{schema}.{identifier}"

    def create_schema(self, schema: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {schema}"

    def drop_relation(self, relation: str) -> str:
        return f"DROP TABLE IF EXISTS {relation}"

    def truncate_relation(self, relation: str) -> str:
        return f"TRUNCATE TABLE {relation}"

    def rename_relation(self, old: str, new: str) -> str:
        return f"ALTER TABLE {old} RENAME TO {new}"
