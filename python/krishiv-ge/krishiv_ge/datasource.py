"""Great Expectations Krishiv datasource (R15 S4.3)."""

from __future__ import annotations

from typing import Any, Iterator, Optional


class KrishivDatasource:
    """Minimal GE-style datasource over Krishiv SQL batches."""

    def __init__(self, name: str = "krishiv") -> None:
        self.name = name

    def get_batch(self, query: str, batches: Optional[list] = None) -> list:
        if batches is not None:
            return batches
        raise ValueError("provide batches= for offline validation")

    def validate_not_null(self, batch: Any, column: str) -> dict[str, Any]:
        import pyarrow as pa

        if not isinstance(batch, pa.RecordBatch):
            raise TypeError("batch must be pyarrow RecordBatch")
        col = batch.column(batch.schema.get_field_index(column))
        nulls = col.null_count
        success = nulls == 0
        return {
            "success": success,
            "result": {"element_count": batch.num_rows, "unexpected_count": nulls},
        }


class KrishivSQLAlchemyDataConnector:
    """Flight SQL connector placeholder implementing batch spec resolution."""

    def __init__(self, flight_sql_host: str, flight_sql_port: int = 31337) -> None:
        self.flight_sql_host = flight_sql_host
        self.flight_sql_port = flight_sql_port

    def get_batch_data(self, table_name: Optional[str] = None, query: Optional[str] = None) -> str:
        if query:
            return query
        if table_name:
            return f'SELECT * FROM "{table_name}"'
        raise ValueError("table_name or query required")
