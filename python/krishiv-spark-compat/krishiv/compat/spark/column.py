from __future__ import annotations

from typing import Any, Union

from krishiv.compat.spark.dataframe import Column


def col(name: str) -> Column:
    return Column(name)


def lit(value: Any) -> Column:
    if isinstance(value, str):
        return Column(f"'{value}'")
    return Column(str(value))


def when(condition: Column, value: Any) -> _When:
    return _When(condition, value)


class _When:
    def __init__(self, condition: Column, value: Any) -> None:
        self._condition = condition
        val_sql = value.sql if isinstance(value, Column) else repr(value)
        self._sql = f"CASE WHEN {condition.sql} THEN {val_sql}"

    def otherwise(self, value: Any) -> Column:
        val_sql = value.sql if isinstance(value, Column) else repr(value)
        return Column(f"{self._sql} ELSE {val_sql} END")
