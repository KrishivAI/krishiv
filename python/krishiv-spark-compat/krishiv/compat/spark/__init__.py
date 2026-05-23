"""PySpark-compatible API surface for Krishiv (R15)."""

from krishiv.compat.spark.column import col, lit, when
from krishiv.compat.spark.dataframe import DataFrame
from krishiv.compat.spark.functions import avg, count, explode, max, min, sum
from krishiv.compat.spark.session import SparkSession

__all__ = [
    "SparkSession",
    "DataFrame",
    "col",
    "lit",
    "when",
    "avg",
    "sum",
    "count",
    "min",
    "max",
    "explode",
]
