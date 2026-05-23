from krishiv.compat.spark.dataframe import Column


def avg(column: str) -> str:
    return f"avg({column})"


def sum(column: str) -> str:
    return f"sum({column})"


def count(column: str = "*") -> str:
    return f"count({column})"


def min(column: str) -> str:
    return f"min({column})"


def max(column: str) -> str:
    return f"max({column})"


def explode(column: str) -> Column:
    return Column(f"explode({column})")
