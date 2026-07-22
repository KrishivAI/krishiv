"""PySpark-compatible data-type descriptors for Krishiv.

This module mirrors the public surface of :mod:`pyspark.sql.types` closely
enough that migrating code can ``from krishiv.types import StructType,
StructField, IntegerType, StringType`` unchanged. The types are lightweight
value objects: they describe a schema, render to Krishiv's ``cast()`` type
grammar, and (when :mod:`pyarrow` is available) map to Arrow types so
:meth:`krishiv.Session.createDataFrame` can build a typed table from Python
data.

Only stable, semantically-unambiguous types are exposed. Each concrete
``DataType`` subclass provides:

* ``typeName()`` / ``simpleString()`` — the PySpark identity of the type,
* ``jsonValue()`` — the PySpark JSON representation,
* ``cast_string()`` — the string Krishiv's :meth:`Column.cast` understands,
* ``to_arrow()`` — the corresponding :mod:`pyarrow` type (raises a clear
  error if pyarrow is not installed).
"""

from __future__ import annotations

from typing import Any, Optional


class DataType:
    """Base class for all Krishiv/PySpark data types."""

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash((type(self).__name__, tuple(sorted(self.__dict__.items()))))

    @classmethod
    def typeName(cls) -> str:
        name = cls.__name__
        if name.endswith("Type"):
            name = name[: -len("Type")]
        return name[0].lower() + name[1:]

    def simpleString(self) -> str:
        return self.typeName()

    def jsonValue(self) -> Any:
        return self.typeName()

    def cast_string(self) -> str:
        return self.simpleString()

    def to_arrow(self):  # noqa: ANN201
        raise NotImplementedError(
            f"{type(self).__name__} has no pyarrow mapping"
        )


def _require_pyarrow():  # noqa: ANN202
    try:
        import pyarrow as pa  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - exercised only without pyarrow
        raise ImportError(
            "pyarrow is required for this operation. "
            "Install with: pip install krishiv[arrow]"
        ) from exc
    return pa


class NullType(DataType):
    def cast_string(self) -> str:
        return "null"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().null()


class StringType(DataType):
    def cast_string(self) -> str:
        return "string"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().large_utf8()


class BinaryType(DataType):
    def cast_string(self) -> str:
        return "binary"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().large_binary()


class BooleanType(DataType):
    def cast_string(self) -> str:
        return "boolean"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().bool_()


class ByteType(DataType):
    def simpleString(self) -> str:
        return "tinyint"

    def cast_string(self) -> str:
        return "tinyint"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().int8()


class ShortType(DataType):
    def simpleString(self) -> str:
        return "smallint"

    def cast_string(self) -> str:
        return "smallint"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().int16()


class IntegerType(DataType):
    def simpleString(self) -> str:
        return "int"

    def cast_string(self) -> str:
        return "int"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().int32()


class LongType(DataType):
    def simpleString(self) -> str:
        return "bigint"

    def cast_string(self) -> str:
        return "bigint"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().int64()


class FloatType(DataType):
    def simpleString(self) -> str:
        return "float"

    def cast_string(self) -> str:
        return "float"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().float32()


class DoubleType(DataType):
    def simpleString(self) -> str:
        return "double"

    def cast_string(self) -> str:
        return "double"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().float64()


class DateType(DataType):
    def cast_string(self) -> str:
        return "date"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().date32()


class TimestampType(DataType):
    def cast_string(self) -> str:
        return "timestamp"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().timestamp("us", tz="UTC")


class TimestampNTZType(DataType):
    def simpleString(self) -> str:
        return "timestamp_ntz"

    def cast_string(self) -> str:
        return "timestamp"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().timestamp("us")


class DecimalType(DataType):
    def __init__(self, precision: int = 10, scale: int = 0) -> None:
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return f"DecimalType({self.precision},{self.scale})"

    def simpleString(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def jsonValue(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def cast_string(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().decimal128(self.precision, self.scale)


class ArrayType(DataType):
    def __init__(self, elementType: DataType, containsNull: bool = True) -> None:
        self.elementType = elementType
        self.containsNull = containsNull

    def __repr__(self) -> str:
        return f"ArrayType({self.elementType!r}, {self.containsNull})"

    def simpleString(self) -> str:
        return f"array<{self.elementType.simpleString()}>"

    def jsonValue(self) -> dict:
        return {
            "type": "array",
            "elementType": self.elementType.jsonValue(),
            "containsNull": self.containsNull,
        }

    def cast_string(self) -> str:
        return self.simpleString()

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().large_list(self.elementType.to_arrow())


class MapType(DataType):
    def __init__(
        self,
        keyType: DataType,
        valueType: DataType,
        valueContainsNull: bool = True,
    ) -> None:
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def __repr__(self) -> str:
        return f"MapType({self.keyType!r}, {self.valueType!r}, {self.valueContainsNull})"

    def simpleString(self) -> str:
        return f"map<{self.keyType.simpleString()},{self.valueType.simpleString()}>"

    def jsonValue(self) -> dict:
        return {
            "type": "map",
            "keyType": self.keyType.jsonValue(),
            "valueType": self.valueType.jsonValue(),
            "valueContainsNull": self.valueContainsNull,
        }

    def to_arrow(self):  # noqa: ANN201
        pa = _require_pyarrow()
        return pa.map_(self.keyType.to_arrow(), self.valueType.to_arrow())


class StructField(DataType):
    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: Optional[dict] = None,
    ) -> None:
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return f"StructField('{self.name}', {self.dataType!r}, {self.nullable})"

    def simpleString(self) -> str:
        return f"{self.name}:{self.dataType.simpleString()}"

    def jsonValue(self) -> dict:
        return {
            "name": self.name,
            "type": self.dataType.jsonValue(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        }

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().field(
            self.name, self.dataType.to_arrow(), nullable=self.nullable
        )


class StructType(DataType):
    def __init__(self, fields: Optional[list] = None) -> None:
        self.fields = list(fields) if fields else []
        self.names = [f.name for f in self.fields]

    def __repr__(self) -> str:
        return f"StructType([{', '.join(repr(f) for f in self.fields)}])"

    def __iter__(self):
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.fields[key]
        for field in self.fields:
            if field.name == key:
                return field
        raise KeyError(key)

    def add(
        self,
        field,
        data_type: Optional[DataType] = None,
        nullable: bool = True,
        metadata: Optional[dict] = None,
    ) -> "StructType":
        """Append a field (PySpark ``StructType.add``). Chainable."""
        if isinstance(field, StructField):
            self.fields.append(field)
        else:
            if data_type is None:
                raise ValueError("add(name, data_type) requires a data_type")
            self.fields.append(StructField(field, data_type, nullable, metadata))
        self.names = [f.name for f in self.fields]
        return self

    def fieldNames(self) -> list:
        return list(self.names)

    def simpleString(self) -> str:
        return f"struct<{','.join(f.simpleString() for f in self.fields)}>"

    def jsonValue(self) -> dict:
        return {"type": "struct", "fields": [f.jsonValue() for f in self.fields]}

    def to_arrow(self):  # noqa: ANN201
        return _require_pyarrow().schema([f.to_arrow() for f in self.fields])


# Simple mapping from a PySpark simpleString / DDL fragment to a DataType.
_ATOMIC_BY_NAME = {
    "null": NullType,
    "void": NullType,
    "string": StringType,
    "str": StringType,
    "binary": BinaryType,
    "boolean": BooleanType,
    "bool": BooleanType,
    "byte": ByteType,
    "tinyint": ByteType,
    "short": ShortType,
    "smallint": ShortType,
    "int": IntegerType,
    "integer": IntegerType,
    "long": LongType,
    "bigint": LongType,
    "float": FloatType,
    "real": FloatType,
    "double": DoubleType,
    "date": DateType,
    "timestamp": TimestampType,
    "timestamp_ntz": TimestampNTZType,
}


def _parse_atomic(name: str) -> DataType:
    key = name.strip().lower()
    if key in _ATOMIC_BY_NAME:
        return _ATOMIC_BY_NAME[key]()
    if key.startswith("decimal"):
        inside = key[key.find("(") + 1 : key.rfind(")")] if "(" in key else "10,0"
        precision, _, scale = inside.partition(",")
        return DecimalType(int(precision or 10), int(scale or 0))
    raise ValueError(f"unsupported type string: {name!r}")


__all__ = [
    "DataType",
    "NullType",
    "StringType",
    "BinaryType",
    "BooleanType",
    "ByteType",
    "ShortType",
    "IntegerType",
    "LongType",
    "FloatType",
    "DoubleType",
    "DateType",
    "TimestampType",
    "TimestampNTZType",
    "DecimalType",
    "ArrayType",
    "MapType",
    "StructField",
    "StructType",
]
