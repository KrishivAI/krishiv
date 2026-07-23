"""Exercise every krishiv.types descriptor (simpleString / jsonValue /
cast_string / to_arrow / eq / hash / repr) — maximises krishiv.types coverage."""
import pyarrow as pa
import pytest

from krishiv import types as T

ATOMIC = [
    (T.NullType(), "null", "null"),
    (T.StringType(), "string", "string"),
    (T.BinaryType(), "binary", "binary"),
    (T.BooleanType(), "boolean", "boolean"),
    (T.ByteType(), "tinyint", "tinyint"),
    (T.ShortType(), "smallint", "smallint"),
    (T.IntegerType(), "int", "int"),
    (T.LongType(), "bigint", "bigint"),
    (T.FloatType(), "float", "float"),
    (T.DoubleType(), "double", "double"),
    (T.DateType(), "date", "date"),
    (T.TimestampType(), "timestamp", "timestamp"),
    (T.TimestampNTZType(), "timestamp_ntz", "timestamp"),
]


@pytest.mark.parametrize("dtype,simple,cast", ATOMIC)
def test_atomic_type_surface(dtype, simple, cast):
    assert dtype.simpleString() == simple
    assert dtype.cast_string() == cast
    assert isinstance(dtype.typeName(), str)
    assert dtype.jsonValue() == dtype.typeName()
    assert isinstance(repr(dtype), str)
    assert dtype == type(dtype)()          # __eq__
    assert hash(dtype) == hash(type(dtype)())  # __hash__
    assert dtype.to_arrow() is not None    # pyarrow mapping


def test_decimal_type():
    d = T.DecimalType(12, 3)
    assert d.simpleString() == "decimal(12,3)"
    assert d.cast_string() == "decimal(12,3)"
    assert d.jsonValue() == "decimal(12,3)"
    assert "DecimalType(12,3)" in repr(d)
    assert d.to_arrow() == pa.decimal128(12, 3)


def test_array_type():
    a = T.ArrayType(T.IntegerType(), containsNull=False)
    assert a.simpleString() == "array<int>"
    assert a.jsonValue() == {"type": "array", "elementType": "integer", "containsNull": False}
    assert a.cast_string() == "array<int>"
    assert pa.types.is_large_list(a.to_arrow())
    assert "ArrayType" in repr(a)


def test_map_type():
    m = T.MapType(T.StringType(), T.LongType(), valueContainsNull=False)
    assert m.simpleString() == "map<string,bigint>"
    assert m.jsonValue()["type"] == "map"
    assert pa.types.is_map(m.to_arrow())
    assert "MapType" in repr(m)


def test_struct_field_and_type():
    f1 = T.StructField("a", T.IntegerType(), nullable=False, metadata={"k": "v"})
    f2 = T.StructField("b", T.ArrayType(T.StringType()))
    assert f1.simpleString() == "a:int"
    assert f1.jsonValue() == {"name": "a", "type": "integer", "nullable": False, "metadata": {"k": "v"}}
    assert f1.to_arrow().name == "a" and not f1.to_arrow().nullable
    assert "StructField('a'" in repr(f1)

    st = T.StructType([f1, f2])
    assert st.fieldNames() == ["a", "b"]
    assert st.simpleString() == "struct<a:int,b:array<string>>"
    assert st.jsonValue()["type"] == "struct"
    assert len(st) == 2
    assert st[0] is f1 and st["b"] is f2
    assert [f.name for f in st] == ["a", "b"]
    assert isinstance(st.to_arrow(), pa.Schema)
    assert "StructType" in repr(st)


def test_struct_add_chaining():
    st = T.StructType().add("a", T.LongType()).add(T.StructField("b", T.StringType()))
    assert st.fieldNames() == ["a", "b"]
    with pytest.raises(ValueError):
        T.StructType().add("x")  # missing data_type


def test_struct_getitem_keyerror():
    st = T.StructType([T.StructField("a", T.IntegerType())])
    with pytest.raises(KeyError):
        _ = st["missing"]


def test_base_datatype_and_parse_atomic():
    assert T.DataType().typeName() == "data"  # base name derivation
    assert T.DataType().jsonValue() == "data"  # base jsonValue delegates to typeName
    assert T.DataType().cast_string() == "data"  # base cast_string delegates to simpleString
    with pytest.raises(NotImplementedError):
        T.DataType().to_arrow()
    assert isinstance(T._parse_atomic("int"), T.IntegerType)
    assert isinstance(T._parse_atomic("decimal(10,2)"), T.DecimalType)
    with pytest.raises(ValueError):
        T._parse_atomic("nonsense")
