import pytest

import krishiv as ks
from krishiv.sql import functions as F


def test_udf_returns_decorated_function():
    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def double_it(batch):
        return [v * 2 for v in batch["x"]]

    assert callable(double_it)
    assert hasattr(double_it, "__krishiv_udf__")
    meta = double_it.__krishiv_udf__
    assert meta["name"] == "double_it"
    assert meta["output_type"] == "int64"


def test_udf_custom_name():
    @ks.udf(input_types={"x": "int64"}, output_type="int64", name="my_double")
    def double_it(batch):
        return [v * 2 for v in batch["x"]]

    assert double_it.__krishiv_udf__["name"] == "my_double"


def test_udf_custom_output_name():
    @ks.udf(input_types={"x": "int64"}, output_type="int64", output_name="result")
    def double_it(batch):
        return [v * 2 for v in batch["x"]]

    assert double_it.__krishiv_udf__["output_name"] == "result"


def test_rust_scalar_udf_multiply():
    udf = ks.RustScalarUdf.multiply("my_multiply", "x", 3)
    assert isinstance(udf, ks.RustScalarUdf)
    assert udf.name() == "my_multiply"


def test_rust_scalar_udf_register_and_execute():
    session = ks.Session.local()
    udf = ks.RustScalarUdf.multiply("triple", "val", 3)
    session.register_function("triple", udf)
    result = session.sql("SELECT triple(10) AS result").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "30" in pretty


def test_register_udf_with_decorator():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def add_one(batch):
        return [v + 1 for v in batch["x"]]

    session.register_udf(add_one)
    udfs = session.list_udfs()
    assert "add_one" in udfs


def test_register_udf_with_explicit_args():
    session = ks.Session.local()

    def add_ten(batch):
        return [v + 10 for v in batch["x"]]

    session.register_udf(
        "add_ten", add_ten, input_types={"x": "int64"}, output_type="int64"
    )
    udfs = session.list_udfs()
    assert "add_ten" in udfs


def test_register_udf_execute_via_call_function():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def square(batch):
        return [v * v for v in batch["x"]]

    session.register_udf(square)
    result = session.sql("SELECT square(CAST(5 AS BIGINT)) AS answer").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "25" in pretty


def test_register_udf_execute_via_F_call_function():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def negate(batch):
        return [-v for v in batch["x"]]

    session.register_udf(negate)
    col_expr = F.call_function("negate", F.lit(7))
    result = session.sql(f"SELECT {col_expr.sql()} AS val").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "-7" in pretty


def test_register_function_with_rust_scalar_udf():
    session = ks.Session.local()
    udf = ks.RustScalarUdf.multiply("by_five", "n", 5)
    session.register_function("by_five", udf)
    result = session.sql("SELECT by_five(4) AS val").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "20" in pretty


def test_list_udfs_returns_names():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def first_fn(batch):
        return batch["x"]

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def second_fn(batch):
        return batch["x"]

    session.register_udf(first_fn)
    session.register_udf(second_fn)
    udfs = session.list_udfs()
    assert "first_fn" in udfs
    assert "second_fn" in udfs


def test_udf_float_return_type():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="float64")
    def to_float(batch):
        return [float(v) for v in batch["x"]]

    session.register_udf(to_float)
    result = session.sql("SELECT to_float(CAST(42 AS BIGINT)) AS val").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "42.0" in pretty


def test_udf_string_return_type():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="string")
    def to_str(batch):
        return [f"val_{v}" for v in batch["x"]]

    session.register_udf(to_str)
    result = session.sql("SELECT to_str(CAST(7 AS BIGINT)) AS val").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "val_7" in pretty


def test_udf_multi_column_input():
    session = ks.Session.local()

    @ks.udf(input_types={"a": "int64", "b": "int64"}, output_type="int64")
    def add_cols(batch):
        return [x + y for x, y in zip(batch["a"], batch["b"])]

    session.register_udf(add_cols)
    result = session.sql(
        "SELECT add_cols(CAST(10 AS BIGINT), CAST(20 AS BIGINT)) AS val"
    ).collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "30" in pretty


def test_udf_batch_execution_with_table():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def triple(batch):
        return [v * 3 for v in batch["x"]]

    session.register_udf(triple)
    result = session.sql(
        "SELECT triple(x) AS val FROM "
        "(SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t ORDER BY x"
    ).collect()
    assert result.row_count == 3
    pretty = result.pretty()
    assert "3" in pretty
    assert "6" in pretty
    assert "9" in pretty


def test_udf_null_handling():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def maybe_double(batch):
        return [v * 2 if v is not None else None for v in batch["x"]]

    session.register_udf(maybe_double)
    result = session.sql(
        "SELECT maybe_double(x) AS val FROM "
        "(SELECT 1 AS x UNION ALL SELECT NULL UNION ALL SELECT 3) t"
    ).collect()
    assert result.row_count == 3
    pretty = result.pretty()
    assert "2" in pretty
    assert "6" in pretty


def test_udf_boolean_return_type():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="bool")
    def is_positive(batch):
        return [v > 0 for v in batch["x"]]

    session.register_udf(is_positive)
    result = session.sql("SELECT is_positive(CAST(5 AS BIGINT)) AS val").collect()
    assert result.row_count == 1
    pretty = result.pretty()
    assert "true" in pretty.lower()


def test_register_udf_missing_input_types_raises():
    session = ks.Session.local()

    def noop(batch):
        return batch["x"]

    with pytest.raises(Exception):
        session.register_udf("noop", noop)


def test_register_udf_missing_output_type_raises():
    session = ks.Session.local()

    def noop(batch):
        return batch["x"]

    with pytest.raises(Exception):
        session.register_udf("noop", noop, input_types={"x": "int64"})


def test_register_function_duplicate_name_replaces():
    session = ks.Session.local()
    udf1 = ks.RustScalarUdf.multiply("dup_fn", "x", 2)
    udf2 = ks.RustScalarUdf.multiply("dup_fn", "x", 10)
    session.register_function("dup_fn", udf1)
    session.register_function("dup_fn", udf2)
    result = session.sql("SELECT dup_fn(3) AS val").collect()
    pretty = result.pretty()
    assert "30" in pretty


def test_udf_zero_rows():
    session = ks.Session.local()

    @ks.udf(input_types={"x": "int64"}, output_type="int64")
    def double_it(batch):
        return [v * 2 for v in batch["x"]]

    session.register_udf(double_it)
    result = session.sql(
        "SELECT double_it(x) AS val FROM (SELECT 1 AS x) t WHERE FALSE"
    ).collect()
    assert result.row_count == 0
