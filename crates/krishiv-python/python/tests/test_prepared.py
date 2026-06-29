import pytest

import krishiv as ks
from krishiv.sql import functions as F


def _prepare(sql):
    session = ks.Session.local()
    try:
        return session.prepare(sql)
    except (RuntimeError, ks.ModeError) as e:
        pytest.skip(str(e))


def test_prepare_simple_query():
    stmt = _prepare("SELECT $1 AS x")
    assert stmt is not None


def test_parameter_count_single():
    stmt = _prepare("SELECT $1 AS x")
    assert stmt.parameter_count() == 1


def test_sql_returns_query_string():
    stmt = _prepare("SELECT $1 AS x")
    sql = stmt.sql()
    assert isinstance(sql, str)
    assert "$1" in sql


def test_bind_integer():
    stmt = _prepare("SELECT $1 AS x")
    df = stmt.bind([F.lit(42)])
    result = df.collect()
    assert result.row_count == 1
    assert "42" in result.pretty()


def test_bind_string():
    stmt = _prepare("SELECT $1 AS x")
    df = stmt.bind([F.lit("hello")])
    result = df.collect()
    assert result.row_count == 1
    assert "hello" in result.pretty()


def test_bind_float():
    stmt = _prepare("SELECT $1 AS x")
    df = stmt.bind([F.lit(3.14)])
    result = df.collect()
    assert result.row_count == 1
    assert "3.14" in result.pretty()


def test_prepare_multiple_params():
    stmt = _prepare("SELECT $1 AS a, $2 AS b, $3 AS c")
    assert stmt.parameter_count() == 3


def test_bind_multiple_params():
    stmt = _prepare("SELECT $1 AS a, $2 AS b, $3 AS c")
    df = stmt.bind([F.lit(1), F.lit("two"), F.lit(3.0)])
    result = df.collect()
    assert result.row_count == 1
    text = result.pretty()
    assert "1" in text
    assert "two" in text
    assert "3" in text


def test_prepare_no_parameters():
    stmt = _prepare("SELECT 42 AS x")
    assert stmt.parameter_count() == 0


def test_execute_no_param_prepared():
    stmt = _prepare("SELECT 42 AS x")
    df = stmt.bind([])
    result = df.collect()
    assert result.row_count == 1
    assert "42" in result.pretty()


def test_bind_rejects_non_literal():
    stmt = _prepare("SELECT $1 AS x")
    with pytest.raises(TypeError, match="lit"):
        stmt.bind([F.col("x")])


def test_prepare_select_with_alias():
    stmt = _prepare("SELECT $1 AS value")
    assert "value" in stmt.sql()


def test_bind_bool():
    stmt = _prepare("SELECT $1 AS flag")
    df = stmt.bind([F.lit(True)])
    result = df.collect()
    assert result.row_count == 1
    assert "true" in result.pretty().lower()


def test_bind_null():
    stmt = _prepare("SELECT $1 AS missing")
    df = stmt.bind([F.lit(None)])
    result = df.collect()
    assert result.row_count == 1
    arrow = result.to_arrow()
    assert arrow.column("missing").null_count == 1


def test_prepare_expression_query():
    stmt = _prepare("SELECT $1 + $2 AS total")
    assert stmt.parameter_count() == 2
    df = stmt.bind([F.lit(10), F.lit(20)])
    result = df.collect()
    assert result.row_count == 1
    assert "30" in result.pretty()


def test_rebind_same_statement():
    stmt = _prepare("SELECT $1 AS x")
    df1 = stmt.bind([F.lit(1)])
    df2 = stmt.bind([F.lit(2)])
    assert "1" in df1.collect().pretty()
    assert "2" in df2.collect().pretty()


def test_bind_wrong_param_count():
    stmt = _prepare("SELECT $1 AS a, $2 AS b")
    with pytest.raises(ks.KrishivError, match="expects 2 parameters, got 1"):
        stmt.bind([F.lit(1)])
