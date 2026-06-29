import pytest

import krishiv as ks
from krishiv.sql import functions as F


def _sql_upper(column):
    return column.sql().upper()


def test_col_creates_column():
    c = ks.col("amount")
    assert c.sql() == '"amount"'


def test_f_col_creates_column():
    c = F.col("amount")
    assert c.sql() == '"amount"'


def test_f_column_creates_column():
    c = F.column("amount")
    assert c.sql() == '"amount"'


def test_kf_col_creates_column():
    import krishiv.functions as KF

    c = KF.col("amount")
    assert c.sql() == '"amount"'


def test_alias_returns_column_with_name():
    c = ks.col("amount").alias("total")
    assert "total" in c.sql()


def test_alias_sql_contains_alias():
    c = ks.col("x").alias("y")
    sql = c.sql()
    assert '"x"' in sql
    assert '"y"' in sql


def test_asc_sql_contains_asc():
    c = ks.col("amount").asc()
    assert "ASC" in _sql_upper(c)


def test_desc_sql_contains_desc():
    c = ks.col("amount").desc()
    assert "DESC" in _sql_upper(c)


def test_asc_desc_via_functions():
    c_asc = F.asc("amount")
    c_desc = F.desc("amount")
    assert "ASC" in _sql_upper(c_asc)
    assert "DESC" in _sql_upper(c_desc)


def test_cast_sql_renders():
    c = ks.col("amount").cast("int")
    sql = c.sql()
    assert "cast" in sql.lower()
    assert '"amount"' in sql


def test_try_cast_sql_renders():
    c = ks.col("value").try_cast("bigint")
    sql = c.sql()
    assert "try_cast" in sql.lower()
    assert '"value"' in sql


def test_is_null_sql_renders():
    c = ks.col("x").is_null()
    sql = c.sql()
    assert "is null" in sql.lower()
    assert '"x"' in sql


def test_is_not_null_sql_renders():
    c = ks.col("x").is_not_null()
    sql = c.sql()
    assert "is not null" in sql.lower()
    assert '"x"' in sql


def test_addition_operator():
    result = ks.col("a") + ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "+" in sql


def test_subtraction_operator():
    result = ks.col("a") - ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "-" in sql


def test_multiplication_operator():
    result = ks.col("a") * ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "*" in sql


def test_division_operator():
    result = ks.col("a") / ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "/" in sql


def test_arithmetic_with_literal():
    result = ks.col("amount") + ks.lit(5)
    sql = result.sql()
    assert '"amount"' in sql
    assert "5" in sql


def test_chained_arithmetic():
    result = ks.col("x") + ks.col("y") * ks.lit(2)
    sql = result.sql()
    assert '"x"' in sql
    assert '"y"' in sql
    assert "2" in sql


def test_and_operator():
    result = ks.col("a") & ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql


def test_or_operator():
    result = ks.col("a") | ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql


def test_equal_operator():
    result = ks.col("a") == ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "=" in sql


def test_not_equal_operator():
    result = ks.col("a") != ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql


def test_less_than_operator():
    result = ks.col("a") < ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "<" in sql


def test_greater_than_operator():
    result = ks.col("a") > ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert ">" in sql


def test_less_equal_operator():
    result = ks.col("a") <= ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert "<=" in sql


def test_greater_equal_operator():
    result = ks.col("a") >= ks.col("b")
    sql = result.sql()
    assert '"a"' in sql
    assert '"b"' in sql
    assert ">=" in sql


def test_bool_raises_type_error():
    c = ks.col("x")
    with pytest.raises(TypeError, match="lazy expression"):
        bool(c)


def test_bool_on_comparison_raises():
    c = ks.col("x") == ks.lit(1)
    with pytest.raises(TypeError):
        bool(c)


def test_repr_returns_string():
    c = ks.col("amount")
    r = repr(c)
    assert isinstance(r, str)
    assert len(r) > 0


def test_normalized_ast_returns_something():
    c = ks.col("amount")
    ast = c.normalized_ast()
    assert ast is not None


def test_execute_addition():
    session = ks.Session.local()
    result = session.sql("SELECT 10 + 5 AS result").collect()
    assert result.row_count == 1


def test_execute_subtraction():
    session = ks.Session.local()
    result = session.sql("SELECT 20 - 8 AS result").collect()
    assert result.row_count == 1


def test_execute_multiplication():
    session = ks.Session.local()
    result = session.sql("SELECT 6 * 7 AS result").collect()
    assert result.row_count == 1


def test_execute_division():
    session = ks.Session.local()
    result = session.sql("SELECT 15 / 3 AS result").collect()
    assert result.row_count == 1


def test_execute_null_check():
    session = ks.Session.local()
    result = session.sql(
        "SELECT CASE WHEN 1 = 1 THEN NULL ELSE 1 END AS val"
    ).collect()
    assert result.row_count == 1


def test_execute_cast():
    session = ks.Session.local()
    result = session.sql("SELECT CAST(42 AS BIGINT) AS val").collect()
    assert result.row_count == 1


def test_execute_try_cast():
    session = ks.Session.local()
    result = session.sql("SELECT TRY_CAST('not_a_number' AS INT) AS val").collect()
    assert result.row_count == 1


def test_execute_comparison():
    session = ks.Session.local()
    result = session.sql("SELECT 5 > 3 AS val").collect()
    assert result.row_count == 1


def test_execute_boolean():
    session = ks.Session.local()
    result = session.sql("SELECT TRUE AND FALSE AS val").collect()
    assert result.row_count == 1


def test_execute_alias():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS a, 2 AS b")
    result = df.select_columns([ks.col("a").alias("sum")]).collect()
    assert result.row_count == 1


def test_execute_ordering():
    session = ks.Session.local()
    df = session.sql(
        "SELECT val FROM (SELECT 3 AS val UNION ALL SELECT 1 UNION ALL SELECT 2) t"
    )
    result = df.sort(["val"])
    assert result.collect().row_count == 3


def test_execute_desc_ordering():
    session = ks.Session.local()
    df = session.sql(
        "SELECT val FROM (SELECT 1 AS val UNION ALL SELECT 3 UNION ALL SELECT 2) t"
    )
    result = df.order_by(["val"])
    assert result.collect().row_count == 3


def test_execute_is_null():
    session = ks.Session.local()
    result = session.sql(
        "SELECT CASE WHEN 1=1 THEN NULL ELSE 1 END IS NULL AS val"
    ).collect()
    assert result.row_count == 1


def test_execute_is_not_null():
    session = ks.Session.local()
    result = session.sql(
        "SELECT CASE WHEN 1=1 THEN 1 ELSE NULL END IS NOT NULL AS val"
    ).collect()
    assert result.row_count == 1


def test_literal_creation():
    c = ks.lit(42)
    assert "42" in c.sql()


def test_literal_string():
    c = ks.lit("hello")
    sql = c.sql()
    assert "hello" in sql


def test_literal_none():
    c = ks.lit(None)
    assert "NULL" in c.sql().upper()


def test_literal_bool():
    c = ks.lit(True)
    assert "TRUE" in c.sql().upper()


def test_literal_float():
    c = ks.lit(3.14)
    assert "3.14" in c.sql()


def test_column_combined_alias_and_ordering():
    c = ks.col("amount").alias("total").desc()
    sql = c.sql()
    assert '"amount"' in sql
    assert '"total"' in sql
    assert "DESC" in sql.upper()


def test_column_combined_cast_and_null_check():
    c = ks.col("x").cast("int").is_not_null()
    sql = c.sql()
    assert "CAST" in sql.upper()
    assert "IS NOT NULL" in sql.upper()


def test_column_combined_arithmetic_and_alias():
    c = (ks.col("price") * ks.col("qty")).alias("revenue")
    sql = c.sql()
    assert '"price"' in sql
    assert '"qty"' in sql
    assert '"revenue"' in sql


def test_column_combined_comparison_and_alias():
    c = (ks.col("x") > ks.lit(10)).alias("is_large")
    sql = c.sql()
    assert '"x"' in sql
    assert "10" in sql
    assert '"is_large"' in sql


def test_execute_combined_expression():
    session = ks.Session.local()
    df = session.sql("SELECT 10 AS price, 3 AS qty")
    result = df.select_columns([
        (ks.col("price") * ks.col("qty")).alias("revenue")
    ])
    text = result.collect().pretty()
    assert "revenue" in text
    assert "30" in text


def test_execute_combined_null_and_cast():
    session = ks.Session.local()
    df = session.sql("SELECT CAST(NULL AS INT) AS x, CAST(5 AS INT) AS y")
    result = df.select_columns([
        ks.col("x").cast("bigint").alias("x_big"),
        ks.col("y").is_not_null().alias("y_not_null"),
    ])
    text = result.collect().pretty()
    assert "x_big" in text
    assert "y_not_null" in text


def test_column_type_is_column():
    c = ks.col("x")
    assert isinstance(c, ks.Column)
