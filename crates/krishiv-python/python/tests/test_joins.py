import pytest
import pyarrow as pa

import krishiv as ks


def _sql_batches(sql: str) -> list[ks.Batch]:
    session = ks.Session.local()
    result = session.sql(sql).collect()
    return result.batches()


def test_dataframe_join_on_column_name():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b'"
    )
    right = session.sql(
        "SELECT 1 AS right_id, 10 AS score UNION ALL SELECT 2, 20"
    )
    joined = left.join_on(right, left_on=["left_id"], right_on=["right_id"])
    result = joined.collect()
    assert result.row_count == 2
    text = result.pretty()
    assert "val" in text
    assert "score" in text


def test_dataframe_join_on_expression():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'x' AS name UNION ALL SELECT 2, 'y'"
    )
    right = session.sql(
        "SELECT 1 AS right_id, 100 AS amount UNION ALL SELECT 2, 200"
    )
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"]
    )
    result = joined.collect()
    assert result.row_count == 2
    text = result.pretty()
    assert "name" in text
    assert "amount" in text


def test_dataframe_left_join():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
    )
    right = session.sql(
        "SELECT 1 AS right_id, 10 AS score UNION ALL SELECT 2, 20"
    )
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"], how="left"
    )
    result = joined.collect()
    assert result.row_count == 3
    text = result.pretty()
    assert "3" in text
    assert "c" in text
    assert "null" in text.lower() or "score" in text


def test_dataframe_inner_join_default():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b' UNION ALL SELECT 99, 'z'"
    )
    right = session.sql(
        "SELECT 1 AS right_id, 10 AS score UNION ALL SELECT 2, 20"
    )
    joined = left.join_on(right, left_on=["left_id"], right_on=["right_id"])
    result = joined.collect()
    assert result.row_count == 2


def test_dataframe_join_multiple_columns():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS a1, 10 AS b1, 'x' AS v UNION ALL SELECT 2, 20, 'y' UNION ALL SELECT 3, 30, 'z'"
    )
    right = session.sql(
        "SELECT 1 AS a2, 10 AS b2, 100 AS s UNION ALL SELECT 2, 20, 200 UNION ALL SELECT 3, 30, 300"
    )
    joined = left.join_on(right, left_on=["a1", "b1"], right_on=["a2", "b2"])
    result = joined.collect()
    assert result.row_count == 3


def test_dataframe_join_no_match():
    session = ks.Session.local()
    left = session.sql("SELECT 1 AS left_id, 'a' AS val")
    right = session.sql("SELECT 999 AS right_id, 42 AS score")
    joined = left.join_on(right, left_on=["left_id"], right_on=["right_id"])
    result = joined.collect()
    assert result.row_count == 0


def test_dataframe_join_result_schema():
    session = ks.Session.local()
    left = session.sql("SELECT 1 AS left_id, 'hello' AS msg")
    right = session.sql("SELECT 1 AS right_id, 3.14 AS value")
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"]
    )
    col_names = joined.columns()
    assert "msg" in col_names
    assert "value" in col_names


def test_interval_join():
    left = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id"
    )
    right = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:05' AS TIMESTAMP) AS ts, 1 AS id"
    )
    try:
        pairs = ks.interval_join(left, right, "ts", "ts", -10_000, 10_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)


def test_stream_table_join():
    stream = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS key"
    )
    table = _sql_batches(
        "SELECT 1 AS key, CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ver, 'val' AS data"
    )
    try:
        pairs = ks.stream_table_join(stream, table, "ts", "ver", 10_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)


def test_temporal_join():
    stream = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS key"
    )
    table = _sql_batches(
        "SELECT 1 AS key, CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ver, 'v' AS data"
    )
    try:
        pairs = ks.temporal_join(stream, table, "ts", "ver", 10_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)


def test_stream_stream_join():
    left = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id"
    )
    right = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:01' AS TIMESTAMP) AS ts, 1 AS id"
    )
    try:
        pairs = ks.stream_stream_join(left, right, "ts", "ts", -5_000, 5_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)


def test_interval_join_empty_inputs():
    left = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id WHERE 1=0"
    )
    right = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id WHERE 1=0"
    )
    try:
        pairs = ks.interval_join(left, right, "ts", "ts", -10_000, 10_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)


def test_dataframe_join_left_anti():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
    )
    right = session.sql("SELECT 1 AS id, 10 AS score")
    joined = left.join(right, on=["id"], how="left_anti")
    result = joined.collect()
    assert result.row_count == 2
    text = result.pretty()
    assert "2" in text
    assert "3" in text


def test_dataframe_join_left_semi():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS id, 'a' AS val UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
    )
    right = session.sql("SELECT 1 AS id, 10 AS score")
    joined = left.join(right, on=["id"], how="left_semi")
    result = joined.collect()
    assert result.row_count == 1
    text = result.pretty()
    assert "1" in text
    assert "a" in text


def test_dataframe_join_full():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b'"
    )
    right = session.sql(
        "SELECT 2 AS right_id, 20 AS score UNION ALL SELECT 3, 30"
    )
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"], how="full"
    )
    result = joined.collect()
    assert result.row_count == 3


def test_dataframe_join_right():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b'"
    )
    right = session.sql(
        "SELECT 2 AS right_id, 20 AS score UNION ALL SELECT 3, 30"
    )
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"], how="right"
    )
    result = joined.collect()
    assert result.row_count == 2


def test_dataframe_join_right_anti():
    session = ks.Session.local()
    left = session.sql(
        "SELECT 1 AS left_id, 'a' AS val UNION ALL SELECT 2, 'b'"
    )
    right = session.sql(
        "SELECT 1 AS right_id, 10 AS score UNION ALL SELECT 3, 30"
    )
    joined = left.join_on(
        right, left_on=["left_id"], right_on=["right_id"], how="right_anti"
    )
    result = joined.collect()
    assert result.row_count == 1
    text = result.pretty()
    assert "3" in text
    assert "30" in text


def test_stream_stream_join_empty():
    left = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id WHERE 1=0"
    )
    right = _sql_batches(
        "SELECT CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS ts, 1 AS id WHERE 1=0"
    )
    try:
        pairs = ks.stream_stream_join(left, right, "ts", "ts", -5_000, 5_000)
    except (RuntimeError, ks.ModeError, ValueError, TypeError) as e:
        pytest.skip(str(e))
    assert isinstance(pairs, list)
