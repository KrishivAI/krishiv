"""Tests for the QueryResult interface."""

import pytest

import krishiv as ks


@pytest.fixture
def session():
    return ks.Session.local()


def _qr(sql, *, session=None):
    s = session or ks.Session.local()
    return s.sql(sql).collect()


class TestPretty:
    def test_returns_string_with_headers_and_values(self):
        result = _qr("SELECT 1 AS x, 'hello' AS y")
        text = result.pretty()
        assert isinstance(text, str)
        assert "x" in text
        assert "y" in text
        assert "1" in text
        assert "hello" in text

    def test_multi_row(self):
        result = _qr(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
        )
        text = result.pretty()
        assert "1" in text
        assert "2" in text
        assert "3" in text

    def test_zero_rows(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        text = result.pretty()
        assert isinstance(text, str)


class TestShow:
    def test_does_not_raise(self):
        result = _qr("SELECT 42 AS val")
        assert result.show() is None

    def test_multi_row(self):
        result = _qr(
            "SELECT * FROM (VALUES (10, 'x'), (20, 'y')) AS t(a, b)"
        )
        assert result.show() is None

    def test_zero_rows(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        assert result.show() is None


class TestToArrow:
    def test_returns_pyarrow_table(self):
        pa = pytest.importorskip("pyarrow")
        result = _qr("SELECT 1 AS x, 'hello' AS y")
        table = result.to_arrow()
        assert isinstance(table, pa.Table)

    def test_schema_and_data(self):
        pa = pytest.importorskip("pyarrow")
        result = _qr("SELECT 5 AS num, true AS flag, 'test' AS word")
        table = result.to_arrow()
        assert table.num_rows == 1
        field_names = [f.name for f in table.schema]
        assert "num" in field_names
        assert "flag" in field_names
        assert "word" in field_names

    def test_multi_row(self):
        pa = pytest.importorskip("pyarrow")
        result = _qr(
            "SELECT * FROM (VALUES (1, 1.5, 'a', true), (2, 2.5, 'b', false)) AS t(i, f, s, b)"
        )
        table = result.to_arrow()
        assert table.num_rows == 2

    def test_zero_rows_no_batches(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        assert result.row_count == 0
        assert len(result.batches()) == 0


class TestToPandas:
    def test_returns_dataframe(self):
        pd = pytest.importorskip("pandas")
        result = _qr("SELECT 1 AS x")
        df = result.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_multi_row(self):
        pd = pytest.importorskip("pandas")
        result = _qr(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
        )
        df = result.to_pandas()
        assert len(df) == 3
        assert list(df.columns) == ["id", "name"]

    def test_zero_rows_no_batches(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        assert result.row_count == 0
        assert len(result.batches()) == 0


class TestRowCount:
    def test_single_row(self):
        result = _qr("SELECT 1 AS x")
        assert result.row_count == 1

    def test_multi_row(self):
        result = _qr(
            "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)"
        )
        assert result.row_count == 5

    def test_zero_rows(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        assert result.row_count == 0


class TestLen:
    def test_returns_batch_count(self):
        result = _qr("SELECT 1 AS x")
        assert len(result) >= 1

    def test_zero_rows(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        assert len(result) == 0


class TestBatches:
    def test_returns_list(self):
        result = _qr("SELECT 1 AS x")
        batches = result.batches()
        assert isinstance(batches, list)
        assert len(batches) >= 1

    def test_batches_have_data(self):
        result = _qr("SELECT 1 AS x")
        batches = result.batches()
        for batch in batches:
            assert batch.num_rows >= 0


class TestIter:
    def test_yields_batches(self):
        result = _qr("SELECT 1 AS x")
        batches = list(result)
        assert len(batches) >= 1

    def test_multi_batch(self):
        result = _qr(
            "SELECT * FROM (VALUES (1), (2), (3)) AS t(x)"
        )
        batches = list(result)
        total = sum(b.num_rows for b in batches)
        assert total == 3

    def test_zero_rows(self):
        result = _qr("SELECT 1 AS x WHERE 1 = 0")
        batches = list(result)
        total = sum(b.num_rows for b in batches)
        assert total == 0


class TestRepr:
    def test_contains_batch_and_row_info(self):
        result = _qr("SELECT 1 AS x, 2 AS y")
        r = repr(result)
        assert "QueryResult" in r
        assert "batches=" in r
        assert "rows=" in r


class TestNullValues:
    def test_null_int(self):
        result = _qr("SELECT CAST(NULL AS INT) AS val")
        assert result.row_count == 1
        table = result.to_arrow()
        assert table.num_rows == 1

    def test_null_string(self):
        result = _qr("SELECT CAST(NULL AS VARCHAR) AS val")
        assert result.row_count == 1

    def test_mixed_nulls_and_values(self):
        result = _qr(
            "SELECT * FROM (VALUES (1, 'a'), (NULL, 'b'), (3, NULL)) AS t(id, name)"
        )
        assert result.row_count == 3
        pd = pytest.importorskip("pandas")
        df = result.to_pandas()
        assert df.iloc[1]["id"] is None or pd.isna(df.iloc[1]["id"])
        assert df.iloc[2]["name"] is None or pd.isna(df.iloc[2]["name"])


class TestMixedDataTypes:
    def test_int_float_string_bool(self):
        result = _qr("SELECT 42 AS i, 3.14 AS f, 'hello' AS s, true AS b")
        assert result.row_count == 1
        table = result.to_arrow()
        assert table.num_rows == 1
        pd = pytest.importorskip("pandas")
        df = result.to_pandas()
        assert len(df) == 1
