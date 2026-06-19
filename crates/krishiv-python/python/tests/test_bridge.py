"""Pandas / PyArrow batch bridges."""

import pytest

import krishiv as ks


def test_batch_to_arrow_and_pandas():
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")

    batch = ks.make_example_batch()
    assert batch.num_rows == 3
    arrow_batch = batch.to_arrow()
    assert arrow_batch.num_rows == 3
    assert arrow_batch.schema.field("n").type == pa.int64()
    df = batch.to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3


def test_batch_repr_html():
    batch = ks.make_example_batch()
    html = batch._repr_html_()
    assert "<table>" in html
    assert "n" in html
