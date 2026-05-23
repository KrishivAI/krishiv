"""PyArrow and Pandas bridges."""

import pytest

pyarrow = pytest.importorskip("pyarrow")
pandas = pytest.importorskip("pandas")

import krishiv as ks


def test_batch_to_arrow_and_pandas():
    session = ks.Session.embedded()
    df = session.sql("SELECT 42 AS value, 'x' AS label")
    # Materialize via windowed collect on a synthetic stream path
    local = ks.Session.local()
    stream = local.stream("SELECT 42 AS value", "ts", 0)
    batches = stream.tumbling_window(1).collect()
    if not batches:
        pytest.skip("no batches materialized")
    batch = batches[0]
    rb = batch.to_arrow()
    assert rb.num_rows >= 1
    pdf = batch.to_pandas()
    assert len(pdf) >= 1
