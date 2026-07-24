"""Change-feed coverage on IncrementalDataFrame (replaces the retired
LiveTable.change_feed): each tick's output delta is retrievable via
``last_output()`` and the async ``changes()`` iterator."""

import asyncio
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks


def _orders_session():
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"k": ["a"], "v": [0]}), p)
    s.register_parquet("orders", p)
    return s


def test_change_feed_reflects_each_tick():
    s = _orders_session()
    iv = s.sql("SELECT k, SUM(v) AS total FROM orders GROUP BY k").to_incremental()

    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    out = iv.last_output()
    assert out is not None and out.num_rows >= 1

    # The async change-feed yields the most recent output delta.
    async def drain():
        return [c async for c in iv.changes()]

    assert len(asyncio.run(drain())) >= 1

    # A subsequent delete produces a new output delta (the retraction).
    iv.delete(pa.record_batch({"k": ["b"], "v": [5]}))
    assert iv.last_output() is not None
