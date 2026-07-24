"""IncrementalDataFrame (delta/IVM mode) — oracle, change-feed, transaction,
cross-mode, and export tests. The same DataFrame plan that runs in batch
(``collect()``) and streaming (``to_streaming()``) also drives an incrementally
maintained view via ``to_incremental()``."""

import asyncio
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks


def _session_with_orders(rows):
    """Embedded session with a registered base table ``orders`` (a real table,
    so the unparsed view SQL keeps ``FROM orders`` as a feedable source)."""
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table(rows), p)
    s.register_parquet("orders", p)
    return s


def _by_key(obj):
    """Normalize a PyBatch or pa.Table to a dict sorted by key ``k``."""
    d = obj.to_pydict() if isinstance(obj, pa.Table) else obj.to_arrow().to_pydict()
    order = sorted(range(len(d["k"])), key=lambda i: d["k"][i])
    return {c: [d[c][i] for i in order] for c in d}


VIEW = "SELECT k, SUM(v) AS total FROM orders GROUP BY k"


def test_export_present():
    assert hasattr(ks, "IncrementalDataFrame")
    assert "IncrementalDataFrame" in ks.__all__


def test_oracle_snapshot_matches_batch_recompute():
    rows = {"k": ["a", "a", "b", "c", "c", "c"], "v": [10, 20, 5, 1, 2, 3]}
    s = _session_with_orders(rows)
    batch = s.sql(VIEW).collect().to_arrow()  # batch oracle over the same rows
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch(rows))  # feed the identical rows as inserts
    snap = iv.snapshot()
    assert snap is not None
    assert _by_key(snap) == _by_key(batch)


def test_cross_mode_identical():
    # The SAME df object: collect() (batch) vs to_incremental() snapshot after
    # feeding the same rows must agree — the unification invariant.
    rows = {"k": ["x", "y", "x", "z"], "v": [3, 7, 4, 9]}
    s = _session_with_orders(rows)
    df = s.sql(VIEW)
    batch = df.collect().to_arrow()
    iv = df.to_incremental()
    iv.insert(pa.record_batch(rows))
    assert _by_key(iv.snapshot()) == _by_key(batch)


def test_delete_retracts():
    rows = {"k": ["a", "a", "b"], "v": [10, 20, 5]}
    s = _session_with_orders(rows)
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch(rows))
    iv.delete(pa.record_batch({"k": ["a"], "v": [20]}))  # retract one 'a' row
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "total")]))
    assert got["a"] == 10 and got["b"] == 5


def test_transaction_single_atomic_tick():
    s = _session_with_orders({"k": ["a"], "v": [1]})
    iv = s.sql(VIEW).to_incremental()
    before = iv.step().tick
    with iv.transaction():
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a", "a"], "v": [10, 20]})))
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["b"], "v": [5]})))
    after = iv.step().tick
    # exactly one tick fired inside the transaction (plus our explicit step()).
    assert after - before == 2
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "total")]))
    assert got["a"] == 30 and got["b"] == 5


def test_change_feed():
    s = _session_with_orders({"k": ["a"], "v": [1]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    out = iv.last_output()
    assert out is not None and out.num_rows >= 1

    async def drain():
        return [c async for c in iv.changes()]

    assert len(asyncio.run(drain())) >= 1
