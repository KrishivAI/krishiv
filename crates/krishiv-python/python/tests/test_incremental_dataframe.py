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


def _settle(iv, source="orders"):
    """Advance one extra tick with a benign row (a fresh region with amount 0 that
    every ``total >= N>0`` filter drops). A downstream view of an *incremental*
    upstream (aggregate) reflects the upstream's PREVIOUS-tick snapshot — the
    documented one-tick IVM lag (no lag for filter/projection upstreams) — so a
    DAG converges after one extra tick."""
    iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"region": ["__settle__"], "amount": [0]})),
             source=source)


def test_view_dag_cascade():
    # Base view v1 = per-region revenue (incremental aggregate). Derived view v2 =
    # high-revenue regions, built fluently on s.view(iv1) and co-registered into
    # v1's job. Feeding 'orders' cascades v1 -> v2 (topological multi-view IVM);
    # v2 lags v1 by one tick, so settle once.
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p)
    s.register_parquet("orders", p)

    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("rev")
    iv2 = s.view(iv1).filter("total >= 100").to_incremental("hi_rev")

    iv1.insert(pa.record_batch({"region": ["us", "us", "eu"], "amount": [100, 50, 40]}))
    _settle(iv1)  # propagate v1 -> v2 (one-tick lag)
    # us=150 (kept), eu=40 (filtered out) — the derived view reflects the cascade.
    snap = iv2.snapshot().to_arrow().to_pydict()
    got = dict(zip(snap["region"], snap["total"]))
    assert got == {"us": 150}, got


def test_view_dag_one_tick_lag_is_documented():
    # Pin the incremental-upstream lag: the derived view is empty right after the
    # first feed, then reflects the base after one more tick.
    s = ks.Session.embedded()
    d = tempfile.mkdtemp(); p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p); s.register_parquet("orders", p)
    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("rev")
    iv2 = s.view(iv1).filter("total >= 100").to_incremental("hi")
    iv1.insert(pa.record_batch({"region": ["us", "us"], "amount": [100, 50]}))
    assert iv2.snapshot() is None                 # lag: not yet propagated
    _settle(iv1)
    snap = iv2.snapshot().to_arrow().to_pydict()
    assert dict(zip(snap["region"], snap["total"])) == {"us": 150}   # converged


def test_change_feed():
    s = _session_with_orders({"k": ["a"], "v": [1]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    out = iv.last_output()
    assert out is not None and out.num_rows >= 1

    async def drain():
        return [c async for c in iv.changes()]

    assert len(asyncio.run(drain())) >= 1


# ─────────────────────── aggregate-type coverage ────────────────────────────
def _oracle_pair(view_sql, rows, keycol="k"):
    """orders registered WITH `rows`; return (batch_dict, incremental_snapshot_dict)
    both normalized/sorted by `keycol` — they must be equal."""
    s = _session_with_orders(rows)
    batch = s.sql(view_sql).collect().to_arrow().to_pydict()
    iv = s.sql(view_sql).to_incremental()
    iv.insert(pa.record_batch(rows))
    snap = iv.snapshot().to_arrow().to_pydict()

    def norm(d):
        order = sorted(range(len(d[keycol])), key=lambda i: str(d[keycol][i]))
        return {c: [d[c][i] for i in order] for c in d}

    return norm(batch), norm(snap)


def test_agg_count():
    b, s = _oracle_pair("SELECT k, COUNT(*) AS n FROM orders GROUP BY k",
                        {"k": ["a", "a", "b", "c", "c"], "v": [1, 2, 3, 4, 5]})
    assert b == s


def test_agg_min_max():
    b, s = _oracle_pair(
        "SELECT k, MIN(v) AS lo, MAX(v) AS hi FROM orders GROUP BY k",
        {"k": ["a", "a", "b", "b"], "v": [3, 1, 9, 4]})
    assert b == s


def test_agg_avg():
    rows = {"k": ["a", "a", "b"], "v": [10.0, 20.0, 7.0]}
    s = _session_with_orders(rows)
    view = "SELECT k, AVG(v) AS m FROM orders GROUP BY k"
    exp = s.sql(view).collect().to_arrow().to_pydict()
    exp = dict(zip(exp["k"], exp["m"]))
    iv = s.sql(view).to_incremental()
    iv.insert(pa.record_batch(rows))
    got = iv.snapshot().to_arrow().to_pydict()
    got = dict(zip(got["k"], got["m"]))
    assert abs(got["a"] - 15.0) < 1e-9 and abs(got["b"] - 7.0) < 1e-9
    assert set(got) == set(exp)


def test_multi_column_group_by():
    rows = {"region": ["us", "us", "eu"], "tier": ["gold", "silver", "gold"], "amt": [10, 20, 5]}
    s = ks.Session.embedded()
    d = tempfile.mkdtemp(); p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table(rows), p); s.register_parquet("orders", p)
    view = "SELECT region, tier, SUM(amt) AS total FROM orders GROUP BY region, tier"
    iv = s.sql(view).to_incremental()
    iv.insert(pa.record_batch(rows))
    got = {(r, t): tot for r, t, tot in zip(*[iv.snapshot().to_arrow().to_pydict()[c]
                                              for c in ("region", "tier", "total")])}
    assert got == {("us", "gold"): 10, ("us", "silver"): 20, ("eu", "gold"): 5}


def test_filter_projection_view():
    rows = {"k": ["a", "b", "c"], "v": [5, 25, 40]}
    s = _session_with_orders(rows)
    view = "SELECT k, v FROM orders WHERE v > 20"
    iv = s.sql(view).to_incremental()
    iv.insert(pa.record_batch(rows))
    got = dict(zip(*[iv.snapshot().to_arrow().to_pydict()[c] for c in ("k", "v")]))
    assert got == {"b": 25, "c": 40}


# ─────────────────────── Z-set change coverage ──────────────────────────────
def test_update_before_after():
    rows = {"k": ["a", "b"], "v": [10, 5]}
    s = _session_with_orders(rows)
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch(rows))
    # change a's 10 -> 100 via update(before, after)
    iv.update(pa.record_batch({"k": ["a"], "v": [10]}),
              pa.record_batch({"k": ["a"], "v": [100]}))
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "total")]))
    assert got == {"a": 100, "b": 5}


def test_incremental_across_multiple_feeds():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [5, 7]}))
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "total")]))
    assert got == {"a": 15, "b": 7}


# ─────────────────────── multi-source coverage ──────────────────────────────
def _session_two(a_rows, b_rows):
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    for name, rows in (("orders", a_rows), ("returns", b_rows)):
        p = os.path.join(d, f"{name}.parquet")
        pq.write_table(pa.table(rows), p)
        s.register_parquet(name, p)
    return s


def test_multi_source_join_feed_both():
    a = {"k": ["a", "b"], "amt": [100, 50]}
    b = {"k": ["a", "b"], "ret": [10, 5]}
    s = _session_two(a, b)
    view = ("SELECT o.k AS k, SUM(o.amt) - SUM(r.ret) AS net "
            "FROM orders o JOIN returns r ON o.k = r.k GROUP BY o.k")
    iv = s.sql(view).to_incremental()
    assert sorted(iv.source_names) == ["orders", "returns"]
    with iv.transaction():
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch(a)), source="orders")
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch(b)), source="returns")
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "net")]))
    assert got == {"a": 90, "b": 45}


def test_apply_explicit_source_single():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "v": [10]})), source="orders")
    got = dict(zip(*[_by_key(iv.snapshot())[c] for c in ("k", "total")]))
    assert got == {"a": 10}


def test_source_inference_error_on_multi_source():
    s = _session_two({"k": ["a"], "amt": [1]}, {"k": ["a"], "ret": [0]})
    iv = s.sql("SELECT o.k AS k, o.amt FROM orders o JOIN returns r ON o.k=r.k").to_incremental()
    import pytest
    with pytest.raises(Exception):
        # ambiguous: >1 source and no source= given
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "amt": [1]})))


# ─────────────────────── lifecycle / edge coverage ──────────────────────────
def test_empty_snapshot_before_feed():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    assert iv.snapshot() is None  # no output produced yet


def test_transaction_rollback_on_exception_does_not_tick():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    before = iv.step().tick
    try:
        with iv.transaction():
            iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "v": [99]})))
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    # __exit__ on exception must NOT fire the tick.
    assert iv.step().tick - before == 1  # only our explicit steps advanced


def test_auto_generated_names_unique():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    a = s.sql(VIEW).to_incremental()
    b = s.sql(VIEW).to_incremental()
    assert a.name != b.name and a.name and b.name


def test_getters():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental("myview")
    assert iv.name == "myview"
    assert iv.source_names == ["orders"]


# ─────────────────────── view-DAG coverage ──────────────────────────────────
def test_view_dag_three_level_chain():
    s = ks.Session.embedded()
    d = tempfile.mkdtemp(); p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p)
    s.register_parquet("orders", p)
    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("v1")
    iv2 = s.view(iv1).filter("total >= 50").to_incremental("v2")     # high-revenue regions
    iv3 = s.view(iv2).filter("total >= 100").to_incremental("v3")    # very-high-revenue
    iv1.insert(pa.record_batch({"region": ["us", "us", "eu", "ap"],
                                "amount": [100, 50, 60, 30]}))
    _settle(iv1)  # propagate v1 -> v2 -> v3 (v1 is incremental; the filter chain
    _settle(iv1)  # cascades within a tick, so 2 settles fully converge all levels)
    # us=150, eu=60, ap=30 -> v2 keeps {us,eu} -> v3 keeps {us}
    got2 = set(_by_key_col(iv2.snapshot(), "region"))
    got3 = set(_by_key_col(iv3.snapshot(), "region"))
    assert got2 == {"us", "eu"} and got3 == {"us"}


def _by_key_col(batch, col):
    return batch.to_arrow().to_pydict()[col]
