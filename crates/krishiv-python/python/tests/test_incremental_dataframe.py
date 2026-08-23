"""IncrementalDataFrame (delta/IVM mode) — oracle, change-feed, transaction,
cross-mode, and export tests. The same DataFrame plan that runs in batch
(``collect()``) and streaming (``to_streaming()``) also drives an incrementally
maintained view via ``to_incremental()``."""

import ast
import os
import pathlib
import tempfile
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
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


def _weights(delta):
    """DeltaBatch -> {row tuple: weight}; +1 inserted the row, -1 retracted it."""
    d = delta.to_batch().to_arrow().to_pydict()
    weights = d.pop("_weight")
    columns = list(d)
    return {tuple(d[c][i] for c in columns): weights[i] for i in range(len(weights))}


def _totals(iv):
    """The view's snapshot as {k: total}."""
    snapshot = _by_key(iv.snapshot())
    return dict(zip(snapshot["k"], snapshot["total"]))


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


def test_view_dag_cascade():
    # Base view v1 = per-region revenue (incremental aggregate). Derived view v2 =
    # high-revenue regions, built fluently on s.view(iv1) and co-registered into
    # v1's job. Feeding 'orders' cascades v1 -> v2 (topological multi-view IVM)
    # within the SAME tick, so one feed is the whole story.
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p)
    s.register_parquet("orders", p)

    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("rev")
    iv2 = s.view(iv1).filter("total >= 100").to_incremental("hi_rev")

    iv1.insert(pa.record_batch({"region": ["us", "us", "eu"], "amount": [100, 50, 40]}))
    # us=150 (kept), eu=40 (filtered out) — the derived view reflects the cascade.
    snap = iv2.snapshot().to_arrow().to_pydict()
    got = dict(zip(snap["region"], snap["total"]))
    assert got == {"us": 150}, got


def test_view_dag_resolves_in_the_same_tick():
    # ONE feed is enough: the engine walks the view DAG in topological order, so
    # the derived view reads the base's output in the tick that produced it
    # (IVM-AUD-CORE-17). This was once a one-tick lag — a permanently EMPTY
    # derived view, in fact — so both halves are pinned here: the derived view
    # carries the base's rows after the first feed, and the tick that produced
    # them reports no error for it.
    s = ks.Session.embedded()
    d = tempfile.mkdtemp(); p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p); s.register_parquet("orders", p)
    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("rev")
    iv2 = s.view(iv1).filter("total >= 100").to_incremental("hi")
    report = iv1.insert(pa.record_batch({"region": ["us", "us"], "amount": [100, 50]}))
    assert report.errored_views == []
    snap = iv2.snapshot()
    assert snap is not None, "the derived view must not need a second tick"
    snap = snap.to_arrow().to_pydict()
    assert dict(zip(snap["region"], snap["total"])) == {"us": 150}


def test_change_feed():
    s = _session_with_orders({"k": ["a"], "v": [1]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    # WHAT the tick published, not merely that something is there: the peek is a
    # coalescing watch, so `is not None` holds forever after any output.
    assert _weights(iv.last_output()) == {("a", 10): 1, ("b", 5): 1}
    change = iv.next_change()
    assert change is not None and _weights(change) == {("a", 10): 1, ("b", 5): 1}
    assert iv.next_change() is None  # consumed exactly once


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
    with pytest.raises(RuntimeError, match=r"reads 2 sources .*pass source="):
        # ambiguous: >1 source and no source= given
        iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "amt": [1]})))


# ─────────────────────── lifecycle / edge coverage ──────────────────────────
def test_empty_snapshot_before_feed():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    assert iv.snapshot() is None  # no output produced yet


def test_transaction_abort_discards_the_feeds_entirely():
    # The invariant is not "no tick fired" (a later tick would apply the feed
    # anyway, which is what the old version of this test missed): it is that the
    # engine never received the aborted feed at all.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a"], "v": [1]}))  # committed baseline
    before = iv.step().tick
    with pytest.raises(RuntimeError, match="boom"):
        with iv.transaction():
            iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "v": [99]})))
            raise RuntimeError("boom")
    # __exit__ on exception must not fire the tick ...
    assert iv.step().tick - before == 1  # only our explicit steps advanced
    # ... and no later tick can surface the discarded feed either.
    iv.step()
    iv.step()
    assert _totals(iv) == {"a": 1}


def test_nested_transactions_commit_once_at_the_outermost_exit():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    before = iv.step().tick
    with iv.transaction():
        iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
        with iv.transaction():
            iv.insert(pa.record_batch({"k": ["b"], "v": [5]}))
        # The inner exit must not commit: nothing is visible yet.
        assert iv.snapshot() is None
    assert _totals(iv) == {"a": 10, "b": 5}
    assert iv.step().tick - before == 2  # one commit tick + this explicit step


def test_nested_transaction_abort_discards_only_its_own_feeds():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    with iv.transaction():
        iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
        with pytest.raises(RuntimeError, match="inner"):
            with iv.transaction():
                iv.insert(pa.record_batch({"k": ["b"], "v": [5]}))
                raise RuntimeError("inner")
        iv.insert(pa.record_batch({"k": ["c"], "v": [7]}))
    assert _totals(iv) == {"a": 10, "c": 7}  # 'b' was rolled back, 'a'/'c' were not


def test_session_view_does_not_relabel_an_unrelated_failure(monkeypatch):
    # The TypeError s.view() raises names ONE diagnosis: the view's snapshot does
    # not fit its declared output schema. Anything else going wrong in that cast
    # — a missing pyarrow method on an old install, a bug in this function — is a
    # different problem, and reporting it as the view's fault is a false
    # diagnosis. Only Arrow's own cast failures are relabelled.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
    assert s.view(iv).count() == 1  # the happy path still works

    import sys
    import types

    def exploding(*args, **kwargs):
        raise AttributeError("type object 'RecordBatch' has no attribute 'cast'")

    # `_session_view` imports pyarrow when it runs, so a stand-in module reaches
    # the cast without touching pyarrow's (immutable) types. Everything else is
    # the real pyarrow — only the one call the cast goes through is broken.
    proxy = types.ModuleType("pyarrow")
    proxy.__dict__.update(pa.__dict__)
    proxy.Table = types.SimpleNamespace(from_batches=exploding)
    monkeypatch.setitem(sys.modules, "pyarrow", proxy)
    with pytest.raises(AttributeError, match="no attribute 'cast'"):
        s.view(iv)


def test_a_failed_abort_does_not_replace_the_users_exception():
    # __exit__ still has to abort while the block unwinds, and that abort can
    # itself fail. When it does, the user's exception is what propagates — the
    # abort's failure is attached, not substituted. Before this, a corrupted
    # block surfaced "transaction() exited without a matching enter" and the
    # real error was gone.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    with pytest.raises(RuntimeError, match="the user's own failure") as caught:
        with iv.transaction():
            iv.apply(ks.DeltaBatch.from_inserts(pa.record_batch({"k": ["a"], "v": [99]})))
            # Close the block out from under __exit__, so its own abort raises.
            iv._txn_exit(False)
            raise RuntimeError("the user's own failure")
    reported = repr(caught.value) + " ".join(getattr(caught.value, "__notes__", []))
    assert "without a matching enter" in reported, (
        "the abort's failure must still be reported somewhere, not swallowed"
    )
    assert iv.snapshot() is None  # and the aborted feed still never reached the engine


def test_transaction_rejects_a_feed_from_another_thread():
    # A second thread feeding mid-block would either be swallowed into someone
    # else's atomic tick or race its commit; it is refused instead.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    failures = []

    def feed_from_elsewhere():
        try:
            iv.insert(pa.record_batch({"k": ["z"], "v": [99]}))
        except Exception as exc:  # noqa: BLE001 - recorded and asserted below
            failures.append(exc)

    with iv.transaction():
        iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
        other = threading.Thread(target=feed_from_elsewhere)
        other.start()
        other.join()
    assert failures and "another thread" in str(failures[0])
    assert _totals(iv) == {"a": 10}


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
    # One feed converges every level: the DAG is walked in topological order,
    # so v1 -> v2 -> v3 all resolve inside the tick.
    # us=150, eu=60, ap=30 -> v2 keeps {us,eu} -> v3 keeps {us}
    got2 = set(_by_key_col(iv2.snapshot(), "region"))
    got3 = set(_by_key_col(iv3.snapshot(), "region"))
    assert got2 == {"us", "eu"} and got3 == {"us"}


def _by_key_col(batch, col):
    return batch.to_arrow().to_pydict()[col]


# ─────────────────────── CDC event coverage ─────────────────────────────────
def test_apply_cdc_insert_delete_and_update():
    # before=/after= are the row images of one change event, exactly as in
    # DeltaBatch.from_cdc. (This method used to pass its single argument as
    # `before`, so every event — including inserts — was applied as a DELETE.)
    rows = {"k": ["a", "b"], "v": [10, 5]}
    s = _session_with_orders(rows)
    iv = s.sql(VIEW).to_incremental()

    iv.apply_cdc(after=pa.record_batch(rows))  # INSERT
    assert _totals(iv) == {"a": 10, "b": 5}

    iv.apply_cdc(before=pa.record_batch({"k": ["b"], "v": [5]}))  # DELETE
    assert _totals(iv) == {"a": 10}

    # UPDATE: one atomic delta carrying the retraction and the insertion.
    iv.apply_cdc(before=pa.record_batch({"k": ["a"], "v": [10]}),
                 after=pa.record_batch({"k": ["a"], "v": [70]}))
    assert _totals(iv) == {"a": 70}
    assert _weights(iv.next_change()) == {("a", 10): -1, ("a", 70): 1}


def test_apply_cdc_without_a_row_image_raises():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    with pytest.raises(ValueError, match="at least one row image"):
        iv.apply_cdc()


def test_apply_cdc_refuses_an_unlabelled_row_image():
    # The old signature took one positional "event" and silently treated it as
    # the BEFORE image; a caller written against it now gets a TypeError instead
    # of a delete it did not ask for.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    with pytest.raises(TypeError):
        iv.apply_cdc(pa.record_batch({"k": ["a"], "v": [10]}))


# ─────────────────────── step-failure surfacing ─────────────────────────────
def test_a_view_that_cannot_be_evaluated_raises_instead_of_going_quiet():
    # errored_views is the engine's only per-view failure channel: a view that
    # fails to evaluate is skipped and its snapshot silently stops changing.
    # apply()/step() must not swallow that.
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
    assert _totals(iv) == {"a": 10}

    with pytest.raises(RuntimeError, match="failed to evaluate"):
        # a delta whose columns the view's operators cannot read
        iv.insert(pa.record_batch({"kk": ["a"], "vv": [1]}))

    # and the snapshot is unchanged, which is exactly why silence was wrong
    assert _totals(iv) == {"a": 10}


def test_a_sibling_views_failure_is_reported_not_raised():
    # A view that fails belongs to ITS handle. Here the derived view narrows the
    # base's total to a TINYINT, which the data overflows; the base view itself
    # is fine. Feeding through the base handle must therefore not raise — the
    # sibling's failure is reported through the summary instead.
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"region": ["us"], "amount": [0]}), p)
    s.register_parquet("orders", p)
    iv1 = s.sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region").to_incremental("base")
    s.view(iv1).select_exprs(["region", "CAST(total AS TINYINT) AS total"]).to_incremental("derived")

    report = iv1.insert(pa.record_batch({"region": ["us"], "amount": [200]}))
    assert [e.view for e in report.errored_views] == ["derived"]
    assert _by_key_col(iv1.snapshot(), "region") == ["us"]  # the base is fine


# ─────────────────────── Session.view coverage ──────────────────────────────
def test_view_collects_the_views_rows():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental("rev_read")
    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    got = s.view(iv).collect().to_arrow().to_pydict()
    assert dict(zip(got["k"], got["total"])) == {"a": 10, "b": 5}
    assert s.view(iv).count() == 2


def test_view_registers_no_table_and_no_temp_files(monkeypatch):
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental("rev_clean")
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))

    def _no_temp_dirs(*args, **kwargs):
        raise AssertionError("Session.view must not create temp directories")

    monkeypatch.setattr(tempfile, "mkdtemp", _no_temp_dirs)
    df = s.view(iv)
    # The registration lives only as long as planning: nothing is left behind to
    # shadow a later table of the same name, and the plan still has the rows.
    assert s.table_exists("rev_clean") is False
    assert df.count() == 1


def test_view_refuses_to_shadow_a_real_table():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    d = tempfile.mkdtemp()
    p = os.path.join(d, "report.parquet")
    pq.write_table(pa.table({"k": ["z"], "total": [1]}), p)
    s.register_parquet("report", p)

    iv = s.sql(VIEW).to_incremental("report")  # deliberately collides
    with pytest.raises(ValueError, match="already has a table named"):
        s.view(iv)
    # ...and the real table survives, readable and unchanged.
    assert s.table_exists("report")
    assert s.sql("SELECT * FROM report").collect().to_arrow().to_pydict()["k"] == ["z"]


def test_view_name_that_needs_quoting():
    s = _session_with_orders({"k": ["a"], "v": [0]})
    iv = s.sql(VIEW).to_incremental("group")  # a reserved word, unquotable bare
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
    assert s.view(iv).count() == 1


# ─────────────────────── published type surface ─────────────────────────────
IVM_STUB_CLASSES = ("DeltaBatch", "IncrementalDataFrame", "IvmJob", "StepSummary", "ViewError")


def test_type_stub_publishes_the_ivm_surface():
    """The ``.pyi`` is the published type surface for the IVM classes, checked
    both ways and with no allowlist in either direction.

    Completeness: every public attribute those classes carry at runtime — Rust
    ``#[pymethods]``, ``#[pyo3(get)]`` fields, and the conveniences grafted on in
    ``krishiv._pyspark`` alike — must be declared, so a newly added API cannot
    ship invisible to type checkers. Truthfulness: every member declared must
    exist at runtime. Private names (leading underscore) are exempt from the
    completeness half only; the stub may still declare them, and what it
    declares is still checked against the runtime.
    """
    stub = pathlib.Path(ks.__file__).with_name("krishiv.pyi").read_text(encoding="utf-8")
    classes = {n.name: n for n in ast.parse(stub).body if isinstance(n, ast.ClassDef)}

    def members(class_name):
        return {
            n.name
            for n in classes[class_name].body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

    for name in IVM_STUB_CLASSES:
        assert name in classes, f"{name} is missing from krishiv.pyi"

    for class_name in IVM_STUB_CLASSES:
        runtime = getattr(ks, class_name)
        declared = members(class_name)
        live = {
            name
            for name in dir(runtime)
            if not name.startswith("_") and name not in vars(object)
        }
        assert not (live - declared), (
            f"krishiv.pyi does not declare {sorted(live - declared)} on {class_name}, "
            f"which exists at runtime — regenerate it with scripts/check_api_surface.py --write"
        )
        assert not {m for m in declared if not hasattr(runtime, m)}, (
            f"krishiv.pyi declares {sorted(m for m in declared if not hasattr(runtime, m))} "
            f"on {class_name}, which does not exist at runtime"
        )

    # The two entry points into the IVM surface live on the batch classes.
    assert "to_incremental" in members("DataFrame") and hasattr(ks.DataFrame, "to_incremental")
    assert "view" in members("Session") and hasattr(ks.Session, "view")
