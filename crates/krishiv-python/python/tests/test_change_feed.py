"""Change-feed coverage on IncrementalDataFrame (replaces the retired
LiveTable.change_feed).

There are two readers with deliberately different contracts, and these tests pin
both by asserting the delta's CONTENTS — ``is not None`` cannot fail once any
tick has ever produced output, because the engine's peek is a *coalescing watch*
that keeps serving the last value forever:

* ``last_output()`` — the peek: repeats, and survives a tick that published
  nothing;
* ``next_change()`` — the cursor over it: hands each published delta over at
  most once, and returns ``None`` after a tick that published nothing.
"""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

VIEW = "SELECT k, SUM(v) AS total FROM orders GROUP BY k"


def _orders_session():
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table({"k": ["a"], "v": [0]}), p)
    s.register_parquet("orders", p)
    return s


def _weights(delta):
    """DeltaBatch -> {row tuple: weight}; +1 inserted the row, -1 retracted it."""
    d = delta.to_batch().to_arrow().to_pydict()
    weights = d.pop("_weight")
    columns = list(d)
    return {tuple(d[c][i] for c in columns): weights[i] for i in range(len(weights))}


def test_change_feed_reflects_each_tick():
    s = _orders_session()
    iv = s.sql(VIEW).to_incremental()

    iv.insert(pa.record_batch({"k": ["a", "b"], "v": [10, 5]}))
    assert _weights(iv.last_output()) == {("a", 10): 1, ("b", 5): 1}

    # The cursor hands that delta over exactly once.
    first = iv.next_change()
    assert first is not None
    assert _weights(first) == {("a", 10): 1, ("b", 5): 1}
    assert iv.next_change() is None

    # A delete publishes the retraction — and only the retraction.
    iv.delete(pa.record_batch({"k": ["b"], "v": [5]}))
    second = iv.next_change()
    assert second is not None
    assert _weights(second) == {("b", 5): -1}
    assert iv.next_change() is None


def test_tick_without_output_does_not_re_serve_the_previous_delta():
    s = _orders_session()
    iv = s.sql(VIEW).to_incremental()
    iv.insert(pa.record_batch({"k": ["a"], "v": [10]}))
    assert iv.next_change() is not None

    iv.step()  # nothing was fed, so this tick publishes nothing
    assert iv.next_change() is None
    # ...while the peek still shows the PREVIOUS tick's delta, indistinguishable
    # from a fresh one. That gap is why the cursor exists; pin it so it stays
    # documented rather than rediscovered.
    assert _weights(iv.last_output()) == {("a", 10): 1}


def test_change_feed_is_empty_before_the_first_tick():
    s = _orders_session()
    iv = s.sql(VIEW).to_incremental()
    assert iv.last_output() is None
    assert iv.next_change() is None
