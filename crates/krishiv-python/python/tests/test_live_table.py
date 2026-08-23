"""Migration coverage: the retired Python ``LiveTable`` maps 1:1 onto
``IncrementalDataFrame`` with zero functionality lost.

  s.live_table(name, sql)      -> s.sql(sql).to_incremental(name)
  .ingest_row(id, "insert")    -> .insert(batch)
  .ingest_row(id, "delete")    -> .delete(batch)
  .refresh()                   -> .step()   (auto after apply)
  .change_feed()               -> .next_change() / .last_output()
  .drop()                      -> handle lifecycle (the fresh per-view registry is
                                  freed when the IncrementalDataFrame is GC'd)
"""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks


def _orders_session(rows):
    s = ks.Session.embedded()
    d = tempfile.mkdtemp()
    p = os.path.join(d, "orders.parquet")
    pq.write_table(pa.table(rows), p)
    s.register_parquet("orders", p)
    return s


def test_create_incremental_view_and_ingest():
    s = _orders_session({"customer_id": [1], "amount": [0]})
    iv = s.sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    ).to_incremental("orders_summary")
    assert iv.name == "orders_summary"
    iv.insert(pa.record_batch({"customer_id": [1, 1, 2], "amount": [10, 20, 5]}))
    snap = iv.snapshot().to_arrow().to_pydict()
    got = dict(zip(snap["customer_id"], snap["total"]))
    assert got[1] == 30 and got[2] == 5


def _weights(delta):
    """DeltaBatch -> {row tuple: weight}; +1 inserted the row, -1 retracted it."""
    d = delta.to_batch().to_arrow().to_pydict()
    weights = d.pop("_weight")
    columns = list(d)
    return {tuple(d[c][i] for c in columns): weights[i] for i in range(len(weights))}


def test_change_feed_after_ingest():
    # The retired LiveTable.change_feed yielded the rows that changed, so this
    # asserts the rows — `is not None` would also pass on a stale delta left over
    # from an earlier tick, which is exactly what the coalescing peek serves.
    s = _orders_session({"customer_id": [1], "amount": [0]})
    iv = s.sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    ).to_incremental()
    iv.insert(pa.record_batch({"customer_id": [1], "amount": [10]}))
    assert _weights(iv.last_output()) == {(1, 10): 1}
    change = iv.next_change()
    assert change is not None and _weights(change) == {(1, 10): 1}

    # ingest_row(id, "delete") -> .delete(batch): the feed carries the retraction.
    iv.delete(pa.record_batch({"customer_id": [1], "amount": [10]}))
    assert _weights(iv.next_change()) == {(1, 10): -1}
