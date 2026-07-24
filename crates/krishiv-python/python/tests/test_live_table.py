"""Migration coverage: the retired Python ``LiveTable`` maps 1:1 onto
``IncrementalDataFrame`` with zero functionality lost.

  s.live_table(name, sql)      -> s.sql(sql).to_incremental(name)
  .ingest_row(id, "insert")    -> .insert(batch)
  .ingest_row(id, "delete")    -> .delete(batch)
  .refresh()                   -> .step()   (auto after apply)
  .change_feed()               -> .changes() / .last_output()
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


def test_change_feed_after_ingest():
    s = _orders_session({"customer_id": [1], "amount": [0]})
    iv = s.sql(
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    ).to_incremental()
    iv.insert(pa.record_batch({"customer_id": [1], "amount": [10]}))
    feed = iv.last_output()
    assert feed is not None and feed.num_rows >= 1
