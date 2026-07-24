"""Streaming transformation chain on the unified StreamingDataFrame."""

import pyarrow as pa

import krishiv as ks
from krishiv import agg as kagg
from krishiv.krishiv import Batch

DAY = 24 * 3600 * 1000


def _events(s):
    tbl = pa.table({
        "user_id": ["u1", "u2", "u1", "u2", "u1"],
        "amount": [1.0, 2.0, 3.0, 4.0, 5.0],
        "ts": [0, 100 * DAY, 200 * DAY, 100 * DAY, 0],
    })
    s.register_record_batches("events", [Batch(b) for b in tbl.to_batches()])
    return s.sql("SELECT * FROM events")


def test_with_watermark_and_key_by_chain():
    s = ks.Session.embedded()
    sdf = (_events(s).to_streaming()
           .with_event_time("ts").with_watermark_lag(5000).key_by("user_id"))
    assert type(sdf).__name__ == "StreamingDataFrame"


def test_windowed_agg_chain_conserves_rows():
    s = ks.Session.embedded()
    sdf = (_events(s).to_streaming().with_event_time("ts").key_by("user_id")
           .tumbling_window(30 * DAY)
           .agg(events=kagg.count(), total=kagg.sum("amount")))
    t = pa.Table.from_batches([b.to_arrow() for b in sdf.collect()])
    assert {"events", "total"} <= set(t.schema.names)
    assert sum(t.column("events").to_pylist()) == 5  # every row bucketed
    assert abs(sum(t.column("total").to_pylist()) - 15.0) < 1e-6


def test_session_stream_sql_source_is_streaming_dataframe():
    s = ks.Session.embedded()
    sdf = s.stream("SELECT n, ts FROM (VALUES (1, 1000)) t(n, ts)", "ts", 0)
    assert type(sdf).__name__ == "StreamingDataFrame"
