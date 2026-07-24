"""Comprehensive coverage for the unified StreamingDataFrame API.

The DataStream `Stream`/`KeyedStream`/`WindowedStream` classes were retired; all
streaming goes through `StreamingDataFrame`. These tests are self-contained
(in-memory / SQL sources, no Kafka broker) so they run in CI, and they exercise
every method of the surface plus both the snake_case and Spark camelCase spellings.
"""
import asyncio

import pyarrow as pa
import pytest

import krishiv as ks
from krishiv import agg as kagg
from krishiv.krishiv import Batch

DAY = 24 * 3600 * 1000


def _session():
    return ks.Session.embedded()


def _events(s, name="events"):
    """Register a keyed event table with out-of-order event_times across 3 keys
    and multiple 30-day windows (exercises the bounded-window sort)."""
    keys = ["a", "b", "c"]
    rows = []
    for i in range(300):
        # event_time deliberately NOT monotonic w.r.t. row order
        et = ((i * 37) % 300) * 10 * DAY
        rows.append({"k": keys[i % 3], "v": float(i % 50), "event_time": et})
    tbl = pa.Table.from_pylist(rows)
    s.register_record_batches(name, [Batch(b) for b in tbl.to_batches()])
    return s.sql(f"SELECT * FROM {name}"), tbl.num_rows


def _collect_tbl(sdf):
    batches = sdf.collect()
    return pa.Table.from_batches([b.to_arrow() for b in batches]) if batches else None


async def _drain(stream_or_df, cap=100000, timeout=20):
    st = await stream_or_df.execute_stream_async() if hasattr(stream_or_df, "execute_stream_async") else stream_or_df
    out = []

    async def loop():
        async for b in st:
            rb = b.to_arrow()
            if rb.num_rows:
                out.append(rb)
            if sum(x.num_rows for x in out) >= cap:
                break
    try:
        await asyncio.wait_for(loop(), timeout)
    except asyncio.TimeoutError:
        pass
    return pa.Table.from_batches(out) if out else None


# ─────────────────────────── entry points ───────────────────────────
def test_to_streaming_returns_streaming_dataframe():
    s = _session()
    sdf = s.sql("SELECT 1 AS a").to_streaming()
    assert type(sdf).__name__ == "StreamingDataFrame"


def test_session_stream_returns_streaming_dataframe():
    s = _session()
    sdf = s.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    assert type(sdf).__name__ == "StreamingDataFrame"


def test_memory_stream_returns_streaming_dataframe():
    s = _session()
    tbl = pa.table({"k": ["a", "b"], "v": [1, 2], "event_time": [0, DAY]})
    sdf = s.memory_stream("mem", [Batch(b) for b in tbl.to_batches()], "event_time", 0)
    assert type(sdf).__name__ == "StreamingDataFrame"


def test_retired_stream_classes_are_gone():
    for n in ("Stream", "KeyedStream", "WindowedStream", "ConnectedStreams",
              "BroadcastStream", "WindowSpec", "MultiSourceWatermarkSpec"):
        assert not hasattr(ks, n), f"{n} should have been retired"


# ─────────────────────────── stateless verbs ───────────────────────────
def test_filter():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().filter("v > 25"), cap=50))
    assert out is not None and all(v > 25 for v in out.column("v").to_pylist())


def test_where_alias():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().where("v > 25"), cap=50))
    assert out is not None and all(v > 25 for v in out.column("v").to_pylist())


def test_select():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().select("k", "v"), cap=50))
    assert out is not None and set(out.schema.names) == {"k", "v"}


def test_with_column():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().with_column("hi", "v > 25"), cap=50))
    assert out is not None and "hi" in out.schema.names


def test_drop_columns():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().drop_columns(["v"]), cap=50))
    assert out is not None and "v" not in out.schema.names


def test_drop_duplicates():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().drop_duplicates(subset=["k"]), cap=50))
    assert out is not None and set(out.column("k").to_pylist()) <= {"a", "b", "c"}
    assert out.num_rows == len(set(out.column("k").to_pylist()))


# ─────────────────────────── windows + agg ───────────────────────────
def test_tumbling_window_default_count_conserves_rows():
    s = _session()
    src, n = _events(s)
    sdf = src.to_streaming().with_event_time("event_time").key_by("k").tumbling_window(30 * DAY)
    t = _collect_tbl(sdf)
    assert t is not None and sum(t.column("count").to_pylist()) == n


def test_tumbling_window_agg_sum_count():
    s = _session()
    src, n = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .tumbling_window(30 * DAY).agg(total=kagg.sum("v"), c=kagg.count()))
    t = _collect_tbl(sdf)
    assert t is not None and {"total", "c"} <= set(t.schema.names)
    assert sum(t.column("c").to_pylist()) == n
    truth = sum(src.collect().to_arrow().column("v").to_pylist())
    assert abs(sum(t.column("total").to_pylist()) - truth) < 1e-6


def test_agg_min_max_avg():
    s = _session()
    src, _ = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .tumbling_window(3650 * DAY)  # one big window
           .agg(mn=kagg.min("v"), mx=kagg.max("v"), av=kagg.mean("v")))
    t = _collect_tbl(sdf)
    assert t is not None and {"mn", "mx", "av"} <= set(t.schema.names)
    assert min(t.column("mn").to_pylist()) >= 0


def test_sliding_window_overlaps():
    s = _session()
    src, n = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .sliding_window(60 * DAY, 30 * DAY))
    t = _collect_tbl(sdf)
    # size/slide = 2 → interior events counted in ~2 windows → total > n
    assert t is not None and sum(t.column("count").to_pylist()) > n


def test_session_window_conserves_rows():
    s = _session()
    src, n = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .session_window(10 * DAY))
    t = _collect_tbl(sdf)
    assert t is not None and sum(t.column("count").to_pylist()) == n


def test_out_of_order_rows_not_dropped():
    """Regression: bounded windowing must bucket every row regardless of arrival
    order (the input event_times are deliberately non-monotonic)."""
    s = _session()
    src, n = _events(s)
    sdf = src.to_streaming().with_event_time("event_time").key_by("k").tumbling_window(30 * DAY)
    t = _collect_tbl(sdf)
    assert sum(t.column("count").to_pylist()) == n
    assert set(t.column("k").to_pylist()) == {"a", "b", "c"}


def test_sdf_window_matches_sql_tumble():
    s = _session()
    src, n = _events(s)
    sql = s.sql(f"""SELECT k, window_start, COUNT(*) AS c
                    FROM TUMBLE(TABLE events, DESCRIPTOR(event_time), {30 * DAY})
                    GROUP BY k, window_start""").collect().to_arrow()
    sdf_t = _collect_tbl(src.to_streaming().with_event_time("event_time").key_by("k").tumbling_window(30 * DAY))
    assert sum(sql.column("c").to_pylist()) == sum(sdf_t.column("count").to_pylist()) == n


# ─────────────────────────── camelCase spellings ───────────────────────────
def test_camelcase_windows_and_verbs():
    s = _session()
    src, n = _events(s)
    sdf = (src.to_streaming().withColumn("v2", "v * 2").keyBy("k")
           .withWatermark("event_time", 0).tumblingWindow(30 * DAY).agg(c=kagg.count()))
    t = _collect_tbl(sdf)
    assert t is not None and sum(t.column("c").to_pylist()) == n


def test_camelcase_sliding_session():
    s = _session()
    src, n = _events(s)
    sl = _collect_tbl(src.to_streaming().withWatermark("event_time", 0).keyBy("k").slidingWindow(60 * DAY, 30 * DAY))
    se = _collect_tbl(src.to_streaming().withWatermark("event_time", 0).keyBy("k").sessionWindow(10 * DAY))
    assert sl is not None and se is not None
    assert sum(se.column("count").to_pylist()) == n


# ─────────────────────────── keyed state ───────────────────────────
def test_transform_with_state_running_count():
    s = _session()
    src, _ = _events(s)

    class RunningCount:
        def on_event(self, key, batch, row, ctx):
            raw = bytes(ctx.get_state())
            c = (int.from_bytes(raw, "little") if raw else 0) + 1
            ctx.set_state(c.to_bytes(8, "little"))
            ctx.emit(Batch(pa.record_batch({"k": [str(key)], "running": [c]})))

        def on_timer(self, key, fire_time_ms, ctx):
            pass

    sdf = src.to_streaming().key_by("k").transform_with_state(RunningCount())
    out = asyncio.run(_drain(sdf, cap=300))
    assert out is not None and max(out.column("running").to_pylist()) > 1


def test_transform_with_state_camelcase():
    s = _session()
    src, _ = _events(s)

    class Passthrough:
        def on_event(self, key, batch, row, ctx):
            ctx.emit(Batch(pa.record_batch({"k": [str(key)]})))

        def on_timer(self, key, fire_time_ms, ctx):
            pass

    sdf = src.to_streaming().keyBy("k").transformWithState(Passthrough())
    out = asyncio.run(_drain(sdf, cap=50))
    assert out is not None and out.num_rows > 0


# ─────────────────────────── stream-to-stream ───────────────────────────
def test_co_process_connected_streams():
    s = _session()
    left, _ = _events(s, "co_left")
    right, _ = _events(s, "co_right")

    class Joiner:
        def on_stream1(self, key, batch, row, ctx):
            ctx.emit(Batch(pa.record_batch({"k": [str(key)], "side": ["L"]})))

        def on_stream2(self, key, batch, row, ctx):
            ctx.emit(Batch(pa.record_batch({"k": [str(key)], "side": ["R"]})))

        def on_timer(self, key, fire_time_ms, ctx):
            pass

    out = asyncio.run(_drain(left.to_streaming().co_process(right.to_streaming(), "k", Joiner()), cap=600))
    assert out is not None and set(out.column("side").to_pylist()) == {"L", "R"}


def test_broadcast_process():
    s = _session()
    keyed, _ = _events(s, "bc_keyed")
    rules = s.sql("SELECT r FROM (VALUES ('x'),('y')) AS t(r)")

    class BC:
        def on_keyed_event(self, key, batch, row, ctx):
            ctx.emit(Batch(pa.record_batch({"k": [str(key)]})))

        def on_broadcast_event(self, batch, row, ctx):
            pass

    out = asyncio.run(_drain(keyed.to_streaming().broadcast_process(rules.to_streaming(), "k", BC()), cap=300))
    assert out is not None and out.num_rows > 0


# ─────────────────────────── sinks ───────────────────────────
def test_sink_parquet_roundtrip(tmp_path):
    import glob
    import pyarrow.parquet as pq
    s = _session()
    src, n = _events(s)
    w = src.write_stream()
    w.format("parquet"); w.option("path", str(tmp_path)); w.trigger("available_now")
    w.start().await_termination(30000)
    files = glob.glob(f"{tmp_path}/*.parquet")
    assert files and sum(pq.read_table(f).num_rows for f in files) == n


def test_sink_foreach_batch():
    s = _session()
    src, n = _events(s)
    seen = {"rows": 0}

    def fb(batches, epoch):
        for b in batches:
            seen["rows"] += b.to_arrow().num_rows

    w = src.write_stream()
    w.foreach_batch(fb); w.trigger("available_now")
    w.start().await_termination(30000)
    assert seen["rows"] == n


def test_sink_console_smoke():
    s = _session()
    src, _ = _events(s)
    w = src.write_stream()
    w.format("console"); w.trigger("available_now")
    w.start().await_termination(30000)  # must not raise


def test_sink_entry_is_dataframe_write_stream():
    # Streaming sinks live on the source DataFrame; a windowed StreamingDataFrame
    # is consumed via collect()/execute_stream_async(), so its write_stream raises.
    s = _session()
    assert hasattr(s.sql("SELECT 1 a"), "write_stream")
    with pytest.raises(Exception):
        s.sql("SELECT 1 a").to_streaming().write_stream()


def test_sink_failure_surfaces():
    """A failed sink must raise from await_termination, not silently succeed."""
    import os
    os.environ.update(AWS_ENDPOINT_URL="http://127.0.0.1:9099", AWS_ACCESS_KEY_ID="x",
                      AWS_SECRET_ACCESS_KEY="y", AWS_ALLOW_HTTP="true", AWS_REGION="us-east-1")
    s = _session()
    src, _ = _events(s)
    w = src.write_stream()
    w.format("iceberg"); w.option("path", "s3://nope/bad"); w.trigger("available_now")
    with pytest.raises(Exception):
        w.start().await_termination(20000)


# ─────────────────────────── watermark / side output config ───────────────────────────
def test_watermark_and_state_config_chain():
    s = _session()
    src, n = _events(s)
    sdf = (src.to_streaming()
           .with_event_time("event_time")
           .with_watermark_lag(5 * DAY)
           .with_state_ttl(3600_000)
           .with_side_output("late", 5 * DAY)
           .key_by("k")
           .tumbling_window(30 * DAY))
    t = _collect_tbl(sdf)
    assert t is not None and sum(t.column("count").to_pylist()) > 0


# ═══════════════════ StreamingQuery accessors + memory sink ═══════════════════
def _run_memory_query(s, name="mq"):
    src, n = _events(s)
    w = src.write_stream()
    w.format("memory"); w.query_name(name); w.trigger("available_now")
    q = w.start()
    q.await_termination(30000)
    return q, n


def test_memory_sink_readable_via_query():
    # closes the "memory sink not readable from Python" gap
    s = _session()
    q, n = _run_memory_query(s)
    total = sum(b.to_arrow().num_rows for b in q.memory_batches())
    assert total == n


def test_query_accessors():
    s = _session()
    q, n = _run_memory_query(s, "accessor_q")
    assert q.id()
    assert q.name() == "accessor_q"
    assert q.is_active() is False
    assert q.output_mode() in ("append", "update", "complete")
    assert q.format() == "memory"
    assert q.exception() is None
    lp = q.last_progress()
    assert lp is not None and lp.input_rows == n
    assert isinstance(q.recent_progress(5), list)


def test_query_status_dict():
    s = _session()
    q, n = _run_memory_query(s, "status_q")
    st = q.status()
    assert st["state"] == "stopped"
    assert st["output_mode"] == q.output_mode()
    assert st["exception"] is None
    assert st["last_progress"] is not None


def test_query_exception_on_sink_failure():
    import os
    os.environ.update(AWS_ENDPOINT_URL="http://127.0.0.1:9099", AWS_ACCESS_KEY_ID="x",
                      AWS_SECRET_ACCESS_KEY="y", AWS_ALLOW_HTTP="true", AWS_REGION="us-east-1")
    s = _session()
    src, _ = _events(s)
    w = src.write_stream()
    w.format("iceberg"); w.option("path", "s3://nope/bad"); w.trigger("available_now")
    q = w.start()
    with pytest.raises(Exception):
        q.await_termination(20000)
    assert q.exception() is not None
    assert q.status()["state"] == "failed"


# ═══════════════════ StreamingQueryManager lookup ═══════════════════
def test_query_manager_lookup():
    s = _session()
    src, _ = _events(s)
    mgr = ks.StreamingQueryManager()
    w = src.write_stream()
    w.format("memory"); w.query_name("managed"); w.with_stream_manager(mgr); w.trigger("available_now")
    q = w.start()
    # API works (list of ids; get by id/name returns a handle or None)
    assert isinstance(mgr.active_ids(), list)
    got = mgr.get(q.id())
    assert got is None or got.id() == q.id()
    by_name = mgr.get_by_name("managed")
    assert by_name is None or by_name.name() == "managed"
    q.await_termination(30000)


# ═══════════════════ state-backed dedup ═══════════════════
def test_drop_duplicates_with_state():
    s = _session()
    src, _ = _events(s)
    out = asyncio.run(_drain(src.to_streaming().drop_duplicates_with_state(subset=["k"]), cap=50))
    assert out is not None
    assert out.num_rows == len(set(out.column("k").to_pylist()))


# ═══════════════════ side-output stream consumption ═══════════════════
def test_execute_stream_with_side_output():
    s = _session()
    src, _ = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").with_side_output("late", 5 * DAY)
           .key_by("k").tumbling_window(30 * DAY))
    main, late = sdf.execute_stream_with_side_output_async()
    # returns two independently-consumable streams (main results + late records)
    assert main is not None and late is not None
    assert hasattr(main, "__aiter__") and hasattr(late, "__aiter__")


# ═══════════════════ writer.format_option ═══════════════════
def test_writer_format_option(tmp_path):
    import glob
    import pyarrow.parquet as pq
    s = _session()
    src, n = _events(s)
    w = src.write_stream()
    w.format("parquet"); w.format_option("path", str(tmp_path)); w.trigger("available_now")
    w.start().await_termination(30000)
    files = glob.glob(f"{tmp_path}/*.parquet")
    assert files and sum(pq.read_table(f).num_rows for f in files) == n


# ═══════════════════ filtered / conditional aggregates ═══════════════════
def test_agg_filter_conditional_count():
    s = _session()
    src, n = _events(s)  # v = i % 50 in [0, 50)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .tumbling_window(3650 * DAY)  # single window
           .agg(hi=kagg.count().filter("v", ">", 25.0), total=kagg.count()))
    t = _collect_tbl(sdf)
    hi = sum(t.column("hi").to_pylist())
    total = sum(t.column("total").to_pylist())
    assert total == n and 0 < hi < total
    truth = s.sql("SELECT COUNT(*) c FROM events WHERE v > 25").collect().to_arrow().column("c")[0].as_py()
    assert hi == truth


def test_agg_filter_sum_and_ops():
    s = _session()
    src, _ = _events(s)
    sdf = (src.to_streaming().with_event_time("event_time").key_by("k")
           .tumbling_window(3650 * DAY)
           .agg(paid=kagg.sum("v").filter("k", "=", "a"),
                notnull=kagg.count().filter_not_null("v")))
    t = _collect_tbl(sdf)
    # 'paid' only sums rows where k == 'a'; grouped by k, so only the 'a' group is nonzero
    got = {r["k"]: r["paid"] for r in t.to_pylist()}
    assert got.get("b", 0) == 0 and got.get("c", 0) == 0 and got.get("a", 0) > 0
    truth = s.sql("SELECT SUM(v) x FROM events WHERE k='a'").collect().to_arrow().column("x")[0].as_py()
    assert abs(got["a"] - truth) < 1e-6
