"""Coverage for the unified StreamingDataFrame API.

The DataStream `Stream`/`KeyedStream`/`WindowedStream` classes were retired; all
streaming goes through `StreamingDataFrame`, and its one write terminal is
`write()` -> `StreamWriter` -> `start()` -> `StreamingJob`. These tests are
self-contained (in-memory / SQL sources, no Kafka broker) so they run in CI, and
they cover the transformation surface in both the snake_case and Spark camelCase
spellings.
"""
import asyncio
import time

import pyarrow as pa
import pytest

import krishiv as ks
from krishiv import agg as kagg
from krishiv.krishiv import Batch, StreamWriter

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


def test_keyed_state_and_process_functions_are_top_level_exported():
    # The keyed-state family + Flink-style process functions used by
    # transform_with_state / co_process / broadcast_process must be reachable as
    # top-level `krishiv.X` (not only via the compiled `krishiv.krishiv` submodule)
    # and honored by `from krishiv import *`.
    from krishiv import krishiv as _sub
    for n in ("ValueState", "ListState", "MapState", "AggregatingState",
              "ProcessContext", "BroadcastContext",
              "apply_process_function", "apply_async_io"):
        assert hasattr(ks, n), f"{n} should be top-level exported"
        assert n in ks.__all__, f"{n} missing from __all__"
        assert getattr(ks, n) is getattr(_sub, n), f"{n} must be the submodule object"


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


# ═══════════════ the write terminal: sdf.write() -> StreamWriter -> job ═══════════════
# There is ONE streaming write terminal: `StreamingDataFrame.write()` returns a
# `StreamWriter`, and `start(session, name)` returns the unified `StreamingJob`
# (id / push / drain / flush / stop). The old `df.write_stream()` builder and its
# `StreamingQuery` / `StreamingQueryManager` handles no longer exist in the
# extension, so nothing here reaches for them.
def _windowed(src):
    """The write terminal needs a windowed pipeline; this is the shape the whole
    section writes from."""
    return (src.to_streaming()
            .with_event_time("event_time")
            .key_by("k")
            .tumbling_window(30 * DAY))


def _await_rows(read, expected, timeout=30):
    """Poll `read()` until it reports `expected` rows. `start()` returns as soon as
    the job is registered — delivery to an engine sink happens on the job's own
    loop — and the job handle exposes no completion hook, so the sink itself is
    the only thing to wait on."""
    deadline = time.time() + timeout
    got = 0
    while time.time() < deadline:
        try:
            got = read()
        except Exception:  # a file caught mid-write is not yet an answer
            got = 0
        if got >= expected:
            return got
        time.sleep(0.2)
    return got


def test_sink_parquet_roundtrip(tmp_path):
    """A parquet sink receives the windowed output: every input row is counted
    into exactly one window, so the counts sum back to the input row count."""
    import glob
    import pyarrow.parquet as pq
    s = _session()
    src, n = _events(s)
    w = _windowed(src).write()
    w.format("parquet"); w.option("path", str(tmp_path)); w.trigger("available_now")
    job = w.start(s, "parquet_job")

    def counted():
        files = glob.glob(f"{tmp_path}/*.parquet")
        return sum(sum(pq.read_table(f).column("count").to_pylist()) for f in files)

    assert _await_rows(counted, n) == n
    files = glob.glob(f"{tmp_path}/*.parquet")
    assert set(pq.read_table(files[0]).schema.names) == {
        "k", "window_start_ms", "window_end_ms", "count"
    }
    job.stop()


def test_sink_console_smoke():
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.format("console"); w.trigger("available_now")
    job = w.start(s, "console_job")  # must not raise
    assert job.id
    job.stop()


def test_write_terminal_lives_on_the_streaming_dataframe():
    """`write()` is a StreamingDataFrame method, and it needs a window: a plain
    source stream is refused BY NAME rather than started and left silent."""
    s = _session()
    src, _ = _events(s)
    assert not hasattr(s.sql("SELECT 1 a"), "write_stream")
    assert isinstance(_windowed(src).write(), StreamWriter)
    with pytest.raises(Exception, match="needs a windowed pipeline"):
        src.to_streaming().write().start(s, "unwindowed")


def test_write_stream_is_the_pyspark_spelling_of_the_same_terminal():
    """`df.writeStream` is PySpark's name for `df.to_streaming().write()`."""
    s = _session()
    assert isinstance(s.sql("SELECT 1 a").writeStream, StreamWriter)


def test_writer_refuses_an_unknown_output_mode():
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.output_mode("nonsense")
    with pytest.raises(Exception, match="unknown output mode"):
        w.start(s, "bad_mode")


def test_writer_refuses_an_unknown_trigger():
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.trigger("nonsense")
    with pytest.raises(Exception, match="unknown trigger"):
        w.start(s, "bad_trigger")


def test_writer_start_is_single_use():
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.trigger("continuous")
    job = w.start(s, "once_only")
    with pytest.raises(Exception, match="already been called"):
        w.start(s, "again")
    job.stop()


# ═══════════════════ StreamingJob: push / drain / flush / stop ═══════════════════
def test_sinkless_job_is_driven_through_the_handle():
    """With no sink configured the output stays drainable through the job: push
    input, drain closed windows, flush the rest. Every input row lands in exactly
    one window, so drain + flush account for all of them."""
    s = _session()
    src, n = _events(s)
    tbl = pa.Table.from_pylist([
        {"k": ["a", "b", "c"][i % 3], "v": float(i % 50), "event_time": ((i * 37) % 300) * 10 * DAY}
        for i in range(300)
    ])
    w = _windowed(src).write()
    w.trigger("continuous")
    job = w.start(s, "drain_job")
    job.push([Batch(b) for b in tbl.to_batches()])

    drained = []
    deadline = time.time() + 30
    while time.time() < deadline:
        drained += job.drain()
        if sum(sum(b.to_arrow().column("count").to_pylist()) for b in drained):
            break
        time.sleep(0.2)
    drained += job.flush()
    assert sum(sum(b.to_arrow().column("count").to_pylist()) for b in drained) == n
    job.stop()


def test_engine_sink_job_refuses_drain_and_flush_by_name():
    """A job delivering to an engine sink has no drainable buffer, and bounded
    completion is the trigger's job — both are refused by name, not half-served."""
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.format("console"); w.trigger("available_now")
    job = w.start(s, "refusals")
    with pytest.raises(Exception, match="not a drainable buffer"):
        job.drain()
    with pytest.raises(Exception, match="available_now trigger, not flush"):
        job.flush()
    job.stop()


def test_job_id_and_repr():
    s = _session()
    src, _ = _events(s)
    w = _windowed(src).write()
    w.trigger("continuous")
    job = w.start(s, "named_job")
    assert job.id == "named_job"          # `id` is a property, not a method
    assert "named_job" in repr(job)
    job.stop()


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
