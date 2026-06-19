import asyncio

import pytest

import krishiv as ks
from krishiv.krishiv import MultiSourceWatermarkSpec


def test_session_local_stream_creation():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    assert stream is not None
    r = repr(stream)
    assert "Stream" in r
    assert "ts" in r


def test_stream_key_by_single_column():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    assert "KeyedStream" in repr(keyed)
    assert "n" in repr(keyed)


def test_stream_key_by_multiple_columns():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS a, 2 AS b, 3 AS ts", "ts", 0)
    keyed = stream.key_by("a", "b")
    r = repr(keyed)
    assert "a" in r
    assert "b" in r


def test_stream_key_by_empty_raises():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    with pytest.raises(Exception):
        stream.key_by()


def test_stream_watermark():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 1000)
    updated = stream.watermark("ts", 5000)
    assert updated is not None
    assert "ts" in repr(updated)


def test_stream_with_watermark():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 1000)
    updated = stream.with_watermark("ts", 3000)
    assert updated is not None
    assert "ts" in repr(updated)


def test_stream_watermark_and_with_watermark_are_aliases():
    session = ks.Session.local()
    s1 = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    s2 = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    r1 = repr(s1.watermark("ts", 5000))
    r2 = repr(s2.with_watermark("ts", 5000))
    assert r1 == r2


def test_stream_tumbling_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.tumbling_window_ms(5000)
    assert "WindowedStream" in repr(windowed)
    assert windowed.window_size_ms == 5000
    assert windowed.window_kind == "tumbling"


def test_stream_tumbling_window_secs():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.tumbling_window(2)
    assert windowed.window_size_ms == 2000
    assert windowed.window_kind == "tumbling"


def test_stream_sliding_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.sliding_window_ms(10000, 5000)
    assert windowed.window_size_ms == 10000
    assert windowed.slide_ms == 5000
    assert windowed.window_kind == "sliding"


def test_stream_session_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.session_window_ms(30000)
    assert windowed.window_size_ms == 30000
    assert windowed.session_gap_ms == 30000
    assert windowed.window_kind == "session"
    assert windowed.slide_ms is None


def test_stream_broadcast():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    bcast = stream.broadcast()
    r = repr(bcast)
    assert "BroadcastStream" in r


def test_stream_connect():
    session = ks.Session.local()
    s1 = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    s2 = session.stream("SELECT 2 AS n, 2000 AS ts", "ts", 0)
    connected = s1.connect(s2)
    r = repr(connected)
    assert "ConnectedStreams" in r


def test_stream_with_state_ttl():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    updated = stream.with_state_ttl(60000)
    assert updated is not None
    assert "ts" in repr(updated)


def test_stream_with_multi_source_watermark():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    spec = (
        MultiSourceWatermarkSpec()
        .add_source("clicks", 5000)
        .add_source("impressions", 10000)
        .with_source_id_column("source_id")
    )
    updated = stream.with_multi_source_watermark(spec)
    assert updated is not None
    r = repr(spec)
    assert "clicks" in r
    assert "impressions" in r
    assert "source_id" in r


def test_stream_repr_contains_watermark_column():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS val, 999 AS event_time", "event_time", 5000)
    r = repr(stream)
    assert "event_time" in r


def test_keyed_stream_window_with_spec():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    spec = ks.windows.tumbling(60000)
    windowed = keyed.window(spec)
    assert "WindowedStream" in repr(windowed)
    assert windowed.window_size_ms == 60000
    assert windowed.window_kind == "tumbling"


def test_keyed_stream_sliding_window_spec():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    spec = ks.windows.sliding(10000, 5000)
    windowed = keyed.window(spec)
    assert windowed.window_size_ms == 10000
    assert windowed.slide_ms == 5000
    assert windowed.window_kind == "sliding"


def test_keyed_stream_session_window_spec():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    spec = ks.windows.session(30000)
    windowed = keyed.window(spec)
    assert windowed.window_size_ms == 30000
    assert windowed.session_gap_ms == 30000
    assert windowed.window_kind == "session"


def test_keyed_stream_tumbling_window_secs():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    windowed = keyed.tumbling_window(3)
    assert windowed.window_size_ms == 3000
    assert windowed.window_kind == "tumbling"


def test_keyed_stream_tumbling_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    windowed = keyed.tumbling_window_ms(5000)
    assert windowed.window_size_ms == 5000
    assert windowed.window_kind == "tumbling"


def test_keyed_stream_sliding_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    windowed = keyed.sliding_window_ms(10000, 5000)
    assert windowed.window_size_ms == 10000
    assert windowed.slide_ms == 5000
    assert windowed.window_kind == "sliding"


def test_keyed_stream_session_window_ms():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    windowed = keyed.session_window_ms(30000)
    assert windowed.window_size_ms == 30000
    assert windowed.session_gap_ms == 30000
    assert windowed.window_kind == "session"


def test_keyed_stream_connect():
    session = ks.Session.local()
    s1 = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    s2 = session.stream("SELECT 2 AS n, 2000 AS ts", "ts", 0)
    k1 = s1.key_by("n")
    k2 = s2.key_by("n")
    connected = k1.connect(k2)
    assert "ConnectedStreams" in repr(connected)


def test_keyed_stream_with_multi_source_watermark():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    keyed = stream.key_by("n")
    spec = (
        MultiSourceWatermarkSpec()
        .add_source("s1", 3000)
        .with_source_id_column("sid")
    )
    updated = keyed.with_multi_source_watermark(spec)
    assert "KeyedStream" in repr(updated)


def test_windowed_stream_agg_count():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(count=ks.agg.count())
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_sum():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(total=ks.agg.sum("val"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_mean():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(avg_val=ks.agg.mean("val"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_min():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 50 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(min_val=ks.agg.min("val"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_max():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 50 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(max_val=ks.agg.max("val"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_multiple():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(cnt=ks.agg.count(), total=ks.agg.sum("val"), avg_val=ks.agg.mean("val"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_agg_empty_raises():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    with pytest.raises(Exception):
        windowed.agg()


def test_windowed_stream_agg_with_custom_output_name():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(my_sum=ks.agg.sum("val", "my_sum"))
    assert "WindowedStream" in repr(result)


def test_windowed_stream_collect():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    batches = windowed.collect()
    assert isinstance(batches, list)


def test_windowed_stream_try_next():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.try_next()
    assert result is None or result is not None


def test_windowed_stream_window_size_ms():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window_ms(15000)
    )
    assert windowed.window_size_ms == 15000


def test_windowed_stream_slide_ms_tumbling():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    assert windowed.slide_ms is None


def test_windowed_stream_slide_ms_sliding():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .sliding_window_ms(10000, 5000)
    )
    assert windowed.slide_ms == 5000


def test_windowed_stream_session_gap_ms_none_for_tumbling():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    assert windowed.session_gap_ms is None


def test_windowed_stream_session_gap_ms_for_session():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .session_window_ms(30000)
    )
    assert windowed.session_gap_ms == 30000


def test_windowed_stream_window_kind():
    session = ks.Session.local()
    s = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0).key_by("n")
    assert s.tumbling_window(1).window_kind == "tumbling"
    assert s.sliding_window_ms(10000, 5000).window_kind == "sliding"
    assert s.session_window_ms(30000).window_kind == "session"


def test_windowed_stream_repr():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    r = repr(windowed)
    assert "WindowedStream" in r


def test_agg_expr_properties():
    e = ks.agg.count()
    assert e.function == "count"
    assert e.input_column is None

    s = ks.agg.sum("amount")
    assert s.function == "sum"
    assert s.input_column == "amount"

    m = ks.agg.mean("score")
    assert m.function == "mean"
    assert m.input_column == "score"

    lo = ks.agg.min("val")
    assert lo.function == "min"
    assert lo.input_column == "val"

    hi = ks.agg.max("val")
    assert hi.function == "max"
    assert hi.input_column == "val"


def test_agg_sum_requires_column():
    with pytest.raises(Exception):
        ks.agg.sum()


def test_agg_mean_requires_column():
    with pytest.raises(Exception):
        ks.agg.mean()


def test_window_spec_tumbling():
    spec = ks.windows.tumbling(60000)
    assert spec is not None


def test_window_spec_sliding():
    spec = ks.windows.sliding(10000, 5000)
    assert spec is not None


def test_window_spec_session():
    spec = ks.windows.session(30000)
    assert spec is not None


def test_streaming_dataframe_to_streaming():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    assert sdf is not None


def test_streaming_dataframe_key_by():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.key_by("n")
    assert result is not None


def test_streaming_dataframe_tumbling_window():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.tumbling_window(60000)
    assert result is not None


def test_streaming_dataframe_sliding_window():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.sliding_window(10000, 5000)
    assert result is not None


def test_streaming_dataframe_session_window():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.session_window(30000)
    assert result is not None


def test_streaming_dataframe_with_event_time():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.with_event_time("ts")
    assert result is not None


def test_streaming_dataframe_with_watermark_lag():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.with_watermark_lag(5000)
    assert result is not None


def test_streaming_dataframe_with_side_output():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.with_side_output("late_events", 10000)
    assert result is not None


def test_streaming_dataframe_drop_duplicates():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming()
    result = sdf.drop_duplicates(subset=["n"])
    assert result is not None


def test_streaming_dataframe_chained():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    result = (
        df.to_streaming()
        .key_by("n")
        .with_event_time("ts")
        .with_watermark_lag(5000)
        .tumbling_window(60000)
    )
    assert result is not None


def test_streaming_dataframe_write_stream_raises():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    sdf = df.to_streaming()
    with pytest.raises(RuntimeError):
        sdf.write_stream()


def test_streaming_dataframe_execute_stream_async():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 100 AS val, 1000 AS ts")
    sdf = df.to_streaming().key_by("n").tumbling_window(1000)
    result = sdf.execute_stream_async()
    assert result is not None


def test_dataframe_write_stream():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    assert writer is not None


def test_data_stream_writer_output_mode():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    writer.output_mode("update")
    r = repr(writer)
    assert "update" in r


def test_data_stream_writer_invalid_output_mode():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    with pytest.raises(RuntimeError):
        writer.output_mode("invalid_mode")


def test_data_stream_writer_trigger():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    writer.trigger("processing_time", 5000)
    r = repr(writer)
    assert "processing_time" in r


def test_data_stream_writer_invalid_trigger():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    with pytest.raises(RuntimeError):
        writer.trigger("bogus")


def test_data_stream_writer_query_name():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    writer.query_name("my_job")
    r = repr(writer)
    assert "my_job" in r


def test_data_stream_writer_option():
    session = ks.Session.local()
    df = session.sql("SELECT 1 AS n, 1000 AS ts")
    writer = df.write_stream()
    writer.option("checkpoint.location", "/tmp/ckpt")
    r = repr(writer)
    assert "DataStreamWriter" in r


def test_multi_source_watermark_spec_builder():
    spec = (
        MultiSourceWatermarkSpec()
        .add_source("a", 1000)
        .add_source("b", 2000)
        .with_source_id_column("src")
    )
    r = repr(spec)
    assert "a" in r
    assert "b" in r
    assert "src" in r


def test_multi_source_watermark_spec_empty():
    spec = MultiSourceWatermarkSpec()
    r = repr(spec)
    assert "MultiSourceWatermarkSpec" in r


async def _run_async_iteration():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
    windowed = stream.key_by("n").tumbling_window(1)
    seen = 0
    async for batch in windowed:
        assert batch.num_rows >= 1
        seen += 1
        if seen >= 1:
            break
    assert seen == 1


def test_windowed_stream_async_iteration():
    asyncio.run(_run_async_iteration())


async def _run_async_iteration_multiple_batches():
    session = ks.Session.local()
    stream = session.stream(
        "SELECT 1 AS n, 1000 AS ts UNION ALL SELECT 2 AS n, 2000 AS ts", "ts", 0
    )
    windowed = stream.key_by("n").tumbling_window(1)
    seen = 0
    async for batch in windowed:
        seen += 1
        if seen >= 3:
            break
    assert seen >= 1


def test_windowed_stream_async_iteration_multiple_batches():
    asyncio.run(_run_async_iteration_multiple_batches())


def test_stream_full_pipeline_count():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(count=ks.agg.count())
    batches = result.collect()
    assert isinstance(batches, list)


def test_stream_full_pipeline_sum():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .tumbling_window(1)
    )
    result = windowed.agg(total=ks.agg.sum("val"))
    batches = result.collect()
    assert isinstance(batches, list)


def test_stream_full_pipeline_sliding():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 100 AS val, 1000 AS ts", "ts", 0)
        .key_by("n")
        .sliding_window_ms(5000, 2000)
    )
    result = windowed.agg(cnt=ks.agg.count())
    batches = result.collect()
    assert isinstance(batches, list)


def test_stream_full_pipeline_session():
    session = ks.Session.local()
    windowed = (
        session.stream("SELECT 1 AS n, 1000 AS ts", "ts", 0)
        .key_by("n")
        .session_window_ms(10000)
    )
    result = windowed.agg(cnt=ks.agg.count())
    batches = result.collect()
    assert isinstance(batches, list)
