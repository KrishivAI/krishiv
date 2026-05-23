"""Streaming transformation chain."""

import krishiv as ks


def test_with_watermark_and_key_by():
    session = ks.Session.local()
    stream = (
        session.stream("events", watermark_column="ts", max_lateness_ms=5000)
        .with_watermark("ts", 5000)
        .key_by("user_id")
    )
    assert "user_id" in repr(stream)


def test_window_requires_watermark():
    session = ks.Session.local()
    stream = session.stream("events", watermark_column="ts", max_lateness_ms=1000)
    keyed = stream.key_by("user_id")
    windowed = keyed.window(ks.windows.tumbling(60_000))
    assert "WindowedStream" in repr(windowed)


def test_agg_chain():
    session = ks.Session.local()
    windowed = (
        session.stream("events", watermark_column="ts", max_lateness_ms=1000)
        .key_by("user_id")
        .window(ks.windows.tumbling(60_000))
    )
    result = windowed.agg(
        events=ks.agg.count(),
        total=ks.agg.sum("amount"),
    )
    assert "Stream" in repr(result)
