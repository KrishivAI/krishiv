"""Transformation chain: watermark, key_by, window."""

import pytest

import krishiv as ks


def test_with_watermark_and_key_by():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS user_id, 2 AS value", "ts", 1000)
    keyed = stream.with_watermark("ts", 500).key_by("user_id")
    assert keyed is not None


def test_window_requires_watermark():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS user_id", "", 1000)
    keyed = stream.key_by("user_id")
    with pytest.raises(ks.SchemaError):
        keyed.window(ks.windows.tumbling(60_000))


def test_tumbling_window_chain():
    session = ks.Session.local()
    stream = session.stream("SELECT 1 AS user_id, 10 AS value", "ts", 1000)
    windowed = (
        stream.with_watermark("ts", 500)
        .key_by("user_id")
        .tumbling_window(60)
    )
    batches = windowed.collect()
    assert isinstance(batches, list)
