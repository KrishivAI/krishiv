"""Error hierarchy smoke tests."""

import pytest

import krishiv as ks


def test_embedded_stream_does_not_raise_mode_error():
    session = ks.Session.embedded()
    stream = session.stream("SELECT 1 AS ts", "ts", 1000)
    assert stream is not None


def test_exception_hierarchy():
    assert issubclass(ks.QueryError, ks.KrishivError)
    assert issubclass(ks.SchemaError, ks.KrishivError)
    assert issubclass(ks.ModeError, ks.KrishivError)
