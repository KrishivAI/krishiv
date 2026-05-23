"""Error hierarchy smoke tests."""

import pytest

import krishiv as ks


def test_mode_error_on_embedded_stream():
    session = ks.Session.embedded()
    with pytest.raises(ks.ModeError):
        session.stream("events", "ts", 1000)


def test_exception_hierarchy():
    assert issubclass(ks.QueryError, ks.KrishivError)
    assert issubclass(ks.SchemaError, ks.KrishivError)
    assert issubclass(ks.ModeError, ks.KrishivError)
