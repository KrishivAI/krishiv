"""R13 deployment-mode factory smoke tests."""

import os

import pytest

import krishiv as ks


def test_embedded_mode():
    session = ks.Session.embedded()
    assert session.mode == "embedded"


def test_local_mode():
    session = ks.Session.local()
    assert session.mode == "local"


def test_connect_mode():
    session = ks.Session.connect("http://localhost:50051")
    assert session.mode == "distributed"


def test_from_env_without_coordinator(monkeypatch):
    monkeypatch.delenv("KRISHIV_COORDINATOR", raising=False)
    monkeypatch.delenv("KRISHIV_COORDINATOR_URL", raising=False)
    monkeypatch.delenv("KRISHIV_MODE", raising=False)
    session = ks.Session.from_env()
    assert session.mode == "local"


def test_from_env_with_coordinator(monkeypatch):
    monkeypatch.setenv("KRISHIV_COORDINATOR", "http://coordinator:50051")
    session = ks.Session.from_env()
    assert session.mode == "distributed"


def test_embedded_stream_raises_mode_error():
    session = ks.Session.embedded()
    with pytest.raises(ks.ModeError):
        session.stream("events", "ts", 1000)
