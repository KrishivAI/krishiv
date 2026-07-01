"""R13 deployment-mode factory smoke tests."""

import os

import pytest

import krishiv as ks


def test_embedded_mode():
    session = ks.Session.embedded()
    assert session.mode == "embedded"


def test_local_mode():
    session = ks.Session.local()
    assert session.mode == "embedded"
    assert session.is_embedded()
    assert not session.is_single_node()
    assert not session.is_distributed()


def test_connect_mode():
    session = ks.Session.connect("http://localhost:50051")
    assert session.mode == "distributed"
    assert not session.is_embedded()
    assert not session.is_single_node()
    assert session.is_distributed()


def test_single_node_mode():
    session = ks.Session.single_node("http://localhost:50051")
    assert session.mode == "local"
    assert not session.is_embedded()
    assert session.is_single_node()
    assert not session.is_distributed()


def test_from_env_without_coordinator(monkeypatch):
    monkeypatch.delenv("KRISHIV_COORDINATOR", raising=False)
    monkeypatch.delenv("KRISHIV_COORDINATOR_URL", raising=False)
    monkeypatch.delenv("KRISHIV_MODE", raising=False)
    session = ks.Session.from_env()
    assert session.mode == "embedded"


def test_from_env_with_coordinator(monkeypatch):
    monkeypatch.delenv("KRISHIV_MODE", raising=False)
    monkeypatch.setenv("KRISHIV_COORDINATOR", "http://coordinator:50051")
    session = ks.Session.from_env()
    assert session.mode in ("local", "distributed")
    assert not session.is_embedded()


def test_embedded_stream_is_allowed():
    session = ks.Session.embedded()
    stream = session.stream("SELECT 1 AS ts", "ts", 1000)
    assert stream is not None
