"""R13 deployment-mode factory smoke tests (no maturin required for import check)."""

import os


def test_embedded_mode():
    import krishiv as ks

    session = ks.Session.embedded()
    assert session.mode == "embedded"


def test_local_mode():
    import krishiv as ks

    session = ks.Session.local()
    assert session.mode == "local"


def test_connect_mode():
    import krishiv as ks

    session = ks.Session.connect("http://localhost:50051")
    assert session.mode == "distributed"


def test_from_env_without_coordinator():
    import krishiv as ks

    os.environ.pop("KRISHIV_COORDINATOR", None)
    session = ks.Session.from_env()
    assert session.mode in ("embedded", "local")


def test_from_env_with_coordinator(monkeypatch):
    import krishiv as ks

    monkeypatch.setenv("KRISHIV_COORDINATOR", "http://coordinator:50051")
    session = ks.Session.from_env()
    assert session.mode == "distributed"
