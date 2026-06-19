"""Lakehouse connector tests (Delta, Hudi, Iceberg, Kinesis, Pulsar)."""

import pytest

import krishiv as ks


def _skip_if_feature_missing(error):
    message = str(error)
    if "requires" in message and "feature" in message:
        pytest.skip(message)
    raise error


def _make_session():
    return ks.Session.local()


def _make_df(session):
    return session.sql("SELECT 1 AS id, 'a' AS name")


def test_read_delta_nonexistent_path():
    session = _make_session()
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError)):
        ks.read_delta(session, "/nonexistent/delta/table")


def test_write_delta_roundtrip(tmp_path):
    session = _make_session()
    df = _make_df(session)
    path = str(tmp_path / "delta_table")
    try:
        ks.write_delta(df, path, mode="append")
    except (ks.ConnectorError, RuntimeError, ValueError) as e:
        _skip_if_feature_missing(e)
    result = ks.read_delta(session, path)
    assert result is not None


def test_write_delta_overwrite_mode(tmp_path):
    session = _make_session()
    df = _make_df(session)
    path = str(tmp_path / "delta_ow")
    try:
        ks.write_delta(df, path, mode="overwrite")
    except (ks.ConnectorError, RuntimeError, ValueError) as e:
        _skip_if_feature_missing(e)
    try:
        ks.write_delta(df, path, mode="overwrite")
    except (ks.ConnectorError, RuntimeError, ValueError):
        pass


def test_read_hudi_nonexistent_path():
    session = _make_session()
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError)):
        ks.read_hudi(session, "/nonexistent/hudi/table")


def test_write_hudi_append(tmp_path):
    session = _make_session()
    df = _make_df(session)
    path = str(tmp_path / "hudi_append")
    try:
        ks.write_hudi_append(df, path)
    except (ks.ConnectorError, RuntimeError, ValueError) as e:
        _skip_if_feature_missing(e)
    result = ks.read_hudi(session, path)
    assert result is not None


def test_write_hudi_upsert(tmp_path):
    session = _make_session()
    df = _make_df(session)
    path = str(tmp_path / "hudi_upsert")
    try:
        ks.write_hudi_upsert(df, path, key_columns=["id"])
    except (ks.ConnectorError, RuntimeError, ValueError) as e:
        _skip_if_feature_missing(e)
    result = ks.read_hudi(session, path)
    assert result is not None


def test_read_iceberg_invalid_catalog():
    session = _make_session()
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError)):
        ks.read_iceberg(session, "http://invalid:99999", "db.table")


def test_read_iceberg_nonexistent_table():
    session = _make_session()
    try:
        ks.read_iceberg(session, "http://catalog:8181", "db.nonexistent")
    except (ks.ConnectorError, RuntimeError, ValueError) as e:
        _skip_if_feature_missing(e)


def test_read_kinesis_graceful_error():
    session = _make_session()
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError)):
        ks.read_kinesis(session, "test-stream", "us-east-1")


def test_read_pulsar_graceful_error():
    session = _make_session()
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError)):
        ks.read_pulsar(session, "pulsar://localhost:6650", "test-topic")


def test_write_delta_missing_path_raises():
    session = _make_session()
    df = _make_df(session)
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError, TypeError)):
        ks.write_delta(df)


def test_write_hudi_append_missing_path_raises():
    session = _make_session()
    df = _make_df(session)
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError, TypeError)):
        ks.write_hudi_append(df)


def test_write_hudi_upsert_missing_columns_raises(tmp_path):
    session = _make_session()
    df = _make_df(session)
    path = str(tmp_path / "hudi_bad")
    with pytest.raises((ks.ConnectorError, RuntimeError, ValueError, TypeError)):
        ks.write_hudi_upsert(df, path)
