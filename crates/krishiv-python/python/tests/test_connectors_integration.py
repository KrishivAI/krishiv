"""Connector integration tests (Kafka / Iceberg)."""

import os

import pytest

import krishiv as ks


def _skip_if_feature_missing(error):
    message = str(error)
    if "requires" in message and "feature" in message:
        pytest.skip(message)
    raise error


def test_read_kafka_builds_stream_handle():
    session = ks.Session.local()
    try:
        stream = ks.read_kafka(session, "events", "localhost:9092")
    except ks.ConnectorError as error:
        _skip_if_feature_missing(error)
    assert stream is not None
    windowed = stream.with_watermark("ts", 1000).key_by("_raw").tumbling_window(60)
    assert windowed is not None


def test_read_iceberg_builds_stream_handle():
    session = ks.Session.local()
    try:
        stream = ks.read_iceberg(session, "http://catalog:8181", "db.events")
    except ks.ConnectorError as error:
        _skip_if_feature_missing(error)
    assert stream is not None
    windowed = stream.with_watermark("ts", 1000).key_by("user_id").tumbling_window(30)
    assert windowed is not None


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    reason="set KAFKA_BOOTSTRAP_SERVERS for live Kafka test",
)
def test_read_kafka_live_broker_smoke():
    """Optional live broker test when CI provides Kafka."""
    session = ks.Session.local()
    stream = ks.read_kafka(
        session,
        os.environ.get("KAFKA_TEST_TOPIC", "krishiv-test"),
        os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    )
    assert stream.with_watermark("ts", 5000) is not None


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("ICEBERG_CATALOG_URI"),
    reason="set ICEBERG_CATALOG_URI for live Iceberg catalog test",
)
def test_read_iceberg_live_catalog_smoke():
    session = ks.Session.local()
    table = os.environ.get("ICEBERG_TEST_TABLE", "default.events")
    stream = ks.read_iceberg(session, os.environ["ICEBERG_CATALOG_URI"], table)
    assert stream.tumbling_window(60) is not None
