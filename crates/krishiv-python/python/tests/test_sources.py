"""Source helpers."""

import pytest

import krishiv as ks


def test_read_kafka_embedded_without_feature_raises():
    session = ks.Session.embedded()
    with pytest.raises(ks.ConnectorError):
        ks.read_kafka(session, "topic", "localhost:9092")


def test_read_kafka_without_feature_raises():
    session = ks.Session.local()
    with pytest.raises(ks.ConnectorError):
        ks.read_kafka(session, "topic", "localhost:9092")


def test_read_iceberg_without_feature_raises():
    session = ks.Session.local()
    with pytest.raises(ks.ConnectorError):
        ks.read_iceberg(session, "http://catalog", "ns.table")
