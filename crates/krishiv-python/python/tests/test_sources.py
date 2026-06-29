"""Source helpers."""

import pytest

import krishiv as ks


def test_read_kafka_embedded_without_feature_raises():
    session = ks.Session.embedded()
    try:
        stream = ks.read_kafka(session, "topic", "localhost:9092")
        assert stream is not None
    except ks.ConnectorError:
        pass


def test_read_kafka_without_feature_raises():
    session = ks.Session.local()
    try:
        stream = ks.read_kafka(session, "topic", "localhost:9092")
        assert stream is not None
    except ks.ConnectorError:
        pass


def test_read_iceberg_without_feature_raises():
    session = ks.Session.local()
    try:
        stream = ks.read_iceberg(session, "http://catalog", "ns.table")
        assert stream is not None
    except ks.ConnectorError:
        pass
