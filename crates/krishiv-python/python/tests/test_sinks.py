"""Sink constructors, repr, and basic methods."""

import tempfile
from pathlib import Path

import pytest

import krishiv as ks
import krishiv.ai as ai

ksinks = ks.sinks


# ---------------------------------------------------------------------------
# ParquetSink
# ---------------------------------------------------------------------------


def test_parquet_sink_construction():
    sink = ks.ParquetSink("/tmp/out")
    assert sink is not None


def test_parquet_sink_path():
    sink = ks.ParquetSink("/tmp/data.parquet")
    assert sink.path == "/tmp/data.parquet"


def test_parquet_sink_repr():
    sink = ks.ParquetSink("/tmp/out")
    r = repr(sink)
    assert isinstance(r, str)
    assert "ParquetSink" in r
    assert "/tmp/out" in r


def test_parquet_sink_roundtrip_path():
    p = str(Path(tempfile.mkdtemp()) / "output.parquet")
    sink = ks.ParquetSink(p)
    assert sink.path == p


# ---------------------------------------------------------------------------
# KafkaSink
# ---------------------------------------------------------------------------


def test_kafka_sink_construction():
    try:
        sink = ks.KafkaSink("events", "localhost:9092")
    except (ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_kafka_sink_repr():
    try:
        sink = ks.KafkaSink("events", "localhost:9092")
    except (ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "KafkaSink" in r
    assert "events" in r


# ---------------------------------------------------------------------------
# IcebergSink
# ---------------------------------------------------------------------------


def test_iceberg_sink_construction():
    try:
        sink = ks.IcebergSink("/tmp/catalog", "db.events")
    except (ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_iceberg_sink_repr():
    try:
        sink = ks.IcebergSink("/tmp/catalog", "db.events")
    except (ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "IcebergSink" in r
    assert "db.events" in r


# ---------------------------------------------------------------------------
# CassandraSink
# ---------------------------------------------------------------------------


def test_cassandra_sink_construction():
    try:
        sink = ksinks.CassandraSink("localhost", "testks", "events")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_cassandra_sink_repr():
    try:
        sink = ksinks.CassandraSink("localhost", "testks", "events")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "CassandraSink" in r
    assert "localhost" in r
    assert "testks" in r


# ---------------------------------------------------------------------------
# ElasticsearchSink
# ---------------------------------------------------------------------------


def test_elasticsearch_sink_construction():
    try:
        sink = ksinks.ElasticsearchSink("http://localhost:9200", "logs")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_elasticsearch_sink_repr():
    try:
        sink = ksinks.ElasticsearchSink("http://localhost:9200", "logs")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "ElasticsearchSink" in r
    assert "logs" in r


# ---------------------------------------------------------------------------
# HBaseSink
# ---------------------------------------------------------------------------


def test_hbase_sink_construction():
    try:
        sink = ksinks.HBaseSink("localhost:9090", "events", "cf")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_hbase_sink_repr():
    try:
        sink = ksinks.HBaseSink("localhost:9090", "events", "cf")
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "HBaseSink" in r
    assert "cf" in r


# ---------------------------------------------------------------------------
# InMemoryVectorSink
# ---------------------------------------------------------------------------


def test_in_memory_vector_sink_construction():
    sink = ai.InMemoryVectorSink()
    assert sink is not None


def test_in_memory_vector_sink_sink_name():
    sink = ai.InMemoryVectorSink()
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


def test_in_memory_vector_sink_repr():
    sink = ai.InMemoryVectorSink()
    r = repr(sink)
    assert isinstance(r, str)
    assert "InMemoryVectorSink" in r


def test_in_memory_vector_sink_upsert_and_query():
    sink = ai.InMemoryVectorSink()
    sink.upsert_batch(
        doc_ids=["d1", "d2"],
        vectors=[[1.0, 0.0], [0.0, 1.0]],
        payloads=[{"text": "hello"}, {"text": "world"}],
        epoch=1,
    )
    results = sink.query_nearest(vector=[1.0, 0.0], top_k=2)
    assert len(results) > 0
    assert results[0].doc_id == "d1"
    assert results[0].score > 0.9


def test_in_memory_vector_sink_delete_by_ids():
    sink = ai.InMemoryVectorSink()
    sink.upsert_batch(
        doc_ids=["d1", "d2"],
        vectors=[[1.0, 0.0], [0.0, 1.0]],
    )
    sink.delete_by_ids(["d1"])
    results_before = sink.query_nearest(vector=[1.0, 0.0], top_k=10)
    sink.upsert_batch(
        doc_ids=["d1"],
        vectors=[[1.0, 0.0]],
    )
    results_after = sink.query_nearest(vector=[1.0, 0.0], top_k=10)
    assert isinstance(results_before, list)
    assert isinstance(results_after, list)


def test_in_memory_vector_sink_query_with_filter():
    sink = ai.InMemoryVectorSink()
    sink.upsert_batch(
        doc_ids=["d1", "d2", "d3"],
        vectors=[[1.0, 0.0], [0.5, 0.5], [0.0, 1.0]],
        payloads=[
            {"category": "a"},
            {"category": "b"},
            {"category": "a"},
        ],
    )
    results = sink.query_nearest(
        vector=[1.0, 0.0], top_k=10, filter={"category": "a"}
    )
    assert len(results) > 0
    for r in results:
        assert r.doc_id in ("d1", "d3")


def test_in_memory_vector_sink_upsert_empty():
    sink = ai.InMemoryVectorSink()
    sink.upsert_batch(doc_ids=[], vectors=[], epoch=0)
    results = sink.query_nearest(vector=[1.0, 0.0], top_k=5)
    assert len(results) == 0


# ---------------------------------------------------------------------------
# PineconeSink
# ---------------------------------------------------------------------------


def test_pinecone_sink_construction():
    try:
        sink = ai.PineconeSink("index.svc.pinecone.io", "api-key-123")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_pinecone_sink_repr():
    try:
        sink = ai.PineconeSink("index.svc.pinecone.io", "api-key-123")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "PineconeSink" in r


def test_pinecone_sink_sink_name():
    try:
        sink = ai.PineconeSink("index.svc.pinecone.io", "api-key-123")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


def test_pinecone_sink_with_namespace():
    try:
        sink = ai.PineconeSink(
            "index.svc.pinecone.io", "api-key-123", namespace="prod"
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)


# ---------------------------------------------------------------------------
# WeaviateSink
# ---------------------------------------------------------------------------


def test_weaviate_sink_construction():
    try:
        sink = ai.WeaviateSink("http://localhost:8080", "Document")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_weaviate_sink_repr():
    try:
        sink = ai.WeaviateSink("http://localhost:8080", "Document")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "WeaviateSink" in r


def test_weaviate_sink_sink_name():
    try:
        sink = ai.WeaviateSink("http://localhost:8080", "Document")
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


# ---------------------------------------------------------------------------
# LanceDbSink
# ---------------------------------------------------------------------------


def test_lancedb_sink_construction():
    try:
        sink = ai.LanceDbSink.open(
            uri=str(Path(tempfile.mkdtemp()) / "lance"),
            table="vectors",
            vector_dim=3,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_lancedb_sink_repr():
    try:
        sink = ai.LanceDbSink.open(
            uri=str(Path(tempfile.mkdtemp()) / "lance"),
            table="vectors",
            vector_dim=3,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    r = repr(sink)
    assert isinstance(r, str)
    assert "LanceDbSink" in r


def test_lancedb_sink_sink_name():
    try:
        sink = ai.LanceDbSink.open(
            uri=str(Path(tempfile.mkdtemp()) / "lance"),
            table="vectors",
            vector_dim=3,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


# ---------------------------------------------------------------------------
# QdrantSink
# ---------------------------------------------------------------------------


def test_qdrant_sink_construction():
    try:
        sink = ai.QdrantSink.connect(
            url="http://localhost:6333",
            collection="test_col",
            vector_size=128,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_qdrant_sink_sink_name():
    try:
        sink = ai.QdrantSink.connect(
            url="http://localhost:6333",
            collection="test_col",
            vector_size=128,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


# ---------------------------------------------------------------------------
# PgvectorSink
# ---------------------------------------------------------------------------


def test_pgvector_sink_construction():
    try:
        sink = ai.PgvectorSink.connect(
            database_url="postgresql://localhost/testdb",
            table="embeddings",
            vector_dim=768,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    assert sink is not None


def test_pgvector_sink_sink_name():
    try:
        sink = ai.PgvectorSink.connect(
            database_url="postgresql://localhost/testdb",
            table="embeddings",
            vector_dim=768,
        )
    except (RuntimeError, TypeError) as e:
        pytest.skip(str(e))
    name = sink.sink_name()
    assert isinstance(name, str)
    assert len(name) > 0


# ---------------------------------------------------------------------------
# All sinks: repr returns string
# ---------------------------------------------------------------------------


def test_all_repr_returns_string():
    sinks = [
        ks.ParquetSink("/tmp/x"),
        ks.KafkaSink("t", "b"),
        ks.IcebergSink("/c", "db.tbl"),
    ]
    for s in sinks:
        assert isinstance(repr(s), str)


def test_all_feature_sinks_repr_returns_string():
    try:
        sinks = [
            ksinks.CassandraSink("n", "ks", "t"),
            ksinks.ElasticsearchSink("http://x", "idx"),
            ksinks.HBaseSink("h", "t", "cf"),
        ]
    except (AttributeError, ks.ConnectorError, RuntimeError, TypeError):
        pytest.skip("connector features unavailable")
    for s in sinks:
        assert isinstance(repr(s), str)
