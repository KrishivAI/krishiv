# krishiv-python

Python bindings for Krishiv via PyO3.

## Overview

`krishiv-python` exposes Krishiv's SQL engine, DataFrame API, and connector
framework to Python. Install via pip:

```bash
pip install krishiv
```

## Features

| Feature | Description |
|---------|-------------|
| `kafka` | Kafka source/sink |
| `iceberg` | Apache Iceberg support |
| `kinesis` | AWS Kinesis source |
| `pulsar` | Apache Pulsar source |
| `cassandra` | Cassandra sink |
| `elasticsearch` | Elasticsearch sink |
| `hbase` | HBase sink |
| `vector-sinks` | Vector database sinks |
| `qdrant` | Qdrant vector sink |
| `pgvector` | pgvector sink |

## Usage

```python
import krishiv as ks

session = ks.Session()
df = session.sql("SELECT 42 as answer")
print(df.collect().pretty())
```

## Building

```bash
cd crates/krishiv-python
maturin develop --release
```

## License

Apache-2.0
