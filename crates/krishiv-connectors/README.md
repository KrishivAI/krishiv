# krishiv-connectors

Source and sink connectors for external data systems.

## Overview

`krishiv-connectors` provides a unified connector framework with 20+
integrations:

| Category | Connectors |
|----------|------------|
| Lakehouse | Parquet, Iceberg, Delta Lake, Hudi |
| Streaming | Kafka, Kinesis, Pulsar |
| Databases | PostgreSQL (pgvector), Cassandra, Scylla, HBase |
| Search | Elasticsearch, Qdrant |
| Object Store | Local FS, S3 |
| Format | Avro, Vortex |

## Features

Enable connectors via Cargo features:

```toml
[dependencies]
krishiv-connectors = { version = "0.2", features = ["kafka", "iceberg", "s3"] }
```

### Presets

| Preset | Includes |
|--------|----------|
| `local` | parquet, s3, kafka, two-phase |
| `full` | local + avro, iceberg, schema-registry, state |
| `extended` | full + delta, hudi, vector-sinks, qdrant, pgvector |

## License

Apache-2.0
