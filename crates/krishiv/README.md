# krishiv

Krishiv — hybrid batch and streaming compute engine.

## Overview

Krishiv is a Rust-native framework for batch SQL, streaming pipelines, and
lakehouse-oriented data work. It provides a single runtime model across
embedded, single-node, and distributed modes.

## Features

| Feature | Description |
|---------|-------------|
| `embedded` | In-process library (default) |
| `single-node` | Flight SQL + local shuffle + SQLite metadata |
| `distributed` | etcd metadata backend |
| `k8s` | Kubernetes operator + CRD support |
| `kafka` | Kafka source/sink connectors |
| `iceberg` | Apache Iceberg catalog integration |
| `delta` | Delta Lake table support |

## Quick Start

```rust
use krishiv::Session;

#[tokio::main]
async fn main() {
    let session = Session::new();
    let df = session.sql("SELECT 42 as answer").await.unwrap();
    let batches = df.collect().await.unwrap();
}
```

## License

Apache-2.0
