<p align="center">
  <img src="docs/assets/krishiv-banner.svg" alt="Krishiv — Rust-native batch SQL, streaming, and lakehouse compute" width="100%">
</p>

<p align="center">
  <a href="https://crates.io/crates/krishiv"><img src="https://img.shields.io/crates/v/krishiv.svg" alt="crates.io"></a>
  <a href="https://pypi.org/project/krishiv/"><img src="https://img.shields.io/pypi/v/krishiv.svg" alt="PyPI"></a>
  <a href="https://github.com/KrishivAI/krishiv/pkgs/container/krishiv"><img src="https://img.shields.io/badge/docker-ghcr.io-blue" alt="Docker"></a>
  <a href="https://github.com/KrishivAI/krishiv/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License"></a>
</p>

**Krishiv** is a Rust-native hybrid compute engine that unifies **batch SQL**, **streaming pipelines**, and **incremental view maintenance** under one Apache Arrow / DataFusion runtime. The same engine runs embedded in your process, as a single-node daemon, or as a distributed cluster.

---

> **⚠️ Pre-release — not for production use**
> Krishiv is under active development and has not yet reached its first stable release. APIs, storage formats, and wire protocols may change between versions. We recommend using Krishiv for evaluation, prototyping, and development purposes only. Please wait for the first stable release before deploying to production.

---

## Install

### Docker (recommended for getting started)

```bash
docker pull ghcr.io/krishivai/krishiv:latest
docker run --rm -it ghcr.io/krishivai/krishiv:latest sql --query "SELECT 42 AS answer"
```

Or run a single-node daemon with Flight SQL on `:50051`:

```bash
docker run -d --name krishiv -p 50051:50051 ghcr.io/krishivai/krishiv:latest local start
```

### Rust (crates.io)

```toml
[dependencies]
krishiv = "0.1"
```

For library use, add the specific crates you need:

```toml
[dependencies]
krishiv-api     = "0.1"   # Session, DataFrame, IncrementalFlow
krishiv-delta   = "0.1"   # DeltaBatch, IVM operators
krishiv-connectors = { version = "0.1", features = ["iceberg"] }
```

### Python (PyPI)

```bash
pip install krishiv
```

With optional extras:

```bash
pip install "krishiv[arrow]"       # PyArrow + Pandas
pip install "krishiv[iceberg]"     # Iceberg lakehouse support
pip install "krishiv[all]"         # everything
```

---

## Quick Start

### Batch SQL

**Rust**

```rust
use krishiv_api::Session;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let session = Session::new();
    session.register_record_batch("orders", orders_batch)?;

    let result = session
        .sql("SELECT status, COUNT(*) AS n FROM orders GROUP BY status")
        .await?;
    println!("{result:?}");
    Ok(())
}
```

**Python**

```python
import pyarrow as pa
import krishiv

session = krishiv.Session()
session.register_table("orders", pa.table({"status": ["a", "b", "a"], "amount": [10.0, 25.0, 5.0]}))

result = session.sql("SELECT status, COUNT(*) AS n FROM orders GROUP BY status")
print(result.to_pandas())
```

**CLI**

```bash
krishiv sql --query "SELECT 1 AS value"
krishiv explain --query "SELECT 1 AS value"
```

### Streaming

```python
import krishiv

stream = krishiv.StreamSession()
stream.register_window("orders_1m", "orders", tumbling="60s")
stream.register_view("totals", "SELECT window_start, SUM(amount) AS total FROM orders_1m GROUP BY window_start")

for batch in stream.start():
    print(f"window: {batch.num_rows()} rows")
```

### Incremental View Maintenance (IVM)

```python
import pyarrow as pa
import krishiv

flow = krishiv.IncrementalFlow()
flow.register_view(
    "order_counts",
    "SELECT status, COUNT(*) AS n FROM orders GROUP BY status",
    pa.schema([pa.field("status", pa.utf8()), pa.field("n", pa.int64())]),
)

# Tick 1 — new data arrives
flow.feed_source("orders", krishiv.DeltaBatch.from_inserts(orders_batch))
flow.step()

# Get the incremental delta
delta = flow.watch_view("order_counts")
print(delta.filter_positive().to_pandas())   # new rows
print(delta.filter_negative().to_pandas())   # retracted rows
```

---

## Deployment Modes

| Mode | When to use | Start |
|---|---|---|
| **Docker** | Quick eval, CI, sandbox | `docker run ghcr.io/krishivai/krishiv:latest local start` |
| **Embedded** | Library in your Rust/Python process | `Session::new()` |
| **Single-node** | Local daemon with Flight SQL | `krishiv local start` |
| **Distributed** | Coordinator + executor cluster | `krishiv clusterd` |
| **Kubernetes** | CRD-driven production deployment | `kubectl apply -k deploy/k8s/operator` |

---

## What's Inside

- **Apache Arrow** columnar memory — zero-copy between operators
- **DataFusion** SQL engine — full `SELECT`, `JOIN`, `GROUP BY`, window functions
- **Iceberg-first lakehouse** — catalog integration, Parquet read/write, snapshot isolation
- **Exactly-once semantics** — for certified source/sink/checkpoint combinations
- **Pluggable connectors** — Kafka, S3, Parquet, Iceberg (Delta and Hudi experimental)
- **Durable state** — RocksDB-backed keyed state with TTL and checkpoint/restore

---

## Crate Map

| Crate | Purpose |
|---|---|
| `krishiv` | CLI binary (`sql`, `explain`, `jobs`, `local start`) |
| `krishiv-api` | `Session`, `DataFrame`, `IncrementalFlow` |
| `krishiv-delta` | `DeltaBatch`, IVM operators, `IntegrateOp` |
| `krishiv-sql` | DataFusion SQL integration, DDL, catalog |
| `krishiv-connectors` | Source/sink SDK, Iceberg, Kafka, Parquet |
| `krishiv-runtime` | Embedded, single-node, distributed routing |
| `krishiv-scheduler` | Coordinator, metadata, task lifecycle |
| `krishiv-executor` | Executor process and task runner |
| `krishiv-dataflow` | Arrow operators, windows, joins, stateful ops |
| `krishiv-state` | RocksDB state, checkpoints, savepoints |
| `krishiv-shuffle` | Data-plane shuffle (memory, disk, object store) |
| `krishiv-python` | PyO3 Python bindings |

---

## Building from Source

```bash
# Check everything compiles
cargo check --workspace

# Run tests
cargo test --workspace --exclude krishiv-python

# Build single-node binary
cargo build --release -p krishiv --features single-node

# Build distributed + Kubernetes binary
cargo build --release -p krishiv --features full
```

### Docker build

```bash
# Fast local image (pre-built binaries)
docker build -f deploy/docker/Dockerfile.fast -t krishiv:local .

# Production image (multi-stage, ~50MB)
docker build -f deploy/docker/Dockerfile.prod -t krishiv:prod .
```

---

## Documentation

- [Architecture](docs/architecture.md) — engine internals and crate boundaries
- [Engine Contracts](docs/contracts/engine-semantics.md) — batch, streaming, delivery guarantees
- [Connector SDK](docs/connector-sdk.md) — building source/sink connectors
- [Roadmap](docs/ROADMAP.md) — compute-engine priorities
- [Compatibility](docs/COMPATIBILITY.md) — API and metadata upgrade policy
- [Contributing](CONTRIBUTING.md) — how to open a change

---

Krishiv is licensed under the [Apache License 2.0](LICENSE).
