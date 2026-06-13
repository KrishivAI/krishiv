![Krishiv logo](docs/assets/krishiv-logo.svg)

# Krishiv

Krishiv is a Rust-native hybrid compute framework for batch SQL, streaming
pipelines, and lakehouse-oriented data work.

The codebase is organized around one runtime model across embedded, single-node,
and distributed execution. Apache Arrow is the columnar data model, and
DataFusion is the SQL and local execution foundation.

## Documentation

- [`docs/README.md`](docs/README.md) — contributor entry point and crate map.
- [`docs/architecture.md`](docs/architecture.md) — implemented architecture and engine boundary.
- [`docs/contracts/engine-semantics.md`](docs/contracts/engine-semantics.md) — batch, streaming, and delivery guarantees.
- [`ROADMAP.md`](ROADMAP.md) — public compute-engine priorities and platform exclusions.
- [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) — API and durable-artifact upgrade policy.
- [`docs/connector-sdk.md`](docs/connector-sdk.md) — connector implementation and certification.
- [`CONTRIBUTING.md`](CONTRIBUTING.md), [`GOVERNANCE.md`](GOVERNANCE.md), and [`SECURITY.md`](SECURITY.md) — project participation and reporting.

## Quick Start

```bash
cargo check --workspace
cargo run -p krishiv -- sql --query "select 1 as value"
cargo run -p krishiv -- explain --query "select 1 as value"
cargo run -p krishiv -- jobs
```

Run focused tests while iterating:

```bash
cargo test -p krishiv-runtime
cargo test -p krishiv-scheduler --lib
cargo test -p krishiv-executor --lib
```

## Workspace

Primary crates:

- `krishiv` - CLI and user-facing facade.
- `krishiv-api` - Session, DataFrame, Stream API.
- `krishiv-sql` - DataFusion integration and SQL helpers.
- `krishiv-plan` - logical/physical plans and task fragments.
- `krishiv-runtime` - embedded, single-node, and distributed runtime routing.
- `krishiv-scheduler` - coordinator, metadata, leadership, task lifecycle.
- `krishiv-executor` - executor process and task runner.
- `krishiv-dataflow` - Arrow operator runtime.
- `krishiv-shuffle`, `krishiv-state` - data-plane services (checkpoint merged into state).
- `krishiv-connectors` - source/sink contracts, capability/maturity metadata, I/O, and Iceberg-first lakehouse support; Delta/Hudi/vector integrations are optional and experimental.
- `krishiv-operator`, `krishiv-ui`, `krishiv-flight-sql`, `krishiv-python` - deployment and interface surfaces.

See `docs/README.md` for the full crate map and `docs/contracts/engine-semantics.md` for the published engine guarantees.

## Contributing

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a change. Bug, engine
feature, and connector proposal forms are available in GitHub. Architecture and
durable-format changes should include an ADR under `docs/decisions/`.

Krishiv is licensed under the [Apache License 2.0](LICENSE).
