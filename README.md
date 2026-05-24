![Krishiv logo](docs/assets/krishiv-logo.svg)

# Krishiv

Krishiv is a Rust-native hybrid compute framework for **batch SQL**, **stateful streaming**, and **lakehouse pipelines**.

It is designed around one engine model across embedded, single-node, and distributed execution so teams can develop locally and scale with consistent semantics.

---

## Why Krishiv

- **One engine for batch + streaming**: no split-brain architecture between two execution systems.
- **Rust-first runtime**: predictable performance, strong type safety, and native systems-level control.
- **Columnar core**: Apache Arrow record batches for in-memory and network data flow.
- **SQL and API surfaces**: SQL CLI plus Rust and Python APIs.
- **Lakehouse-aware**: Iceberg/Delta/Hudi-oriented roadmap and integration path.

---

## Current Status

Krishiv is under active development with release-by-release tracking in `docs/implementation/`.

- Current implementation status: `docs/implementation/status.md`
- Architecture roadmap: `docs/architecture/krishiv-roadmap.md`

If you are evaluating for production, review the implementation status and release tracker notes first.

---

## Quick Start

## 1) Prerequisites

- Rust toolchain (stable)
- Cargo

Check your setup:

```bash
rustc --version
cargo --version
```

## 2) Clone and build

```bash
git clone <your-repo-url> krishiv
cd krishiv
cargo check --workspace
```

## 3) Run SQL from the CLI

```bash
cargo run -p krishiv -- sql --query "select 1 as value"
```

View logical/physical plan:

```bash
cargo run -p krishiv -- explain --query "select 1 as value"
```

List local jobs:

```bash
cargo run -p krishiv -- jobs
```

## 4) Run API examples

Local SQL over Parquet:

```bash
cargo run -p krishiv-api --example local_sql_parquet
```

Memory stream example:

```bash
cargo run -p krishiv-api --example memory_stream
```

---

## Choose an Execution Mode

Krishiv sessions support:

- `Embedded` — in-process execution in your Rust app
- `SingleNode` — local runtime path on one node
- `Distributed` — remote coordinator/executor path (actively evolving)

For most new users, start with **Embedded** or **SingleNode** while validating queries and pipeline logic.

---

## End-User Workflow (Recommended)

1. **Prototype locally** with CLI and embedded/single-node examples.
2. **Validate semantics** with your representative SQL + stream workloads.
3. **Add integration tests** for your schemas and connector paths.
4. **Track supported guarantees** (for example exactly-once certification) from docs before enabling production-critical workflows.
5. **Scale deployment mode** as your workload and release maturity requirements are met.

---

## Documentation Guide

- Project architecture: `docs/architecture/krishiv-roadmap.md`
- Implementation trackers by release: `docs/implementation/README.md`
- Current status and next tasks: `docs/implementation/status.md`
- Embedded examples: `examples/embedded/README.md`
- Batch SQL examples: `examples/batch-sql/README.md`

---

## Contributing

Contributions are welcome.

Suggested contribution flow:

1. Read `AGENTS.md` and relevant implementation tracker docs.
2. Pick a small scoped task from current release trackers.
3. Add tests with your change.
4. Update tracker/status docs when behavior or scope changes.

---

## Vision

Krishiv aims to provide an OSS engine foundation competitive in modern data platforms, with a managed platform layer built on top over time.

The roadmap intentionally stages correctness, performance, and operability milestones release by release.
