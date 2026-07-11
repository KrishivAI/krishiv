# Cargo feature graph

Krishiv's Cargo features gate **optional dependency families**, not runtime
behaviour. Execution mode (embedded / single-node / distributed) is always
selected at runtime via `SessionBuilder` / `KRISHIV_MODE` — see
[`docs/README.md`](README.md). This document is the map of what each feature
pulls in and how presets compose, so contributors stop adding orphan
forwarders.

## Layers

```
leaf flags (krishiv-connectors)
        │  parquet · s3 · kafka · avro · iceberg · delta · hudi · vortex
        │  schema-registry · kinesis · pulsar-source · elasticsearch
        │  cassandra · hbase · jdbc · vector-sinks · qdrant · pgvector · cloud
        ▼
forwarders (krishiv-sql, krishiv-api, krishiv-runtime, krishiv-flight-sql,
            krishiv-executor, krishiv-python)
        │  each re-exports the leaf flag it needs, e.g.
        │  krishiv-api/kafka → krishiv-connectors/kafka
        ▼
deployment presets (krishiv binary)
           minimal · local(=embedded+single-node) · full · extended
           embedded · single-node · distributed · bare-metal(=distributed)
           cluster(=distributed) · k8s
```

A leaf flag should be **defined once** in `krishiv-connectors` and forwarded
upward. When you add a connector, add the leaf flag there, then forward it only
through the crates that actually reference the gated symbols.

## The Iceberg rule (lean embedded)

The heavyweight Iceberg tree (`iceberg`, `iceberg-datafusion`,
`datafusion-iceberg`) is **opt-in**. It must not be reachable from a lean
embedded build:

- `krishiv-sql` `default = []` — it does **not** enable Iceberg by default.
- `krishiv-sql`'s dependency on `krishiv-connectors` enables only
  `parquet` (default) + `kafka` + `s3`, **not** `iceberg`.
- The DataFusion catalog DML interception is gated on
  `cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))`, and the
  `iceberg-datafusion` feature pulls `krishiv-connectors/iceberg`.
- The `krishiv` binary's `iceberg` preset re-enables `krishiv-sql/iceberg`, so
  `full` / `k8s` builds are unchanged while `embedded` stays lean.

Validate with:

```sh
cargo tree -p krishiv --no-default-features --features embedded | grep -c iceberg   # → 0
cargo tree -p krishiv --no-default-features --features full     | grep -c iceberg   # → >0
```

## Deployment presets (krishiv binary)

| Preset        | Enables                                              | Notes |
|---------------|------------------------------------------------------|-------|
| `embedded`    | (none)                                               | baseline; in-process, no optional deps |
| `single-node` | `flight-sql`, `shuffle`                              | local daemon + RocksDB metadata |
| `distributed` | `flight-sql`, `shuffle`, `etcd`                     | remote cluster + etcd metadata |
| `bare-metal`  | = `distributed`                                      | alias |
| `cluster`     | = `distributed`                                      | alias (preferred name) |
| `k8s`         | `distributed` + operator CRD/reconciler              | |
| `local`       | `embedded` + `single-node`                           | default |
| `full`        | `single-node` + `distributed` + `k8s` + `kafka` + `iceberg` | |
| `extended` (connectors) | `full` + `delta` + `hudi` + vector sinks   | experimental |

`bare-metal` and `cluster` are exact aliases of `distributed`; prefer
`cluster`. They exist for operator ergonomics and are kept deliberately.

## Connector leaf flags

| Flag | Pulls |
|------|-------|
| `parquet` (default) | local Parquet I/O |
| `s3`, `cloud` | object-store backends (AWS / GCS / Azure) |
| `kafka` | `rdkafka` (librdkafka C lib) |
| `schema-registry` | kafka + Avro + reqwest |
| `avro` | `apache-avro` |
| `iceberg` | `iceberg` + `iceberg-datafusion` |
| `delta`, `hudi` | lakehouse table formats (thin today) |
| `vortex` | Vortex columnar format |
| `kinesis`, `pulsar-source` | cloud streaming sources |
| `elasticsearch`, `cassandra`, `hbase`, `jdbc` | external sinks |
| `vector-sinks` → `qdrant` / `pgvector` | AI/vector sinks |

## Quarantined features (known-broken, tracked)

These optional, **non-preset** features do not currently compile: each rotted
against a dependency-API upgrade and no build/CI ever exercised them in
isolation, so the breakage went unnoticed. They are excluded from the
`just lint-features` guard via `--exclude-features` so the guard stays green for
the supported surface. They are **not** in any shipping preset (`local`, `full`,
`extended`, or the deployment presets), so no supported build is affected.

| Crate | Feature | Root cause (dependency-API drift) |
|-------|---------|-----------------------------------|
| connectors | `pulsar-source` | `pulsar::{Message, MessageId}` import paths changed |
| connectors | `cassandra` | `scylla` builder dropped `request_timeout`; `CassandraConfig` derives + manual `Debug` conflict |
| connectors | `elasticsearch` | `TransportBuilder::connect_timeout` removed |
| connectors | `vortex` | `vortex` import surface changed |
| connectors | `cloud` | `object_store` 0.13 GCS `with_endpoint` removed; Azure builder type change |
| sql | `rest-catalog` | `iceberg-catalog-rest` `RestCatalogConfig` / `RestCatalog::new` now private |
| sql | `unity-catalog` | depends on `rest-catalog` |
| sql | `glue-catalog` | depends on `rest-catalog` |

**To un-quarantine:** fix the connector/catalog against its current dependency
API, drop it from the `--exclude-features` list in `just lint-features`, and the
guard will enforce it from then on. Track each as its own follow-up task.

`postgres-catalog` was similarly rotted (`FileWrite`/`FileRead` trait bounds,
`TableCommit::into_parts`, `FileIOBuilder` factory injection, `TableCommit`
builder privatised) and **has been fixed** (Phase 51, 2026-07-11): it now uses
`KrishivStorageFactory`, `TableCommit::apply`, and one-shot `OutputFile::write`
/ `InputFile::read`; its two integration tests run against live Postgres in
the `test-external` CI tier. The fix also added an advisory lock around
`migrate()` — concurrent `CREATE TABLE IF NOT EXISTS` from two booting nodes
races on Postgres's `pg_type` catalog.

The `iceberg` / `iceberg-datafusion` / `local-catalog` path was similarly rotted
(sqlparser 0.61 `FromTable`/`Statement::Delete`/`Update` changes) and **has been
fixed** — it is in the guarded surface.

## Adding a feature — checklist

1. Define the leaf flag in `krishiv-connectors` (or the owning crate).
2. Forward it **only** through crates that reference its gated symbols.
3. If it pulls a heavy tree, keep it out of any crate's `default`.
4. Add it to a preset only if it belongs in that deployment surface.
5. Run the feature guard: `just lint-features` (cargo-hack `--each-feature`).
