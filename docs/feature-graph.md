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
        │  kafka · avro · schema-registry · iceberg · lakehouse · vortex
        │  kinesis · pulsar-source · elasticsearch · cassandra · hbase · jdbc
        │  vector-sinks · qdrant · pgvector · cloud · state · two-phase
        ▼
forwarders (krishiv-sql, krishiv-api, krishiv-runtime, krishiv-flight-sql,
            krishiv-executor, krishiv-python)
        │  each re-exports the leaf flag it needs, e.g.
        │  krishiv-api/kafka → krishiv-connectors/kafka
        ▼
deployment presets (krishiv binary)
           local (default) · prod (what ships) · full
           embedded · single-node · distributed · bare-metal(=distributed) · k8s
```

Parquet and CSV are the unconditional base — there is no `parquet` flag, and no
`s3` flag (object stores are `cloud`). Delta Lake and Hudi ship with
`lakehouse`; they had flags that gated no module and no dependency, so the
formats were always compiled while only their certification suites were
switched off. Deployment presets exist **only** on the binary: `local` / `full`
/ `extended` used to be declared on `krishiv-connectors` as well, with different
contents and no consumer, so `--features full` meant two different capability
sets one level apart.

A leaf flag should be **defined once** in `krishiv-connectors` and forwarded
upward. When you add a connector, add the leaf flag there, then forward it only
through the crates that actually reference the gated symbols.

## The Iceberg rule (lean embedded)

The heavyweight Iceberg tree (`iceberg`, `iceberg-datafusion`,
`datafusion-iceberg`) is **opt-in**. It must not be reachable from a lean
embedded build:

- `krishiv-sql` `default = []` — it does **not** enable Iceberg by default.
- `krishiv-sql`'s dependency on `krishiv-connectors` enables only `lakehouse`,
  **not** `iceberg` and **not** `kafka`. It used to pin `kafka` too, which put
  librdkafka in every build including `embedded` (see "Where features must not
  be declared" below).
- The DataFusion catalog DML interception is gated on
  `cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))`, and the
  `iceberg-datafusion` feature pulls `krishiv-connectors/iceberg`.
- The `krishiv` binary's `iceberg` preset re-enables `krishiv-sql/iceberg`, so
  `full` / `k8s` builds are unchanged while `embedded` stays lean.

Validate with:

```sh
cargo tree -p krishiv --no-default-features --features embedded | grep -c iceberg   # → 0
cargo tree -p krishiv --no-default-features --features full     | grep -c iceberg   # → >0
cargo tree -p krishiv --no-default-features --features embedded | grep -c rdkafka   # → 0
cargo tree -p krishiv --no-default-features --features prod     | grep -c rdkafka   # → 1
```

## Where features must not be declared

A leaf flag enabled in a `[dependencies]` entry is on for every build, and no
preset can switch it off:

```toml
# wrong — the `kafka` feature above this line can no longer gate anything
krishiv-connectors = { path = "…", features = ["kafka"] }
```

Six crates did this (`krishiv-sql`, `-api`, `-mcp`, `-python`, `-dataflow`,
`-executor`), which is why `--features embedded` built rdkafka, rocksdb and
reqwest while advertising "no optional deps". Two shapes make it tempting, and
both are the actual bug:

- **A module gated on its own feature.** `pub mod kafka` was `#[cfg(feature =
  "kafka")]`, so the module vanished entirely without the flag — even though the
  file already carried complete `#[cfg(not(feature = "kafka"))]` stubs, which had
  therefore never compiled and had silently drifted out of API parity. Gate the
  *backend*, not the module, and let the stub arm keep the API stable.
- **A feature-gated enum variant.** `ConnectorRole::VectorSink` changed the
  shape of a public enum, so every downstream `match` needed the feature too.
  Keep the tag; gate the driver behind it.

`just lint-features` sweeps `--each-feature` over the leaf crates, the forwarder
tier and the binary, so a flag that stops gating what it claims to gate fails
there.

## Deployment presets (krishiv binary)

| Preset        | Enables                                              | Notes |
|---------------|------------------------------------------------------|-------|
| `embedded`    | (none)                                               | explicit empty marker for `--no-default-features` |
| `single-node` | `flight-sql`, `shuffle`                              | local daemon + RocksDB metadata |
| `distributed` | `flight-sql`, `shuffle`, `etcd`                      | remote cluster + etcd metadata |
| `bare-metal`  | = `distributed`                                      | alias, for `run_bare_metal.sh` and the justfile |
| `k8s`         | `distributed` + operator CRD/reconciler              | |
| `local`       | `embedded` + `single-node` + jemalloc + rest-catalog + iceberg + ui | the `default` |
| `prod`        | `distributed` + rest-catalog + kafka + iceberg + cloud + jemalloc + jdbc + elasticsearch + ui | **what `build-fast-engine.sh` ships** |
| `full`        | `single-node` + `distributed` + `k8s` + kafka + iceberg + cloud + jdbc + elasticsearch + ui | widest build |

`embedded` gates nothing by design — it is the name for the baseline so
`check-embedded` / `test-embedded` / release.yml can say which build they mean.
`bare-metal` is an exact alias of `distributed`, kept for operator ergonomics.
There is no `cluster`, `minimal` or `extended` preset on the binary.

## Connector leaf flags

| Flag | Pulls |
|------|-------|
| (none — base) | local Parquet and CSV I/O |
| `cloud` | object-store backends (AWS / GCS / Azure) |
| `kafka` | `rdkafka` (librdkafka C lib) |
| `schema-registry` | kafka + Avro + reqwest + prost |
| `avro` | `apache-avro` |
| `lakehouse` | rocksdb + chrono + url; Delta Lake and Hudi ship here |
| `iceberg` | `lakehouse` + `iceberg` + datafusion + typetag |
| `state` | `krishiv-state` (durable keyed state) |
| `two-phase` | two-phase-commit Parquet/S3 sink |
| `vortex` | Vortex columnar format |
| `kinesis`, `pulsar-source` | cloud streaming sources |
| `elasticsearch`, `cassandra`, `hbase`, `jdbc` | external sinks |
| `vector-sinks` → `qdrant` / `pgvector` | AI/vector sinks |
| `exactly-once-integration` | gates one test target, not a capability |

## Previously quarantined features (all fixed, now guard-enforced)

These optional, **non-preset** features had each rotted against a dependency-API
upgrade while no build or CI job exercised them in isolation, so the breakage
went unnoticed. All are fixed and enforced now; the table is kept as the record
of what rotted and why, not as a list of live exclusions.

**Quarantine list: EMPTY as of 2026-07-25.** Every optional feature below was
fixed and the `--exclude-features` escape hatch removed from `just
lint-features`, so the per-feature guard now enforces all of them.

| Crate | Feature | Root cause (dependency-API drift) | Fixed |
|-------|---------|-----------------------------------|-------|
| connectors | `pulsar-source` | `pulsar::{Message, MessageId}` import paths changed | 2026-07-25 — `Message`/`MessageData` now imported from `pulsar::consumer`; deferred ack uses `ack_with_id(topic, id)` since only the id is retained |
| connectors | `cassandra` | `scylla` builder dropped `request_timeout`; `CassandraConfig` derives + manual `Debug` conflict | 2026-07-25 — request deadline moved to an `ExecutionProfile`, connect bounded by `connection_timeout`, duplicate `#[derive(Debug)]` dropped |
| connectors | `elasticsearch` | `TransportBuilder::connect_timeout` removed | 2026-07-25 — whole-request `timeout` only (the client exposes no separate connect timeout) |
| connectors | `vortex` | `vortex` import surface changed | already building; un-quarantined 2026-07-25 |
| connectors | `cloud` | `object_store` 0.13 GCS `with_endpoint` removed; Azure builder type change | already building; un-quarantined 2026-07-25 |
| sql | `rest-catalog` | `iceberg-catalog-rest` `RestCatalogConfig` / `RestCatalog::new` now private | already building; un-quarantined 2026-07-25 |
| sql | `unity-catalog` | depends on `rest-catalog` | already building; un-quarantined 2026-07-25 |
| sql | `glue-catalog` | depends on `rest-catalog` | already building; un-quarantined 2026-07-25 |

**How this was missed:** the three genuinely-broken features were excluded from
`just lint-features`, which is the only job that compiles them — `full` does not
include elasticsearch/cassandra/pulsar-source, so no other CI target touched
them. A connector can therefore be listed as reachable in the
[connector reachability matrix](reference/connector-reachability-matrix.md) and
still be unshippable. The guard now has no exclusions for exactly this reason.

**If a feature rots again:** fix it against the current dependency API. Do not
re-introduce an `--exclude-features` list in `just lint-features` — that is what
let these three ship broken. A feature that cannot be kept building should be
deleted (wire-or-delete, Phase 51), not hidden from the guard.

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
2. Forward it **only** through crates that reference its gated symbols, in
   `[features]` — never as `features = [...]` on a `[dependencies]` entry.
3. Check it gates a **dependency**. A flag that is only `= ["other-feature"]`
   cannot make any build leaner; it will end up gating test coverage instead,
   which is how `delta` and `hudi` came to switch off their own certification
   suites while shipping 4.2k lines unconditionally.
4. If it pulls a heavy tree, keep it out of any crate's `default`.
5. Add it to a preset only if it belongs in that deployment surface — and if it
   is in no preset, `lint-features` is the only thing that will ever build it.
6. Run the feature guard: `just lint-features` (cargo-hack `--each-feature`).
