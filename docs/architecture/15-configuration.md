# Configuration

Krishiv is configured at three layers: **Cargo features** decide what is
compiled in, **environment variables** (the `KRISHIV_*` registry) decide how a
process behaves, and **session/SQL settings** decide how one query runs. This
document explains each layer and the rules that keep them honest.

## Layer 1: Cargo features (what is built)

Features gate **optional dependency families**, never runtime behaviour;
execution mode is always a runtime choice (`01`).

```
leaf flags (krishiv-connectors)
   kafka · avro · schema-registry · iceberg · lakehouse · vortex · cloud
   kinesis · pulsar-source · elasticsearch · cassandra · hbase · jdbc
   vector-sinks · qdrant · pgvector · state · two-phase
        ▼ forwarded by
forwarders (krishiv-sql, -api, -runtime, -flight-sql, -executor, -python)
        ▼ composed into
deployment presets (the krishiv binary only)
```

| Preset | Enables | Notes |
|---|---|---|
| `embedded` | nothing | the named baseline for `--no-default-features` |
| `single-node` | `flight-sql`, `shuffle` | local daemon, RocksDB metadata |
| `distributed` | `flight-sql`, `shuffle`, `etcd` | remote cluster |
| `bare-metal` | = `distributed` | alias |
| `k8s` | `distributed` + operator | |
| `local` (**default**) | `embedded` + `single-node` + jemalloc + rest-catalog + iceberg + ui | |
| `prod` | `distributed` + rest-catalog + kafka + iceberg + cloud + jemalloc + jdbc + elasticsearch + ui | what `build-fast-engine.sh` ships |
| `full` | everything above plus `k8s` | widest build |

Rules, each learned from a real breakage:

- **Define a leaf flag once** in `krishiv-connectors`; forward it only through
  crates that reference the gated symbols. Presets exist only on the binary
  (they used to exist on the connectors crate too, with different contents).
- **Never enable a leaf flag in a `[dependencies]` entry** — it is then on for
  every build and no preset can turn it off. Six crates did this and
  `--features embedded` built librdkafka, RocksDB, and reqwest.
- **Gate the backend, not the module.** A `#[cfg(feature)]` on `pub mod kafka`
  hid the `not(feature)` stubs so they never compiled and drifted; keep the
  module, gate the implementation.
- **Never gate an enum variant** (`ConnectorRole::VectorSink`): every
  downstream `match` would need the feature too.
- **The Iceberg rule**: the heavyweight Iceberg tree is unreachable from an
  embedded build — `krishiv-sql` has `default = []` and depends on
  `krishiv-connectors` with only `lakehouse`; the binary's `iceberg` preset
  re-enables it. Verify with `cargo tree -p krishiv --no-default-features
  --features embedded | grep -c iceberg` → 0.
- `just lint-features` sweeps `--each-feature` over the leaf crate, the
  forwarders, and the binary; a flag that stops gating what it claims fails
  there. The quarantine list of rotted optional features has been empty since
  2026-07-25.

Parquet and CSV are unconditional; there is no `parquet` or `s3` flag
(object stores are `cloud`); Delta Lake and Hudi ship inside `lakehouse`.

## Layer 2: environment variables (how a process behaves)

Every `KRISHIV_*` variable is declared in
`krishiv_common::env_registry` with its type, default, and owning crate, and
`../reference/env-flags.md` is **generated** from that registry (216 flags at
the time of writing; a test fails if the document drifts, and
`KRISHIV_BLESS_ENV_FLAGS=1` regenerates it). Do not edit the reference by
hand. Rules the registry enforces:

- A variable is parsed by a pure function over `Option<&str>` so its parsing
  is unit-tested without `set_var` (which is `unsafe` under edition 2024 and
  forbidden).
- The env path and any argv twin share one parser and one clamp — a
  `--poll-interval` clamped to `≥ 1` with an unclamped `KRISHIV_…_SECS` was a
  busy-poll flood, because Kubernetes takes the env path.
- Defaults are pinned by `declared_default_number` tests so the documented
  default and the code's default cannot drift.
- Zero is rejected where zero means "do nothing" (egress cap, input cap) and
  preserved where zero means "immediately" (egress backpressure).

The families, with the document that explains each:

| Family | Examples | See |
|---|---|---|
| mode and endpoints | `KRISHIV_MODE`, `KRISHIV_COORDINATOR_URL`, `KRISHIV_COORDINATOR_HTTP`, `KRISHIV_REMOTE_EXEC`, `KRISHIV_TARGET_PARALLELISM` | `01` |
| durability and storage | `KRISHIV_DURABILITY_PROFILE`, `KRISHIV_METADATA_*`, `KRISHIV_ETCD_*`, `KRISHIV_SHUFFLE_URI`, `KRISHIV_STATE_*`, `KRISHIV_CHECKPOINT_*` | `14` |
| production and auth | `KRISHIV_PRODUCTION`, `KRISHIV_ALLOW_*`, `*_BEARER_TOKEN*`, `KRISHIV_TLS_*`, `KRISHIV_OIDC_*` | `12` |
| executor capacity | `KRISHIV_TASK_SLOTS`, `KRISHIV_QUERY_MEMORY_LIMIT_BYTES`, `KRISHIV_TASK_TARGET_PARALLELISM`, `KRISHIV_SHUFFLE_STORE_BYTES`, `KRISHIV_SHUFFLE_PAGE_CACHE_BYTES` | `05` |
| SQL engine and optimizer | `KRISHIV_JOIN_REORDER`, `KRISHIV_CTE_MATERIALIZE`, `KRISHIV_SEMI_JOIN_*`, `KRISHIV_HASH_JOIN_SINGLE_PARTITION_THRESHOLD_*`, `KRISHIV_REPARTITION_FILE_MIN_SIZE`, `KRISHIV_STAGE_TARGET_PARTITIONS`, `KRISHIV_STAGE_REUSE` | `02`, `03` |
| adaptive execution | `KRISHIV_AQE`, `KRISHIV_AQE_COALESCE`, `KRISHIV_AQE_SKEW_*`, `KRISHIV_AQE_TARGET_PARTITION_BYTES` | `03` |
| shuffle transport | `KRISHIV_SHUFFLE_FETCH_*`, `KRISHIV_SHUFFLE_*_COMPRESSION`, `KRISHIV_SHUFFLE_SERVE_CONCURRENCY`, `KRISHIV_SHUFFLE_SPILL_THRESHOLD_BYTES`, `KRISHIV_SHUFFLE_PARTITIONS` | `06` |
| scheduler | `KRISHIV_JOB_GC_GRACE_SECS`, `KRISHIV_QUEUE_*`, `KRISHIV_RESULT_SPOOL_*`, `KRISHIV_INLINE_RESULT_MAX_BYTES` | `04` |
| streaming dials | `KRISHIV_IDLE_TICK_MS`, `KRISHIV_STREAM_PROFILE`, `KRISHIV_STREAM_LINGER_MS`, `KRISHIV_RLOOP_*`, `KRISHIV_WATERMARK_IDLE_MS` | `08` |
| state | `KRISHIV_ROCKSDB_WRITE_BUFFER_MB`, `KRISHIV_ROCKSDB_MAX_OPEN_FILES` | `07` |
| IVM | `KRISHIV_IVM_SHARDS`, `KRISHIV_IVM_MEMORY_LIMIT_BYTES` | `09` |
| observability | `KRISHIV_LOG_FORMAT`, `RUST_LOG`, OTLP endpoint | `13` |
| frontends | `KRISHIV_MCP_*`, `KRISHIV_UI_TOKEN*` | `11` |
| runtime plumbing | `KRISHIV_FALLBACK_RUNTIME_THREADS` | `17` |
| test/bless switches | `KRISHIV_BLESS_*`, `KRISHIV_TPCH_DATA_DIR*` | `17` |

## Layer 3: session and SQL settings (how a query runs)

`SessionBuilder::with_config(key, value)` and `SET key = value` reach
DataFusion's `SessionConfig` and Krishiv's own keys; `02-sql-engine.md`
lists the values Krishiv sets by default (`target_partitions`, batch size,
`repartition_file_min_size`, hash-join thresholds, `pushdown_filters` off)
and why. Statement-level hints are not supported; `EXPLAIN` shows the
effective plan.

## Precedence

explicit builder/flag → environment variable → profile default → compiled
default. A flag and its environment twin never disagree on parsing or
clamping, by the registry rule above.

## Related

- `../reference/env-flags.md` (generated, authoritative list), `01`, `02`,
  `05`, `14`, `17`.
