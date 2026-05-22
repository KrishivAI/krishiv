# Krishiv R12–R20: Nine-Release Strategic Roadmap

Generated: 2026-05-21  
Scope: R12 through R20, covering foundation repair, Python-first developer experience,
incremental computation, Spark compatibility, advanced streaming, AI/ML pipelines,
storage unification, multi-region scale, and enterprise platform readiness.

---

## Strategic Arc

R1–R11 built and stabilised a GA-quality distributed compute engine. The next
nine releases pursue three compounding goals:

1. **Correctness and completeness** (R12–R13): repair the 90 confirmed audit
   findings, connect the real Kafka runtime, enable remote coordinator mode, and
   deliver a Python API that is genuinely usable outside of embedded demos.

2. **Competitive differentiation** (R14–R17): surpass the three most relevant
   OSS compute engines in their own categories —
   - **CocoIndex** (incremental computation for AI pipelines): live tables,
     function-level memoization, schema evolution;
   - **Sail / LakeHQ** (Spark SQL drop-in): PySpark API shim, Spark function
     coverage, dbt adapter;
   - **Pathway** (Python-first unified stream+batch): schema-driven Python API,
     asyncio-native streaming, 350+ connector parity.

3. **Enterprise scale** (R18–R20): Delta/Hudi/Iceberg REST unification, multi-
   region federation, autoscaling, compliance tooling, and managed service
   packaging.

### Python API Philosophy

Across R13–R20 the Python API converges on a single ergonomic model:

```
Schema-declared → Source → Transformation chain → Sink → Run/Await
```

Inspired by Pathway's table-centric model but extended with Krishiv's
distributed execution, Flink-style stateful semantics, and CocoIndex-style
incremental memoization. Users who know pandas, PySpark, or Pathway will
find the transition natural. Jupyter-first: every result type implements
`_repr_html_()`.

---

## Release Table

| Release | Theme | Quarter |
|---------|-------|---------|
| R12 | Foundation Completeness & Real Connectivity | Q3 2026 |
| R13 | Python-First Streaming API | Q4 2026 |
| R14 | Incremental Computation & CDC Lakehouse | Q1 2027 |
| R15 | Spark SQL & Ecosystem Compatibility | Q2 2027 |
| R16 | Advanced Stateful Streaming & Exactly-Once | Q3 2027 |
| R17 | AI/ML Native Data Platform | Q4 2027 |
| R18 | Storage Format Unification & Time Travel | Q1 2028 |
| R19 | Multi-Region, Autoscaling & Cloud-Native | Q2 2028 |
| R20 | Enterprise Platform & Ecosystem | Q3 2028 |

---

## R12: Foundation Completeness & Real Connectivity

### Goal

Eliminate every P0 critical bug from the 2026-05-21 audit, wire real Kafka
into the streaming and CDC stacks, enable the remote coordinator CLI mode, and
complete the deferred R4 AQE and performance items. Nothing new is promised
until the existing promises work correctly.

### Why Now

The GA platform (R11) has 21 confirmed P0 bugs — crashes, data loss, split-
brain, and security holes. A single `block_on` call inside an async context
panics the Kubernetes operator on every leader-election tick. The checkpoint
barrier epoch is silently discarded, breaking exactly-once contracts. The
entire streaming and CDC stack requires rdkafka to be useful outside CI.
These must be fixed before any new feature work.

### Audit Items Addressed (P0 → all; P1 → selected high-value)

| ID | Summary |
|----|---------|
| P0.1 | Fix dual `SqlEngine` split in `SessionBuilder` — share one `Arc<SqlEngine>` |
| P0.3 | Fix `block_on_krishiv` runtime creation per call — use `Handle::current().block_on()` |
| P0.4 | Fix blocking filesystem I/O in async shuffle/checkpoint — `spawn_blocking` |
| P0.5 | Fix barrier epoch loss in `OperatorQueueReceiver::recv` — pending-barrier slot |
| P0.6 | Fix silent checkpoint snapshot failure — propagate error in `CheckpointAckRequest` |
| P0.7 | Fix `RedbStateBackend::load_snapshot` partial failure — atomic redb transaction |
| P0.8 | Fix `unix_now_ms` clock underflow — return `StateError::ClockError` |
| P0.9 | Fix `decode_if_live` panic on corrupt data — return `StateError::CorruptEntry` |
| P0.10 | Fix `downcast_ref().unwrap()` panics in exec operators |
| P0.11 | Fix `LeaderElection::block_on` called from async context — make trait methods `async` |
| P0.12 | Fix K8s `Merge` patch ignoring `resourceVersion` — switch to `Patch::Apply` |
| P0.13 | Fix flight-sql `check_table_access` never invoked — parse tables before execution |
| P0.14 | Fix `MaskingRule::Redact` schema corruption — `new_null_array` for non-string columns |
| P0.15 | Fix non-deterministic hash masking — use `sha2::Sha256` |
| P0.16 | Fix `TtlStateBackend` snapshot portability — strip TTL prefix in snapshot |
| P0.17 | Fix proto wire field drops — complete `executor_heartbeat_request_to_wire` |
| P0.18 | Fix `SlidingWindowOperator::window_starts` infinite loop — validate slide_ms |
| P0.19 | Fix O(n²) duplicate detection — `HashSet<usize>` |
| P0.20 | Fix `HttpEmitter::emit` swallows 4xx/5xx — `.error_for_status()?` |
| P0.21 | Fix `audit_log` duplicate events |
| P1.1 | Fix streaming heartbeat O(jobs×tasks) — add `HashMap<TaskId, (JobId, StageId)>` index |
| P1.2 | Wire gRPC channel pool — `HashMap<endpoint, Channel>` reused across calls |
| P1.17 | Fix `CoalesceRule::apply` no-op stub — implement AQE partition coalescing |
| P1.23 | Fix `recover_from_store` stale in-memory state |
| P1.24 | Fix `retry_stage` wrong task state — `Assigned` vs `Pending` |
| P1.28 | Fix `RateLimiter` first-call over-refill |

### New Features

**Real Kafka (rdkafka)**

Wire `rdkafka` into `CdcToLakehousePipeline` and the streaming source
stack. Implement `RdkafkaCdcEventSource` behind the `CdcEventSource` trait
introduced in R11. Add `KafkaSource` backed by a real consumer group with
offset commit, partition assignment, and rebalance handling.

```rust
// Rust API
let pipeline = CdcToLakehousePipeline::builder()
    .source(RdkafkaCdcEventSource::new("kafka:9092", "my-topic", "my-group"))
    .sink(IcebergSink::new("s3://warehouse/table"))
    .build();
pipeline.run_with_source(...).await?;
```

**Remote Coordinator CLI**

Enable `--coordinator http://addr:7070` on all CLI sub-commands. The CLI
dispatches all commands through the coordinator gRPC API instead of the
in-process coordinator. This is the first mode where `krishiv savepoint`,
`krishiv restore`, and `krishiv jobs` work against real distributed clusters.

```bash
export KRISHIV_COORDINATOR=http://coordinator:7070
krishiv jobs
krishiv savepoint --job streaming-job-1 --label "pre-deploy"
krishiv checkpoints list --job streaming-job-1
```

**LZ4/Zstd Shuffle Compression** (R4 deferred)

Add compression negotiation to shuffle write/read paths. Default: `None`.
Available: `Lz4`, `Zstd`. Configurable per job via `JobSpec.shuffle_compression`.

**AQE Coalescing** (R4b deferred)

Implement `CoalesceRule::apply` to rewrite shuffle partition count based on
runtime partition size statistics. Small-partition merging reduces task overhead
on skewed joins.

### Acceptance Gate

- All 21 P0 audit items pass regression tests.
- `CdcToLakehousePipeline` runs an end-to-end test against a real Kafka broker
  (Docker Compose in CI) with exactly-once delivery to a local Iceberg table.
- `krishiv savepoint --coordinator http://...` triggers a real savepoint on a
  live cluster.
- `cargo test --workspace` passes; `cargo clippy --workspace -- -D warnings` clean.

### Maturity gaps, risks, and resolutions (post-R12 review)

R12 closed the original P0/P1 audit inventory and several structural slices (Kafka
feature gate, `CoalesceRule`, compression codecs, federation skeleton). A separate
**subsystem maturity review** (2026-05-22) identified additional gaps where library
code exists but **binaries, enforcement, or end-to-end paths** remain incomplete.

**Canonical register:** [`r12-maturity-gap-register.md`](r12-maturity-gap-register.md)

| Theme | Example gap IDs | Target resolution |
|-------|-----------------|-------------------|
| Checkpoint fencing not enforced at write | GAP-CP-03, GAP-CK-01, GAP-CK-04 | **R12 carryover** — wire `validate_fencing_token` in `commit_epoch` / restore |
| Remote CLI / distributed session stubs | GAP-RT-01, GAP-RT-04 | **R12 carryover** (RPCs) + **R13** (Flight SQL `DistributedBackend`) |
| Runtime backends accept-only | GAP-RT-01, GAP-RT-03 | **R13** — ADR-12.3/12.4/12.5; `WindowedStream` → executor fragments |
| Streaming state not durable | GAP-ST-01, GAP-ST-03, GAP-ST-05 | **R16** — `StateBackend` + barrier transport (ADR-16.3) |
| Shuffle compression off hot path | GAP-SH-01, GAP-SH-03 | **R12 carryover** — executor path + stable hash |
| Connector / matrix honesty | GAP-CN-01, GAP-CN-03 | **R12 carryover** (kafka compile) + **R14** (full certification) |
| HA coordinator | GAP-CP-01, GAP-CP-02 | **R16** / R9 lease model |
| Policy bypass on default `sql()` | GAP-RT-05 | **R12 carryover** / R13 fail-closed when policy configured |
| Doc vs code drift | GAP-DOC-01 | **R12 carryover** — trackers require L4 validation per gap ID |

**R12 carryover sprint (before R13):** GAP-CP-03, GAP-CK-01, GAP-CN-01, GAP-RT-04,
GAP-CP-04–06, GAP-SH-01/03, GAP-RT-05, GAP-DOC-01. See the register for full
acceptance tests per gap.

**Deferred to R13+ (unchanged intent):** S6.2 SingleNode mpsc, S6.3 embedded
streaming redirect, full Flight transport, watermark-aware Kafka — now tracked with
gap IDs in the register.

---

## R13: Python-First Streaming API

### Goal

Deliver a Python streaming API that is genuinely competitive with Pathway's
developer experience: schema-declared, asyncio-native, Jupyter-friendly, and
installable from PyPI with a single `pip install krishiv`.

### Why Now

R8 shipped Python bindings marked beta. Pathway and CocoIndex are both gaining
traction because they offer simple Python APIs that feel natural to data
scientists and ML engineers. Krishiv's current Python surface (`PySession.sql()`)
requires users to understand the Rust internals. R13 changes this.

### Features

**Packaging**

- maturin build pipeline for manylinux2014 wheels.
- PyPI package: `pip install krishiv`.
- `.pyi` type stub generation for all public Python APIs.
- Optional extras: `krishiv[kafka]`, `krishiv[iceberg]`, `krishiv[arrow]`.

**Schema-Declared Sources**

```python
import krishiv as pw

class OrderEvent(ks.Schema):
    order_id: str
    user_id: str
    amount: float
    event_time: ks.DateTimeUtc

# Read from Kafka with schema enforcement
orders = ks.read_kafka(
    servers="kafka:9092",
    topic="orders",
    schema=OrderEvent,
    group_id="my-pipeline",
)

# Read from Parquet
events = ks.read_parquet("s3://bucket/events/", schema=OrderEvent)

# Read from Iceberg
catalog = ks.read_iceberg("s3://warehouse/", table="orders")
```

**Asyncio-Native Streaming**

```python
# Full asyncio integration
async def main():
    session = await ks.Session.connect_async("http://coordinator:7070")
    stream = await session.read_kafka_async("events", schema=OrderEvent)

    async for batch in stream.window(tumbling="5m"):
        df = batch.to_pandas()
        print(df.groupby("user_id")["amount"].sum())

asyncio.run(main())
```

**Transformation Chain**

```python
result = (
    orders
    .with_watermark("event_time", max_delay="30s")
    .key_by("user_id")
    .window(tumbling="1h")
    .agg(
        total=ks.sum("amount"),
        count=ks.count(),
        last_order=ks.max("event_time"),
    )
)
```

**Windowing API**

```python
# Tumbling
stream.window(tumbling="5m")
stream.window(tumbling=timedelta(minutes=5))

# Sliding
stream.window(sliding=SlidingWindow(size="10m", slide="2m"))

# Session
stream.window(session=SessionWindow(gap="30s"))

# Global (accumulate all)
stream.window(global_window=True)
```

**Sinks**

```python
# Write to Parquet
result.sink(ks.sinks.parquet("s3://output/hourly/", partition_by="date"))

# Write back to Kafka
result.sink(ks.sinks.kafka("kafka:9092", topic="user-totals"))

# Write to Iceberg
result.sink(ks.sinks.iceberg("s3://warehouse/", table="user_totals"))

# Collect for inspection
batches = await result.collect_async(timeout=30)
df = ks.to_pandas(batches)
```

**Jupyter Display**

```python
# _repr_html_() on all result types
stream  # renders live-updating card in Jupyter
result.preview()  # renders first 100 rows in a table
```

**Error Hierarchy**

```python
from krishiv.errors import (
    KrishivError,         # base
    QueryError,           # SQL parse/plan errors
    SchemaError,          # schema mismatch
    ConnectorError,       # source/sink failures
    CheckpointError,      # save/restore failures
    AuthorizationError,   # policy denials
)
```

**Pandas/PyArrow Bridge**

```python
# Arrow-zero-copy to pandas
df = batch.to_pandas()                    # zero-copy when possible
table = batch.to_arrow()                  # PyArrow Table
batches = result.to_arrow_batches()       # iterator of RecordBatch

# From pandas
stream = ks.from_pandas(df, schema=OrderEvent)
```

### Acceptance Gate

- `pip install krishiv` installs on Linux (manylinux2014), macOS (arm64, x86_64).
- End-to-end test: read Kafka → tumbling window → sink Parquet in under 20 lines of Python.
- `.pyi` stubs pass `mypy --strict` on the test suite.
- Jupyter rendering works in JupyterLab 4.x.
- `ks.read_parquet`, `ks.read_kafka`, `ks.read_iceberg` all round-trip through
  their respective sinks in integration tests.

---

## R14: Incremental Computation & CDC Lakehouse

### Goal

Deliver CocoIndex-competitive incremental computation: `CREATE LIVE TABLE` SQL,
function-level transformation memoization, multi-table CDC fan-out with schema
evolution, and exactly-once CDC-to-Iceberg pipelines. A user should be able to
define a live analytical table once and have it stay current as the source
changes — without writing custom delta logic.

### Why Now

CocoIndex is gaining enterprise adoption specifically because it solves "stale
data feeding LLMs and dashboards" without requiring engineers to manually
write incremental logic. Krishiv has the CDC connector and Iceberg sink from R10,
but no incremental view maintenance and no schema evolution. This release makes
the CDC-to-lakehouse story production-grade.

### Features

**Live Tables (Incremental Materialized Views)**

```sql
-- SQL API
CREATE LIVE TABLE user_totals AS
SELECT user_id, sum(amount) as total, count(*) as order_count
FROM orders_cdc
GROUP BY user_id
REFRESH ON CHANGE;                 -- incremental, not full re-scan

-- With explicit sink
CREATE LIVE TABLE daily_revenue
STORED AS ICEBERG LOCATION 's3://warehouse/daily_revenue'
AS SELECT date_trunc('day', event_time) as day, sum(amount) as revenue
FROM orders_cdc
GROUP BY 1
REFRESH EVERY '1m';
```

```python
# Python API
live = session.live_table(
    query="""
        SELECT user_id, sum(amount) as total
        FROM orders GROUP BY user_id
    """,
    source=ks.sources.kafka_cdc(
        servers="kafka:9092",
        topic="orders-cdc",
        format="debezium",
    ),
    sink=ks.sinks.iceberg("s3://warehouse/", table="user_totals"),
    refresh=ks.Trigger.on_change(),
)

# Subscribe to change notifications
live.on_change(lambda changeset: notify_dashboard(changeset))

# Run indefinitely
await live.run_async()
```

**Function-Level Memoization** (CocoIndex-inspired)

```python
@ks.transform(memo=True, namespace="doc-pipeline")
def embed_document(doc: ks.Row, model: str = "text-embedding-3-small") -> ks.Row:
    """Only re-runs when doc content or model changes."""
    vector = openai.embed(doc["content"], model=model)
    return {**doc, "embedding": vector, "model": model}

# Apply across a streaming table — skips unchanged rows
docs = ks.read_iceberg("s3://warehouse/docs")
embedded = docs.transform(embed_document)
embedded.sink(ks.sinks.qdrant("http://qdrant:6333", collection="docs"))
```

**Multi-Table CDC Fan-Out**

```python
cdc = ks.cdc_pipeline(
    sources=[
        ks.sources.kafka_cdc("orders-cdc", table="orders", schema=OrderSchema),
        ks.sources.kafka_cdc("customers-cdc", table="customers", schema=CustomerSchema),
    ],
    sink=ks.sinks.iceberg_catalog("s3://warehouse/"),
    schema_evolution=ks.SchemaEvolution(
        allow_add_columns=True,
        allow_rename_columns=True,
        allow_type_coercion=ks.TypeCoercion.SAFE,
        breaking_changes=ks.BreakingChangePolicy.FAIL,
    ),
)
await cdc.run_async()
```

**Schema Evolution**

Support for:
- Add nullable columns (always safe).
- Rename columns (with backward-compat alias).
- Widen types (`INT32` → `INT64`, `FLOAT32` → `FLOAT64`).
- Drop columns (configurable: ignore, error, null-fill).
- Incompatible changes (configurable: fail, quarantine, schema-version branch).

**Change-Set Output (ChangeFeed)**

```python
# Emit only changed rows downstream
feed = live.as_changefeed()  # emits (op: insert/update/delete, row)

async for change in feed:
    if change.op == ks.Op.DELETE:
        cache.evict(change.row["user_id"])
    else:
        cache.upsert(change.row["user_id"], change.row)
```

**Exactly-Once CDC → Iceberg**

Certify exactly-once delivery for the path:
`Kafka (Debezium 2.x) → Krishiv CDC pipeline → Iceberg (two-phase commit)`.

Uses Kafka transactional producer for offset commits, Iceberg snapshot commit
for sink writes. Coordinator barriers align Kafka offsets with Iceberg snapshot
IDs in the checkpoint metadata.

### Acceptance Gate

- `CREATE LIVE TABLE` runs an end-to-end test: insert rows into Postgres → Debezium
  → Kafka → Krishiv live table → Iceberg. Verify Iceberg snapshot reflects inserts
  within 10 seconds.
- Schema evolution test: add a nullable column mid-stream; pipeline continues
  without restart.
- Memoized transform test: change one document in a 10k-document corpus; verify
  only the changed document's transform is re-executed.
- Exactly-once test: kill coordinator mid-checkpoint; restart; verify no duplicate
  rows in Iceberg.

---

## R15: Spark SQL & Ecosystem Compatibility

### Goal

Deliver a Sail-competitive Spark SQL compatibility layer so enterprise teams with
existing PySpark codebases can migrate to Krishiv without rewriting their
transformations. Add first-class integrations with dbt, Airflow, and Great
Expectations to fit into existing data engineering stacks.

### Why Now

70%+ of enterprise data teams use Spark. Sail is winning mindshare among teams
that want Rust-native performance without code rewrites. Krishiv already uses
DataFusion (which Sail also uses). The delta is primarily the PySpark API shim
and Spark SQL function coverage.

### Features

**SparkSession Compatibility Shim**

```python
from krishiv.compat.spark import SparkSession, col, avg, sum, explode

spark = (
    SparkSession.builder
    .appName("MyPipeline")
    .remote("sc://coordinator:7070")   # Krishiv coordinator as Spark Connect server
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# Standard PySpark — works unchanged
df = spark.read.parquet("s3://bucket/sales")
result = (
    df
    .filter(col("region") == "US")
    .groupBy("product_id")
    .agg(sum("revenue").alias("total_revenue"), avg("margin").alias("avg_margin"))
    .orderBy("total_revenue", ascending=False)
)
result.write.mode("overwrite").parquet("s3://output/us-summary")
```

**Spark SQL Function Coverage**

Implement Spark 3.5 SQL function parity in DataFusion, covering:
- Date/time: `to_timestamp`, `date_format`, `date_add`, `date_diff`, `trunc`
- String: `regexp_replace`, `split`, `concat_ws`, `lpad`, `rpad`, `levenshtein`
- Array: `explode`, `explode_outer`, `posexplode`, `array_contains`, `flatten`
- Struct/Map: `struct`, `map`, `map_keys`, `map_values`, `from_json`, `to_json`
- Window: `row_number`, `dense_rank`, `lead`, `lag`, `ntile`, `first_value`
- ML/stats: `percentile_approx`, `corr`, `covar_pop`, `skewness`, `kurtosis`

**Spark Connect Protocol Server**

Expose a Spark Connect gRPC endpoint on the coordinator so tools that speak
Spark Connect (DBT Spark adapter, Databricks SDK, native PySpark ≥ 3.4) connect
without a shim.

**dbt Adapter**

```yaml
# profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: krishiv
      coordinator: http://coordinator:7070
      database: default
      schema: analytics
      threads: 8
```

```sql
-- models/user_totals.sql (standard dbt, runs on Krishiv)
SELECT user_id, sum(amount) as total
FROM {{ ref('orders') }}
GROUP BY user_id
```

**Airflow Operator**

```python
from krishiv.airflow import KrishivSubmitJobOperator, KrishivJobSensor

submit = KrishivSubmitJobOperator(
    task_id="run_etl",
    coordinator="http://coordinator:7070",
    job_spec={"query": "INSERT INTO summary SELECT ...", "parallelism": 8},
)
wait = KrishivJobSensor(
    task_id="wait_etl",
    coordinator="http://coordinator:7070",
    job_id="{{ task_instance.xcom_pull('run_etl') }}",
)
submit >> wait
```

**Migration Tooling**

```bash
# Analyze a PySpark script and emit compatibility report
krishiv compat analyze my_spark_job.py

# Output:
# ✓ DataFrame operations: 100% compatible
# ✗ spark.read.jdbc: JDBC connector required (R15 feature)
# ⚠ UDF type annotations missing: provide return type for type inference
```

**Great Expectations Integration**

```python
import great_expectations as ge
from krishiv.ge import KrishivDatasource

datasource = KrishivDatasource(coordinator="http://coordinator:7070")
batch = datasource.get_batch("orders", query="SELECT * FROM orders LIMIT 10000")
suite = ge.ExpectationSuite("orders_quality")
suite.expect_column_values_to_not_be_null("order_id")
suite.expect_column_values_to_be_between("amount", 0, 100_000)
results = batch.validate(suite)
```

### Acceptance Gate

- PySpark TPC-H SF10 benchmark queries Q1–Q22 produce identical results via
  SparkSession shim.
- dbt `dbt run` against a Krishiv profile executes all standard model types
  (table, view, incremental).
- Airflow DAG with `KrishivSubmitJobOperator` submits and polls a job successfully.
- Migration analyzer correctly identifies at least 10 categories of
  incompatibility from a real-world PySpark script.

---

## R16: Advanced Stateful Streaming & Exactly-Once

### Goal

Deliver Flink-competitive stateful streaming: complex event processing, temporal
joins, exactly-once across all certified connector pairs, state rescaling on
restore, and late-data side outputs. This is the release where Krishiv can
replace Flink for streaming workloads without sacrificing correctness.

### Why Now

R5–R6 delivered the streaming foundations and checkpoint protocol. R11–R13
stabilised the platform and Python API. R16 is the right time to add the
advanced stateful patterns that enterprise fraud-detection, IoT, and financial
streaming workloads require — the use cases that drive Flink adoption.

### Features

**Complex Event Processing (CEP)**

```python
# Detect fraud: purchase within 5m of login from a different country
fraud_pattern = (
    ks.Pattern()
    .begin("login",    lambda e: e["type"] == "login")
    .followed_by("purchase", lambda e: e["type"] == "purchase")
    .where(lambda login, purchase: login["country"] != purchase["country"])
    .within("5m")
)

alerts = events.key_by("user_id").match_pattern(fraud_pattern)
alerts.sink(ks.sinks.kafka("kafka:9092", topic="fraud-alerts"))
```

**Temporal Joins**

```python
# Stream-table join: enrich events with latest user profile
enriched = events.temporal_join(
    table=session.read_iceberg("user_profiles"),
    on="user_id",
    as_of="event_time",
    tolerance="1h",  # accept profile updates up to 1h stale
)

# Stream-stream join: correlate payments with authorisations
matched = payments.interval_join(
    authorisations,
    left_key="auth_id",
    right_key="auth_id",
    lower_bound="-5m",
    upper_bound="30m",
    event_time=("payment_time", "auth_time"),
)
```

**Exactly-Once — Full Certification Matrix**

| Source | Sink | Exactly-Once |
|--------|------|-------------|
| Kafka | Iceberg | ✓ R14 |
| Kafka | Kafka | ✓ R16 (Kafka transactions) |
| Kafka | Parquet/S3 | ✓ R16 (2PC) |
| S3/Parquet | Iceberg | ✓ R16 |
| S3/Parquet | Kafka | ✓ R16 |

**State Rescaling**

```python
# Restore a 4-partition job into an 8-partition deployment
session.restore(
    job_id="user-aggregation",
    epoch=42,
    new_parallelism=8,
    key_rescaling=ks.RescalingStrategy.CONSISTENT_HASHING,
)
```

**Late Data & Side Outputs**

```python
result, late_events = (
    stream
    .with_watermark("event_time", max_delay="5m")
    .window(tumbling="1h")
    .agg(total=ks.sum("amount"))
    .with_side_output("late", lateness_threshold="1h")
)

# Route late data to a repair queue
late_events.sink(ks.sinks.kafka("repair-queue"))
```

**Full gRPC Barrier Transport** (R6b deferred)

Replace the in-process barrier simulation with a real gRPC checkpoint barrier
that flows through each operator in the distributed execution graph. Enables
exactly-once across multi-host executor topologies.

**RocksDB Incremental Checkpointing** (R5.2 deferred)

Incremental state snapshots: only changed RocksDB SSTables are uploaded to
object storage per checkpoint epoch. Reduces checkpoint time from O(total state)
to O(changed state).

**State Schema Migration**

```python
@ks.state_migration(from_version=1, to_version=2)
def migrate_user_state(old: dict) -> dict:
    """Add 'lifetime_value' field with a default."""
    return {**old, "lifetime_value": old.get("total", 0) * 1.1}
```

### Acceptance Gate

- CEP fraud-detection test: inject 10,000 events; pattern matches ≥ 99% of
  synthetic fraud cases.
- Temporal join test: stream-table join produces identical output to a reference
  Flink job on the Nexmark benchmark dataset.
- Exactly-once Kafka→Kafka test: kill executor mid-window; verify no duplicate
  output messages.
- Rescaling test: save a 4-partition job, restore as 8-partition, verify output
  correctness with `cargo test`.
- Incremental checkpoint test: second checkpoint uploads < 10% of the data of
  the first checkpoint.

---

## R17: AI/ML Native Data Platform

### Goal

Make Krishiv the native compute engine for AI/ML data pipelines: embedding
generation, RAG index building, vector store sinks, LLM UDFs, and incremental
re-indexing as source data changes. Target the use cases where CocoIndex
currently wins.

### Why Now

The fastest-growing enterprise data workload is ML feature engineering and LLM
context pipelines. CocoIndex built its early community entirely on this use case.
Krishiv already has the streaming, incremental, and Python-API foundations from
R13–R16. R17 adds the AI-specific operators and connectors.

### Features

**Vector Store Sink Connectors**

```python
# Qdrant
stream.sink(ks.sinks.qdrant(
    host="http://qdrant:6333",
    collection="documents",
    vector_field="embedding",
    payload_fields=["title", "url", "updated_at"],
))

# Pinecone
stream.sink(ks.sinks.pinecone(
    api_key=os.environ["PINECONE_API_KEY"],
    index="my-index",
    namespace="prod",
))

# pgvector (Postgres)
stream.sink(ks.sinks.pgvector(
    connection_string="postgresql://...",
    table="document_embeddings",
    vector_column="embedding",
    upsert_key="doc_id",
))

# Weaviate / LanceDB
stream.sink(ks.sinks.weaviate(url="http://weaviate:8080", class_name="Document"))
stream.sink(ks.sinks.lancedb(uri="s3://bucket/lance/", table="docs"))
```

**Embedding UDFs**

```python
# OpenAI
embedder = ks.embedders.openai(
    model="text-embedding-3-small",
    api_key=os.environ["OPENAI_API_KEY"],
    batch_size=100,
    rate_limit=500,  # requests/min
)

# HuggingFace (local)
embedder = ks.embedders.huggingface(
    model="BAAI/bge-small-en-v1.5",
    device="cpu",          # or "cuda", "mps"
    batch_size=32,
)

# Apply
embedded = docs.embed("content", embedder=embedder)
```

**RAG Pipeline High-Level API**

```python
# Define a live RAG index — updates automatically as source changes
index = ks.rag_index(
    source=ks.sources.iceberg("s3://warehouse/documents"),
    embedder=ks.embedders.openai("text-embedding-3-small"),
    vector_store=ks.sinks.qdrant("http://qdrant:6333", collection="docs"),
    chunker=ks.chunkers.recursive_text(chunk_size=512, overlap=50),
    refresh=ks.Trigger.on_change(),
)
await index.build_async()

# Query the index
results = await index.query(
    "What is the refund policy?",
    top_k=5,
    filters={"category": "support"},
)
```

**Text Chunking Operators**

```python
docs.chunk(ks.chunkers.recursive_text(chunk_size=512, overlap=50))
docs.chunk(ks.chunkers.sentence(max_sentences=5))
docs.chunk(ks.chunkers.token_aware(model="cl100k_base", max_tokens=512))
docs.chunk(ks.chunkers.markdown_section())   # chunk by ## headings
```

**LLM UDFs**

```python
@ks.llm_udf(
    model="gpt-4o-mini",
    prompt="Classify this text into one of: [positive, negative, neutral]. Text: {text}",
    output_type=str,
    cache=True,             # memoize identical inputs
    rate_limit=1000,        # requests/min
)
def classify_sentiment(text: str) -> str: ...

events = events.with_column("sentiment", classify_sentiment(ks.col("review_text")))
```

**Hybrid Batch+Stream Feature Store**

```python
# Backfill historical features in batch, then switch to stream for live updates
feature_store = ks.feature_store(
    backfill=ks.read_parquet("s3://historical/events/"),
    live=ks.read_kafka("events", servers="kafka:9092"),
    features={
        "user_30d_spend": ks.window_agg(
            "amount", window=ks.tumbling("30d"), agg=ks.sum
        ),
        "user_avg_order": ks.window_agg(
            "amount", window=ks.tumbling("30d"), agg=ks.avg
        ),
    },
    output=ks.sinks.redis(host="redis:6379", key_prefix="user:"),
)
```

**Semantic Deduplication**

```python
# Remove near-duplicate documents using cosine similarity of embeddings
deduped = embedded.semantic_dedup(
    vector_field="embedding",
    threshold=0.95,
    keep=ks.DeduplicationPolicy.LATEST,
)
```

### Acceptance Gate

- RAG pipeline test: 10k documents → Qdrant; query returns correct top-5 results.
- Embedding test: `ks.embedders.huggingface` produces identical vectors across
  two runs on the same input.
- LLM UDF test: memoized UDF called twice on the same input makes exactly one
  API call.
- Feature store test: backfill produces identical aggregate results to the live
  stream path after convergence.
- All four vector store sinks (Qdrant, Pinecone, pgvector, Weaviate) pass
  certification tests.

---

## R18: Storage Format Unification & Time Travel

### Goal

Unify Krishiv's lakehouse storage story: read and write Delta Lake, Apache Hudi,
and all Iceberg catalog flavours. Add time travel SQL, `MERGE INTO` DML, and
schema registry integration. An enterprise team should be able to connect Krishiv
to any modern lakehouse without format-specific workarounds.

### Why Now

Enterprise lakehouses are multi-format. Databricks teams use Delta, AWS teams
use Iceberg, Cloudera teams use Hudi. Krishiv's R8 Iceberg support is a start
but not sufficient for a team migrating from a Delta Lake environment. This
release unifies all three formats under one API.

### Features

**Delta Lake Read/Write**

```python
# Read Delta table
df = session.read_delta("s3://bucket/orders-delta", version=42)

# Write Delta table
df.write_delta(
    "s3://output/orders-delta",
    mode="merge",
    merge_key="order_id",
    schema_evolution=True,
)

# SQL
session.sql("SELECT * FROM delta.`s3://bucket/orders-delta`")
session.sql("""
    MERGE INTO delta.`s3://output/orders-delta` t
    USING staging s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Apache Hudi Read Support**

```python
df = session.read_hudi("s3://bucket/trips-hudi", query_type="snapshot")
df = session.read_hudi("s3://bucket/trips-hudi", query_type="incremental",
                       begin_instant="20240101000000")
```

**Iceberg REST Catalog**

```python
# AWS Glue
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    catalog=ks.catalogs.glue(region="us-east-1", database="analytics"),
)

# Tabular / Nessie
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    catalog=ks.catalogs.nessie(uri="http://nessie:19120/api/v1", ref="main"),
)

# Any Iceberg REST endpoint
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    catalog=ks.catalogs.iceberg_rest("https://catalog.example.com"),
)
```

**Time Travel SQL**

```sql
-- By timestamp
SELECT * FROM orders TIMESTAMP AS OF '2025-01-01 00:00:00';
SELECT * FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-01';

-- By snapshot ID (Iceberg)
SELECT * FROM orders VERSION AS OF 8473628394;

-- By Delta version
SELECT * FROM delta.`s3://bucket/orders` VERSION AS OF 42;
```

```python
# Python API
historical = session.read_iceberg("orders", as_of="2025-01-01")
historical = session.read_delta("s3://orders-delta", version=42)
```

**`MERGE INTO` Statement**

```sql
MERGE INTO target_table t
USING source_table s
ON t.id = s.id
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET amount = s.amount, updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (id, amount, created_at) VALUES (s.id, s.amount, current_timestamp())
```

**Schema Registry Integration**

```python
stream = ks.read_kafka(
    servers="kafka:9092",
    topic="orders",
    schema=ks.schema_registry.confluent(
        url="http://schema-registry:8081",
        subject="orders-value",
        format="avro",         # or "protobuf", "json"
    ),
)
```

**Iceberg Partition Evolution** (R8.2 deferred)

Support adding, dropping, and replacing partition specs on existing Iceberg
tables without full table rewrites.

### Acceptance Gate

- Delta Lake round-trip: write 1M rows → read back → verify exact count and content.
- Hudi incremental query: read only rows changed since a given instant.
- Time travel: query an Iceberg table at 5 historical timestamps; each returns
  the correct snapshot.
- `MERGE INTO`: upsert 100k rows into an Iceberg table; verify no duplicates.
- Schema registry: Avro-encoded Kafka topic is deserialized into the correct
  Arrow schema.

---

## R19: Multi-Region, Autoscaling & Cloud-Native

### Goal

Deliver global-scale production deployment: multi-region coordinator federation,
KEDA-based autoscaling, spot/preemptible instance recovery, bare-metal HA via
etcd, and cost-aware job placement. An enterprise running jobs across AWS, GCP,
and Azure should be able to manage them from a single Krishiv control plane.

### Why Now

R8–R11 established the single-region Kubernetes deployment story. Enterprise
customers operating at global scale need multi-region failover, autoscaling that
responds to streaming lag, and cost optimisation across cloud providers. Pathway
and Sail are single-region at this stage; this is a differentiation window.

### Features

**Multi-Region Coordinator Federation**

```python
session = ks.Session.connect(
    coordinators={
        "us-east-1": "http://coord-us:7070",
        "eu-west-1": "http://coord-eu:7070",
        "ap-southeast-1": "http://coord-ap:7070",
    },
    routing=ks.RoutingPolicy.nearest(latency_threshold_ms=50),
    failover=ks.FailoverPolicy.automatic(rpo_seconds=30, rto_seconds=60),
)
```

**KEDA Autoscaling**

```yaml
# k8s/helm/values.yaml
autoscaling:
  enabled: true
  provider: keda
  triggers:
    - type: kafka
      topic: orders
      lagTarget: 1000          # scale up when lag > 1000 messages
    - type: prometheus
      query: krishiv_executor_cpu_usage > 0.8
  minReplicas: 2
  maxReplicas: 50
```

```python
# Python API
job = session.submit_job(
    pipeline,
    autoscale=ks.AutoscalePolicy(
        min_executors=2,
        max_executors=50,
        scale_metric=ks.ScaleMetric.source_lag(target_lag="30s"),
        scale_down_delay="5m",
    ),
)
```

**Spot/Preemptible Instance Recovery**

```python
placement = ks.PlacementPolicy(
    node_preferences=["spot", "on-demand"],   # prefer spot, fall back
    checkpoint_on_preemption=True,            # checkpoint before eviction
    max_preemption_fraction=0.5,              # never more than 50% spot
    recovery_timeout="2m",                    # restart tasks within 2m
)
```

**Bare-Metal HA (etcd-backed)** (R9 bare-metal HA deferred)

```bash
# etcd for bare-metal leader election
krishiv-coordinator \
  --listen 0.0.0.0:7070 \
  --ha-mode etcd \
  --etcd-endpoints http://etcd-1:2379,http://etcd-2:2379 \
  --data-dir ./meta
```

**Cost-Aware Job Placement**

```python
placement = ks.PlacementPolicy(
    budget=ks.BudgetConstraint(
        max_hourly_cost_usd=10.0,
        cloud_provider="aws",
        region="us-east-1",
    ),
    optimize_for=ks.OptimizationGoal.COST,   # or LATENCY, THROUGHPUT
)
```

**Global Job Routing**

```python
# Route batch jobs to cheapest region, streaming to lowest-latency
session.submit_job(
    pipeline,
    routing=ks.JobRoutingPolicy(
        batch_jobs="cheapest_region",
        streaming_jobs="lowest_latency",
        data_locality=True,    # prefer region where data lives
    ),
)
```

**Serverless Execution Mode**

Short-lived batch jobs can run the coordinator as a serverless function (AWS
Lambda, Google Cloud Run) rather than a long-lived process:

```python
# Lambda handler
def handler(event, context):
    session = ks.Session.serverless(runtime="aws_lambda")
    result = session.sql("SELECT count(*) FROM parquet.`s3://bucket/data/`")
    return result.collect()
```

### Acceptance Gate

- Multi-region failover test: kill the active region's coordinator; verify the
  standby region takes over within the configured RTO.
- KEDA autoscaling test: inject lag spike → executor count scales up → lag
  reduces → executor count scales down.
- Spot recovery test: terminate a spot executor mid-checkpoint; verify the job
  resumes without data loss within 2 minutes.
- Cost-aware placement test: submit identical jobs with COST and LATENCY
  optimization; verify different instance-type selections.

---

## R20: Enterprise Platform & Ecosystem

### Goal

Deliver a complete enterprise data platform: self-serve portal, automated data
catalog with lineage, GDPR/HIPAA compliance tooling, SLA management, dbt-native
execution engine, and Helm + Terraform + cloud marketplace packaging for managed
service deployment.

### Why Now

OSS projects graduate to enterprise products when they deliver a complete
operational experience. At R20, Krishiv's compute capability will be mature
enough that the value gap shifts from "does it work?" to "can operations teams
run and govern it?". This release closes that gap.

### Features

**Self-Serve Data Portal**

A React-based web application shipping alongside the coordinator:

- Catalog browser: tables, schemas, column lineage, statistics.
- Job management: submit, cancel, inspect plans, view checkpoints.
- Live pipeline view: DAG visualisation with per-operator throughput and lag.
- Policy management: RBAC roles, column masking rules, row filters.
- Cost dashboard: per-job, per-namespace, per-team cost attribution.

**Data Catalog with Automated Lineage**

```python
# Lineage is recorded automatically as jobs run
lineage = session.catalog().lineage()

# Query lineage graph
upstream = lineage.for_table("daily_revenue").upstream(depth=3)
downstream = lineage.for_column("daily_revenue.revenue").downstream()

# Export to OpenMetadata / DataHub
lineage.export(format="openmetadata", target="http://openmetadata:8585/api")
lineage.export(format="datahub", target="http://datahub:8080")
```

**GDPR / CCPA Compliance Tooling**

```python
# Data retention policy
session.set_retention_policy(
    table="user_events",
    ttl_days=365,
    delete_strategy=ks.DeletionStrategy.HARD_DELETE,  # or SOFT_DELETE, ANONYMIZE
    compliance=ks.Compliance.GDPR,
)

# Right-to-erasure pipeline
job = session.submit_erasure_job(
    user_id="user-42",
    tables=["user_events", "orders", "profiles"],
    verify_completeness=True,   # audit trail of what was deleted
)

# Data classification scan
classification = session.scan_classification(
    table="customer_data",
    classifiers=[ks.classifiers.PII, ks.classifiers.PHI, ks.classifiers.PCI],
)
```

**SOC2 / HIPAA Audit Trails**

```python
# Tamper-evident audit log
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    audit=ks.AuditConfig(
        backend=ks.audit_backends.immutable_s3(
            bucket="s3://audit-logs/krishiv/",
            encrypt=True,
            hash_chaining=True,    # each entry hashes the previous
        ),
        log_query_results=False,   # log metadata only, not data
        compliance=ks.Compliance.SOC2 | ks.Compliance.HIPAA,
    ),
)
```

**SLA Management**

```python
job = session.submit_job(
    pipeline,
    sla=ks.SLA(
        max_processing_lag="5m",
        max_checkpoint_age="10m",
        breach_action=ks.BreachAction.alert(channel="pagerduty://svc-id"),
        critical_breach_action=ks.BreachAction.restart_pipeline(),
    ),
)

# View SLA status
sla_status = await session.jobs.sla_status("streaming-job-1")
# → SLAStatus(current_lag="2m", breaches_24h=0, health=HEALTHY)
```

**dbt-Native Execution Engine**

```yaml
# Enable Krishiv as a dbt materialization engine
# dbt_project.yml
models:
  my_project:
    +materialized: krishiv_incremental   # custom materialization
    +krishiv_trigger: on_change          # incremental on CDC change
```

**OpenMetadata / DataHub Integration**

```python
# Push catalog and lineage events automatically
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    catalog_sync=ks.CatalogSync(
        openmetadata_url="http://openmetadata:8585",
        datahub_url="http://datahub:8080",
        sync_interval="5m",
    ),
)
```

**Managed Service Packaging**

- Helm chart with production defaults (HA, TLS, RBAC, resource limits).
- Terraform modules: AWS EKS, GCP GKE, Azure AKS.
- AWS Marketplace AMI with one-click coordinator + executor cluster.
- Docker Compose quick-start for single-machine evaluation.
- `krishiv-cloud` CLI for managed service provisioning.

```bash
# One-command managed cluster
krishiv-cloud cluster create \
  --name prod \
  --region us-east-1 \
  --cloud aws \
  --coordinator-size m5.xlarge \
  --executor-count 10 \
  --executor-size r5.4xlarge
```

**Multi-Tenant Namespace Isolation**

```python
# Hardware-isolated tenant namespaces
session = ks.Session.connect(
    coordinator="http://coordinator:7070",
    namespace="team-analytics",
    isolation=ks.IsolationLevel.PROCESS,  # or NETWORK, CGROUP, VM
)
```

### Acceptance Gate

- Data portal: a non-engineer can discover a table, view its column lineage, and
  submit a SQL job through the UI without CLI access.
- GDPR erasure: erasure job deletes all rows for a given `user_id` from all
  configured tables and produces a verifiable audit record.
- SLA breach: inject artificial lag spike; PagerDuty alert fires within 2 minutes.
- dbt `dbt run --full-refresh` + `dbt run` (incremental) both complete
  successfully with Krishiv adapter.
- Helm install on a fresh `kind` cluster: all components healthy in < 5 minutes.

---

## Cross-Cutting Concerns (All Releases)

### Deferred Scope Addressed Across R12–R20

| Deferred Item | Addressed In |
|--------------|-------------|
| AQE coalescing (R4b) | R12 |
| LZ4/Zstd shuffle compression (R4c) | R12 |
| Full gRPC barrier transport (R6b) | R16 |
| Remote coordinator CLI | R12 |
| rdkafka Kafka source | R12 |
| Python Stream binding | R13 |
| maturin/PyPI wheels | R13 |
| `.pyi` stubs | R13 |
| Iceberg partition evolution | R18 |
| Iceberg time travel SQL | R18 |
| Delta Lake | R18 |
| Incremental materialized views | R14 |
| Multi-table CDC fan-out | R14 |
| Schema evolution | R14 |
| Exactly-once all pairs | R16 |
| RocksDB incremental checkpoints | R16 |
| State rescaling | R16 |
| Spark API compatibility | R15 |
| dbt adapter | R15 |
| Multi-region HA | R19 |
| Bare-metal HA (etcd) | R19 |
| KEDA autoscaling | R19 |
| Data catalog with lineage | R20 |
| GDPR compliance | R20 |
| Managed service packaging | R20 |

### Audit Items P0 (21) and P1 (28) — Target Releases

| Priority | Count | Target |
|----------|-------|--------|
| P0 (crash / data loss / security) | 21 | **All in R12** |
| P1 (wrong behaviour / broken contracts) | 28 | R12 (8 high-value), R13–R14 (remainder) |
| P2 (performance) | 16 | R12–R13 (hot-path items), remainder in relevant release |
| P3 (cleanup / dead code) | 25 | Folded into same-file changes across R12–R16 |

### Test Strategy

Each release adds to a cumulative regression suite:

- R12: P0/P1 regression tests, Kafka integration tests, remote CLI E2E.
- R13: Python API conformance tests, wheel smoke tests across platforms.
- R14: CDC exactly-once tests, schema evolution fuzz tests, live-table lag tests.
- R15: Spark TPC-H compatibility suite, dbt adapter tests.
- R16: CEP correctness, temporal join correctness, exactly-once all pairs.
- R17: AI connector certification, embedding determinism, RAG recall@5.
- R18: Format round-trip correctness, time travel correctness, MERGE INTO tests.
- R19: Multi-region failover, KEDA scaling, spot recovery.
- R20: SLA breach alerting, GDPR erasure verifiability, portal accessibility.

### Stability Policy

From R12 onward, the `1.x` public API surface (Rust `krishiv-api`, Python
`krishiv`) follows SemVer. Breaking changes require a major version bump and a
minimum 2-release deprecation notice. Internal crates (`krishiv-scheduler`,
`krishiv-exec`, etc.) are allowed to evolve without SemVer guarantees.

---

## Open Questions (Require ADR Before Implementation)

| Question | Needed Before | Options |
|----------|--------------|---------|
| Incremental view refresh storage: store deltas in Iceberg or in a separate change log? | R14 | Iceberg equality-delete files vs. Kafka change-topic |
| Spark Connect protocol: implement natively or use the Spark Connect proto directly? | R15 | Native impl (full control) vs. Spark proto (zero compat code) |
| CEP engine: implement in Rust from scratch or embed NFA-based matching from a library? | R16 | Rust NFA impl vs. `cereal` / custom |
| LLM UDF execution isolation: in-process (fast, risky) or subprocess (safe, overhead)? | R17 | `spawn_blocking` with timeout vs. subprocess with IPC |
| Delta Lake: `delta-rs` crate (mature) or reimplement natively? | R18 | `delta-rs` (recommended, community maintained) |
| Multi-region metadata: Raft log replication (strong) or async replication (available)? | R19 | `openraft` vs. CRDTs vs. Postgres streaming replication |
| SaaS isolation: process-level (simpler) or VM-level (stronger, expensive)? | R20 | K8s namespaces + NetworkPolicy vs. separate K8s clusters |
