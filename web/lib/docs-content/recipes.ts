import type { DocPage } from '../docs-data';
import { DIAGRAM_CDC_ICEBERG } from './diagrams';

export const recipesPages: DocPage[] = [
  {
    slug: 'recipes',
    group: 'Recipes',
    title: 'Recipes — "I want to…"',
    description: 'Task-oriented examples for the things you actually do with Krishiv.',
    status: 'Available',
    body: `
<p>Each recipe is a working example with status labels. Use them as starting points — every recipe links to the full reference page for deeper detail.</p>

<h2 id="batch">Batch and SQL</h2>
<table class="api-table">
  <thead><tr><th>I want to…</th><th>Recipe</th><th>Status</th></tr></thead>
  <tbody>
    <tr><td>Run a SQL aggregation over a Parquet file</td><td><a href="/docs/latest/recipes/parquet-aggregation">Parquet → SQL aggregation</a></td><td>Available</td></tr>
    <tr><td>Filter, group, and sort with the DataFrame API</td><td><a href="/docs/latest/recipes/dataframe-101">DataFrame 101</a></td><td>Available</td></tr>
    <tr><td>Register a UDF and call it from SQL</td><td><a href="/docs/latest/recipes/sql-udf">SQL UDFs</a></td><td>Available</td></tr>
  </tbody>
</table>

<h2 id="streaming">Streaming</h2>
<table class="api-table">
  <thead><tr><th>I want to…</th><th>Recipe</th><th>Status</th></tr></thead>
  <tbody>
    <tr><td>Compute tumbling-window counts over a stream</td><td><a href="/docs/latest/recipes/tumbling-window">Tumbling window aggregation</a></td><td>Available</td></tr>
    <tr><td>Read from Kafka and write to Parquet</td><td><a href="/docs/latest/recipes/kafka-to-parquet">Kafka → Parquet pipeline</a></td><td>Preview</td></tr>
    <tr><td>Maintain a stateful process function per key</td><td><a href="/docs/latest/recipes/stateful-process">Stateful process function</a></td><td>Available</td></tr>
  </tbody>
</table>

<h2 id="lakehouse">Lakehouse</h2>
<table class="api-table">
  <thead><tr><th>I want to…</th><th>Recipe</th><th>Status</th></tr></thead>
  <tbody>
    <tr><td>Upsert into an Iceberg table from a source</td><td><a href="/docs/latest/recipes/iceberg-upsert">Iceberg upsert with MERGE INTO</a></td><td>Preview</td></tr>
    <tr><td>Time-travel read on an Iceberg table</td><td><a href="/docs/latest/recipes/iceberg-time-travel">Iceberg time travel</a></td><td>Preview</td></tr>
    <tr><td>Run a live ingestion table queryable from SQL</td><td><a href="/docs/latest/recipes/live-table">Live table ingestion</a></td><td>Experimental</td></tr>
  </tbody>
</table>

<h2 id="operations">Operations</h2>
<table class="api-table">
  <thead><tr><th>I want to…</th><th>Recipe</th><th>Status</th></tr></thead>
  <tbody>
    <tr><td>Deploy Krishiv on a single host with durable state</td><td><a href="/docs/latest/recipes/single-node-deploy">Single-node durable deployment</a></td><td>Available</td></tr>
    <tr><td>Build an exactly-once pipeline (certified combo)</td><td><a href="/docs/latest/recipes/exactly-once-pipeline">Exactly-once pipeline</a></td><td>Preview</td></tr>
    <tr><td>See what a streaming job is doing right now</td><td><a href="/docs/latest/recipes/observe-job">Observing a running job</a></td><td>Preview</td></tr>
  </tbody>
</table>

<div class="note-box"><strong>Tip:</strong> Don't see your task? Check the <a href="/docs/latest">full docs index</a> or open an issue on <a href="https://github.com/krishiv-data/krishiv">GitHub</a>.</div>
`,
  },

  {
    slug: 'recipes/parquet-aggregation',
    group: 'Recipes',
    title: 'Parquet → SQL aggregation',
    description: 'Read a Parquet file, run a SQL aggregation, write to a new Parquet file.',
    status: 'Available',
    body: `
<p>The shortest path from "I have a Parquet file" to "I have a result." Uses the embedded runtime — no cluster required.</p>

<h2 id="python">Python</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()

orders = session.read_parquet("data/orders.parquet")
session.register_parquet("orders", "data/orders.parquet")

result = session.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    WHERE event_time >= CURRENT_DATE - INTERVAL '30' DAY
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 100
""")
result.write_parquet("out/top_customers.parquet")
result.show(10)
</code></pre>

<h2 id="rust">Rust</h2>
<pre><code class="language-rust">use krishiv_api::{col, lit, sum, Session};

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    session.register_parquet("orders", "data/orders.parquet").await?;

    let df = session.sql("
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        WHERE event_time &gt;= CURRENT_DATE - INTERVAL '30' DAY
        GROUP BY customer_id
        ORDER BY total DESC
        LIMIT 100
    ").await?;
    df.write_parquet("out/top_customers.parquet", None).await?;
    df.show().await?;
    Ok(())
}
</code></pre>

<h2 id="notes">Notes</h2>
<ul>
  <li>The internal data model is <strong>Apache Arrow RecordBatch</strong>; reading and writing both go through it.</li>
  <li>SQL parsing and execution are delegated to <strong>DataFusion</strong>; you can use any standard DataFusion SQL syntax.</li>
  <li>For larger files, prefer reading a directory of partitioned Parquet files instead of one giant file.</li>
</ul>
<p>See the <a href="/docs/latest/python/session">Python Session</a> and <a href="/docs/latest/connectors/parquet">Parquet &amp; Object Store</a> pages for full options.</p>
`,
  },

  {
    slug: 'recipes/dataframe-101',
    group: 'Recipes',
    title: 'DataFrame 101 — filter, group, sort',
    description: 'Use the typed DataFrame API instead of SQL.',
    status: 'Available',
    body: `
<p>The DataFrame API is the typed, chainable alternative to SQL. Use whichever fits the call site — both go through the same planner.</p>

<h2 id="python">Python</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv.functions import col, lit, sum, count

session = ks.Session.embedded()

top = (session.read_parquet("data/orders.parquet")
    .filter(col("status") == lit("paid"))
    .group_by(["region", "category"])
    .agg([sum(col("amount")).alias("total"),
          count(col("*")).alias("n")])
    .order_by(["total"], ascending=False)
    .limit(20))
top.show()
</code></pre>

<h2 id="rust">Rust</h2>
<pre><code class="language-rust">use krishiv_api::{col, count_all, lit, sum, Session};

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let df = session.read_parquet("data/orders.parquet").await?
        .filter(col("status").eq(lit("paid")))?
        .group_by(vec![col("region"), col("category")])?
        .agg(vec![
            sum(col("amount")).alias("total"),
            count_all().alias("n"),
        ])?
        .sort(vec![col("total").desc()])?
        .limit(20);
    df.show().await?;
    Ok(())
}
</code></pre>

<h2 id="sql-vs-df">SQL or DataFrame?</h2>
<table class="api-table">
  <thead><tr><th>Use SQL when…</th><th>Use DataFrame when…</th></tr></thead>
  <tbody>
    <tr><td>The query is one-off, ad-hoc, or shared with analysts.</td><td>You are composing a pipeline programmatically.</td></tr>
    <tr><td>You want to keep the query string portable.</td><td>You want compile-time checking of column names and types.</td></tr>
    <tr><td>You need window functions or <code>MATCH_RECOGNIZE</code>.</td><td>You are building a library or framework on top of Krishiv.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'recipes/sql-udf',
    group: 'Recipes',
    title: 'SQL UDFs — register and call a Python function from SQL',
    description: 'Expose a Python or Rust callable as a SQL function.',
    status: 'Available',
    body: `
<h2 id="python">Python (scalar UDF)</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()

@ks.udf(return_type="utf8")
def greet(name: str) -&gt; str:
    return f"hello, {name}"

session.register_udf("greet", greet, ["utf8"], "utf8")

session.sql("SELECT greet(name) FROM users").show()
</code></pre>
<div class="note-box">If you are using DataFusion-style registration, the supported call is <code>session.register_udf(name, fn, input_types, return_type)</code>. See <a href="/docs/latest/python/session">Python Session</a>.</div>

<h2 id="rust">Rust (scalar UDF)</h2>
<pre><code class="language-rust">use krishiv_api::{col, Session};
use datafusion::logical_expr::Volatility;
use datafusion::functions_nested;

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    // Register a closure-based scalar UDF
    session.register_table_udf_fn(
        "shout",
        std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        |_args| Ok(arrow::array::RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty()))),
    )?;
    Ok(())
}
</code></pre>
<div class="note-box">Most production UDFs are written as DataFusion <code>ScalarUDF</code> implementations and registered via <code>session.register_udf(udf)</code>. See the <a href="/docs/latest/rust/expressions">Rust Expressions</a> page for the patterns.</div>

<h2 id="tvf">SQL-body table-valued function</h2>
<p>For pure-SQL TVFs you do not need to register anything in code — see the <a href="/docs/latest/sql/udf-sql">SQL UDFs</a> reference.</p>
`,
  },

  {
    slug: 'recipes/tumbling-window',
    group: 'Recipes',
    title: 'Tumbling window aggregation',
    description: 'Group a stream into fixed-size, non-overlapping time windows.',
    status: 'Available',
    body: `
<p>Tumbling windows partition a stream into fixed-size buckets aligned to the epoch. Use them for "events per minute", "errors per hour", etc.</p>

<h2 id="sql">SQL</h2>
<pre><code class="language-sql">SELECT
  tumble_start(event_time, INTERVAL '1 minute') AS window_start,
  tumble_end(event_time,   INTERVAL '1 minute') AS window_end,
  COUNT(*) AS events
FROM events
GROUP BY tumble_start(event_time, INTERVAL '1 minute'),
         tumble_end(event_time,   INTERVAL '1 minute');
</code></pre>

<h2 id="python">Python (Stream API)</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv.functions import count, sum, col

session = ks.Session.embedded()
schema = ...  # PyArrow schema: event_time timestamp, user_id utf8, amount float64
stream, sender = session.memory_stream(schema)

windowed = (stream
    .watermark("event_time", 5000)        # 5s allowed lateness
    .key_by("user_id")
    .tumbling_window(60_000)              # 1-minute windows
    .agg([count(col("*")).alias("events"),
          sum(col("amount")).alias("total")]))

# Push events and collect results
import pyarrow as pa
sender.send(pa.record_batch([...]))
print(windowed.try_next())  # next windowed aggregate
</code></pre>

<h2 id="rust">Rust (Stream API)</h2>
<pre><code class="language-rust">use krishiv_api::{col, count, sum, Session};

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let (stream, sender) = session.memory_stream(schema)?;

    let windowed = stream
        .watermark("event_time", 5_000)?
        .key_by("user_id")?
        .tumbling_window(60_000)
        .agg(vec![count(col("*")), sum(col("amount"))]);
    Ok(())
}
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/window-functions">Window Functions</a> — <code>TUMBLE</code>, <code>HOP</code>, <code>SESSION</code> helpers in SQL</li>
  <li><a href="/docs/latest/python/stream">Python Stream &amp; Windows</a></li>
  <li><a href="/docs/latest/rust/stream">Rust Stream &amp; KeyedStream</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/kafka-to-parquet',
    group: 'Recipes',
    title: 'Kafka → Parquet pipeline',
    description: 'Read a Kafka topic, run a streaming aggregation, write the result to Parquet.',
    status: 'Preview',
    body: `
<div class="warn-box"><strong>Preview:</strong> The Kafka source and Parquet sink are implemented. End-to-end certification depends on the durability profile and certified source/sink combination — see <a href="/docs/latest/connectors">Connectors</a>.</div>

<h2 id="requirements">Requirements</h2>
<ul>
  <li>Enable the <code>kafka</code> Cargo feature (or <code>maturin develop --features kafka</code> for Python).</li>
  <li>Use the <code>single-node-durable</code> or <code>distributed-durable</code> profile for at-least-once delivery.</li>
</ul>

<h2 id="ddl">Declare with SQL DDL</h2>
<pre><code class="language-sql">CREATE SOURCE orders_raw
TYPE KAFKA
OPTIONS (
  'brokers'           = 'broker1:9092,broker2:9092',
  'topic'             = 'orders',
  'group.id'          = 'krishiv-orders',
  'auto.offset.reset' = 'latest',
  'format'            = 'json'
)
WITH SCHEMA (
  order_id   BIGINT   NOT NULL,
  customer   VARCHAR,
  amount     DOUBLE,
  event_time TIMESTAMP
);

CREATE SINK per_minute_totals
TYPE PARQUET
OPTIONS (
  'path'        = 's3://my-bucket/out/per_minute/',
  'compression' = 'zstd'
);

START PIPELINE orders_raw TO per_minute_totals
AS
SELECT
  tumble_start(event_time, INTERVAL '1 minute') AS window_start,
  customer,
  SUM(amount) AS total
FROM orders_raw
GROUP BY tumble_start(event_time, INTERVAL '1 minute'), customer;
</code></pre>

<h2 id="python">Python equivalent</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.local()  # single-node daemon
session.register_kafka_source(
    "orders_raw", schema,
    brokers="broker1:9092",
    topic="orders",
    group="krishiv-orders",
)

(session.read_stream()
    .format("kafka")
    .option("topic", "orders")
    .load()
    .group_by(window("event_time", "1 minute"), "customer")
    .agg(sum("amount").alias("total"))
    .write_stream()
    .format("parquet")
    .option("path", "s3://my-bucket/out/per_minute/")
    .option("checkpoint", "s3://my-bucket/checkpoints/per_minute/")
    .start())
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/kafka">Kafka Connector</a> — full option reference</li>
  <li><a href="/docs/latest/connectors/parquet">Parquet &amp; Object Store</a></li>
  <li><a href="/docs/latest/operations/checkpointing">Checkpointing</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/stateful-process',
    group: 'Recipes',
    title: 'Stateful process function',
    description: 'Run a per-key process function with ValueState.',
    status: 'Available',
    body: `
<p>Process functions run per record with access to keyed state. Use them for running counts, de-duplication, pattern tracking, and per-key enrichment.</p>

<h2 id="python">Python</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv import apply_process_function, ProcessContext, ValueState

def count_per_key(ctx: ProcessContext, batch, state: ValueState):
    seen = state.get_json() or 0
    seen += batch.num_rows
    state.set_json(seen)
    ctx.emit(batch)

session = ks.Session.embedded()
stream, sender = session.memory_stream(schema)

keyed = stream.key_by("user_id")
out = apply_process_function(keyed, count_per_key, ValueState("seen"))
</code></pre>
<div class="note-box">State is keyed by the <code>key_by</code> partition. <code>ValueState</code> stores one value per key. <code>MapState</code> and <code>ListState</code> are also available — see <a href="/docs/latest/python/state">Python State</a>.</div>

<h2 id="state-durability">State durability</h2>
<p>State is backed by <code>krishiv-state</code> — in-memory under <code>dev-local</code>, RocksDB under <code>single-node-durable</code> and <code>distributed-durable</code>. State is restored from checkpoint on restart.</p>
`,
  },

  {
    slug: 'recipes/iceberg-upsert',
    group: 'Recipes',
    title: 'Iceberg upsert with MERGE INTO',
    description: 'Merge a stream of changes into an Iceberg table using copy-on-write.',
    status: 'Preview',
    body: `
<div class="warn-box"><strong>Preview:</strong> <code>MERGE INTO</code> on Iceberg uses copy-on-write. Merge-on-read and distributed atomic commit certification are ongoing. The target table must be registered under a <code>KrishivCatalog</code>.</div>

<h2 id="prereqs">Prerequisites</h2>
<ul>
  <li>Enable the <code>iceberg</code> Cargo/Python feature.</li>
  <li>Register a REST catalog (see <a href="/docs/latest/connectors/iceberg">Iceberg connector</a>).</li>
  <li>The target table must exist in the catalog.</li>
</ul>

<h2 id="sql">SQL</h2>
<pre><code class="language-sql">MERGE INTO my_catalog.warehouse.inventory AS tgt
USING incoming_stock AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND src.quantity = 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET tgt.quantity = tgt.quantity + src.quantity
WHEN NOT MATCHED THEN INSERT (product_id, quantity)
VALUES (src.product_id, src.quantity);
</code></pre>

<h2 id="python">Python (in-process MemoryLakehouseTable for tests)</h2>
<pre><code class="language-python">import krishiv as ks
import pyarrow as pa

session = ks.Session.embedded()
table = ks.MemoryLakehouseTable(pa.schema([("product_id", pa.int64()), ("quantity", pa.int64())]))
table.append(pa.record_batch([(1, 10), (2, 20)], schema=table.schema()))

# Apply an update
table.update_where("product_id = 1", {"quantity": 99})
print(table.snapshot_rows())  # 2
</code></pre>
<p>For production Iceberg tables, use the SQL <code>MERGE INTO</code> form above — the <code>MemoryLakehouseTable</code> is for tests and local exploration only.</p>
`,
  },

  {
    slug: 'recipes/iceberg-time-travel',
    group: 'Recipes',
    title: 'Iceberg time travel',
    description: 'Read an Iceberg table as it existed at a point in time.',
    status: 'Preview',
    body: `
<p>Krishiv uses <code>FOR SYSTEM_TIME AS OF</code> to select a historical snapshot. The closest snapshot at or before the given timestamp is returned.</p>

<pre><code class="language-sql">SELECT customer_id, SUM(amount) AS total
FROM my_catalog.warehouse.orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 00:00:00'
GROUP BY customer_id;
</code></pre>

<h2 id="notes">Notes</h2>
<ul>
  <li>Only Iceberg tables registered under a <code>KrishivCatalog</code> support time travel.</li>
  <li>The snapshot selection rule is "at or before" the given timestamp. If no snapshot exists at or before, the query errors.</li>
  <li>Multiple time-travel refs in the same query are resolved independently.</li>
</ul>
<p>See the <a href="/docs/latest/sql/as-of-queries">AS-OF Queries</a> reference for the full syntax and the <a href="/docs/latest/connectors/iceberg">Iceberg connector</a> for catalog setup.</p>
`,
  },

  {
    slug: 'recipes/live-table',
    group: 'Recipes',
    title: 'Live table ingestion',
    description: 'Ingest rows into a live table that is queryable from SQL.',
    status: 'Experimental',
    body: `
<p>Live tables are append-only tables backed by an in-memory change feed. They are immediately queryable from SQL — no batch refresh required.</p>
<div class="warn-box"><strong>Experimental:</strong> Live tables are functional but the API and storage backend may change. Not certified for production use.</div>

<pre><code class="language-python">import krishiv as ks
import pyarrow as pa

session = ks.Session.embedded()
session.sql("CREATE LIVE TABLE sensor_readings")

lt = session.live_table("sensor_readings")
lt.ingest_row({"sensor_id": "s1", "value": 23.4, "ts": 1700000000})
lt.ingest_row({"sensor_id": "s2", "value": 22.1, "ts": 1700000001})
lt.refresh()

session.sql("SELECT * FROM sensor_readings ORDER BY ts").show()
</code></pre>

<h2 id="change-feed">Reading the change feed</h2>
<pre><code class="language-python">for record in lt.change_feed():
    print(record)
</code></pre>
<p>See <a href="/docs/latest/sql/live-tables">Live Tables (SQL)</a> for the DDL form and <a href="/docs/latest/python/lakehouse">Python Lakehouse</a> for the full API.</p>
`,
  },

  {
    slug: 'recipes/single-node-deploy',
    group: 'Recipes',
    title: 'Single-node durable deployment',
    description: 'Run Krishiv as a local daemon with RocksDB state and local shuffle.',
    status: 'Available',
    body: `
<p>The single-node daemon gives you restart-durable state and shuffle on a single host. It is the right starting point for production-style deployments that do not yet need a cluster.</p>

<h2 id="build">Build</h2>
<pre><code class="language-bash"># GCC 15 hosts need this workaround for rocksdb
CXXFLAGS="-include cstdint" cargo build -p krishiv --features single-node --release
</code></pre>

<h2 id="start">Start the daemon</h2>
<pre><code class="language-bash">./target/release/krishiv server start \
  --coordinator-addr 0.0.0.0:50051 \
  --durability single-node-durable \
  --checkpoint-dir /var/krishiv/checkpoints
</code></pre>

<h2 id="connect">Connect from a client</h2>
<pre><code class="language-bash">export KRISHIV_COORDINATOR=http://localhost:50051
</code></pre>
<pre><code class="language-python">import krishiv as ks
session = ks.Session.from_env()  # reads KRISHIV_COORDINATOR
</code></pre>
<pre><code class="language-rust">use krishiv_api::Session;
let session = Session::from_env().await?;
</code></pre>

<h2 id="profiles">What each durability profile gives you</h2>
<table class="api-table">
  <thead><tr><th>Profile</th><th>State</th><th>Shuffle</th><th>Checkpoints</th></tr></thead>
  <tbody>
    <tr><td><code>dev-local</code></td><td>In-memory</td><td>In-memory</td><td>Ephemeral</td></tr>
    <tr><td><code>single-node-durable</code></td><td>RocksDB</td><td>Local disk</td><td>Local filesystem</td></tr>
    <tr><td><code>distributed-durable</code></td><td>RocksDB</td><td>Tiered (local + object store)</td><td>Object store + etcd</td></tr>
  </tbody>
</table>
<p>See the <a href="/docs/latest/operations/deployment">Deployment</a> and <a href="/docs/latest/concepts/execution-model">Execution Model</a> pages for the full picture.</p>
`,
  },

  {
    slug: 'recipes/exactly-once-pipeline',
    group: 'Recipes',
    title: 'Exactly-once pipeline',
    description: 'Build a pipeline with exactly-once delivery using a certified source/sink/checkpoint combination.',
    status: 'Preview',
    body: `
<div class="warn-box"><strong>Important:</strong> "Exactly-once" in Krishiv is a property of a <em>specific</em> source + sink + checkpoint combination, not a global guarantee. Use this recipe only with a certified combination; otherwise prefer <em>at-least-once</em> with an idempotent sink.</div>

<h2 id="what">What "exactly-once" means here</h2>
<p>The end-to-end delivery guarantee is the <em>weakest</em> guarantee supplied by the source, sink, checkpoint storage, and durability profile. Exactly-once requires all four:</p>
<table class="api-table">
  <thead><tr><th>Component</th><th>Requirement</th></tr></thead>
  <tbody>
    <tr><td>Source</td><td>Supports offset/position tracking (e.g. Kafka with <code>group.id</code>).</td></tr>
    <tr><td>Sink</td><td>Transactional or two-phase. Output is committed atomically with the checkpoint.</td></tr>
    <tr><td>Checkpoint storage</td><td>Atomic, fenced. Object store + etcd under <code>distributed-durable</code>.</td></tr>
    <tr><td>Coordinator</td><td>Fenced with an epoch token so stale completions are rejected.</td></tr>
  </tbody>
</table>

<h2 id="skeleton">Skeleton (Kafka → Iceberg, distributed-durable)</h2>
<pre><code class="language-bash">export KRISHIV_DURABILITY_PROFILE=distributed-durable
export KRISHIV_COORDINATOR=https://coord.internal:50051
export KRISHIV_COORDINATOR_BEARER_TOKEN=...
export KRISHIV_SHUFFLE_OBJECT_STORE_URI=s3://bucket/shuffle/
</code></pre>
<pre><code class="language-sql">-- A transactional Iceberg sink
CREATE SINK orders_eo
TYPE ICEBERG
OPTIONS (
  'catalog.uri' = 'http://catalog:8181',
  'warehouse'   = 's3://bucket/wh',
  'commit'      = 'transactional'
);

START PIPELINE orders_raw TO orders_eo
AS SELECT * FROM orders_raw;
</code></pre>

<h2 id="verify">Verify</h2>
<ul>
  <li>Force-kill an executor mid-pipeline; the coordinator fences it and restarts from the last committed checkpoint.</li>
  <li>Replay the source; the sink should not produce duplicate committed snapshots.</li>
  <li>Inspect commit metadata on the Iceberg table to confirm the snapshot lineage.</li>
</ul>
<p>See <a href="/docs/latest/connectors">Connectors</a> for the certified source/sink matrix and <a href="/docs/latest/operations/checkpointing">Checkpointing</a> for the protocol details.</p>
`,
  },

  {
    slug: 'recipes/observe-job',
    group: 'Recipes',
    title: 'Observing a running job',
    description: 'See what a streaming job is doing right now — status, progress, plan.',
    status: 'Preview',
    body: `
<h2 id="from-python">From Python</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.from_env()

for job in session.jobs():
    print(job.id(), job.name(), job.state())

# Or for a specific job handle
handle = session.submit_async(plan)
print(handle.status(), handle.progress())
</code></pre>

<h2 id="explain">Explain the plan</h2>
<pre><code class="language-python">df = session.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id")
print(df.explain())           # DataFusion logical + physical plan
print(df.explain_logical())   # logical plan only
</code></pre>

<h2 id="endpoints">HTTP endpoints (single-node or distributed)</h2>
<table class="api-table">
  <thead><tr><th>Path</th><th>Purpose</th></tr></thead>
  <tbody>
    <tr><td><code>GET /healthz</code></td><td>Coordinator liveness.</td></tr>
    <tr><td><code>GET /readyz</code></td><td>Coordinator readiness.</td></tr>
    <tr><td><code>GET /metrics</code></td><td>Prometheus metrics (when <code>KRISHIV_METRICS_PORT</code> is set).</td></tr>
  </tbody>
</table>

<h2 id="logs">Logs</h2>
<p>Set the log filter via <code>KRISHIV_LOG</code> (e.g. <code>info,krishiv_scheduler=debug</code>). The scheduler is the most common place to look first when a job is stuck.</p>
`,
  },

  {
    slug: 'recipes/stream-table-join',
    group: 'Recipes',
    title: 'Stream-Table Temporal Join',
    description: 'Enrich a Kafka stream with the latest version of a slowly-changing Iceberg dimension table.',
    status: 'Available',
    feature_flags: ['iceberg'],
    body: `
<p>The classic "click enrichment" pattern: a fast stream of events joined against a slowly-changing dimension table, with the correct historical version of the dimension row applied at the event's time.</p>

<h2 id="when">When to use</h2>
<p>You have a high-volume event stream (clicks, orders, telemetry) that needs enrichment from a slowly-changing reference dataset (users, products, geo, feature flags). Re-computing the dimension on every event is wasteful; reading the whole dimension on every join is wasteful too. Use a temporal join so each event picks the version of the dimension that was current at its event time.</p>

<h2 id="dim">Step 1 — Create the versioned dimension</h2>
<pre><code class="language-sql">-- Dimension with a validity window. validity_start is set on each write;
-- validity_end is open-ended for the current version.
CREATE TABLE users (
  user_id     BIGINT NOT NULL,
  tier        VARCHAR,
  country     VARCHAR,
  validity_start TIMESTAMP NOT NULL,
  validity_end   TIMESTAMP
);
</code></pre>
<p>Krishiv's temporal join uses <code>validity_start</code> and <code>validity_end</code> columns. The planner picks the version with <code>validity_start &lt;= event_time AND (validity_end IS NULL OR event_time &lt; validity_end)</code>.</p>

<h2 id="stream">Step 2 — Register the stream</h2>
<pre><code class="language-rust">let session = Session::embedded().await?;
session.register_kafka_source(
    "clicks",
    clicks_schema,
    "broker:9092",
    "clicks",
    "krishiv-app",
)?;
</code></pre>

<h2 id="join">Step 3 — Join</h2>
<pre><code class="language-rust">use krishiv_api::temporal_join;

let enriched = temporal_join(
    session.table("clicks")?.with_event_time("event_time")?,
    session.table("users")?,
    "event_time",
    &amp;["user_id"],
    /* inner = */ false,  // left outer
)?;
</code></pre>
<p>Or via SQL — the planner rewrites the temporal-as-of join automatically:</p>
<pre><code class="language-sql">SELECT c.event_time, c.url, u.tier, u.country
FROM clicks c
JOIN users u FOR SYSTEM_TIME AS OF c.event_time
  ON c.user_id = u.user_id;
</code></pre>

<h2 id="state">State and watermark</h2>
<p>Each executor keeps a <code>VersionedTableState</code> for the dimension. It's bounded by <code>max_versions_per_key</code> (default 8). Set a watermark on the stream so late events pick a stable dimension version:</p>
<pre><code class="language-rust">let stream = session.table("clicks")?
    .with_event_time("event_time")?
    .watermark("event_time", 5_000)?;  // 5 s allowed lateness
</code></pre>

<h2 id="perf">Performance</h2>
<ul>
<li>Per-key state grows as the dimension evolves; expect ~100 B per version per key.</li>
<li>For very wide dimensions, project the join columns: <code>SELECT c.*, u.tier, u.country FROM ...</code> instead of <code>SELECT *</code>.</li>
<li>If the dimension fits in memory, set <code>KRISHIV_DIMENSION_CACHE=1</code> to skip the version lookup.</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/streaming/joins">Streaming Joins</a></li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/connectors/kafka">Kafka</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/cdc-to-iceberg',
    group: 'Recipes',
    title: 'CDC to Iceberg',
    description: 'Stream Debezium-format change events from Kafka into an Iceberg table with exactly-once commits.',
    status: 'Available',
    feature_flags: ['iceberg', 'kafka'],
    body: `
<p>The CDC router bridges a Debezium-style Kafka topic (the standard for MySQL, Postgres, MongoDB change streams) into an Iceberg table. The destination is updated with <code>MERGE</code> semantics keyed by the row's primary key, so inserts, updates, and deletes all land in the right place.</p>
${DIAGRAM_CDC_ICEBERG}

<h2 id="topology">Topology</h2>
<pre><code class="language-text">MySQL  ──►  Debezium  ──►  Kafka (orders.cdc)  ──►  Krishiv CDC router  ──►  Iceberg (orders)
                                                                                   │
                                                                                   └─►  DLQ
</code></pre>

<h2 id="setup">Step 1 — Iceberg target</h2>
<pre><code class="language-sql">-- Create the table once (e.g. via Spark or the Iceberg CLI)
CREATE TABLE orders (
  order_id   BIGINT,
  user_id    BIGINT,
  amount     DOUBLE,
  status     VARCHAR,
  ts         TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts));
</code></pre>
<p>Register it in Krishiv via a catalog:</p>
<pre><code class="language-bash">export KRISHIV_ICEBERG_CATALOG_URI=http://catalog:8181
export KRISHIV_ICEBERG_WAREHOUSE=s3://my-bucket/warehouse
</code></pre>

<h2 id="config">Step 2 — Configure the CDC router</h2>
<pre><code class="language-rust">use krishiv_connectors::cdc::CdcRouter;
use krishiv_connectors::iceberg::IcebergSink;

let sink = IcebergSink::new(&amp;catalog_uri, &amp;warehouse, "orders")
    .with_two_phase_commit();

let router = CdcRouter::builder()
    .source_kafka("broker:9092", "orders.cdc", "krishiv-cdc")
    .sink(sink)
    .key_columns(&amp;["order_id"])
    .dlq_parquet("./dlq/")
    .build()?;
</code></pre>

<h2 id="cds">Step 3 — Run</h2>
<pre><code class="language-rust">router.run().await?;
</code></pre>
<p>Each Kafka record carries a Debezium envelope: <code>op</code> (one of <code>c</code>/<code>u</code>/<code>d</code>/<code>r</code>), <code>before</code>, <code>after</code>, and <code>ts_ms</code>. Krishiv rewrites them to SQL operations:</p>
<table class="api-table">
<thead><tr><th>Debezium op</th><th>Iceberg action</th></tr></thead>
<tbody>
<tr><td><code>c</code> (create)</td><td><code>INSERT</code></td></tr>
<tr><td><code>u</code> (update)</td><td><code>MERGE</code> (matched by <code>key_columns</code>)</td></tr>
<tr><td><code>d</code> (delete)</td><td><code>DELETE</code></td></tr>
<tr><td><code>r</code> (read)</td><td>no-op</td></tr>
</tbody>
</table>

<h2 id="eos">Step 4 — Verify exactly-once</h2>
<p>With the <code>iceberg</code> feature built and the coordinator running with <code>distributed-durable</code>, the CDC router uses two-phase commit. On failure mid-commit, the Iceberg snapshot is not renamed; the source Kafka offset is not committed. On restart, the router replays from the last committed offset and the same operations produce the same snapshot.</p>

<h2 id="dlq">Step 5 — Watch the DLQ</h2>
<p>Records that don't parse (missing <code>op</code>, schema mismatch) go to <code>./dlq/</code> as Parquet. Each row carries a <code>_dlq_reason</code> string column. Counts in <code>krishiv_cdc_dlq_total{topic, reason}</code>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/cdc">CDC Routing</a></li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/connectors/two-phase-commit">Two-Phase Commit</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/two-phase-commit',
    group: 'Recipes',
    title: 'Two-Phase Commit Pipeline',
    description: 'Build an exactly-once pipeline: Kafka → Krishiv → Iceberg with two-phase commit.',
    status: 'Available',
    feature_flags: ['iceberg', 'kafka'],
    body: `
<p>The recipe that proves it works: ingest a Kafka topic, run a windowed aggregation, and write the result to Iceberg with exactly-once delivery.</p>

<h2 id="prereqs">Prerequisites</h2>
<ul>
<li>Kafka cluster with a topic that has multiple partitions and a stable schema.</li>
<li>Iceberg REST catalog (Nessie, Polaris, or Glue).</li>
<li>Object store for both Kafka (if not local) and Iceberg.</li>
<li>Build with <code>cargo build --release --features 'kafka iceberg' -p krishiv</code>.</li>
</ul>

<h2 id="env">Step 1 — Environment</h2>
<pre><code class="language-bash">export KRISHIV_COORDINATOR=https://coord.internal:50051
export KRISHIV_COORDINATOR_BEARER_TOKEN=...
export KRISHIV_DURABILITY_PROFILE=distributed-durable
export KRISHIV_SHUFFLE_OBJECT_STORE_URI=s3://my-bucket/shuffle/
export KRISHIV_ICEBERG_CATALOG_URI=http://catalog:8181
export KRISHIV_ICEBERG_WAREHOUSE=s3://my-bucket/warehouse/
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
</code></pre>

<h2 id="topology">Step 2 — Topology</h2>
<pre><code class="language-text">orders-topic  ──►  Kafka source  ──►  windowed aggregation  ──►  Iceberg sink (2PC)
                            │                                            │
                            └─►  checkpoint (Kafka offset)  ◄───────────┘
</code></pre>

<h2 id="code">Step 3 — The pipeline</h2>
<pre><code class="language-rust">use krishiv_api::{Session, col, count, sum};
use krishiv_connectors::iceberg::IcebergSink;
use std::sync::Arc;

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    // Distributed: must point at a coordinator
    let session = Session::connect("https://coord.internal:50051").await?;
    session.register_kafka_source(
        "orders",
        orders_schema(),
        "broker:9092",
        "orders",
        "krishiv-app",
    )?;

    // Build the streaming query
    let per_minute = session
        .table("orders")?
        .to_streaming()
        .with_event_time("event_time")
        .watermark("event_time", 5_000)  // 5 s allowed lateness
        .tumbling_window(60_000)         // 1-minute windows
        .agg(vec![count(col("*")), sum(col("amount"))]);

    // Two-phase commit Iceberg sink
    let sink = Arc::new(
        IcebergSink::new("http://catalog:8181", "s3://my-bucket/warehouse", "orders_per_minute")
            .with_two_phase_commit()
    );

    // Start the query
    let query = per_minute
        .write_stream()
        .output_mode(krishiv_api::OutputMode::Append)
        .trigger(krishiv_api::Trigger::ProcessingTime(5_000))
        .format("iceberg")
        .option("sink", sink)
        .option("checkpoint.location", "s3://my-bucket/ckpt/orders_per_minute/")
        .start().await?;

    // Wait for it
    query.await_termination().await?;
    Ok(())
}
</code></pre>

<h2 id="verify">Step 4 — Verify exactly-once</h2>
<ol>
<li>Push the same Kafka batch twice (replay it). The Iceberg table should not gain duplicate rows.</li>
<li>Kill the coordinator mid-pipeline (or simulate with a chaos test). The Iceberg snapshot is not renamed; on restart, the Kafka offset is not committed; the batch is replayed from the last committed checkpoint and the snapshot is committed exactly once.</li>
<li>Inspect the Iceberg snapshot lineage: <code>SELECT snapshot_id, parent_snapshot_id, summary FROM orders_per_minute.snapshots;</code>. Each commit produces a single child snapshot; no double-parent edges.</li>
</ol>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/two-phase-commit">Two-Phase Commit</a></li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/tooling/chaos">Chaos Testing</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/deploy-on-k8s',
    group: 'Recipes',
    title: 'Deploy on Kubernetes',
    description: 'Install the operator, declare a KrishivCluster, and verify a streaming job runs end-to-end.',
    status: 'Available',
    feature_flags: ['k8s'],
    body: `
<p>The fastest path to a distributed Krishiv cluster is the <code>krishiv-operator</code> with a <code>KrishivCluster</code> CRD. The operator reconciles the CRD into a coordinator Deployment, an executor Deployment, services, and (if your cluster has it) an Ingress.</p>

<h2 id="prereqs">Prerequisites</h2>
<ul>
<li>Kubernetes 1.27+</li>
<li>cert-manager (for the operator's webhook — optional)</li>
<li>An S3 / GCS / ADLS bucket for checkpoints and shuffle</li>
<li>A container registry you can push to</li>
</ul>

<h2 id="install">Step 1 — Build and push the image</h2>
<pre><code class="language-bash">docker build -t my-registry.example.com/krishiv:v0.1.0 -f Dockerfile.fast .
docker push my-registry.example.com/krishiv:v0.1.0
</code></pre>
<p>The same image runs as the coordinator, the executor, and the operator. The binary name determines the role.</p>

<h2 id="crds">Step 2 — Apply CRDs and the operator</h2>
<pre><code class="language-bash">kubectl apply -f k8s/operator/krishiv-crd.yaml
kubectl apply -f k8s/operator/operator-deployment.yaml
</code></pre>
<p>Or with Helm (if your team has a chart):</p>
<pre><code class="language-bash">helm install krishiv-operator ./charts/krishiv-operator \
  --set image.repository=my-registry.example.com/krishiv \
  --set image.tag=v0.1.0
</code></pre>

<h2 id="cluster">Step 3 — Declare a KrishivCluster</h2>
<pre><code class="language-yaml">apiVersion: krishiv.io/v1
kind: KrishivCluster
metadata:
  name: prod
spec:
  image: my-registry.example.com/krishiv:v0.1.0
  coordinators: 1
  executors: 4
  durabilityProfile: distributed-durable
  checkpointStorage:
    uri: s3://my-bucket/krishiv/checkpoints/
  shuffleStorage:
    uri: s3://my-bucket/krishiv/shuffle/
  auth:
    bearerTokenSecret: krishiv-bearer-token
  config:
    KRISHIV_OIDC_AUDIENCE: krishiv-prod
    KRISHIV_OIDC_JWKS_URI: https://auth.example.com/.well-known/jwks.json
</code></pre>

<h2 id="verify">Step 4 — Verify</h2>
<pre><code class="language-bash">kubectl get krishivcluster prod
kubectl get pods -l app=krishiv,role=coordinator
kubectl get pods -l app=krishiv,role=executor

# Port-forward and check the UI
kubectl port-forward svc/krishiv-coordinator 2002:2002
open http://localhost:2002/ui

# Run a SQL query against the cluster
kubectl port-forward svc/krishiv-coordinator 2003:2003 &amp;
krishiv sql --remote -c grpc://localhost:2003 --query "SELECT 1"
</code></pre>

<h2 id="job">Step 5 — Submit a job</h2>
<pre><code class="language-yaml">apiVersion: krishiv.io/v1
kind: KrishivJob
metadata:
  name: orders-per-minute
spec:
  cluster: prod
  sql: |
    CREATE SOURCE orders TYPE KAFKA
      OPTIONS ('brokers' = 'broker:9092', 'topic' = 'orders', 'group.id' = 'krishiv-app');
    CREATE SINK per_minute TYPE ICEBERG
      OPTIONS ('catalog.uri' = '...', 'warehouse' = '...', 'commit' = 'transactional', 'table' = 'orders_per_minute');
    START PIPELINE orders TO per_minute AS
      SELECT tumble_start(event_time, INTERVAL '1 minute') AS window_start,
             customer_id, SUM(amount) AS total
      FROM orders
      GROUP BY tumble_start(event_time, INTERVAL '1 minute'), customer_id;
  checkpoint:
    location: s3://my-bucket/krishiv/ckpt/orders-per-minute/
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/operations/deployment">Deployment</a></li>
  <li><a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/python-remote-cluster',
    group: 'Recipes',
    title: 'Python Client to Remote Cluster',
    description: 'Connect from a Python notebook or script to a remote Krishiv coordinator.',
    status: 'Available',
    body: `
<p>The Python bindings talk to a remote coordinator over Flight SQL (data plane) and gRPC (control plane). Setup is one import, one <code>connect</code> call.</p>

<h2 id="install">Install</h2>
<pre><code class="language-bash">pip install --no-build-isolation krishiv
</code></pre>
<p>Or from a source checkout:</p>
<pre><code class="language-bash">maturin develop --manifest-path crates/krishiv-python/Cargo.toml
</code></pre>

<h2 id="env">Environment</h2>
<pre><code class="language-bash">export KRISHIV_COORDINATOR=https://coord.example.com:50051
export KRISHIV_COORDINATOR_BEARER_TOKEN=...   # or use API key
export KRISHIV_OIDC_AUDIENCE=krishiv-prod     # if using OIDC
</code></pre>

<h2 id="connect">Connect</h2>
<pre><code class="language-python">import krishiv as ks

# Read KRISHIV_COORDINATOR and KRISHIV_COORDINATOR_BEARER_TOKEN
session = ks.Session.from_env()

# Or pass explicitly
session = ks.Session.connect(
    "https://coord.example.com:50051",
    grpc_url="https://coord.example.com:50051",
    target_parallelism=8,
    state_ttl_ms=3_600_000,  # 1 hour state TTL
)

# Or use API keys (server: KRISHIV_API_KEYS=key1=user,...)
session = ks.Session.connect("https://coord.example.com:50051", api_key="key1")
</code></pre>

<h2 id="query">Run a query</h2>
<pre><code class="language-python">df = session.sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
print(df.collect().pretty())
</code></pre>

<h2 id="register">Register a Parquet source</h2>
<pre><code class="language-python">session.register_parquet("orders", "s3://my-bucket/data/orders/")
df = session.sql("SELECT * FROM orders WHERE amount &gt; 100")
df.write_parquet("s3://my-bucket/out/big_orders/")
</code></pre>

<h2 id="ivm">Incremental view</h2>
<pre><code class="language-python">ivm = session.ivm("order_totals")
ivm.register_view("order_totals", "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
batch = pa.record_batch(...)  # pyarrow
ivm.tick("orders", batch)
print(ivm.snapshot("order_totals").to_pandas())
</code></pre>

<h2 id="explain">Explain</h2>
<pre><code class="language-python">print(session.sql("SELECT * FROM orders JOIN users ON orders.user_id = users.id").explain())
</code></pre>

<h2 id="stream">Streaming from a topic</h2>
<pre><code class="language-python">session.register_kafka_source("clicks", schema, "broker:9092", "clicks", "krishiv-app")
(session.table("clicks").to_streaming()
       .with_event_time("event_time")
       .tumbling_window(60_000)
       .agg([ks.count(ks.col("*"))])
       .write_stream()
       .format("iceberg")
       .option("table", "clicks_per_minute")
       .start())
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/python">Python API</a></li>
  <li><a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/jwt-auth',
    group: 'Recipes',
    title: 'JWT / OIDC Auth on the Coordinator',
    description: 'Configure the coordinator to validate end-user bearer tokens against an OIDC provider.',
    status: 'Available',
    body: `
<p>For end-user auth (analysts hitting the UI, BI tools connecting via JDBC), wire the coordinator to your OIDC provider. Krishiv validates the bearer token against a JWKS endpoint and enforces the configured audience.</p>

<h2 id="prereqs">Prerequisites</h2>
<ul>
<li>An OIDC provider (Auth0, Keycloak, Okta, Azure AD, or any compliant implementation).</li>
<li>An OIDC application with a known <code>aud</code> claim (Krishiv requires it in production).</li>
<li>The provider's JWKS URL (typically <code>https://issuer/.well-known/jwks.json</code>).</li>
</ul>

<h2 id="config">Step 1 — Coordinator env</h2>
<pre><code class="language-bash">export KRISHIV_PRODUCTION=1
export KRISHIV_OIDC_JWKS_URI=https://auth.example.com/.well-known/jwks.json
export KRISHIV_OIDC_AUDIENCE=krishiv-prod
export KRISHIV_COORDINATOR_BEARER_TOKEN=...  # still required for service-to-service
export KRISHIV_UI_TOKEN=...                  # separate from OIDC; optional
</code></pre>
<p>Production mode (<code>KRISHIV_PRODUCTION=1</code>) requires that <code>KRISHIV_OIDC_AUDIENCE</code> be set, otherwise the coordinator refuses to start.</p>

<h2 id="roles">Step 2 — Role mapping</h2>
<p>Krishiv recognises three standard roles for management endpoints:</p>
<table class="api-table">
<thead><tr><th>Role</th><th>Can</th></tr></thead>
<tbody>
<tr><td><code>admin</code></td><td>Everything: submit, cancel, savepoint, restore, manage executors.</td></tr>
<tr><td><code>writer</code></td><td>Submit queries and write data. Cannot manage jobs or executors.</td></tr>
<tr><td><code>reader</code></td><td>Read-only: query, list jobs, read state. Cannot write or modify.</td></tr>
</tbody>
</table>
<p>The role is read from the JWT claim. Configure the claim name at the OIDC provider — typically <code>https://krishiv/roles</code> or <code>groups</code>. The coordinator's <code>validate_grpc_auth_for_role</code> enforces it.</p>

<h2 id="client">Step 3 — Client side</h2>
<p>Python:</p>
<pre><code class="language-python">import os, krishiv as ks
# The SDK reads the bearer token from the standard OIDC token endpoint
os.environ["KRISHIV_OIDC_TOKEN"] = open("/run/secrets/oidc-token").read()
session = ks.Session.connect("https://coord.example.com:50051")
</code></pre>
<p>CLI:</p>
<pre><code class="language-bash">KRISHIV_OIDC_TOKEN=... krishiv sql --query "SELECT 1"
</code></pre>
<p>Browser (UI): the UI prompts for a bearer token on first load. Store it in <code>localStorage</code>; it's sent as <code>Authorization: Bearer ...</code> on every request.</p>

<h2 id="rotate">Step 4 — Key rotation</h2>
<p>The coordinator caches the JWKS for <code>KRISHIV_OIDC_JWKS_REFRESH_SECS</code> (default 600). On rotation the provider publishes the new key alongside the old; the next refresh picks it up. Existing tokens continue to work until they expire.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a></li>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
</ul>
`,
  },

  {
    slug: 'recipes/iceberg-schema-migration',
    group: 'Recipes',
    title: 'Iceberg Schema Evolution',
    description: 'Add, drop, rename, and reorder columns in an Iceberg table without rewriting data.',
    status: 'Available',
    feature_flags: ['iceberg'],
    body: `
<p>Iceberg supports several schema-evolution operations natively — no data rewrite required. Krishiv's planner detects the change, rewrites the SQL accordingly, and the Iceberg connector commits a new metadata file.</p>

<h2 id="ops">Supported operations</h2>
<table class="api-table">
<thead><tr><th>Operation</th><th>SQL form</th><th>Iceberg behavior</th></tr></thead>
<tbody>
<tr><td>Add column</td><td><code>ALTER TABLE orders ADD COLUMN region VARCHAR</code></td><td>New metadata file; readers fill the new column with <code>NULL</code>.</td></tr>
<tr><td>Drop column</td><td><code>ALTER TABLE orders DROP COLUMN legacy_col</code></td><td>New metadata file; readers return <code>NULL</code> for the dropped column.</td></tr>
<tr><td>Rename column</td><td><code>ALTER TABLE orders RENAME COLUMN amt TO amount</code></td><td>Metadata-only change.</td></tr>
<tr><td>Reorder columns</td><td><code>ALTER TABLE orders ALTER COLUMN region AFTER customer_id</code></td><td>Metadata-only change.</td></tr>
<tr><td>Widen type</td><td><code>ALTER TABLE orders ALTER COLUMN amount TYPE DOUBLE</code> (from FLOAT)</td><td>Metadata-only change if the new type is a superset.</td></tr>
<tr><td>Promote to required</td><td>(<code>SET REQUIRED</code>)</td><td>Requires backfill if any existing row is <code>NULL</code>.</td></tr>
</tbody>
</table>

<h2 id="add">Add a column</h2>
<pre><code class="language-sql">ALTER TABLE orders ADD COLUMN region VARCHAR;

-- Existing queries continue to work: new column is NULL.
SELECT * FROM orders LIMIT 1;  -- region is NULL

-- Update existing rows (in a maintenance job)
UPDATE orders SET region = 'us-east' WHERE user_id IN (...);
</code></pre>
<p>For backfill at scale, write a streaming job that derives the new column and merges it back.</p>

<h2 id="rename">Rename a column</h2>
<pre><code class="language-sql">ALTER TABLE orders RENAME COLUMN amt TO amount;
</code></pre>
<p>Existing queries that reference <code>amt</code> will break — fix them in the same deploy. Or add a temporary compatibility view:</p>
<pre><code class="language-sql">CREATE OR REPLACE VIEW orders_legacy AS SELECT order_id, user_id, amt AS amount, status, ts FROM orders;
</code></pre>

<h2 id="widen">Widen a type</h2>
<pre><code class="language-sql">-- FLOAT to DOUBLE is metadata-only.
ALTER TABLE orders ALTER COLUMN amount TYPE DOUBLE;

-- INT to BIGINT is metadata-only too.
ALTER TABLE orders ALTER COLUMN user_id TYPE BIGINT;

-- Narrowing (DOUBLE to FLOAT) is NOT supported by Iceberg and fails.
</code></pre>

<h2 id="branch">Branches and tags (zero-copy experiments)</h2>
<p>For risky changes, branch first:</p>
<pre><code class="language-sql">-- Create a branch at the current main snapshot
CALL system.create_branch('orders', 'experiment', main_ref);

-- Run your migration on the branch
ALTER TABLE orders_branch ADD COLUMN region VARCHAR;
-- (or do it in Python via the Iceberg REST API)

-- Inspect the diff via time-travel
SELECT * FROM orders FOR SYSTEM_TIME AS OF 'experiment';

-- Promote or drop
CALL system.fast_forward('orders', 'experiment', main_ref);
-- or
CALL system.drop_branch('orders', 'experiment');
</code></pre>

<h2 id="comp">Compatibility modes</h2>
<table class="api-table">
<thead><tr><th>Mode</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>backward</code> (default)</td><td>Old readers can read new data. Add/drop/widen are OK; rename breaks old readers.</td></tr>
<tr><td><code>forward</code></td><td>New readers can read old data. Drop/widen are OK; add breaks new readers.</td></tr>
<tr><td><code>full</code></td><td>Both. Most restrictive.</td></tr>
<tr><td><code>none</code></td><td>No compatibility checks. Use only for development.</td></tr>
</tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/sql/as-of-queries">AS-OF Queries</a></li>
  <li><a href="/docs/latest/state/savepoints-and-migration">Savepoints and Migration</a></li>
</ul>
`,
  },
];
