import type { DocPage } from '../docs-data';

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
];
