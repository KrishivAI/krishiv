import type { DocPage } from '../docs-data';
import { DIAGRAM_ICEBERG_SNAPSHOTS } from './diagrams';

export const connectorsPages: DocPage[] = [
  {
    slug: 'connectors',
    group: 'Connectors',
    title: 'Connectors Overview',
    description: 'Source and sink connectors for Kafka, Parquet, S3, Iceberg, and more.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Connectors live in <code>krishiv-connectors</code>. They implement the <code>Source</code> and <code>Sink</code> traits and are registered with a session via <code>register_*</code> methods or SQL DDL (<code>CREATE SOURCE</code> / <code>CREATE SINK</code>). Each connector carries its own Cargo feature gate.</p>

<h2 id="source-connectors">Source Connectors</h2>
<table class="api-table">
  <thead><tr><th>Connector</th><th>Feature</th><th>SQL DDL Object</th></tr></thead>
  <tbody>
    <tr><td>Kafka (Confluent or Apache)</td><td><code>kafka</code></td><td><code>CREATE SOURCE … TYPE KAFKA</code></td></tr>
    <tr><td>Parquet (local / S3 / ADLS)</td><td>Always available</td><td><code>CREATE SOURCE … TYPE PARQUET</code></td></tr>
    <tr><td>CSV / NDJSON</td><td>Always available</td><td><code>CREATE SOURCE … TYPE CSV</code></td></tr>
    <tr><td>Iceberg (REST catalog)</td><td><code>iceberg</code></td><td><code>CREATE SOURCE … TYPE ICEBERG</code></td></tr>
    <tr><td>Delta Lake</td><td><code>delta</code></td><td><code>CREATE SOURCE … TYPE DELTA</code></td></tr>
    <tr><td>Hudi</td><td><code>hudi</code></td><td><code>CREATE SOURCE … TYPE HUDI</code></td></tr>
    <tr><td>Arrow Flight</td><td><code>flight-sql</code></td><td>Registered programmatically</td></tr>
  </tbody>
</table>

<h2 id="sink-connectors">Sink Connectors</h2>
<table class="api-table">
  <thead><tr><th>Connector</th><th>Feature</th><th>SQL DDL Object</th></tr></thead>
  <tbody>
    <tr><td>Parquet (local / S3)</td><td>Always available</td><td><code>CREATE SINK … TYPE PARQUET</code></td></tr>
    <tr><td>CSV / NDJSON</td><td>Always available</td><td><code>CREATE SINK … TYPE CSV</code></td></tr>
    <tr><td>Kafka</td><td><code>kafka</code></td><td><code>CREATE SINK … TYPE KAFKA</code></td></tr>
    <tr><td>Iceberg</td><td><code>iceberg</code></td><td><code>CREATE SINK … TYPE ICEBERG</code></td></tr>
    <tr><td>Cassandra</td><td><code>cassandra</code></td><td>Programmatic only</td></tr>
    <tr><td>Elasticsearch</td><td><code>elasticsearch</code></td><td>Programmatic only</td></tr>
    <tr><td>HBase</td><td><code>hbase</code></td><td>Programmatic only</td></tr>
    <tr><td>Vector stores</td><td><code>vector-sinks</code></td><td>Programmatic only</td></tr>
  </tbody>
</table>

<h2 id="delivery-guarantees">Delivery Guarantees</h2>
<p>The effective delivery guarantee is the <em>weakest</em> guarantee supported by the source, sink, and durability profile combination:</p>
<table class="api-table">
  <thead><tr><th>Guarantee</th><th>Requirement</th></tr></thead>
  <tbody>
    <tr><td>Best effort</td><td>Default — no special source or sink requirements.</td></tr>
    <tr><td>At-least-once</td><td>Source must support offset/position tracking. Requires <code>single-node-durable</code> profile or higher.</td></tr>
    <tr><td>Effectively-once</td><td>Idempotent/key-based sink. At-least-once source. Duplicate writes converge on one result.</td></tr>
    <tr><td>Exactly-once</td><td>Certified source + transactional sink + <code>distributed-durable</code> profile. Source position and sink commit are coordinated by checkpoint protocol.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'connectors/kafka',
    group: 'Connectors',
    title: 'Kafka Connector',
    description: 'Source and sink connector for Apache Kafka and Confluent.',
    status: 'Available',
    body: `
<h2 id="requirements">Requirements</h2>
<p>Enable the <code>kafka</code> Cargo feature. In Python: <code>maturin develop --features kafka</code>.</p>

<h2 id="sql-ddl">SQL DDL</h2>
<pre><code class="language-sql">-- Kafka source (streaming)
CREATE SOURCE orders_raw
TYPE KAFKA
OPTIONS (
  'brokers'           = 'broker1:9092,broker2:9092',
  'topic'             = 'orders',
  'group.id'          = 'krishiv-consumer-1',
  'auto.offset.reset' = 'latest',
  'format'            = 'json'        -- 'json' | 'avro' | 'protobuf'
)
WITH SCHEMA (
  order_id   BIGINT   NOT NULL,
  customer   VARCHAR,
  amount     DOUBLE,
  event_time TIMESTAMP
);

-- Kafka sink
CREATE SINK results_sink
TYPE KAFKA
OPTIONS (
  'brokers' = 'broker1:9092',
  'topic'   = 'results',
  'format'  = 'json'
);
</code></pre>

<h2 id="rust-api">Rust API</h2>
<pre><code class="language-rust">use krishiv_api::{Session, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id",   DataType::Int64,   false),
        Field::new("customer",   DataType::Utf8,    true),
        Field::new("amount",     DataType::Float64, true),
        Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
    ]));
    session.register_kafka_source(
        "orders_raw",
        schema,
        "broker1:9092,broker2:9092",
        "orders",
        "krishiv-consumer-1",
    )?;
    let df = session.sql("SELECT * FROM orders_raw WHERE amount > 100").await?;
    df.show().await?;
    Ok(())
}
</code></pre>

<h2 id="python-api">Python API</h2>
<pre><code class="language-python">import krishiv as ks
import pyarrow as pa

session = ks.Session.embedded()
schema = pa.schema([
    pa.field("order_id",   pa.int64()),
    pa.field("customer",   pa.utf8()),
    pa.field("amount",     pa.float64()),
    pa.field("event_time", pa.timestamp("ms")),
])
session.register_kafka_source(
    "orders_raw", schema,
    brokers="broker1:9092",
    topic="orders",
    group="krishiv-consumer-1",
)
session.sql("SELECT customer, SUM(amount) AS total FROM orders_raw GROUP BY customer").show()
</code></pre>

<h2 id="options">Kafka Options</h2>
<table class="api-table">
  <thead><tr><th>Option</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>brokers</code></td><td>—</td><td>Comma-separated Kafka broker list. Required.</td></tr>
    <tr><td><code>topic</code></td><td>—</td><td>Topic name. Required.</td></tr>
    <tr><td><code>group.id</code></td><td>—</td><td>Consumer group ID. Required for sources.</td></tr>
    <tr><td><code>auto.offset.reset</code></td><td><code>latest</code></td><td><code>earliest</code> | <code>latest</code></td></tr>
    <tr><td><code>format</code></td><td><code>json</code></td><td><code>json</code> | <code>avro</code> | <code>protobuf</code></td></tr>
    <tr><td><code>schema.registry.url</code></td><td>—</td><td>Confluent Schema Registry URL (required for Avro/Protobuf).</td></tr>
    <tr><td><code>fetch.message.max.bytes</code></td><td><code>1048576</code></td><td>Max fetch size per partition.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'connectors/parquet',
    group: 'Connectors',
    title: 'Parquet & Object Store',
    description: 'Reading and writing Parquet on local disk, S3, Azure ADLS, and GCS.',
    status: 'Available',
    body: `
<h2 id="local">Local Parquet</h2>
<pre><code class="language-sql">-- Register a local Parquet file as a SQL table
CREATE EXTERNAL TABLE orders
STORED AS PARQUET
LOCATION 'data/orders.parquet';

-- Or a directory of Parquet files
CREATE EXTERNAL TABLE orders_partitioned
STORED AS PARQUET
LOCATION 'data/orders/';
</code></pre>

<h2 id="s3">S3 (AWS or S3-compatible)</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders_s3
STORED AS PARQUET
LOCATION 's3://my-bucket/data/orders/'
OPTIONS (
  'aws.region'             = 'us-east-1',
  'aws.access_key_id'      = '...',
  'aws.secret_access_key'  = '...'
);
</code></pre>
<div class="note-box">Credentials can also be provided via environment variables: <code>AWS_REGION</code>, <code>AWS_ACCESS_KEY_ID</code>, <code>AWS_SECRET_ACCESS_KEY</code>, or instance metadata (EKS IRSA).</div>

<h2 id="azure">Azure ADLS Gen2</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders_adls
STORED AS PARQUET
LOCATION 'abfss://mycontainer@myaccount.dfs.core.windows.net/path/'
OPTIONS (
  'azure.account_name' = 'myaccount',
  'azure.account_key'  = '...'
);
</code></pre>

<h2 id="gcs">Google Cloud Storage</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders_gcs
STORED AS PARQUET
LOCATION 'gs://my-gcs-bucket/path/'
OPTIONS (
  'gcp.service_account_path' = '/path/to/service-account.json'
);
</code></pre>

<h2 id="rust-api">Rust API</h2>
<pre><code class="language-rust">// Local
let df = session.read_parquet("data/orders.parquet").await?;

// S3 (via registered object store)
session.register_s3_object_store("my-bucket", s3_config)?;
session.register_parquet("orders", "s3://my-bucket/orders/").await?;
let df = session.table("orders")?;
</code></pre>

<h2 id="python-api">Python API</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()

# Local
df = session.read_parquet("data/orders.parquet")
df.show()

# Register and use as SQL table
session.register_parquet("orders", "data/orders.parquet")
session.sql("SELECT * FROM orders LIMIT 5").show()

# Write
df.write_parquet("/tmp/output.parquet")
</code></pre>

<h2 id="write-options">Write Options</h2>
<table class="api-table">
  <thead><tr><th>Option</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>compression</code></td><td><code>snappy</code></td><td><code>snappy</code> | <code>gzip</code> | <code>zstd</code> | <code>lz4</code> | <code>none</code></td></tr>
    <tr><td><code>row_group_size</code></td><td><code>1048576</code></td><td>Rows per Parquet row group.</td></tr>
    <tr><td><code>write_batch_size</code></td><td><code>1024</code></td><td>Rows per Arrow write batch.</td></tr>
    <tr><td><code>max_row_group_size</code></td><td><code>1048576</code></td><td>Maximum row group size.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'connectors/iceberg',
    group: 'Connectors',
    title: 'Iceberg',
    description: 'Apache Iceberg REST catalog, table reads, and MERGE INTO.',
    status: 'Preview',
    body: `
<h2 id="requirements">Requirements</h2>
<p>Enable the <code>iceberg</code> Cargo/Python feature. Krishiv targets Apache Iceberg v2+ (v3 for row lineage). The primary certified catalog is REST.</p>
${DIAGRAM_ICEBERG_SNAPSHOTS}

<h2 id="catalog-registration">Catalog Registration</h2>
<pre><code class="language-sql">CREATE CATALOG my_catalog
TYPE ICEBERG_REST
OPTIONS (
  'uri'       = 'http://catalog.internal:8181',
  'warehouse' = 's3://my-bucket/warehouse',
  'token'     = '&lt;oauth-token&gt;'           -- optional
);
</code></pre>
<p>After registration, tables are addressable as <code>my_catalog.my_namespace.my_table</code> in SQL.</p>

<h2 id="reading">Reading Iceberg Tables</h2>
<pre><code class="language-sql">-- Snapshot read (current)
SELECT * FROM my_catalog.ns.orders WHERE order_date &gt;= '2024-01-01';

-- Time travel: Krishiv uses FOR SYSTEM_TIME AS OF (timestamp only).
-- The closest snapshot at or before the given timestamp is selected.
SELECT * FROM my_catalog.ns.orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 00:00:00';
</code></pre>
<p>Time travel is resolved by Krishiv before the query is handed to DataFusion. See <a href="/docs/latest/sql/as-of-queries">AS-OF Queries</a> for the full syntax and snapshot-selection rules.</p>

<h2 id="writing">Writing Iceberg Tables</h2>
<pre><code class="language-sql">-- Append
INSERT INTO my_catalog.ns.orders SELECT * FROM new_orders;

-- Overwrite by predicate
INSERT OVERWRITE my_catalog.ns.orders
OVERWRITE PARTITION (order_date = '2024-06-01')
SELECT * FROM daily_orders WHERE order_date = '2024-06-01';

-- Merge
MERGE INTO my_catalog.ns.orders AS target
USING new_data AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
</code></pre>

<h2 id="rust-api">Rust API</h2>
<pre><code class="language-rust">use krishiv_api::{Session, SessionBuilder, KrishivCatalog, Result};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let catalog = Arc::new(KrishivCatalog::rest("http://catalog.internal:8181", "s3://bucket/wh")?);
    let session = Session::builder()
        .with_iceberg_catalog(catalog, "my_catalog")?
        .build().await?;

    let df = session.sql("SELECT count(*) FROM my_catalog.ns.orders").await?;
    df.show().await?;
    Ok(())
}
</code></pre>

<h2 id="python-api">Python API</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()
# IcebergRestCatalog for metadata inspection
catalog = ks.IcebergRestCatalog(uri="http://catalog:8181", warehouse="s3://bucket/wh")
tables = catalog.list_tables("my_ns")
print(tables)

# Direct read (iceberg feature required)
df = ks.read_iceberg("s3://bucket/wh/my_ns/orders/", catalog_uri="http://catalog:8181")
df.show()
</code></pre>
`,
  },

  {
    slug: 'operations/scheduler',
    group: 'Operations',
    title: 'Scheduler',
    description: 'Coordinator lifecycle, job scheduling, task assignment, and fencing.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>The <code>krishiv-scheduler</code> crate implements the job coordinator. In single-node mode it runs in-process; in distributed mode it runs as a separate daemon accepting Flight/gRPC task-control connections from executors.</p>

<h2 id="coordinator">Coordinator</h2>
<p>The coordinator is the single authoritative owner of job state within an epoch. It:</p>
<ul>
  <li>Accepts job submissions from sessions.</li>
  <li>Fragments jobs into tasks and assigns them to executors.</li>
  <li>Tracks task liveness and triggers reassignment on failure.</li>
  <li>Issues epoch fences to prevent stale completions from being accepted.</li>
  <li>Writes job/task metadata to an in-memory store (dev) or to etcd (distributed-durable).</li>
</ul>

<h2 id="task-lifecycle">Task Lifecycle</h2>
<table class="api-table">
  <thead><tr><th>State</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>Pending</code></td><td>Task created; waiting for an available executor.</td></tr>
    <tr><td><code>Assigned</code></td><td>Task sent to an executor; heartbeat required.</td></tr>
    <tr><td><code>Running</code></td><td>Executor acknowledged start.</td></tr>
    <tr><td><code>Completed</code></td><td>Executor reported success; output is committed.</td></tr>
    <tr><td><code>Failed</code></td><td>Executor reported failure or heartbeat timed out. May be retried.</td></tr>
    <tr><td><code>Cancelled</code></td><td>Job cancelled by user; task instructed to stop.</td></tr>
  </tbody>
</table>

<h2 id="fencing">Fencing</h2>
<p>Each coordinator epoch has a monotone fence token. Executors include this token in completion reports. Stale completions (from a prior epoch or a replaced coordinator) are rejected, preventing double-commit.</p>

<h2 id="configuration">Configuration</h2>
<table class="api-table">
  <thead><tr><th>Environment Variable</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>KRISHIV_COORDINATOR</code></td><td>—</td><td>Flight endpoint for remote sessions.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN</code></td><td>—</td><td>Bearer token for coordinator gRPC auth.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKENS</code></td><td>—</td><td>Comma/newline-separated accepted tokens (rotation).</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN_FILE</code></td><td>—</td><td>File-based token for live reload.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS</code></td><td><code>60</code></td><td>Interval for reloading token file.</td></tr>
    <tr><td><code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code></td><td>—</td><td>Token for executor task-control gRPC.</td></tr>
    <tr><td><code>KRISHIV_MAX_TASK_RETRIES</code></td><td><code>3</code></td><td>Maximum task retries before failing the job.</td></tr>
    <tr><td><code>KRISHIV_HEARTBEAT_TIMEOUT_SECS</code></td><td><code>30</code></td><td>Executor heartbeat timeout before reassignment.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'operations/checkpointing',
    group: 'Operations',
    title: 'Checkpointing',
    description: 'Checkpoint and savepoint configuration, paths, and recovery.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv uses barrier-based checkpointing similar to Apache Flink. A barrier event is injected into all input streams simultaneously. When all operators have processed the barrier, the coordinator saves a consistent snapshot of all keyed state and source offsets.</p>

<h2 id="durability-profiles">Durability Profiles</h2>
<table class="api-table">
  <thead><tr><th>Profile</th><th>State Backend</th><th>Checkpoint Storage</th></tr></thead>
  <tbody>
    <tr><td><code>dev-local</code></td><td>In-memory</td><td>Ephemeral temp dir; not restart-durable.</td></tr>
    <tr><td><code>single-node-durable</code></td><td>RocksDB (local)</td><td>Local filesystem. Survives process restarts on same host.</td></tr>
    <tr><td><code>distributed-durable</code></td><td>RocksDB (restored from checkpoint)</td><td>Object store (S3/ADLS/GCS). Etcd tracks checkpoint metadata.</td></tr>
  </tbody>
</table>

<h2 id="configuration">Configuration</h2>
<table class="api-table">
  <thead><tr><th>Config Key</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>krishiv.checkpoint.interval_ms</code></td><td><code>60000</code></td><td>Checkpoint interval in milliseconds.</td></tr>
    <tr><td><code>krishiv.checkpoint.storage.path</code></td><td>—</td><td>Local path or object-store URI (e.g. <code>s3://bucket/checkpoints/</code>).</td></tr>
    <tr><td><code>krishiv.checkpoint.retain</code></td><td><code>3</code></td><td>Number of completed checkpoints to retain.</td></tr>
    <tr><td><code>krishiv.checkpoint.min_pause_ms</code></td><td><code>500</code></td><td>Minimum gap between successive checkpoint barriers.</td></tr>
    <tr><td><code>krishiv.savepoint.path</code></td><td>—</td><td>Manual savepoint output path. Triggered via <code>session.take_savepoint()</code>.</td></tr>
  </tbody>
</table>

<h2 id="recovery">Recovery</h2>
<pre><code class="language-bash"># Resume from latest checkpoint
cargo run -p krishiv -- start --job-id &lt;id&gt; --resume-from latest

# Resume from specific checkpoint
cargo run -p krishiv -- start --job-id &lt;id&gt; --resume-from &lt;checkpoint-uri&gt;

# Resume from savepoint
cargo run -p krishiv -- start --job-id &lt;id&gt; --resume-from savepoint://&lt;path&gt;
</code></pre>
<div class="warn-box"><strong>Warning:</strong> Savepoints are operator-topology-aware. Adding, removing, or reordering operators between a savepoint and the recovery job may fail or produce incorrect state restoration.</div>
`,
  },

  {
    slug: 'operations/shuffle',
    group: 'Operations',
    title: 'Shuffle',
    description: 'Shuffle service configuration for local, disk, and distributed deployments.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Shuffle moves data between tasks across partition boundaries. The <code>krishiv-shuffle</code> crate provides pluggable shuffle backends selected by the active durability profile.</p>

<h2 id="backends">Shuffle Backends</h2>
<table class="api-table">
  <thead><tr><th>Backend</th><th>Durability Profile</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td>In-memory</td><td><code>dev-local</code></td><td>Arrow batches in a tokio channel. Zero durability; fastest path.</td></tr>
    <tr><td>Local disk</td><td><code>single-node-durable</code></td><td>Batches spilled to local disk under <code>KRISHIV_SHUFFLE_DIR</code>. Survives task restart on same host.</td></tr>
    <tr><td>Object store</td><td><code>distributed-durable</code></td><td>Batches written to S3/ADLS/GCS. Enables cross-host executor reassignment.</td></tr>
    <tr><td>Flight</td><td>Any distributed</td><td>Direct Arrow Flight streaming between executor pairs (low-latency; no intermediate persistence).</td></tr>
  </tbody>
</table>

<h2 id="configuration">Configuration</h2>
<table class="api-table">
  <thead><tr><th>Environment Variable</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>KRISHIV_SHUFFLE_DIR</code></td><td><code>/tmp/krishiv-shuffle</code></td><td>Local disk spill directory (single-node profile).</td></tr>
    <tr><td><code>KRISHIV_SHUFFLE_OBJECT_STORE_URI</code></td><td>—</td><td>Object store URI for distributed shuffle (e.g. <code>s3://bucket/shuffle/</code>).</td></tr>
    <tr><td><code>KRISHIV_SHUFFLE_PARTITIONS</code></td><td><code>auto</code></td><td>Number of shuffle partitions. <code>auto</code> = <code>executor_count × 2</code>.</td></tr>
    <tr><td><code>KRISHIV_SHUFFLE_BATCH_SIZE</code></td><td><code>8192</code></td><td>Maximum rows per shuffle batch.</td></tr>
    <tr><td><code>KRISHIV_SHUFFLE_COMPRESS</code></td><td><code>lz4</code></td><td>Shuffle payload compression: <code>lz4</code> | <code>zstd</code> | <code>none</code>.</td></tr>
  </tbody>
</table>

<h2 id="partitioning">Partitioning</h2>
<p>Krishiv supports three shuffle partitioning strategies:</p>
<ul>
  <li><strong>Hash partitioning:</strong> rows are hashed on one or more key columns. Used for <code>GROUP BY</code>, joins, and keyed operators.</li>
  <li><strong>Range partitioning:</strong> rows are sorted and split at boundary values. Used for range-based aggregations.</li>
  <li><strong>Broadcast:</strong> one partition is replicated to all downstream tasks. Used for small build-side joins.</li>
</ul>
`,
  },

  {
    slug: 'operations/deployment',
    group: 'Operations',
    title: 'Deployment',
    description: 'Embedded, single-node, and Kubernetes deployment options.',
    status: 'Available',
    body: `
<h2 id="embedded">Embedded Mode</h2>
<p>Run Krishiv entirely in-process. No external services required. Best for development, testing, and embedding in an application.</p>
<pre><code class="language-rust">let session = Session::embedded().await?;
</code></pre>
<pre><code class="language-python">session = ks.Session.embedded()
</code></pre>

<h2 id="single-node">Single-Node Daemon</h2>
<p>Runs a coordinator + one executor on a single host. Requires the <code>single-node</code> feature. Provides restart-durable state via RocksDB and local disk shuffle.</p>
<pre><code class="language-bash">cargo build -p krishiv --features single-node --release

# Start the daemon
./target/release/krishiv server start \
  --coordinator-addr 0.0.0.0:50051 \
  --durability single-node-durable \
  --checkpoint-dir /var/krishiv/checkpoints

# Connect from a client
export KRISHIV_COORDINATOR=http://localhost:50051
</code></pre>

<h2 id="kubernetes">Kubernetes Deployment</h2>
<p>Requires the <code>k8s</code> feature. A Kubernetes operator manages <code>KrishivCluster</code> CRDs and spawns coordinator and executor pods.</p>
<pre><code class="language-bash">cargo build -p krishiv --features k8s --release

# Apply CRD and operator
kubectl apply -f deploy/krishiv-crd.yaml
kubectl apply -f deploy/krishiv-operator.yaml

# Create a cluster
kubectl apply -f - &lt;&lt;EOF
apiVersion: krishiv.io/v1
kind: KrishivCluster
metadata:
  name: my-cluster
spec:
  coordinators: 1
  executors: 4
  durabilityProfile: distributed-durable
  checkpointStorage:
    uri: s3://my-bucket/checkpoints/
  shuffleStorage:
    uri: s3://my-bucket/shuffle/
EOF
</code></pre>

<h2 id="env-vars">Key Environment Variables</h2>
<table class="api-table">
  <thead><tr><th>Variable</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>KRISHIV_COORDINATOR</code></td><td>Flight endpoint for remote client sessions.</td></tr>
    <tr><td><code>KRISHIV_DURABILITY_PROFILE</code></td><td><code>dev-local</code> | <code>single-node-durable</code> | <code>distributed-durable</code></td></tr>
    <tr><td><code>KRISHIV_CHECKPOINT_DIR</code></td><td>Checkpoint directory path (local or object-store URI).</td></tr>
    <tr><td><code>KRISHIV_SHUFFLE_DIR</code></td><td>Local shuffle spill directory.</td></tr>
    <tr><td><code>KRISHIV_MAX_PARALLELISM</code></td><td>Target task parallelism (default: CPU count).</td></tr>
    <tr><td><code>KRISHIV_LOG</code></td><td>Log filter string (e.g. <code>info,krishiv_scheduler=debug</code>).</td></tr>
    <tr><td><code>KRISHIV_METRICS_PORT</code></td><td>Prometheus metrics scrape port.</td></tr>
  </tbody>
</table>

<h2 id="bare-metal">Bare-metal / VM cluster</h2>
<p>Process-managed <code>clusterd</code> + N executors, started by <code>krishiv cluster start</code>. Port plan: <code>clusterd</code> listens on <code>127.0.0.1:2001</code> (gRPC) and <code>127.0.0.1:2002</code> (HTTP / UI). Executors bind to <code>(2005 + 2i, 2006 + 2i)</code> so adjacent executors never collide.</p>
<pre><code class="language-bash">krishiv cluster start --executors 4 --http-addr 0.0.0.0:2002
krishiv cluster status
krishiv cluster stop
</code></pre>
<p>Env: <code>KRISHIV_CLUSTER_DATA_DIR</code> (default <code>.krishiv/cluster</code>), <code>KRISHIV_CLUSTER_HTTP_ADDR</code>.</p>

<h2 id="k8s">Kubernetes</h2>
<p>Requires the <code>k8s</code> Cargo feature. The <code>krishiv-operator</code> binary manages <code>KrishivCluster</code> and <code>KrishivJob</code> CRDs.</p>
<pre><code class="language-yaml">apiVersion: krishiv.io/v1
kind: KrishivCluster
metadata:
  name: my-cluster
spec:
  coordinators: 1
  executors: 4
  durabilityProfile: distributed-durable
  checkpointStorage:
    uri: s3://my-bucket/checkpoints/
  shuffleStorage:
    uri: s3://my-bucket/shuffle/
</code></pre>
<p>The operator reconciles by creating <code>coordinator</code> and <code>executor</code> Pods, services for Flight SQL, gRPC, HTTP/UI, and an <code>Ingress</code> (if your cluster has an ingress controller). The <code>krishiv.executor-id</code> label is set by the operator. Lease-based leader election is used for active-coordinator failover.</p>
<p>Manifests in the workspace at <code>k8s/</code> include the CRDs, operator deployment, executor deployment template, and a dev <code>krishiv-dev.yaml</code> for local kind/k3d testing.</p>

<h2 id="flight-sql">Arrow Flight SQL</h2>
<p>The <code>krishiv_flight_server</code> binary (or the co-located sidecar on <code>clusterd</code>) exposes the standard Arrow Flight SQL protocol:</p>
<pre><code class="language-bash">krishiv flight-server
# env: KRISHIV_FLIGHT_ADDR (default 127.0.0.1:2003)
# env: KRISHIV_COORDINATOR_HTTP (default http://127.0.0.1:2002)
</code></pre>
<p>Any JDBC/ODBC client with the Arrow Flight SQL driver, plus the Python <code>adbc-driver-manager</code> package, can connect:</p>
<pre><code class="language-python">import adbc_driver_manager
import adbc_driver_flightsql

conn = adbc_driver_flightsql.connect("grpc://127.0.0.1:2003")
cur = conn.cursor()
cur.execute("SELECT 42")
</code></pre>
<p>The <code>krishiv-flight-sql</code> crate (<code>feature = "flight-sql"</code>) provides the same surface as a library for embedding the server in another axum app.</p>

<h2 id="sql-gateway">SQL Gateway (JDBC/ODBC)</h2>
<p>The <code>krishiv-sql-gateway</code> crate is a thin facade that exposes the standard JDBC API over a <code>SessionPool</code>:</p>
<pre><code class="language-rust">use krishiv_sql_gateway::{GatewaySession, SessionPool};

let pool = SessionPool::new(|| GatewaySession::connect("http://coord:2002"));
let stmt = pool.get()?.prepare("SELECT * FROM orders WHERE id = ?")?;
let rows = stmt.execute(&amp;[ScalarValue::Int64(42)])?;
</code></pre>
<p>Intended for vendors that ship a JDBC driver. The gateway is intentionally versioned independently from <code>krishiv-api</code>.</p>

<h2 id="shuffle">External shuffle service</h2>
<p>Run <code>krishiv shuffle-svc</code> on a dedicated host and point executors at it via <code>KRISHIV_SHUFFLE_OBJECT_STORE_URI</code>:</p>
<pre><code class="language-bash">krishiv shuffle-svc
# env: KRISHIV_SHUFFLE_ADDR (default 0.0.0.0:2004)
# env: KRISHIV_SHUFFLE_DIR (default /var/krishiv/shuffle)
</code></pre>
<p>The service speaks the same Flight-based shuffle protocol the in-process tiered store uses. It is recommended for clusters where shuffle I/O is hot or where the local disk is small (e.g. many small executor pods on the same node).</p>

<h2 id="co-location">Co-locating the daemons</h2>
<p>On a single host, the standard pattern is:</p>
<pre><code class="language-text">clusterd  (coordinator gRPC + HTTP/UI + optional Flight SQL sidecar)
   │
   ├── executor  (data plane)
   ├── shuffle-svc  (optional, for large shuffles)
   └── flight-server  (optional, if you want a separate Arrow Flight endpoint)
</code></pre>
<p>All four are spawned by <code>krishiv local start</code> and managed by the cluster state file at <code>.krishiv/local/cluster.json</code>. <a href="/docs/latest/cli/cluster">CLI: local &amp; cluster</a> for the full lifecycle.</p>

<h2 id="health">Health and Metrics</h2>
<ul>
<li><code>GET /healthz</code> — coordinator liveness check.</li>
<li><code>GET /readyz</code> — coordinator readiness check.</li>
<li><code>GET /metrics</code> — Prometheus metrics endpoint (if <code>KRISHIV_METRICS_PORT</code> is set).</li>
</ul>
`,
  },

  {
    slug: 'operations/auth-and-security',
    group: 'Operations',
    title: 'Auth & Security',
    description: 'API keys, bearer tokens, JWT/OIDC, fail-closed production mode, and the production checklist.',
    status: 'Available',
    body: `
<p>Krishiv supports four layers of authentication. Pick the right one for your topology, then verify that production mode (<code>KRISHIV_PRODUCTION=1</code>) is happy with the rest of your config.</p>

<h2 id="api-keys">1. API keys (Flight SQL and SQL API)</h2>
<p>Static keys for short-lived clients and CI. Configure server-side:</p>
<pre><code class="language-bash"># Comma-separated key=user pairs
export KRISHIV_API_KEYS="key1=alice,key2=ci-bot,key3="
</code></pre>
<p>Client-side:</p>
<pre><code class="language-bash">krishiv sql --api-key key1 --query "SELECT 1"
</code></pre>
<pre><code class="language-python">import krishiv as ks
session = ks.Session.connect("http://coord:50051")
session.sql_as("key1", "SELECT 1")
</code></pre>
<p>API keys are intended for service-to-service auth, not for end users.</p>

<h2 id="bearer">2. Bearer tokens (gRPC and HTTP management)</h2>
<p>The default for production. Tokens are validated on every gRPC call and every <code>/api/v1</code> HTTP call.</p>
<table class="api-table">
<thead><tr><th>Variable</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN</code></td><td>Single static token. Set on the coordinator and on every client.</td></tr>
<tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN_FILE</code></td><td>File path. Hot-reloaded when the file changes. Use for Kubernetes secrets mounted as files.</td></tr>
<tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKENS</code></td><td>Comma/newline-separated list of accepted tokens. Use for rotation windows.</td></tr>
<tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKENS_FILE</code></td><td>File with one token per line. Hot-reloaded.</td></tr>
<tr><td><code>KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS</code></td><td>How often to re-read the file. Default: 60 s.</td></tr>
<tr><td><code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code></td><td>Token the executor presents to the coordinator's task-control gRPC.</td></tr>
</tbody>
</table>

<h3>Rotation</h3>
<ol>
<li>Set <code>KRISHIV_COORDINATOR_BEARER_TOKENS=&lt;new&gt;,&lt;old&gt;</code> on the coordinator. Both are accepted.</li>
<li>Roll clients to use the new token.</li>
<li>Remove the old token from the list.</li>
</ol>

<h2 id="oidc">3. JWT / OIDC (SSO, end-user auth)</h2>
<p>For end-user auth, plug in an OIDC provider. Krishiv validates the bearer token against a JWKS endpoint.</p>
<table class="api-table">
<thead><tr><th>Variable</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>KRISHIV_OIDC_JWKS_URI</code></td><td>OIDC JWKS URL. Set on the coordinator.</td></tr>
<tr><td><code>KRISHIV_OIDC_AUDIENCE</code></td><td>Required in production. The <code>aud</code> claim must match.</td></tr>
</tbody>
</table>
<p>Programmatic:</p>
<pre><code class="language-rust">use krishiv_api::{Session, JwtAuthProvider};

let provider = Arc::new(JwtAuthProvider::new(jwks_url, audience));
let session = Session::connect("http://coord:50051")
    .with_auth(provider);
</code></pre>
<p>Roles: <code>validate_grpc_auth</code> / <code>validate_grpc_auth_for_role</code> enforce role-based access for management endpoints. Standard roles: <code>admin</code>, <code>writer</code>, <code>reader</code>.</p>

<h2 id="ui-token">4. UI bearer token</h2>
<p>Separate from gRPC. Set <code>KRISHIV_UI_TOKEN</code> on the coordinator. The browser is prompted for the token; it is stored in <code>localStorage</code> and sent as <code>Authorization: Bearer ...</code> on every UI request.</p>
<p><code>/healthz</code> always stays anonymous (so liveness probes work). All other UI routes require the token.</p>

<h2 id="fail-closed">Production fail-closed</h2>
<p>Set <code>KRISHIV_PRODUCTION=1</code> (or any truthy value) to enable the production checks. The runtime will refuse to start, or refuse the offending command, if it detects:</p>
<table class="api-table">
<thead><tr><th>Check</th><th>What it rejects</th></tr></thead>
<tbody>
<tr><td><code>requires_http_auth</code></td><td>Coordinator or UI started without a bearer token in production.</td></tr>
<tr><td><code>requires_file_backed_state</code></td><td>State backend that is not RocksDB on local disk or a checkpointed object store.</td></tr>
<tr><td><code>allows_alpha_api</code></td><td>Calls to the alpha API surface (e.g. <code>unbounded_memory_stream</code>).</td></tr>
<tr><td><code>allows_memory_checkpoint_uri</code></td><td>Checkpoint URI of <code>memory://</code> or <code>ephemeral://</code>.</td></tr>
<tr><td><code>allows_unbounded_shuffle_store</code></td><td>Shuffle store that has no upper bound (e.g. in-memory only).</td></tr>
<tr><td><code>forbids_simulation_connectors</code></td><td>Connectors marked as test/simulation in production.</td></tr>
<tr><td><code>profile_requires_durable_window_state</code></td><td>Stateful streaming operators with non-durable state.</td></tr>
<tr><td><code>profile_requires_authenticated_flight</code></td><td>Flight SQL exposed without auth.</td></tr>
<tr><td><code>profile_requires_authenticated_ui</code></td><td>UI exposed without auth.</td></tr>
<tr><td><code>profile_requires_fail_closed_metadata</code></td><td>Metadata store without fencing / leases.</td></tr>
<tr><td><code>profile_forbids_native_scalar_udfs</code></td><td>Native scalar UDFs (which have full process access).</td></tr>
<tr><td><code>requires_manual_kafka_commit</code></td><td>Kafka sources where commits are not bound to checkpoints.</td></tr>
<tr><td><code>allow_legacy_task_fragments</code></td><td>Pre-R5 task fragment format.</td></tr>
<tr><td><code>allow_anonymous_http_override</code></td><td>Anonymous HTTP enabled by an override env (only via <code>ALLOW_ANONYMOUS_HTTP_ENV</code>).</td></tr>
</tbody>
</table>

<h2 id="policy">Policy hooks (table-level access control)</h2>
<p>Beyond authentication, you can attach a <code>PolicyHook</code> to a session or to specific tables:</p>
<pre><code class="language-rust">use krishiv_api::{Session, PolicyHook};

struct MyPolicy;
impl PolicyHook for MyPolicy {
    fn on_query(&amp;self, q: &amp;ParsedQuery) -&gt; Result&lt;(), PolicyError&gt; {
        if q.references_table("pii") &amp;&amp; !q.has_tag("allow_pii") { Err(PolicyError::Denied) }
    }
}
let session = Session::builder().with_policy(Arc::new(MyPolicy)).build().await?;
</code></pre>

<h2 id="checklist">Production checklist</h2>
<ul>
<li><input type="checkbox" disabled> <code>KRISHIV_PRODUCTION=1</code></li>
<li><input type="checkbox" disabled> <code>KRISHIV_COORDINATOR_BEARER_TOKEN</code> set (or OIDC configured)</li>
<li><input type="checkbox" disabled> <code>KRISHIV_OIDC_AUDIENCE</code> set if using OIDC</li>
<li><input type="checkbox" disabled> <code>KRISHIV_UI_TOKEN</code> set</li>
<li><input type="checkbox" disabled> <code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code> set on every executor</li>
<li><input type="checkbox" disabled> Checkpoint storage is an object store, not <code>memory://</code></li>
<li><input type="checkbox" disabled> Durability profile is <code>single-node-durable</code> or <code>distributed-durable</code></li>
<li><input type="checkbox" disabled> No anonymous HTTP overrides (<code>KRISHIV_ALLOW_ANONYMOUS_HTTP=1</code>)</li>
<li><input type="checkbox" disabled> No manual Kafka commit (<code>requires_manual_kafka_commit</code>)</li>
<li><input type="checkbox" disabled> OTLP tracing endpoint set (<code>OTEL_EXPORTER_OTLP_ENDPOINT</code>)</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
  <li><a href="/docs/latest/operations/deployment">Deployment</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/s3',
    group: 'Connectors',
    title: 'S3 / Object Store',
    description: 'Reading and writing Parquet on S3, ADLS, GCS, and MinIO.',
    status: 'Preview',
    body: `
<p>S3, ADLS, GCS, and MinIO share the same <code>object_store</code> backend. They are referenced by URI in the same places as local paths.</p>

<h2 id="uri">URI form</h2>
<pre><code class="language-text">s3://bucket/path/to/table/
s3://bucket/path/to/file.parquet
abfss://container@account.dfs.core.windows.net/path/
gs://bucket/path/
</code></pre>

<h2 id="auth">Authentication</h2>
<table class="api-table">
<thead><tr><th>Provider</th><th>Default chain</th></tr></thead>
<tbody>
<tr><td>AWS S3</td><td>Env vars, then IMDS / IRSA / instance profile.</td></tr>
<tr><td>MinIO (S3-compatible)</td><td><code>AWS_ENDPOINT_URL</code> + static keys.</td></tr>
<tr><td>Azure ADLS Gen2</td><td>Workload identity / managed identity / service principal.</td></tr>
<tr><td>GCS</td><td>Application default credentials / service account file.</td></tr>
</tbody>
</table>

<h2 id="read">Reading</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders
STORED AS PARQUET
LOCATION 's3://my-bucket/data/orders/';
</code></pre>
<p>Or directly:</p>
<pre><code class="language-rust">let df = session.read_parquet("s3://my-bucket/data/orders/2024-q1/*.parquet").await?;
</code></pre>

<h2 id="write">Writing</h2>
<pre><code class="language-bash">df.write_parquet("s3://my-bucket/out/orders/").await?    # Rust
df.write_parquet("s3://my-bucket/out/orders/")           # Python
</code></pre>
<p>Writes use the same <code>ParquetSink</code> and respect <code>write_parquet_with_options</code> (compression, row group size).</p>

<h2 id="creds">Credentials in code</h2>
<p>You can also pass credentials explicitly via <code>CREATE EXTERNAL TABLE</code> options or object-store config:</p>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders
STORED AS PARQUET
LOCATION 's3://my-bucket/data/orders/'
OPTIONS (
  'aws.region'            = 'us-east-1',
  'aws.access_key_id'     = '...',
  'aws.secret_access_key' = '...'
);
</code></pre>
<p>For production, prefer env-var / IAM-based auth and leave these options empty.</p>

<h2 id="perf">Performance</h2>
<ul>
<li>Reads use <code>ParquetReadOptions::with_pushdown_filters(true)</code>, <code>with_enable_page_index(true)</code>, <code>with_enable_bloom_filter(true)</code> by default in production profiles.</li>
<li>Writes coalesce small row groups to <code>max_row_group_size</code> (default 1 MB / 1 048 576 rows).</li>
<li>For directory-based sources, use partition pruning: <code>s3://bucket/table/year=2024/month=01/</code>.</li>
</ul>

<div class="warn-box"><strong>Preview:</strong> S3 / ADLS / GCS writes are feature-complete but end-to-end certification against a specific object store is still in progress. See <a href="/docs/latest/tooling/connector-certification">Connector Certification</a> for the matrix.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/parquet">Parquet &amp; Object Store</a></li>
  <li><a href="/docs/latest/connectors/overview">Connectors Overview</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/avro',
    group: 'Connectors',
    title: 'Avro',
    description: 'Reading and writing Avro files with optional Confluent schema registry.',
    status: 'Preview',
    feature_flags: ['avro'],
    body: `
<p>Avro is supported as a file format. The schema is read from the file header; if you have a Confluent schema registry, the connector can fetch the writer's schema by id.</p>

<h2 id="read">Reading</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE events
STORED AS AVRO
LOCATION '/var/data/events/';
</code></pre>
<p>Or:</p>
<pre><code class="language-rust">let df = session.read_avro("/var/data/events/").await?;
</code></pre>

<h2 id="write">Writing</h2>
<pre><code class="language-rust">df.write_avro("/var/data/events_out/").await?;
</code></pre>
<p>Writer options: snappy compression (default), uncompressed, deflate.</p>

<h2 id="registry">Schema registry integration</h2>
<p>Set <code>KRISHIV_AVRO_REGISTRY_URL=http://schema-registry:8081</code>. The connector will look up the latest schema by name when reading, and register the writer's schema on write (with subject naming <code>{topic}-value</code> or <code>{topic}-key</code> by default; override with the <code>subject</code> option).</p>

<h2 id="perf">Performance</h2>
<p>Avro uses <code>apache_avro</code> for parsing. Decoding is dominated by string and bytes columns. For very wide schemas, enable <code>project_columns</code> in the read options to project before deserialization.</p>

<div class="warn-box"><strong>Preview:</strong> The Avro codec is feature-complete. Schema-registry integration is in the certification suite.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/schema-registry">Schema Registry</a></li>
  <li><a href="/docs/latest/connectors/overview">Connectors Overview</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/kinesis',
    group: 'Connectors',
    title: 'Kinesis',
    description: 'Reading from Amazon Kinesis Data Streams as a streaming source.',
    status: 'Preview',
    feature_flags: ['kinesis'],
    body: `
<p>Krishiv reads from Kinesis Data Streams as a streaming source. The connector is a thin wrapper over the AWS SDK Kinesis client and uses the standard Kinesis GetRecords / GetShardIterator model.</p>

<h2 id="register">Registering a source</h2>
<pre><code class="language-rust">session.register_kinesis_source(
    "orders_stream",
    schema,                  // Arrow schema
    "us-east-1",
    "my-stream",
    "shard-iterator-type-latest",  // latest | trim-horizon | at-sequence-number | after-sequence-number
)?;
</code></pre>

<h2 id="offsets">Offsets and checkpointing</h2>
<p>Krishiv's checkpoint integration captures the Kinesis <code>SequenceNumber</code> per shard in the <code>SourceOffset</code>. On restart, the source resumes from the checkpointed sequence number, falling back to <code>shard-iterator-type</code> if the checkpoint is missing or the stream was re-created.</p>

<h2 id="limits">Limits and quotas</h2>
<ul>
<li>Max <code>RecordsPerShardPerSecond</code>: 1 000 (soft quota). Plan partitions accordingly.</li>
<li>Max record size: 1 MB.</li>
<li>GetRecords returns up to 10 MB or 10 000 records per call.</li>
</ul>

<h2 id="auth">Auth</h2>
<p>Standard AWS SDK auth chain: env vars, then IMDS / IRSA / instance profile. Explicit <code>aws.access_key_id</code> / <code>aws.secret_access_key</code> options are honored but not recommended in production.</p>

<div class="warn-box"><strong>Preview:</strong> Source-only at this release. No Kinesis sink.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/kafka">Kafka Connector</a></li>
  <li><a href="/docs/latest/streaming/overview">Streaming Overview</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/pulsar',
    group: 'Connectors',
    title: 'Pulsar',
    description: 'Reading from Apache Pulsar topics as a streaming source.',
    status: 'Preview',
    feature_flags: ['pulsar-source'],
    body: `
<p>Pulsar is supported as a streaming source. The connector uses the standard Pulsar <code>Consumer</code> model with explicit subscription name, and is compatible with both single-tenant and multi-tenant clusters.</p>

<h2 id="register">Registering a source</h2>
<pre><code class="language-rust">session.register_pulsar_source(
    "orders",
    schema,
    "pulsar://broker:6650",
    "persistent://public/default/orders",
    "krishiv-app",                  // subscription name
    "shared",                       // subscription type: exclusive | shared | failover | key_shared
)?;
</code></pre>

<h2 id="features">Supported features</h2>
<table class="api-table">
<thead><tr><th>Feature</th><th>Notes</th></tr></thead>
<tbody>
<tr><td>Schemas</td><td>Bytes, string, JSON, Avro. Protobuf via schema registry (in flight).</td></tr>
<tr><td>Compression</td><td>LZ4, ZLIB, ZSTD, SNAPPY (server-side decompression is on by default).</td></tr>
<tr><td>Acknowledgement</td><td>Per-message cumulative ack bound to checkpoint epoch.</td></tr>
<tr><td>DLQ</td><td>Standard Pulsar retry topic. Krishiv surfaces the count in <code>krishiv_streaming_rows_emitted_total</code> with a <code>topic</code> label.</td></tr>
</tbody>
</table>

<h2 id="offsets">Offsets and checkpointing</h2>
<p>Per-subscription <code>MessageId</code> is stored in the source offset. On restart, the source seeks to the checkpointed message id.</p>

<h2 id="auth">Auth</h2>
<p>Pulsar uses TLS + token auth. Configure via <code>pulsar://broker:6650</code> with a token in the URL or via <code>PULSAR_TOKEN</code> env.</p>

<div class="warn-box"><strong>Preview:</strong> Source-only at this release. No Pulsar sink.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/kafka">Kafka Connector</a></li>
  <li><a href="/docs/latest/streaming/overview">Streaming Overview</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/vector-sinks',
    group: 'Connectors',
    title: 'Vector Sinks',
    description: 'Vector database connectors for embedding search (LanceDB, Pinecone, Weaviate, Qdrant, pgvector).',
    status: 'Experimental',
    feature_flags: ['vector-sinks'],
    body: `
<p>Krishiv ships five vector-store sinks. They share a common interface so you can swap targets without changing pipeline code.</p>

<h2 id="interface">Common interface</h2>
<table class="api-table">
<thead><tr><th>Method</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>upsert_batch(batch)</code></td><td>Write a batch of points (id, vector, payload).</td></tr>
<tr><td><code>query_nearest(vector, k) -&gt; Vec&lt;ScoredChunk&gt;</code></td><td>k-NN search. Returns scored chunks with payload.</td></tr>
<tr><td><code>delete_by_ids(ids)</code></td><td>Delete by id.</td></tr>
<tr><td><code>sink_name() -&gt; &amp;'static str</code></td><td>For metrics labels and CLI output.</td></tr>
</tbody>
</table>

<h2 id="backends">Backends</h2>
<table class="api-table">
<thead><tr><th>Backend</th><th>Feature</th><th>Status</th><tr></tr>
<thead><tr><th>Backend</th><th>Feature</th><th>Status</th></tr></thead>
<tbody>
<tr><td><code>InMemoryVectorSink</code></td><td>(always)</td><td>Preview — for tests and prototypes.</td></tr>
<tr><td><code>LanceDbSink::open(path, table)</code></td><td><code>vector-sinks</code></td><td>Preview — local file-backed.</td></tr>
<tr><td><code>PgvectorSink::connect(conn_str, table)</code></td><td><code>vector-sinks</code> + <code>pgvector</code></td><td>Experimental.</td></tr>
<tr><td><code>QdrantSink::connect(url, collection)</code></td><td><code>vector-sinks</code> + <code>qdrant</code></td><td>Experimental.</td></tr>
<tr><td><code>PineconeSink::new(api_key, index)</code></td><td><code>vector-sinks</code></td><td>Preview.</td></tr>
<tr><td><code>WeaviateSink::connect(url, class)</code></td><td><code>vector-sinks</code></td><td>Preview.</td></tr>
</tbody>
</table>

<h2 id="data">Data shape</h2>
<p>All sinks expect a <code>RecordBatch</code> with at least:</p>
<table class="api-table">
<thead><tr><th>Column</th><th>Type</th></tr></thead>
<tbody>
<tr><td><code>id</code></td><td>utf8 or int64</td></tr>
<tr><td><code>vector</code></td><td>list&lt;float32&gt; (or fixed-size list)</td></tr>
<tr><td><code>payload</code></td><td>struct&lt;…&gt; — backend-specific fields</td></tr>
</tbody>
</table>
<p>Schema validation is the caller's responsibility; <code>point_id_from_doc_epoch</code> is a helper that turns a timestamped id into a deterministic u64.</p>

<h2 id="python">Python</h2>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()
sink = ks.LanceDbSink.open("./vectors", "embeddings")
session.sql("SELECT id, vector, payload FROM embeddings")
       .write_stream()
       .format("vector")
       .option("sink", sink)
       .start()
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/python/sinks">Python Sinks</a></li>
  <li><a href="/docs/latest/tooling/connector-certification">Connector Certification</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/schema-registry',
    group: 'Connectors',
    title: 'Schema Registry',
    description: 'Confluent-compatible HTTP client for Avro, Protobuf, and JSON-Schema payload decoding.',
    status: 'Preview',
    feature_flags: ['schema-registry'],
    body: `
<p>The <code>schema_registry</code> feature pulls schemas from a Confluent-compatible registry so Krishiv can decode <code>bytes</code> payloads into typed <code>RecordBatch</code>es. The registry client is also exported for use in custom UDFs.</p>

<h2 id="config">Configuration</h2>
<table class="api-table">
<thead><tr><th>Variable</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>KRISHIV_AVRO_REGISTRY_URL</code></td><td>Registry base URL, e.g. <code>http://schema-registry:8081</code>.</td></tr>
<tr><td><code>KRISHIV_PROTO_REGISTRY_URL</code></td><td>Same for Protobuf schemas.</td></tr>
<tr><td><code>KRISHIV_JSON_REGISTRY_URL</code></td><td>Same for JSON-Schema (used in <code>json.value</code> decoding).</td></tr>
</tbody>
</table>

<h2 id="subject">Subject naming</h2>
<p>For a Kafka topic <code>orders</code> with key/value schemas, the default subjects are <code>orders-key</code> and <code>orders-value</code>. Override per source with the <code>registry.subject.key</code> / <code>registry.subject.value</code> options.</p>

<h2 id="evolution">Schema evolution</h2>
<p>The connector fetches the schema by id embedded in the message. Compatibility is enforced by the registry, not by Krishiv. A backward-incompatible schema change is rejected by the registry and the source fails to start.</p>

<h2 id="client">Using the client directly</h2>
<pre><code class="language-rust">use krishiv_connectors::schema_registry::SchemaRegistryClient;

let client = SchemaRegistryClient::new("http://schema-registry:8081");
let schema = client.get_latest_schema("orders-value").await?;
let avro = schema.parse_avro()?;
</code></pre>

<h2 id="python">Python</h2>
<pre><code class="language-python">import krishiv as ks
schema = ks.schema_registry_confluent("http://schema-registry:8081", "orders-value")
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/avro">Avro</a></li>
  <li><a href="/docs/latest/connectors/kafka">Kafka</a></li>
  <li><a href="/docs/latest/connectors/overview">Connectors Overview</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/two-phase-commit',
    group: 'Connectors',
    title: 'Two-Phase Commit & Delivery Guarantees',
    description: 'Atomic sink writes, the writer–commit–ack protocol, and the certified delivery-guarantee matrix.',
    status: 'Available',
    body: `
<p>Two-phase commit (2PC) is what turns at-least-once into exactly-once. Krishiv implements the standard <em>prepare → commit</em> protocol, coordinated by the checkpoint barrier.</p>

<h2 id="protocol">The protocol</h2>
<ol>
<li>The coordinator injects a <strong>checkpoint barrier</strong> into every task.</li>
<li>Each task drains its current output, writes to its sink via <code>prepare</code>, and acknowledges the barrier with the staged <code>commit handle</code>.</li>
<li>Once every task has acked, the coordinator commits the epoch: every sink's <code>commit</code> is called. The checkpoint is final only after every commit succeeds.</li>
<li>If any task fails before ack, the coordinator aborts the epoch: every <code>prepare</code>d commit is <code>abort</code>ed. No partial state is left visible.</li>
</ol>

<h2 id="writers">Two-phase sink implementations</h2>
<table class="api-table">
<thead><tr><th>Sink</th><th>Notes</th></tr></thead>
<tbody>
<tr><td><code>LocalParquetTwoPhaseCommitSink</code></td><td>Writes to a temp dir, then renames on commit. Crash-safe on local disk.</td></tr>
<tr><td><code>InMemoryTwoPhaseCommitSink</code></td><td>For tests.</td></tr>
<tr><td><code>IcebergNativeTwoPhaseCommit</code> (<code>iceberg</code>)</td><td>Writes a new Iceberg snapshot, then renames the metadata pointer on commit.</td></tr>
<tr><td><code>HudiTwoPhaseCommitSink</code> (<code>lakehouse</code>)</td><td>Writes a Hudi commit, then publishes the timeline on commit.</td></tr>
<tr><td><code>LocalDeltaTwoPhaseCommitSink</code> (<code>lakehouse</code>)</td><td>Writes a Delta log entry, then renames the checkpoint on commit.</td></tr>
<tr><td>Kafka transactional sink (<code>kafka</code>)</td><td>Uses the Kafka EOS API: <code>initTransactions</code> → <code>beginTransaction</code> → <code>sendOffsetsToTransaction</code> → <code>commitTransaction</code>.</td></tr>
</tbody>
</table>

<h2 id="exactly-once">When you get exactly-once</h2>
<p>Exactly-once delivery requires <em>all</em> of:</p>
<ol>
<li>Source supports offset commit (e.g. Kafka).</li>
<li>Sink supports 2PC (or is transactional in the source's sense).</li>
<li>Durability profile is <code>distributed-durable</code>.</li>
<li>Coordinator uses fenced commits (epoch tokens).</li>
</ol>
<p>See the <a href="/docs/latest/tooling/connector-certification">Connector Certification</a> matrix for the certified combinations.</p>

<h2 id="quality">Data quality (DLT-style)</h2>
<p>Pair 2PC with a <code>DataQualityConfig</code> to drop or fail records that violate expectations:</p>
<pre><code class="language-rust">use krishiv_connectors::quality::{DataQualityConfig, DataQualityRule, QualityAction};

let cfg = DataQualityConfig::new()
    .rule(DataQualityRule::not_null("user_id"))
    .rule(DataQualityRule::range("amount", 0.0, 1_000_000.0))
    .on_violation(QualityAction::Drop)
    .with_dead_letter(DeadLetterSink::parquet("./dlq/"));
</code></pre>

<h2 id="recipe">See also</h2>
<ul>
  <li><a href="/docs/latest/tooling/connector-certification">Connector Certification</a> — the source of truth for which combinations are supported</li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/connectors/kafka">Kafka</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/quality',
    group: 'Connectors',
    title: 'Data Quality & Dead Letter',
    description: 'Per-source data quality rules, drop / fail actions, and the dead-letter sink.',
    status: 'Available',
    body: `
<p>Data quality in Krishiv is a per-source config that runs on the write path (between the operator and the sink). It produces either dropped rows, a failed query, or routed-to-DLQ rows.</p>

<h2 id="config">Configuration</h2>
<pre><code class="language-rust">use krishiv_connectors::quality::{DataQualityConfig, DataQualityRule, QualityAction, DeadLetterSink};

let cfg = DataQualityConfig::new()
    .rule(DataQualityRule::not_null("user_id"))
    .rule(DataQualityRule::range("amount", 0.0, 1_000_000.0))
    .rule(DataQualityRule::regex("email", r"^[^@]+@[^@]+$"))
    .on_violation(QualityAction::Drop)
    .with_dead_letter(DeadLetterSink::parquet("./dlq/"));

let sink = IcebergSink::new(...).with_quality(cfg);
</code></pre>

<h2 id="rules">Rule kinds</h2>
<table class="api-table">
<thead><tr><th>Rule</th><th>What it checks</th></tr></thead>
<tbody>
<tr><td><code>not_null(col)</code></td><td>Column is non-null for every row in the batch.</td></tr>
<tr><td><code>range(col, min, max)</code></td><td>Column is in [<code>min</code>, <code>max</code>] for every row.</td></tr>
<tr><td><code>regex(col, pattern)</code></td><td>Column matches the regex.</td></tr>
<tr><td><code>enum(col, &amp;[v1, v2])</code></td><td>Column is one of the listed values.</td></tr>
<tr><td><code>custom(col, fn(batch) -&gt; Mask)</code></td><td>Custom boolean mask per row.</td></tr>
</tbody>
</table>

<h2 id="actions">Actions</h2>
<table class="api-table">
<thead><tr><th>Action</th><th>Behavior</th></tr></thead>
<tbody>
<tr><td><code>Drop</code></td><td>Remove the offending rows from the batch. Continue.</td></tr>
<tr><td><code>Fail</code></td><td>Fail the query with <code>DataQualityError</code>. Return non-zero in the CLI.</td></tr>
<tr><td><code>DeadLetter</code></td><td>Send the offending rows to a separate sink (e.g. Parquet) and continue with the clean rows.</td></tr>
</tbody>
</table>

<h2 id="dlq">Dead-letter sink</h2>
<p>A dead-letter sink is itself a Krishiv sink. It writes to a separate file, table, or topic:</p>
<pre><code class="language-rust">DeadLetterSink::parquet("./dlq/")
DeadLetterSink::iceberg("dlq_catalog", "dlq.dlq_table")
DeadLetterSink::kafka("broker:9092", "dlq.orders")
</code></pre>
<p>Each dead-letter record carries an extra <code>_dlq_reason</code> string column with the rule that failed.</p>

<h2 id="metrics">Metrics</h2>
<p>Per rule and per source, Krishiv exposes:</p>
<ul>
<li><code>krishiv_dataquality_dropped_total{source, rule}</code></li>
<li><code>krishiv_dataquality_failed_total{source, rule}</code></li>
<li><code>krishiv_dataquality_dlq_total{source, rule, sink}</code></li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/two-phase-commit">Two-Phase Commit</a></li>
  <li><a href="/docs/latest/observability/metrics">Metrics</a></li>
</ul>
`,
  },

  {
    slug: 'connectors/cdc',
    group: 'Connectors',
    title: 'CDC Routing',
    description: 'Bridging Kafka CDC topics into lakehouse sinks with offset tracking.',
    status: 'Available',
    feature_flags: ['lakehouse'],
    body: `
<p>The CDC router lets you bridge a Kafka CDC topic (Debezium-style: <code>op</code> column with values <code>c</code>/<code>u</code>/<code>d</code> + <code>before</code>/<code>after</code> payloads) into a lakehouse table that supports merge / upsert.</p>

<h2 id="topology">Topology</h2>
<pre><code class="language-text">Kafka CDC topic → krishiv CDC router → Iceberg / Delta / Hudi (with two-phase commit)
                            │
                            └─ DLQ (rows that don't match the schema)
</code></pre>

<h2 id="config">Configuration</h2>
<pre><code class="language-rust">use krishiv_connectors::cdc::CdcRouter;

let router = CdcRouter::builder()
    .source_kafka("broker:9092", "orders.cdc", "krishiv-cdc")
    .sink_iceberg("catalog.uri", "warehouse", "orders")
    .key_columns(&amp;["order_id"])
    .dlq_parquet("./dlq/")
    .build()?;
</code></pre>

<h2 id="ops">Supported operations</h2>
<table class="api-table">
<thead><tr><th>CDC op</th><th>Sink action</th></tr></thead>
<tbody>
<tr><td><code>c</code> (create)</td><td><code>INSERT</code></td></tr>
<tr><td><code>u</code> (update)</td><td><code>MERGE</code> (matched by <code>key_columns</code>)</td></tr>
<tr><td><code>d</code> (delete)</td><td><code>DELETE</code></td></tr>
<tr><td><code>r</code> (read, snapshot)</td><td>Ignored (no-op)</td></tr>
</tbody>
</table>

<h2 id="offset">Offset tracking</h2>
<p>Per-partition Kafka offsets are captured in the checkpoint. On restart, the router resumes from the last committed offset. Exactly-once requires the destination sink to support 2PC; otherwise the router falls back to at-least-once.</p>

<h2 id="schema">Schema mapping</h2>
<p>The router expects the CDC payload to have at least:</p>
<table class="api-table">
<thead><tr><th>Column</th><th>Type</th></tr></thead>
<tbody>
<tr><td><code>op</code></td><td>utf8 — one of <code>c</code>, <code>u</code>, <code>d</code>, <code>r</code></td></tr>
<tr><td><code>before</code></td><td>struct (or null) — the row before the change</td></tr>
<tr><td><code>after</code></td><td>struct (or null) — the row after the change</td></tr>
<tr><td><code>ts_ms</code></td><td>int64 — source timestamp</td></tr>
</tbody>
</table>
<p>Configure schema with <code>.with_before_field("before")</code>, <code>.with_after_field("after")</code>, <code>.with_op_field("op")</code>, <code>.with_ts_field("ts_ms")</code>.</p>

<h2 id="dlq">DLQ for malformed records</h2>
<p>Records that don't parse (missing <code>op</code>, missing <code>before</code>/<code>after</code>, schema mismatch) are routed to the DLQ sink with an explanation in <code>_dlq_reason</code>. Counts in <code>krishiv_cdc_dlq_total{topic, reason}</code>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/kafka">Kafka Connector</a></li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/recipes/cdc-to-iceberg">CDC to Iceberg recipe</a></li>
</ul>
`,
  },
];
