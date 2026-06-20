import type { DocPage } from '../docs-data';

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

-- Time travel by snapshot ID
SELECT * FROM my_catalog.ns.orders FOR VERSION AS OF 1234567890;

-- Time travel by timestamp
SELECT * FROM my_catalog.ns.orders FOR TIMESTAMP AS OF '2024-06-01 00:00:00';
</code></pre>

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

<h2 id="health">Health and Metrics</h2>
<ul>
  <li><code>GET /healthz</code> — coordinator liveness check.</li>
  <li><code>GET /readyz</code> — coordinator readiness check.</li>
  <li><code>GET /metrics</code> — Prometheus metrics endpoint (if <code>KRISHIV_METRICS_PORT</code> is set).</li>
</ul>
`,
  },
];
