import type { DocPage } from '../docs-data';
import { DIAGRAM_DISTRIBUTED_TOPOLOGY, DIAGRAM_INCREMENTAL_FLOW, DIAGRAM_PIPELINE_BUILDER } from './diagrams';

export const gettingStartedPages: DocPage[] = [
  {
    slug: '',
    group: 'Getting Started',
    title: 'Introduction',
    description: 'Krishiv — one engine for batch SQL, streaming pipelines, and incremental view maintenance.',
    status: 'Available',
    body: `
<div class="note-box"><strong>New here?</strong> Jump to <a href="#sixty-seconds">60 seconds to your first query</a>, or read <a href="/docs/latest/why-krishiv">Why Krishiv</a> first if you are evaluating alternatives.</div>

<h2 id="what-is">What is Krishiv?</h2>
<p>Krishiv is a Rust-native compute framework that unifies batch SQL, streaming pipelines, and incremental view maintenance under a single execution model. It uses <strong>Apache Arrow RecordBatch</strong> as the internal columnar data model and <strong>DataFusion</strong> for SQL parsing, planning, expressions, and local execution.</p>
<p>The same session, plan, and scheduler/executor runtime works across embedded (in-process), single-node daemon, and distributed cluster deployments.</p>

<h2 id="when-to-use">When to use Krishiv</h2>
<p>Krishiv fits when you need more than one of these from the same engine and the same APIs:</p>
<ul>
  <li>Batch SQL over Parquet, CSV, Iceberg, Delta, or Hudi.</li>
  <li>Streaming pipelines with event-time windows, watermarks, and keyed state.</li>
  <li>Incremental view maintenance that updates as source data changes.</li>
  <li>A single Rust-native runtime that can run embedded, as a local daemon, or on a cluster.</li>
</ul>
<p>Read <a href="/docs/latest/why-krishiv">Why Krishiv</a> for an honest comparison with alternatives.</p>

<h2 id="interfaces">Three interfaces, one engine</h2>
<table class="api-table">
  <thead><tr><th>Interface</th><th>Use it when…</th></tr></thead>
  <tbody>
    <tr><td><strong>SQL</strong> (<a href="/docs/latest/sql">SQL Reference</a>)</td><td>The query is one-off, ad-hoc, or shared with analysts.</td></tr>
    <tr><td><strong>Rust API</strong> (<code>krishiv-api</code>, <a href="/docs/latest/rust">Rust API</a>)</td><td>You are building a library, service, or framework.</td></tr>
    <tr><td><strong>Python API</strong> (<code>krishiv</code>, <a href="/docs/latest/python">Python API</a>)</td><td>You are prototyping, scripting, or integrating with PyArrow / pandas.</td></tr>
  </tbody>
</table>
<p>All three share the same planner, optimizer, and runtime. SQL parses into the same <code>Expr</code> AST that the DataFrame API builds directly.</p>

<h2 id="key-properties">Key Properties</h2>
<ul>
  <li><strong>Unified execution:</strong> batch and streaming share Arrow batches, planning, runtime routing, and scheduler/executor boundaries.</li>
  <li><strong>Rust-native:</strong> Rust 2024 + Tokio; typed IDs, typed plans, typed errors, explicit durability profiles.</li>
  <li><strong>Three interfaces:</strong> SQL, Rust API (<code>krishiv-api</code>), Python bindings (<code>krishiv</code> via PyO3).</li>
  <li><strong>Iceberg-first lakehouse:</strong> Apache Iceberg is the primary lakehouse platform (Preview — see <a href="/product/maturity">Maturity</a>).</li>
  <li><strong>Incremental processing:</strong> <code>DeltaBatch</code> (weighted Arrow rows) and <code>IncrementalFlow</code> for incremental view maintenance (Experimental).</li>
</ul>

<h2 id="sixty-seconds">60 seconds to your first query</h2>
<p>Pick your language. All three run in-process with no external services.</p>

<h3 id="py-quick">Python</h3>
<pre><code class="language-bash">pip install --no-build-isolation krishiv
# or, from a source checkout:
maturin develop --manifest-path crates/krishiv-python/Cargo.toml
</code></pre>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()
df = session.sql("SELECT 42 AS answer")
df.show()
</code></pre>

<h3 id="rs-quick">Rust</h3>
<pre><code class="language-toml">[dependencies]
krishiv-api = { path = "../crates/krishiv-api" }
tokio = { version = "1", features = ["full"] }
</code></pre>
<pre><code class="language-rust">use krishiv_api::Session;

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    session.sql("SELECT 42 AS answer").await?.show().await?;
    Ok(())
}
</code></pre>

<h3 id="sql-quick">SQL (via the CLI)</h3>
<pre><code class="language-bash">cargo run -p krishiv -- sql --query "SELECT 42 AS answer"
</code></pre>

<p>Output: a single-row table with <code>answer = 42</code>. From here, the <a href="/docs/latest/tutorial">Your first Krishiv pipeline</a> tutorial walks you through reading a file, aggregating, and writing a result.</p>

<h2 id="next">What to read next</h2>
<table class="api-table">
  <thead><tr><th>If you want to…</th><th>Go to…</th></tr></thead>
  <tbody>
    <tr><td>Understand the mental model (Session → plan → runtime → coordinator → executor)</td><td><a href="/docs/latest/concepts/how-it-executes">How Krishiv executes a query</a></td></tr>
    <tr><td>Evaluate against Spark, Flink, DataFusion, DuckDB</td><td><a href="/docs/latest/why-krishiv">Why Krishiv</a></td></tr>
    <tr><td>Build a real pipeline end-to-end</td><td><a href="/docs/latest/tutorial">Your first Krishiv pipeline</a></td></tr>
    <tr><td>Look up a specific API</td><td><a href="/docs/latest/python">Python API</a> · <a href="/docs/latest/rust">Rust API</a> · <a href="/docs/latest/sql">SQL</a></td></tr>
    <tr><td>Solve a specific task ("I want to…")</td><td><a href="/docs/latest/recipes">Recipes</a></td></tr>
    <tr><td>Check what is production-ready today</td><td><a href="/product/maturity">Feature Maturity</a></td></tr>
  </tbody>
</table>

<h2 id="architecture">Architecture at a Glance</h2>
<pre><code>SQL / Rust API / Python API
  └─ Session + catalog
     └─ DataFusion + Krishiv plan + optimizer
        └─ ExecutionRuntime
              Embedded          → in-process
              SingleNode        → local Flight/gRPC daemon
              Distributed       → remote Flight/gRPC cluster
           └─ Coordinator
              └─ ExecutorTaskRunner
                 └─ Arrow/DataFusion ops, shuffle, state, checkpoints, connectors
</code></pre>
<p>For the full breakdown see the <a href="/docs/latest/concepts/architecture">Architecture reference</a>.</p>

<h2 id="crate-map">Workspace Crate Map</h2>
<table class="api-table">
  <thead><tr><th>Crate</th><th>Responsibility</th></tr></thead>
  <tbody>
    <tr><td><code>krishiv</code></td><td>User-facing facade and CLI binary.</td></tr>
    <tr><td><code>krishiv-api</code></td><td>Session, DataFrame, Stream, IncrementalFlow, and all public Rust API types.</td></tr>
    <tr><td><code>krishiv-sql</code></td><td>DataFusion integration, SQL execution, catalog and table-provider abstractions.</td></tr>
    <tr><td><code>krishiv-plan</code></td><td>Logical/physical plans, expression AST, UDF contracts, governance/policy, CEP.</td></tr>
    <tr><td><code>krishiv-runtime</code></td><td>Embedded, single-node, and remote runtime routing.</td></tr>
    <tr><td><code>krishiv-dataflow</code></td><td>Arrow operator runtime, queues, barriers, windows, joins, stateful ops.</td></tr>
    <tr><td><code>krishiv-scheduler</code></td><td>Coordinator, job/task lifecycle, metadata stores, leadership, gRPC server.</td></tr>
    <tr><td><code>krishiv-executor</code></td><td>Executor process, task runner, shuffle/checkpoint hooks.</td></tr>
    <tr><td><code>krishiv-state</code></td><td>In-memory and RocksDB-backed keyed state, TTL, migration, checkpoint/savepoint.</td></tr>
    <tr><td><code>krishiv-connectors</code></td><td>Source/sink contracts, Parquet/Kafka/S3 paths, Iceberg-first lakehouse helpers.</td></tr>
    <tr><td><code>krishiv-python</code></td><td>PyO3 Python bindings.</td></tr>
    <tr><td><code>krishiv-shuffle</code></td><td>In-memory, local disk, object-store, and Flight-oriented shuffle support.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'why-krishiv',
    group: 'Getting Started',
    title: 'Why Krishiv',
    description: 'An honest comparison with Spark, Flink, DataFusion, and DuckDB — and the trade-offs.',
    status: 'Available',
    body: `
<p>This page is written for someone evaluating whether to adopt Krishiv. It is opinionated but tries to be honest about where Krishiv is not the right choice.</p>

<h2 id="tl-dr">TL;DR</h2>
<p>Krishiv is for teams that want <strong>batch, streaming, and incremental view maintenance from the same engine and the same APIs</strong>, written in Rust, and are willing to accept that some capabilities (distributed executor IVM, end-to-end exactly-once, full Iceberg certification) are still maturing.</p>
<p>If you only need batch SQL, <strong>DuckDB</strong> is faster and more mature. If you need a battle-tested distributed batch + streaming stack today, <strong>Spark + Flink</strong> is the safer choice. If you want a Rust SQL engine you can embed but do not need streaming or incremental, <strong>DataFusion</strong> standalone is leaner.</p>

<h2 id="vs-spark-flink">vs. Apache Spark + Apache Flink</h2>
<table class="api-table">
  <thead><tr><th>Dimension</th><th>Spark + Flink</th><th>Krishiv</th></tr></thead>
  <tbody>
    <tr><td>Batch SQL</td><td>Spark SQL (mature, very large ecosystem)</td><td>DataFusion-backed (mature, smaller ecosystem)</td></tr>
    <tr><td>Streaming</td><td>Flink (mature, certified for exactly-once at scale)</td><td>Same runtime as batch (Available for in-process; Preview for end-to-end pipelines)</td></tr>
    <tr><td>Incremental views</td><td>Materialized views in Flink, separate tooling elsewhere</td><td>First-class via <code>IncrementalFlow</code> (Experimental)</td></tr>
    <tr><td>APIs</td><td>Java/Scala/Python, multiple disjoint APIs</td><td>Rust, Python, SQL — one shared plan</td></tr>
    <tr><td>Runtime</td><td>JVM (Spark), JVM (Flink)</td><td>Rust + Tokio (single binary, no JVM)</td></tr>
    <tr><td>Ecosystem maturity</td><td>10+ years, broad connector coverage</td><td>Early — see <a href="/product/maturity">Maturity</a></td></tr>
    <tr><td>Lakehouse formats</td><td>Delta Lake, Iceberg, Hudi connectors</td><td>Iceberg-first (Preview), Delta/Hudi sources (Preview)</td></tr>
    <tr><td>Exactly-once</td><td>Certified at scale in production deployments</td><td>Available for specific certified source/sink/checkpoint combinations only</td></tr>
  </tbody>
</table>
<p><strong>Pick Spark + Flink</strong> if you need a battle-tested, large-ecosystem system today. <strong>Pick Krishiv</strong> if you want one engine, one set of APIs, and a Rust-native binary — and you are willing to grow with it.</p>

<h2 id="vs-datafusion">vs. Apache DataFusion (standalone)</h2>
<p>DataFusion is the SQL engine inside Krishiv. Krishiv adds:</p>
<ul>
  <li>A <strong>distributed scheduler and executor</strong> (DataFusion is a single-process library).</li>
  <li>A <strong>streaming runtime</strong> with event-time windows, watermarks, and barriers.</li>
  <li><strong>Stateful operators</strong> with RocksDB-backed keyed state and checkpointing.</li>
  <li><strong>Connectors</strong> for Kafka, Iceberg, S3/ADLS/GCS, vector stores.</li>
  <li>An <strong>incremental view-maintenance runtime</strong> (<code>IncrementalFlow</code>).</li>
  <li>A <strong>Python binding</strong> and a <strong>CLI</strong>.</li>
</ul>
<p><strong>Pick DataFusion</strong> if you want to embed a SQL engine in a Rust application and you do not need distributed execution, streaming, or IVM. <strong>Pick Krishiv</strong> if you want all of the above and are willing to accept a larger API surface and a less mature codebase.</p>

<h2 id="vs-duckdb">vs. DuckDB</h2>
<p>DuckDB is an excellent single-node analytical SQL engine with mature Parquet, CSV, and JSON support. It is faster than Krishiv for many pure batch workloads on a single host.</p>
<p><strong>Pick DuckDB</strong> for single-node analytical SQL when you do not need streaming, IVM, or distributed execution. <strong>Pick Krishiv</strong> if you need streaming, IVM, or a distributed runtime, or if you want the same APIs from Python, Rust, and SQL.</p>

<h2 id="trade-offs">Honest trade-offs</h2>
<table class="api-table">
  <thead><tr><th>You give up</th><th>You gain</th></tr></thead>
  <tbody>
    <tr><td>Maturity of Spark/Flink in production at scale</td><td>One engine, one API surface, one binary</td></tr>
    <tr><td>DuckDB's single-node analytical performance</td><td>Streaming + IVM in the same runtime</td></tr>
    <tr><td>DataFusion's library-only footprint</td><td>Distributed scheduler, connectors, Python bindings</td></tr>
    <tr><td>JVM tooling and ecosystem</td><td>Rust-native: smaller binary, predictable performance, no JVM tuning</td></tr>
  </tbody>
</table>

<h2 id="check-maturity">Before you commit</h2>
<p>Read the <a href="/product/maturity">Feature Maturity</a> page. Anything marked <em>Preview</em> or <em>Planned</em> is not production-ready today. The <a href="/docs/latest/recipes">recipes</a> show what works end-to-end with current status labels.</p>
`,
  },

  {
    slug: 'tutorial',
    group: 'Getting Started',
    title: 'Your first Krishiv pipeline',
    description: 'A 15-minute end-to-end tutorial: read a file, run a query, write a result.',
    status: 'Available',
    body: `
<p>This tutorial takes you from zero to a working pipeline that reads a CSV file, runs a SQL aggregation, and writes the result back to Parquet. Pick Python or Rust — both are first-class.</p>

<h2 id="prereqs">Prerequisites</h2>
<ul>
  <li><strong>Python:</strong> Python 3.10+, then <code>pip install --no-build-isolation krishiv</code> (or build from source: <code>maturin develop --manifest-path crates/krishiv-python/Cargo.toml</code>).</li>
  <li><strong>Rust:</strong> Rust 1.80+ and the <code>just</code> runner.</li>
  <li>~50 lines of code, no external services.</li>
</ul>

<h2 id="step-1">Step 1 — Create the input</h2>
<p>Create <code>data/orders.csv</code> with the following content (the path is referenced in later steps):</p>
<pre><code class="language-csv">order_id,customer_id,amount,event_time
1,c1,42.50,1700000000
2,c2,15.00,1700000060
3,c1,9.99,1700000120
4,c3,120.00,1700000180
5,c2,7.50,1700000240
6,c1,300.00,1700000300
7,c3,55.25,1700000360
8,c2,12.40,1700000420
</code></pre>

<h2 id="step-2-py">Step 2a — Python pipeline</h2>
<p>Create <code>pipeline.py</code>:</p>
<pre><code class="language-python">import krishiv as ks
from krishiv.functions import col, sum, desc

# 1. Create an in-process session (no daemon, no cluster).
session = ks.Session.embedded()

# 2. Register the CSV as a SQL table.
session.register_csv("orders", "data/orders.csv")

# 3. Run a SQL aggregation.
top_customers = session.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
    ORDER BY total DESC
    LIMIT 5
""")

# 4. Print the result.
print("Top customers by total spend:")
top_customers.show()

# 5. Write the result to a Parquet file.
top_customers.write_parquet("out/top_customers.parquet")
print("Wrote out/top_customers.parquet")
</code></pre>
<p>Run it:</p>
<pre><code class="language-bash">python pipeline.py
</code></pre>
<p>Expected output (numbers match the CSV above):</p>
<pre><code class="language-text">Top customers by total spend:
+--------------+-------+
| customer_id  | total |
+--------------+-------+
| c1           | 352.49|
| c3           | 175.25|
| c2           |  34.90|
+--------------+-------+

Wrote out/top_customers.parquet
</code></pre>

<h2 id="step-2-rs">Step 2b — Rust pipeline</h2>
<p>Create <code>src/main.rs</code>:</p>
<pre><code class="language-rust">use krishiv_api::{col, desc, sum, Session};

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    session.register_csv("orders", "data/orders.csv").await?;

    let df = session.sql("
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
        ORDER BY total DESC
        LIMIT 5
    ").await?;

    println!("Top customers by total spend:");
    df.show().await?;
    df.write_parquet("out/top_customers.parquet", None).await?;
    println!("Wrote out/top_customers.parquet");
    Ok(())
}
</code></pre>
<p>Run it:</p>
<pre><code class="language-bash">cargo run
</code></pre>

<h2 id="step-3">Step 3 — Inspect the result</h2>
<p>Read the Parquet file you just wrote and confirm the schema:</p>
<pre><code class="language-python">import pyarrow.parquet as pq
table = pq.read_table("out/top_customers.parquet")
print(table.schema)
print(table.to_pandas())
</code></pre>

<h2 id="step-4">Step 4 — Modify the query</h2>
<p>Try changing the query. Some ideas:</p>
<ul>
  <li>Add a <code>WHERE amount &gt; 20</code> filter.</li>
  <li>Add <code>COUNT(*) AS n_orders</code> alongside <code>SUM(amount)</code>.</li>
  <li>Switch the DataFrame API for the SQL string (see the <a href="/docs/latest/recipes/dataframe-101">DataFrame 101</a> recipe).</li>
  <li>Read a Parquet file instead of CSV (see <a href="/docs/latest/connectors/parquet">Parquet &amp; Object Store</a>).</li>
  <li>Run the same code against the <code>single-node</code> daemon for restart-durable state (see <a href="/docs/latest/recipes/single-node-deploy">Single-node deployment</a>).</li>
</ul>

<h2 id="what-you-learned">What you learned</h2>
<ul>
  <li><code>Session.embedded()</code> creates an in-process engine — no daemon, no cluster.</li>
  <li><code>register_csv</code> / <code>register_parquet</code> expose a file as a SQL table.</li>
  <li>SQL is parsed and planned by DataFusion and executed by the same runtime as DataFrame and Stream.</li>
  <li>Results are Arrow <code>RecordBatch</code> values, and writes go back through Arrow.</li>
</ul>

<h2 id="next">Next steps</h2>
<ul>
  <li><a href="/docs/latest/recipes">Recipes</a> — task-oriented examples (tumbling windows, Iceberg upserts, Kafka pipelines, etc.)</li>
  <li><a href="/docs/latest/concepts/how-it-executes">How Krishiv executes a query</a> — the journey from <code>session.sql(...)</code> to result</li>
  <li><a href="/product/maturity">Feature Maturity</a> — what is production-ready today</li>
</ul>
`,
  },

  {
    slug: 'getting-started',
    group: 'Getting Started',
    title: 'Getting Started',
    description: 'Build and run your first Krishiv query in embedded mode.',
    status: 'Available',
    body: `
<div class="note-box"><strong>Just want to see it run?</strong> Skip to <a href="#sixty-seconds">60 seconds to your first query</a>. The full setup is below if you need it.</div>

<h2 id="prerequisites">Prerequisites</h2>
<ul>
  <li>Rust 1.80+ (2024 edition)</li>
  <li>Cargo and the <code>just</code> command runner</li>
  <li>Python 3.10+ and <code>maturin</code> for Python bindings</li>
</ul>

<h2 id="sixty-seconds">60 seconds to your first query</h2>
<h3>Python</h3>
<pre><code class="language-bash">pip install --no-build-isolation krishiv
</code></pre>
<pre><code class="language-python">import krishiv as ks
session = ks.Session.embedded()
session.sql("SELECT 42 AS answer").show()
</code></pre>

<h3>Rust</h3>
<pre><code class="language-toml">[dependencies]
krishiv-api = { path = "../crates/krishiv-api" }
tokio = { version = "1", features = ["full"] }
</code></pre>
<pre><code class="language-rust">use krishiv_api::Session;

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;
    session.sql("SELECT 42 AS answer").await?.show().await?;
    Ok(())
}
</code></pre>

<h2 id="first-query">First Query — Embedded Mode</h2>
<p>Embedded mode runs entirely in-process. No daemon or cluster is needed.</p>
<pre><code class="language-rust">use krishiv_api::{Session, Result};

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let result = session.sql("SELECT 42 AS answer").await?.collect().await?;
    println!("{result:?}");
    Ok(())
}
</code></pre>

<h2 id="python-quickstart">Python Quickstart</h2>
<pre><code class="language-bash">maturin develop --manifest-path crates/krishiv-python/Cargo.toml
</code></pre>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()
df = session.sql("SELECT 42 AS answer")
df.show()
</code></pre>

<h2 id="cli">CLI</h2>
<pre><code class="language-bash">cargo run -p krishiv -- sql --query "SELECT 1 AS value"
cargo run -p krishiv -- explain --query "SELECT 1 AS value"
cargo run -p krishiv -- jobs
</code></pre>

<h2 id="validation">Validation Commands</h2>
<pre><code class="language-bash">cargo check --workspace
cargo test --workspace
cargo test -p krishiv-runtime
cargo clippy --workspace --exclude krishiv-python -- -D warnings
cargo fmt --check
</code></pre>

<h2 id="next">Where to go next</h2>
<ul>
  <li><a href="/docs/latest/tutorial">Your first Krishiv pipeline</a> — read a file, run a query, write a result.</li>
  <li><a href="/docs/latest/installation">Installation</a> — Cargo features, build presets, Python extras.</li>
  <li><a href="/docs/latest/concepts/how-it-executes">How Krishiv executes a query</a> — the mental model.</li>
</ul>
`,
  },

  {
    slug: 'installation',
    group: 'Getting Started',
    title: 'Installation',
    description: 'Build features, Cargo feature presets, and Python setup.',
    status: 'Available',
    body: `
<h2 id="rust-features">Rust Feature Presets</h2>
<p>Cargo features select compiled capabilities. They are additive — do not use them as mutually exclusive mode switches.</p>
<table class="api-table">
  <thead><tr><th>Feature</th><th>Purpose</th></tr></thead>
  <tbody>
    <tr><td><code>minimal</code></td><td>Smallest facade; no optional deployment capabilities.</td></tr>
    <tr><td><code>local</code></td><td>Default developer build: embedded + single-node.</td></tr>
    <tr><td><code>embedded</code></td><td>In-process API use; no optional dependencies.</td></tr>
    <tr><td><code>single-node</code></td><td>Local daemon/in-process cluster with Flight SQL, shuffle, RocksDB metadata.</td></tr>
    <tr><td><code>distributed</code></td><td>Remote cluster support with Flight SQL, shuffle, etcd metadata.</td></tr>
    <tr><td><code>k8s</code></td><td>Distributed + Kubernetes operator/CRD.</td></tr>
    <tr><td><code>full</code></td><td>Distributed/k8s, Kafka, primary Iceberg. Excludes AI/vector and secondary lakehouse formats.</td></tr>
  </tbody>
</table>

<h2 id="optional-features">Optional Integration Features</h2>
<table class="api-table">
  <thead><tr><th>Feature</th><th>Purpose</th></tr></thead>
  <tbody>
    <tr><td><code>flight-sql</code></td><td>Arrow Flight SQL transport/server.</td></tr>
    <tr><td><code>shuffle</code></td><td>Shuffle service/store.</td></tr>
    <tr><td><code>etcd</code></td><td>etcd-backed scheduler metadata.</td></tr>
    <tr><td><code>kafka</code></td><td>Kafka connector.</td></tr>
    <tr><td><code>iceberg</code></td><td>Primary lakehouse platform.</td></tr>
    <tr><td><code>delta</code></td><td>Optional experimental Delta compatibility.</td></tr>
    <tr><td><code>ui</code></td><td>Operator UI integration.</td></tr>
  </tbody>
</table>

<h2 id="build-commands">Build Commands</h2>
<pre><code class="language-bash">just check                  # verify all four modes compile
just build-single-node      # debug binary for local dev
just build-bare-metal       # release binary for VMs
just build-k8s              # release binary + operator for Kubernetes

# GCC 15 workaround for RocksDB
CXXFLAGS="-include cstdint" just build-single-node
</code></pre>

<h2 id="python-features">Python Features</h2>
<table class="api-table">
  <thead><tr><th>Feature</th><th>Purpose</th></tr></thead>
  <tbody>
    <tr><td><code>kafka</code></td><td>Kafka sources/connectors.</td></tr>
    <tr><td><code>iceberg</code></td><td>Iceberg lakehouse bindings.</td></tr>
    <tr><td><code>vector-sinks</code></td><td>Optional vector sink compatibility.</td></tr>
    <tr><td><code>qdrant</code></td><td>Experimental Qdrant vector sink.</td></tr>
    <tr><td><code>pgvector</code></td><td>Experimental pgvector sink.</td></tr>
  </tbody>
</table>
<pre><code class="language-bash">maturin develop --manifest-path crates/krishiv-python/Cargo.toml
maturin develop --manifest-path crates/krishiv-python/Cargo.toml --features iceberg
maturin develop --manifest-path crates/krishiv-python/Cargo.toml --features kafka
</code></pre>
`,
  },

  {
    slug: 'concepts/how-it-executes',
    group: 'Concepts',
    title: 'How Krishiv executes a query',
    description: 'A visual walkthrough from session.sql(...) to result — for a single batch SQL query.',
    status: 'Available',
    body: `
<p>This page follows one query — <code>session.sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")</code> — from your code to the result, so the rest of the documentation has a place to anchor.</p>

<h2 id="diagram">At a glance</h2>
<pre><code>Your code
  session.sql("SELECT customer_id, SUM(amount) ... FROM orders ...")
        │
        ▼
[1] SqlEngine.parse()        ── DataFusion SQL → LogicalPlan
        │
        ▼
[2] Krishiv plan + optimizer ── typed Expr AST, predicate pushdown,
        │                          policy/governance hooks
        ▼
[3] ExecutionRuntime.accept_plan()
        │
        ├── Embedded      →  [4a] in-process task graph
        ├── SingleNode    →  [4a] in-process task graph + Flight endpoint
        └── Distributed   →  [4b] remote coordinator gRPC
                                  │
                                  ▼
[5] Coordinator.submit_job()
        │  - job lifecycle, fencing, metadata store
        ▼
[6] Scheduler → Tasks → Executor(s)
        │  - shuffle partitioning, source/sink wiring
        ▼
[7] DataFusion + Arrow operators
        │  - scan, filter, aggregate, window, join, ...
        ▼
[8] Result: Vec&lt;RecordBatch&gt; (or streaming iterator)
</code></pre>

<h2 id="step-by-step">Step by step</h2>

<h3 id="step-1">1. Parse</h3>
<p><code>Session::sql(...)</code> hands the string to a DataFusion <code>SessionContext</code>. The result is a <strong>logical plan</strong> — a tree of relational operators (Scan, Filter, Aggregate, Project, …). At this point nothing has run yet.</p>

<h3 id="step-2">2. Optimize</h3>
<p>Krishiv runs the DataFusion optimizer, then layers its own plan-level transformations: predicate pushdown into source providers, projection pruning, governance/policy hooks, and UDF resolution. The output is a <strong>physical plan</strong> — operators with specific implementations, partitioning, and shuffle requirements.</p>

<h3 id="step-3">3. Route</h3>
<p><code>ExecutionRuntime::accept_plan(plan)</code> decides where the work runs:</p>
<table class="api-table">
  <thead><tr><th>RuntimeMode</th><th>What happens</th></tr></thead>
  <tbody>
    <tr><td><code>Embedded</code></td><td>Build the task graph in-process and run it on the calling thread (or <code>spawn_blocking</code> for DataFusion work).</td></tr>
    <tr><td><code>SingleNode</code></td><td>Same as embedded, but with a Flight SQL endpoint so other clients can attach.</td></tr>
    <tr><td><code>Distributed</code></td><td>Serialize the plan and ship it to the coordinator gRPC endpoint. <strong>No silent fallback</strong> — if the endpoint is unreachable, the call fails.</td></tr>
  </tbody>
</table>

<h3 id="step-4">4. Submit</h3>
<p>The coordinator receives the plan, fragments it into tasks, and assigns each task to an executor. Each executor runs the task on the Arrow operator runtime inside <code>krishiv-dataflow</code> — queues, barriers, windows, stateful joins, all working on <code>RecordBatch</code> values.</p>

<h3 id="step-5">5. Shuffle, state, and checkpoints</h3>
<p>Cross-partition data flows through <code>krishiv-shuffle</code> (in-memory, local disk, or object store, depending on the durability profile). Stateful operators read and write to <code>krishiv-state</code> (in-memory or RocksDB). At configured intervals, the coordinator triggers a checkpoint that snapshots state and source offsets atomically.</p>

<h3 id="step-6">6. Collect</h3>
<p>For a batch query, the runtime gathers the terminal batches and returns them as a <code>Vec&lt;RecordBatch&gt;</code> (Rust) or a list of <code>pa.RecordBatch</code> (Python). For a streaming query, the same pipeline returns a stream / iterator instead.</p>

<h2 id="arrow-everywhere">Arrow everywhere</h2>
<p>The same <code>RecordBatch</code> type flows through every layer — sources, operators, shuffle, and sinks. There is no row-by-row marshalling and no JVM-style boxed objects. This is the main reason Krishiv is competitive with engines that have had years more development time: the columnar format does most of the heavy lifting, and Rust keeps the abstractions cheap.</p>

<h2 id="durability">Where durability fits</h2>
<p>Three explicit profiles control what the runtime uses for metadata, state, shuffle, and checkpoints:</p>
<table class="api-table">
  <thead><tr><th>Profile</th><th>State</th><th>Shuffle</th><th>Checkpoints</th></tr></thead>
  <tbody>
    <tr><td><code>dev-local</code></td><td>In-memory</td><td>In-memory</td><td>Ephemeral</td></tr>
    <tr><td><code>single-node-durable</code></td><td>RocksDB</td><td>Local disk</td><td>Local filesystem</td></tr>
    <tr><td><code>distributed-durable</code></td><td>RocksDB (restored)</td><td>Tiered (local + object store)</td><td>Object store + etcd</td></tr>
  </tbody>
</table>
<p>See the <a href="/docs/latest/concepts/execution-model">Execution Model</a> reference for the full list of env vars and the <a href="/docs/latest/operations/checkpointing">Checkpointing</a> page for the protocol.</p>

<h2 id="streaming">What changes for streaming</h2>
<p>For streaming queries, the same pipeline runs as a long-lived <em>job</em>: a coordinator-owned process that ingests batches from sources (Kafka, memory streams, registered unbounded tables), processes them through the same operator runtime, and emits results. The plan is the same shape — what changes is that operators push their output to downstream operators instead of pulling all input first. See <a href="/docs/latest/python/stream">Python Stream</a> / <a href="/docs/latest/rust/stream">Rust Stream</a>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/concepts/execution-model">Execution Model</a> — <code>RuntimeMode</code>, <code>ExecutionPlacement</code>, durability profiles</li>
  <li><a href="/docs/latest/concepts/architecture">Architecture</a> — crate boundaries and design invariants</li>
  <li><a href="/docs/latest/concepts/distributed-mode">Distributed Mode</a> — what the coordinator and executors actually do</li>
</ul>
`,
  },

  {
    slug: 'concepts/execution-model',
    group: 'Concepts',
    title: 'Execution Model',
    description: 'RuntimeMode, ExecutionPlacement, and how plans move through Krishiv.',
    status: 'Available',
    body: `
<h2 id="runtime-vs-placement">RuntimeMode vs ExecutionPlacement</h2>
<p><code>RuntimeMode</code> is the user-visible mode; <code>ExecutionPlacement</code> describes where data-plane work may actually run. They are intentionally separate.</p>
<table class="api-table">
  <thead><tr><th>RuntimeMode</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>Embedded</code></td><td>In-process; no daemon. Best for tests and local API use.</td></tr>
    <tr><td><code>SingleNode</code></td><td>All engine pieces on one host; may use local daemon or in-process cluster.</td></tr>
    <tr><td><code>Distributed</code></td><td>Remote coordinator/executor transport. Requires an explicit Flight coordinator URL.</td></tr>
  </tbody>
</table>
<div class="note-box"><strong>Note:</strong> Distributed sessions must not silently fall back to in-process execution. An explicit <code>KRISHIV_COORDINATOR</code> endpoint is required.</div>

<h2 id="sync-async-seam">Sync/Async Seam</h2>
<p>The primary sync methods (<code>collect_batch_sql</code>, <code>accept_plan</code>) delegate to async variants (<code>collect_batch_sql_async</code>) via <code>block_on</code> at a single seam. Callers inside Tokio contexts should prefer async variants. Remote runtimes drive Flight/gRPC calls directly in the async path; in-process runtimes off-load DataFusion work to the blocking pool via <code>spawn_blocking</code>.</p>

<h2 id="durability-profiles">Durability Profiles</h2>
<table class="api-table">
  <thead><tr><th>Profile</th><th>State</th><th>Shuffle</th><th>Checkpoints</th></tr></thead>
  <tbody>
    <tr><td><code>dev-local</code></td><td>In-memory</td><td>In-memory</td><td>Ephemeral local; not restart-durable</td></tr>
    <tr><td><code>single-node-durable</code></td><td>RocksDB local</td><td>Local disk</td><td>Local filesystem; restart-durable on one host</td></tr>
    <tr><td><code>distributed-durable</code></td><td>RocksDB (restored from checkpoint)</td><td>Tiered: local + object store</td><td>Object store; etcd metadata; fenced coordination</td></tr>
  </tbody>
</table>

<h2 id="delivery-guarantees">Delivery Guarantees</h2>
<p>The end-to-end delivery guarantee is the <em>weakest</em> guarantee supplied by the source, sink, checkpoint storage, and selected durability profile.</p>
<ul>
  <li><strong>Best effort:</strong> failure can lose or duplicate records.</li>
  <li><strong>At least once:</strong> acknowledged source positions are not advanced before durable output, but replay may duplicate output.</li>
  <li><strong>Effectively once:</strong> deterministic/idempotent sink keys make repeated writes converge on one visible result.</li>
  <li><strong>Exactly once:</strong> source position and sink publication are coordinated by checkpoint protocol and a transactional/two-phase sink. Requires certified source + sink + checkpoint combination.</li>
</ul>
`,
  },

  {
    slug: 'concepts/architecture',
    group: 'Concepts',
    title: 'Architecture',
    description: 'Crate boundaries, runtime routing, and design invariants.',
    status: 'Available',
    body: `
<h2 id="invariants">Architecture Invariants</h2>
<ul>
  <li>Do not build separate engines for batch and streaming.</li>
  <li>One active job coordinator per job; executors are replaceable data-plane workers.</li>
  <li>Shuffle, state, checkpoint, metadata, and connector behavior live behind crate APIs.</li>
  <li>Prefer typed IDs, typed fragments, typed errors, and capability flags over stringly-routed public contracts.</li>
</ul>

<h2 id="plan-flow">Plan Flow</h2>
<pre><code>SQL / API input
  └─ Session.sql() / session.dataframe()
     └─ SqlEngine (DataFusion parse + optimize + plan)
        └─ krishiv-plan: LogicalPlan / PhysicalPlan
           └─ ExecutionRuntime.accept_plan()
              └─ Coordinator.submit_job()
                 └─ Scheduler → tasks → ExecutorTaskRunner
                    └─ Dataflow operators (Arrow, windowing, joins, state)
                       └─ Shuffle, checkpoints, connectors
</code></pre>
<p>For a narrative walkthrough of these steps, see <a href="/docs/latest/concepts/how-it-executes">How Krishiv executes a query</a>.</p>

<h2 id="session-catalog">Session and Catalog</h2>
<p>Each <code>Session</code> owns a <code>SqlEngine</code> backed by a DataFusion <code>SessionContext</code>. The catalog bridges Krishiv's <code>InMemoryCatalog</code> (or an Iceberg <code>KrishivCatalog</code>) into DataFusion's catalog provider interface, making registered tables available to SQL queries.</p>

<h2 id="scheduler-executor">Scheduler / Executor Boundary</h2>
<p>The coordinator is the single authoritative owner of job state. Executors receive task assignments and execute Arrow/DataFusion operators. Task retries may replay records; the coordinator fence prevents stale task completions from being accepted. Executors are stateless between task assignments except for state retrieved from checkpoint/state stores.</p>

<h2 id="distributed-auth">Distributed Auth</h2>
<p>Production coordinator and executor task-control gRPC require bearer-token auth:</p>
<ul>
  <li><code>KRISHIV_COORDINATOR_BEARER_TOKEN</code> — client token sent on every call.</li>
  <li><code>KRISHIV_COORDINATOR_BEARER_TOKENS</code> — comma/newline-separated accepted server tokens for rotation windows.</li>
  <li><code>KRISHIV_COORDINATOR_BEARER_TOKEN_FILE</code> — file-based token for long-lived servers.</li>
  <li><code>KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS</code> — live reload interval for token files.</li>
  <li><code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code> — token for executor gRPC.</li>
</ul>
`,
  },

  {
    slug: 'concepts/distributed-mode',
    group: 'Concepts',
    title: 'Distributed Mode',
    description: 'What the coordinator and executors actually do, and how to operate them.',
    status: 'Preview',
    body: `
<div class="warn-box"><strong>Preview:</strong> Distributed mode has the building blocks in place — Flight coordinator, executor gRPC, bearer-token auth, etcd metadata, object-store shuffle, fenced checkpoints. End-to-end certification work continues; verify your specific workload with the maintainers before relying on it for production.</div>

<h2 id="topology">Topology</h2>
${DIAGRAM_DISTRIBUTED_TOPOLOGY}
<p>The ASCII tree below is the same topology in text form, useful for grepping logs:</p>
<pre><code>Client (Python / Rust / SQL)
   │  Arrow Flight SQL
   ▼
Coordinator  (krishiv-scheduler)
   │  Task-control gRPC (bearer token)
   ▼
Executor(s)  (krishiv-executor)
   │
   ▼
Arrow / DataFusion operators, shuffle, state, sources, sinks
</code></pre>

<h2 id="coordinator">Coordinator responsibilities</h2>
<ul>
  <li>Accept job submissions from clients.</li>
  <li>Fragment plans into tasks and assign them to executors.</li>
  <li>Issue epoch <strong>fence tokens</strong> so stale task completions are rejected (no double-commit).</li>
  <li>Drive checkpoint barriers and persist checkpoint metadata to etcd.</li>
  <li>Surface job status, health, and metrics.</li>
</ul>

<h2 id="executor">Executor responsibilities</h2>
<ul>
  <li>Connect to the coordinator with a bearer token.</li>
  <li>Receive task assignments and execute them on the Arrow operator runtime.</li>
  <li>Report heartbeats and task completions with the current fence token.</li>
  <li>Read/write local RocksDB state and local-disk shuffle when in the <code>distributed-durable</code> profile.</li>
  <li>Use the object-store shuffle backend for cross-host data exchange.</li>
</ul>

<h2 id="explicit-endpoint">An explicit endpoint is required</h2>
<p>Distributed mode will <strong>not</strong> silently fall back to in-process execution. If <code>KRISHIV_COORDINATOR</code> is unset or unreachable, <code>Session::connect(...)</code> and <code>Session::from_env()</code> fail with a configuration error. This is intentional — you should not get distributed-looking results from an in-process engine.</p>

<h2 id="auth">Auth (required for production)</h2>
<table class="api-table">
  <thead><tr><th>Variable</th><th>Purpose</th></tr></thead>
  <tbody>
    <tr><td><code>KRISHIV_COORDINATOR</code></td><td>Coordinator gRPC endpoint.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN</code></td><td>Bearer token sent on every call.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKENS</code></td><td>Accepted server tokens (rotation).</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN_FILE</code></td><td>File-based token, live-reloaded.</td></tr>
    <tr><td><code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code></td><td>Token for executor → coordinator gRPC.</td></tr>
  </tbody>
</table>
<p>See the <a href="/docs/latest/concepts/architecture">Architecture</a> page for the crate-level breakdown.</p>

<h2 id="deployment">Deployment options</h2>
<ul>
  <li><a href="/docs/latest/operations/deployment">Deployment</a> — embedded, single-node daemon, and Kubernetes operator / CRD.</li>
  <li><a href="/docs/latest/recipes/single-node-deploy">Single-node durable recipe</a> — quick local daemon setup.</li>
  <li><a href="/docs/latest/operations/scheduler">Scheduler</a> — coordinator lifecycle and task state machine.</li>
  <li><a href="/docs/latest/operations/shuffle">Shuffle</a> — backends and configuration.</li>
    <li><a href="/docs/latest/operations/checkpointing">Checkpointing</a> — protocol and recovery.</li>
</ul>
`,
  },

  {
    slug: 'concepts/incremental-flow',
    group: 'Concepts',
    title: 'IncrementalFlow',
    description: 'The programmatic API for incremental view maintenance: sources, views, ticks, checkpoints, watches.',
    status: 'Experimental',
    body: `
<p>Where <a href="/docs/latest/sql/incremental-views">Incremental Views</a> are the SQL face of IVM, <code>IncrementalFlow</code> is the Rust face. It gives you programmatic control over sources, views, ticks, checkpoints, and watches. Use it when you need a custom tick trigger, a non-SQL source, or a watch that pushes deltas to another system.</p>
${DIAGRAM_INCREMENTAL_FLOW}

<h2 id="shape">Shape of a flow</h2>
<pre><code class="language-rust">use krishiv_ivm::IncrementalFlow;

let mut flow = IncrementalFlow::new();

// 1. Declare one or more sources (record-batched inputs)
flow.register_source("orders", orders_schema)?;

// 2. Declare views as SQL; the flow orders them topologically
flow.register_view("order_totals",
    "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")?;
flow.register_view("top_customers",
    "SELECT * FROM order_totals ORDER BY total DESC LIMIT 10")?;

// 3. Drive ticks; each tick returns a StepSummary
let summary = flow.tick("orders", delta_batch).await?;
println!("rows_in={} rows_out={} duration_ms={}",
    summary.rows_in, summary.rows_out, summary.duration_ms);

// 4. Read snapshots (current materialised state) or watch for deltas
let snapshot = flow.snapshot("top_customers").await?;
let mut rx = flow.watch_output("top_customers")?;
while let Some(delta) = rx.recv().await { /* … */ }
</code></pre>

<h2 id="step">Step and StepSummary</h2>
<p>Each <code>tick(source, DeltaBatch)</code> call:</p>
<ol>
<li>Integrates the delta into the source's running snapshot (via <code>apply_delta</code>).</li>
<li>Walks the view DAG in topological order.</li>
<li>Diffs each view's full SQL result against its previous output to produce a true <code>DeltaBatch</code>.</li>
<li>Emits the non-empty deltas to watches and downstream views.</li>
</ol>
<p>StepSummary reports <code>rows_in</code> (rows received this tick), <code>rows_out</code> (delta rows emitted), and <code>duration_ms</code>.</p>

<h2 id="delta-batch">DeltaBatch (the data type)</h2>
<p><code>DeltaBatch</code> is a <code>RecordBatch</code> with one extra <code>Int64</code> column named <code>_weight</code> (value <code>+1</code> for inserts, <code>-1</code> for retractions, <code>0</code> for cancelled-by-update). The runtime treats weights &ne; 0 as "row presence" with multiplicity; <code>0</code> weights are dropped before emission.</p>

<h2 id="dirty">Dirty-bit scheduling</h2>
<p>When the planner sees that a view's SQL references no dirty source or upstream view, that view is skipped in the tick. This makes the per-tick cost proportional to the size of the change, not the size of the data.</p>

<h2 id="dedup">Content-addressed dedup</h2>
<p>Opt-in per source. Krishiv hashes each incoming batch; identical back-to-back batches (same content) are coalesced. Capped at <code>DEDUP_SEEN_CAPACITY = 10 000 000</code> entries; older entries are dropped silently and counted via <code>krishiv_dedup_dropped_total</code>.</p>

<h2 id="checkpoint">Checkpoint and restore</h2>
<pre><code class="language-rust">flow.checkpoint("s3://bucket/krishiv/ivm/").await?;
// … time passes, possibly failures …
flow.restore("s3://bucket/krishiv/ivm/").await?;
// the next tick resumes from the restored state
</code></pre>
<p>Delta checkpoints (per source) serialise only the delta since the last checkpoint. The full state is rebuilt by replaying deltas on restore.</p>

<h2 id="checkpoint-full">Full checkpoints (for migrations)</h2>
<p><code>checkpoint_full(path)</code> writes the entire materialised state, not just the delta. Use for cross-version migrations where the delta format may have changed.</p>

<h2 id="force-diff">Coordinator-authoritative remote ticks</h2>
<p>When a flow runs in distributed mode, <code>force_diff_based()</code> makes a remote tick bit-identical to a central tick. The coordinator acquires a per-job <code>step_lock</code>, parallelises the step across executors for partitioned views, and waits for all shards to finish before publishing. On failure it re-feeds the pending delta rather than re-computing from the source.</p>

<h2 id="partitioning">Partitioned flows</h2>
<p><code>PartitionedIncrementalFlow</code> shards the source data by a partition key (typically a hash of the primary key). Each shard is an independent flow; the coordinator merges the deltas. Use for very high-volume sources or when the materialised state does not fit in one executor's memory.</p>

<h2 id="vector">Vector views</h2>
<p>You can register a view whose body is a vector-store query (e.g. "k-nearest neighbours to this embedding"). The view's materialised state is a list of point ids + payloads. <code>IvmVectorSink::spawn_vector_view</code> wires a flow to a vector store; <code>VectorViewSpec</code> configures the distance metric and index parameters.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/incremental-views">Incremental Views (SQL)</a></li>
  <li><a href="/docs/latest/state/timers">Timers</a></li>
  <li><a href="/docs/latest/recipes/live-table">Live Table recipe</a></li>
</ul>
`,
  },

  {
    slug: 'concepts/pipeline-builder',
    group: 'Concepts',
    title: 'Pipeline Builder',
    description: 'Fluent Rust API for sources, views, sinks, CDC, and data-quality expectations.',
    status: 'Available',
    body: `
<p>The Pipeline DSL is the canonical way to compose a multi-stage data flow in Rust. <code>PipelineBuilder</code> is the fluent entry point; <code>Pipeline</code> is the validated, runnable plan.</p>
${DIAGRAM_PIPELINE_BUILDER}

<h2 id="shape">Shape of a pipeline</h2>
<pre><code class="language-rust">use krishiv_api::PipelineBuilder;

let pipeline = PipelineBuilder::new("orders_to_totals")
    .source("orders", source_cdc)             // CDC change stream
    .source("users", source_memory)            // in-memory reference data
    .view("enriched",
          "SELECT o.*, u.tier FROM orders o JOIN users u ON o.user_id = u.id",
          /* materialized = */ true)
    .view("totals",
          "SELECT user_id, SUM(amount) AS total FROM enriched GROUP BY user_id",
          /* materialized = */ true)
    .flow("top", "SELECT * FROM totals ORDER BY total DESC LIMIT 100")  // fan-in via UNION ALL
    .sink_memory("enriched", vec![])         // also tee to memory for tests
    .sink("totals", sink_iceberg)              // primary sink
    .expect("totals", "non_negative_total",
            "total &gt;= 0", OnViolation::Drop)
    .build()?;

pipeline.validate()?;
let report = pipeline.run(RunPolicy::Coalesce).await?;
</code></pre>

<h2 id="modes">Pipeline modes</h2>
<p><code>PipelineMode</code> is auto-inferred from the source kind:</p>
<table class="api-table">
<thead><tr><th>Source</th><th>Mode</th><th>Behavior</th></tr></thead>
<tbody>
<tr><td><code>source_cdc(...)</code></td><td><code>Ivm</code></td><td>Each tick produces a <code>DeltaBatch</code>; views are updated incrementally.</td></tr>
<tr><td><code>source_memory(...)</code></td><td><code>Batch</code></td><td>The whole batch is run once; <code>run</code> blocks until the result is emitted.</td></tr>
<tr><td>Any other</td><td><code>Stream</code></td><td>Continuous query with the configured trigger.</td></tr>
</tbody>
</table>
<p>Override with <code>.mode(PipelineMode::Ivm)</code> when the inference is wrong (e.g. a Kafka source in a non-IVM pipeline).</p>

<h2 id="sources">Sources</h2>
<table class="api-table">
<thead><tr><th>Method</th><th>What it does</th></tr></thead>
<tbody>
<tr><td><code>.source(name, Ingest)</code></td><td>Bounded source: file, in-memory batches, or a SQL subquery.</td></tr>
<tr><td><code>.source_cdc(name, Vec&lt;CdcChange&gt;)</code></td><td>CDC source: <code>insert</code> / <code>delete</code> / <code>update(before, after)</code> records.</td></tr>
<tr><td><code>.source_memory(name, Vec&lt;RecordBatch&gt;)</code></td><td>In-memory batches (tests and one-shots).</td></tr>
</tbody>
</table>
<p>For streaming sources (Kafka, Kinesis, Pulsar) build the flow with the lower-level <code>DataStreamWriter</code> or use <code>Pipeline::run</code> in stream mode against a registered source.</p>

<h2 id="views">Views</h2>
<table class="api-table">
<thead><tr><th>Method</th><th>What it does</th></tr></thead>
<tbody>
<tr><td><code>.view(name, sql, materialized)</code></td><td>Register a view. <code>materialized = true</code> materialises it; <code>false</code> inlines the query on read.</td></tr>
<tr><td><code>.temp_view(name, sql)</code></td><td>A non-materialised view (always inlined). For ad-hoc helpers.</td></tr>
<tr><td><code>.flow(target, sql)</code></td><td>Append a view to another view's <code>UNION ALL</code>. Use for fan-in pipelines.</td></tr>
</tbody>
</table>

<h2 id="sinks">Sinks</h2>
<table class="api-table">
<thead><tr><th>Method</th><th>What it does</th></tr></thead>
<tbody>
<tr><td><code>.sink(view, Egress)</code></td><td>Send a view's output to a sink (Parquet, Iceberg, Kafka, etc.).</td></tr>
<tr><td><code>.sink_memory(view, Arc&lt;Mutex&lt;Vec&lt;RecordBatch&gt;&gt;&gt;)</code></td><td>Send to an in-memory buffer (tests, sampling).</td></tr>
</tbody>
</table>
<p>One pipeline can fan out the same view to multiple sinks.</p>

<h2 id="expectations">Expectations (DLT-style data quality)</h2>
<p>Attach a quality gate to a view:</p>
<pre><code class="language-rust">use krishiv_api::{Expectation, OnViolation};

let exp = Expectation::new("totals", "non_negative_total", "total &gt;= 0", OnViolation::Drop);
let pipeline = builder.expect("totals", "non_negative_total", "total &gt;= 0", OnViolation::Drop).build()?;
</code></pre>
<p><code>OnViolation::Drop</code> removes the offending rows; <code>OnViolation::Fail</code> fails the run. Counters in <code>krishiv_dataquality_dropped_total</code> and <code>krishiv_dataquality_failed_total</code>.</p>

<h2 id="run-policy">RunPolicy</h2>
<p>Controls how the runtime schedules ticks:</p>
<ul>
<li><code>RunPolicy::Coalesce</code> — batch up source updates that arrive within a window. Default for IVM and stream modes.</li>
<li><code>RunPolicy::Immediate</code> — tick on every source update. Lowest latency, highest overhead.</li>
<li><code>RunPolicy::Batched(interval)</code> — tick on a wall-clock interval. Use for backfills.</li>
</ul>
<p>Combine with <code>pipeline.run(policy)</code> to start, or <code>pipeline.refresh(policy)</code> to force a full-state rebuild from sources.</p>

<h2 id="validate">Validation</h2>
<p>Before <code>run</code>, <code>Pipeline::validate()</code> checks:</p>
<ul>
<li>All referenced sources exist.</li>
<li>All view SQL parses.</li>
<li>All sink connectors are available given the active Cargo features.</li>
<li>The inferred mode matches the requested mode.</li>
</ul>
<p>Returns a <code>Vec&lt;ValidationError&gt;</code> (empty on success). Wire this into CI for pre-deployment checks.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/pipeline-ddl">Pipeline DDL</a></li>
  <li><a href="/docs/latest/cli/pipeline">CLI: pipeline</a></li>
  <li><a href="/docs/latest/connectors/quality">Data Quality &amp; Dead Letter</a></li>
</ul>
`,
  },
];
