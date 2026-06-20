import type { DocPage } from '../docs-data';

export const gettingStartedPages: DocPage[] = [
  {
    slug: '',
    group: 'Getting Started',
    title: 'Introduction',
    description: 'Krishiv — one engine for batch SQL, streaming pipelines, and incremental processing.',
    status: 'Available',
    body: `
<h2 id="overview">What is Krishiv?</h2>
<p>Krishiv is a Rust-native compute framework that unifies batch SQL, streaming pipelines, and incremental view maintenance under a single execution model. It uses <strong>Apache Arrow RecordBatch</strong> as the internal columnar data model and <strong>DataFusion</strong> for SQL parsing, planning, expressions, and local execution.</p>
<p>The same session, plan, and scheduler/executor runtime works across embedded (in-process), single-node daemon, and distributed cluster deployments.</p>

<h2 id="key-properties">Key Properties</h2>
<ul>
  <li><strong>Unified execution:</strong> batch and streaming share Arrow batches, planning, runtime routing, and scheduler/executor boundaries.</li>
  <li><strong>Rust-native:</strong> Rust 2024 + Tokio; typed IDs, typed plans, typed errors, explicit durability profiles.</li>
  <li><strong>Three interfaces:</strong> SQL, Rust API (<code>krishiv-api</code>), Python bindings (<code>krishiv-python</code> via PyO3).</li>
  <li><strong>Iceberg-first lakehouse:</strong> Apache Iceberg is the primary certified lakehouse platform.</li>
  <li><strong>Incremental processing:</strong> <code>DeltaBatch</code> (weighted Arrow rows) and <code>IncrementalFlow</code> for incremental view maintenance.</li>
</ul>

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
    slug: 'getting-started',
    group: 'Getting Started',
    title: 'Getting Started',
    description: 'Build and run your first Krishiv query in embedded mode.',
    status: 'Available',
    body: `
<h2 id="prerequisites">Prerequisites</h2>
<ul>
  <li>Rust 1.80+ (2024 edition)</li>
  <li>Cargo and the <code>just</code> command runner</li>
  <li>Python 3.10+ and <code>maturin</code> for Python bindings</li>
</ul>

<h2 id="first-query">First Query — Embedded Mode</h2>
<p>Embedded mode runs entirely in-process. No daemon or cluster is needed.</p>
<pre><code class="language-rust">use krishiv_api::{Session, Result};

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
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
];
