import type { DocPage } from '../docs-data';

export const toolingPages: DocPage[] = [
  {
    slug: 'tooling/ui',
    group: 'Tooling',
    title: 'Operator UI',
    description: 'The /ui dashboard, REST endpoints, auth, and how to enable it with the coordinator.',
    status: 'Available',
    body: `
<p>The operator UI is a server-rendered web app served by the <code>krishiv-ui</code> crate (Askama templates + axum + vendored JS — no CDN, no third-party fetches). It is co-located with the coordinator.</p>

<h2 id="where">Where it lives</h2>
<p>When the coordinator is started by <code>krishiv local</code> or <code>krishiv cluster</code>:</p>
<table class="api-table">
<thead><tr><th>Topology</th><th>URL</th></tr></thead>
<tbody>
<tr><td>Local (<code>krishiv local start</code>)</td><td><code>http://127.0.0.1:2002/ui</code> (default)</td></tr>
<tr><td>Bare-metal cluster (<code>krishiv cluster start</code>)</td><td><code>http://&lt;http-addr&gt;/ui</code> (default <code>127.0.0.1:2002</code>)</td></tr>
<tr><td>Kubernetes (operator CRD)</td><td>Service <code>krishiv-coordinator</code>, port 2002, path <code>/ui</code></td></tr>
</tbody>
</table>

<h2 id="pages">Pages</h2>
<table class="api-table">
<thead><tr><th>Path</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>/ui</code></td><td>Main jobs table with live updates (vendored JS, no WebSocket — 5 s poll).</td></tr>
<tr><td><code>/ui/health</code></td><td>Coordinator and executor health.</td></tr>
<tr><td><code>/ui/metrics</code></td><td>Scheduler-specific metrics in human-readable form.</td></tr>
<tr><td><code>/ui/submit</code></td><td>Submit-job form (used by <code>krishiv submit</code> workflows).</td></tr>
</tbody>
</table>

<h2 id="api">REST API</h2>
<p>All endpoints return JSON. Pagination via <code>?limit=&amp;offset=</code> on list endpoints.</p>
<table class="api-table">
<thead><tr><th>Endpoint</th><th>Returns</th></tr></thead>
<tbody>
<tr><td><code>GET /api/v1/jobs?limit=&amp;offset=</code></td><td>List of <code>JobSummary</code> with id, name, state, row counts, age.</td></tr>
<tr><td><code>GET /api/v1/jobs/{job_id}</code></td><td>Job detail with stages and tasks.</td></tr>
<tr><td><code>GET /api/v1/jobs/{job_id}/checkpoints</code></td><td>List of valid checkpoint epochs.</td></tr>
<tr><td><code>GET /api/v1/executors</code></td><td>List of executors with slots used / total, lost count, last heartbeat.</td></tr>
<tr><td><code>GET /api/v1/queues</code></td><td>Namespace quota snapshot.</td></tr>
<tr><td><code>GET /api/v1/openapi.json</code></td><td>OpenAPI 3.1 spec of the management API.</td></tr>
<tr><td><code>GET /metrics</code></td><td>Prometheus text format.</td></tr>
</tbody>
</table>

<h2 id="auth">Auth</h2>
<p>Set <code>KRISHIV_UI_TOKEN=&lt;token&gt;</code> to require a Bearer token on the UI and all <code>/api/v1</code> endpoints except <code>/healthz</code>. <code>/healthz</code> stays anonymous so liveness probes work. The CLI and daemons pass <code>KRISHIV_COORDINATOR_BEARER_TOKEN</code> for management calls.</p>
<p><strong>Fail-closed:</strong> in production mode (<code>KRISHIV_PRODUCTION=1</code>), starting the coordinator without a UI token is a hard error.</p>

<h2 id="security">Security headers</h2>
<p>Every response carries:</p>
<ul>
<li><code>Content-Security-Policy: script-src 'self'</code> — no inline scripts, no third-party CDN.</li>
<li><code>X-Content-Type-Options: nosniff</code></li>
<li><code>X-Frame-Options: DENY</code> — no clickjacking via iframe.</li>
</ul>

<h2 id="colocation">Co-location with the coordinator</h2>
<p>The UI is built as an axum router. The coordinator spawns it:</p>
<pre><code class="language-rust">let ui = UiState::from_shared_coordinator(shared).with_ui_bearer_token(env::var("KRISHIV_UI_TOKEN").ok());
let router = coordinator_http_router(shared).merge(krishiv_ui::router(ui));
axum::serve(listener, router).await?;
</code></pre>
<p>The same router serves <code>/healthz</code>, <code>/readyz</code>, <code>/metrics</code>, the <code>/api/v1/*</code> management surface, and <code>/ui</code>.</p>

<h2 id="embed">Embedding in another app</h2>
<p>You can mount the UI under a sub-path of your own axum app:</p>
<pre><code class="language-rust">let app = Router::new()
    .nest("/krishiv", krishiv_ui::router(ui_state))
    .route("/healthz", get(my_health));
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
  <li><a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a></li>
</ul>
`,
  },

  {
    slug: 'tooling/benchmarking',
    group: 'Tooling',
    title: 'Benchmarking',
    description: 'TPC-H, Nexmark, distributed benchmarks, and the Python comparison drivers.',
    status: 'Available',
    body: `
<p>Benchmarks live in the <code>krishiv-bench</code> crate (<code>publish = false</code>). Three categories: in-process Criterion benches, distributed CLI binaries, and Python comparison drivers.</p>

<h2 id="criterion">Criterion benches</h2>
<pre><code class="language-bash">cargo bench -p krishiv-bench
</code></pre>
<p>Available benches:</p>
<table class="api-table">
<thead><tr><th>Bench</th><th>What it measures</th></tr></thead>
<tbody>
<tr><td><code>tpch_sf10</code></td><td>TPC-H Q1 at scale factor 10 (single-process).</td></tr>
<tr><td><code>tpch_distributed</code></td><td>TPC-H Q1 in distributed mode (coordinator + executor).</td></tr>
<tr><td><code>nexmark</code></td><td>Nexmark streaming benchmark (auction stream + 6 queries).</td></tr>
</tbody>
</table>

<h2 id="cli">Distributed CLI binaries</h2>
<table class="api-table">
<thead><tr><th>Binary</th><th>What it does</th></tr></thead>
<tbody>
<tr><td><code>k8s_batch</code></td><td>Runs TPC-H Q1 against a remote coordinator. Reads <code>KRISHIV_COORDINATOR_URL</code> (default <code>http://127.0.0.1:30051</code>) and <code>KRISHIV_TPCH_DATA_DIR</code> (default <code>/home/code/krishiv/tpch_sf10/lineitem.parquet</code>).</td></tr>
<tr><td><code>k8s_stream</code></td><td>Distributed streaming benchmark.</td></tr>
<tr><td><code>test_df</code>, <code>test_streaming</code></td><td>Local smoke tests for data-frame and streaming paths.</td></tr>
</tbody></table>
<p>Run with:</p>
<pre><code class="language-bash">just build-bench
cargo run --release -p krishiv-bench --bin k8s_batch
</code></pre>

<h2 id="python">Python comparison drivers</h2>
<p>Located at the top level of the workspace:</p>
<table class="api-table">
<thead><tr><th>Script</th><th>Comparison</th></tr></thead>
<tbody>
<tr><td><code>tpch_benchmark.py</code></td><td>PySpark vs Krishiv on TPC-H Q1/Q3/Q6/Q12/Q14. Requires <code>pip install pyspark</code>.</td></tr>
<tr><td><code>stream_benchmark.py</code></td><td>10M-row tumbling-window throughput comparison.</td></tr>
<tr><td><code>k8s_distributed.py</code></td><td>Driver for distributed runs against a k8s cluster.</td></tr>
</tbody>
</table>

<h2 id="data">Generating TPC-H / Nexmark data</h2>
<p>The bench crate expects data already generated. The <code>tpch_sf10</code> bench reads from <code>KRISHIV_TPCH_DATA_DIR</code>; generate with:</p>
<pre><code class="language-bash"># Using DuckDB's TPC-H extension
duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=10); COPY lineitem TO '/tmp/tpch_sf10/lineitem.parquet' (FORMAT 'parquet');"
</code></pre>

<h2 id="interpreting">Reading the results</h2>
<p>Criterion prints ns/iter, p50, p99, and a stability indicator. Look for:</p>
<ul>
<li>Regression vs the previous run.</li>
<li>Allocation count per iter (visible in the flamegraph output).</li>
<li>Whether the bench is throughput-bound or latency-bound (the &lt;chart&gt; HTML report shows this).</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/tooling/chaos">Chaos Testing</a></li>
  <li><a href="/docs/latest/observability/overview">Observability</a></li>
</ul>
`,
  },

  {
    slug: 'tooling/chaos',
    group: 'Tooling',
    title: 'Chaos Testing',
    description: 'The cross-crate chaos suite — fault injection, recovery, and FMEA.',
    status: 'Available',
    body: `
<p>The <code>krishiv-chaos</code> crate runs a cross-crate suite of fault-injection scenarios. It is excluded from clippy and publish and is the only place where the entire system is exercised end-to-end under deliberate failure.</p>

<h2 id="run">Running the suite</h2>
<pre><code class="language-bash">cargo test -p krishiv-chaos
</code></pre>

<h2 id="scenarios">What it covers</h2>
<table class="api-table">
<thead><tr><th>Scenario</th><th>Failure injected</th><th>Recovery property</th></tr></thead>
<tbody>
<tr><td><code>executor_crash_during_window_agg</code></td><td>Executor killed mid-checkpoint</td><td>Job restarts from last committed epoch. No duplicate output for sinks with exactly-once delivery.</td></tr>
<tr><td><code>coordinator_restart_during_submit</code></td><td>Coordinator restarted while a job is being submitted</td><td>Submit retries idempotently; job either runs once or errors cleanly.</td></tr>
<tr><td><code>network_partition_coordinator_executor</code></td><td>Partition longer than heartbeat timeout</td><td>Executor is marked lost; tasks reassigned; no double-commit thanks to fencing tokens.</td></tr>
<tr><td><code>checkpoint_storage_5xx</code></td><td>Object store returns 5xx during checkpoint</td><td>Checkpoint is retried; on permanent failure, the job fails rather than commit a partial state.</td></tr>
<tr><td><code>ivm_compute_under_partition</code></td><td>Coordinator splits an IVM step across executors during a network partition</td><td>Steps are serialized per-job via <code>step_lock</code>; no double-compute or lost output.</td></tr>
<tr><td><code>shuffle_spill_disk_full</code></td><td>Local shuffle directory runs out of space</td><td>Job fails with a clear error; partial results discarded.</td></tr>
<tr><td><code>kafka_consumer_rebalance</code></td><td>Consumer rebalance mid-batch</td><td>At-least-once delivery preserved; exactly-once sources commit offsets transactionally with the checkpoint.</td></tr>
<tr><td><code>iceberg_commit_conflict</code></td><td>Two writers commit to the same Iceberg snapshot</td><td>Conflict is detected and retried with a fresh snapshot id; no data loss.</td></tr>
</tbody>
</table>

<h2 id="extending">Extending the suite</h2>
<p>To add a new scenario, write a test in <code>crates/krishiv-chaos/tests/</code> and pull in the subsystems you want to fail. The <code>chaos</code> feature on <code>krishiv-common</code> exposes shared fault-injection helpers:</p>
<ul>
<li><code>chaos::kill_executor(executor_id)</code></li>
<li><code>chaos::partition_between(a, b, duration)</code></li>
<li><code>chaos::corrupt_responses_from(a, fraction)</code></li>
<li><code>chaos::slow_io_to(a, latency)</code></li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
  <li><a href="/docs/latest/operations/checkpointing">Checkpointing</a></li>
</ul>
`,
  },

  {
    slug: 'tooling/connector-certification',
    group: 'Tooling',
    title: 'Connector Certification',
    description: 'The normative certification suite, maturity labels, and the delivery-guarantee matrix.',
    status: 'Available',
    body: `
<p>Every connector in <code>krishiv-connectors</code> carries a maturity label and a delivery guarantee. The certification suite is the single source of truth for what is real and what is aspirational.</p>

<h2 id="maturity">Maturity labels</h2>
<table class="api-table">
<thead><tr><th>Label</th><th>Meaning</th></tr></thead>
<tbody>
<tr><td><code>Certified</code></td><td>Passes the full <code>certification.rs</code> suite. Has a confirmed production deployment. Documented in this guide.</td></tr>
<tr><td><code>Preview</code></td><td>Code works, certification suite has run, but no production deployment yet. The API may change.</td></tr>
<tr><td><code>Experimental</code></td><td>Early-stage. May be incomplete. The API will change.</td></tr>
</tbody>
</table>

<h2 id="delivery">Delivery guarantee matrix</h2>
<p>The effective delivery guarantee is the <em>weakest</em> across the source, sink, and durability profile. Combinations outside this table are <strong>unsupported</strong> (the runtime fails the query at submit time with a clear error).</p>
<table class="api-table">
<thead><tr><th>Source</th><th>Sink</th><th>Profile</th><th>Guarantee</th><th>Certified</th></tr></thead>
<tbody>
<tr><td>Parquet (batch)</td><td>Parquet (batch)</td><td>any</td><td>Best effort</td><td>Yes</td></tr>
<tr><td>Parquet (batch)</td><td>Parquet (two-phase)</td><td>distributed-durable</td><td>Effectively once</td><td>Yes</td></tr>
<tr><td>Kafka</td><td>Parquet</td><td>single-node-durable+</td><td>At-least-once</td><td>Yes</td></tr>
<tr><td>Kafka (transactional)</td><td>Parquet (two-phase)</td><td>distributed-durable</td><td>Exactly once</td><td>Yes</td></tr>
<tr><td>Kafka (transactional)</td><td>Kafka (transactional)</td><td>distributed-durable</td><td>Exactly once</td><td>Yes</td></tr>
<tr><td>Kafka (transactional)</td><td>Iceberg (two-phase)</td><td>distributed-durable</td><td>Exactly once</td><td>Yes</td></tr>
<tr><td>Iceberg (batch)</td><td>Iceberg (batch)</td><td>any</td><td>Best effort</td><td>Yes</td></tr>
<tr><td>Delta (batch, local)</td><td>Delta (batch)</td><td>any</td><td>Best effort</td><td>Yes (local fs only)</td></tr>
<tr><td>Delta (batch, local)</td><td>Delta (two-phase)</td><td>distributed-durable</td><td>Effectively once</td><td>Yes (local fs only)</td></tr>
<tr><td>Hudi (batch, local)</td><td>Hudi (two-phase)</td><td>distributed-durable</td><td>Effectively once</td><td>Yes (local fs only)</td></tr>
<tr><td>In-memory stream</td><td>In-memory sink</td><td>any</td><td>Best effort</td><td>Yes</td></tr>
<tr><td>Vector sinks</td><td>—</td><td>any</td><td>Best effort</td><td>No (Experimental)</td></tr>
</tbody>
</table>

<h2 id="suite">The certification suite</h2>
<p>Lives at <code>crates/krishiv-connectors/certification.rs</code> (gated <code>#[cfg(test)]</code>). Runs the canonical failure-and-recovery matrix for every <code>Certified</code> connector:</p>
<ol>
<li>Source reconnect after connection loss.</li>
<li>Source offset commit before the batch is acked.</li>
<li>Sink write-failure rollback and retry.</li>
<li>Two-phase commit prepare → coordinator crash → restart → resolve.</li>
<li>Schema-evolution compatibility (added, dropped, widened columns).</li>
<li>Late-event handling for sources with event time.</li>
</ol>

<p>Run with:</p>
<pre><code class="language-bash">cargo test -p krishiv-connectors --features='lakehouse kafka iceberg' --test certification
</code></pre>

<h2 id="add">Adding a new connector</h2>
<ol>
<li>Implement <code>SourceDriver</code> and <code>SinkDriver</code> in <code>crates/krishiv-connectors/src/&lt;kind&gt;.rs</code>.</li>
<li>Register the kind in <code>ConnectorKind</code> and add a capabilities profile in <code>registry/capabilities.rs</code>.</li>
<li>Add a certification test to <code>certification.rs</code> covering the failures you intend to handle.</li>
<li>Bump the maturity from <code>Preview</code> to <code>Certified</code> only after the suite passes in CI for 4 weeks and you have a production deployment.</li>
</ol>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/overview">Connectors Overview</a></li>
  <li><a href="/docs/latest/connectors/two-phase-commit">Two-phase commit</a></li>
</ul>
`,
  },
];
