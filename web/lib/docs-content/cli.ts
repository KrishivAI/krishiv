import type { DocPage } from '../docs-data';

export const cliPages: DocPage[] = [
  {
    slug: 'cli/overview',
    group: 'CLI Reference',
    title: 'CLI Overview',
    description: 'The krishiv binary, subcommands, global flags, and routing to remote coordinators.',
    status: 'Available',
    body: `
<p>The <code>krishiv</code> binary is the canonical user interface for everything that does not need an SDK. It handles one-shot SQL, continuous streaming jobs, pipeline projects, state inspection, savepoints, and the lifecycle of local and bare-metal clusters.</p>

<h2 id="help">Discovering commands</h2>
<pre><code class="language-bash">krishiv --help
krishiv help sql            # topic-scoped help
krishiv help stream
krishiv help cluster
</code></pre>

<h2 id="global">Global flags</h2>
<table class="api-table">
<thead><tr><th>Flag / env</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>-c, --coordinator &lt;URL&gt;</code> / <code>KRISHIV_COORDINATOR</code></td><td>Route <code>state</code>, <code>savepoint</code>, <code>restore</code>, <code>checkpoints</code> commands to a remote coordinator.</td></tr>
<tr><td><code>-V, --version</code></td><td>Print version and exit.</td></tr>
<tr><td><code>-h, --help</code></td><td>Print help and exit.</td></tr>
<tr><td><code>KRISHIV_PRODUCTION</code></td><td>Fail-closed on unsafe overrides. See <a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a>.</td></tr>
</tbody>
</table>

<h2 id="routing">Routing a command to a remote coordinator</h2>
<p>Several commands (<code>state</code>, <code>savepoint</code>, <code>restore</code>, <code>checkpoints</code>) can run against either the embedded local runtime or a remote coordinator. Pick the target with <code>-c</code> or <code>KRISHIV_COORDINATOR</code>:</p>
<pre><code class="language-bash"># Local (embedded)
krishiv savepoint --job my-pipeline --label before-v2

# Remote
krishiv -c http://coord.internal:50051 savepoint --job my-pipeline --label before-v2
KRISHIV_COORDINATOR=http://coord.internal:50051 krishiv state inspect --job my-pipeline
</code></pre>

<h2 id="commands">All subcommands at a glance</h2>
<table class="api-table">
<thead><tr><th>Command</th><th>What it does</th></tr></thead>
<tbody>
<tr><td><code>sql</code></td><td>Run a SQL statement. <a href="/docs/latest/cli/sql">details</a></td></tr>
<tr><td><code>explain</code></td><td>Run a SQL statement and print logical / physical plan.</td></tr>
<tr><td><code>stream submit / push / poll</code></td><td>Run a continuous window job in-process. <a href="/docs/latest/cli/stream">details</a></td></tr>
<tr><td><code>table read</code></td><td>Read a Parquet, Delta, or Hudi file. <a href="/docs/latest/cli/table">details</a></td></tr>
<tr><td><code>pipeline init / dry-run / run</code></td><td>Scaffold and execute a pipeline project. <a href="/docs/latest/cli/pipeline">details</a></td></tr>
<tr><td><code>submit</code></td><td>Submit a job to the in-process R2 scheduler (used by benchmarks and tests).</td></tr>
<tr><td><code>jobs [--distributed]</code></td><td>List running and recent jobs.</td></tr>
<tr><td><code>state inspect</code></td><td>Dump per-operator state for a job.</td></tr>
<tr><td><code>savepoint</code></td><td>Trigger a named savepoint.</td></tr>
<tr><td><code>restore</code></td><td>Restart a job from a checkpoint or savepoint.</td></tr>
<tr><td><code>checkpoints list</code></td><td>List valid checkpoint epochs for a job.</td></tr>
<tr><td><code>local start|stop|restart|status</code></td><td>Manage a local Spark-like cluster. <a href="/docs/latest/cli/cluster">details</a></td></tr>
<tr><td><code>cluster start|stop|restart|status|verify-network</code></td><td>Manage a bare-metal cluster. <a href="/docs/latest/cli/cluster">details</a></td></tr>
<tr><td><code>coordinator</code></td><td>Run the active coordinator daemon.</td></tr>
<tr><td><code>clusterd</code></td><td>Run the cluster control plane (co-locates coordinator gRPC + HTTP + optional UI + optional Flight SQL).</td></tr>
<tr><td><code>job-coordinator</code></td><td>Run a per-job coordinator for very-large-job sharding.</td></tr>
<tr><td><code>executor</code></td><td>Run an executor.</td></tr>
<tr><td><code>flight-server</code></td><td>Run a standalone Flight SQL server. (Requires <code>flight-sql</code> feature.)</td></tr>
<tr><td><code>shuffle-svc</code></td><td>Run a standalone external shuffle service. (Requires <code>shuffle</code> feature.)</td></tr>
</tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/cli/sql">SQL &amp; Explain</a></li>
  <li><a href="/docs/latest/cli/stream">Stream</a></li>
  <li><a href="/docs/latest/cli/table">Table</a></li>
  <li><a href="/docs/latest/cli/pipeline">Pipeline</a></li>
  <li><a href="/docs/latest/cli/cluster">Local &amp; Cluster</a></li>
  <li><a href="/docs/latest/cli/state-and-checkpoints">State &amp; Checkpoints</a></li>
  <li><a href="/docs/latest/operations/deployment">Deployment</a></li>
</ul>
`,
  },

  {
    slug: 'cli/sql',
    group: 'CLI Reference',
    title: 'SQL & Explain',
    description: 'krishiv sql and krishiv explain flags, --mode, --local, --remote, --timeout, --api-key.',
    status: 'Available',
    body: `
<p>Both commands share the same flags. <code>explain</code> additionally prints the logical and physical plan.</p>

<h2 id="usage">Usage</h2>
<pre><code class="language-bash">krishiv sql --query "SELECT 42 AS answer"
krishiv sql --local --mode single-node --query "SELECT 1"
krishiv sql --remote -c http://127.0.0.1:50051 --query "SELECT 1"
krishiv sql --api-key dev-key --query "SELECT * FROM people"
krishiv explain --query "SELECT * FROM orders WHERE amount &gt; 100"
</code></pre>

<h2 id="flags">Flags</h2>
<table class="api-table">
<thead><tr><th>Flag</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>-q, --query &lt;SQL&gt;</code> (required)</td><td>The statement to run. Multi-statement is supported with <code>;</code> separator; only the last statement's result is printed.</td></tr>
<tr><td><code>--mode &lt;embedded|single-node|distributed&gt;</code></td><td>Override the runtime mode for this query. Default: <code>embedded</code>.</td></tr>
<tr><td><code>--local</code></td><td>Shortcut for <code>--mode single-node</code>. Also uses <code>Session::execute_local</code> instead of <code>Session::sql</code>, which never routes to a remote coordinator even if one is configured.</td></tr>
<tr><td><code>--remote</code></td><td>Use <code>Session::execute_remote</code>. Requires a configured coordinator (<code>KRISHIV_COORDINATOR</code> or <code>-c</code>). Fails with a clear error if no coordinator is reachable.</td></tr>
<tr><td><code>--timeout &lt;SECS&gt;</code></td><td>Per-query timeout in seconds. Default: 30.</td></tr>
<tr><td><code>--api-key &lt;KEY&gt;</code></td><td>Authenticate the query with a static API key. Server-side keys are configured via <code>KRISHIV_API_KEYS=key1=user,key2=svc,...</code>.</td></tr>
<tr><td><code>--parquet &lt;table=path&gt;</code></td><td>Register a Parquet file as a SQL table before running the query. Repeatable.</td></tr>
</tbody>
</table>

<h2 id="multi">Multi-statement</h2>
<p><code>--query</code> can contain multiple statements separated by <code>;</code>. Only the last statement's result is printed. To capture intermediate results, run them as separate invocations.</p>

<h2 id="output">Output</h2>
<table class="api-table">
<thead><tr><th>Statement</th><th>Output</th></tr></thead>
<tbody>
<tr><td><code>SELECT</code></td><td>ASCII table with up to <code>--limit</code> rows.</td></tr>
<tr><td><code>EXPLAIN</code> (any statement)</td><td>Plan text after the result.</td></tr>
<tr><td><code>CREATE</code> / <code>DROP</code></td><td>"OK" with timing.</td></tr>
<tr><td><code>INSERT</code> / <code>MERGE</code> / <code>UPDATE</code> / <code>DELETE</code></td><td>Affected row count and timing.</td></tr>
<tr><td><code>START PIPELINE</code></td><td>Sink output (memory, console) and the running query ID.</td></tr>
</tbody>
</table>

<h2 id="env">Environment variables</h2>
<table class="api-table">
<thead><tr><th>Variable</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>KRISHIV_COORDINATOR</code></td><td>Default coordinator URL for <code>--remote</code>.</td></tr>
<tr><td><code>KRISHIV_MODE</code></td><td>Default mode for the CLI (overridden by <code>--mode</code>).</td></tr>
<tr><td><code>KRISHIV_REMOTE_EXEC</code></td><td><code>1</code> / <code>true</code> / <code>yes</code> / <code>on</code> forces <code>execute_remote</code>.</td></tr>
<tr><td><code>KRISHIV_API_KEYS</code></td><td>Server-side: comma-separated <code>key=user</code> pairs for API key auth.</td></tr>
</tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/cli/overview">CLI Overview</a></li>
  <li><a href="/docs/latest/sql">SQL Reference</a></li>
</ul>
`,
  },

  {
    slug: 'cli/stream',
    group: 'CLI Reference',
    title: 'Stream',
    description: 'krishiv stream submit / push / poll — continuous window jobs in-process.',
    status: 'Available',
    body: `
<p><code>krishiv stream</code> runs a continuous window job entirely in the CLI process. It is intended for local development, smoke tests, and streaming CLI demos — not for production. For production, use the coordinator.</p>

<h2 id="usage">Usage</h2>
<pre><code class="language-bash"># 1. Submit a job (returns a name to use in push/poll)
krishiv stream submit --job-id events \
  --window tumbling --window-size-ms 60000 \
  --key-column user_id --event-time-column ts

# 2. Push batches (Parquet files)
krishiv stream push --job-id events --parquet ./batch_001.parquet
krishiv stream push --job-id events --parquet ./batch_002.parquet

# 3. Poll for results
krishiv stream poll --job-id events
</code></pre>

<h2 id="submit-flags">submit flags</h2>
<table class="api-table">
<thead><tr><th>Flag</th><th>Default</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>--job-id &lt;ID&gt;</code> (required)</td><td>—</td><td>Unique job ID. Use the same ID for <code>push</code> and <code>poll</code>.</td></tr>
<tr><td><code>--window &lt;tumbling|sliding|session&gt;</code></td><td><code>tumbling</code></td><td>Window type.</td></tr>
<tr><td><code>--window-size-ms &lt;MS&gt;</code></td><td><code>60000</code></td><td>Window size.</td></tr>
<tr><td><code>--slide-ms &lt;MS&gt;</code></td><td><code>30000</code></td><td>Slide (sliding windows only).</td></tr>
<tr><td><code>--session-gap-ms &lt;MS&gt;</code></td><td><code>5000</code></td><td>Inactivity gap (session windows only).</td></tr>
<tr><td><code>--key-column &lt;COL&gt;</code></td><td><code>user_id</code></td><td>Key column for the windowed aggregation.</td></tr>
<tr><td><code>--event-time-column &lt;COL&gt;</code></td><td><code>ts</code></td><td>Event-time column for watermarks.</td></tr>
<tr><td><code>--watermark-lag-ms &lt;MS&gt;</code></td><td><code>0</code></td><td>Allowed lateness in milliseconds.</td></tr>
</tbody>
</table>

<h2 id="push-flags">push flags</h2>
<table class="api-table">
<thead><tr><th>Flag</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>--job-id &lt;ID&gt;</code> (required)</td><td>Must match a previously submitted job.</td></tr>
<tr><td><code>--parquet &lt;PATH&gt;</code> (required)</td><td>Path to a Parquet file. Rows are read into <code>RecordBatch</code>es and pushed to the job's queue.</td></tr>
</tbody>
</table>

<h2 id="poll-flags">poll flags</h2>
<table class="api-table">
<thead><tr><th>Flag</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>--job-id &lt;ID&gt;</code> (required)</td><td>Must match a previously submitted job.</td></tr>
</tbody>
</table>
<p>Poll drains the accumulated output and pretty-prints it. The CLI does not checkpoint <code>stream</code> jobs; closing the process loses the state.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/streaming/windows-and-watermarks">Windows and Watermarks</a></li>
  <li><a href="/docs/latest/cli/overview">CLI Overview</a></li>
</ul>
`,
  },

  {
    slug: 'cli/table',
    group: 'CLI Reference',
    title: 'Table',
    description: 'krishiv table read — scan Parquet, Delta, or Hudi files from the command line.',
    status: 'Available',
    body: `
<p><code>krishiv table read</code> is the fastest way to peek at a lakehouse file without starting a session.</p>

<h2 id="usage">Usage</h2>
<pre><code class="language-bash">krishiv table read --path /var/data/orders.parquet --format parquet --limit 20

krishiv table read --path s3://bucket/path/to/table/ --format delta --version 4

krishiv table read --path /var/hudi/orders/ --format hudi --hudi-query incremental --hudi-begin 20240101000000
</code></pre>

<h2 id="flags">Flags</h2>
<table class="api-table">
<thead><tr><th>Flag</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>--path &lt;PATH&gt;</code> (required)</td><td>Path. Local filesystem or object-store URI (S3/ADLS/GCS).</td></tr>
<tr><td><code>--format &lt;FORMAT&gt;</code> (required)</td><td><code>parquet</code>, <code>delta</code>, or <code>hudi</code>.</td></tr>
<tr><td><code>--version &lt;N&gt;</code></td><td>(Delta) Specific version to read.</td></tr>
<tr><td><code>--hudi-query &lt;snapshot|incremental&gt;</code></td><td>(Hudi) Default: <code>snapshot</code>.</td></tr>
<tr><td><code>--hudi-begin &lt;INSTANT&gt;</code></td><td>(Hudi incremental) Begin instant.</td></tr>
<tr><td><code>--limit &lt;N&gt;</code></td><td>Max rows to print. Default: unbounded (use Ctrl-C to stop).</td></tr>
</tbody>
</table>

<div class="warn-box"><strong>Preview:</strong> Delta and Hudi read local filesystem paths only. S3 and remote catalog reads are in flight.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/parquet">Parquet connector</a></li>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg connector</a></li>
  <li><a href="/docs/latest/cli/overview">CLI Overview</a></li>
</ul>
`,
  },

  {
    slug: 'cli/pipeline',
    group: 'CLI Reference',
    title: 'Pipeline',
    description: 'krishiv pipeline init / dry-run / run — the .sql project workflow.',
    status: 'Available',
    body: `
<p>A pipeline project is a directory of <code>.sql</code> files containing <code>CREATE SOURCE</code>, <code>CREATE INCREMENTAL VIEW</code>, <code>CREATE SINK</code>, and <code>START PIPELINE</code> statements. The CLI loads, validates, and runs them as a unit.</p>

<h2 id="init">init — scaffold a project</h2>
<pre><code class="language-bash">krishiv pipeline init --name my-pipeline --dir ./my-pipeline
</code></pre>
<p>Produces:</p>
<pre><code class="language-text">my-pipeline/
├── pipeline.sql          # entry point (sources → views → sinks)
├── sources/
│   └── orders.sql
├── views/
│   └── order_totals.sql
└── sinks/
    └── totals_to_iceberg.sql
</code></pre>
<p>Edit the scaffolded files, then run.</p>

<h2 id="dry-run">dry-run — validate without executing</h2>
<pre><code class="language-bash">krishiv pipeline dry-run ./my-pipeline
</code></pre>
<p>Loads every <code>.sql</code> file, splits on <code>;</code>, ignores <code>--</code> comment lines, normalises whitespace, and validates each statement against the live catalogs. Does not execute <code>START PIPELINE</code>.</p>

<h2 id="run">run — execute</h2>
<pre><code class="language-bash">krishiv pipeline run ./my-pipeline
krishiv pipeline run ./my-pipeline --full-refresh    # reset IVM first
</code></pre>
<p>Runs all statements in order. <code>--full-refresh</code> calls <code>reset_ivm_job(name)</code> for every incremental view before the first tick. <code>START PIPELINE</code> returns the sink's memory output (for sinks that emit to memory); use <code>--full-refresh</code> to force a state reset.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/pipeline-ddl">Pipeline DDL</a></li>
  <li><a href="/docs/latest/recipes/parquet-aggregation">Parquet → SQL aggregation recipe</a></li>
  <li><a href="/docs/latest/cli/overview">CLI Overview</a></li>
</ul>
`,
  },

  {
    slug: 'cli/cluster',
    group: 'CLI Reference',
    title: 'Local & Cluster',
    description: 'krishiv local / cluster — managing single-node and bare-metal deployments.',
    status: 'Available',
    body: `
<p>Two lifecycle commands for managing a self-hosted Krishiv process tree. Both use a JSON file under <code>.krishiv/</code> to track PIDs and config.</p>

<h2 id="local">krishiv local</h2>
<p>Spawns a coordinator, a Flight SQL sidecar, and an executor on one machine. UI on <code>http://&lt;http-addr&gt;/ui</code>.</p>
<pre><code class="language-bash">krishiv local start
krishiv local status
krishiv local stop
krishiv local restart
</code></pre>
<p>Env vars: <code>KRISHIV_LOCAL_DATA_DIR</code> (default <code>.krishiv/local</code>), <code>KRISHIV_LOCAL_HTTP_ADDR</code> (default <code>127.0.0.1:2002</code>).</p>
<p>Persists <code>&lt;data_dir&gt;/cluster.json</code> with the assigned ports and PIDs.</p>

<h2 id="cluster">krishiv cluster</h2>
<p>Bare-metal cluster control plane + N executors. <code>clusterd</code> listens on <code>127.0.0.1:2001</code> (gRPC) and <code>127.0.0.1:2002</code> (HTTP / UI). Executors bind to <code>(2005 + 2i, 2006 + 2i)</code> so adjacent executors never collide.</p>
<pre><code class="language-bash">krishiv cluster start --executors 4
krishiv cluster status
krishiv cluster verify-network
krishiv cluster stop
</code></pre>
<p>Flags: <code>--data-dir &lt;DIR&gt;</code>, <code>--executors &lt;N&gt;</code> (default 2), <code>--http-addr &lt;HOST:PORT&gt;</code>. Env: <code>KRISHIV_CLUSTER_DATA_DIR</code> (default <code>.krishiv/cluster</code>), <code>KRISHIV_CLUSTER_HTTP_ADDR</code> (default <code>127.0.0.1:2002</code>).</p>

<h2 id="daemon">Daemon subcommands</h2>
<p>For finer control, run the daemons directly. Each is the long-running process that <code>local</code> and <code>cluster</code> manage.</p>
<table class="api-table">
<thead><tr><th>Daemon</th><th>What it is</th></tr></thead>
<tbody>
<tr><td><code>krishiv coordinator</code></td><td>Active coordinator. gRPC + HTTP + optional UI + optional Flight SQL sidecar.</td></tr>
<tr><td><code>krishiv clusterd</code></td><td>Cluster control plane — coordinator + leader election + UI + optional Flight SQL.</td></tr>
<tr><td><code>krishiv job-coordinator</code></td><td>Per-job coordinator for very-large-job sharding. Multiple instances can run for one job.</td></tr>
<tr><td><code>krishiv executor</code></td><td>Data-plane worker. Connects to the coordinator gRPC and pulls task assignments.</td></tr>
<tr><td><code>krishiv flight-server</code></td><td>Standalone Arrow Flight SQL server. Env: <code>KRISHIV_FLIGHT_ADDR</code> (default <code>127.0.0.1:2003</code>).</td></tr>
<tr><td><code>krishiv shuffle-svc</code></td><td>External shuffle service. Env: <code>KRISHIV_SHUFFLE_ADDR</code> (default <code>0.0.0.0:2004</code>), <code>KRISHIV_SHUFFLE_DIR</code>.</td></tr>
</tbody></table>
<p>If you build with the <code>flight-sql</code> or <code>shuffle</code> feature off, the corresponding daemon prints <em>build with feature flight-sql</em> / <em>build with feature shuffle</em> and exits 2.</p>

<h2 id="ui">UI on local / cluster</h2>
<p>When started via <code>local start</code> or <code>cluster start</code>, the operator UI is mounted at <code>http://&lt;http-addr&gt;/ui</code>. <a href="/docs/latest/tooling/ui">UI</a> for routes and security.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/operations/deployment">Deployment</a></li>
  <li><a href="/docs/latest/tooling/ui">UI</a></li>
  <li><a href="/docs/latest/cli/overview">CLI Overview</a></li>
</ul>
`,
  },

  {
    slug: 'cli/state-and-checkpoints',
    group: 'CLI Reference',
    title: 'State & Checkpoints',
    description: 'krishiv state / savepoint / restore / checkpoints / jobs — operational commands.',
    status: 'Available',
    body: `
<p>Operational commands for inspecting and managing long-running jobs. Most can run against either the embedded local runtime or a remote coordinator.</p>

<h2 id="jobs">jobs</h2>
<pre><code class="language-bash">krishiv jobs                        # local jobs
krishiv --distributed jobs          # queries the coordinator for its view
</code></pre>
<p>Returns a tabular dump of jobs with ID, name, state, and row counts.</p>

<h2 id="state">state inspect</h2>
<pre><code class="language-bash"># Local
krishiv state inspect --job my-pipeline --operator my-operator

# Remote
krishiv -c http://coord.internal:50051 state inspect --job my-pipeline --operator my-operator

# Read state from a custom storage path (e.g. for forensic inspection)
krishiv state inspect --job my-pipeline --operator my-operator --storage-path /var/krishiv/inspect
</code></pre>
<p>Dumps the per-key values for a single operator, with type, key (hashed), and value bytes. Sensitive values are redacted unless you opt in.</p>

<h2 id="savepoint">savepoint</h2>
<pre><code class="language-bash">krishiv savepoint --job my-pipeline --label before-v2
</code></pre>
<p>Triggers a savepoint with the given label. The savepoint is stored under the job's checkpoint base directory.</p>

<h2 id="restore">restore</h2>
<pre><code class="language-bash"># From a specific epoch
krishiv restore --job my-pipeline --epoch 42

# From a savepoint
krishiv restore --job my-pipeline --epoch 17 --storage-path s3://bucket/savepoints/17
</code></pre>
<p>Restarts the job from the given epoch. The runtime replays from the chosen checkpoint; the original (running) instance is cancelled.</p>

<h2 id="checkpoints">checkpoints list</h2>
<pre><code class="language-bash">krishiv checkpoints list --job my-pipeline
krishiv -c http://coord.internal:50051 checkpoints list --job my-pipeline --storage-path s3://bucket/krishiv/
</code></pre>
<p>Lists valid (complete, integrity-checked) checkpoint epochs for a job.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/state/savepoints-and-migration">Savepoints and Migration</a></li>
  <li><a href="/docs/latest/operations/checkpointing">Checkpointing</a></li>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
</ul>
`,
  },
];
