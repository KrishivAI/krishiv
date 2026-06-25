import type { DocPage } from '../docs-data';

export const observabilityPages: DocPage[] = [
  {
    slug: 'observability/overview',
    group: 'Observability',
    title: 'Observability Overview',
    description: 'Metrics, logs, traces, and health endpoints — wired in via the krishiv_metrics crate.',
    status: 'Available',
    body: `
<p>Every Krishiv process — CLI, daemon, executor, even a Python session — initializes the same observability stack on startup. This page is the index; details live in the sub-pages.</p>

<h2 id="init">Initialization</h2>
<p>Called from <code>krishiv/main.rs</code> and every daemon entry point:</p>
<pre><code class="language-rust">use krishiv_metrics::{init, MetricsConfig, TracerExporter};

let cfg = MetricsConfig {
    service_name: "krishiv-coordinator".into(),
    otlp_endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
    log_filter: "info,krishiv_scheduler=debug".into(),
    exporter: TracerExporter::Otlp,
    ..Default::default()
};
let _handle = init(cfg)?;
</code></pre>

<p>Defaults: <code>service_name = "krishiv"</code>, no OTLP endpoint, log filter is <code>RUST_LOG</code> or <code>info</code>.</p>

<h2 id="three-pillars">Three pillars</h2>
<table class="api-table">
<thead><tr><th>Pillar</th><th>Where it lives</th><th>Where you read it</th></tr></thead>
<tbody>
<tr><td><strong>Metrics</strong></td><td>In-process <code>KrishivMetrics</code> singleton; rendered as Prometheus text.</td><td><code>GET /metrics</code>, Grafana, OTLP collector</td></tr>
<tr><td><strong>Logs</strong></td><td><code>tracing-subscriber</code> with env-filter and FMT or JSON output.</td><td>stdout / journald / your log shipper</td></tr>
<tr><td><strong>Traces</strong></td><td>OpenTelemetry via <code>tracing-opentelemetry</code>; W3C trace-context over gRPC metadata.</td><td>OTLP collector, Jaeger, Tempo</td></tr>
</tbody>
</table>

<h2 id="surface">Public surface</h2>
<ul>
<li><code>krishiv_metrics::MetricsConfig</code> / <code>TracerExporter::{Otlp, Stdout, InMemory}</code></li>
<li><code>krishiv_metrics::init(config) -&gt; Result&lt;MetricsHandle&gt;</code></li>
<li><code>krishiv_metrics::current_traceparent() -&gt; Option&lt;String&gt;</code> / <code>current_tracestate()</code></li>
<li><code>krishiv_metrics::global_metrics() -&gt; &amp;'static KrishivMetrics</code></li>
<li><code>krishiv_metrics::render_prometheus() -&gt; String</code></li>
<li><code>krishiv_metrics::system_metrics() -&gt; &amp;'static SystemMetrics</code></li>
<li>Typed reports: <code>ObservabilityReport</code> and its <code>ReportJob</code>, <code>ReportTask</code>, <code>ReportExecutor</code>, <code>ReportCheckpoint</code>, <code>ReportShuffle</code>, <code>ReportStreamingState</code> sub-types — for programmatic status surfaces.</li>
</ul>

<h2 id="env">Environment variables</h2>
<table class="api-table">
<thead><tr><th>Variable</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>OTEL_EXPORTER_OTLP_ENDPOINT</code></td><td>Enables OTLP trace export to this endpoint.</td></tr>
<tr><td><code>KRISHIV_PRODUCTION</code></td><td>Set to anything truthy to fail-closed on unsafe overrides (alpha API, anonymous HTTP, manual Kafka commit).</td></tr>
<tr><td><code>KRISHIV_METRICS_PORT</code></td><td>Exposes a dedicated <code>/metrics</code> HTTP endpoint on this port in addition to the one on the coordinator HTTP server.</td></tr>
<tr><td><code>RUST_LOG</code></td><td>Log filter, e.g. <code>info,krishiv_scheduler=debug,sqlx=warn</code>.</td></tr>
</tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/metrics">Metrics</a></li>
  <li><a href="/docs/latest/observability/logs">Logs</a></li>
  <li><a href="/docs/latest/observability/tracing">Distributed Tracing</a></li>
  <li><a href="/docs/latest/observability/health">Health &amp; Status</a></li>
</ul>
`,
  },

  {
    slug: 'observability/metrics',
    group: 'Observability',
    title: 'Metrics',
    description: 'Process-wide KrishivMetrics singleton, Prometheus text, and the OTLP push path.',
    status: 'Available',
    body: `
<p>Metrics live in a single process-wide <code>KrishivMetrics</code> struct (one per process — coordinator, executor, CLI). The struct carries ~120 counter, gauge, and histogram fields covering the scheduler, executors, sources, sinks, IVM, shuffle, and state.</p>

<h2 id="counter-list">Counters and gauges (selection)</h2>
<table class="api-table">
<thead><tr><th>Metric</th><th>Type</th><th>Labels</th></tr></thead>
<tbody>
<tr><td><code>krishiv_tasks_submitted_total</code></td><td>counter</td><td><code>job_id</code></td></tr>
<tr><td><code>krishiv_tasks_succeeded_total</code></td><td>counter</td><td><code>job_id</code>, <code>task_id</code></td></tr>
<tr><td><code>krishiv_tasks_failed_total</code></td><td>counter</td><td><code>job_id</code>, <code>reason</code></td></tr>
<tr><td><code>krishiv_task_attempt_total</code></td><td>counter</td><td><code>job_id</code>, <code>attempt</code></td></tr>
<tr><td><code>krishiv_checkpoint_epoch</code></td><td>gauge</td><td><code>job_id</code></td></tr>
<tr><td><code>krishiv_checkpoint_epochs_total</code></td><td>counter</td><td><code>job_id</code>, <code>status</code></td></tr>
<tr><td><code>krishiv_checkpoint_commit_duration_seconds</code></td><td>histogram</td><td><code>job_id</code></td></tr>
<tr><td><code>krishiv_watermark_ms</code></td><td>gauge</td><td><code>job_id</code>, <code>source_id</code></td></tr>
<tr><td><code>krishiv_source_offset_lag</code></td><td>gauge</td><td><code>job_id</code>, <code>source_id</code></td></tr>
<tr><td><code>krishiv_executor_slots_used</code></td><td>gauge</td><td><code>executor_id</code></td></tr>
<tr><td><code>krishiv_executor_lost_total</code></td><td>counter</td><td><code>reason</code></td></tr>
<tr><td><code>krishiv_state_key_count</code></td><td>gauge</td><td><code>job_id</code>, <code>operator_id</code></td></tr>
<tr><td><code>krishiv_state_bytes</code></td><td>gauge</td><td><code>job_id</code>, <code>operator_id</code></td></tr>
<tr><td><code>krishiv_shuffle_bytes_written_total</code></td><td>counter</td><td><code>job_id</code>, <code>stage_id</code></td></tr>
<tr><td><code>krishiv_shuffle_records_written</code></td><td>counter</td><td><code>job_id</code>, <code>stage_id</code></td></tr>
<tr><td><code>krishiv_shuffle_local_blocks_fetched</code></td><td>counter</td><td><code>job_id</code>, <code>stage_id</code></td></tr>
<tr><td><code>krishiv_shuffle_remote_blocks_fetched</code></td><td>counter</td><td><code>job_id</code>, <code>stage_id</code></td></tr>
<tr><td><code>krishiv_grpc_call_duration_seconds</code></td><td>histogram</td><td><code>method</code></td></tr>
<tr><td><code>krishiv_streaming_rows_emitted_total</code></td><td>counter</td><td><code>job_id</code>, <code>task_id</code></td></tr>
<tr><td><code>krishiv_job_queue_depth</code></td><td>gauge</td><td><code>namespace</code></td></tr>
</tbody>
</table>
<p>Full list: <code>krishiv_metrics::KrishivMetrics</code>.</p>

<h2 id="histograms">Histograms</h2>
<p>Buckets are sensible for the typical value ranges. For latency: <code>0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 1800</code> seconds. For sizes: powers of two from 256 B to 1 GiB.</p>

<h2 id="prometheus">Prometheus text format</h2>
<p>Coordinator, executors, and the operator UI all expose <code>GET /metrics</code> returning Prometheus text exposition. <code>render_prometheus()</code> serialises with one HELP and one TYPE per metric family, including all labelled variants. Suitable for direct scrape by Prometheus or VictoriaMetrics.</p>

<h2 id="otlp">OTLP push</h2>
<p>Set <code>OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317</code> and Krishiv will push traces (and metrics if your OTel SDK is configured to) over gRPC. Same pipeline as the rest of your fleet.</p>

<h2 id="scheduler-metrics">Scheduler-specific metrics</h2>
<p>Beyond the <code>KrishivMetrics</code> singleton, the scheduler exposes additional metrics through <code>SchedulerMetrics::scheduler_metrics()</code>:</p>
<ul>
<li><code>krishiv_running_tasks</code></li>
<li><code>krishiv_task_retries_total</code></li>
<li><code>krishiv_failed_assignments_total</code></li>
<li><code>krishiv_max_executor_heartbeat_age_ticks</code></li>
</ul>
<p>These are rendered into the same <code>/metrics</code> response when the UI is co-located with the coordinator.</p>

<h2 id="per-job-cleanup">Per-job cleanup</h2>
<p>When a job ends, call <code>global_metrics().remove_job(job_id)</code> to drop all labelled variants. The CLI and the coordinator do this automatically on job completion and on coordinator restart.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/overview">Observability Overview</a></li>
  <li><a href="/docs/latest/observability/tracing">Distributed Tracing</a></li>
</ul>
`,
  },

  {
    slug: 'observability/logs',
    group: 'Observability',
    title: 'Logs',
    description: 'tracing-subscriber setup, log levels, structured logging, and the per-crate defaults.',
    status: 'Available',
    body: `
<p>Krishiv uses <code>tracing</code> + <code>tracing-subscriber</code>. By default the CLI and daemons emit human-readable FMT logs to stderr; set <code>KRISHIV_LOG_FORMAT=json</code> for structured JSON.</p>

<h2 id="filter">Log filter</h2>
<p>Standard <code>tracing-subscriber</code> env-filter. Examples:</p>
<pre><code class="language-bash"># Default: info globally, debug in the scheduler
RUST_LOG="info,sqlx=warn,rdkafka=warn,hyper=warn,krishiv_scheduler=debug" krishiv coordinator

# Quiet everything but the dataflow
RUST_LOG="warn,krishiv_dataflow=info" krishiv sql --query "SELECT 1"
</code></pre>

<p>The CLI sets a default filter of <code>info,sqlx=warn,rdkafka=warn,hyper=warn</code> so routine SQL noise stays out of the way.</p>

<h2 id="format">Format</h2>
<p>Two formats, chosen at init:</p>
<table class="api-table">
<thead><tr><th>Format</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>Fmt</code> (default)</td><td>Human-readable, color-aware. Best for local dev and journald.</td></tr>
<tr><td><code>Json</code></td><td>One JSON object per line. Best for log shippers (Fluent Bit, Vector, Loki).</td></tr>
</tbody>
</table>

<h2 id="fields">What you'll see in a log line</h2>
<pre><code class="language-text">2026-04-12T18:33:21.124Z  INFO krishiv_scheduler::coordinator: job_id=my-pipeline epoch=42 attempt=1 message="task succeeded" duration_ms=87
</code></pre>
<p>Every log line carries the standard tracing fields. Crate-specific fields are documented with the event that emits them.</p>

<h2 id="tracing-instrument">Instrumentation guide for contributors</h2>
<ul>
<li>Use <code>#[tracing::instrument(skip_all, fields(job_id, task_id))]</code> on functions; the macro emits enter/exit events with the field set.</li>
<li>For hot paths, prefer <code>tracing::trace!</code> or <code>debug!</code> and let the user turn them on.</li>
<li>Include the units in field names where ambiguous (<code>duration_ms</code>, <code>bytes</code>, <code>rows</code>).</li>
<li>Avoid logging in tight loops. Use a counter or sample instead.</li>
</ul>

<h2 id="integration">Log shipper integration</h2>
<p>Two common patterns:</p>
<ol>
<li><strong>JSON to stdout, ship from there</strong>: set <code>KRISHIV_LOG_FORMAT=json</code>, let journald / Docker / your runtime pick the lines up.</li>
<li><strong>OTLP logs</strong>: enable <code>opentelemetry-otlp</code> with logs enabled; traces and logs share the same pipeline.</li>
</ol>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/tracing">Distributed Tracing</a></li>
  <li><a href="/docs/latest/observability/overview">Observability Overview</a></li>
</ul>
`,
  },

  {
    slug: 'observability/tracing',
    group: 'Observability',
    title: 'Distributed Tracing',
    description: 'W3C trace context, OTLP exporters, and how traces are propagated across gRPC and Flight.',
    status: 'Available',
    body: `
<p>Krishiv emits W3C trace-context spans for every operation that crosses a meaningful boundary: SQL planning, task assignment, checkpoint barrier, shuffle fetch, and every gRPC call.</p>

<h2 id="pipeline">Span pipeline</h2>
<p>The default pipeline (<code>TracerExporter::Otlp</code>) looks like:</p>
<pre><code class="language-text">Session::sql()
  └─ DataFusion::plan
       └─ krishiv_sql::plan_sql
            └─ Coordinator::submit_job
                 └─ Scheduler::schedule
                      └─ Executor::run_task
                           └─ Dataflow::execute_window
                                └─ StateBackend::get  ← (span propagates through state I/O)
</code></pre>
<p>Every span is annotated with the standard fields: <code>job_id</code>, <code>task_id</code>, <code>epoch</code>, <code>operator_id</code>, plus the current <code>traceparent</code> and <code>tracestate</code>.</p>

<h2 id="grpc">gRPC propagation</h2>
<p>Every gRPC service in <code>krishiv-proto</code> (coordinator-to-executor, executor-to-executor, coordinator-management) injects and extracts <code>traceparent</code> via <code>inject_trace_context</code> / <code>extract_trace_context</code> in <code>krishiv-metrics::grpc</code>. This means a single trace spans the coordinator and the executor that ran a task.</p>

<h2 id="config">Configuration</h2>
<table class="api-table">
<thead><tr><th>Exporter</th><th>When to use</th></tr></thead>
<tbody>
<tr><td><code>TracerExporter::Otlp</code></td><td>Default. Pushes to <code>OTEL_EXPORTER_OTLP_ENDPOINT</code> over gRPC.</td></tr>
<tr><td><code>TracerExporter::Stdout</code></td><td>Local dev. Prints spans to stderr.</td></tr>
<tr><td><code>TracerExporter::InMemory(exporter)</code></td><td>Tests. Inspect spans in-process.</td></tr>
</tbody>
</table>

<h2 id="api">API</h2>
<pre><code class="language-rust">use tracing::{trace, info_span};

let _span = info_span!("checkpoint_commit", job_id = %job_id, epoch).entered();
// ... work that should be tracked
drop(_span); // exits the span
</code></pre>
<p>Cross-process context:</p>
<pre><code class="language-rust">use krishiv_metrics::{current_traceparent, current_tracestate};

let tp = current_traceparent();     // e.g. "00-aabbcc..-112233..-01"
let ts = current_tracestate();      // vendor-specific
</code></pre>

<h2 id="duration">gRPC duration histogram</h2>
<p>The <code>GrpcDurationLayer</code> middleware records <code>krishiv_grpc_call_duration_seconds</code> per method. The <code>GrpcDurationService</code> wrapper makes it trivial to add to a tower service.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/metrics">Metrics</a></li>
  <li><a href="/docs/latest/observability/overview">Observability Overview</a></li>
</ul>
`,
  },

  {
    slug: 'observability/health',
    group: 'Observability',
    title: 'Health & Status',
    description: 'Liveness, readiness, scheduler status endpoints, and the ObservabilityReport types.',
    status: 'Available',
    body: `
<p>Every long-running Krishiv process exposes a small set of HTTP endpoints for liveness, readiness, and status. The CLI also has commands that return machine-readable JSON for use in scripts and CI.</p>

<h2 id="endpoints">HTTP endpoints</h2>
<table class="api-table">
<thead><tr><th>Path</th><th>Process</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>GET /healthz</code></td><td>coordinator, clusterd, executor, UI</td><td>Liveness. Returns <code>200 OK</code> if the process is alive. Anonymous in all profiles.</td></tr>
<tr><td><code>GET /readyz</code></td><td>coordinator, clusterd, executor</td><td>Readiness. Returns <code>200 OK</code> if the process can serve traffic. <em>Requires auth in production.</em></td></tr>
<tr><td><code>GET /metrics</code></td><td>coordinator, executor, UI</td><td>Prometheus text format.</td></tr>
<tr><td><code>GET /api/v1/openapi.json</code></td><td>coordinator</td><td>OpenAPI 3.1 spec for the management API.</td></tr>
<tr><td><code>GET /api/v1/jobs</code></td><td>coordinator</td><td>List jobs (paginated with <code>?limit=&amp;offset=</code>).</td></tr>
<tr><td><code>GET /api/v1/jobs/{id}</code></td><td>coordinator</td><td>Job detail with stages and tasks.</td></tr>
<tr><td><code>GET /api/v1/executors</code></td><td>coordinator</td><td>List executors and their health.</td></tr>
<tr><td><code>GET /api/v1/queues</code></td><td>coordinator</td><td>Namespace quota snapshot.</td></tr>
<tr><td><code>GET /api/v1/openapi.json</code></td><td>coordinator</td><td>OpenAPI 3.1 spec for the management API.</td></tr>
</tbody>
</table>

<h2 id="cli">CLI status commands</h2>
<pre><code class="language-bash"># List running and recent jobs
krishiv jobs [--distributed]

# Inspect operator state for a job
krishiv state inspect --job my-pipeline --operator my-operator

# Trigger a savepoint
krishiv savepoint --job my-pipeline --label before-deploy

# Show the cluster status
krishiv local status
krishiv cluster status
</code></pre>

<h2 id="typed">Typed status reports</h2>
<p>Programmatic consumers should use the typed report structs (per the <code>krishiv-metrics::observability_report</code> module):</p>
<pre><code class="language-rust">use krishiv_metrics::ObservabilityReport;

let report: ObservabilityReport = build_report(&amp;coordinator, &amp;executors);
for job in &amp;report.jobs {
    println!("{} state={:?} rows={}", job.id, job.state, job.total_rows);
}
for ex in &amp;report.executors {
    println!("{} slots={}/{} lost={}", ex.id, ex.slots_used, ex.slots_total, ex.lost_count);
}
</code></pre>
<p>Sub-types: <code>ReportJob</code>, <code>ReportStage</code>, <code>ReportTask</code>, <code>ReportRuntimeStats</code>, <code>ReportExecutor</code>, <code>ReportCheckpoint</code>, <code>ReportShuffle</code>, <code>ReportStreamingState</code>, <code>ReportEvent</code>, <code>ReportConnectorMetrics</code>.</p>

<h2 id="system-metrics">System metrics</h2>
<p>For capacity planning, <code>krishiv_metrics::system_metrics() -&gt; &amp;'static SystemMetrics</code> exposes:</p>
<ul>
<li>CPU cores (logical)</li>
<li>Total and available memory bytes</li>
<li>Hostname, OS, kernel version</li>
<li>Process ID, uptime seconds</li>
</ul>

<h2 id="auth">Auth on management endpoints</h2>
<p>All endpoints except <code>/healthz</code> require a bearer token in production profiles. Set <code>KRISHIV_COORDINATOR_BEARER_TOKEN</code> (or the file / multi-token variants) before starting the coordinator. The UI also accepts a separate <code>KRISHIV_UI_TOKEN</code>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/observability/metrics">Metrics</a></li>
  <li><a href="/docs/latest/operations/auth-and-security">Auth &amp; Security</a></li>
</ul>
`,
  },
];
