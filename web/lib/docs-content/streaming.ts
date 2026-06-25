import type { DocPage } from '../docs-data';

export const streamingPages: DocPage[] = [
  {
    slug: 'streaming/overview',
    group: 'Streaming',
    title: 'Streaming Overview',
    description: 'How Krishiv models streams, event time, watermarks, and the unified batch+stream runtime.',
    status: 'Available',
    body: `
<p>Streaming in Krishiv is not a separate engine — it's the same <code>Session</code>, the same operator runtime, and the same <code>DataFrame</code> API you use for batch. What changes is the <strong>boundedness</strong> of the input and the operator shape: streaming sources are <em>unbounded</em> and operators that hold per-key or per-window state run forever.</p>

<h2 id="two-runtimes">Two runtimes, one API</h2>
<p>You write a query once. At planning time Krishiv tags the plan as <strong>bounded</strong> or <strong>unbounded</strong>:</p>
<table class="api-table">
<thead><tr><th>Plan type</th><th>Source</th><th>Operators allowed</th></tr></thead>
<tbody>
<tr><td><strong>Bounded</strong></td><td>Parquet, CSV, JSON, Iceberg, Delta, Hudi, in-memory</td><td>All DataFrame operators. <code>collect()</code> returns all rows.</td></tr>
<tr><td><strong>Unbounded</strong></td><td>Kafka, Kinesis, Pulsar, registered streaming table, in-memory unbounded stream</td><td><code>StreamingDataFrame</code> operators: event-time, key_by, windowed aggregation, joins, side output.</td></tr>
</tbody>
</table>

<h2 id="event-time">Event time, processing time, and watermarks</h2>
<p>Each streaming record carries a <strong>timestamp column</strong> (event time) and an arrival timestamp (processing time). The operator runtime advances a <strong>watermark</strong> based on the event-time column. Operators that need a notion of "now" (windows, late events) read the watermark.</p>

<p>Three watermark policies, applied per stream or globally:</p>
<table class="api-table">
<thead><tr><th>Policy</th><th>When to use</th></tr></thead>
<tbody>
<tr><td><code>WatermarkSpec::fixed_lag_ms(lag_ms)</code></td><td>Single source, fixed allowed lateness. <em>This is the default.</em></td></tr>
<tr><td><code>MultiSourceWatermarkSpec</code></td><td>Multiple streaming sources joined together; effective watermark is the min across all sources (with optional idle timeout).</td></tr>
<tr><td>Processing-time timer</td><td>Wall-clock driven events (heartbeats, SLA timers). Not for windowing.</td></tr>
</tbody>
</table>

<h2 id="two-styles">Two streaming styles</h2>
<p>Krishiv supports both <strong>micro-batch continuous queries</strong> (Spark Structured Streaming style) and <strong>true streaming</strong> (Flink style). You pick the style when you call <code>write_stream()</code>:</p>
<table class="api-table">
<thead><tr><th>Style</th><th>Trigger</th><th>When it fits</th></tr></thead>
<tbody>
<tr><td>Micro-batch</td><td><code>ProcessingTime(n)</code> or <code>Once</code></td><td>Periodic aggregates, simple sinks, lower operational complexity. Default.</td></tr>
<tr><td>Continuous</td><td><code>Continuous(checkpoint_interval_ms)</code></td><td>Sub-second latency requirements. Higher coordinator load.</td></tr>
<tr><td>Available-now</td><td><code>AvailableNow</code></td><td>Process all currently-available data and stop. Used for backfills.</td></tr>
</tbody>
</table>

<h2 id="where-to-go">Where to go next</h2>
<ul>
  <li><a href="/docs/latest/streaming/windows-and-watermarks">Windows and Watermarks</a> — tumbling, sliding, session, late events</li>
  <li><a href="/docs/latest/streaming/joins">Streaming Joins</a> — stream-table temporal, stream-stream interval, regular</li>
  <li><a href="/docs/latest/streaming/stateful-process-functions">Stateful Process Functions</a> — ProcessFunction, timers, ConnectedStreams, BroadcastState</li>
  <li><a href="/docs/latest/streaming/queries-and-lifecycle">Queries and Lifecycle</a> — DataStreamWriter, StreamingQuery, listeners, modes</li>
</ul>
`,
  },

  {
    slug: 'streaming/windows-and-watermarks',
    group: 'Streaming',
    title: 'Windows and Watermarks',
    description: 'Tumbling, sliding, and session windows; watermark strategies; late-event handling.',
    status: 'Available',
    body: `
<p>Krishiv supports the three standard window types. All are defined as part of the operator runtime in <code>krishiv-dataflow</code> and have SQL equivalents in <code>krishiv-sql</code> for the <code>GROUP BY</code> form.</p>

<h2 id="window-types">Window types</h2>
<table class="api-table">
<thead><tr><th>Type</th><th>SQL helper</th><th>Stream API</th><th>Description</th></tr></thead>
<tbody>
<tr><td><strong>Tumbling</strong></td><td><code>tumble_start(ts, interval)</code> / <code>tumble_end(ts, interval)</code></td><td><code>.tumbling_window(size_ms)</code></td><td>Fixed-size non-overlapping windows aligned to the epoch.</td></tr>
<tr><td><strong>Sliding (Hop)</strong></td><td><code>hop_start(ts, slide, size)</code> / <code>hop_end(ts, slide, size)</code></td><td><code>.sliding_window(size_ms, slide_ms)</code></td><td>Overlapping windows; each row may belong to multiple. <em>Bounded only in this release.</em></td></tr>
<tr><td><strong>Session</strong></td><td><code>session_start(ts, gap)</code> / <code>session_end(ts, gap)</code></td><td><code>.session_window(gap_ms)</code></td><td>Windows that close after an inactivity gap. <em>Bounded only in this release.</em></td></tr>
</tbody>
</table>

<h2 id="sql-example">SQL example</h2>
<pre><code class="language-sql">SELECT
  tumble_start(event_time, INTERVAL '1 minute') AS window_start,
  tumble_end(event_time,   INTERVAL '1 minute') AS window_end,
  COUNT(*) AS events
FROM events
GROUP BY
  tumble_start(event_time, INTERVAL '1 minute'),
  tumble_end(event_time,   INTERVAL '1 minute');
</code></pre>

<h2 id="stream-api-example">Stream API example (Rust)</h2>
<pre><code class="language-rust">use krishiv_api::{Session, Result, col, count, sum};

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let (stream, sender) = session.memory_stream(schema)?;

    let per_minute = stream
        .watermark("event_time", 5_000)?           // 5 s allowed lateness
        .key_by("user_id")?
        .tumbling_window(60_000)                  // 1-minute windows
        .agg(vec![count(col("*")), sum(col("amount"))]);

    sender.send(batch)?;
    let next_window = per_minute.collect_with_aggs(vec![count(col("*"))]).await?;
    Ok(())
}
</code></pre>

<h2 id="watermarks">Watermarks</h2>
<p>The watermark is the operator runtime's "I have seen all events with event time &le; W." Three places to set it:</p>
<ol>
<li><strong>Per-column, per-stream</strong> via <code>.watermark("event_time", lag_ms)</code> or <code>with_watermark(WatermarkSpec::fixed_lag_ms(...))</code>.</li>
<li><strong>Across multiple sources</strong> via <code>MultiSourceWatermarkSpec::new().source("a", spec).source("b", spec)</code>. The effective watermark is <em>min</em> across all sources (configurable idle timeout).</li>
<li><strong>Via SQL</strong> with <code>SET watermark.lag = INTERVAL '5' SECOND</code> (session-scoped, applies to the next query in the same session).</li>
</ol>

<h2 id="late-events">Late events</h2>
<p>Events whose event time is &lt; current watermark are <em>late</em>. Default behavior: drop. Two opt-ins:</p>
<table class="api-table">
<thead><tr><th>Strategy</th><th>API</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>CountingLateEventHandler</code> (default)</td><td>counts in <code>watermark.record_late_drop()</code></td><td>metrics / alerting</td></tr>
<tr><td>Custom handler</td><td>implement <code>trait LateEventHandler</code> and pass via <code>with_late_event_handler(handler)</code></td><td>route to side output, or count by key</td></tr>
<tr><td>Event-time TTL</td><td>set <code>TtlStateBackend.set_watermark(...)</code> and <code>StateTtlConfig</code> with <code>ttl_ms</code></td><td>bound state growth for late-arriving keys</td></tr>
</tbody>
</table>

<h2 id="stateful-windows">Stateful vs stateless</h2>
<p>Tumbling and sliding windows are <strong>state-backed</strong>: their accumulators persist to RocksDB (under the <code>single-node-durable</code> or <code>distributed-durable</code> durability profile) so they survive restart. In <code>dev-local</code> mode they use in-memory state backed by a checkpoint file.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/recipes/tumbling-window">Tumbling window recipe</a></li>
  <li><a href="/docs/latest/sql/window-functions">SQL Window Functions</a></li>
  <li><a href="/docs/latest/streaming/joins">Streaming Joins</a></li>
</ul>
`,
  },

  {
    slug: 'streaming/joins',
    group: 'Streaming',
    title: 'Streaming Joins',
    description: 'Stream-table temporal joins, stream-stream interval joins, and regular joins on bounded input.',
    status: 'Available',
    body: `
<p>Streaming joins in Krishiv come in three flavors. Pick by what you need: a slowly-changing dimension table, two correlated event streams, or a batch lookup.</p>

<h2 id="temporal-join">Stream-table temporal join (as-of)</h2>
<p>Join a stream against a versioned table where each row of the table has a validity window. The engine picks the version of the dimension row that was current at the time of the stream event. This is the SQL <code>FOR SYSTEM_TIME AS OF</code> pattern.</p>

<pre><code class="language-rust">use krishiv_api::{Session, temporal_join, Result};

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let enriched = temporal_join(
        session.read_kafka("orders", schema, "broker:9092", "orders", "app")?
            .with_event_time("event_time")?,
        session.table("users")?,  // versioned dimension
        "event_time",
        &["user_id"],
        false,  // inner=false ⇒ left outer
    )?;
    Ok(())
}
</code></pre>

<p>Python equivalent: <code>stream_table_join(stream, table, "event_time", ["user_id"], inner=False)</code>.</p>

<h2 id="interval-join">Stream-stream interval join</h2>
<p>Two streams with time-range correlation: for each event on the left, find events on the right whose time is within a window. Useful for click→impression joins, fraud detection, correlating sensor events.</p>

<pre><code class="language-rust">use krishiv_api::{interval_join, Result};

#[tokio::main]
async fn main() -&gt; Result&lt;()&gt; {
    let joined = interval_join(
        clicks,
        impressions,
        IntervalJoinSpec {
            lower_bound_ms: -10_000,  // impression can be 10 s before click
            upper_bound_ms: +30_000,  // or 30 s after
            key_column: "ad_id".into(),
            max_buffer_per_side: 10_000,
        },
    )?;
    Ok(())
}
</code></pre>

<p>Per-key state is bounded by <code>max_buffer_per_side</code>; older events are dropped and counted via the late-event handler.</p>

<h2 id="regular-join">Regular join on streaming input</h2>
<p>If both inputs are <strong>bounded</strong> (e.g. two Parquet files), use the standard DataFrame <code>join</code> — no special streaming semantics. If one or both inputs are unbounded, you must give the planner a window: <code>.tumbling_window(...)</code> first, then <code>.join(...)</code>. The join will run as a windowed join (state per window).</p>

<h2 id="state">State and backpressure</h2>
<table class="api-table">
<thead><tr><th>Join</th><th>State backend</th><th>Backpressure</th></tr></thead>
<tbody>
<tr><td>Temporal</td><td><code>VersionedTableState</code> per join key</td><td>Drops late events past TTL; counts via <code>LateEventHandler</code>.</td></tr>
<tr><td>Interval</td><td><code>PerKeyIntervalJoin</code> with <code>max_buffer_per_side</code></td><td>Oldest events dropped first when buffer full.</td></tr>
<tr><td>Windowed</td><td>Same as windowed aggregation: RocksDB per-key accumulators</td><td>Watermark-driven emission.</td></tr>
</tbody>
</table>

<div class="note-box"><strong>Note:</strong> All three joins require both sides to have an event-time column set. If the dimension table has no <code>FOR SYSTEM_TIME AS OF</code> version column, use a regular join with a <code>watermark(...)</code> on the stream side.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/recipes/stateful-process">Stateful process function recipe</a></li>
  <li><a href="/docs/latest/streaming/windows-and-watermarks">Windows and Watermarks</a></li>
  <li><a href="/docs/latest/sql/as-of-queries">AS-OF Queries</a></li>
</ul>
`,
  },

  {
    slug: 'streaming/stateful-process-functions',
    group: 'Streaming',
    title: 'Stateful Process Functions',
    description: 'Per-record processing with keyed state, timers, and multi-stream coordination.',
    status: 'Available',
    body: `
<p>When windowed aggregation is the wrong shape — for example, "per-user dedup with custom logic", "per-card fraud scoring with two event types", or "broadcast a rules table to all partitions" — you want a <strong>stateful process function</strong>. Krishiv provides three flavors.</p>

<h2 id="apply-process">ProcessFunction (single stream)</h2>
<p>The simplest model. <code>on_event</code> is called per record, with access to per-key state and timers.</p>

<pre><code class="language-rust">use krishiv_api::{apply_process_function, ValueState, ProcessContext, ProcessFunction};

struct Counter { state: ValueState&lt;i64&gt; }

impl ProcessFunction for Counter {
    fn on_event(&amp;mut self, _key: &amp;[u8], batch: &amp;RecordBatch, _row: usize, ctx: &amp;mut ProcessContext) {
        let n = self.state.get_json().unwrap_or(0);
        self.state.set_json(n + batch.num_rows() as i64);
        ctx.emit(batch.clone());
    }
    fn on_timer(&amp;mut self, _key: &amp;[u8], _fire_ms: i64, _ctx: &amp;mut ProcessContext) {}
}

let out = apply_process_function(stream, "user_id", Counter { state: ValueState::new("count") }, Default::default())?;
</code></pre>

<p>Python: <code>apply_process_function(stream, "user_id", my_fn, ValueState("count"))</code>.</p>

<h2 id="timers">Timers</h2>
<p>Two timer kinds per ProcessFunction:</p>
<table class="api-table">
<thead><tr><th>Kind</th><th>API</th><th>When it fires</th></tr></thead>
<tbody>
<tr><td><strong>Event-time</strong></td><td><code>ctx.register_event_time_timer(fire_time_ms)</code></td><td>When the watermark crosses the fire time.</td></tr>
<tr><td><strong>Processing-time</strong></td><td><code>ctx.register_processing_time_timer(fire_time_ms)</code></td><td>When wall clock crosses the fire time.</td></tr>
</tbody>
</table>

<p>Timers are per-key. When fired, the runtime calls <code>on_timer</code> on the same ProcessFunction instance. The function can then emit, modify state, and register new timers.</p>

<h2 id="connected-streams">ConnectedStreams (two streams, one function)</h2>
<p>Co-process function: receive events from two keyed streams, share state. Useful for click→impression correlation that needs to share a "this user is suspicious" flag.</p>

<pre><code class="language-rust">use krishiv_api::{connect_streams, CoProcessFunction, ProcessContext, ValueState};

struct FraudScorer { flagged: ValueState&lt;bool&gt; }

impl CoProcessFunction for FraudScorer {
    fn on_event(&amp;mut self, _key: &amp;[u8], batch: &amp;RecordBatch, _row: usize, ctx: &amp;mut ProcessContext) {
        let flagged = self.flagged.get_json().unwrap_or(false);
        if flagged { ctx.emit(batch.clone()); }
    }
}

let out = connect_streams(left, right, FraudScorer { flagged: ValueState::new("flagged") })?;
</code></pre>

<h2 id="broadcast">BroadcastState (rules shared across partitions)</h2>
<p>When every parallel instance of a function needs the same read-only state (e.g. a small rules table) use BroadcastState. Updates are propagated to all parallel instances. Reads are local.</p>

<pre><code class="language-rust">use krishiv_api::{broadcast_stream, BroadcastState, BroadcastProcessFunction};

struct ApplyRules { rules: BroadcastState&lt;i32, String&gt; }
impl BroadcastProcessFunction for ApplyRules {
    fn on_broadcast(&amp;mut self, _key: &amp;[u8], batch: &amp;RecordBatch, _row: usize, ctx: &amp;mut ProcessContext) {
        if let Some(rule) = self.rules.get(&amp;42) {
            // apply rule
        }
        ctx.emit(batch.clone());
    }
}
let out = broadcast_stream(stream, &amp;rules_descriptor, ApplyRules { rules: BroadcastState::new("rules") })?;
</code></pre>

<h2 id="state-primitives">State primitives</h2>
<p>All process functions take a state descriptor. Krishiv ships five kinds:</p>
<table class="api-table">
<thead><tr><th>Kind</th><th>API</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>ValueState&lt;T&gt;</code></td><td><code>state.get_json() / set_json(v) / clear()</code></td><td>Single value per key.</td></tr>
<tr><td><code>ListState&lt;T&gt;</code></td><td><code>state.add_json(v) / get() / clear()</code></td><td>Append-only list per key.</td></tr>
<tr><td><code>MapState&lt;K,V&gt;</code></td><td><code>state.put_json(k, v) / get(k) / remove(k)</code></td><td>Keyed map per outer key.</td></tr>
<tr><td><code>ReducingState&lt;T&gt;</code></td><td><code>state.add(v) / get() / merge(v)</code></td><td>Monoid-reducible accumulator per key.</td></tr>
<tr><td><code>BroadcastState&lt;K,V&gt;</code></td><td>shared across all parallel instances</td><td>Read-mostly lookup tables.</td></tr>
</tbody>
</table>

<p>State backends: <code>RocksDbStateBackend</code> (default in <code>single-node-durable</code> / <code>distributed-durable</code>) or in-memory (default in <code>dev-local</code>). See <a href="/docs/latest/state/overview">State</a>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/recipes/stateful-process">Stateful process recipe</a></li>
  <li><a href="/docs/latest/state/overview">State</a></li>
</ul>
`,
  },

  {
    slug: 'streaming/queries-and-lifecycle',
    group: 'Streaming',
    title: 'Queries and Lifecycle',
    description: 'DataStreamWriter, StreamingQuery, output modes, triggers, listeners, cancellation.',
    status: 'Available',
    body: `
<p>A streaming query in Krishiv is a long-running <strong>job</strong> owned by the coordinator (or by the local cluster in single-node mode). The DataStreamWriter is the way you start one, and the StreamingQuery handle is how you observe and stop it.</p>

<h2 id="writer">DataStreamWriter</h2>
<pre><code class="language-rust">use krishiv_api::Session;

#[tokio::main]
async fn main() -&gt; krishiv_api::Result&lt;()&gt; {
    let session = Session::embedded().await?;

    let streaming = session
        .read_parquet("data/orders.parquet").await?
        .to_streaming()
        .with_event_time("event_time")
        .tumbling_window(60_000)
        .agg(vec![count(col("*"))]);

    let query = streaming
        .write_stream()
        .output_mode(OutputMode::Append)              // Append | Update | Complete
        .trigger(Trigger::ProcessingTime(5_000))     // 5-s micro-batches
        .format("parquet")                          // kafka | parquet | iceberg | memory | console
        .option("path", "out/per_minute/")
        .option("checkpoint.location", "ckpt/")
        .start().await?;

    Ok(())
}
</code></pre>

<h2 id="output-modes">Output modes</h2>
<table class="api-table">
<thead><tr><th>Mode</th><th>What is emitted</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>Append</code> (default)</td><td>Only new rows. Triggers downstream sinks to commit only the new data.</td><td>Idempotent sinks (Parquet, Iceberg) — the safe default.</td></tr>
<tr><td><code>Update</code></td><td>Inserts and updates keyed by primary key. State-backed, requires <code>OutputMode::Update</code> support in the sink.</td><td>Materialized result tables, dedup pipelines.</td></tr>
<tr><td><code>Complete</code></td><td>The full result table for each trigger.</td><td>Small aggregate tables where the sink can rewrite.</td></tr>
</tbody>
</table>

<h2 id="triggers">Triggers</h2>
<table class="api-table">
<thead><tr><th>Trigger</th><th>Behavior</th><th>Latency</th></tr></thead>
<tbody>
<tr><td><code>Once</code></td><td>Process all available data, then stop. No checkpoints.</td><td>One-shot backfill.</td></tr>
<tr><td><code>AvailableNow</code></td><td>Process all currently-available data in batched micro-triggers, then stop. Checkpoints between triggers.</td><td>Backfill that survives restart.</td></tr>
<tr><td><code>ProcessingTime(n)</code></td><td>Trigger every <code>n</code> milliseconds. Checkpointed.</td><td>Seconds-scale latency. Default for production.</td></tr>
<tr><td><code>Continuous(n)</code></td><td>Run as a true streaming pipeline; checkpoint every <code>n</code> ms.</td><td>Sub-second latency. Higher coordinator overhead.</td></tr>
</tbody>
</table>

<h2 id="formats">Sinks (formats)</h2>
<p><code>.format(name)</code> takes a string and dispatches:</p>
<table class="api-table">
<thead><tr><th>Format</th><th>Notes</th></tr></thead>
<tbody>
<tr><td><code>memory</code></td><td>Writes to an in-process <code>Vec&lt;RecordBatch&gt;</code>. <code>query.memory_batches()</code> retrieves.</td></tr>
<tr><td><code>console</code></td><td>Prints to stdout. Useful for debugging.</td></tr>
<tr><td><code>parquet</code></td><td>Local file or S3/ADLS/GCS path. Checkpointed.</td></tr>
<tr><td><code>kafka</code></td><td>Topic + bootstrap servers. Use <code>with_kafka_transactional(...)</code> for exactly-once.</td></tr>
<tr><td><code>iceberg</code></td><td>Catalog URI + warehouse. Two-phase commit for exactly-once.</td></tr>
<tr><td><code>foreach_batch</code></td><td>Pass a closure that gets called with each micro-batch.</td></tr>
</tbody>
</table>

<h2 id="lifecycle">StreamingQuery lifecycle</h2>
<pre><code class="language-rust">let query = handle.start().await?;

// observe
println!("id={} state={:?}", query.id(), query.status().state);
let progress = query.last_progress();
if let Some(p) = progress {
    println!("trigger={:?} input_rows={} output_rows={}", p.trigger, p.input_rows, p.output_rows);
}

// wait with timeout
query.await_termination_timeout(Duration::from_secs(60)).await?;

// stop
query.stop();
</code></pre>

<p>On Drop the query is stopped automatically.</p>

<h2 id="listeners">StreamingQueryManager and listeners</h2>
<p>For long-lived services that own many queries, use <code>StreamingQueryManager</code> to register listeners:</p>
<pre><code class="language-rust">let mgr = StreamingQueryManager::new();
mgr.add_listener(Arc::new(MyListener));

struct MyListener;
impl StreamingQueryListener for MyListener {
    fn on_query_terminated(&amp;self, e: &amp;QueryTerminatedEvent) {
        log::error!("query {} terminated: {:?}", e.query_id, e.exception);
    }
}
</code></pre>

<p>Query events include the last <code>StreamingQueryProgress</code> so you can write a clean shutdown handler.</p>

<h2 id="cancel">Cancellation and timeout</h2>
<ul>
<li>Per-query timeout: <code>session.sql_with_timeout("...", 30_000)</code> (ms).</li>
<li>Cancel a query: <code>query.stop()</code> or <code>session.operation_registry().cancel(op_id)</code>.</li>
<li>Coordinator-driven cancel via gRPC: <code>TaskCancellationRequest</code> (per <code>krishiv-proto</code>).</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/streaming/overview">Streaming Overview</a></li>
  <li><a href="/docs/latest/python/stream">Python Stream API</a></li>
  <li><a href="/docs/latest/rust/stream">Rust Stream API</a></li>
</ul>
`,
  },
];
