import type { DocPage } from '../docs-data';
import { DIAGRAM_STATE_TYPES, DIAGRAM_TIMERS, DIAGRAM_SAVEPOINTS } from './diagrams';

export const statePages: DocPage[] = [
  {
    slug: 'state/overview',
    group: 'State',
    title: 'State Overview',
    description: 'Keyed state, state backends, durability profiles, and the role of state in checkpoints.',
    status: 'Available',
    body: `
<p>Krishiv's state is <strong>per-key, typed, and persistent</strong>. It is the substrate for windows, joins, process functions, IVM, and any operator that needs to remember something about a key. This page is the conceptual overview; the API reference lives in <a href="/docs/latest/state/state-types">State Types</a> and <a href="/docs/latest/state/savepoints-and-migration">Savepoints and Migration</a>.</p>

<h2 id="concepts">Concepts</h2>
<table class="api-table">
<thead><tr><th>Concept</th><th>Definition</th></tr></thead>
<tbody>
<tr><td><strong>State</strong></td><td>A per-key, named, typed value (or list / map / reducer / broadcast). Stored in a state backend.</td></tr>
<tr><td><strong>State descriptor</strong></td><td>The schema for a state slot: name, key encoding, value type, TTL. Created at job-submit time.</td></tr>
<tr><td><strong>State backend</strong></td><td>The store that actually persists state. Currently: <code>InMemoryStateBackend</code> or <code>RocksDbStateBackend</code>, with <code>TtlStateBackend</code> as an optional wrapper.</td></tr>
<tr><td><strong>Checkpoint</strong></td><td>A consistent snapshot of all state for a job, written to the configured checkpoint storage. Coordinated by the coordinator.</td></tr>
<tr><td><strong>Savepoint</strong></td><td>A user-triggered, named checkpoint. Listable, deletable, restorable.</td></tr>
<tr><td><strong>Durability profile</strong></td><td>Decides which backends are in use: <code>dev-local</code>, <code>single-node-durable</code>, <code>distributed-durable</code>.</td></tr>
</tbody>
</table>

<h2 id="durability">Durability profiles</h2>
<p>Set the profile at the session level via <code>KRISHIV_DURABILITY_PROFILE</code> (env) or by choosing the matching <code>Session</code> factory. Each profile selects the right combination of state, shuffle, and checkpoint backends.</p>
<table class="api-table">
<thead><tr><th>Profile</th><th>State</th><th>Shuffle</th><th>Checkpoints</th><th>When to use</th></tr></thead>
<tbody>
<tr><td><code>dev-local</code></td><td>In-memory</td><td>In-memory</td><td>Ephemeral (in-process)</td><td>Examples, tests, dev laptops.</td></tr>
<tr><td><code>single-node-durable</code></td><td>RocksDB (local disk)</td><td>Local disk</td><td>Local filesystem</td><td>Single-host production. Restart-durable.</td></tr>
<tr><td><code>distributed-durable</code></td><td>RocksDB (restored from checkpoint)</td><td>Tiered: local + object store</td><td>Object store + etcd metadata</td><td>Multi-host. Fenced, fault-tolerant.</td></tr>
</tbody>
</table>

<h2 id="state-encoding">State encoding</h2>
<p>State values are stored as a single byte buffer per (operator, key, slot). The encoding is:</p>
<pre><code class="language-text">[8-byte LE expires_at_ms (optional, when TTL is enabled)][postcard-serialized value]
</code></pre>
<p>The TTL prefix is included only if <code>TtlStateBackend::set_watermark(...)</code> has been called. When a key is read and its <code>expires_at_ms</code> is in the past, the value is treated as absent and the entry is removed on the next write. This means TTL works correctly even if the event-time watermark lags the wall clock.</p>

<h2 id="storage">Storage URIs</h2>
<p>Checkpoints and shuffle data are written through URI-typed backends:</p>
<table class="api-table">
<thead><tr><th>URI</th><th>Backend</th></tr></thead>
<tbody>
<tr><td><code>file:///var/krishiv/ckpt</code> / <code>/var/krishiv/ckpt</code></td><td><code>LocalFsCheckpointStorage</code></td></tr>
<tr><td><code>s3://bucket/path</code></td><td><code>ObjectStoreCheckpointStorage</code> (S3, GCS, ADLS, MinIO — anything <code>object_store</code> supports)</td></tr>
<tr><td><code>memory://</code> / dev only</td><td><code>EphemeralCheckpointStorage</code></td></tr>
</tbody>
</table>
<p>Helper: <code>open_checkpoint_storage_from_uri(uri) -&gt; Arc&lt;dyn CheckpointStorage&gt;</code> picks the right backend.</p>

<h2 id="migrations">Migrations</h2>
<p>When you change the type or encoding of a state value across releases, register a migration:</p>
<pre><code class="language-rust">use krishiv_api::{register_state_migration, state_migration, apply_state_migration};

register_state_migration("my_state", 1, 2, state_migration::&lt;OldType, NewType&gt;(
    |old| NewType { ... }
));
</code></pre>
<p>Migrations run automatically when a checkpoint with an older schema version is loaded. See <a href="/docs/latest/state/savepoints-and-migration">Savepoints and Migration</a>.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/state/state-types">State Types</a> — <code>ValueState</code>, <code>ListState</code>, <code>MapState</code>, <code>ReducingState</code>, <code>BroadcastState</code></li>
  <li><a href="/docs/latest/state/savepoints-and-migration">Savepoints and Migration</a></li>
  <li><a href="/docs/latest/state/queryable-state">Queryable State</a></li>
  <li><a href="/docs/latest/state/timers">Timers</a></li>
  <li><a href="/docs/latest/concepts/execution-model">Execution Model</a> — durability profiles in detail</li>
</ul>
`,
  },

  {
    slug: 'state/state-types',
    group: 'State',
    title: 'State Types',
    description: 'ValueState, ListState, MapState, ReducingState, BroadcastState — APIs and idioms.',
    status: 'Available',
    body: `
<p>Five state primitives. All are per-key, named, and back by a state backend. They are <code>serde::Serialize + DeserializeOwned</code> by default (the blanket <code>StateValue</code> trait).</p>
${DIAGRAM_STATE_TYPES}

<h2 id="value">ValueState&lt;T&gt;</h2>
<p>One value per key. The most common primitive.</p>
<pre><code class="language-rust">use krishiv_api::ValueState;

let state = ValueState::&lt;i64&gt;::new("seen_count");

// in on_event
let n = state.get_json().unwrap_or(0);
state.set_json(n + 1);
</code></pre>

<p>Python: <code>state = ValueState("seen_count")</code>; <code>state.set_json(n); n = state.get_json()</code>.</p>

<h2 id="list">ListState&lt;T&gt;</h2>
<p>Append-only list per key. Good for "all events for this user in the last hour" or "rolling 1000 prices for this symbol".</p>
<table class="api-table">
<thead><tr><th>Method</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>add_json(v)</code> / <code>add(v)</code></td><td>Append <code>v</code> to the list.</td></tr>
<tr><td><code>get() / get_json() -&gt; Vec&lt;T&gt;</code></td><td>Read the list (a copy).</td></tr>
<tr><td><code>length() / is_empty()</code></td><td>Count.</td></tr>
<tr><td><code>clear()</code></td><td>Drop the list.</td></tr>
</tbody>
</table>
<p>If you need bounded retention, wrap with a TTL or trim in <code>on_timer</code>.</p>

<h2 id="map">MapState&lt;K, V&gt;</h2>
<p>Keyed map per outer key. Good for "user has these active sessions" or "this device has these attributes".</p>
<table class="api-table">
<thead><tr><th>Method</th><th>Effect</th></tr></thead>
<tbody>
<tr><td><code>put_json(k, v) / put(k, v)</code></td><td>Set <code>k -&gt; v</code>.</td></tr>
<tr><td><code>get(k) / get_json(k) -&gt; Option&lt;V&gt;</code></td><td>Read or None.</td></tr>
<tr><td><code>contains(k) -&gt; bool</code></td><td>Existence check.</td></tr>
<tr><td><code>keys() / values() / entries()</code></td><td>Iterate.</td></tr>
<tr><td><code>remove(k) / clear()</code></td><td>Delete one or all.</td></tr>
</tbody>
</table>

<h2 id="reducing">ReducingState&lt;T&gt;</h2>
<p>A monoid-reducible accumulator per key. Useful for "running max", "running quantile sketch", or "running HyperLogLog".</p>
<pre><code class="language-rust">use krishiv_api::ReducingState;

let max_amt = ReducingState::&lt;i64&gt;::new("max_amount");

// in on_event
let prev = max_amt.get().unwrap_or(i64::MIN);
max_amt.merge(batch_amount.max(prev));
</code></pre>
<p>The merge op is a user-supplied associative function; the engine never inspects the value. This means the accumulator is opaque from the engine's point of view — the state backend just stores and forwards bytes.</p>

<h2 id="broadcast">BroadcastState&lt;K, V&gt;</h2>
<p>Shared across all parallel instances of a function. Updates are propagated to every instance; reads are local. Use for small, read-mostly lookup tables (rules, features, geo data).</p>
<pre><code class="language-rust">use krishiv_api::{BroadcastState, BroadcastStateDescriptor};

let rules = BroadcastStateDescriptor::&lt;i32, String&gt;::new("rules");
let state = BroadcastState::&lt;i32, String&gt;::new("rules");
let v = state.get(&amp;42); // local read
</code></pre>
<p>Size limit: keep broadcast state &lt; 10 MB per descriptor. Larger belongs in a real table source.</p>

<h2 id="ttl">TTL with TtlStateBackend</h2>
<p>Wrap any state backend in a TTL to evict keys after a configurable time, either wall-clock or event-time:</p>
<pre><code class="language-rust">use krishiv_api::{TtlConfig, TtlStateBackend, RocksDbStateBackend};
use std::sync::Arc;
use std::time::Duration;

let inner = Arc::new(RocksDbStateBackend::open("/var/krishiv/state")?);
let ttl = TtlStateBackend::new(inner, TtlConfig::new(60_000)); // 60 s
// event-time mode: when a watermark is set, expiry is in event time, not wall clock
ttl.set_watermark(current_watermark_ms);
</code></pre>

<p>Wire format: each state value is stored as <code>[8-byte LE expires_at_ms][raw bytes]</code>. On read, expired entries are treated as absent and removed on the next write. This is what makes TTL safe when the event-time watermark lags the wall clock.</p>

<h2 id="backends">Backends</h2>
<table class="api-table">
<thead><tr><th>Backend</th><th>Use</th><th>Persistence</th></tr></thead>
<tbody>
<tr><td><code>InMemoryStateBackend</code></td><td>Tests, <code>dev-local</code></td><td>None</td></tr>
<tr><td><code>RocksDbStateBackend</code></td><td>Production (<code>single-node-durable</code> and up)</td><td>Local filesystem, crash-safe</td></tr>
<tr><td><code>TtlStateBackend&lt;B&gt;</code></td><td>Wrapper for any backend to add TTL semantics</td><td>Same as inner</td></tr>
</tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/state/overview">State Overview</a></li>
  <li><a href="/docs/latest/streaming/stateful-process-functions">Stateful Process Functions</a></li>
  <li><a href="/docs/latest/python/state">Python State</a></li>
</ul>
`,
  },

  {
    slug: 'state/savepoints-and-migration',
    group: 'State',
    title: 'Savepoints and Schema Migration',
    description: 'User-triggered snapshots, restore, and migrating state across versions.',
    status: 'Available',
    body: `
<p>Checkpoints are coordinator-driven and automatic. <strong>Savepoints</strong> are user-triggered, named, and listable — useful for "snapshot before I deploy the new model" workflows. <strong>Schema migration</strong> is the mechanism for evolving the shape of state values across releases.</p>
${DIAGRAM_SAVEPOINTS}

<h2 id="savepoints">Savepoints</h2>
<p>CLI:</p>
<pre><code class="language-bash"># Take a savepoint with a label
krishiv savepoint --job my-pipeline --label before-v2

# List savepoints for a job
krishiv checkpoints list --job my-pipeline

# Restore from a savepoint (coordinator restarts the job from that epoch)
krishiv restore --job my-pipeline --epoch 42

# Delete a savepoint
# (the savepoint directory is removed from object storage)
</code></pre>

<p>Each savepoint records:</p>
<table class="api-table">
<thead><tr><th>Field</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>format_version</code></td><td><code>SAVEPOINT_FORMAT_VERSION = 1</code></td></tr>
<tr><td><code>savepoint_id</code></td><td>Unique ID; usually the epoch number.</td></tr>
<tr><td><code>label</code></td><td>User-supplied string.</td></tr>
<tr><td><code>job_id</code></td><td>The job it belongs to.</td></tr>
<tr><td><code>epoch</code></td><td>The checkpoint epoch this savepoint aliases.</td></tr>
<tr><td><code>operator_versions</code></td><td>Map of operator UID → behavior version. Used for migration.</td></tr>
<tr><td><code>created_at_secs</code></td><td>Unix timestamp.</td></tr>
</tbody>
</table>

<p>Savepoints are stored at <code>{base_dir}/{job_id}/savepoints/{savepoint_id}/meta.json</code>.</p>

<h2 id="migrations">State schema migration</h2>
<p>If you change the type or encoding of a state value across releases, you need a migration. Register one for each operator + version pair:</p>

<pre><code class="language-rust">use krishiv_api::{register_state_migration, state_migration};

register_state_migration("fraud_score", 1, 2, state_migration::&lt;OldScore, NewScore&gt;(
    |old| NewScore {
        value: old.value * 1.5,    // reweight
        model_version: 2,
    }
));
</code></pre>

<p>When the coordinator loads a checkpoint with an older <code>operator_version</code>, the migration is applied. Two helpers:</p>
<table class="api-table">
<thead><tr><th>Function</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>migrate_snapshot(value_migrator, &amp;[(from, to)])</code></td><td>Value-only migration.</td></tr>
<tr><td><code>migrate_snapshot_with_keys(key_migrator, value_migrator, &amp;[(from, to)])</code></td><td>Both key and value migration (e.g. when the key encoding changes).</td></tr>
</tbody>
</table>

<h2 id="incremental">Incremental checkpointing</h2>
<p>For RocksDB-backed state, full checkpoints can be expensive. Krishiv supports incremental checkpointing via <code>RocksDbIncrementalCheckpointer</code>:</p>
<ul>
<li>Each checkpoint only writes SST files that changed since the last one.</li>
<li>Metadata is tracked in an <code>SstEpochManifest</code>.</li>
<li>Restoration rebuilds the state directory by layering SSTs from manifest entries.</li>
</ul>
<p>Enable by setting the checkpoint storage URI to one with an <code>RocksDbIncremental</code> hint, or by passing an explicit <code>IncrementalCheckpointer</code> in the session config.</p>

<h2 id="rescaling">Rescaling (key-group redistribution)</h2>
<p>When you change the parallelism of a job, state keys are remapped via <code>KeyGroupRescaler</code>:</p>
<ul>
<li>The rescaler computes which existing key-groups map to which new ones.</li>
<li>Each rescaled key is rewritten with <code>EntryRouting</code>.</li>
<li>A <code>RescaleChecksum</code> is computed to verify that no key is lost or duplicated.</li>
</ul>
<p>Rescaling happens automatically on restore when the new parallelism differs from the checkpoint.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/operations/checkpointing">Checkpointing</a> — automatic checkpoints</li>
  <li><a href="/docs/latest/state/overview">State Overview</a></li>
</ul>
`,
  },

  {
    slug: 'state/queryable-state',
    group: 'State',
    title: 'Queryable State',
    description: 'Expose per-key state to ad-hoc SQL queries without running the full pipeline.',
    status: 'Available',
    body: `
<p>Sometimes you want to ask "what's the current fraud score for user 42?" without standing up a full streaming query. Queryable State exposes a per-key state slot as a table that can be queried with SQL.</p>

<h2 id="how">How it works</h2>
<p>You mark a state descriptor as <strong>queryable</strong>. Behind the scenes:</p>
<ol>
<li>A background thread in the executor keeps a hot in-memory copy of the state values.</li>
<li>The state is exposed as a virtual table named after the descriptor: <code>SELECT * FROM queryable_state('fraud_score')</code> (Rust API) or via the SQL Gateway.</li>
<li>Reads are eventually consistent — they reflect the value as of the most recent completed checkpoint, with a small lag (typically &lt; 1 s).</li>
</ol>

<h2 id="api">API</h2>
<pre><code class="language-rust">use krishiv_state::QueryableStateStore;

let store = QueryableStateStore::new("fraud_score", state_descriptor);
let value: Option&lt;f64&gt; = store.get(&amp;key_bytes).await?;
let all: Vec&lt;(Vec&lt;u8&gt;, f64)&gt; = store.scan_prefix(&amp;prefix).await?;
</code></pre>

<p>HTTP API (via the coordinator):</p>
<pre><code class="language-bash"># List queryable state tables
curl http://coord:2002/api/v1/queryable

# Get one key
curl http://coord:2002/api/v1/queryable/fraud_score/key/0x2A

# Scan a prefix
curl 'http://coord:2002/api/v1/queryable/fraud_score/scan?prefix=0x00&limit=100'
</code></pre>

<p>Python:</p>
<pre><code class="language-python">from krishiv import session
score = session.queryable_get("fraud_score", key=42)
</code></pre>

<h2 id="consistency">Consistency</h2>
<table class="api-table">
<thead><tr><th>Profile</th><th>Read freshness</th></tr></thead>
<tbody>
<tr><td><code>dev-local</code></td><td>Read-after-write in the same task.</td></tr>
<tr><td><code>single-node-durable</code></td><td>Reads reflect the last completed checkpoint (typically &lt; 1 s lag).</td></tr>
<tr><td><code>distributed-durable</code></td><td>Reads reflect the last committed snapshot on the executor that owns the key. Cross-executor reads may be a checkpoint behind.</td></tr>
</tbody>
</table>

<h2 id="perf">Performance and limits</h2>
<ul>
<li>Each queryable descriptor costs one thread per executor. Don't enable more than ~20 per executor.</li>
<li>Stored values are kept in a bounded LRU per descriptor. Configure with <code>QueryableStateStore::with_capacity(n)</code>.</li>
<li>Read latency: p50 &lt; 1 ms, p99 &lt; 10 ms for values &lt; 1 KB.</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/state/overview">State Overview</a></li>
  <li><a href="/docs/latest/observability/health">Health & Status</a></li>
</ul>
`,
  },

  {
    slug: 'state/timers',
    group: 'State',
    title: 'Timers',
    description: 'Event-time and processing-time timers for stateful functions and IVM.',
    status: 'Available',
    body: `
<p>Timers let a stateful function do work at a specific time, instead of in response to an event. They are the canonical way to implement "do X after N seconds of inactivity" or "emit the result of this window at 12:00:00".</p>
${DIAGRAM_TIMERS}

<h2 id="kinds">Timer kinds</h2>
<table class="api-table">
<thead><tr><th>Kind</th><th>Fires when</th><th>API</th></tr></thead>
<tbody>
<tr><td><strong>Event-time</strong></td><td>The job's watermark crosses the timer's fire time. Order is preserved per key.</td><td><code>ctx.register_event_time_timer(fire_ms)</code></td></tr>
<tr><td><strong>Processing-time</strong></td><td>Wall-clock crosses the timer's fire time. Order is <em>not</em> guaranteed across keys.</td><td><code>ctx.register_processing_time_timer(fire_ms)</code></td></tr>
</tbody>
</table>

<h2 id="eviction">Event-time vs processing-time</h2>
<p>Use event-time timers when the semantics of your function are tied to data (windowing, late-event detection, key-timeout). Use processing-time timers for wall-clock-driven work (heartbeats, periodic flushes, SLA deadlines).</p>

<h2 id="api">API</h2>
<pre><code class="language-rust">use krishiv_api::{ProcessContext, TimerKind};

fn on_event(&amp;mut self, _key: &amp;[u8], batch: &amp;RecordBatch, _row: usize, ctx: &amp;mut ProcessContext) {
    ctx.register_event_time_timer(batch.column_by_name("event_time").unwrap().as_any().downcast_ref::&lt;...&gt;());
}

fn on_timer(&amp;mut self, _key: &amp;[u8], fire_time_ms: i64, ctx: &amp;mut ProcessContext) {
    // do periodic work
}
</code></pre>

<p>Timers are per-key. When the timer fires, the engine calls <code>on_timer</code> on the same ProcessFunction instance with the same key. From there you can emit, modify state, and register new timers.</p>

<h2 id="persistence">Persistence</h2>
<p>Timers are persisted as part of the operator state. On restart, timers that have not yet fired are re-armed. Processing-time timers use the engine's <em>checkpoint time</em> as their anchor — they don't re-fire past events that happened during downtime.</p>

<h2 id="ivm-timers">Timers in IVM</h2>
<p>IVM views can use <code>WatermarkTracker</code> and <code>LatenessSpec</code> to declaratively express timing without explicit timers. The <code>LATENESS</code> clause in <code>CREATE INCREMENTAL VIEW</code> sets a per-source late-event tolerance:</p>
<pre><code class="language-sql">CREATE INCREMENTAL VIEW order_totals AS
  SELECT customer_id, SUM(amount) AS total
  FROM orders
  GROUP BY customer_id
LATENESS event_time INTERVAL '10' SECOND;
</code></pre>
<p>This is sugar for a watermark with a 10 s allowed lateness. See <a href="/docs/latest/sql/incremental-views">Incremental Views</a>.</p>

<h2 id="service">Timer services</h2>
<p>For functions that are not part of a streaming query but still need timers (e.g. background cleanup tasks), Krishiv provides two timer services:</p>
<ul>
<li><code>InMemoryTimerService</code> — fast, ephemeral.</li>
<li><code>ProcessingTimeTimerService</code> — backed by a state store so timers survive restart.</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/streaming/stateful-process-functions">Stateful Process Functions</a></li>
  <li><a href="/docs/latest/state/overview">State Overview</a></li>
</ul>
`,
  },
];
