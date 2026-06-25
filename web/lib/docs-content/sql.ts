import type { DocPage } from '../docs-data';

export const sqlPages: DocPage[] = [
  {
    slug: 'sql',
    group: 'SQL Reference',
    title: 'SQL Overview',
    description: 'Krishiv SQL surface: DataFusion base plus Krishiv extensions.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv SQL is built on <strong>DataFusion</strong>, which implements a broad subset of ANSI SQL. Krishiv adds extensions for streaming, incremental views, live tables, pipeline DDL, lakehouse DML, pattern matching, and session management.</p>

<h2 id="standard-sql">Standard SQL (DataFusion)</h2>
<p>All standard SELECT semantics are supported: projections, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, CTEs (WITH), subqueries, UNNEST, window functions (<code>ROW_NUMBER</code>, <code>RANK</code>, <code>LAG</code>, <code>LEAD</code>, etc.), UNION / INTERSECT / EXCEPT, JOINs, string/date/numeric/aggregate/array functions, CASE expressions, CAST, COALESCE, and NULLIF.</p>

<h2 id="krishiv-extensions">Krishiv SQL Extensions</h2>
<table class="api-table">
  <thead><tr><th>Extension</th><th>Status</th><th>Summary</th></tr></thead>
  <tbody>
    <tr><td>Window functions: <code>tumble_start / tumble_end / hop_start / hop_end</code></td><td>Available</td><td>Temporal bucket helpers for streaming GROUP BY.</td></tr>
    <tr><td><code>MATCH_RECOGNIZE</code></td><td>Available</td><td>Complex Event Processing (CEP) pattern matching.</td></tr>
    <tr><td><code>MERGE INTO</code></td><td>Preview</td><td>Upsert/delete on Iceberg tables.</td></tr>
    <tr><td><code>DELETE FROM &lt;iceberg-table&gt;</code></td><td>Preview</td><td>Copy-on-write row delete on Iceberg.</td></tr>
    <tr><td><code>UPDATE &lt;iceberg-table&gt;</code></td><td>Preview</td><td>Copy-on-write row update on Iceberg.</td></tr>
    <tr><td><code>CREATE INCREMENTAL VIEW</code></td><td>Experimental</td><td>Register an incrementally maintained view.</td></tr>
    <tr><td><code>REFRESH / DROP INCREMENTAL VIEW</code></td><td>Experimental</td><td>Lifecycle DDL for incremental views.</td></tr>
    <tr><td><code>CREATE LIVE TABLE</code></td><td>Experimental</td><td>Register a live-ingestion table.</td></tr>
    <tr><td><code>REFRESH / DROP LIVE TABLE</code></td><td>Experimental</td><td>Lifecycle DDL for live tables.</td></tr>
    <tr><td><code>CREATE SOURCE / CREATE SINK</code></td><td>Experimental</td><td>Pipeline DDL for named sources and sinks.</td></tr>
    <tr><td><code>START PIPELINE</code></td><td>Experimental</td><td>Start a named pipeline from registered source/sink.</td></tr>
    <tr><td><code>CREATE EXTERNAL TABLE … STORED AS KAFKA</code></td><td>Preview</td><td>Register a Kafka topic as a streaming table.</td></tr>
    <tr><td><code>&lt;table&gt; FOR SYSTEM_TIME AS OF &lt;expr&gt;</code></td><td>Preview</td><td>Time-travel read on Iceberg tables.</td></tr>
    <tr><td><code>CREATE FUNCTION … RETURNS TABLE LANGUAGE SQL</code></td><td>Available</td><td>SQL-body table-valued functions.</td></tr>
    <tr><td><code>SET shuffle.partitions = N</code></td><td>Available</td><td>Override shuffle bucket count for this session.</td></tr>
    <tr><td><code>DESCRIBE &lt;table&gt;</code></td><td>Available</td><td>Return schema of a registered table.</td></tr>
    <tr><td><code>EXPLAIN [VERBOSE] &lt;query&gt;</code></td><td>Available</td><td>Show DataFusion logical/physical plan.</td></tr>
    <tr><td><code>CALL system.&lt;proc&gt;(...)</code></td><td>Preview</td><td>Iceberg maintenance procedures (expire_snapshots, rewrite_data_files, etc.).</td></tr>
  </tbody>
</table>

<h2 id="error-handling">Error Handling</h2>
<p>SQL errors are returned as typed <code>SqlError</code> values: <code>EmptyQuery</code>, <code>EmptyTableName</code>, <code>Unsupported</code>, <code>DataFusion</code>, <code>Optimizer</code>, <code>AccessDenied</code>, <code>OperationCancelled</code>, <code>Timeout</code>.</p>
`,
  },

  {
    slug: 'sql/window-functions',
    group: 'SQL Reference',
    title: 'Window Functions',
    description: 'Analytic window functions (RANK, LAG, NTILE, …) and temporal windows (TUMBLE, HOP, SESSION).',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv supports the full set of standard SQL analytic window functions plus temporal window helpers (TUMBLE / HOP / SESSION) for streaming <code>GROUP BY</code>. Analytic functions compute over a <em>window</em> defined by <code>OVER (...)</code> and do not collapse rows; temporal windows collapse rows into one per window.</p>

<h2 id="tumble">TUMBLE — Fixed-Size Non-Overlapping Windows</h2>
<p>Each row belongs to exactly one window. Windows are aligned to the epoch.</p>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>tumble_start(ts, interval)</code></td><td>Start timestamp of the tumbling window containing <code>ts</code>.</td></tr>
    <tr><td><code>tumble_end(ts, interval)</code></td><td>End (exclusive) timestamp of the tumbling window containing <code>ts</code>.</td></tr>
  </tbody>
</table>
<pre><code class="language-sql">SELECT
  tumble_start(event_time, INTERVAL '1 minute') AS window_start,
  tumble_end(event_time,   INTERVAL '1 minute') AS window_end,
  COUNT(*) AS events
FROM events
GROUP BY tumble_start(event_time, INTERVAL '1 minute'),
         tumble_end(event_time,   INTERVAL '1 minute');
</code></pre>

<h2 id="hop">HOP — Sliding Windows</h2>
<p>Each row may belong to multiple overlapping windows. Window size ≥ slide size.</p>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>hop_start(ts, slide, size)</code></td><td>Start of the hop window containing <code>ts</code>.</td></tr>
    <tr><td><code>hop_end(ts, slide, size)</code></td><td>End of the hop window containing <code>ts</code>.</td></tr>
  </tbody>
</table>
<pre><code class="language-sql">-- 5-minute windows advancing every 1 minute
SELECT
  hop_start(event_time, INTERVAL '1 minute', INTERVAL '5 minutes') AS window_start,
  COUNT(*) AS events
FROM events
GROUP BY hop_start(event_time, INTERVAL '1 minute', INTERVAL '5 minutes'),
         hop_end(event_time,   INTERVAL '1 minute', INTERVAL '5 minutes');
</code></pre>

<h2 id="analytic">Analytic functions</h2>
<p>Standard <code>OVER (PARTITION BY … ORDER BY …)</code> functions:</p>
<table class="api-table">
  <thead><tr><th>Function</th><th>Returns</th><th>Notes</th></tr></thead>
  <tbody>
    <tr><td><code>ROW_NUMBER()</code></td><td>1, 2, 3, …</td><td>Unique within each window; ties broken by <code>ORDER BY</code> arbitrarily.</td></tr>
    <tr><td><code>RANK()</code></td><td>1, 1, 3, 4</td><td>Ties share the rank; the next rank is skipped.</td></tr>
    <tr><td><code>DENSE_RANK()</code></td><td>1, 1, 2, 3</td><td>Ties share the rank; the next rank is not skipped.</td></tr>
    <tr><td><code>PERCENT_RANK()</code></td><td>0.0 — 1.0</td><td>(rank - 1) / (partition_size - 1).</td></tr>
    <tr><td><code>CUME_DIST()</code></td><td>0.0 — 1.0</td><td>Number of rows with rank ≤ current / partition size.</td></tr>
    <tr><td><code>NTILE(n)</code></td><td>1 — n</td><td>Distribute rows across <code>n</code> buckets as evenly as possible.</td></tr>
    <tr><td><code>LAG(col, n, default)</code></td><td>Previous row's <code>col</code></td><td><code>n</code> rows back. <code>default</code> when out of window.</td></tr>
    <tr><td><code>LEAD(col, n, default)</code></td><td>Next row's <code>col</code></td><td><code>n</code> rows forward.</td></tr>
    <tr><td><code>FIRST_VALUE(col)</code></td><td>First value in window</td><td></td></tr>
    <tr><td><code>LAST_VALUE(col)</code></td><td>Last value in window</td><td>By default respects the frame.</td></tr>
    <tr><td><code>NTH_VALUE(col, n)</code></td><td>n-th value in window</td><td>1-indexed.</td></tr>
  </tbody>
</table>

<h3 id="analytic-examples">Examples</h3>
<pre><code class="language-sql">-- Top 3 orders per customer
SELECT *
FROM (
  SELECT
    customer_id, order_id, amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
  FROM orders
)
WHERE rn &lt;= 3;

-- Day-over-day change
SELECT
  day, amount,
  amount - LAG(amount) OVER (ORDER BY day) AS change
FROM daily_totals;

-- Quartile per region
SELECT
  region, value,
  NTILE(4) OVER (PARTITION BY region ORDER BY value) AS quartile
FROM measurements;
</code></pre>

<h3 id="analytic-frame">Frames</h3>
<p>By default the window frame is <code>RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW</code>. Override with an explicit <code>ROWS</code> / <code>RANGE</code> frame:</p>
<pre><code class="language-sql">SELECT
  ts, value,
  AVG(value) OVER (
    ORDER BY ts
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_avg_7
FROM measurements;
</code></pre>

<h2 id="dataflow-api">Dataflow API Windows</h2>
<p>The Python/Rust streaming APIs expose windows on <code>Stream</code> / <code>KeyedStream</code> directly — see the <a href="/docs/latest/python/stream">Python Stream API</a> and <a href="/docs/latest/rust/stream">Rust Stream API</a> pages.</p>
`,
  },

  {
    slug: 'sql/match-recognize',
    group: 'SQL Reference',
    title: 'MATCH_RECOGNIZE',
    description: 'Complex Event Processing (CEP) pattern matching over event sequences.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p><code>MATCH_RECOGNIZE</code> detects sequences of rows that match a regular-expression-like pattern. Krishiv implements it via the built-in <code>PatternMatcher</code> and intercepts the statement before DataFusion parsing (DataFusion does not parse <code>MATCH_RECOGNIZE</code> natively).</p>

<h2 id="syntax">Syntax</h2>
<pre><code class="language-sql">SELECT *
FROM &lt;source_table&gt;
MATCH_RECOGNIZE (
  PARTITION BY &lt;column&gt; [, ...]
  ORDER BY &lt;column&gt; [ASC|DESC]
  MEASURES
    &lt;pattern_var&gt;.&lt;column&gt; AS &lt;alias&gt; [, ...]
  ONE ROW PER MATCH
  PATTERN (&lt;pattern&gt;)
  DEFINE
    &lt;pattern_var&gt; AS &lt;condition&gt; [, ...]
)
</code></pre>

<h2 id="example">Example — Detect Price Rise followed by Drop</h2>
<pre><code class="language-sql">SELECT * FROM ticks
MATCH_RECOGNIZE (
  PARTITION BY symbol
  ORDER BY ts
  MEASURES
    FIRST(UP.price) AS start_price,
    LAST(DOWN.price) AS end_price
  ONE ROW PER MATCH
  PATTERN (UP+ DOWN+)
  DEFINE
    UP   AS price > PREV(price),
    DOWN AS price < PREV(price)
)
</code></pre>

<h2 id="streaming-notes">Streaming Sources</h2>
<p>When the source table is an unbounded streaming source, Krishiv materialises a bounded window of recent events (default 100 000 rows) and runs the pattern matcher over that window. Set <code>KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT</code> to override. Results cover only the collected window.</p>

<h2 id="clauses">Supported Clauses</h2>
<table class="api-table">
  <thead><tr><th>Clause</th><th>Support</th></tr></thead>
  <tbody>
    <tr><td><code>PARTITION BY</code></td><td>Available — matches run per partition key.</td></tr>
    <tr><td><code>ORDER BY</code></td><td>Available — rows within a partition are sorted before matching.</td></tr>
    <tr><td><code>MEASURES</code></td><td>Available — extract columns from matched pattern variables.</td></tr>
    <tr><td><code>ONE ROW PER MATCH</code></td><td>Available (default).</td></tr>
    <tr><td><code>ALL ROWS PER MATCH</code></td><td>Not yet supported.</td></tr>
    <tr><td><code>PATTERN</code></td><td>Available — quantifiers <code>+</code>, <code>*</code>, <code>?</code>, concatenation, alternation.</td></tr>
    <tr><td><code>DEFINE</code></td><td>Available — Boolean conditions referencing previous row values via <code>PREV()</code> and <code>FIRST()</code>/<code>LAST()</code>.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'sql/merge-into',
    group: 'SQL Reference',
    title: 'MERGE INTO',
    description: 'Upsert and conditional delete on Iceberg tables.',
    status: 'Preview',
    body: `
<h2 id="overview">Overview</h2>
<p><code>MERGE INTO</code> applies upsert and conditional delete logic to an Iceberg table using copy-on-write semantics. The target table must be managed by a registered <code>KrishivCatalog</code> Iceberg catalog.</p>
<div class="warn-box"><strong>Preview:</strong> Copy-on-write semantics are implemented. Merge-on-read and distributed atomic commit certification are ongoing.</div>

<h2 id="syntax">Syntax</h2>
<pre><code class="language-sql">MERGE INTO &lt;target_table&gt; [AS &lt;alias&gt;]
USING &lt;source_table_or_subquery&gt; [AS &lt;alias&gt;]
ON &lt;join_condition&gt;
WHEN MATCHED [AND &lt;condition&gt;] THEN UPDATE SET &lt;col&gt; = &lt;expr&gt; [, ...]
WHEN MATCHED [AND &lt;condition&gt;] THEN DELETE
WHEN NOT MATCHED [AND &lt;condition&gt;] THEN INSERT (&lt;cols&gt;) VALUES (&lt;exprs&gt;)
</code></pre>

<h2 id="example">Example</h2>
<pre><code class="language-sql">MERGE INTO inventory AS tgt
USING incoming_stock AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND src.quantity = 0 THEN DELETE
WHEN MATCHED THEN UPDATE SET tgt.quantity = tgt.quantity + src.quantity
WHEN NOT MATCHED THEN INSERT (product_id, quantity) VALUES (src.product_id, src.quantity)
</code></pre>

<h2 id="return">Return</h2>
<p>The statement returns a single-row result with a <code>merged_rows</code> integer count.</p>

<h2 id="dml">Related DML</h2>
<p>For Iceberg tables, <code>DELETE FROM &lt;table&gt; WHERE &lt;predicate&gt;</code> and <code>UPDATE &lt;table&gt; SET &lt;col&gt; = &lt;expr&gt; WHERE &lt;predicate&gt;</code> are also intercepted and routed through copy-on-write Iceberg delete/update paths when the table is registered under a <code>KrishivCatalog</code>.</p>
`,
  },

  {
    slug: 'sql/incremental-views',
    group: 'SQL Reference',
    title: 'Incremental Views',
    description: 'CREATE INCREMENTAL VIEW DDL for incrementally maintained query results.',
    status: 'Experimental',
    body: `
<h2 id="overview">Overview</h2>
<p>Incremental views maintain a query result that updates incrementally when source data changes, rather than re-running the full query on each tick. Implemented via <code>IncrementalFlow</code> backed by <code>DeltaBatch</code> (weighted Arrow rows).</p>
<div class="warn-box"><strong>Experimental:</strong> End-to-end connector certification and distributed executor-side IVM are in progress.</div>

<h2 id="ddl">DDL Statements</h2>
<table class="api-table">
  <thead><tr><th>Statement</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>CREATE INCREMENTAL VIEW &lt;name&gt; AS &lt;query&gt;</code></td><td>Register an incremental view backed by a SQL query.</td></tr>
    <tr><td><code>DECLARE INCREMENTAL VIEW &lt;name&gt; AS &lt;query&gt; WITH ORDINAL</code></td><td>Register with an explicit ordinal (row-position) column for ordered IVM.</td></tr>
    <tr><td><code>REFRESH INCREMENTAL VIEW &lt;name&gt;</code></td><td>Re-evaluate and update the view from current source data.</td></tr>
    <tr><td><code>DROP INCREMENTAL VIEW &lt;name&gt;</code></td><td>Remove the view registration.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-sql">CREATE INCREMENTAL VIEW order_totals AS
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id;

REFRESH INCREMENTAL VIEW order_totals;
SELECT * FROM order_totals;
</code></pre>

<h2 id="python-api">Python / Rust API</h2>
<p>See the <a href="/docs/latest/concepts/incremental-flow">IncrementalFlow</a> page for the full programmatic surface. The SQL DDL and the <code>IncrementalFlow</code> API share the same underlying registry.</p>

<h2 id="lateness">LATENESS clause</h2>
<p>Set a per-source late-event tolerance that the planner uses to set the watermark lag:</p>
<pre><code class="language-sql">CREATE INCREMENTAL VIEW order_totals AS
  SELECT customer_id, SUM(amount) AS total
  FROM orders
  GROUP BY customer_id
LATENESS event_time INTERVAL '10' SECOND;
</code></pre>
<p>This is sugar for the IVM runtime receiving a 10 s allowed-lateness watermark on <code>orders</code>.</p>

<h2 id="recursive">DECLARE RECURSIVE VIEW</h2>
<p>For graph-shaped queries (transitive closure, recursive hierarchy):</p>
<pre><code class="language-sql">DECLARE RECURSIVE VIEW ancestor_of AS
  SELECT id, parent_id AS ancestor FROM org_chart
  UNION ALL
  SELECT a.id, oc.parent_id FROM ancestor_of a JOIN org_chart oc ON a.ancestor = oc.id;
</code></pre>
<p>Max iterations default: 1000 (configurable per view).</p>

<h2 id="delta">Delta batches (under the hood)</h2>
<p>Each tick produces a <code>DeltaBatch</code> — a <code>RecordBatch</code> with an extra <code>Int64 _weight</code> column where <code>+1</code> means "row was inserted since last tick" and <code>-1</code> means "row was retracted". Downstream consumers (watches, IVM views, sinks) interpret weights to update their own state.</p>

<h2 id="coordinator">Distributed IVM (coordinator-authoritative)</h2>
<p>When a job is split across executors, the coordinator acquires a per-job <code>step_lock</code> before computing the next tick. This makes a remote tick bit-identical to a central tick. For partitioned views, the coordinator parallelises the step across executors and waits for all shards to complete before publishing the merged delta.</p>
<p>On failure, the coordinator re-feeds the pending delta from the last successful checkpoint rather than re-computing from the source. The <code>apply_computed_tick</code> API lets a partial result be replayed into a downstream view that has recovered from a crash.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/concepts/incremental-flow">IncrementalFlow (concepts)</a></li>
  <li><a href="/docs/latest/state/timers">Timers</a></li>
  <li><a href="/docs/latest/recipes/live-table">Live Table recipe</a></li>
</ul>
`,
  },

  {
    slug: 'sql/live-tables',
    group: 'SQL Reference',
    title: 'Live Tables',
    description: 'CREATE LIVE TABLE for streaming ingestion targets queryable by SQL.',
    status: 'Experimental',
    body: `
<h2 id="overview">Overview</h2>
<p>A live table is a named, append-oriented table that accepts rows from the <code>LiveTable.ingest_row()</code> API and is immediately queryable via SQL. Changes can be observed via a <code>change_feed</code> iterator.</p>

<h2 id="ddl">DDL Statements</h2>
<table class="api-table">
  <thead><tr><th>Statement</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>CREATE LIVE TABLE &lt;name&gt;</code></td><td>Register a live table visible to SQL queries.</td></tr>
    <tr><td><code>REFRESH LIVE TABLE &lt;name&gt;</code></td><td>Flush pending inserts into the queryable snapshot.</td></tr>
    <tr><td><code>DROP LIVE TABLE &lt;name&gt;</code></td><td>Remove the live table.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-sql">CREATE LIVE TABLE sensor_readings;
</code></pre>
<pre><code class="language-python">import krishiv as ks

session = ks.Session.embedded()
session.sql("CREATE LIVE TABLE sensor_readings")
lt = session.live_table("sensor_readings")
lt.ingest_row({"sensor_id": "s1", "value": 23.4, "ts": 1700000000})
lt.refresh()
session.sql("SELECT * FROM sensor_readings").show()
</code></pre>
`,
  },

  {
    slug: 'sql/pipeline-ddl',
    group: 'SQL Reference',
    title: 'Pipeline DDL',
    description: 'CREATE SOURCE, CREATE SINK, and START PIPELINE for named connectors.',
    status: 'Experimental',
    body: `
<h2 id="overview">Overview</h2>
<p>Pipeline DDL lets you register named sources and sinks using SQL and then start a pipeline that connects them. The source and sink registrations are stored in the <code>PipelineRegistry</code> and can be inspected or restarted without re-specifying connection details.</p>

<h2 id="statements">Statements</h2>
<table class="api-table">
  <thead><tr><th>Statement</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>CREATE SOURCE &lt;name&gt; FROM &lt;connector&gt; OPTIONS (...)</code></td><td>Register a named data source.</td></tr>
    <tr><td><code>CREATE SINK &lt;name&gt; INTO &lt;connector&gt; OPTIONS (...)</code></td><td>Register a named data sink.</td></tr>
    <tr><td><code>DROP SOURCE &lt;name&gt;</code></td><td>Remove a registered source.</td></tr>
    <tr><td><code>DROP SINK &lt;name&gt;</code></td><td>Remove a registered sink.</td></tr>
    <tr><td><code>START PIPELINE &lt;source&gt; TO &lt;sink&gt;</code></td><td>Start a streaming pipeline connecting source to sink.</td></tr>
  </tbody>
</table>

<h2 id="kafka-example">Kafka Example</h2>
<pre><code class="language-sql">CREATE EXTERNAL TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  ts TIMESTAMP
) STORED AS KAFKA
LOCATION 'orders-topic'
OPTIONS (
  'bootstrap.servers' = 'kafka:9092',
  'group.id'          = 'krishiv-consumer'
);
</code></pre>
`,
  },

  {
    slug: 'sql/as-of-queries',
    group: 'SQL Reference',
    title: 'AS-OF Queries',
    description: 'Time-travel reads on Iceberg tables using FOR SYSTEM_TIME AS OF, plus joins and branches/tags.',
    status: 'Preview',
    feature_flags: ['iceberg'],
    body: `
<h2 id="overview">Overview</h2>
<p>AS-OF queries read a historical snapshot of an Iceberg table as it existed at a given point in time. Krishiv preprocesses the <code>FOR SYSTEM_TIME AS OF</code> clause and resolves it to the appropriate snapshot before handing the rewritten query to DataFusion.</p>

<h2 id="syntax">Syntax</h2>
<pre><code class="language-sql">SELECT *
FROM &lt;table&gt; FOR SYSTEM_TIME AS OF TIMESTAMP '&lt;iso-timestamp&gt;'

-- or using a binding expression
SELECT *
FROM &lt;table&gt; FOR SYSTEM_TIME AS OF &lt;timestamp_expr&gt;
</code></pre>

<h2 id="example">Example</h2>
<pre><code class="language-sql">-- Read the orders table as it was at a specific point in time
SELECT customer_id, SUM(amount) AS total
FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 12:00:00'
GROUP BY customer_id;
</code></pre>

<h2 id="joins">FOR SYSTEM_TIME AS OF on joins</h2>
<p>Use AS-OF on one or both sides of a join to align versions:</p>
<pre><code class="language-sql">-- Orders as of T1, users as of T1
SELECT o.order_id, o.amount, u.tier
FROM orders o FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 00:00:00' AS o
JOIN users  u FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 00:00:00' AS u
  ON o.user_id = u.user_id;
</code></pre>
<p>This is the standard "temporal as-of join" pattern. See <a href="/docs/latest/streaming/joins">Streaming Joins</a> for the streaming equivalent.</p>

<h2 id="branches">References to branches and tags</h2>
<p>Reference a named branch or tag instead of a timestamp:</p>
<pre><code class="language-sql">SELECT * FROM orders FOR SYSTEM_TIME AS OF 'experiment';   -- branch
SELECT * FROM orders FOR SYSTEM_TIME AS OF 'release-v1';    -- tag
</code></pre>
<p>Branches track the latest snapshot on that line of development. Tags are immutable pointers.</p>

<h2 id="snapshot">Choosing a snapshot by ID</h2>
<p>If you have the snapshot id, you can read it directly (avoids the timestamp-to-snapshot lookup):</p>
<pre><code class="language-sql">SELECT * FROM orders FOR SYSTEM_VERSION AS OF 1234567890;
</code></pre>

<h2 id="notes">Notes</h2>
<ul>
  <li>Only supported on Iceberg tables registered under a <code>KrishivCatalog</code> catalog.</li>
  <li>The snapshot closest to and not after the given timestamp is selected.</li>
  <li>Multiple AS-OF refs in the same query are resolved independently.</li>
  <li>AS-OF with a branch / tag is resolved to the current snapshot of that reference at planning time.</li>
  <li>Requires Iceberg catalog configured with the <code>iceberg</code> feature.</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/sql/alter-table">ALTER TABLE</a></li>
  <li><a href="/docs/latest/streaming/joins">Streaming Joins</a> — temporal joins on streaming input</li>
  <li><a href="/docs/latest/recipes/iceberg-time-travel">Iceberg time travel recipe</a></li>
</ul>
`,
  },

  {
    slug: 'sql/set-commands',
    group: 'SQL Reference',
    title: 'SET Commands',
    description: 'Session-level configuration via SET statements and environment variables.',
    status: 'Available',
    body: `
<h2 id="set-statements">SET Statements</h2>
<table class="api-table">
  <thead><tr><th>Statement</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>SET shuffle.partitions = N</code></td><td>Override the shuffle bucket count (positive integer). Pass <code>0</code> to restore auto-sizing.</td></tr>
  </tbody>
</table>
<pre><code class="language-sql">SET shuffle.partitions = 8;
SELECT * FROM large_table;
</code></pre>

<h2 id="env-vars">Environment Variables</h2>
<table class="api-table">
  <thead><tr><th>Variable</th><th>Default</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>KRISHIV_COORDINATOR</code></td><td>—</td><td>gRPC endpoint for distributed mode.</td></tr>
    <tr><td><code>KRISHIV_COORDINATOR_BEARER_TOKEN</code></td><td>—</td><td>Bearer token sent to the coordinator.</td></tr>
    <tr><td><code>KRISHIV_EXECUTOR_TASK_BEARER_TOKEN</code></td><td>—</td><td>Bearer token for executor gRPC.</td></tr>
    <tr><td><code>KRISHIV_PLAN_CACHE_MAX_ENTRIES</code></td><td>256</td><td>Maximum cached query plans per SQL engine.</td></tr>
    <tr><td><code>KRISHIV_QUERY_MEMORY_LIMIT_BYTES</code></td><td>none</td><td>Per-engine DataFusion memory pool limit; 0 = unbounded.</td></tr>
    <tr><td><code>KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT</code></td><td>100000</td><td>Max rows materialised for MATCH_RECOGNIZE on streaming sources.</td></tr>
    <tr><td><code>KRISHIV_DURABILITY_PROFILE</code></td><td>dev-local</td><td>One of <code>dev-local</code>, <code>single-node-durable</code>, <code>distributed-durable</code>.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'sql/udf-sql',
    group: 'SQL Reference',
    title: 'SQL UDFs',
    description: 'CREATE FUNCTION … RETURNS TABLE LANGUAGE SQL for SQL-body table functions.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv intercepts <code>CREATE FUNCTION … RETURNS TABLE</code> DDL to register SQL-body table-valued functions (TVFs). The function body is a parameterised SQL query. Only <code>LANGUAGE SQL</code> is supported; other languages are rejected.</p>

<h2 id="syntax">Syntax</h2>
<pre><code class="language-sql">CREATE FUNCTION &lt;name&gt;(&lt;param&gt; &lt;type&gt; [, ...])
RETURNS TABLE (&lt;col&gt; &lt;type&gt; [, ...])
LANGUAGE SQL
AS '&lt;sql-body&gt;';
</code></pre>

<h2 id="example">Example</h2>
<pre><code class="language-sql">-- Define a TVF that returns recent orders for a customer
CREATE FUNCTION recent_orders(cust_id BIGINT)
RETURNS TABLE (order_id BIGINT, amount DOUBLE, ts TIMESTAMP)
LANGUAGE SQL
AS 'SELECT order_id, amount, ts FROM orders WHERE customer_id = cust_id ORDER BY ts DESC LIMIT 10';

-- Use the TVF in a query
SELECT * FROM recent_orders(42);
</code></pre>

<h2 id="rust-registration">Rust Registration (closure-based)</h2>
<pre><code class="language-rust">engine.register_table_udf_fn(
    "generate_ints",
    Schema::new(vec![Field::new("n", DataType::Int64, false)]),
    |args| {
        let count = match args.first() {
            Some(ScalarValue::Int64(Some(n))) => *n,
            _ => 10,
        };
        let arr = Int64Array::from_iter(0..count);
        Ok(RecordBatch::try_from_iter([("n", Arc::new(arr) as _)])?)
    },
)?;
</code></pre>
`,
  },

  {
    slug: 'sql/error-codes',
    group: 'SQL Reference',
    title: 'Error Codes',
    description: 'SQLSTATE error codes returned by Krishiv SQL operations.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv maps SQL errors to standard SQLSTATE codes via the <code>sqlstate_for</code> function in <code>krishiv-sql</code>. Errors carry both a typed <code>SqlError</code> variant and an SQLSTATE string.</p>

<h2 id="error-variants">SqlError Variants</h2>
<table class="api-table">
  <thead><tr><th>Variant</th><th>SQLSTATE</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>EmptyQuery</code></td><td>42000</td><td>Query was empty or whitespace only.</td></tr>
    <tr><td><code>EmptyTableName</code></td><td>42602</td><td>A table name argument was empty.</td></tr>
    <tr><td><code>Unsupported</code></td><td>0A000</td><td>SQL feature not available in this release.</td></tr>
    <tr><td><code>InvalidTableFunction</code></td><td>42883</td><td>Table function declaration or registration was invalid.</td></tr>
    <tr><td><code>DataFusion</code></td><td>42000</td><td>DataFusion returned an error (message preserved).</td></tr>
    <tr><td><code>Optimizer</code></td><td>42000</td><td>Krishiv plan optimizer failed.</td></tr>
    <tr><td><code>AccessDenied</code></td><td>42501</td><td>Query blocked by auth or policy hook.</td></tr>
    <tr><td><code>OperationCancelled</code></td><td>57014</td><td>Query was cancelled by the caller.</td></tr>
    <tr><td><code>Timeout</code></td><td>57014</td><td>Query exceeded its configured execution timeout.</td></tr>
  </tbody>
</table>

<h2 id="feature-matrix">SQL Feature Matrix</h2>
<p>The <code>feature_matrix()</code> function in <code>krishiv-sql::grammar</code> returns the full list of SQL features with their implementation status (<code>Available</code>, <code>Preview</code>, <code>Experimental</code>, <code>NotPlanned</code>). Use <code>features_by_status()</code> to filter by status or <code>features_for_category()</code> to filter by category.</p>
`,
  },

  {
    slug: 'sql/grouping-sets',
    group: 'SQL Reference',
    title: 'ROLLUP, CUBE, and GROUPING SETS',
    description: 'Multi-level aggregation with ROLLUP, CUBE, GROUPING SETS, and the GROUPING() function.',
    status: 'Available',
    body: `
<p>When you want subtotals at multiple granularities (region × day, region, day, grand total), <code>ROLLUP</code> and <code>CUBE</code> avoid writing the same query four times with different <code>GROUP BY</code> clauses.</p>

<h2 id="rollup">ROLLUP</h2>
<p><code>ROLLUP(a, b, c)</code> produces the hierarchy <code>(a, b, c)</code>, <code>(a, b)</code>, <code>(a)</code>, <code>()</code>. Each rollup level adds one fewer column to the group-by.</p>
<pre><code class="language-sql">SELECT
  region, country, category,
  SUM(amount) AS total,
  COUNT(*)    AS n
FROM orders
GROUP BY ROLLUP(region, country, category)
ORDER BY region NULLS LAST, country NULLS LAST, category NULLS LAST;
</code></pre>
<p>Output rows have <code>NULL</code> in columns that were rolled up. The grand-total row is all <code>NULL</code>s.</p>

<h2 id="cube">CUBE</h2>
<p><code>CUBE(a, b, c)</code> produces every combination: 2³ = 8 rows of grouping (including the grand total).</p>
<pre><code class="language-sql">SELECT region, category, SUM(amount) AS total
FROM orders
GROUP BY CUBE(region, category);
</code></pre>
<p>Use CUBE for OLAP cubes where every combination is meaningful.</p>

<h2 id="grouping-sets">GROUPING SETS</h2>
<p>Explicit list of grouping sets, when ROLLUP / CUBE produce too many:</p>
<pre><code class="language-sql">SELECT region, country, category, SUM(amount) AS total
FROM orders
GROUP BY GROUPING SETS (
  (region, country, category),
  (region, country),
  (region),
  ()
);
</code></pre>
<p>Equivalent to writing four separate <code>UNION ALL</code> queries — but with the cost of a single scan.</p>

<h2 id="grouping-fn">The GROUPING() function</h2>
<p>Distinguish a rollup-level NULL (which means "rolled up") from a data NULL (which means "value was NULL in the source row):</p>
<pre><code class="language-sql">SELECT
  region, country,
  GROUPING(region)   AS region_is_rollup,
  GROUPING(country)  AS country_is_rollup,
  SUM(amount)        AS total
FROM orders
GROUP BY ROLLUP(region, country);
</code></pre>
<p><code>GROUPING(col)</code> returns 1 if <code>col</code> was rolled up in this row, 0 otherwise. <code>GROUPING_ID(a, b, c)</code> returns a bitmask if you prefer a single column.</p>

<h2 id="filtering">Filtering rollup rows</h2>
<p>Often you want to drop the subtotals and keep only the leaf level, or vice versa:</p>
<pre><code class="language-sql">-- Only the grand total
HAVING GROUPING_ID(region, country) = 3;

-- Only the leaves (no rollups)
HAVING GROUPING(region) = 0 AND GROUPING(country) = 0;
</code></pre>

<h2 id="ivm">With incremental views</h2>
<p>ROLLUP / CUBE / GROUPING SETS work inside <code>CREATE INCREMENTAL VIEW</code>. The IVM runtime tracks each grouping set independently and emits per-set deltas.</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/window-functions">Window Functions</a> — analytic <code>OVER (...)</code> functions</li>
  <li><a href="/docs/latest/sql/incremental-views">Incremental Views</a></li>
</ul>
`,
  },

  {
    slug: 'sql/pivot-unpivot',
    group: 'SQL Reference',
    title: 'PIVOT and UNPIVOT',
    description: 'Reshape rows to columns with PIVOT, and columns to rows with UNPIVOT.',
    status: 'Available',
    body: `
<p><code>PIVOT</code> and <code>UNPIVOT</code> are the SQL form of reshape. For the DataFrame API form, see <a href="/docs/latest/python/dataframe#reshape">Python DataFrame — Reshape</a> and <a href="/docs/latest/rust/dataframe#group">Rust DataFrame — Group / Aggregate</a>.</p>

<h2 id="pivot">PIVOT</h2>
<p>Rotates rows to columns, aggregating values that share a key.</p>
<pre><code class="language-sql">SELECT *
FROM orders
PIVOT (
  SUM(amount) FOR region IN ('us-east', 'us-west', 'eu')
) AS p (order_id, customer_id, us_east, us_west, eu);
</code></pre>
<p>The <code>AS p (...)</code> clause names the output columns. Columns not in the <code>PIVOT</code> key (<code>order_id</code>, <code>customer_id</code>) are passed through unchanged.</p>

<h3 id="pivot-multiple">Multiple aggregates</h3>
<pre><code class="language-sql">SELECT *
FROM orders
PIVOT (
  SUM(amount) AS total_amount,
  COUNT(*)    AS n
  FOR region IN ('us-east' AS us_east, 'eu' AS eu)
) AS p (order_id, customer_id, us_east_total, us_east_n, eu_total, eu_n);
</code></pre>

<h2 id="unpivot">UNPIVOT</h2>
<p>The reverse: columns to rows. Useful for turning wide results (one column per period) into long format (a <code>period</code> column and a <code>value</code> column).</p>
<pre><code class="language-sql">SELECT *
FROM monthly_totals
UNPIVOT (
  total FOR month IN (jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)
);
</code></pre>
<p>Result columns: <code>region</code> (passed through), <code>month</code> (the literal label), <code>total</code> (the value).</p>

<h2 id="nulls">NULL handling in UNPIVOT</h2>
<p>By default, <code>UNPIVOT</code> excludes rows where the source column is <code>NULL</code>. Use <code>UNPIVOT INCLUDE NULLS</code> (where supported) to keep them:</p>
<pre><code class="language-sql">SELECT *
FROM monthly_totals
UNPIVOT INCLUDE NULLS (
  total FOR month IN (jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)
);
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/window-functions">Window Functions</a></li>
  <li><a href="/docs/latest/sql/grouping-sets">ROLLUP, CUBE, GROUPING SETS</a></li>
</ul>
`,
  },

  {
    slug: 'sql/lateral-unnest',
    group: 'SQL Reference',
    title: 'LATERAL, UNNEST, and generate_series',
    description: 'Lateral joins, unnesting arrays and structs, and virtual sequences.',
    status: 'Available',
    body: `
<p>Three SQL features that turn row-oriented data into something more useful for analytics: <code>UNNEST</code> flattens arrays, <code>LATERAL</code> lets you reference earlier FROM items in a subquery, and <code>generate_series</code> emits a virtual sequence.</p>

<h2 id="unnest">UNNEST — flatten an array column</h2>
<pre><code class="language-sql">-- 'tags' is a list&lt;utf8&gt; column. UNNEST expands one row per element.
SELECT user_id, tag
FROM users
CROSS JOIN UNNEST(users.tags) AS t(tag);
</code></pre>
<p>Rows whose <code>tags</code> column is <code>NULL</code> are dropped (use <code>CROSS JOIN UNNEST(...)</code> with a default if you want them).</p>

<h2 id="lateral">LATERAL — correlated subquery in FROM</h2>
<p><code>LATERAL</code> lets the subquery on the right reference columns from the left side. The most common use is "for each row, compute something from that row":</p>
<pre><code class="language-sql">-- For each user, the top 3 orders by amount
SELECT u.id, o.amount, o.ts
FROM users u
CROSS JOIN LATERAL (
  SELECT amount, ts
  FROM orders
  WHERE orders.user_id = u.id
  ORDER BY amount DESC
  LIMIT 3
) o;
</code></pre>

<p>Equivalent to a correlated subquery in the SELECT list, but cheaper when you need to return multiple rows per outer row.</p>

<h2 id="generate">generate_series — virtual sequence</h2>
<pre><code class="language-sql">SELECT generate_series(TIMESTAMP '2024-01-01', TIMESTAMP '2024-01-31', INTERVAL '1 day') AS day;
</code></pre>
<p>Three forms:</p>
<table class="api-table">
<thead><tr><th>Form</th><th>Emits</th></tr></thead>
<tbody>
<tr><td><code>generate_series(start, stop)</code></td><td>Integers from <code>start</code> to <code>stop</code>, step 1.</td></tr>
<tr><td><code>generate_series(start, stop, step)</code></td><td>Integers from <code>start</code> to <code>stop</code>, step <code>step</code>.</td></tr>
<tr><td><code>generate_series(start, stop, interval)</code></td><td>Timestamps from <code>start</code> to <code>stop</code>, step <code>interval</code>.</td></tr>
</tbody>
</table>

<p>Use for filling missing time buckets:</p>
<pre><code class="language-sql">WITH days AS (
  SELECT generate_series(DATE '2024-01-01', DATE '2024-01-31', INTERVAL '1 day') AS day
)
SELECT
  d.day,
  COALESCE(SUM(o.amount), 0) AS total
FROM days d
LEFT JOIN orders o ON DATE_TRUNC('day', o.ts) = d.day
GROUP BY d.day
ORDER BY d.day;
</code></pre>

<h2 id="unnest-struct">UNNEST on structs</h2>
<p>Unnest a struct column by naming its fields:</p>
<pre><code class="language-sql">-- 'address' is a struct&lt;street VARCHAR, city VARCHAR, zip VARCHAR&gt;
SELECT user_id, addr.street, addr.city, addr.zip
FROM users
CROSS JOIN UNNEST(users.address) AS a(street, city, zip);
</code></pre>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/recursive-cte">Recursive CTE</a></li>
  <li><a href="/docs/latest/sql/window-functions">Window Functions</a></li>
</ul>
`,
  },

  {
    slug: 'sql/recursive-cte',
    group: 'SQL Reference',
    title: 'Recursive CTE',
    description: 'WITH RECURSIVE for graph traversal and hierarchies, plus DECLARE RECURSIVE VIEW for IVM.',
    status: 'Available',
    body: `
<p>A recursive CTE has a base case and a recursive case joined by <code>UNION</code> or <code>UNION ALL</code>. It runs until the working set is empty or the max-iterations guard trips.</p>

<h2 id="basic">Basic shape</h2>
<pre><code class="language-sql">WITH RECURSIVE ancestors(id, ancestor, depth) AS (
  -- Base: start at each user
  SELECT id, parent_id, 1 FROM users WHERE parent_id IS NOT NULL
  UNION ALL
  -- Recursive: walk up one more level
  SELECT a.id, u.parent_id, a.depth + 1
  FROM ancestors a
  JOIN users u ON a.ancestor = u.id
  WHERE a.depth &lt; 10    -- max-iterations guard
)
SELECT * FROM ancestors;
</code></pre>
<p>Use <code>UNION</code> (deduplicated) for tree-shaped recursion, <code>UNION ALL</code> when duplicate rows are fine (graph traversal).</p>

<h2 id="guards">Iteration guards</h2>
<table class="api-table">
<thead><tr><th>Guard</th><th>When to use</th></tr></thead>
<tbody>
<tr><td><code>WHERE depth &lt; N</code> in the recursive arm</td><td>Tree / bounded hierarchy.</td></tr>
<tr><td>Stop when no new rows are produced</td><td>Default — Krishiv detects a fixed point.</td></tr>
<tr><td>Session-level <code>KRISHIV_RECURSIVE_MAX_ITERATIONS</code> (default 1000)</td><td>Safety net for runaway recursion.</td></tr>
</tbody>
</table>

<h2 id="examples">Common patterns</h2>

<h3 id="bom">Bill of materials explosion</h3>
<pre><code class="language-sql">WITH RECURSIVE bom(part, sub, qty, depth) AS (
  SELECT part, sub, qty, 1 FROM components WHERE part = 'widget'
  UNION ALL
  SELECT bom.part, c.sub, bom.qty * c.qty, bom.depth + 1
  FROM bom JOIN components c ON bom.sub = c.part
  WHERE bom.depth &lt; 20
)
SELECT * FROM bom;
</code></pre>

<h3 id="graph">Shortest path in a graph</h3>
<pre><code class="language-sql">WITH RECURSIVE paths(src, dst, hops, path) AS (
  SELECT src, dst, 1, ARRAY[src, dst]::text[] FROM edges
  UNION ALL
  SELECT p.src, e.dst, p.hops + 1, p.path || ARRAY[e.dst]
  FROM paths p JOIN edges e ON p.dst = e.src
  WHERE NOT (e.dst = ANY(p.path))  -- avoid cycles
    AND p.hops &lt; 10
)
SELECT * FROM paths;
</code></pre>

<h2 id="ivm">Recursive incremental views</h2>
<p>For graph-shaped views that need to update incrementally as new edges arrive, use <code>DECLARE RECURSIVE VIEW</code> in the IVM DSL:</p>
<pre><code class="language-sql">DECLARE RECURSIVE VIEW ancestor_of AS
  SELECT id, parent_id AS ancestor FROM org_chart
  UNION ALL
  SELECT a.id, oc.parent_id FROM ancestor_of a JOIN org_chart oc ON a.ancestor = oc.id;
</code></pre>
<p>Each <code>tick</code> of the IVM job walks one level of recursion. The default max-iterations is 1000 (configurable per view).</p>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/incremental-views">Incremental Views</a> — <code>DECLARE RECURSIVE VIEW</code> and friends</li>
  <li><a href="/docs/latest/sql/lateral-unnest">LATERAL, UNNEST, generate_series</a></li>
</ul>
`,
  },

  {
    slug: 'sql/create-function',
    group: 'SQL Reference',
    title: 'CREATE FUNCTION',
    description: 'Register a SQL-bodied or Python-bodied function as a callable UDF in the session.',
    status: 'Available',
    feature_flags: ['python'],
    body: `
<p><code>CREATE FUNCTION</code> registers a UDF for the rest of the session. The function can be called from SQL, from the DataFrame API, or from another function.</p>

<h2 id="sql-body">LANGUAGE SQL</h2>
<p>Inline the function body as a SQL query. The query must be a single <code>SELECT</code> (with optional <code>FROM</code> for table-valued functions).</p>

<h3 id="sql-scalar">Scalar UDF</h3>
<pre><code class="language-sql">CREATE FUNCTION greet(name VARCHAR) RETURNS VARCHAR
LANGUAGE SQL
AS 'SELECT CONCAT(''hello, '', name)';
</code></pre>
<p>Call it like any built-in function:</p>
<pre><code class="language-sql">SELECT greet('world');   -- "hello, world"
</code></pre>

<h3 id="sql-tvf">Table-valued function (TVF)</h3>
<pre><code class="language-sql">CREATE FUNCTION recent_orders(cust_id BIGINT)
RETURNS TABLE (order_id BIGINT, amount DOUBLE, ts TIMESTAMP)
LANGUAGE SQL
AS 'SELECT order_id, amount, ts FROM orders
    WHERE customer_id = cust_id
    ORDER BY ts DESC LIMIT 10';
</code></pre>
<p>Call it in the <code>FROM</code> clause:</p>
<pre><code class="language-sql">SELECT * FROM recent_orders(42);
</code></pre>

<h2 id="python-body">LANGUAGE PYTHON</h2>
<p>Pass a Python callable. Krishiv wraps the call in a Tokio task and serialises via Arrow C Data Interface.</p>
<pre><code class="language-python">import krishiv as ks
session = ks.Session.embedded()

@ks.udf(return_type="utf8")
def greet(name: str) -> str:
    return f"hello, {name}"

session.register_udf("greet", greet, ["utf8"], "utf8")
session.sql("SELECT greet('world')").show()
</code></pre>
<p>Equivalently, the session's <code>register_scalar_udf</code> and <code>register_aggregate_udf</code> methods accept Rust- or Python-implemented UDFs.</p>

<h2 id="lifecycle">Function lifecycle</h2>
<p>Functions are session-scoped. They are dropped on session close. To persist across sessions, use a startup script or the deployment's bootstrap mechanism.</p>

<h2 id="overload">Overloading</h2>
<p>You can register multiple functions with the same name and different argument types. The planner picks the best match for each call site. If no match exists, you get a clear type error.</p>

<h2 id="security">Security</h2>
<div class="warn-box"><strong>Preview:</strong> <code>LANGUAGE PYTHON</code> is gated by the <code>python</code> Cargo feature. In production profiles (<code>KRISHIV_PRODUCTION=1</code>) native scalar UDFs are <strong>forbidden</strong> by default; enable them only via <code>KRISHIV_ALLOW_FULL_PRIVILEGE_UDFS=1</code> after reviewing the security implications.</div>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/sql/udf-sql">SQL UDFs</a> — registration via <code>register_table_udf_fn</code></li>
  <li><a href="/docs/latest/recipes/sql-udf">SQL UDF recipe</a></li>
</ul>
`,
  },

  {
    slug: 'sql/alter-table',
    group: 'SQL Reference',
    title: 'ALTER TABLE',
    description: 'Add, drop, rename, and reorder columns; widen types; manage schema evolution.',
    status: 'Available',
    feature_flags: ['iceberg'],
    body: `
<p><code>ALTER TABLE</code> in Krishiv is supported primarily for Iceberg tables. The operations are <strong>metadata-only</strong> for the most part — no data rewrite required.</p>

<h2 id="ops">Supported operations</h2>
<table class="api-table">
<thead><tr><th>Operation</th><th>SQL form</th><th>What it does</th></tr></thead>
<tbody>
<tr><td>Add column</td><td><code>ALTER TABLE t ADD COLUMN c TYPE</code></td><td>Metadata-only. New column is <code>NULL</code> for existing rows.</td></tr>
<tr><td>Drop column</td><td><code>ALTER TABLE t DROP COLUMN c</code></td><td>Metadata-only. Readers return <code>NULL</code>.</td></tr>
<tr><td>Rename column</td><td><code>ALTER TABLE t RENAME COLUMN old TO new</code></td><td>Metadata-only. Existing queries break.</td></tr>
<tr><td>Reorder column</td><td><code>ALTER TABLE t ALTER COLUMN c AFTER other</code></td><td>Metadata-only. Storage layout unchanged.</td></tr>
<tr><td>Widen type</td><td><code>ALTER TABLE t ALTER COLUMN c TYPE NEW</code></td><td>Metadata-only if the new type is a superset (e.g. INT → BIGINT, FLOAT → DOUBLE).</td></tr>
<tr><td>Set comment</td><td><code>ALTER TABLE t ALTER COLUMN c COMMENT '...'</code></td><td>Metadata-only.</td></tr>
<tr><td>Add partition spec</td><td><code>ALTER TABLE t ADD PARTITION FIELD bucket(16, id)</code></td><td>Metadata-only. New writes use the new spec; old data is not rewritten.</td></tr>
</tbody>
</table>

<h2 id="examples">Examples</h2>
<pre><code class="language-sql">-- Add a new column (NULL for existing rows)
ALTER TABLE orders ADD COLUMN region VARCHAR;

-- Rename safely (will break old queries — deploy in lockstep)
ALTER TABLE orders RENAME COLUMN amt TO amount;

-- Widen a type (INT → BIGINT is metadata-only)
ALTER TABLE orders ALTER COLUMN user_id TYPE BIGINT;

-- Drop a column
ALTER TABLE orders DROP COLUMN legacy_col;
</code></pre>

<h2 id="compatibility">Compatibility modes</h2>
<p>Set the compatibility mode on the table to control which operations are allowed:</p>
<table class="api-table">
<thead><tr><th>Mode</th><th>Description</th></tr></thead>
<tbody>
<tr><td><code>backward</code> (default)</td><td>Old readers can read new data. Add/drop/widen are OK.</td></tr>
<tr><td><code>forward</code></td><td>New readers can read old data. Drop/widen are OK.</td></tr>
<tr><td><code>full</code></td><td>Both. Most restrictive.</td></tr>
<tr><td><code>none</code></td><td>No checks. Use only for development.</td></tr>
</tbody>
</table>

<h2 id="branch">Branches and tags for risky changes</h2>
<p>For destructive experiments, branch first:</p>
<pre><code class="language-sql">-- Create a branch at the current main snapshot
CALL system.create_branch('orders', 'experiment', main_ref);

-- Run the migration on the branch
ALTER TABLE orders_branch ADD COLUMN region VARCHAR;

-- Inspect via time-travel
SELECT * FROM orders FOR SYSTEM_TIME AS OF 'experiment';

-- Promote or drop
CALL system.fast_forward('orders', 'experiment', main_ref);
CALL system.drop_branch('orders', 'experiment');
</code></pre>

<h2 id="unsupported">Unsupported operations</h2>
<ul>
<li>Narrowing a column type (DOUBLE → FLOAT).</li>
<li>Renaming a partition column.</li>
<li>Changing the partition spec on existing data without a re-partition job.</li>
</ul>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/connectors/iceberg">Iceberg</a></li>
  <li><a href="/docs/latest/sql/as-of-queries">AS-OF Queries</a></li>
  <li><a href="/docs/latest/recipes/iceberg-schema-migration">Iceberg schema migration recipe</a></li>
</ul>
`,
  },
];
