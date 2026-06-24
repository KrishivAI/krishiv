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
    description: 'TUMBLE, HOP, and SESSION temporal windows for streaming GROUP BY.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Krishiv registers temporal window helper UDFs on top of DataFusion's standard analytic window functions. Use these with GROUP BY to aggregate streaming data into fixed-size or sliding time windows.</p>

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
<p>See the <a href="/docs/latest/python/session">Python Session</a> page for <code>IncrementalFlow</code> bindings. The SQL DDL and the <code>IncrementalFlow</code> API share the same underlying registry.</p>
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
    description: 'Time-travel reads on Iceberg tables using FOR SYSTEM_TIME AS OF.',
    status: 'Preview',
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

<h2 id="notes">Notes</h2>
<ul>
  <li>Only supported on Iceberg tables registered under a <code>KrishivCatalog</code> catalog.</li>
  <li>The snapshot closest to and not after the given timestamp is selected.</li>
  <li>Multiple AS-OF refs in the same query are resolved independently.</li>
  <li>Requires Iceberg catalog configured with the <code>iceberg</code> feature.</li>
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
];
