import type { DocPage } from '../docs-data';

export const pythonPages: DocPage[] = [
  {
    slug: 'python',
    group: 'Python API',
    title: 'Python API Overview',
    description: 'krishiv Python package — PyO3 bindings for the Krishiv compute engine.',
    status: 'Available',
    body: `
<h2 id="install">Installation</h2>
<pre><code class="language-bash">maturin develop --manifest-path crates/krishiv-python/Cargo.toml
</code></pre>
<pre><code class="language-python">import krishiv as ks
from krishiv import Session, DataFrame, Stream
from krishiv.functions import col, lit, sum, avg, count
from krishiv.sql import functions as sf   # SQL helper functions
</code></pre>

<h2 id="modules">Package Layout</h2>
<table class="api-table">
  <thead><tr><th>Module</th><th>Contents</th></tr></thead>
  <tbody>
    <tr><td><code>krishiv</code></td><td>All public classes: <code>Session</code>, <code>DataFrame</code>, <code>Stream</code>, sinks, state, etc.</td></tr>
    <tr><td><code>krishiv.functions</code></td><td>Expression builder functions (<code>col</code>, <code>lit</code>, <code>sum</code>, <code>avg</code>, …)</td></tr>
    <tr><td><code>krishiv.sql.functions</code></td><td>SQL scalar functions (<code>upper</code>, <code>lower</code>, <code>date_trunc</code>, <code>coalesce</code>, …)</td></tr>
  </tbody>
</table>

<h2 id="quickstart">Quick Start</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv.functions import col, lit, sum

# Embedded session (in-process, no daemon)
session = ks.Session.embedded()

# SQL
df = session.sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
df.show()

# DataFrame API
df2 = session.read_parquet("data/orders.parquet")
result = (df2
    .filter(col("amount") > lit(100))
    .group_by(["customer_id"])
    .agg([sum(col("amount")).alias("total")])
    .order_by(["total"], ascending=False)
    .limit(10))
result.show()
</code></pre>
`,
  },

  {
    slug: 'python/session',
    group: 'Python API',
    title: 'Session',
    description: 'Session class — entry point for all Krishiv Python workloads.',
    status: 'Available',
    body: `
<h2 id="constructors">Constructors</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>Session.embedded()</code></td><td>Create an in-process embedded session. No daemon or cluster needed.</td></tr>
    <tr><td><code>Session.local()</code></td><td>Create a single-node local session.</td></tr>
    <tr><td><code>Session.from_env()</code></td><td>Create a session using <code>KRISHIV_COORDINATOR</code> environment variable.</td></tr>
    <tr><td><code>Session.connect(url: str)</code></td><td>Connect to a remote Krishiv coordinator at <code>url</code>.</td></tr>
    <tr><td><code>Session()</code></td><td>Create a default embedded session (same as <code>Session.embedded()</code>).</td></tr>
  </tbody>
</table>

<div class="note-box"><strong>Common methods:</strong> Most workloads use only a handful of <code>Session</code> methods. Start with: <code>embedded()</code>, <code>sql(query)</code>, <code>read_parquet(path)</code>, <code>register_parquet(name, path)</code>, <code>table(name)</code>, and <code>read_stream()</code>. The full tables below are the complete reference.</div>

<h2 id="sql-methods">SQL Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>sql(query: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Plan and return a lazy DataFrame for the given SQL.</td></tr>
    <tr><td><code>sql_as(query: str, name: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Plan SQL and alias the result as a temp view.</td></tr>
    <tr><td><code>sql_with_timeout(query: str, timeout_ms: int) -> DataFrame</code></td><td><code>DataFrame</code></td><td>SQL with execution timeout in milliseconds.</td></tr>
    <tr><td><code>prepare(query: str) -> PreparedStatement</code></td><td><code>PreparedStatement</code></td><td>Create a parameterised prepared statement.</td></tr>
    <tr><td><code>execute_local(query: str) -> QueryResult</code></td><td><code>QueryResult</code></td><td>Execute SQL immediately and return all results.</td></tr>
    <tr><td><code>execute_remote(query: str) -> QueryHandle</code></td><td><code>QueryHandle</code></td><td>Submit SQL to a remote coordinator asynchronously.</td></tr>
  </tbody>
</table>

<h2 id="data-registration">Data Registration</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>read_parquet(path: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read a local Parquet file.</td></tr>
    <tr><td><code>read_parquet_with_options(path, opts) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read Parquet with typed options (batch_size, etc.).</td></tr>
    <tr><td><code>read_csv(path: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read a local CSV file (auto-detects header).</td></tr>
    <tr><td><code>read_csv_with_options(path, opts) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read CSV with options (delimiter, has_header).</td></tr>
    <tr><td><code>read_json(path: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read a local NDJSON file.</td></tr>
    <tr><td><code>read_file(path: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Read a file (format inferred from extension).</td></tr>
    <tr><td><code>register_parquet(name, path)</code></td><td><code>None</code></td><td>Register a Parquet file as a named SQL table.</td></tr>
    <tr><td><code>register_record_batches(name, batches)</code></td><td><code>None</code></td><td>Register PyArrow batches as a SQL table.</td></tr>
    <tr><td><code>register_unbounded(name, schema)</code></td><td><code>None</code></td><td>Register an unbounded streaming table. Returns a push handle.</td></tr>
    <tr><td><code>register_kafka_source(name, schema, brokers, topic, group)</code></td><td><code>None</code></td><td>Register a Kafka topic as a streaming table.</td></tr>
    <tr><td><code>table(name: str) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Reference a registered table as a DataFrame.</td></tr>
    <tr><td><code>dataframe(record_batches) -> DataFrame</code></td><td><code>DataFrame</code></td><td>Create a DataFrame from a list of PyArrow RecordBatches.</td></tr>
    <tr><td><code>deregister_table(name)</code></td><td><code>None</code></td><td>Remove a registered table.</td></tr>
    <tr><td><code>table_exists(name: str) -> bool</code></td><td><code>bool</code></td><td>Check if a table is registered.</td></tr>
    <tr><td><code>list_tables() -> list[str]</code></td><td><code>list[str]</code></td><td>Return registered table names.</td></tr>
  </tbody>
</table>

<h2 id="udf-registration">UDF Registration</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>register_udf(name, fn, input_types, return_type)</code></td><td>Register a Python callable as a scalar UDF.</td></tr>
    <tr><td><code>register_aggregate_udf(name, accumulator_class)</code></td><td>Register a Python class as an aggregate UDF.</td></tr>
    <tr><td><code>register_table_udf(name, fn, schema)</code></td><td>Register a Python callable as a table-valued UDF.</td></tr>
    <tr><td><code>register_function(name, fn)</code></td><td>Register a generic Python function (type-inferred).</td></tr>
    <tr><td><code>list_udfs() -> list[str]</code></td><td>List registered scalar UDF names.</td></tr>
    <tr><td><code>list_aggregate_udfs() -> list[str]</code></td><td>List registered aggregate UDF names.</td></tr>
    <tr><td><code>list_table_udfs() -> list[str]</code></td><td>List registered table UDF names.</td></tr>
  </tbody>
</table>

<h2 id="streaming">Streaming Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>stream(name: str) -> Stream</code></td><td><code>Stream</code></td><td>Reference a registered unbounded table as a Stream.</td></tr>
    <tr><td><code>memory_stream(schema) -> tuple[Stream, Sender]</code></td><td><code>(Stream, Sender)</code></td><td>Create an in-memory stream and its push handle.</td></tr>
    <tr><td><code>memory_stream_collect(schema, batches) -> Stream</code></td><td><code>Stream</code></td><td>Create a bounded stream pre-loaded with batches.</td></tr>
    <tr><td><code>from_bounded_stream(schema, batches) -> Stream</code></td><td><code>Stream</code></td><td>Create a bounded (finite) stream.</td></tr>
    <tr><td><code>from_source(source) -> Stream</code></td><td><code>Stream</code></td><td>Create a stream from a source connector object.</td></tr>
    <tr><td><code>submit_stream_job(plan, name) -> JobStatus</code></td><td><code>JobStatus</code></td><td>Submit a streaming plan to the scheduler.</td></tr>
    <tr><td><code>push_stream_job_input(job_id, batch)</code></td><td><code>None</code></td><td>Push a batch to a running stream job's unbounded input.</td></tr>
    <tr><td><code>poll_stream_job(job_id) -> JobStatus</code></td><td><code>JobStatus</code></td><td>Poll status of a submitted stream job.</td></tr>
    <tr><td><code>close_unbounded_input(table_name)</code></td><td><code>None</code></td><td>Signal end-of-stream for an unbounded input table.</td></tr>
    <tr><td><code>read_stream() -> DataStreamReader</code></td><td><code>DataStreamReader</code></td><td>Get a Spark-style structured streaming reader.</td></tr>
    <tr><td><code>submit_async(plan) -> QueryHandle</code></td><td><code>QueryHandle</code></td><td>Submit a streaming plan and get an async handle.</td></tr>
  </tbody>
</table>

<h2 id="config-auth">Config and Auth</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>get_config(key: str) -> str | None</code></td><td>Read a session config value.</td></tr>
    <tr><td><code>set_config(key: str, value: str)</code></td><td>Set a session config value.</td></tr>
    <tr><td><code>unset_config(key: str)</code></td><td>Remove a session config override.</td></tr>
    <tr><td><code>configs() -> dict[str, str]</code></td><td>Return all current session configs.</td></tr>
    <tr><td><code>mode() -> str</code></td><td>Return the current execution mode string.</td></tr>
    <tr><td><code>with_auth_token(token: str) -> Session</code></td><td>Return a session copy with a bearer token attached.</td></tr>
    <tr><td><code>with_oidc_provider(provider) -> Session</code></td><td>Return a session copy with an OIDC auth provider.</td></tr>
    <tr><td><code>with_policy(hook) -> Session</code></td><td>Return a session copy with a governance policy hook.</td></tr>
    <tr><td><code>operation_registry() -> OperationRegistry</code></td><td>Access the operation cancellation/progress registry.</td></tr>
    <tr><td><code>jobs() -> list[JobStatus]</code></td><td>List running and recent jobs.</td></tr>
    <tr><td><code>live_table(name: str) -> LiveTable</code></td><td>Access a registered live table.</td></tr>
    <tr><td><code>is_streaming_query(query: str) -> bool</code></td><td>Check if a SQL string references a streaming source.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'python/dataframe',
    group: 'Python API',
    title: 'DataFrame',
    description: 'DataFrame class — lazy query plan builder for batch and streaming.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p><code>DataFrame</code> is lazy — operations build a logical plan. Execution happens on <code>collect()</code>, <code>show()</code>, or <code>write_*</code> methods.</p>

<div class="note-box"><strong>Common methods:</strong> <code>filter</code>, <code>select</code>, <code>group_by</code>, <code>agg</code>, <code>order_by</code>, <code>limit</code>, <code>join</code>, and <code>with_column</code> cover ~90% of batch transformations. Reach for <code>pivot</code>, <code>unpivot</code>, <code>sample</code>, and the rest only when you need them.</div>

<h2 id="project">Project / Schema</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>select(*cols)</code></td><td>Project by Column expressions, names, or SQL strings.</td></tr>
    <tr><td><code>select_columns(*names)</code></td><td>Project by column name strings.</td></tr>
    <tr><td><code>select_exprs(*exprs)</code></td><td>Project by SQL expression strings.</td></tr>
    <tr><td><code>with_column(name, col_expr)</code></td><td>Add or replace a column.</td></tr>
    <tr><td><code>drop_columns(*names)</code></td><td>Remove columns by name.</td></tr>
    <tr><td><code>rename(old, new)</code></td><td>Rename a single column.</td></tr>
    <tr><td><code>alias(name)</code></td><td>Alias the DataFrame as a subquery name.</td></tr>
  </tbody>
</table>
<pre><code class="language-python">df.select(col("customer_id"), sum(col("amount")).alias("total"))
df.with_column("is_high_value", col("total") &gt; lit(1000))
df.rename("cust", "customer_id")
</code></pre>

<h2 id="filter">Filter / Shape</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>filter(condition)</code></td><td>Boolean Column or SQL string.</td></tr>
    <tr><td><code>filter_column(col_name, condition)</code></td><td>Filter on a named column.</td></tr>
    <tr><td><code>where(condition)</code></td><td>Alias for <code>filter</code>.</td></tr>
    <tr><td><code>limit(n: int)</code></td><td>Keep at most <code>n</code> rows.</td></tr>
    <tr><td><code>distinct()</code></td><td>Remove duplicate rows.</td></tr>
    <tr><td><code>drop_nulls(*cols)</code></td><td>Drop rows with NULL in any (or all) of the named columns.</td></tr>
    <tr><td><code>fill_null(value, *cols)</code></td><td>Fill NULLs with a constant or a per-column map.</td></tr>
    <tr><td><code>sample(fraction, seed=None)</code></td><td>Random row sampling (Bernoulli).</td></tr>
    <tr><td><code>repartition(n)</code></td><td>Set the output partition count.</td></tr>
  </tbody>
</table>

<h2 id="group">Group / Aggregate</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>group_by(*cols)</code></td><td>Group by Column expressions.</td></tr>
    <tr><td><code>group_by_columns(*names)</code></td><td>Group by column name strings.</td></tr>
    <tr><td><code>GroupedDataFrame.agg(*exprs)</code></td><td>Apply aggregates. Returns a DataFrame.</td></tr>
    <tr><td><code>GroupedDataFrame.agg_columns(*cols)</code></td><td>Typed-Column overload.</td></tr>
    <tr><td><code>GroupedDataFrame.agg_grouping_sets(spec, *exprs)</code></td><td><code>GROUPING SETS</code> (or CUBE / ROLLUP via spec).</td></tr>
    <tr><td><code>GroupedDataFrame.count()</code></td><td>Shorthand for <code>agg([count_all()])</code>.</td></tr>
    <tr><td><code>GroupedDataFrame.cube(*cols)</code> / <code>rollup(*cols)</code></td><td>CUBE / ROLLUP shortcuts.</td></tr>
  </tbody>
</table>
<pre><code class="language-python">(df.group_by("region")
   .agg(sum(col("amount")).alias("total"),
        count_all().alias("n"))
   .order_by("total", ascending=False)
   .limit(10))
</code></pre>

<h2 id="join">Join</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>join(other, on, how='inner')</code></td><td>Equi-join. <code>on</code> is a column name or a list.</td></tr>
    <tr><td><code>join_on(other, condition, how='inner')</code></td><td>Join with an arbitrary ON expression (non-equi).</td></tr>
  </tbody>
</table>
<p><code>how</code> is one of <code>'inner'</code>, <code>'left'</code>, <code>'right'</code>, <code>'full'</code>, <code>'left_semi'</code>, <code>'right_semi'</code>, <code>'left_anti'</code>, <code>'right_anti'</code>.</p>
<p>For temporal / interval joins on streaming input, see <a href="/docs/latest/streaming/joins">Streaming Joins</a>.</p>

<h2 id="set">Set operations</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>SQL</th></tr></thead>
  <tbody>
    <tr><td><code>union(other)</code></td><td><code>UNION ALL</code></td></tr>
    <tr><td><code>union_distinct(other)</code></td><td><code>UNION DISTINCT</code></td></tr>
    <tr><td><code>intersect(other)</code> / <code>intersect_distinct(other)</code></td><td><code>INTERSECT DISTINCT</code></td></tr>
    <tr><td><code>except_(other)</code> / <code>except_distinct(other)</code></td><td><code>EXCEPT DISTINCT</code></td></tr>
  </tbody>
</table>
<p>All require matching schemas. Use <code>select</code> first to align columns.</p>

<h2 id="reshape">Reshape</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>pivot(group, pivot_column, aggregate, values)</code></td><td>Pivot rows to columns. <code>agg</code> defaults to <code>'first'</code>.</td></tr>
    <tr><td><code>unpivot(ids, values, var_col, val_col)</code></td><td>Melt columns into rows.</td></tr>
  </tbody>
</table>
<p>For the SQL syntax form of <code>PIVOT</code> / <code>UNPIVOT</code>, see <a href="/docs/latest/sql/pivot-unpivot">SQL PIVOT / UNPIVOT</a>.</p>

<h2 id="stream">Stream</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>to_streaming()</code></td><td><code>StreamingDataFrame</code></td><td>Convert to a structured streaming pipeline.</td></tr>
  </tbody>
</table>
<p>Once on a <code>StreamingDataFrame</code>, see <a href="/docs/latest/python/stream">Stream</a> for windowed, keyed, and side-output operators.</p>

<h2 id="cache">Cache</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>cache()</code></td><td>Materialise in memory. Future <code>collect</code>/<code>show</code> skip recomputation.</td></tr>
    <tr><td><code>persist()</code></td><td>Alias for <code>cache</code>.</td></tr>
    <tr><td><code>unpersist()</code></td><td>Drop the cached materialisation.</td></tr>
    <tr><td><code>create_or_replace_temp_view(name)</code></td><td>Register as a SQL temp view for the rest of the session.</td></tr>
  </tbody>
</table>
<div class="note-box"><strong>Note:</strong> <code>cache</code> holds a copy in RAM. For large results, write to Parquet and re-read instead.</div>

<h2 id="write">Write</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>write() -&gt; DataFrameWriter</code></td><td>Return a writer with format-specific options.</td></tr>
    <tr><td><code>write_parquet(path, compression=None)</code></td><td>Local Parquet file or object-store URI.</td></tr>
    <tr><td><code>write_parquet_with_options(path, *, compression, max_row_group_size)</code></td><td>Same with typed options.</td></tr>
    <tr><td><code>write_csv(path)</code></td><td>Local CSV file.</td></tr>
    <tr><td><code>write_csv_with_options(path, *, delimiter, has_header)</code></td><td>Same with options.</td></tr>
    <tr><td><code>write_json(path)</code></td><td>NDJSON.</td></tr>
    <tr><td><code>write_file(path, format, *, mode, partition_by, max_rows_per_file)</code></td><td>Unified writer; format inferred if omitted.</td></tr>
    <tr><td><code>write_stream() -&gt; DataStreamWriter</code></td><td>Streaming writer. See <a href="/docs/latest/streaming/queries-and-lifecycle">Queries and Lifecycle</a>.</td></tr>
  </tbody>
</table>

<h2 id="execute">Execute</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>collect() -&gt; list[RecordBatch]</code></td><td>Execute and collect all Arrow batches.</td></tr>
    <tr><td><code>collect_async() -&gt; Awaitable</code></td><td>Async version, for use in async contexts.</td></tr>
    <tr><td><code>collect_batches()</code></td><td>Alias for <code>collect</code>.</td></tr>
    <tr><td><code>collect_pretty() -&gt; str</code></td><td>Return a formatted ASCII table string.</td></tr>
    <tr><td><code>collect_with_stats() -&gt; tuple</code></td><td>Returns <code>(batches, ExecutionStats)</code> with output_rows and cpu_nanos.</td></tr>
    <tr><td><code>show(n=20)</code></td><td>Print the first <code>n</code> rows.</td></tr>
    <tr><td><code>describe() -&gt; DataFrame</code></td><td>Summary statistics: count, null_count, mean, std, min, max, median.</td></tr>
    <tr><td><code>num_rows() -&gt; int</code></td><td>Execute and return the row count.</td></tr>
    <tr><td><code>execute_stream_async() -&gt; Awaitable</code></td><td>Async execute returning a batch iterator.</td></tr>
  </tbody>
</table>

<h2 id="explain">Explain</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>explain(verbose=False) -&gt; str</code></td><td>Default plan text (logical + physical).</td></tr>
    <tr><td><code>explain_logical() -&gt; str</code></td><td>Logical plan only.</td></tr>
    <tr><td><code>explain_mode(mode) -&gt; str</code></td><td>Specify the explain mode. <code>mode</code> is one of <code>'logical'</code>, <code>'physical'</code>, <code>'analyze'</code>.</td></tr>
  </tbody>
</table>
<p><code>explain</code> is free — it does not run the plan, just inspects it. Use it to verify pushdown, partition pruning, and join order.</p>

<h2 id="schema">Schema and metadata</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>schema() -&gt; Schema</code></td><td>Arrow schema of the output.</td></tr>
    <tr><td><code>columns() -&gt; list[str]</code></td><td>Column names.</td></tr>
    <tr><td><code>is_bounded() -&gt; bool</code></td><td>True for batch / False for streaming.</td></tr>
    <tr><td><code>boundedness() -&gt; str</code></td><td><code>'bounded'</code> or <code>'unbounded'</code>.</td></tr>
  </tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/rust/dataframe">Rust DataFrame</a> — same surface in Rust</li>
  <li><a href="/docs/latest/python/stream">Python Stream</a> — for unbounded plans</li>
  <li><a href="/docs/latest/streaming/queries-and-lifecycle">Queries and Lifecycle</a> — write_stream, output modes, triggers</li>
</ul>
`,
  },

  {
    slug: 'python/stream',
    group: 'Python API',
    title: 'Stream & Windows',
    description: 'Stream, KeyedStream, WindowedStream — streaming pipeline builders.',
    status: 'Available',
    body: `
<h2 id="stream">Stream</h2>
<div class="note-box"><strong>Common pattern:</strong> Get a stream from <code>session.memory_stream(schema)</code> or <code>session.read_kafka(...)</code>, call <code>key_by</code>, then <code>tumbling_window</code> or <code>sliding_window_ms</code>, then <code>agg</code>, then <code>collect</code> (bounded) or <code>try_next</code> (unbounded). See <a href="/docs/latest/recipes/tumbling-window">Tumbling window recipe</a> for a full example.</div>
<p>Obtained from <code>session.stream(name)</code>, <code>session.memory_stream(schema)</code>, or the top-level <code>read_kafka()</code> / <code>read_kinesis()</code> helpers.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>key_by(key_col: str) -> KeyedStream</code></td><td><code>KeyedStream</code></td><td>Partition the stream by a key column.</td></tr>
    <tr><td><code>broadcast() -> BroadcastStream</code></td><td><code>BroadcastStream</code></td><td>Broadcast stream to all operator instances.</td></tr>
    <tr><td><code>connect(other: Stream) -> ConnectedStreams</code></td><td><code>ConnectedStreams</code></td><td>Pair two streams for co-process.</td></tr>
    <tr><td><code>watermark(col, lag_ms) -> Stream</code></td><td><code>Stream</code></td><td>Assign watermark using event-time column and lag.</td></tr>
    <tr><td><code>with_watermark(spec) -> Stream</code></td><td><code>Stream</code></td><td>Assign watermark with a typed WatermarkSpec.</td></tr>
    <tr><td><code>with_multi_source_watermark(spec) -> Stream</code></td><td><code>Stream</code></td><td>Multi-source watermark alignment.</td></tr>
    <tr><td><code>with_state_ttl(config) -> Stream</code></td><td><code>Stream</code></td><td>Attach TTL policy to downstream keyed state.</td></tr>
    <tr><td><code>tumbling_window(size_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Fixed-size non-overlapping window.</td></tr>
    <tr><td><code>tumbling_window_ms(size_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Same as <code>tumbling_window</code>.</td></tr>
    <tr><td><code>sliding_window_ms(size_ms, slide_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Sliding (hop) window.</td></tr>
    <tr><td><code>session_window_ms(gap_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Session window with inactivity gap.</td></tr>
  </tbody>
</table>

<h2 id="keyedstream">KeyedStream</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>tumbling_window(size_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Per-key tumbling window.</td></tr>
    <tr><td><code>tumbling_window_ms(size_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Alias.</td></tr>
    <tr><td><code>sliding_window_ms(size_ms, slide_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Per-key sliding window.</td></tr>
    <tr><td><code>session_window_ms(gap_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Per-key session window.</td></tr>
    <tr><td><code>window(spec) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Apply a typed window specification.</td></tr>
    <tr><td><code>connect(other: KeyedStream) -> ConnectedStreams</code></td><td><code>ConnectedStreams</code></td><td>Pair two keyed streams for co-process.</td></tr>
    <tr><td><code>with_multi_source_watermark(spec) -> KeyedStream</code></td><td><code>KeyedStream</code></td><td>Multi-source watermark for aligned processing.</td></tr>
  </tbody>
</table>

<h2 id="windowedstream">WindowedStream</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>agg(exprs) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Apply aggregate expressions to each window.</td></tr>
    <tr><td><code>collect() -> list[RecordBatch]</code></td><td><code>list</code></td><td>Materialise all window results (bounded streams only).</td></tr>
    <tr><td><code>try_next() -> RecordBatch | None</code></td><td><code>RecordBatch | None</code></td><td>Poll for the next window result batch.</td></tr>
    <tr><td><code>window_kind() -> str</code></td><td><code>str</code></td><td>"Tumbling", "Sliding", or "Session".</td></tr>
    <tr><td><code>window_size_ms() -> int</code></td><td><code>int</code></td><td>Window size in milliseconds.</td></tr>
    <tr><td><code>slide_ms() -> int | None</code></td><td><code>int | None</code></td><td>Slide interval (sliding windows only).</td></tr>
    <tr><td><code>session_gap_ms() -> int | None</code></td><td><code>int | None</code></td><td>Inactivity gap (session windows only).</td></tr>
    <tr><td><code>tumbling_window(size_ms) -> WindowedStream</code></td><td><code>WindowedStream</code></td><td>Re-window an existing windowed stream.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv.functions import count, sum, col

session = ks.Session.embedded()
schema = ...  # PyArrow Schema
stream, sender = session.memory_stream(schema)

windowed = (stream
    .watermark("event_time", 5000)   # 5-second lag
    .key_by("user_id")
    .tumbling_window(60_000))        # 1-minute windows

result = windowed.agg([count(col("*")).alias("events"), sum(col("amount")).alias("total")])

# Push data
import pyarrow as pa
sender.send(pa.record_batch(...))

# Collect results
batches = result.collect()
</code></pre>
`,
  },

  {
    slug: 'python/functions',
    group: 'Python API',
    title: 'Expression Functions',
    description: 'col, lit, expr, aggregate and scalar functions in krishiv.functions.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>Import from <code>krishiv.functions</code> or <code>krishiv.sql.functions</code>:</p>
<pre><code class="language-python">from krishiv.functions import col, lit, sum, avg, count, expr
from krishiv.sql.functions import upper, lower, date_trunc, coalesce
</code></pre>

<h2 id="column-literal">Column and Literal</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Signature</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>col(name)</code></td><td><code>col(name: str) -> Column</code></td><td>Reference a column by name.</td></tr>
    <tr><td><code>column(name)</code></td><td><code>column(name: str) -> Column</code></td><td>Alias for <code>col</code>.</td></tr>
    <tr><td><code>lit(value)</code></td><td><code>lit(value: Any) -> Column</code></td><td>Constant literal value (int, float, str, bool, None).</td></tr>
    <tr><td><code>expr(sql)</code></td><td><code>expr(sql: str) -> Column</code></td><td>Parse a SQL expression string into a Column.</td></tr>
    <tr><td><code>call_function(name, *args)</code></td><td><code>Column</code></td><td>Call a named SQL function or UDF with Column arguments.</td></tr>
    <tr><td><code>function(name, *args)</code></td><td><code>Column</code></td><td>Alias for <code>call_function</code>.</td></tr>
  </tbody>
</table>

<h2 id="aggregates">Aggregate Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>count(col='*')</code></td><td>Count rows or non-null values of a column.</td></tr>
    <tr><td><code>count_all()</code></td><td>COUNT(*) — count all rows.</td></tr>
    <tr><td><code>sum(col)</code></td><td>Sum of numeric values.</td></tr>
    <tr><td><code>avg(col)</code></td><td>Arithmetic mean.</td></tr>
    <tr><td><code>mean(col)</code></td><td>Alias for <code>avg</code>.</td></tr>
    <tr><td><code>min(col)</code></td><td>Minimum value.</td></tr>
    <tr><td><code>max(col)</code></td><td>Maximum value.</td></tr>
  </tbody>
</table>

<h2 id="null-functions">Null / Conditional</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>coalesce(*cols)</code></td><td>Return the first non-null value.</td></tr>
    <tr><td><code>ifnull(value, replacement)</code></td><td>Return <code>replacement</code> if <code>value</code> is null.</td></tr>
    <tr><td><code>nullif(left, right)</code></td><td>Return null if <code>left == right</code>, else <code>left</code>.</td></tr>
    <tr><td><code>isnull(col)</code></td><td>True if the column is null.</td></tr>
    <tr><td><code>isnotnull(col)</code></td><td>True if the column is not null.</td></tr>
    <tr><td><code>isnan(col)</code></td><td>True if the value is NaN.</td></tr>
  </tbody>
</table>

<h2 id="string-functions">String Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>upper(col)</code></td><td>Convert to uppercase.</td></tr>
    <tr><td><code>lower(col)</code></td><td>Convert to lowercase.</td></tr>
    <tr><td><code>length(col)</code></td><td>String length (character count).</td></tr>
    <tr><td><code>trim(col)</code></td><td>Strip leading and trailing whitespace.</td></tr>
    <tr><td><code>ltrim(col)</code></td><td>Strip leading whitespace.</td></tr>
    <tr><td><code>rtrim(col)</code></td><td>Strip trailing whitespace.</td></tr>
    <tr><td><code>concat(*cols)</code></td><td>Concatenate strings.</td></tr>
  </tbody>
</table>

<h2 id="math-functions">Math Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>abs(col)</code></td><td>Absolute value.</td></tr>
    <tr><td><code>round(col, scale=0)</code></td><td>Round to <code>scale</code> decimal places.</td></tr>
    <tr><td><code>floor(col)</code></td><td>Floor (round down to integer).</td></tr>
    <tr><td><code>ceil(col)</code></td><td>Ceiling (round up to integer).</td></tr>
    <tr><td><code>sqrt(col)</code></td><td>Square root.</td></tr>
    <tr><td><code>exp(col)</code></td><td>e raised to the power of <code>col</code>.</td></tr>
    <tr><td><code>log(col)</code></td><td>Natural logarithm.</td></tr>
    <tr><td><code>sin(col)</code></td><td>Sine (radians).</td></tr>
    <tr><td><code>cos(col)</code></td><td>Cosine (radians).</td></tr>
    <tr><td><code>tan(col)</code></td><td>Tangent (radians).</td></tr>
  </tbody>
</table>

<h2 id="date-functions">Date/Time Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>current_date()</code></td><td>Current date (evaluates at query planning time).</td></tr>
    <tr><td><code>current_timestamp()</code></td><td>Current timestamp.</td></tr>
    <tr><td><code>date_trunc(unit, timestamp)</code></td><td>Truncate to a time unit ('year', 'month', 'day', 'hour', 'minute', 'second').</td></tr>
  </tbody>
</table>

<h2 id="sort-cast">Sort and Cast</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>asc(col)</code></td><td>Wrap column in ascending sort order.</td></tr>
    <tr><td><code>desc(col)</code></td><td>Wrap column in descending sort order.</td></tr>
    <tr><td><code>cast(col, data_type)</code></td><td>Cast column to <code>data_type</code> string (e.g. <code>'int64'</code>, <code>'utf8'</code>).</td></tr>
    <tr><td><code>try_cast(col, data_type)</code></td><td>Cast returning null on failure instead of erroring.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'python/sinks',
    group: 'Python API',
    title: 'Sinks',
    description: 'Sink classes for writing data to Parquet, Cassandra, Elasticsearch, HBase, and vector stores.',
    status: 'Preview',
    body: `
<h2 id="overview">Overview</h2>
<p>Sink classes accept Arrow <code>RecordBatch</code> data and write it to external systems. In most code paths you do not call a sink class directly — you call <code>df.write_parquet(path)</code> or <code>session.sql(...).write_*</code> and the framework picks the right sink. Construct a sink class directly only when you need the <code>write_batches</code> API for a batch list.</p>

<h2 id="parquet">ParquetSink</h2>
<div class="api-sig">ParquetSink(path: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>path() -> str</code></td><td>Return the target file path.</td></tr>
  </tbody>
</table>
<pre><code class="language-python">import krishiv as ks
sink = ks.ParquetSink("/tmp/output.parquet")
# Write is triggered by session.sql(...).write_parquet(sink)
</code></pre>

<h2 id="cassandra">CassandraSink</h2>
<div class="api-sig">CassandraSink(hosts: list[str], keyspace: str, table: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>write_batches(batches: list[RecordBatch])</code></td><td>Write a list of Arrow batches to Cassandra.</td></tr>
  </tbody>
</table>

<h2 id="elasticsearch">ElasticsearchSink</h2>
<div class="api-sig">ElasticsearchSink(url: str, index: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>write_batches(batches)</code></td><td>Bulk-index batches into Elasticsearch.</td></tr>
  </tbody>
</table>

<h2 id="hbase">HBaseSink</h2>
<div class="api-sig">HBaseSink(zookeeper_quorum: str, table: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>write_batches(batches)</code></td><td>Write batches to an HBase table.</td></tr>
  </tbody>
</table>

<h2 id="vector-sinks">Vector Sinks</h2>
<p>Vector sinks implement a shared interface: <code>sink_name()</code>, <code>upsert_batch(batch)</code>, <code>delete_by_ids(ids)</code>, <code>query_nearest(vector, k)</code>. They require the <code>vector-sinks</code> or platform-specific Cargo feature.</p>
<table class="api-table">
  <thead><tr><th>Sink</th><th>Constructor</th><th>Feature</th></tr></thead>
  <tbody>
    <tr><td><code>InMemoryVectorSink</code></td><td><code>InMemoryVectorSink(dim: int)</code></td><td>Always available</td></tr>
    <tr><td><code>LanceDbSink</code></td><td><code>LanceDbSink.open(path, table)</code></td><td><code>vector-sinks</code></td></tr>
    <tr><td><code>PineconeSink</code></td><td><code>PineconeSink(api_key, index_name)</code></td><td><code>vector-sinks</code></td></tr>
    <tr><td><code>QdrantSink</code></td><td><code>QdrantSink.connect(url, collection)</code></td><td><code>qdrant</code></td></tr>
    <tr><td><code>PgvectorSink</code></td><td><code>PgvectorSink.connect(conn_str, table)</code></td><td><code>pgvector</code></td></tr>
    <tr><td><code>WeaviateSink</code></td><td><code>WeaviateSink(url, class_name)</code></td><td><code>vector-sinks</code></td></tr>
  </tbody>
</table>
<div class="note-box">Vector sinks that depend on a disabled Cargo feature return a friendly <code>RuntimeError</code> naming the missing feature and the <code>maturin develop --features</code> command.</div>
`,
  },

  {
    slug: 'python/state',
    group: 'Python API',
    title: 'State',
    description: 'ValueState, MapState, ListState — per-key operator state for process functions.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>State objects are created inside process functions and scoped to the current key. State is backed by <code>krishiv-state</code> (in-memory or RocksDB depending on the durability profile). State is serialised to/from JSON values.</p>

<h2 id="valuestate">ValueState</h2>
<div class="api-sig">ValueState(name: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>key() -> str</code></td><td><code>str</code></td><td>Return the current partition key.</td></tr>
    <tr><td><code>set_json(value: Any)</code></td><td><code>None</code></td><td>Store a JSON-serialisable value for the current key.</td></tr>
    <tr><td><code>clear()</code></td><td><code>None</code></td><td>Delete the stored value for the current key.</td></tr>
  </tbody>
</table>

<h2 id="mapstate">MapState</h2>
<div class="api-sig">MapState(name: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>key() -> str</code></td><td><code>str</code></td><td>Return the current partition key.</td></tr>
    <tr><td><code>put_json(map_key: str, value: Any)</code></td><td><code>None</code></td><td>Store a value at <code>map_key</code> within the keyed map.</td></tr>
    <tr><td><code>clear()</code></td><td><code>None</code></td><td>Clear all entries for the current key.</td></tr>
  </tbody>
</table>

<h2 id="liststate">ListState</h2>
<div class="api-sig">ListState(name: str)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>key() -> str</code></td><td><code>str</code></td><td>Return the current partition key.</td></tr>
    <tr><td><code>add_json(value: Any)</code></td><td><code>None</code></td><td>Append a JSON-serialisable value to the list for the current key.</td></tr>
    <tr><td><code>clear()</code></td><td><code>None</code></td><td>Clear the list for the current key.</td></tr>
  </tbody>
</table>

<h2 id="example">Example — Stateful Process Function</h2>
<pre><code class="language-python">import krishiv as ks
from krishiv import apply_process_function, ProcessContext, ValueState

def count_events(ctx: ProcessContext, batch, state: ValueState):
    current = state.set_json.__doc__  # read pattern
    n = 0  # accumulate from batch
    for _ in range(batch.num_rows):
        n += 1
    state.set_json(n)
    ctx.emit(batch)

session = ks.Session.embedded()
stream, sender = session.memory_stream(schema)
keyed = stream.key_by("user_id")
result = apply_process_function(keyed, count_events, ValueState("event_count"))
</code></pre>
`,
  },

  {
    slug: 'python/query-result',
    group: 'Python API',
    title: 'QueryResult & QueryHandle',
    description: 'Collect results and manage async query lifecycle.',
    status: 'Available',
    body: `
<h2 id="queryresult">QueryResult</h2>
<p>Returned by synchronous <code>execute_local()</code> calls. Contains a list of collected Arrow batches.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>batches() -> list[RecordBatch]</code></td><td><code>list</code></td><td>Return the raw Arrow RecordBatch list.</td></tr>
    <tr><td><code>row_count() -> int</code></td><td><code>int</code></td><td>Total number of rows across all batches.</td></tr>
    <tr><td><code>pretty() -> str</code></td><td><code>str</code></td><td>Return a formatted ASCII table string.</td></tr>
    <tr><td><code>show()</code></td><td><code>None</code></td><td>Print the formatted table to stdout.</td></tr>
    <tr><td><code>to_arrow() -> Table</code></td><td><code>pyarrow.Table</code></td><td>Convert to a PyArrow Table.</td></tr>
    <tr><td><code>to_pandas() -> DataFrame</code></td><td><code>pandas.DataFrame</code></td><td>Convert to a pandas DataFrame (requires pandas).</td></tr>
    <tr><td><code>len() -> int</code></td><td><code>int</code></td><td>Same as <code>row_count()</code>; also works with <code>len(result)</code>.</td></tr>
  </tbody>
</table>

<h2 id="queryhandle">QueryHandle</h2>
<p>Returned by <code>execute_remote()</code> and <code>submit_async()</code>. Represents an in-flight asynchronous query.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>query_id() -> str</code></td><td><code>str</code></td><td>Unique query identifier.</td></tr>
    <tr><td><code>status() -> str</code></td><td><code>str</code></td><td>Current query status: "Running", "Completed", "Failed", "Cancelled".</td></tr>
    <tr><td><code>is_done() -> bool</code></td><td><code>bool</code></td><td>True if the query has completed (any terminal state).</td></tr>
    <tr><td><code>progress() -> float</code></td><td><code>float</code></td><td>Estimated progress in [0.0, 1.0].</td></tr>
    <tr><td><code>collect() -> QueryResult</code></td><td><code>QueryResult</code></td><td>Block until done and collect all results.</td></tr>
    <tr><td><code>cancel()</code></td><td><code>None</code></td><td>Request cancellation of the query.</td></tr>
  </tbody>
</table>

<h2 id="jobstatus">JobStatus</h2>
<p>Returned by <code>session.submit_stream_job()</code> and <code>session.jobs()</code>.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>id() -> str</code></td><td><code>str</code></td><td>Job ID.</td></tr>
    <tr><td><code>name() -> str</code></td><td><code>str</code></td><td>Job name.</td></tr>
    <tr><td><code>state() -> str</code></td><td><code>str</code></td><td>Job state: "Running", "Completed", "Failed", etc.</td></tr>
  </tbody>
</table>
`,
  },

  {
    slug: 'python/lakehouse',
    group: 'Python API',
    title: 'Lakehouse',
    description: 'LiveTable, MemoryLakehouseTable, IcebergRestCatalog, and HudiWriteResult.',
    status: 'Preview',
    body: `
<h2 id="livetable">LiveTable</h2>
<p>Obtained via <code>session.live_table(name)</code>. Provides row-level ingestion into a live SQL-queryable table.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>name() -> str</code></td><td><code>str</code></td><td>Return the table name.</td></tr>
    <tr><td><code>ingest_row(row: dict)</code></td><td><code>None</code></td><td>Append a single row (dict of column → value).</td></tr>
    <tr><td><code>refresh()</code></td><td><code>None</code></td><td>Flush pending inserts into the queryable snapshot.</td></tr>
    <tr><td><code>change_feed() -> ChangeFeedIter</code></td><td><code>ChangeFeedIter</code></td><td>Get an async iterator of change records.</td></tr>
    <tr><td><code>drop()</code></td><td><code>None</code></td><td>Drop and unregister the live table.</td></tr>
  </tbody>
</table>

<h2 id="memorylakehouse">MemoryLakehouseTable</h2>
<p>An in-memory Iceberg-like table that supports snapshot-based DML. Useful for testing lakehouse patterns without a real Iceberg catalog.</p>
<div class="api-sig">MemoryLakehouseTable(schema: Schema, name: str = "")</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>append(batches)</code></td><td><code>None</code></td><td>Append Arrow batches as a new snapshot.</td></tr>
    <tr><td><code>overwrite(batches)</code></td><td><code>None</code></td><td>Replace all data with new batches.</td></tr>
    <tr><td><code>delete_where(predicate: str)</code></td><td><code>int</code></td><td>Delete rows matching a SQL predicate. Returns deleted count.</td></tr>
    <tr><td><code>update_where(predicate, assignments)</code></td><td><code>int</code></td><td>Update matching rows. Returns updated count.</td></tr>
    <tr><td><code>merge(source_batches, condition, actions)</code></td><td><code>None</code></td><td>Apply MERGE logic (insert/update/delete) from a source.</td></tr>
    <tr><td><code>evolve_schema(new_schema)</code></td><td><code>None</code></td><td>Evolve the table schema (add nullable columns).</td></tr>
    <tr><td><code>current_snapshot_id() -> int</code></td><td><code>int</code></td><td>Return the current snapshot ID.</td></tr>
  </tbody>
</table>

<h2 id="icebergrestcatalog">IcebergRestCatalog</h2>
<div class="api-sig">IcebergRestCatalog(uri: str, warehouse: str = None, token: str = None)</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>list_tables(namespace: str) -> list[str]</code></td><td><code>list[str]</code></td><td>List all table names in a namespace.</td></tr>
    <tr><td><code>load_table_metadata(namespace, table) -> dict</code></td><td><code>dict</code></td><td>Load raw Iceberg table metadata JSON.</td></tr>
  </tbody>
</table>

<h2 id="top-level-functions">Top-Level Lakehouse Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>read_iceberg(uri, catalog_uri=None) -> DataFrame</code></td><td>Read an Iceberg table (requires <code>iceberg</code> feature).</td></tr>
    <tr><td><code>read_delta(path, version=None) -> DataFrame</code></td><td>Read a Delta Lake table directory (requires <code>delta</code> feature).</td></tr>
    <tr><td><code>read_hudi(path, query_type='snapshot') -> DataFrame</code></td><td>Read a Hudi table.</td></tr>
    <tr><td><code>write_hudi_append(df, path) -> HudiWriteResult</code></td><td>Append a DataFrame to a Hudi table.</td></tr>
    <tr><td><code>write_hudi_upsert(df, path, key_col) -> HudiWriteResult</code></td><td>Upsert a DataFrame into a Hudi table by key column.</td></tr>
  </tbody>
</table>

<h2 id="hudiwriteresult">HudiWriteResult</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>instant() -> str</code></td><td><code>str</code></td><td>Hudi commit instant timestamp.</td></tr>
    <tr><td><code>rows_inserted() -> int</code></td><td><code>int</code></td><td>Number of rows inserted.</td></tr>
    <tr><td><code>rows_updated() -> int</code></td><td><code>int</code></td><td>Number of rows updated.</td></tr>
    <tr><td><code>snapshot_rows() -> int</code></td><td><code>int</code></td><td>Total rows in the table after the write.</td></tr>
  </tbody>
</table>
`,
  },
];
