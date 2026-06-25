import type { DocPage } from '../docs-data';

export const rustPages: DocPage[] = [
  {
    slug: 'rust',
    group: 'Rust API',
    title: 'Rust API Overview',
    description: 'Public API surface of the krishiv-api crate.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>The <code>krishiv-api</code> crate is the user-facing Rust API. It re-exports every public type at the crate root. Add it to your <code>Cargo.toml</code>:</p>
<pre><code class="language-toml">[dependencies]
krishiv = { path = "../krishiv", features = ["embedded"] }
</code></pre>
<p>Or use the api crate directly:</p>
<pre><code class="language-toml">[dependencies]
krishiv-api = { path = "../crates/krishiv-api" }
</code></pre>

<h2 id="top-level-exports">Key Top-Level Exports</h2>
<table class="api-table">
  <thead><tr><th>Type / Function</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>Session</code>, <code>SessionBuilder</code></td><td>Entry point for all Krishiv workloads.</td></tr>
    <tr><td><code>DataFrame</code>, <code>GroupedDataFrame</code></td><td>Lazy query plan builder.</td></tr>
    <tr><td><code>Stream</code>, <code>KeyedStream</code></td><td>Streaming data pipeline builder.</td></tr>
    <tr><td><code>IncrementalFlow</code></td><td>Incremental view maintenance.</td></tr>
    <tr><td><code>WindowedStream</code>, <code>SessionWindowedStream</code>, <code>SlidingWindowedStream</code></td><td>Windowed streaming aggregation.</td></tr>
    <tr><td><code>DataStreamReader</code>, <code>DataStreamWriter</code>, <code>StreamingQuery</code></td><td>Structured streaming API (Spark-style).</td></tr>
    <tr><td><code>QueryHandle</code>, <code>QueryResult</code></td><td>Async query execution and result collection.</td></tr>
    <tr><td><code>PreparedStatement</code></td><td>Parameterised SQL statements.</td></tr>
    <tr><td><code>col</code>, <code>lit</code>, <code>expr</code>, <code>avg</code>, <code>count</code>, <code>sum</code>, <code>min</code>, <code>max</code></td><td>Expression builder functions.</td></tr>
    <tr><td><code>Pipeline</code>, <code>PipelineBuilder</code></td><td>Source-to-sink pipeline execution.</td></tr>
    <tr><td><code>ValueState</code>, <code>MapState</code>, <code>ListState</code></td><td>Keyed operator state.</td></tr>
    <tr><td><code>KrishivError</code>, <code>Result</code></td><td>Error type and alias.</td></tr>
    <tr><td><code>RecordBatch</code>, <code>Schema</code>, <code>Field</code>, <code>DataType</code></td><td>Re-exported Arrow types.</td></tr>
  </tbody>
</table>

<h2 id="quick-example">Quick Example</h2>
<pre><code class="language-rust">use krishiv_api::{Session, Result, col, lit};

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;

    // SQL path
    let result = session.sql("SELECT 1 + 1 AS two").await?.collect().await?;

    // DataFrame path
    let df = session.read_parquet("data/sales.parquet").await?
        .filter(col("amount").gt(lit(100)))?
        .select(&["customer_id", "amount"])?;
    df.show().await?;

    Ok(())
}
</code></pre>
`,
  },

  {
    slug: 'rust/session',
    group: 'Rust API',
    title: 'Session & SessionBuilder',
    description: 'Create and configure a Krishiv session for batch, streaming, or incremental work.',
    status: 'Available',
    body: `
<h2 id="sessionbuilder">SessionBuilder</h2>
<p>All sessions are constructed via <code>SessionBuilder</code>. The builder is obtained from <code>Session::builder()</code> and follows the builder pattern.</p>
<div class="api-sig">Session::builder() -> SessionBuilder</div>

<h2 id="builder-methods">Builder Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>embedded()</code></td><td>Shorthand for <code>SessionBuilder::default().build().await</code> in embedded mode.</td></tr>
    <tr><td><code>mode(ExecutionMode)</code></td><td>Set the execution mode: <code>Embedded</code>, <code>SingleNode</code>, <code>Distributed</code>.</td></tr>
    <tr><td><code>coordinator_url(impl Into&lt;String&gt;)</code></td><td>Flight/gRPC endpoint for distributed mode.</td></tr>
    <tr><td><code>with_auth(impl AuthProvider)</code></td><td>Attach a bearer-token or custom auth provider.</td></tr>
    <tr><td><code>with_policy(impl PolicyHook)</code></td><td>Attach a governance/access-control hook.</td></tr>
    <tr><td><code>target_parallelism(NonZeroUsize)</code></td><td>DataFusion target partition count for parallel execution.</td></tr>
    <tr><td><code>with_iceberg_catalog(Arc&lt;KrishivCatalog&gt;, name)</code></td><td>Register an Iceberg catalog under a given name (requires <code>iceberg-catalog</code> feature).</td></tr>
    <tr><td><code>with_shuffle_partitions(Option&lt;u32&gt;)</code></td><td>Override shuffle bucket count; <code>None</code> = auto.</td></tr>
    <tr><td><code>config(key, value)</code></td><td>Set a session config key.</td></tr>
    <tr><td><code>build() -> Result&lt;Session&gt;</code></td><td>Construct and return the session (async, requires <code>.await</code>).</td></tr>
  </tbody>
</table>

<h2 id="session-constructors">Session Constructors</h2>
<div class="api-sig">Session::embedded() -> impl Future&lt;Output = Result&lt;Session&gt;&gt;</div>
<div class="api-sig">Session::from_env() -> impl Future&lt;Output = Result&lt;Session&gt;&gt;</div>
<div class="api-sig">Session::connect(url: &amp;str) -> impl Future&lt;Output = Result&lt;Session&gt;&gt;</div>

<h2 id="session-sql">SQL Methods</h2>
<div class="note-box"><strong>Common methods:</strong> Most workloads use <code>Session::embedded()</code>, <code>sql(query)</code>, <code>read_parquet(path)</code>, <code>register_parquet(name, path)</code>, and <code>table(name)</code>. The full tables below are the complete reference.</div>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>sql(query: &amp;str)</code></td><td><code>Future&lt;Result&lt;DataFrame&gt;&gt;</code></td><td>Parse and plan a SQL query. Returns a lazy DataFrame.</td></tr>
    <tr><td><code>sql_with_timeout(query, ms)</code></td><td><code>Future&lt;Result&lt;DataFrame&gt;&gt;</code></td><td>Same as <code>sql</code> with an execution timeout in milliseconds.</td></tr>
    <tr><td><code>prepare(query: &amp;str)</code></td><td><code>Future&lt;Result&lt;PreparedStatement&gt;&gt;</code></td><td>Create a parameterised prepared statement.</td></tr>
    <tr><td><code>explain(query: &amp;str)</code></td><td><code>Future&lt;Result&lt;String&gt;&gt;</code></td><td>Return the DataFusion logical plan as a string.</td></tr>
  </tbody>
</table>

<h2 id="session-data">Data Registration</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>read_parquet(path)</code></td><td><code>Future&lt;Result&lt;DataFrame&gt;&gt;</code></td><td>Read a local Parquet file into a DataFrame.</td></tr>
    <tr><td><code>read_csv(path)</code></td><td><code>Future&lt;Result&lt;DataFrame&gt;&gt;</code></td><td>Read a local CSV file.</td></tr>
    <tr><td><code>read_json(path)</code></td><td><code>Future&lt;Result&lt;DataFrame&gt;&gt;</code></td><td>Read a local NDJSON file.</td></tr>
    <tr><td><code>register_parquet(name, path)</code></td><td><code>Future&lt;Result&lt;()&gt;&gt;</code></td><td>Register a Parquet file as a named SQL table.</td></tr>
    <tr><td><code>register_record_batches(name, batches)</code></td><td><code>Future&lt;Result&lt;()&gt;&gt;</code></td><td>Register in-memory Arrow batches as a SQL table.</td></tr>
    <tr><td><code>register_udf(udf: ScalarUdf)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register a scalar UDF.</td></tr>
    <tr><td><code>register_aggregate_udf(udf)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register an aggregate UDF.</td></tr>
    <tr><td><code>register_table_udf_fn(name, schema, f)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register a closure-based table-valued function.</td></tr>
    <tr><td><code>register_kafka_source(name, schema, brokers, topic, group)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register a Kafka topic as an unbounded streaming table.</td></tr>
    <tr><td><code>deregister_table(name)</code></td><td><code>Result&lt;()&gt;</code></td><td>Remove a registered table.</td></tr>
    <tr><td><code>table_exists(name)</code></td><td><code>bool</code></td><td>Check if a table is registered.</td></tr>
    <tr><td><code>list_tables()</code></td><td><code>Vec&lt;String&gt;</code></td><td>List registered table names.</td></tr>
  </tbody>
</table>

<h2 id="session-streaming">Streaming Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>memory_stream(schema)</code></td><td><code>Result&lt;(Stream, Sender)&gt;</code></td><td>Create an in-memory stream and its push handle.</td></tr>
    <tr><td><code>from_bounded_stream(schema, batches)</code></td><td><code>Result&lt;Stream&gt;</code></td><td>Create a bounded stream from a static list of batches.</td></tr>
    <tr><td><code>submit_stream_job(plan, name)</code></td><td><code>Future&lt;Result&lt;JobStatus&gt;&gt;</code></td><td>Submit a streaming job to the scheduler.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-rust">use krishiv_api::{Session, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;

    // Register in-memory data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id",  DataType::Int64,   false),
        Field::new("val", DataType::Float64, true),
    ]));
    // ... build RecordBatch ...

    // SQL query
    let df = session.sql("SELECT id, val FROM my_table WHERE val > 10").await?;
    df.show().await?;
    Ok(())
}
</code></pre>
`,
  },

  {
    slug: 'rust/dataframe',
    group: 'Rust API',
    title: 'DataFrame',
    description: 'Lazy query plan builder for batch and bounded streaming workloads.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p><code>DataFrame</code> is a lazy plan builder. Operations compose a logical plan; execution only happens on <code>collect()</code>, <code>show()</code>, or <code>execute_stream()</code>.</p>

<div class="note-box"><strong>Common methods:</strong> <code>filter</code>, <code>select</code>, <code>group_by</code>, <code>agg</code>, <code>sort</code>, <code>limit</code>, <code>join</code>, and <code>with_column</code> cover ~90% of batch transformations. The rest are for specialized shapes.</div>

<h2 id="project">Project / Schema</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>select(&amp;[Expr])</code></td><td>Project by expressions: column refs, literals, function calls.</td></tr>
    <tr><td><code>select_columns(&amp;[&amp;str])</code></td><td>Project by column name strings.</td></tr>
    <tr><td><code>select_exprs(&amp;[&amp;str])</code></td><td>Project by SQL expression strings.</td></tr>
    <tr><td><code>with_column(name, expr)</code></td><td>Add or replace a column. Use for derived columns that reference the input.</td></tr>
    <tr><td><code>drop_columns(&amp;[&amp;str])</code></td><td>Remove named columns.</td></tr>
    <tr><td><code>rename(old, new)</code></td><td>Rename a single column.</td></tr>
    <tr><td><code>alias(name)</code></td><td>Alias the DataFrame as a subquery name. Affects the generated SQL only.</td></tr>
  </tbody>
</table>
<pre><code class="language-rust">df.select(&amp;[col("customer_id"), sum(col("amount")).alias("total")])?
df.with_column("is_high_value", col("total").gt(lit(1000)))?
df.rename("cust", "customer_id")?
</code></pre>

<h2 id="filter">Filter / Shape</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>filter(Expr)</code></td><td>Apply a boolean filter predicate.</td></tr>
    <tr><td><code>where(Expr)</code></td><td>Alias for <code>filter</code>.</td></tr>
    <tr><td><code>limit(n: usize)</code></td><td>Retain at most <code>n</code> rows.</td></tr>
    <tr><td><code>distinct()</code></td><td>Remove duplicate rows.</td></tr>
    <tr><td><code>drop_nulls(&amp;[&amp;str])</code></td><td>Drop rows with NULL in any of the named columns.</td></tr>
    <tr><td><code>fill_null(column, value)</code></td><td>Fill NULLs in a column with a constant or a per-column map.</td></tr>
    <tr><td><code>sample(fraction: f64)</code></td><td>Bernoulli sample: keep each row with probability <code>fraction</code>.</td></tr>
    <tr><td><code>repartition(n: usize, key_columns)</code></td><td>Insert a hash exchange on <code>key_columns</code> and produce <code>n</code> partitions. Without <code>key_columns</code>, this is a round-robin repartition.</td></tr>
  </tbody>
</table>

<h2 id="group">Group / Aggregate</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>group_by(&amp;[Expr])</code></td><td>Group rows for aggregation. Returns a <code>GroupedDataFrame</code>.</td></tr>
    <tr><td><code>GroupedDataFrame::agg(&amp;[Expr])</code></td><td>Apply aggregates to each group.</td></tr>
    <tr><td><code>GroupedDataFrame::agg_grouping_sets(spec, &amp;[Expr])</code></td><td>Aggregates with <code>GROUPING SETS</code> (or <code>CUBE</code> / <code>ROLLUP</code> via the <code>GroupingSpec</code> enum).</td></tr>
    <tr><td><code>GroupedDataFrame::count()</code></td><td>Shorthand for <code>agg([count_all()])</code>.</td></tr>
  </tbody>
</table>
<pre><code class="language-rust">df.group_by(&amp;[col("region")])?
  .agg(&amp;[sum(col("amount")).alias("total"), count_all().alias("n")])?
</code></pre>

<h2 id="join">Join</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>join(right, JoinType, left_cols, right_cols)</code></td><td>Equi-join on matching column names.</td></tr>
    <tr><td><code>join_on(right, JoinType, expr)</code></td><td>Join with an arbitrary ON expression (non-equi).</td></tr>
  </tbody>
</table>
<p><code>JoinType</code> is an enum: <code>Inner</code>, <code>Left</code>, <code>Right</code>, <code>Full</code>, <code>LeftSemi</code>, <code>RightSemi</code>, <code>LeftAnti</code>, <code>RightAnti</code>.</p>
<p>For temporal / interval joins on streaming input, see <a href="/docs/latest/streaming/joins">Streaming Joins</a>.</p>

<h2 id="set">Set operations</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>SQL</th></tr></thead>
  <tbody>
    <tr><td><code>union(other)</code></td><td><code>UNION ALL</code></td></tr>
    <tr><td><code>union_distinct(other)</code></td><td><code>UNION DISTINCT</code></td></tr>
    <tr><td><code>intersect(other)</code></td><td><code>INTERSECT DISTINCT</code></td></tr>
    <tr><td><code>except(other)</code></td><td><code>EXCEPT DISTINCT</code></td></tr>
  </tbody>
</table>
<p>All four require matching schemas. Use <code>select</code> first to align columns.</p>

<h2 id="stream">Stream</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>stream()</code></td><td><code>StreamingDataFrame</code></td><td>Convert a bounded plan into a streaming pipeline.</td></tr>
    <tr><td><code>to_streaming()</code></td><td><code>StreamingDataFrame</code></td><td>Alias.</td></tr>
  </tbody>
</table>
<p>Once on a <code>StreamingDataFrame</code>, see <a href="/docs/latest/rust/stream">Stream</a> for windowed, keyed, and side-output operators.</p>

<h2 id="cache">Cache</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>cache()</code></td><td>Materialise the result in memory and reuse it. Future <code>collect</code>/<code>show</code> skip recomputation.</td></tr>
    <tr><td><code>persist()</code></td><td>Alias for <code>cache</code>.</td></tr>
    <tr><td><code>unpersist()</code></td><td>Drop the cached materialisation.</td></tr>
    <tr><td><code>create_or_replace_temp_view(name)</code></td><td>Register the DataFrame as a SQL temp view for the rest of the session.</td></tr>
  </tbody>
</table>
<div class="note-box"><strong>Note:</strong> <code>cache</code> holds a copy in RAM. For large results, write to Parquet/CSV/JSON and re-read instead.</div>

<h2 id="write">Write</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>write() -&gt; DataFrameWriter</code></td><td>Return a writer with format-specific options.</td></tr>
    <tr><td><code>write_parquet(path)</code></td><td>Write to a local Parquet file or object-store URI.</td></tr>
    <tr><td><code>write_parquet_with_options(path, opts)</code></td><td>Same with compression, row group size, etc.</td></tr>
    <tr><td><code>write_parquet_overwrite_partition(path, partition_by)</code></td><td><code>INSERT OVERWRITE TABLE … PARTITION (…)</code> semantics. <em>Returns <code>KrishivError::Unsupported</code> in embedded mode.</em></td></tr>
    <tr><td><code>write_csv(path)</code></td><td>Write to a CSV file.</td></tr>
    <tr><td><code>write_csv_with_options(path, opts)</code></td><td>Same with delimiter / <code>has_header</code> options.</td></tr>
    <tr><td><code>write_json(path)</code></td><td>Write as NDJSON.</td></tr>
    <tr><td><code>write_stream() -&gt; DataStreamWriter</code></td><td>Return a streaming writer for this DataFrame. See <a href="/docs/latest/streaming/queries-and-lifecycle">Queries and Lifecycle</a>.</td></tr>
  </tbody>
</table>

<h2 id="execute">Execute</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>collect()</code></td><td><code>Future&lt;Result&lt;QueryResult&gt;&gt;</code></td><td>Execute and collect. Calling on a streaming plan returns <code>KrishivError::Unsupported</code>.</td></tr>
    <tr><td><code>collect_async()</code></td><td>Async version.</td></tr>
    <tr><td><code>collect_partitioned()</code></td><td>Collect preserving partition boundaries.</td></tr>
    <tr><td><code>collect_with_stats()</code></td><td>Returns <code>(QueryResult, QueryExecutionStats)</code> with output_rows and cpu_nanos.</td></tr>
    <tr><td><code>execute() -&gt; ExecutionResult</code></td><td>Unified batch-or-stream entry: returns <code>Batch(Vec&lt;RecordBatch&gt;)</code> for bounded plans, <code>Stream(KrishivStream)</code> for unbounded.</td></tr>
    <tr><td><code>execute_stream_async()</code></td><td><code>KrishivStream</code></td><td>Pin&lt;Box&lt;dyn Stream&gt;&gt; for streaming plans.</td></tr>
    <tr><td><code>submit_async()</code></td><td><code>QueryHandle</code></td><td>Submit the plan asynchronously and return a handle for status / cancellation.</td></tr>
    <tr><td><code>show(n)</code></td><td>Execute and print the first <code>n</code> rows (default 20).</td></tr>
    <tr><td><code>describe()</code></td><td>Return a DataFrame with summary statistics: count, null_count, mean, std, min, max, median.</td></tr>
    <tr><td><code>num_rows()</code></td><td><code>Result&lt;usize&gt;</code></td><td>Execute and return the row count.</td></tr>
  </tbody>
</table>

<h2 id="explain">Explain</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>explain() -&gt; String</code></td><td>Default plan text (logical + physical).</td></tr>
    <tr><td><code>explain_logical() -&gt; String</code></td><td>Logical plan only.</td></tr>
    <tr><td><code>explain(verbose: bool)</code></td><td>Detailed plan with operator stats.</td></tr>
    <tr><td><code>explain_mode(ExplainMode) -&gt; String</code></td><td>Specify the explain mode. <code>ExplainMode ∈ {Logical, Physical, Analyze}</code>.</td></tr>
  </tbody>
</table>
<p><code>Explain</code> is free — it does not run the plan, just inspects the logical and physical plans. Use it to verify pushdown, partition pruning, and join order.</p>

<h2 id="schema-meta">Schema and metadata</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>schema() -&gt; &amp;Schema</code></td><td>Logical output schema.</td></tr>
    <tr><td><code>columns() -&gt; Vec&lt;&amp;str&gt;</code></td><td>Column names.</td></tr>
    <tr><td><code>logical_plan() -&gt; &amp;LogicalPlan</code></td><td>The unoptimised logical plan tree.</td></tr>
    <tr><td><code>boundedness() -&gt; Boundedness</code></td><td><code>Bounded</code> or <code>Unbounded</code>.</td></tr>
    <tr><td><code>is_bounded() -&gt; bool</code></td><td>Shorthand.</td></tr>
  </tbody>
</table>

<h2 id="see-also">See also</h2>
<ul>
  <li><a href="/docs/latest/python/dataframe">Python DataFrame</a> — same surface in Python</li>
  <li><a href="/docs/latest/rust/stream">Stream</a> — for unbounded plans</li>
  <li><a href="/docs/latest/streaming/queries-and-lifecycle">Queries and Lifecycle</a> — write_stream, output modes, triggers</li>
</ul>
`,
  },

  {
    slug: 'rust/stream',
    group: 'Rust API',
    title: 'Stream & KeyedStream',
    description: 'Streaming pipeline builder for unbounded, event-time workloads.',
    status: 'Available',
    body: `
<h2 id="stream">Stream</h2>
<div class="note-box"><strong>Common pattern:</strong> Get a stream from <code>session.memory_stream(schema)</code> or a registered Kafka source, call <code>watermark</code>, then <code>key_by</code>, then <code>tumbling_window</code> or <code>sliding_window_ms</code>, then <code>agg</code>. See the <a href="/docs/latest/recipes/tumbling-window">Tumbling window recipe</a> for a full example.</div>
<p><code>Stream</code> wraps an unbounded data source and provides chaining methods to build streaming pipelines.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>key_by(key_col)</code></td><td><code>Result&lt;KeyedStream&gt;</code></td><td>Partition the stream by a key column for stateful ops.</td></tr>
    <tr><td><code>broadcast()</code></td><td><code>BroadcastStream</code></td><td>Broadcast stream to all operator instances.</td></tr>
    <tr><td><code>connect(other: Stream)</code></td><td><code>ConnectedStreams</code></td><td>Pair two streams for a co-process function.</td></tr>
    <tr><td><code>watermark(col, lag_ms)</code></td><td><code>Result&lt;Stream&gt;</code></td><td>Assign a watermark using an event-time column and lag.</td></tr>
    <tr><td><code>with_watermark(spec: WatermarkSpec)</code></td><td><code>Result&lt;Stream&gt;</code></td><td>Assign a watermark using a typed spec.</td></tr>
    <tr><td><code>with_multi_source_watermark(spec)</code></td><td><code>Result&lt;Stream&gt;</code></td><td>Multi-source watermark alignment.</td></tr>
    <tr><td><code>with_state_ttl(config: StateTtlConfig)</code></td><td><code>Result&lt;Stream&gt;</code></td><td>Attach a TTL policy to downstream keyed state.</td></tr>
    <tr><td><code>tumbling_window(size_ms)</code></td><td><code>WindowedStream</code></td><td>Apply a tumbling window of the given duration.</td></tr>
    <tr><td><code>sliding_window_ms(size_ms, slide_ms)</code></td><td><code>WindowedStream</code></td><td>Apply a sliding (hop) window.</td></tr>
    <tr><td><code>session_window_ms(gap_ms)</code></td><td><code>WindowedStream</code></td><td>Apply a session window with inactivity gap.</td></tr>
  </tbody>
</table>

<h2 id="keyedstream">KeyedStream</h2>
<p><code>KeyedStream</code> is a stream partitioned by a key. Stateful operators (process functions, window aggregations) operate per-key.</p>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>tumbling_window(size_ms)</code></td><td><code>WindowedStream</code></td><td>Fixed-size non-overlapping window per key.</td></tr>
    <tr><td><code>sliding_window_ms(size_ms, slide_ms)</code></td><td><code>WindowedStream</code></td><td>Sliding/hop window per key.</td></tr>
    <tr><td><code>session_window_ms(gap_ms)</code></td><td><code>SessionWindowedStream</code></td><td>Session window per key.</td></tr>
    <tr><td><code>window(spec: LocalWindowKind)</code></td><td><code>WindowedStream</code></td><td>Apply a window using a typed spec.</td></tr>
    <tr><td><code>connect(other: KeyedStream)</code></td><td><code>ConnectedStreams</code></td><td>Join two keyed streams for co-process.</td></tr>
    <tr><td><code>with_multi_source_watermark(spec)</code></td><td><code>Result&lt;KeyedStream&gt;</code></td><td>Multi-source watermark for aligned processing.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-rust">use krishiv_api::{Session, Result};

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let (stream, sender) = session.memory_stream(schema)?;

    // Build a keyed tumbling window
    let windowed = stream
        .watermark("event_time", 5000)?
        .key_by("user_id")?
        .tumbling_window(60_000); // 1-minute window

    // Aggregate and collect
    let agg = windowed.agg(vec![count(col("*")), sum(col("amount"))]);
    // ... submit or collect
    Ok(())
}
</code></pre>
`,
  },

  {
    slug: 'rust/incremental-flow',
    group: 'Rust API',
    title: 'IncrementalFlow',
    description: 'Incremental view maintenance with DeltaBatch and tick-based processing.',
    status: 'Experimental',
    body: `
<h2 id="overview">Overview</h2>
<p><code>IncrementalFlow</code> maintains query results incrementally by processing <code>DeltaBatch</code> changes (weighted Arrow rows with an <code>Int64 _weight</code> column: <code>+1</code> = insert, <code>-1</code> = retract).</p>

<h2 id="construction">Construction</h2>
<div class="api-sig">IncrementalFlow::new(session: &amp;Session) -> Result&lt;IncrementalFlow&gt;</div>

<h2 id="methods">Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Returns</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>register_source(name, schema)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register a named source that accepts DeltaBatch input.</td></tr>
    <tr><td><code>register_view(name, query)</code></td><td><code>Result&lt;()&gt;</code></td><td>Register a SQL query as an incrementally maintained view.</td></tr>
    <tr><td><code>tick(source_name, delta_batch)</code></td><td><code>Future&lt;Result&lt;StepSummary&gt;&gt;</code></td><td>Deliver a delta batch and advance the view.</td></tr>
    <tr><td><code>snapshot(view_name)</code></td><td><code>Future&lt;Result&lt;Vec&lt;RecordBatch&gt;&gt;&gt;</code></td><td>Read the current materialised snapshot of a view.</td></tr>
    <tr><td><code>watch_output(view_name)</code></td><td><code>Result&lt;Receiver&lt;DeltaBatch&gt;&gt;</code></td><td>Subscribe to incremental output deltas from a view.</td></tr>
    <tr><td><code>checkpoint(path)</code></td><td><code>Future&lt;Result&lt;()&gt;&gt;</code></td><td>Persist the current flow state to a checkpoint path.</td></tr>
    <tr><td><code>restore(path)</code></td><td><code>Future&lt;Result&lt;()&gt;&gt;</code></td><td>Restore flow state from a checkpoint.</td></tr>
  </tbody>
</table>

<h2 id="step-summary">StepSummary</h2>
<table class="api-table">
  <thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>rows_in</code></td><td><code>usize</code></td><td>Rows received in this tick.</td></tr>
    <tr><td><code>rows_out</code></td><td><code>usize</code></td><td>Delta rows emitted to output views.</td></tr>
    <tr><td><code>duration_ms</code></td><td><code>u64</code></td><td>Wall-clock time to process this tick.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-rust">use krishiv_api::{Session, IncrementalFlow, Result};

#[tokio::main]
async fn main() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;
    let mut flow = IncrementalFlow::new(&amp;session)?;

    flow.register_source("orders", orders_schema)?;
    flow.register_view("totals",
        "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")?;

    // Deliver a delta (weighted batch)
    let summary = flow.tick("orders", delta_batch).await?;
    println!("Processed {} rows, emitted {} delta rows", summary.rows_in, summary.rows_out);

    // Read current view
    let snapshot = flow.snapshot("totals").await?;
    Ok(())
}
</code></pre>
`,
  },

  {
    slug: 'rust/expressions',
    group: 'Rust API',
    title: 'Expressions',
    description: 'Builder functions for constructing typed Expr values.',
    status: 'Available',
    body: `
<h2 id="overview">Overview</h2>
<p>The <code>Expr</code> type is the versioned expression AST used throughout <code>DataFrame</code>, <code>GroupedDataFrame</code>, and streaming operators. Build expressions using the provided constructor functions rather than constructing <code>Expr</code> directly.</p>

<h2 id="column-literal">Column and Literal</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Signature</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>col</code></td><td><code>col(name: &amp;str) -> Expr</code></td><td>Reference a column by name.</td></tr>
    <tr><td><code>lit</code></td><td><code>lit(value: impl Into&lt;Literal&gt;) -> Expr</code></td><td>Create a literal constant expression.</td></tr>
    <tr><td><code>expr</code></td><td><code>expr(sql: &amp;str) -> Result&lt;Expr&gt;</code></td><td>Parse a SQL expression string into an <code>Expr</code>.</td></tr>
  </tbody>
</table>

<h2 id="aggregates">Aggregate Functions</h2>
<table class="api-table">
  <thead><tr><th>Function</th><th>Signature</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>count</code></td><td><code>count(expr: Expr) -> Expr</code></td><td>Count non-null values.</td></tr>
    <tr><td><code>count_all</code></td><td><code>count_all() -> Expr</code></td><td>Count all rows (<code>COUNT(*)</code>).</td></tr>
    <tr><td><code>sum</code></td><td><code>sum(expr: Expr) -> Expr</code></td><td>Sum of numeric values.</td></tr>
    <tr><td><code>avg</code></td><td><code>avg(expr: Expr) -> Expr</code></td><td>Arithmetic mean.</td></tr>
    <tr><td><code>min</code></td><td><code>min(expr: Expr) -> Expr</code></td><td>Minimum value.</td></tr>
    <tr><td><code>max</code></td><td><code>max(expr: Expr) -> Expr</code></td><td>Maximum value.</td></tr>
    <tr><td><code>function</code></td><td><code>function(name: &amp;str, args: Vec&lt;Expr&gt;) -> Expr</code></td><td>Call a named SQL function or registered UDF.</td></tr>
  </tbody>
</table>

<h2 id="expr-methods">Expr Methods</h2>
<table class="api-table">
  <thead><tr><th>Method</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>.alias(name)</code></td><td>Give an output alias to the expression.</td></tr>
    <tr><td><code>.gt(rhs)</code></td><td>Greater-than comparison.</td></tr>
    <tr><td><code>.lt(rhs)</code></td><td>Less-than comparison.</td></tr>
    <tr><td><code>.eq(rhs)</code></td><td>Equality comparison.</td></tr>
    <tr><td><code>.and(rhs)</code></td><td>Boolean AND.</td></tr>
    <tr><td><code>.or(rhs)</code></td><td>Boolean OR.</td></tr>
    <tr><td><code>.not()</code></td><td>Boolean NOT.</td></tr>
    <tr><td><code>.cast(data_type)</code></td><td>Explicit type cast.</td></tr>
    <tr><td><code>.is_null()</code></td><td>IS NULL predicate.</td></tr>
    <tr><td><code>.is_not_null()</code></td><td>IS NOT NULL predicate.</td></tr>
    <tr><td><code>.asc()</code></td><td>Wrap in ascending sort order.</td></tr>
    <tr><td><code>.desc()</code></td><td>Wrap in descending sort order.</td></tr>
  </tbody>
</table>

<h2 id="example">Example</h2>
<pre><code class="language-rust">use krishiv_api::{col, lit, sum, count_all};

let df = session.sql("SELECT * FROM sales").await?;
let result = df
    .filter(col("amount").gt(lit(100i64)))?
    .group_by(vec![col("region")])?
    .agg(vec![
        sum(col("amount")).alias("total"),
        count_all().alias("count"),
    ])?
    .sort(vec![col("total").desc()])?
    .collect().await?;
</code></pre>
`,
  },

  {
    slug: 'rust/errors',
    group: 'Rust API',
    title: 'Error Types',
    description: 'KrishivError, Result, and error handling patterns.',
    status: 'Available',
    body: `
<h2 id="krishiverror">KrishivError</h2>
<p><code>KrishivError</code> is the top-level error type returned by all public API methods. It is a non-exhaustive enum that aggregates errors from all Krishiv subsystems.</p>

<div class="api-sig">pub type Result&lt;T&gt; = std::result::Result&lt;T, KrishivError&gt;;</div>

<h2 id="variants">Key Variants</h2>
<table class="api-table">
  <thead><tr><th>Variant</th><th>Description</th></tr></thead>
  <tbody>
    <tr><td><code>Sql(SqlError)</code></td><td>SQL planning or execution error from <code>krishiv-sql</code>.</td></tr>
    <tr><td><code>Runtime(String)</code></td><td>Execution runtime error (coordinator, executor, or scheduler).</td></tr>
    <tr><td><code>Io(std::io::Error)</code></td><td>Filesystem or network I/O error.</td></tr>
    <tr><td><code>Arrow(ArrowError)</code></td><td>Apache Arrow schema or data error.</td></tr>
    <tr><td><code>Config(String)</code></td><td>Session configuration error (missing endpoint, invalid option).</td></tr>
    <tr><td><code>Udf(UdfError)</code></td><td>UDF registration or execution error.</td></tr>
    <tr><td><code>Cancelled</code></td><td>Operation cancelled by the caller.</td></tr>
    <tr><td><code>Timeout</code></td><td>Operation exceeded its configured timeout.</td></tr>
    <tr><td><code>AccessDenied(String)</code></td><td>Request blocked by auth or policy hook.</td></tr>
  </tbody>
</table>

<h2 id="patterns">Error Handling Patterns</h2>
<pre><code class="language-rust">use krishiv_api::{Session, KrishivError, Result};

async fn run() -> Result&lt;()&gt; {
    let session = Session::embedded().await?;

    match session.sql("SELECT bad syntax!!!").await {
        Ok(df) => { df.show().await?; }
        Err(KrishivError::Sql(e)) => {
            eprintln!("SQL error: {e}");
        }
        Err(e) => return Err(e),
    }
    Ok(())
}
</code></pre>

<h2 id="sql-error">SqlError</h2>
<p>See the <a href="/docs/latest/sql/error-codes">SQL Error Codes</a> page for the full list of <code>SqlError</code> variants and their SQLSTATE codes.</p>
`,
  },
];
