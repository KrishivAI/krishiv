// Deliberate sync-over-async boundary module (Phase 51 async contract):
// block_on here bridges a synchronous public surface to the async core.
#![allow(clippy::disallowed_methods)]

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_plan::{ExecutionKind, LogicalPlan, PhysicalPlan};
use krishiv_runtime::{ExecutionRuntime, JobId, JobState, JobStatus, LocalJobRegistry};
use krishiv_sql::KrishivDataFrameOps;

use crate::error::{KrishivError, Result};
use crate::expression::Expr;
use crate::io::DataFrameWriter;
use crate::session::RegisteredParquet;
use crate::types::{ExecutionMode, QueryResult};

/// Unified execution result for [`DataFrame::execute`].
///
/// A batch query produces a finite `Batch` result. A streaming query
/// (referencing an unbounded source) produces a `Stream` that must be
/// consumed incrementally. This type lets callers write a single code
/// path without knowing ahead of time which kind of query they have.
pub enum ExecutionResult {
    /// Query produced a finite set of record batches.
    Batch(Vec<RecordBatch>),
    /// Query produces an unbounded stream of record batches.
    Stream(crate::streaming_dataframe::KrishivStream),
}

impl ExecutionResult {
    /// Collect all batches from a `Batch` result, or collect the full stream.
    ///
    /// **Warning**: calling this on a `Stream` result backed by an unbounded
    /// source will block the executor thread indefinitely. Use this only when
    /// you know the stream is finite (e.g. a bounded window output) or as a
    /// convenience in tests.
    pub async fn into_batches(self) -> Result<Vec<RecordBatch>> {
        match self {
            ExecutionResult::Batch(batches) => Ok(batches),
            ExecutionResult::Stream(mut stream) => {
                use futures::StreamExt as _;
                let mut out = Vec::new();
                while let Some(batch) = stream.next().await {
                    out.push(batch.map_err(|e| KrishivError::Runtime {
                        message: e.to_string(),
                    })?);
                }
                Ok(out)
            }
        }
    }

    /// Returns `true` if this is a streaming result.
    pub fn is_streaming(&self) -> bool {
        matches!(self, ExecutionResult::Stream(_))
    }

    /// Returns `true` if this is a batch result.
    pub fn is_batch(&self) -> bool {
        matches!(self, ExecutionResult::Batch(_))
    }
}

/// Explain output requested by [`DataFrame::explain_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainMode {
    Logical,
    Physical,
    Analyze,
}

/// Lightweight query metrics returned by [`DataFrame::collect_with_stats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryExecutionStats {
    pub output_rows: u64,
    pub cpu_nanos: u64,
}

/// Whether a canonical DataFrame has finite or unbounded input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Boundedness {
    Bounded,
    Unbounded,
}

impl Boundedness {
    pub fn is_bounded(self) -> bool {
        matches!(self, Self::Bounded)
    }
}

/// Canonical equi-join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

impl JoinType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inner => "inner",
            Self::Left => "left",
            Self::Right => "right",
            Self::Full => "full",
            Self::LeftSemi => "left_semi",
            Self::RightSemi => "right_semi",
            Self::LeftAnti => "left_anti",
            Self::RightAnti => "right_anti",
        }
    }
}

/// Grouping-set expansion requested for an aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingSpec {
    Sets(Vec<Vec<Expr>>),
    Cube(Vec<Expr>),
    Rollup(Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PivotValue {
    pub value: crate::ScalarValue,
    pub alias: String,
}

impl PivotValue {
    pub fn new(value: crate::ScalarValue, alias: impl Into<String>) -> Self {
        Self {
            value,
            alias: alias.into(),
        }
    }
}

/// Builder returned by [`DataFrame::write_stream`] — the DataFrame-side entry to
/// the unified live-table write (Phase 61 keystone). Set the refresh mode, then
/// materialize into a named table via [`to_table`](Self::to_table).
pub struct WriteStreamBuilder<'a> {
    df: &'a DataFrame,
    refresh: crate::Refresh,
}

impl<'a> WriteStreamBuilder<'a> {
    /// Select the compute engine by refresh mode (Batch / Incremental /
    /// Continuous). Defaults to [`Refresh::Batch`](crate::Refresh::Batch).
    #[must_use]
    pub fn refresh(mut self, refresh: crate::Refresh) -> Self {
        self.refresh = refresh;
        self
    }

    /// Materialize into table `name` on `session`, choosing the engine from the
    /// refresh mode. `Batch` materializes this DataFrame's result directly (so
    /// transformations not expressible as a single query still work);
    /// `Incremental` registers an IVM materialized view from the DataFrame's
    /// defining query; `Continuous` targets the coordinator-gated streaming
    /// engine.
    ///
    /// This is the **one** materialize-by-refresh-mode entry point. It used to
    /// delegate to `Session::create_live_table`, which was the same router
    /// under a name that described no distinct object: a "live table" was
    /// either this DataFrame collected once (`Batch`), an incremental view
    /// (`Incremental`), or an error (`Continuous`). That method is gone and
    /// the routing lives here.
    pub fn to_table(self, session: &crate::Session, name: &str) -> Result<()> {
        match self.refresh {
            crate::Refresh::Batch => {
                let batches = self.df.collect()?.into_batches();
                session.register_record_batches(name, batches)
            }
            crate::Refresh::Incremental => {
                let query = self.df.sql_query.as_deref().ok_or_else(|| {
                    KrishivError::unsupported(
                        "write_stream().refresh(Incremental).to_table requires a SQL-defined \
                         DataFrame (its defining query drives the materialized view)",
                    )
                })?;
                // The IVM engine owns the maintenance from here.
                session.sql(format!("CREATE MATERIALIZED VIEW {name} AS {query}"))?;
                Ok(())
            }
            crate::Refresh::Continuous => Err(KrishivError::unsupported(format!(
                "write_stream().refresh(Continuous).to_table('{name}') needs a streaming \
                 coordinator to run the continuous job; submit it via the continuous-stream \
                 registration API or a cluster-attached session"
            ))),
        }
    }
}

/// DataFrame API backed by DataFusion for R1 local execution.
#[derive(Clone)]
pub struct DataFrame {
    logical_plan: LogicalPlan,
    sql_dataframe: Option<Arc<dyn KrishivDataFrameOps>>,
    sql_query: Option<String>,
    /// Pre-collected batches — set when the DataFrame is constructed from
    /// already-executed results (e.g. [`Session::sql_as`]).
    pre_collected: Option<Vec<RecordBatch>>,
    mode: ExecutionMode,
    jobs: Arc<Mutex<LocalJobRegistry>>,
    next_job_id: Arc<AtomicU64>,
    _coordinator_url: Option<String>,
    /// Coordinator **HTTP** base URL (distinct from the Flight `_coordinator_url`),
    /// used by `to_incremental` to reach the coordinator's IVM management API in
    /// distributed mode. `None` in embedded mode.
    coordinator_http_url: Option<String>,
    /// Set by `Session::view(iv)` when this DataFrame reads an existing
    /// incremental view's output: `to_incremental` co-registers the derived view
    /// into this base job (same registry / coordinator job) so a feed to the base
    /// cascades to it — the live view-DAG. `None` for a normal root DataFrame.
    ivm_parent: Option<crate::IvmJob>,
    /// The **session's** embedded IVM registry, so `to_incremental` registers
    /// its view where `Session::ivm` and `Session::reset_ivm_job` can see it.
    ///
    /// IVM-AUD-INT-F15 / API-D5: `to_incremental` used to build a fresh private
    /// registry per call, so two calls with the same name produced two isolated
    /// views, `Session::ivm(name)` saw neither, and `reset_ivm_job(name)` could
    /// not drop either. `None` only for DataFrames no session built (`new`,
    /// `from_batches`) — none of which can reach `to_incremental`, because it
    /// needs SQL those constructors do not carry.
    ivm_registry: Option<krishiv_runtime::SharedIvmJobRegistry>,
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<DashMap<String, RegisteredParquet>>,
    /// When set, this DataFrame was produced by `cache()` / `persist()` and
    /// the named in-memory table can be removed via `unpersist()`.
    _cache_name: Option<String>,
    /// When true, always collect from the local DataFusion plan even in remote
    /// mode. Set for lakehouse reads (Delta, Hudi) whose table registrations
    /// live only in the local DataFusion context and cannot be forwarded to a
    /// remote executor.
    force_local: bool,
    /// Directives (Python UDF registrations) that must precede any query text
    /// this DataFrame ships to a coordinator.
    ///
    /// Held apart from `sql_query` rather than baked into it: a transform
    /// discards `sql_query` and re-derives the text from the plan, and the
    /// directives have to survive that. Empty for embedded sessions, which
    /// resolve UDFs in process and never ship anything.
    remote_prelude: Option<String>,
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("logical_plan", &self.logical_plan)
            .field("mode", &self.mode)
            .field("has_sql_query", &self.sql_query.is_some())
            .field(
                "pre_collected",
                &self.pre_collected.as_ref().map(|b| b.len()),
            )
            .finish_non_exhaustive()
    }
}

/// A lazily grouped DataFrame awaiting aggregate expressions.
#[derive(Clone)]
pub struct GroupedDataFrame {
    dataframe: DataFrame,
    group_exprs: Vec<Expr>,
}

impl GroupedDataFrame {
    /// Compute aggregate expressions for each group.
    pub fn agg(&self, aggregate_exprs: &[Expr]) -> Result<DataFrame> {
        // Materialize a pre-collected base (execute_remote/collect result) into
        // an SQL-backed DataFrame so groupBy(...).agg(...) works on remote results.
        let base = self.dataframe.as_sql_backed()?;
        let Some(ops) = &base.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "grouped aggregation requires an SQL-backed DataFrame",
            ));
        };
        let groups = self.group_exprs.iter().map(Expr::node).collect::<Vec<_>>();
        let aggregates = aggregate_exprs.iter().map(Expr::node).collect::<Vec<_>>();
        let new_ops = krishiv_common::async_util::block_on(ops.aggregate(&groups, &aggregates))?;
        Ok(base.with_new_ops(new_ops))
    }

    /// Aggregate using SQL-compatible GROUPING SETS, CUBE, or ROLLUP semantics.
    pub fn agg_grouping(
        &self,
        grouping: GroupingSpec,
        aggregate_exprs: &[Expr],
    ) -> Result<DataFrame> {
        let Some(ops) = &self.dataframe.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "grouping aggregation requires an SQL-backed DataFrame",
            ));
        };
        let aggregates = aggregate_exprs.iter().map(Expr::node).collect::<Vec<_>>();
        let grouping = match &grouping {
            GroupingSpec::Sets(sets) => krishiv_sql::GroupingMode::Sets(
                sets.iter()
                    .map(|set| set.iter().map(Expr::node).collect())
                    .collect(),
            ),
            GroupingSpec::Cube(exprs) => {
                krishiv_sql::GroupingMode::Cube(exprs.iter().map(Expr::node).collect())
            }
            GroupingSpec::Rollup(exprs) => {
                krishiv_sql::GroupingMode::Rollup(exprs.iter().map(Expr::node).collect())
            }
        };
        let new_ops =
            krishiv_common::async_util::block_on(ops.aggregate_grouping(grouping, &aggregates))?;
        Ok(self.dataframe.with_new_ops(new_ops))
    }

    /// Count rows in each group.
    pub fn count(&self) -> Result<DataFrame> {
        self.agg(&[crate::expression::count_all().alias("count")])
    }
}

impl DataFrame {
    /// Create a logical-only DataFrame.
    ///
    /// Returns an error if the orphan embedded runtime backing this DataFrame
    /// cannot be constructed (e.g. the in-process cluster fails to start).
    pub fn new(logical_plan: LogicalPlan) -> Result<Self> {
        Ok(Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: None,
            mode: ExecutionMode::Embedded,
            jobs: Arc::new(Mutex::new(LocalJobRegistry::default())),
            next_job_id: Arc::new(AtomicU64::new(1)),
            _coordinator_url: None,
            coordinator_http_url: None,
            ivm_parent: None,
            ivm_registry: None,
            runtime: crate::session::shared_embedded_runtime()?,
            registered_parquet: Arc::new(DashMap::new()),
            force_local: false,
            _cache_name: None,
            remote_prelude: None,
        })
    }

    /// Force collection from the local DataFusion plan regardless of runtime mode.
    pub(crate) fn with_force_local(mut self) -> Self {
        self.force_local = true;
        self
    }

    /// L-7 / P-24 (audit): centralized policy for "should this DataFrame
    /// be routed to the remote runtime?" A DataFrame executes locally
    /// when the runtime is non-remote (embedded / single-node) or when
    /// `force_local` is set (delta / hudi integrations whose plan is
    /// bound to the local catalog). Previously this expression was
    /// repeated at 5+ call sites; a single source of truth makes
    /// future changes (e.g. a new local-only DataFrame type) one-line.
    /// The exact text to submit to a coordinator for this DataFrame.
    ///
    /// Audit: remote execution used to require `sql_query`, which every
    /// transform discards (`with_new_ops` sets it to `None`). That made the
    /// whole DataFrame transform API — select, filter, join, group_by, union,
    /// and the rest — embedded-only in both the Rust and Python surfaces: a
    /// program that worked locally failed the moment the session pointed at a
    /// cluster, with an error naming a missing input rather than an
    /// unsupported mode.
    ///
    /// The plan can be rendered back to SQL, so it is. `to_sql` unparses the
    /// *current* DataFusion logical plan, which reflects every applied
    /// transform, and falls back to the originating query when the plan cannot
    /// be unparsed. Preferring `sql_query` when it is present keeps the
    /// user's own text — comments, hints and formatting intact — for the
    /// untransformed case, and only reaches for the unparser when there is no
    /// alternative.
    ///
    /// Some plans genuinely cannot be unparsed. Those still fail, but now with
    /// a message that says what happened and what to do about it.
    pub(crate) fn query_for_remote(&self) -> Result<String> {
        let base = match self.sql_query.as_deref() {
            Some(query) => query.to_owned(),
            None => {
                let ops = self.sql_dataframe.as_ref().ok_or_else(|| {
                    KrishivError::unsupported(
                        "this DataFrame carries neither a SQL query nor a plan, so there is \
                         nothing to send to the cluster",
                    )
                })?;
                ops.to_sql().map_err(|error| {
                    KrishivError::unsupported(format!(
                        "this DataFrame's transforms cannot be rendered back to SQL, and a \
                         remote session can only execute SQL ({error}). Either express the \
                         query in SQL and pass it to session.sql(), or call \
                         session.execute_local() to run it in this process against local data"
                    ))
                })?
            }
        };
        Ok(match self.remote_prelude.as_deref() {
            Some(prelude) if !prelude.is_empty() => format!("{prelude}\n{base}"),
            _ => base,
        })
    }

    pub(crate) fn is_locally_evaluated(&self) -> bool {
        !self.runtime.uses_remote_execution() || self.force_local
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_sql_dataframe(
        mode: ExecutionMode,
        sql_dataframe: impl KrishivDataFrameOps + 'static,
        sql_query: Option<String>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        coordinator_url: Option<String>,
        coordinator_http_url: Option<String>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<DashMap<String, RegisteredParquet>>,
        remote_prelude: Option<String>,
        ivm_registry: Option<krishiv_runtime::SharedIvmJobRegistry>,
    ) -> Self {
        let logical_plan = sql_dataframe.krishiv_logical_plan();
        Self {
            logical_plan,
            sql_dataframe: Some(Arc::new(sql_dataframe)),
            sql_query,
            pre_collected: None,
            mode,
            jobs,
            next_job_id,
            _coordinator_url: coordinator_url,
            coordinator_http_url,
            ivm_parent: None,
            ivm_registry,
            runtime,
            registered_parquet,
            force_local: false,
            _cache_name: None,
            remote_prelude,
        }
    }

    /// Construct a [`DataFrame`] from a pre-collected list of record batches.
    ///
    /// Used by [`Session::sql_as`] to wrap the results of a policy-enforced query.
    pub(crate) fn from_batches(
        mode: ExecutionMode,
        batches: Vec<RecordBatch>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<DashMap<String, RegisteredParquet>>,
    ) -> Self {
        let logical_plan = LogicalPlan::new("policy-enforced-query", ExecutionKind::Batch);
        Self {
            logical_plan,
            sql_dataframe: None,
            sql_query: None,
            pre_collected: Some(batches),
            mode,
            jobs,
            next_job_id,
            _coordinator_url: None,
            coordinator_http_url: None,
            ivm_parent: None,
            // A pre-collected DataFrame carries no SQL, so it cannot reach
            // `to_incremental` (which needs a body to register) and has no use
            // for a registry.
            ivm_registry: None,
            runtime,
            registered_parquet,
            force_local: false,
            _cache_name: None,
            remote_prelude: None,
        }
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Explicit finite/unbounded metadata carried by the canonical DataFrame.
    pub fn boundedness(&self) -> Boundedness {
        if self.logical_plan.kind() == ExecutionKind::Streaming {
            Boundedness::Unbounded
        } else {
            Boundedness::Bounded
        }
    }

    pub fn is_bounded(&self) -> bool {
        self.boundedness().is_bounded()
    }

    /// Explain the current plan.
    pub fn explain(&self) -> Result<String> {
        krishiv_common::async_util::block_on(self.explain_async())
    }

    /// Explain the plan at the requested detail level.
    pub fn explain_with(&self, mode: ExplainMode) -> Result<String> {
        match mode {
            ExplainMode::Logical => Ok(self.explain_logical()),
            ExplainMode::Physical => self.explain(),
            ExplainMode::Analyze => {
                let (result, stats) = self.collect_with_stats()?;
                Ok(format!(
                    "{}

Execution statistics:
  output_rows={}
  result_rows={}
  cpu_nanos={}",
                    self.explain()?,
                    stats.output_rows,
                    result.row_count(),
                    stats.cpu_nanos
                ))
            }
        }
    }

    /// Collect local SQL results with engine execution statistics.
    pub fn collect_with_stats(&self) -> Result<(QueryResult, QueryExecutionStats)> {
        if !self.is_locally_evaluated() {
            return Err(KrishivError::unsupported(
                "remote collect_with_stats requires coordinator query-metrics transport",
            ));
        }
        let Some(dataframe) = &self.sql_dataframe else {
            return Ok((
                QueryResult::new(self.pre_collected.clone().unwrap_or_default()),
                QueryExecutionStats::default(),
            ));
        };
        let (batches, stats) =
            krishiv_common::async_util::block_on(dataframe.collect_with_stats())?;
        Ok((
            QueryResult::new(batches),
            QueryExecutionStats {
                output_rows: stats.output_rows,
                cpu_nanos: stats.cpu_nanos,
            },
        ))
    }

    /// Unified execution entry point — routes to `collect_async()` for batch
    /// queries and `execute_stream_async()` for streaming queries.
    ///
    /// The routing decision is based on the logical plan's `ExecutionKind`.
    /// Queries built against registered streaming sources (Kafka, etc.) return
    /// `ExecutionResult::Stream`; all other queries return `ExecutionResult::Batch`.
    ///
    /// This is the preferred API when the caller does not know ahead of time
    /// whether the query is batch or streaming. The existing `collect()` and
    /// `execute_stream_async()` methods remain available for explicit control.
    pub async fn execute(self) -> Result<ExecutionResult> {
        if self.logical_plan.kind() == ExecutionKind::Streaming {
            let stream = self.execute_stream_async().await?;
            Ok(ExecutionResult::Stream(stream))
        } else {
            let result = self.collect_async().await?;
            Ok(ExecutionResult::Batch(result.into_batches()))
        }
    }

    /// Convert this DataFrame into a fluent `StreamingDataFrame` builder
    /// for executing async stream operations with windows and aggregations.
    pub fn stream(&self) -> crate::streaming_dataframe::StreamingDataFrame {
        crate::streaming_dataframe::StreamingDataFrame::new(self.clone())
    }

    /// Configure event-time processing on the canonical DataFrame.
    pub fn with_event_time(
        &self,
        column: impl Into<String>,
    ) -> crate::streaming_dataframe::StreamingDataFrame {
        self.stream().with_event_time(column)
    }

    /// Key the canonical DataFrame for stateful/windowed processing.
    pub fn key_by(
        &self,
        column: impl Into<String>,
    ) -> crate::streaming_dataframe::StreamingDataFrame {
        self.stream().key_by(column)
    }

    /// Configure a tumbling event-time window from the canonical DataFrame.
    pub fn tumbling_window(
        &self,
        event_time: impl Into<String>,
        window_size_ms: u64,
    ) -> crate::streaming_dataframe::StreamingDataFrame {
        self.stream()
            .with_event_time(event_time)
            .tumbling_window(window_size_ms)
    }

    /// Configure a sliding event-time window from the canonical DataFrame.
    pub fn sliding_window(
        &self,
        event_time: impl Into<String>,
        window_size_ms: u64,
        slide_ms: u64,
    ) -> crate::streaming_dataframe::StreamingDataFrame {
        self.stream()
            .with_event_time(event_time)
            .sliding_window(window_size_ms, slide_ms)
    }

    /// Configure a session event-time window from the canonical DataFrame.
    pub fn session_window(
        &self,
        event_time: impl Into<String>,
        gap_ms: u64,
    ) -> crate::streaming_dataframe::StreamingDataFrame {
        self.stream()
            .with_event_time(event_time)
            .session_window(gap_ms)
    }

    /// Execute the query and report per-operator runtime metrics.
    ///
    /// `explain_async` returns the plan; this returns what the plan actually
    /// did — rows per operator, row groups pruned, bytes scanned, elapsed
    /// time per partition. That distinction decides real performance
    /// questions: a plan can show a dynamic filter on a scan and still tell
    /// you nothing, because the filter is only populated at run time.
    ///
    /// Local execution only. A remote plan's metrics live on the executors
    /// that ran the fragments, so they are not reachable through this handle.
    pub async fn explain_analyze_async(&self) -> Result<String> {
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain_analyze().await.map_err(Into::into),
            None => Err(KrishivError::Runtime {
                message: String::from(
                    "EXPLAIN ANALYZE needs a locally-planned query; this handle has none",
                ),
            }),
        }
    }

    pub async fn explain_async(&self) -> Result<String> {
        let is_local = !self.runtime.uses_remote_execution();
        if is_local {
            let df = &self.sql_dataframe;
            if let Some(dataframe) = df {
                return dataframe.explain().await.map_err(Into::into);
            }
        }
        if let Some(query) = self.sql_query.as_deref() {
            return self.runtime.explain_sql(query).map_err(KrishivError::from);
        }
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain().await.map_err(Into::into),
            None => Ok(self.logical_plan.describe()),
        }
    }

    /// Explain the Krishiv logical wrapper only.
    pub fn explain_logical(&self) -> String {
        match &self.sql_dataframe {
            Some(dataframe) => dataframe.explain_logical(),
            None => self.logical_plan.describe(),
        }
    }

    /// Convert this DataFrame into an incrementally-maintained view (delta/IVM
    /// mode) named `name` — the third conversion alongside `collect()` (batch)
    /// and `to_streaming()` (streaming). The DataFrame's plan becomes the view's
    /// SQL body; the returned [`IncrementalDataFrame`](crate::IncrementalDataFrame)
    /// feeds `DeltaBatch` changes and reads `snapshot()` / the output change-feed.
    ///
    /// Requires an SQL-backed DataFrame (built from `session.sql`/`table`/`read_*`),
    /// whose scan leaves are the feedable sources. Inherits the session's mode.
    ///
    /// # This job is pinned to a single flow; `Session::ivm` auto-partitions
    ///
    /// IVM-AUD-API-A3. The job created here never shards, in any mode, because
    /// the view may gain derived views (`Session::view` + `to_incremental`) and
    /// a key-partitioned flow does not cascade a base view's output to
    /// downstream views. [`Session::ivm`](crate::Session::ivm) makes the
    /// opposite choice for the same session and mode: its first single-column
    /// `GROUP BY` view shards the job across `KRISHIV_IVM_SHARDS` flows. So the
    /// same aggregate reaches a fraction of the throughput through this entry
    /// point, and `Session::ivm` cannot host a view-DAG. Neither is a default
    /// the other can override — choose the entry point by which property you
    /// need.
    ///
    /// The view is registered in the **session's** IVM registry, so
    /// `Session::ivm(name)` reaches the same job and `Session::reset_ivm_job`
    /// drops it (IVM-AUD-INT-F15). One consequence of that shared namespace:
    /// this call fails if `name` is already held by a partitioned job, rather
    /// than quietly handing back one that cannot cascade (IVM-AUD-INT-F16).
    pub async fn to_incremental(&self, name: &str) -> Result<crate::IncrementalDataFrame> {
        // IVM-AUD-API-F2. No body this could choose honours the cache. An
        // incremental view is populated by the deltas fed to it, never by a
        // session table, so the rows `cache()` just materialized are absent from
        // the view either way — and *which* body gets chosen turns on an
        // invisible detail of how the DataFrame was built. With a SQL-ops
        // backing, `ops.to_sql()` below re-derives the pre-cache query and the
        // view reads the original sources, so `cache()` bought nothing; without
        // one, `sql_query` is `SELECT * FROM "_krishiv_cache_N"` and the view's
        // only feedable source is a generated name the caller never chose. Both
        // used to return a handle that looked like a working view over the
        // cached data.
        if let Some(cache_name) = &self._cache_name {
            return Err(KrishivError::unsupported(format!(
                "cannot build incremental view '{name}' from a cached DataFrame: cache() \
                 materialized this query into the session table '{cache_name}', and an \
                 incremental view is populated by the deltas you feed it, not by a session \
                 table — so none of the cached rows would reach the view. Depending on how \
                 this DataFrame was built its body would either re-derive the pre-cache query \
                 (making cache() a no-op here) or read '{cache_name}' itself, whose generated \
                 name would then be the view's only feedable source. Call to_incremental() on \
                 the uncached DataFrame and feed it."
            )));
        }
        let body_sql = match &self.sql_dataframe {
            Some(ops) => ops.to_sql()?,
            None => self.sql_query.clone().ok_or_else(|| {
                KrishivError::unsupported(
                    "to_incremental requires an SQL-backed DataFrame \
                     (build it from session.sql/table/read_*)",
                )
            })?,
        };
        let output_schema = self.schema()?;
        // View-DAG: if this DataFrame reads an existing view's output
        // (Session::view), co-register the derived view into the SAME base job so
        // a feed to the base cascades to it (topological multi-view IVM).
        if let Some(parent) = &self.ivm_parent {
            return crate::IncrementalDataFrame::derive_on_job(
                parent.clone(),
                name,
                body_sql,
                output_schema,
            )
            .await;
        }
        // IVM management lives on the coordinator's HTTP port (distinct from the
        // Flight `_coordinator_url`); prefer it, falling back to the Flight URL
        // for single-port/local setups where they coincide.
        let ivm_url = self
            .coordinator_http_url
            .clone()
            .or_else(|| self._coordinator_url.clone());
        crate::IncrementalDataFrame::from_view_sql(
            name,
            body_sql,
            output_schema,
            self.mode,
            ivm_url,
            self.ivm_registry.clone(),
        )
        .await
    }

    /// Tag this DataFrame as reading `parent`'s view output, so `to_incremental`
    /// co-registers the derived view into `parent`'s job. Set by `Session::view`.
    pub fn with_ivm_parent(mut self, parent: crate::IvmJob) -> Self {
        self.ivm_parent = Some(parent);
        self
    }

    /// Collect results.
    pub fn collect(&self) -> Result<QueryResult> {
        krishiv_common::async_util::block_on(self.collect_async())
    }

    /// The execution runtime backing this DataFrame (embedded in-process, or the
    /// remote/coordinator runtime for a distributed session). Exposed so the
    /// streaming builder can route windowed collection through the same
    /// distributed primitive (`collect_bounded_window`) the DataStream path uses.
    pub(crate) fn runtime(&self) -> Arc<dyn ExecutionRuntime> {
        Arc::clone(&self.runtime)
    }

    /// Collect results and convert to a ``DeltaBatch`` (all rows as insertions,
    /// weight +1). Convenience bridge from the SQL/DataFrame API to the IVM
    /// engine: use this to feed SQL query output into an incremental view.
    pub fn collect_as_delta_batch(&self) -> Result<krishiv_delta::DeltaBatch> {
        let result = self.collect()?;
        result.into_delta_batch()
    }

    /// Asynchronously collect results.
    pub async fn collect_async(&self) -> Result<QueryResult> {
        let job_id = self.start_job("local-dataframe");
        self.update_job(&job_id, "local-dataframe", JobState::Running);

        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-dataframe", JobState::Succeeded);
            return Ok(QueryResult::new(batches.clone()));
        }

        // Guard: collecting an unbounded streaming query would block forever.
        // The plan kind is set to Streaming by SqlEngine::sql() when the query
        // references a registered streaming source (Kafka, etc.).
        // Callers should use .stream().execute_stream_async() instead.
        if self.logical_plan.kind() == ExecutionKind::Streaming {
            self.update_job(&job_id, "local-dataframe", JobState::Failed);
            let query_hint = self.sql_query.as_deref().unwrap_or("<streaming query>");
            return Err(KrishivError::unsupported(format!(
                "collect() on streaming query '{}' would block forever on an unbounded source; \
                 use .stream() / .execute_stream_async() to consume the stream incrementally",
                query_hint
            )));
        }

        let uses_remote = !self.is_locally_evaluated();

        let result = if uses_remote {
            match self.query_for_remote() {
                Ok(query) => {
                    let tables = self
                        .registered_parquet
                        .iter()
                        .map(|entry| entry.value().to_registration(entry.key()))
                        .collect::<Vec<_>>();
                    crate::session::runtime_collect_batch_sql(
                        Arc::clone(&self.runtime),
                        &query,
                        &tables,
                        false,
                    )
                    .await
                    .map(QueryResult::new)
                }
                Err(error) => Err(error),
            }
        } else if let Some(dataframe) = &self.sql_dataframe {
            dataframe
                .collect()
                .await
                .map(QueryResult::new)
                .map_err(Into::into)
        } else {
            Err(KrishivError::unsupported(
                "logical-only DataFrame cannot be collected",
            ))
        };

        match &result {
            Ok(_) => self.update_job(&job_id, "local-dataframe", JobState::Succeeded),
            Err(_) => self.update_job(&job_id, "local-dataframe", JobState::Failed),
        }

        result
    }

    /// Asynchronously execute and return a record batch stream.
    pub async fn execute_stream_async(&self) -> Result<crate::streaming_dataframe::KrishivStream> {
        let job_id = self.start_job("local-streaming");
        self.update_job(&job_id, "local-streaming", JobState::Running);

        if let Some(batches) = &self.pre_collected {
            self.update_job(&job_id, "local-streaming", JobState::Succeeded);
            let stream = futures::stream::iter(batches.clone().into_iter().map(Ok));
            return Ok(Box::pin(stream));
        }

        let uses_remote = !self.is_locally_evaluated();

        let result = if uses_remote {
            match self.query_for_remote() {
                Ok(query) => {
                    let tables = self
                        .registered_parquet
                        .iter()
                        .map(|entry| entry.value().to_registration(entry.key()))
                        .collect::<Vec<_>>();
                    let is_streaming = self.logical_plan.kind() == ExecutionKind::Streaming;
                    let batches = crate::session::runtime_collect_batch_sql(
                        Arc::clone(&self.runtime),
                        &query,
                        &tables,
                        is_streaming,
                    )
                    .await?;
                    let stream = futures::stream::iter(batches.into_iter().map(Ok));
                    Ok(Box::pin(stream) as crate::streaming_dataframe::KrishivStream)
                }
                Err(error) => Err(error),
            }
        } else if let Some(dataframe) = &self.sql_dataframe {
            if !self.force_local {
                self.runtime
                    .accept_plan(&PhysicalPlan::new(
                        self.logical_plan.name(),
                        self.logical_plan.kind(),
                    ))
                    .map_err(KrishivError::from)?;
            }
            dataframe
                .execute_stream()
                .await
                .map(|sql_stream| {
                    use futures::StreamExt;
                    // Adapt SqlError → String at the KrishivStream boundary so
                    // callers retain a stable public error type.
                    let mapped = sql_stream.map(|r| r.map_err(|e| e.to_string()));
                    Box::pin(mapped) as crate::streaming_dataframe::KrishivStream
                })
                .map_err(Into::into)
        } else {
            self.runtime
                .accept_plan(&PhysicalPlan::new(
                    self.logical_plan.name(),
                    self.logical_plan.kind(),
                ))
                .map_err(KrishivError::from)?;
            Err(KrishivError::unsupported(
                "logical-only DataFrame cannot be streamed",
            ))
        };

        match &result {
            Ok(_) => self.update_job(&job_id, "local-streaming", JobState::Succeeded),
            Err(_) => self.update_job(&job_id, "local-streaming", JobState::Failed),
        }

        result
    }

    /// Submit this query asynchronously and return a [`QueryHandle`].
    ///
    /// The query is immediately dispatched as a Tokio task.  The handle lets
    /// callers track progress, cancel the query, or `.await` the result via
    /// [`QueryHandle::wait`].
    ///
    /// This is the Phase-E single entry point that routes collect, writes, and
    /// stream submission through one typed handle.
    pub fn submit_async(self) -> crate::query::QueryHandle {
        let id = crate::query::QueryId::next();
        let (mut handle, driver) = crate::query::QueryHandle::new(id);
        let task = tokio::spawn(async move {
            driver.set_running();
            // BATCH-7: if cancelled before execution starts, set the terminal
            // status so wait() returns a clean cancellation error.
            if driver.is_cancelled() {
                driver.set_failed("cancelled before execution started");
                return;
            }
            match self.collect_async().await {
                Ok(result) => {
                    let rows = result.row_count() as u64;
                    driver.update_progress(rows, rows);
                    driver.set_completed(result);
                }
                Err(e) => driver.set_failed(e.to_string()),
            }
        });
        handle._task = Some(task);
        handle
    }

    fn start_job(&self, name: &str) -> JobId {
        let id = JobId::try_new(format!(
            "local-{}",
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        ))
        .unwrap_or_else(|e| unreachable!("job id is always non-empty: {e}"));
        self.update_job(&id, name, JobState::Pending);
        id
    }

    fn update_job(&self, id: &JobId, name: &str, state: JobState) {
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.upsert(JobStatus::new(id.clone(), name, state));
    }

    /// Clone this `DataFrame` without the SQL ops handle, preserving
    /// all runtime state and pre-collected data.
    fn clone_no_ops(&self) -> Self {
        DataFrame {
            logical_plan: self.logical_plan.clone(),
            sql_dataframe: None,
            sql_query: self.sql_query.clone(),
            pre_collected: self.pre_collected.clone(),
            mode: self.mode,
            jobs: self.jobs.clone(),
            next_job_id: self.next_job_id.clone(),
            _coordinator_url: self._coordinator_url.clone(),
            coordinator_http_url: self.coordinator_http_url.clone(),
            ivm_parent: self.ivm_parent.clone(),
            ivm_registry: self.ivm_registry.clone(),
            runtime: self.runtime.clone(),
            registered_parquet: self.registered_parquet.clone(),
            force_local: self.force_local,
            _cache_name: None,
            remote_prelude: self.remote_prelude.clone(),
        }
    }

    /// Materialize a pre-collected DataFrame into an SQL-backed one by
    /// registering its in-memory batches in an ephemeral `SqlEngine`. This lets
    /// the lazy transform methods (select / filter / sort / groupBy / withColumn
    /// / distinct / drop / …) run on results read back from the coordinator
    /// (`execute_remote` / `collect` / `sql_as`) instead of failing with
    /// "requires an SQL-backed DataFrame". No-op when already SQL-backed. The
    /// resulting `SqlDataFrame` owns its DataFusion session state (Arc-backed),
    /// so the ephemeral engine can drop without invalidating it.
    fn as_sql_backed(&self) -> Result<DataFrame> {
        if self.sql_dataframe.is_some() {
            return Ok(self.clone());
        }
        let batches = self.pre_collected.clone().ok_or_else(|| {
            KrishivError::unsupported("DataFrame has neither SQL ops nor collected data")
        })?;
        // Zero batches means no schema to register; a non-empty batch list
        // with zero rows still carries its schema and registers fine.
        if batches.is_empty() {
            return Err(KrishivError::unsupported(
                "cannot transform an empty pre-collected DataFrame with no schema",
            ));
        }
        let engine = krishiv_sql::SqlEngine::new();
        krishiv_common::async_util::block_on(
            engine.register_record_batches("__krishiv_df", batches),
        )?;
        let sql_df =
            krishiv_common::async_util::block_on(engine.sql("SELECT * FROM __krishiv_df"))?;
        let mut df = self.with_new_ops(Box::new(sql_df));
        // The batches are already local (fetched from the coordinator), and the
        // ephemeral engine's in-memory table cannot be shipped for remote
        // execution. Force local collection so transforms on this materialized
        // DataFrame run against the in-memory data instead of failing with
        // "remote execution requires a SQL query".
        df.force_local = true;
        Ok(df)
    }

    /// Create a new `DataFrame` from a new inner ops object, preserving
    /// runtime state from `self`.
    fn with_new_ops(&self, new_ops: Box<dyn KrishivDataFrameOps>) -> Self {
        let ops: Arc<dyn KrishivDataFrameOps> = Arc::from(new_ops);
        let logical_plan = ops.krishiv_logical_plan();
        DataFrame {
            logical_plan,
            sql_dataframe: Some(ops),
            sql_query: None,
            pre_collected: None,
            mode: self.mode,
            jobs: self.jobs.clone(),
            next_job_id: self.next_job_id.clone(),
            _coordinator_url: self._coordinator_url.clone(),
            coordinator_http_url: self.coordinator_http_url.clone(),
            ivm_parent: self.ivm_parent.clone(),
            ivm_registry: self.ivm_registry.clone(),
            runtime: self.runtime.clone(),
            registered_parquet: self.registered_parquet.clone(),
            force_local: self.force_local,
            _cache_name: None,
            remote_prelude: self.remote_prelude.clone(),
        }
    }

    // ── DataFrame transforms (lazy) ─────────────────────────────────────────

    /// Return the Arrow schema of this DataFrame.
    pub fn schema(&self) -> Result<SchemaRef> {
        match (&self.sql_dataframe, &self.pre_collected) {
            (Some(df), _) => Ok(df.schema()),
            (_, Some(batches)) => {
                if let Some(batch) = batches.first() {
                    Ok(batch.schema())
                } else {
                    Err(KrishivError::unsupported(
                        "cannot get schema from empty pre-collected DataFrame",
                    ))
                }
            }
            (None, None) => Err(KrishivError::unsupported(
                "cannot get schema from logical-only DataFrame",
            )),
        }
    }

    /// Column names, in schema order.
    pub fn columns(&self) -> Result<Vec<String>> {
        Ok(self
            .schema()?
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect())
    }

    /// Number of rows (collects the DataFrame).
    pub fn num_rows(&self) -> Result<usize> {
        Ok(self.collect()?.row_count())
    }

    /// Select columns by name.
    pub fn select(&self, columns: &[&str]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.select(columns))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.select(columns),
            None => Err(KrishivError::unsupported(
                "select requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Select typed expressions.
    pub fn select_exprs(&self, expressions: &[Expr]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let expressions = expressions.iter().map(Expr::node).collect::<Vec<_>>();
                let new_ops = krishiv_common::async_util::block_on(df.select_exprs(&expressions))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.select_exprs(expressions),
            None => Err(KrishivError::unsupported(
                "select_exprs requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Unnest (explode) one or more array columns — one output row per element.
    /// Multiple equal-length columns are zipped element-wise. Requires an
    /// SQL-backed DataFrame.
    pub fn unnest(&self, columns: &[&str]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.unnest_columns(columns))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.unnest(columns),
            None => Err(KrishivError::unsupported(
                "unnest requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Filter rows using a typed expression.
    pub fn filter_expr(&self, predicate: Expr) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops =
                    krishiv_common::async_util::block_on(df.filter_expr(predicate.node()))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.filter_expr(predicate),
            None => Err(KrishivError::unsupported(
                "filter_expr requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Group rows by typed expressions.
    pub fn group_by(&self, expressions: &[Expr]) -> GroupedDataFrame {
        GroupedDataFrame {
            dataframe: self.clone(),
            group_exprs: expressions.to_vec(),
        }
    }

    /// Pivot known values into aggregate columns.
    pub fn pivot(
        &self,
        group_exprs: &[Expr],
        pivot_column: Expr,
        aggregate_expr: Expr,
        values: &[PivotValue],
    ) -> Result<DataFrame> {
        let Some(ops) = &self.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "pivot requires an SQL-backed DataFrame",
            ));
        };
        let groups = group_exprs.iter().map(Expr::node).collect::<Vec<_>>();
        let values = values
            .iter()
            .map(|value| (value.value.clone(), value.alias.clone()))
            .collect::<Vec<_>>();
        let new_ops = krishiv_common::async_util::block_on(ops.pivot(
            &groups,
            pivot_column.node(),
            aggregate_expr.node(),
            &values,
        ))?;
        Ok(self.with_new_ops(new_ops))
    }

    /// Unpivot columns into name/value rows while preserving all other columns.
    pub fn unpivot(
        &self,
        columns: &[&str],
        name_column: &str,
        value_column: &str,
    ) -> Result<DataFrame> {
        let Some(ops) = &self.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "unpivot requires an SQL-backed DataFrame",
            ));
        };
        let new_ops =
            krishiv_common::async_util::block_on(ops.unpivot(columns, name_column, value_column))?;
        Ok(self.with_new_ops(new_ops))
    }

    /// Filter rows by a SQL predicate expression.
    pub fn filter(&self, predicate: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.filter(predicate))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.filter(predicate),
            None => Err(KrishivError::unsupported(
                "filter requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Alias for [`filter`].
    pub fn r#where(&self, predicate: &str) -> Result<DataFrame> {
        self.filter(predicate)
    }

    /// Limit the number of rows.
    pub fn limit(&self, n: usize) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.limit(n))?;
                Ok(self.with_new_ops(new_ops))
            }
            // A pre-collected DataFrame (produced by execute_remote / collect /
            // sql_as) has no SQL ops handle — slice its in-memory batches to the
            // first `n` rows instead of erroring. This makes limit()/head()/take()
            // work on results read back from the coordinator, matching the
            // SQL-backed behaviour (previously "limit requires an SQL-backed
            // DataFrame", which broke df.head() on any remote result).
            None => match &self.pre_collected {
                Some(batches) => {
                    let mut sliced = Vec::new();
                    let mut remaining = n;
                    for batch in batches {
                        if remaining == 0 {
                            break;
                        }
                        let take = remaining.min(batch.num_rows());
                        if take > 0 {
                            sliced.push(batch.slice(0, take));
                            remaining -= take;
                        }
                    }
                    // limit(0) (or an all-empty source) must still carry the
                    // schema — keep a zero-row slice of the first batch so
                    // downstream collect()/toPandas() has a schema to render
                    // instead of failing with "Must pass schema".
                    if sliced.is_empty()
                        && let Some(first) = batches.first()
                    {
                        sliced.push(first.slice(0, 0));
                    }
                    let mut df = self.clone_no_ops();
                    df.pre_collected = Some(sliced);
                    Ok(df)
                }
                None => Err(KrishivError::unsupported(
                    "limit requires an SQL-backed or pre-collected DataFrame",
                )),
            },
        }
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.distinct())?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.distinct(),
            None => Err(KrishivError::unsupported(
                "distinct requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Drop rows containing nulls in any selected column. Empty `columns` checks all columns.
    pub fn drop_nulls(&self, columns: &[&str]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.drop_nulls(columns))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.drop_nulls(columns),
            None => Err(KrishivError::unsupported(
                "drop_nulls requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Bernoulli-sample rows using a fraction in the inclusive range 0..=1.
    pub fn sample(&self, fraction: f64) -> Result<DataFrame> {
        if !(0.0..=1.0).contains(&fraction) {
            return Err(KrishivError::InvalidConfig {
                message: "sample fraction must be between 0 and 1".into(),
            });
        }
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.sample(fraction))?;
                Ok(self.with_new_ops(new_ops))
            }
            None => Err(KrishivError::unsupported(
                "sample requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Sort by columns with ascending direction.
    pub fn order_by(&self, columns: &[&str]) -> Result<DataFrame> {
        let descending: Vec<bool> = vec![false; columns.len()];
        self.sort(columns, &descending)
    }

    /// Sort by columns with explicit direction.
    pub fn sort(&self, columns: &[&str], descending: &[bool]) -> Result<DataFrame> {
        if columns.len() != descending.len() {
            return Err(KrishivError::InvalidConfig {
                message: "columns and descending must have the same length".into(),
            });
        }
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.sort(columns, descending))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.sort(columns, descending),
            None => Err(KrishivError::unsupported(
                "sort requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Assign an alias (table name) to this DataFrame.
    pub fn alias(&self, alias: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.alias(alias))?;
                Ok(self.with_new_ops(new_ops))
            }
            None => Ok(DataFrame {
                ..self.clone_no_ops()
            }),
        }
    }

    /// Drop columns by name.
    pub fn drop(&self, columns: &[&str]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.drop_columns(columns))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.drop(columns),
            None => Err(KrishivError::unsupported(
                "drop requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Rename a column.
    pub fn rename(&self, old: &str, new: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.rename_column(old, new))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.rename(old, new),
            None => Err(KrishivError::unsupported(
                "rename requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Rename several columns in one call (PySpark `withColumnsRenamed`) — each
    /// pair is `(existing, new)`, applied left to right. Folds a run of
    /// [`rename`](Self::rename) calls into one polymorphic entry (Phase 61
    /// variant-collapse).
    pub fn with_columns_renamed(&self, renames: &[(&str, &str)]) -> Result<DataFrame> {
        let mut df = self.clone();
        for (old, new) in renames {
            df = df.rename(old, new)?;
        }
        Ok(df)
    }

    /// Add or replace a column with a computed expression (SQL-based).
    pub fn with_column(&self, name: &str, expr: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.with_column(name, expr))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.with_column(name, expr),
            None => Err(KrishivError::unsupported(
                "with_column requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Fill null values in a column with a literal value (uses COALESCE).
    pub fn fill_null(&self, column: &str, value: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.fill_null(column, value))?;
                Ok(self.with_new_ops(new_ops))
            }
            None if self.pre_collected.is_some() => self.as_sql_backed()?.fill_null(column, value),
            None => Err(KrishivError::unsupported(
                "fill_null requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Join with another DataFrame using equi-join keys.
    ///
    /// Both DataFrames must be SQL-backed (created via `sql()` or `read_parquet()`).
    /// Supported join types: inner, left, right, full/outer, left_semi, right_semi,
    /// left_anti, right_anti.
    pub fn join_on(
        &self,
        right: &DataFrame,
        how: JoinType,
        left_on: &[&str],
        right_on: &[&str],
    ) -> Result<DataFrame> {
        self.join(right, how.as_str(), left_on, right_on)
    }

    pub fn join(
        &self,
        right: &DataFrame,
        how: &str,
        left_on: &[&str],
        right_on: &[&str],
    ) -> Result<DataFrame> {
        match (&self.sql_dataframe, &right.sql_dataframe) {
            (Some(left), Some(right)) => {
                let new_ops = krishiv_common::async_util::block_on(left.join(
                    right.as_ref(),
                    how,
                    left_on,
                    right_on,
                ))?;
                Ok(self.with_new_ops(new_ops))
            }
            _ => Err(KrishivError::unsupported(
                "join requires both DataFrames to be SQL-backed",
            )),
        }
    }

    /// Union this DataFrame with another (UNION ALL semantics).
    ///
    /// Both DataFrames must be SQL-backed and have the same number of columns
    /// with compatible types.
    pub fn union(&self, right: &DataFrame) -> Result<DataFrame> {
        match (&self.sql_dataframe, &right.sql_dataframe) {
            (Some(left), Some(right)) => {
                let new_ops = krishiv_common::async_util::block_on(left.union(right.as_ref()))?;
                Ok(self.with_new_ops(new_ops))
            }
            _ => Err(KrishivError::unsupported(
                "union requires both DataFrames to be SQL-backed",
            )),
        }
    }

    /// Union and remove duplicate rows.
    pub fn union_distinct(&self, right: &DataFrame) -> Result<DataFrame> {
        match (&self.sql_dataframe, &right.sql_dataframe) {
            (Some(left), Some(right)) => {
                let ops =
                    krishiv_common::async_util::block_on(left.union_distinct(right.as_ref()))?;
                Ok(self.with_new_ops(ops))
            }
            _ => Err(KrishivError::unsupported(
                "union_distinct requires SQL-backed DataFrames",
            )),
        }
    }

    /// Union with `right`, aligning columns **by name** rather than by position
    /// (PySpark `unionByName`). Both DataFrames must expose the same set of
    /// column names; `right`'s columns are reordered to match `self`'s schema
    /// before the positional [`union`](Self::union). Spark's
    /// `allowMissingColumns=True` (null-filling absent columns) is not yet
    /// supported — a differing column set is a clear error, never a silent
    /// misalignment.
    pub fn union_by_name(&self, right: &DataFrame) -> Result<DataFrame> {
        let left_schema = self.schema()?;
        let right_schema = right.schema()?;
        let left_names: Vec<&str> = left_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let right_names: std::collections::HashSet<&str> = right_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        if left_names.len() != right_names.len()
            || !left_names.iter().all(|name| right_names.contains(name))
        {
            return Err(KrishivError::unsupported(
                "union_by_name requires both DataFrames to have the same set of column names \
                 (allowMissingColumns is not yet supported)",
            ));
        }
        let right_reordered = right.select(&left_names)?;
        self.union(&right_reordered)
    }

    /// Begin a live-table / streaming write (PySpark `df.writeStream`), the
    /// DataFrame-side entry to the Phase 61 keystone. The refresh mode chosen on
    /// the returned builder selects the compute engine; `to_table` needs the
    /// owning [`Session`](crate::Session) (a DataFrame carries no session ref).
    pub fn write_stream(&self) -> WriteStreamBuilder<'_> {
        WriteStreamBuilder {
            df: self,
            refresh: crate::Refresh::Batch,
        }
    }

    /// Return rows present in both DataFrames, preserving duplicate multiplicity.
    pub fn intersect(&self, right: &DataFrame) -> Result<DataFrame> {
        self.intersect_impl(right, false)
    }

    /// Return distinct rows present in both DataFrames.
    pub fn intersect_distinct(&self, right: &DataFrame) -> Result<DataFrame> {
        self.intersect_impl(right, true)
    }

    fn intersect_impl(&self, right: &DataFrame, distinct: bool) -> Result<DataFrame> {
        match (&self.sql_dataframe, &right.sql_dataframe) {
            (Some(left), Some(right)) => {
                let ops =
                    krishiv_common::async_util::block_on(left.intersect(right.as_ref(), distinct))?;
                Ok(self.with_new_ops(ops))
            }
            _ => Err(KrishivError::unsupported(
                "intersect requires SQL-backed DataFrames",
            )),
        }
    }

    /// Return rows from this DataFrame that are absent from `right`.
    pub fn except(&self, right: &DataFrame) -> Result<DataFrame> {
        self.except_impl(right, false)
    }

    /// Return distinct rows from this DataFrame that are absent from `right`.
    pub fn except_distinct(&self, right: &DataFrame) -> Result<DataFrame> {
        self.except_impl(right, true)
    }

    fn except_impl(&self, right: &DataFrame, distinct: bool) -> Result<DataFrame> {
        match (&self.sql_dataframe, &right.sql_dataframe) {
            (Some(left), Some(right)) => {
                let ops =
                    krishiv_common::async_util::block_on(left.except(right.as_ref(), distinct))?;
                Ok(self.with_new_ops(ops))
            }
            _ => Err(KrishivError::unsupported(
                "except requires SQL-backed DataFrames",
            )),
        }
    }

    /// Render the first `num_rows` rows of this DataFrame as a pretty-printed table.
    pub fn show(&self, num_rows: usize) -> Result<String> {
        krishiv_common::async_util::block_on(self.show_async(num_rows))
    }

    /// Asynchronously render the first `num_rows` rows of this DataFrame.
    ///
    /// Prefer this over [`Self::show`] from async code: `show` executes the
    /// query via `block_on`, which cannot be called from a thread already
    /// driving a Tokio runtime without hopping to a fresh OS thread.
    pub async fn show_async(&self, num_rows: usize) -> Result<String> {
        let batches = {
            let df = self.clone();
            async move {
                if let Some(batches) = &df.pre_collected {
                    return Ok(batches.clone());
                }
                if let Some(ops) = &df.sql_dataframe {
                    return ops.collect().await.map_err(Into::into);
                }
                Err(KrishivError::unsupported(
                    "show requires an executable DataFrame",
                ))
            }
        }
        .await?;
        let display: Vec<_> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(|i| b.slice(i, 1)))
            .take(num_rows)
            .collect();
        let schema = batches.first().map(|b| b.schema());
        let combined = if !display.is_empty() {
            let schema = schema.ok_or_else(|| KrishivError::Runtime {
                message: "display is non-empty but batches has no schema".to_string(),
            })?;
            let arrays: Vec<_> = (0..schema.fields().len())
                .map(|col_idx| {
                    let chunks: Vec<&dyn Array> =
                        display.iter().map(|b| b.column(col_idx).as_ref()).collect();
                    arrow::compute::concat(&chunks).map_err(|e| KrishivError::Runtime {
                        message: format!("concat column {col_idx}: {e}"),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            vec![
                RecordBatch::try_new(schema, arrays).map_err(|e| KrishivError::Runtime {
                    message: format!("reconstruct batch for display: {e}"),
                })?,
            ]
        } else {
            vec![]
        };
        let text = arrow::util::pretty::pretty_format_batches(&combined).map_err(|e| {
            KrishivError::Runtime {
                message: e.to_string(),
            }
        })?;
        Ok(text.to_string())
    }

    /// Return a DataFrame with summary statistics (count, null_count, mean, std, min, max, median).
    pub fn describe(&self) -> Result<DataFrame> {
        krishiv_common::async_util::block_on(self.describe_async())
    }

    /// Asynchronously compute summary statistics (count, null_count, mean, std, min, max, median).
    ///
    /// Prefer this over [`Self::describe`] from async code: `describe` runs a
    /// real aggregate query via `block_on`, which cannot be called from a
    /// thread already driving a Tokio runtime without hopping to a fresh OS
    /// thread.
    pub async fn describe_async(&self) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = df.describe().await?;
                Ok(self.with_new_ops(new_ops))
            }
            None => Err(KrishivError::unsupported(
                "describe requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Start a generic file writer builder.
    pub fn write(&self) -> DataFrameWriter {
        DataFrameWriter::new(self.clone())
    }

    // ── Write API ────────────────────────────────────────────────────────────

    /// Write this DataFrame's result as a directory of Parquet part files
    /// through the distributed sink stage (Phase 2.3 staged commit protocol).
    ///
    /// The query is submitted as a batch SQL job whose terminal task carries an
    /// `object-parquet-sink` output contract. Sink tasks stage their output
    /// under `<path>/_staging/<job_id>/`; the coordinator publishes the staged
    /// files as `part-<task_index>-<job_id>.parquet` (optionally under Hive
    /// `col=value` directories) only when the whole job succeeds, and removes
    /// them on failure — the destination never exposes partial output.
    ///
    /// Requires a SQL-backed DataFrame (`session.sql(..)` / `read_parquet`):
    /// the query text is what the sink job executes remotely.
    /// Submit a distributed staged parquet sink when the runtime supports it.
    ///
    /// Returns `Ok(Some(()))` when the sink job was submitted, `Ok(None)` when
    /// the caller should fall back to a local collect-then-write path, and
    /// `Err` for client-side or runtime failures other than unsupported sink.
    pub(crate) fn try_distributed_parquet_sink(
        &self,
        path: &str,
        mode: krishiv_common::write_commit::WriteMode,
        partition_by: &[String],
    ) -> Result<Option<()>> {
        if !(self.is_locally_evaluated() && self.sql_query.is_some()) {
            return Ok(None);
        }
        // The distributed parquet sink re-plans on a fresh fragment engine that
        // only sees file-backed (shippable) tables. If the query references any
        // table that is not a registered file source — e.g. an in-memory table
        // from `register_record_batches` / `createDataFrame` — the fragment
        // cannot resolve it. Skip the sink so the caller falls back to the
        // client-side collect-then-write path, which always works. A false
        // negative here only forgoes the distributed fast path, never correctness.
        if let Some(query) = self.sql_query.as_deref()
            && let Ok(tables) = krishiv_sql::referenced_table_names(query)
            && !tables
                .iter()
                .all(|table| self.registered_parquet.contains_key(table))
        {
            return Ok(None);
        }
        match self.run_sink_write(path, mode, partition_by)? {
            Ok(()) => Ok(Some(())),
            Err(krishiv_runtime::RuntimeError::Unsupported { feature }) => {
                tracing::warn!(
                    feature,
                    "distributed sink write unsupported by runtime; \
                     falling back to client-side collect-then-write"
                );
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Build the sink contract for `path` and submit the sink job.
    ///
    /// The outer error covers client-side problems (non-SQL DataFrame, bad
    /// destination path, invalid contract); the inner result preserves the
    /// runtime error so callers can detect `RuntimeError::Unsupported` and
    /// fall back to client-side writes.
    fn run_sink_write(
        &self,
        path: &str,
        mode: krishiv_common::write_commit::WriteMode,
        partition_by: &[String],
    ) -> Result<std::result::Result<(), krishiv_runtime::RuntimeError>> {
        use std::path::Path;

        let Some(query) = self.sql_query.as_deref() else {
            return Err(KrishivError::unsupported(
                "sink-based parquet writes require a SQL-backed DataFrame \
                 (created via session.sql() or read_parquet()); collect() and \
                 write locally instead",
            ));
        };
        let dest = Path::new(path);
        let parent = dest
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: format!("sink write destination '{path}' must have a parent directory"),
            })?;
        let dest_name = dest
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: format!("sink write destination '{path}' has no directory name"),
            })?;
        std::fs::create_dir_all(parent).map_err(|e| KrishivError::Runtime {
            message: format!(
                "failed to create sink parent directory '{}': {e}",
                parent.display()
            ),
        })?;

        let spec = krishiv_common::write_commit::SinkWriteSpec::staged(
            parent.to_string_lossy().into_owned(),
            dest_name,
            mode,
            partition_by.to_vec(),
        )
        .map_err(|e| KrishivError::InvalidConfig {
            message: e.to_string(),
        })?;
        let contract = format!("object-parquet-sink:{}", spec.contract_payload());

        let tables = self
            .registered_parquet
            .iter()
            .map(|entry| entry.value().to_registration(entry.key()))
            .collect::<Vec<_>>();
        Ok(self
            .runtime
            .collect_batch_sql_sink(query, &tables, &contract))
    }

    /// Write the result of this DataFrame to Parquet.
    ///
    /// Write the DataFrame to `path` as Parquet with dynamic-partition
    /// overwrite semantics (S18 / `INSERT OVERWRITE TABLE … PARTITION (…)`).
    ///
    /// Each row's partition column values determine the partition directory
    /// the file lands in (e.g. `dt=2026-01-01/part-*.parquet`); only the
    /// partitions touched by the new data are overwritten — other
    /// partitions in the destination are preserved.
    ///
    /// For distributed sessions backed by a SQL query, the write runs
    /// through the distributed sink stage with `WriteMode::OverwriteDynamic`.
    /// Embedded sessions are not yet supported (the API is wired but
    /// returns `KrishivError::Unsupported`); use a SingleNode or Distributed
    /// session for full correctness.
    pub fn write_parquet_overwrite_partition(
        &self,
        path: &str,
        partition_by: &[&str],
    ) -> Result<()> {
        let partition_strings: Vec<String> =
            partition_by.iter().map(|s| (*s).to_string()).collect();
        if let Some(()) = self.try_distributed_parquet_sink(
            path,
            krishiv_common::write_commit::WriteMode::OverwriteDynamic,
            &partition_strings,
        )? {
            return Ok(());
        }
        // Embedded fallback: dynamic-partition overwrite requires the
        // executor-side discovery of the produced partition set. A future
        // release can either implement this in-process or reject at the
        // call site once SingleNode mode grows a sink stage.
        Err(KrishivError::Unsupported {
            feature: "write_parquet_overwrite_partition in embedded mode requires the \
                      distributed sink stage (use a SingleNode or Distributed session with \
                      a SQL-backed DataFrame). See S18 in the Spark parity plan."
                .to_string(),
        })
    }

    /// For distributed sessions backed by a SQL query, the write runs through
    /// the distributed sink stage (`path` becomes a directory of
    /// `part-*.parquet` files committed atomically on job success). Embedded
    /// sessions — and distributed runtimes that do not support sink writes —
    /// collect the result client-side and write a single Parquet file at
    /// `path` (the pre-2.3 behavior).
    pub fn write_parquet(&self, path: &str) -> Result<()> {
        use std::fs::File;
        use std::path::Path;

        if let Some(()) = self.try_distributed_parquet_sink(
            path,
            krishiv_common::write_commit::WriteMode::Append,
            &[],
        )? {
            // A zero-row result publishes no part files through the staged
            // sink, leaving nothing at `path` despite the Ok. Fall through to
            // the local write so success always leaves a (schema-only) file.
            if Path::new(path).exists() {
                return Ok(());
            }
        }

        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        // A zero-batch result must still produce a (schema-only) file — a
        // silent Ok with no file on disk reads as success while writing
        // nothing. The schema comes from the plan when no batch carries it.
        let schema = match batches.first() {
            Some(first) => first.schema(),
            None => self.schema().map_err(|e| KrishivError::Runtime {
                message: format!(
                    "write_parquet: empty result and no schema available to \
                     write a schema-only file at '{path}': {e}"
                ),
            })?,
        };
        let file = File::create(Path::new(path)).map_err(|e| KrishivError::Runtime {
            message: format!("failed to create parquet file '{path}': {e}"),
        })?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).map_err(|e| {
            KrishivError::Runtime {
                message: format!("failed to create parquet writer: {e}"),
            }
        })?;
        for batch in &batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("failed to write parquet batch: {e}"),
            })?;
        }
        writer.close().map_err(|e| KrishivError::Runtime {
            message: format!("failed to close parquet writer: {e}"),
        })?;
        Ok(())
    }

    /// Write the result of this DataFrame to a CSV file.
    pub fn write_csv(&self, path: &str) -> Result<()> {
        use std::fs::File;
        use std::path::Path;

        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        // Even an empty result creates the file (empty), so a successful
        // write_csv always leaves a file at `path`.
        let file = File::create(Path::new(path)).map_err(|e| KrishivError::Runtime {
            message: format!("failed to create csv file '{path}': {e}"),
        })?;
        let mut writer = arrow::csv::Writer::new(file);
        for batch in &batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("failed to write csv batch: {e}"),
            })?;
        }
        let _ = writer.into_inner();
        Ok(())
    }

    /// Write the result of this DataFrame to a JSON file (line-delimited JSON / NDJSON).
    pub fn write_json(&self, path: &str) -> Result<()> {
        use std::fs::File;
        use std::path::Path;

        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        // Even an empty result creates the file (empty NDJSON), so a
        // successful write_json always leaves a file at `path`.
        let file = File::create(Path::new(path)).map_err(|e| KrishivError::Runtime {
            message: format!("failed to create json file '{path}': {e}"),
        })?;
        let mut writer = arrow::json::LineDelimitedWriter::new(file);
        for batch in &batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("failed to write json batch: {e}"),
            })?;
        }
        writer.finish().map_err(|e| KrishivError::Runtime {
            message: format!("failed to finalize json: {e}"),
        })?;
        Ok(())
    }

    // ── Cache / persist / temp-view ─────────────────────────────────────────

    /// Materialise this DataFrame into an in-memory table and return a new
    /// DataFrame backed by that table.
    ///
    /// The returned DataFrame refers to the cached table by name; calling
    /// `unpersist()` on it removes the table from the session.
    pub fn cache(&self) -> Result<DataFrame> {
        self.persist_as(None)
    }

    /// Alias for [`cache`] — Spark-compatible name.
    pub fn persist(&self) -> Result<DataFrame> {
        self.cache()
    }

    /// Materialise this DataFrame into an in-memory table with a given name.
    /// When `name` is `None` a unique name is generated.
    fn persist_as(&self, name: Option<String>) -> Result<DataFrame> {
        let batches = krishiv_common::async_util::block_on(self.collect_async())?.into_batches();

        // Generate a stable unique name from a counter so callers can call
        // cache() multiple times without collisions.
        static CACHE_CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let table_name = name.unwrap_or_else(|| {
            let n = CACHE_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            format!("_krishiv_cache_{n}")
        });

        if let Some(ops) = &self.sql_dataframe {
            // Register with the same SQL engine so subsequent SQL queries
            // referencing this table see the cached data.
            krishiv_common::async_util::block_on(
                ops.register_batches(&table_name, batches.clone()),
            )?;
        }

        // Build a new pre-collected DataFrame that holds the batches so
        // callers without an SQL backend can also use collect() on the result.
        let cached = DataFrame {
            logical_plan: self.logical_plan.clone(),
            sql_dataframe: self.sql_dataframe.clone(),
            sql_query: Some(format!("SELECT * FROM \"{table_name}\"")),
            pre_collected: Some(batches),
            mode: self.mode,
            jobs: self.jobs.clone(),
            next_job_id: self.next_job_id.clone(),
            _coordinator_url: self._coordinator_url.clone(),
            coordinator_http_url: self.coordinator_http_url.clone(),
            // IVM-AUD-API-F3: the parent link does NOT survive caching. It means
            // "this DataFrame reads the parent view's live output", and a cached
            // DataFrame reads a frozen copy under a generated table name
            // instead. Carrying it forward made `s.view(iv).cache()` a handle
            // that still claimed the view-DAG relationship it had just severed.
            ivm_parent: None,
            ivm_registry: self.ivm_registry.clone(),
            remote_prelude: self.remote_prelude.clone(),
            runtime: self.runtime.clone(),
            registered_parquet: self.registered_parquet.clone(),
            force_local: self.force_local,
            _cache_name: Some(table_name),
        };
        Ok(cached)
    }

    /// Drop the in-memory table that was created by [`cache`] / [`persist`].
    ///
    /// A no-op if this DataFrame was not previously cached.
    pub fn unpersist(&self) -> Result<()> {
        let Some(ref table_name) = self._cache_name else {
            return Ok(());
        };
        let Some(ops) = &self.sql_dataframe else {
            return Ok(());
        };
        krishiv_common::async_util::block_on(ops.deregister_table(table_name)).map_err(Into::into)
    }

    /// Register this DataFrame as a temporary view under `name`.
    ///
    /// Subsequent `session.sql("SELECT * FROM <name>")` calls will use this
    /// DataFrame's query as a sub-plan.
    pub fn create_or_replace_temp_view(&self, name: &str) -> Result<()> {
        let Some(ops) = &self.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "create_or_replace_temp_view requires an SQL-backed DataFrame",
            ));
        };
        krishiv_common::async_util::block_on(ops.create_view(name, true)).map_err(Into::into)
    }

    /// Write this DataFrame to a Parquet file with typed writer options.
    pub fn write_parquet_with_options(
        &self,
        path: &str,
        opts: &krishiv_sql::ParquetWriterOptions,
    ) -> Result<()> {
        use std::fs::File;
        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        // Match write_parquet: an empty result still writes a schema-only file.
        let schema = match batches.first() {
            Some(first) => first.schema(),
            None => self.schema().map_err(|e| KrishivError::Runtime {
                message: format!(
                    "write_parquet_with_options: empty result and no schema \
                     available to write a schema-only file at '{path}': {e}"
                ),
            })?,
        };
        let props = build_parquet_writer_props(opts)?;
        let file = File::create(path).map_err(|e| KrishivError::Runtime {
            message: format!("failed to create parquet file '{path}': {e}"),
        })?;
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, props).map_err(|e| {
                KrishivError::Runtime {
                    message: format!("failed to create parquet writer: {e}"),
                }
            })?;
        for batch in &batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("failed to write parquet batch: {e}"),
            })?;
        }
        writer.close().map_err(|e| KrishivError::Runtime {
            message: format!("failed to close parquet writer: {e}"),
        })?;
        Ok(())
    }

    /// Write this DataFrame to a CSV file with typed writer options.
    pub fn write_csv_with_options(
        &self,
        path: &str,
        opts: &krishiv_sql::CsvWriterOptions,
    ) -> Result<()> {
        use std::fs::File;
        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        // Even an empty result creates the file, matching write_csv.
        let file = File::create(path).map_err(|e| KrishivError::Runtime {
            message: format!("failed to create csv file '{path}': {e}"),
        })?;
        let delimiter = opts.delimiter.map(|c| c as u8).unwrap_or(b',');
        let has_header = opts.has_header.unwrap_or(true);
        let builder = arrow::csv::WriterBuilder::new()
            .with_delimiter(delimiter)
            .with_header(has_header);
        let mut writer = builder.build(file);
        for batch in &batches {
            writer.write(batch).map_err(|e| KrishivError::Runtime {
                message: format!("failed to write csv batch: {e}"),
            })?;
        }
        Ok(())
    }

    /// Insert a hash-based exchange node into the logical plan. When backed
    /// by a SQL query, works on the logical plan directly.
    #[must_use]
    pub fn repartition(mut self, num_partitions: u32, key_columns: &[&str]) -> Self {
        // Find terminal nodes (not referenced as inputs by any other node).
        let referenced: std::collections::HashSet<&str> = self
            .logical_plan
            .nodes()
            .iter()
            .flat_map(|n| n.inputs().iter().map(|s| s.as_str()))
            .collect();
        let terminals: Vec<&str> = self
            .logical_plan
            .nodes()
            .iter()
            .filter_map(|n| {
                if !referenced.contains(n.id()) {
                    Some(n.id())
                } else {
                    None
                }
            })
            .collect();

        let exchange_id = format!("repartition-{}", self.logical_plan.nodes().len());
        let exchange = krishiv_plan::PlanNode::new(
            &exchange_id,
            format!("exchange hash({})", key_columns.join(", ")),
            self.logical_plan.kind(),
        )
        .with_inputs(terminals.iter().map(|s| s.to_string()))
        .with_partitioning(krishiv_plan::Partitioning::Hash {
            keys: key_columns.iter().map(|s| s.to_string()).collect(),
            buckets: num_partitions,
        });

        self.logical_plan = self.logical_plan.with_node(exchange);
        self
    }
}

/// Build `parquet::file::properties::WriterProperties` from typed options.
///
/// Returns `None` when all options are default so the caller can pass `None`
/// directly to `ArrowWriter::try_new`, which means "use ArrowWriter defaults".
fn build_parquet_writer_props(
    opts: &krishiv_sql::ParquetWriterOptions,
) -> Result<Option<parquet::file::properties::WriterProperties>> {
    if opts.compression.is_none() && opts.max_row_group_size.is_none() {
        return Ok(None);
    }
    let mut builder = parquet::file::properties::WriterProperties::builder();
    if let Some(ref codec_str) = opts.compression {
        use parquet::basic::Compression;
        let codec = match codec_str.to_lowercase().as_str() {
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(Default::default()),
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD(Default::default()),
            "brotli" => Compression::BROTLI(Default::default()),
            "uncompressed" | "none" => Compression::UNCOMPRESSED,
            other => {
                return Err(KrishivError::InvalidConfig {
                    message: format!("unknown parquet compression codec '{other}'"),
                });
            }
        };
        builder = builder.set_compression(codec);
    }
    builder = builder.set_max_row_group_row_count(opts.max_row_group_size);
    Ok(Some(builder.build()))
}

#[cfg(test)]
mod cache_and_ivm_tests {
    use std::sync::Arc;

    use crate::{FeedableJob as _, Job as _};

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn session_with_orders() -> crate::Session {
        let session = crate::SessionBuilder::new()
            .build()
            .expect("session builds");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["US", "EU"])),
                Arc::new(Int64Array::from(vec![10_i64, 5])),
            ],
        )
        .expect("batch");
        session
            .register_record_batches("orders", vec![batch])
            .expect("register");
        session
    }

    /// IVM-AUD-API-F2. `df.cache().to_incremental()` used to return a handle
    /// that looked like an incremental view over the cached data and was not:
    /// an incremental view is populated by feeds, never by a session table, so
    /// the rows `cache()` had just materialized were nowhere in it. This
    /// DataFrame keeps a SQL-ops backing, so the view it produced silently read
    /// `orders` — the pre-cache query, cache() a no-op — which is what the
    /// panic arm below reports; a DataFrame without that backing instead got a
    /// view whose only feedable source was the generated `_krishiv_cache_N`.
    /// Both are the same defect, and the success was the bug.
    #[tokio::test]
    async fn to_incremental_on_a_cached_dataframe_is_refused() {
        let session = session_with_orders();
        let cached = session
            .sql("SELECT region, amount FROM orders")
            .expect("sql")
            .cache()
            .expect("cache");
        let err = match cached.to_incremental("revenue").await {
            Ok(iv) => panic!(
                "a cached DataFrame must not become an incremental view; got sources {:?}",
                iv.source_names()
            ),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("_krishiv_cache_"),
            "the error must name the generated cache table that would have been the only \
             feedable source: {err}"
        );
    }

    /// IVM-AUD-INT-F15 / API-D5. `to_incremental` built a fresh private
    /// registry per call, so the view it registered was invisible to
    /// `Session::ivm(name)` (which made a *second*, empty job under the same
    /// name in the session registry) and `Session::reset_ivm_job(name)` had
    /// nothing to drop. Two `to_incremental("x")` calls in one session likewise
    /// produced two isolated views.
    #[tokio::test]
    async fn to_incremental_registers_into_the_session_registry() {
        use krishiv_delta::DeltaBatch;

        let session = session_with_orders();
        let iv = session
            .sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region")
            .expect("sql")
            .to_incremental("revenue")
            .await
            .expect("to_incremental");

        // The session reaches the SAME job by name — not a second empty one.
        let via_session = session.ivm("revenue").await.expect("session.ivm");
        assert_eq!(via_session.job_id(), "revenue");

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["US"])),
                Arc::new(Int64Array::from(vec![7_i64])),
            ],
        )
        .expect("batch");
        iv.apply(None, &DeltaBatch::from_inserts(batch).expect("delta"))
            .await
            .expect("apply");
        iv.step().await.expect("step");

        let snapshot = via_session
            .snapshot("revenue")
            .await
            .expect("the session handle must know this view");
        assert_eq!(
            snapshot.map(|b| b.num_rows()),
            Some(1),
            "a feed through the IncrementalDataFrame must be visible through Session::ivm"
        );

        // …and the session can drop it, which it could not do to a private one.
        assert!(
            session.reset_ivm_job("revenue"),
            "reset_ivm_job must find the job to_incremental created"
        );
    }

    /// IVM-AUD-API-D2. There was no Rust view-DAG entry point: `with_ivm_parent`
    /// is `pub` but tags a DataFrame that has nothing to plan against, so a
    /// Rust user had to hand-register a table named after the base view, with
    /// the base view's declared schema, before the derived SQL would resolve —
    /// undocumented, and easy to get subtly wrong. `Session::view` does both
    /// halves and drops the registration again.
    #[tokio::test]
    async fn session_view_plans_a_derived_view_against_the_base() {
        use krishiv_delta::DeltaBatch;
        let session = session_with_orders();
        let base = session
            .sql("SELECT region, SUM(amount) AS total FROM orders GROUP BY region")
            .expect("sql")
            .to_incremental("revenue")
            .await
            .expect("to_incremental");

        let derived = session
            .view(&base)
            .expect("the base view must be readable as a DataFrame")
            .to_incremental("big_regions")
            .await
            .expect("the derived view must co-register on the base's job");

        // Co-registered onto the SAME job — that is what makes the cascade work.
        assert_eq!(derived.shared_job().job_id(), base.shared_job().job_id());
        assert_eq!(derived.source_names(), ["revenue"]);

        // The registration must not outlive the call.
        assert!(
            !session.table_exists("revenue").expect("table_exists"),
            "the base view's rows were registered only for planning"
        );

        // One feed, one tick: the derived view carries the base's output.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["US", "EU"])),
                Arc::new(Int64Array::from(vec![10_i64, 5])),
            ],
        )
        .expect("batch");
        base.apply(
            Some("orders"),
            &DeltaBatch::from_inserts(batch).expect("delta"),
        )
        .await
        .expect("apply");
        let report = base.step().await.expect("step");
        assert!(
            report.errored_views.is_empty(),
            "the derived view must plan against its base: {:?}",
            report.errored_views
        );
        assert_eq!(
            derived
                .snapshot()
                .await
                .expect("snapshot")
                .map(|b| b.num_rows()),
            Some(2)
        );
    }

    /// A real table under the view's name is refused, not replaced — the
    /// derived view's SQL could not tell the two apart.
    #[tokio::test]
    async fn session_view_refuses_to_shadow_an_existing_table() {
        let session = session_with_orders();
        let iv = session
            .sql("SELECT region FROM orders")
            .expect("sql")
            .to_incremental("orders_view")
            .await
            .expect("to_incremental");
        session
            .register_record_batches(
                "orders_view",
                vec![
                    RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
                        vec![Arc::new(Int64Array::from(vec![1_i64]))],
                    )
                    .expect("batch"),
                ],
            )
            .expect("register");

        let err = match session.view(&iv) {
            Ok(_) => panic!("a real table of the same name must not be shadowed"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("orders_view"),
            "must name the collision: {err}"
        );
        assert!(
            session.table_exists("orders_view").expect("table_exists"),
            "the real table must survive the refusal"
        );
    }

    /// IVM-AUD-API-F3. `ivm_parent` means "this DataFrame reads that view's
    /// live output"; a cached DataFrame reads a frozen copy under a generated
    /// table name, so the link must not survive `cache()`.
    #[tokio::test]
    async fn cache_drops_the_ivm_parent_link() {
        let session = session_with_orders();
        let registry: krishiv_runtime::SharedIvmJobRegistry =
            Arc::new(krishiv_runtime::IvmJobRegistry::with_default_shards(1));
        let parent = crate::IvmJob::embedded(&registry, "base").expect("embedded job");

        let live = session
            .sql("SELECT region, amount FROM orders")
            .expect("sql")
            .with_ivm_parent(parent);
        assert!(
            live.ivm_parent.is_some(),
            "precondition: the uncached DataFrame carries the parent link"
        );

        let cached = live.cache().expect("cache");
        assert!(
            cached.ivm_parent.is_none(),
            "cache() froze the rows into '{:?}', so it can no longer claim to read the \
             parent view's live output",
            cached._cache_name
        );
    }
}
