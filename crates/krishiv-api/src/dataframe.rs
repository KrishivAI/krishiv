use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use krishiv_plan::{ExecutionKind, LogicalPlan, PhysicalPlan};
use krishiv_runtime::{
    BatchTableRegistration, ExecutionRuntime, JobId, JobState, JobStatus, LocalJobRegistry,
};
use krishiv_sql::KrishivDataFrameOps;

use crate::error::{KrishivError, Result};
use crate::expression::Expr;
use crate::io::DataFrameWriter;
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
    runtime: Arc<dyn ExecutionRuntime>,
    registered_parquet: Arc<DashMap<String, PathBuf>>,
    /// When true, always collect from the local DataFusion plan even in remote
    /// mode. Set for lakehouse reads (Delta, Hudi) whose table registrations
    /// live only in the local DataFusion context and cannot be forwarded to a
    /// remote executor.
    force_local: bool,
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
        let Some(ops) = &self.dataframe.sql_dataframe else {
            return Err(KrishivError::unsupported(
                "grouped aggregation requires an SQL-backed DataFrame",
            ));
        };
        let groups = self
            .group_exprs
            .iter()
            .map(Expr::as_sql)
            .collect::<Vec<_>>();
        let aggregates = aggregate_exprs.iter().map(Expr::as_sql).collect::<Vec<_>>();
        let new_ops = krishiv_common::async_util::block_on(ops.aggregate(&groups, &aggregates))?;
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
            runtime: crate::session::shared_embedded_runtime()?,
            registered_parquet: Arc::new(DashMap::new()),
            force_local: false,
        })
    }

    /// Force collection from the local DataFusion plan regardless of runtime mode.
    pub(crate) fn with_force_local(mut self) -> Self {
        self.force_local = true;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_sql_dataframe(
        mode: ExecutionMode,
        sql_dataframe: impl KrishivDataFrameOps + 'static,
        sql_query: Option<String>,
        jobs: Arc<Mutex<LocalJobRegistry>>,
        next_job_id: Arc<AtomicU64>,
        coordinator_url: Option<String>,
        runtime: Arc<dyn ExecutionRuntime>,
        registered_parquet: Arc<DashMap<String, PathBuf>>,
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
            runtime,
            registered_parquet,
            force_local: false,
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
        registered_parquet: Arc<DashMap<String, PathBuf>>,
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
            runtime,
            registered_parquet,
            force_local: false,
        }
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
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
        if self.runtime.uses_remote_execution() && !self.force_local {
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

    /// Collect results.
    pub fn collect(&self) -> Result<QueryResult> {
        krishiv_common::async_util::block_on(self.collect_async())
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

        let uses_remote = self.runtime.uses_remote_execution() && !self.force_local;

        let result = if uses_remote {
            if let Some(query) = self.sql_query.as_deref() {
                let tables = self
                    .registered_parquet
                    .iter()
                    .map(|entry| {
                        BatchTableRegistration::new(entry.key().clone(), entry.value().clone())
                    })
                    .collect::<Vec<_>>();
                crate::session::runtime_collect_batch_sql(
                    Arc::clone(&self.runtime),
                    query,
                    &tables,
                    false,
                )
                .await
                .map(QueryResult::new)
            } else {
                Err(KrishivError::unsupported(
                    "remote execution requires a SQL query",
                ))
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

        let uses_remote = self.runtime.uses_remote_execution() && !self.force_local;

        let result = if uses_remote {
            if let Some(query) = self.sql_query.as_deref() {
                let tables = self
                    .registered_parquet
                    .iter()
                    .map(|entry| {
                        BatchTableRegistration::new(entry.key().clone(), entry.value().clone())
                    })
                    .collect::<Vec<_>>();
                let batches = crate::session::runtime_collect_batch_sql(
                    Arc::clone(&self.runtime),
                    query,
                    &tables,
                    false,
                )
                .await?;
                let stream = futures::stream::iter(batches.into_iter().map(Ok));
                Ok(Box::pin(stream) as crate::streaming_dataframe::KrishivStream)
            } else {
                Err(KrishivError::unsupported(
                    "remote execution requires a SQL query",
                ))
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
            dataframe.execute_stream().await.map_err(Into::into)
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

    fn start_job(&self, name: &str) -> JobId {
        let id = JobId::try_new(format!(
            "local-{}",
            self.next_job_id.fetch_add(1, Ordering::SeqCst)
        ))
        .expect("job id is always non-empty");
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
            runtime: self.runtime.clone(),
            registered_parquet: self.registered_parquet.clone(),
            force_local: self.force_local,
        }
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
            runtime: self.runtime.clone(),
            registered_parquet: self.registered_parquet.clone(),
            force_local: self.force_local,
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

    /// Select columns by name.
    pub fn select(&self, columns: &[&str]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.select(columns))?;
                Ok(self.with_new_ops(new_ops))
            }
            None => {
                let msg = if self.pre_collected.is_some() {
                    "select on pre-collected DataFrame is not yet supported; collect() first"
                } else {
                    "select requires an SQL-backed DataFrame"
                };
                Err(KrishivError::unsupported(msg))
            }
        }
    }

    /// Select typed expressions.
    pub fn select_exprs(&self, expressions: &[Expr]) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let expressions = expressions.iter().map(Expr::as_sql).collect::<Vec<_>>();
                let new_ops = krishiv_common::async_util::block_on(df.select_exprs(&expressions))?;
                Ok(self.with_new_ops(new_ops))
            }
            None => Err(KrishivError::unsupported(
                "select_exprs requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Filter rows using a typed expression.
    pub fn filter_expr(&self, predicate: Expr) -> Result<DataFrame> {
        self.filter(predicate.as_sql())
    }

    /// Group rows by typed expressions.
    pub fn group_by(&self, expressions: &[Expr]) -> GroupedDataFrame {
        GroupedDataFrame {
            dataframe: self.clone(),
            group_exprs: expressions.to_vec(),
        }
    }

    /// Filter rows by a SQL predicate expression.
    pub fn filter(&self, predicate: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.filter(predicate))?;
                Ok(self.with_new_ops(new_ops))
            }
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
            None => Err(KrishivError::unsupported(
                "limit requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.distinct())?;
                Ok(self.with_new_ops(new_ops))
            }
            None => Err(KrishivError::unsupported(
                "distinct requires an SQL-backed DataFrame",
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
            None => Err(KrishivError::unsupported(
                "rename requires an SQL-backed DataFrame",
            )),
        }
    }

    /// Add or replace a column with a computed expression (SQL-based).
    pub fn with_column(&self, name: &str, expr: &str) -> Result<DataFrame> {
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.with_column(name, expr))?;
                Ok(self.with_new_ops(new_ops))
            }
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

    /// Print the first `num_rows` rows of this DataFrame to stdout.
    pub fn show(&self, num_rows: usize) -> Result<String> {
        let batches = krishiv_common::async_util::block_on({
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
        })?;
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
        match &self.sql_dataframe {
            Some(df) => {
                let new_ops = krishiv_common::async_util::block_on(df.describe())?;
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

    /// Write the result of this DataFrame to a Parquet file.
    pub fn write_parquet(&self, path: &str) -> Result<()> {
        use std::fs::File;
        use std::path::Path;

        let result = krishiv_common::async_util::block_on(self.collect_async())?;
        let batches = result.into_batches();
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
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
        if batches.is_empty() {
            return Ok(());
        }
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
        if batches.is_empty() {
            return Ok(());
        }
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
