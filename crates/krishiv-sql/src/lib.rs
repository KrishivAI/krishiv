#![forbid(unsafe_code)]

//! SQL planning and local execution seam for Krishiv.
//!
//! This crate owns the DataFusion integration for R1 while keeping DataFusion
//! out of the long-term public API exposed by `krishiv-api`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::dataframe::DataFrame as DataFusionDataFrame;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};
use krishiv_catalog::{InMemoryCatalog, datafusion_bridge::DataFusionCatalogBridge};

use krishiv_optimizer::{CostModel, Optimizer};
use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

pub mod cep_sql;
pub mod create_function_ddl;
mod lakehouse;
pub mod live_table;
pub mod policy;
pub mod spark_compat;
pub mod spark_compat_date;
pub mod streaming;
mod udf;
mod window_functions;

pub use cep_sql::{
    MatchRecognizeStatement, execute_streaming_match_recognize, parse_match_recognize,
};
pub use lakehouse::{AsOfTableRef, MergeResult, MergeTargetUnsupportedError, preprocess_as_of_sql};
pub use policy::PolicyEnforcingSqlEngine;

/// SQL result alias.
pub type SqlResult<T> = Result<T, SqlError>;

// ── Plan cache (single-lock, race-free) ──────────────────────────────────────

/// Whether the [`SqlEngine`] internal builder should attempt to register the
/// helper window UDFs (`tumble_start` / `tumble_end` / `hop_start` / `hop_end`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowFnRegistration {
    /// Call `window_functions::register_window_functions`; propagate any error.
    Register,
    /// Skip registration entirely; infallible. Used as a fallback by
    /// [`SqlEngine::new`] when `Register` fails so the engine is still usable
    /// for non-window queries.
    Skip,
}

/// Bounded query-plan cache keyed by query text.
///
/// A single `Mutex<PlanCache>` replaces the previous two-structure approach
/// (`DashMap` + `Mutex<VecDeque>`) which had a TOCTOU race: two threads could
/// both see `len() < MAX` and both insert, growing the cache past the limit.
struct PlanCache {
    map: HashMap<String, datafusion::logical_expr::LogicalPlan>,
    order: VecDeque<String>,
    max: usize,
}

impl PlanCache {
    fn new(max: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            max,
        }
    }

    fn get(&self, key: &str) -> Option<&datafusion::logical_expr::LogicalPlan> {
        self.map.get(key)
    }

    fn insert(&mut self, key: String, plan: datafusion::logical_expr::LogicalPlan) {
        if self.map.len() >= self.max {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, plan);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// SQL-layer errors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SqlError {
    /// Query was empty or whitespace only.
    #[error("SQL query is empty")]
    EmptyQuery,
    /// A table name was empty.
    #[error("table name is empty")]
    EmptyTableName,
    /// The requested SQL feature is not available in R1.
    #[error("unsupported SQL feature: {feature}")]
    Unsupported { feature: String },
    /// DataFusion returned an error.
    #[error("DataFusion error: {message}")]
    DataFusion { message: String },
    /// Access denied by auth or policy check.
    #[error("access denied: {reason}")]
    AccessDenied { reason: String },
}

impl From<datafusion::error::DataFusionError> for SqlError {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion {
            message: value.to_string(),
        }
    }
}

/// SQL planning output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPlan {
    query: String,
    logical_plan: LogicalPlan,
}

impl SqlPlan {
    /// Original query.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Krishiv logical plan wrapper.
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
}

/// Local SQL engine backed by DataFusion.
///
/// **Local-only**: All SQL execution is in-process via DataFusion. No distributed SQL
/// execution path is available in this crate.
/// This crate is scoped to R1 — DataFusion will be abstracted behind
/// the `KrishivDataFrameOps` trait in future releases.
///
/// Methods like `register_parquet`, `read_delta`, and `read_hudi` treat
/// path arguments as local filesystem paths. S3/GCS paths require the
/// object-store connector layer.
/// Maximum number of query plans stored in the plan cache before random eviction.
const PLAN_CACHE_MAX_ENTRIES: usize = 256;

fn resolve_plan_cache_max_entries() -> usize {
    std::env::var("KRISHIV_PLAN_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(PLAN_CACHE_MAX_ENTRIES)
}
const STREAMING_CEP_MAX_ROWS_DEFAULT: usize = 100_000;

/// Resolve the streaming MATCH_RECOGNIZE row cap from a raw env var value.
/// `None` and unparseable values fall back to the documented default of
/// 100_000. Zero is rejected because it would mean "scan zero rows".
pub fn resolve_streaming_match_recognize_limit(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(STREAMING_CEP_MAX_ROWS_DEFAULT)
}

/// Resolve the streaming MATCH_RECOGNIZE row cap from the
/// `KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT` environment variable.
pub fn streaming_match_recognize_limit_from_env() -> usize {
    resolve_streaming_match_recognize_limit(
        std::env::var("KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT")
            .ok()
            .as_deref(),
    )
}

/// Build the DataFusion session config.
///
/// Embedded mode (`parallelism=1`): queries run against small in-process datasets;
/// the extra thread-spawn overhead of multi-partition execution outweighs the gain.
/// Single-node daemon mode should override with `available_parallelism()` after
/// construction if it expects large parquet scans.
fn build_single_node_session_config() -> datafusion::prelude::SessionConfig {
    let mut config = datafusion::prelude::SessionConfig::new()
        .with_target_partitions(1)
        .set_bool("datafusion.optimizer.enable_round_robin_repartition", false);
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    config
}

#[derive(Clone)]
pub struct SqlEngine {
    context: SessionContext,
    krishiv_catalog: Option<Arc<RwLock<InMemoryCatalog>>>,
    view_registry: Option<std::sync::Arc<std::sync::Mutex<MaterializedViewRegistry>>>,
    udf_registry: Option<std::sync::Arc<std::sync::RwLock<krishiv_udf::UdfRegistry>>>,
    /// Table names registered as unbounded streaming sources.
    /// Wrapped in `Arc<RwLock<>>` so that Session clones share the same set.
    streaming_sources: Arc<RwLock<std::collections::HashSet<String>>>,
    /// `true` once any streaming source has been registered.  Checked with a
    /// relaxed atomic load before acquiring `streaming_sources` so that the
    /// common case (no streaming sources, pure batch workload) avoids both the
    /// lock and the SQL parse inside `is_streaming_query`.
    has_streaming_sources: Arc<AtomicBool>,
    /// Optional UDF resource limits to apply when syncing UDFs for this engine.
    /// Set for job-specific engines so sandbox enforcement uses the job's budgets.
    udf_limits: Option<krishiv_udf::ResourceLimits>,
    /// Monotonically increasing version counter; incremented on every UDF
    /// registration or removal. Used to skip `sync_all_udfs()` when nothing
    /// has changed since the last sync.
    udf_registry_version: Arc<AtomicU64>,
    /// The version at which the last `sync_all_udfs()` was performed.
    /// Compared against `udf_registry_version` to detect staleness.
    udf_last_synced_version: Arc<AtomicU64>,
    /// Bounded query plan cache: query text → DataFusion LogicalPlan.
    /// Skips re-parsing and re-optimising identical repeated queries.
    /// Max `PLAN_CACHE_MAX_ENTRIES` entries; oldest entry evicted when full.
    /// Single-lock design prevents the TOCTOU race of the previous two-structure
    /// (`DashMap` + `VecDeque`) implementation.
    plan_cache: Arc<Mutex<PlanCache>>,
}

impl fmt::Debug for SqlEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlEngine")
            .field("backend", &"datafusion")
            .finish_non_exhaustive()
    }
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Create a local SQL engine.
    ///
    /// Window helper UDFs (`tumble_start`, `tumble_end`, `hop_start`, `hop_end`)
    /// are registered as part of construction. If registration fails the
    /// engine is still returned — non-window queries work — and a
    /// `tracing::warn!` is emitted. Use [`SqlEngine::try_new`] when callers
    /// need to surface the registration error.
    pub fn new() -> Self {
        match Self::build_local(None, WindowFnRegistration::Register) {
            Ok(engine) => engine,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "SqlEngine::new: window helper UDF registration failed; \
                     window SQL functions will be unavailable, other queries are unaffected"
                );
                Self::build_local(None, WindowFnRegistration::Skip)
                    .expect("SqlEngine::build_local with WindowFnRegistration::Skip is infallible")
            }
        }
    }

    /// Create a local SQL engine, propagating window helper registration errors.
    ///
    /// Callers that need to abort startup when window functions cannot be
    /// registered should use this constructor.
    pub fn try_new() -> SqlResult<Self> {
        Self::build_local(None, WindowFnRegistration::Register)
    }

    /// Create an engine whose `krishiv` catalog resolves tables registered in `InMemoryCatalog` (P0-10).
    pub fn with_in_memory_catalog(catalog: Arc<RwLock<InMemoryCatalog>>) -> SqlResult<Self> {
        Self::build_local(Some(catalog), WindowFnRegistration::Register)
    }

    /// Internal builder shared by the public constructors.
    ///
    /// `krishiv_catalog` is `Some(...)` when the engine should bridge to an
    /// `InMemoryCatalog`; `None` for a default engine.
    ///
    /// `window_fn_registration` controls whether the helper UDFs
    /// (`tumble_start` / `tumble_end` / `hop_start` / `hop_end`) are
    /// registered. `Skip` is used as a fallback by [`SqlEngine::new`] when
    /// `Register` fails; it is infallible.
    fn build_local(
        krishiv_catalog: Option<Arc<RwLock<InMemoryCatalog>>>,
        window_fn_registration: WindowFnRegistration,
    ) -> SqlResult<Self> {
        // Create streaming_sources first so it can be shared with KafkaTableFactory.
        // DDL-created Kafka tables (CREATE EXTERNAL TABLE … STORED AS KAFKA) then
        // correctly register in is_streaming_query.
        let streaming_sources: Arc<RwLock<std::collections::HashSet<String>>> =
            Arc::new(RwLock::new(std::collections::HashSet::new()));

        let dummy_state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .build();
        let mut table_factories = dummy_state.table_factories().clone();
        table_factories.insert(
            "KAFKA".to_string(),
            Arc::new(crate::kafka_table::KafkaTableFactory {
                streaming_sources: streaming_sources.clone(),
            }),
        );
        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_config(build_single_node_session_config())
            .with_table_factories(table_factories)
            .build();
        let context = SessionContext::new_with_state(state);
        if let Some(catalog) = &krishiv_catalog {
            context.register_catalog(
                "krishiv",
                Arc::new(DataFusionCatalogBridge::new(catalog.clone())),
            );
        }
        if matches!(window_fn_registration, WindowFnRegistration::Register) {
            window_functions::register_window_functions(&context).map_err(|e| {
                SqlError::DataFusion {
                    message: format!("failed to register window helper UDFs: {e}"),
                }
            })?;
        }
        Ok(Self {
            context,
            krishiv_catalog,
            view_registry: None,
            udf_registry: None,
            streaming_sources,
            has_streaming_sources: Arc::new(AtomicBool::new(false)),
            udf_limits: None,
            udf_registry_version: Arc::new(AtomicU64::new(0)),
            udf_last_synced_version: Arc::new(AtomicU64::new(u64::MAX)),
            plan_cache: Arc::new(Mutex::new(PlanCache::new(resolve_plan_cache_max_entries()))),
        })
    }

    /// Mark a table name as a bounded streaming source, returning a sender to push batches into it.
    ///
    /// The returned sender is a `tokio::sync::mpsc::Sender` with capacity
    /// [`crate::streaming::CONTINUOUS_TABLE_CHANNEL_CAPACITY`]. When the
    /// consumer (the DataFusion query plan) is slower than the producer,
    /// `Sender::send(...).await` will backpressure (block) the producer,
    /// and `Sender::try_send(...)` will return `TrySendError::Full`
    /// rather than growing memory without limit. Use
    /// [`register_streaming_table_with_capacity`] for a non-default
    /// capacity.
    pub fn register_streaming_table(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> SqlResult<tokio::sync::mpsc::Sender<RecordBatch>> {
        let (table, tx) = crate::streaming::create_continuous_table(schema).map_err(|e| {
            SqlError::DataFusion {
                message: e.to_string(),
            }
        })?;
        self.context
            .register_table(name, table)
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string());
        self.has_streaming_sources.store(true, Ordering::Release);
        self.invalidate_plan_cache();
        Ok(tx)
    }

    /// Same as [`Self::register_streaming_table`] but with a caller-supplied
    /// channel capacity. Useful for tests that want to exercise the
    /// full/empty channel boundary without pushing
    /// `CONTINUOUS_TABLE_CHANNEL_CAPACITY` (64) batches.
    pub fn register_streaming_table_with_capacity(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
        capacity: usize,
    ) -> SqlResult<tokio::sync::mpsc::Sender<RecordBatch>> {
        let (table, tx) = crate::streaming::create_continuous_table_with_capacity(schema, capacity)
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        self.context
            .register_table(name, table)
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string());
        self.has_streaming_sources.store(true, Ordering::Release);
        self.invalidate_plan_cache();
        Ok(tx)
    }

    /// Register a live Kafka/Redpanda topic as an unbounded streaming table.
    ///
    /// This is the native Rust path — no Python bridge or external process
    /// required.  Under the hood it creates an `rdkafka` consumer and wraps it
    /// in a DataFusion `StreamingTable` so normal SQL queries (`SELECT`,
    /// `GROUP BY`, windowed aggregations) work against the live topic.
    ///
    /// Equivalent SQL DDL:
    /// ```sql
    /// CREATE EXTERNAL TABLE <name> (<cols>) STORED AS KAFKA
    ///   LOCATION '<topic>'
    ///   OPTIONS ('bootstrap.servers' = '…', 'group.id' = '…');
    /// ```
    pub fn register_kafka_source(
        &self,
        table_name: impl AsRef<str>,
        schema: arrow::datatypes::SchemaRef,
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> SqlResult<()> {
        let table_name = table_name.as_ref();
        if table_name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        let config = krishiv_connectors::kafka::KafkaConfig {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            // Enable at-least-once delivery for the streaming SQL path.
            auto_commit_interval_ms: Some(1_000),
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanisms: None,
            enable_idempotence: None,
            transactional_id: None,
        };
        let table =
            crate::kafka_table::create_kafka_streaming_table(schema, config).map_err(|e| {
                SqlError::DataFusion {
                    message: e.to_string(),
                }
            })?;
        if self
            .context
            .table_exist(table_name)
            .map_err(SqlError::from)?
        {
            let _ = self
                .context
                .deregister_table(table_name)
                .map_err(SqlError::from)?;
        }
        self.context
            .register_table(table_name, table)
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_name.to_string());
        self.has_streaming_sources.store(true, Ordering::Release);
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Execute a SQL query and write every result row to a Kafka/Redpanda topic.
    ///
    /// Each row is serialised as a JSON object using the same format as
    /// [`KafkaSink`].  The method blocks until the query stream ends and the
    /// producer queue is flushed, then returns the total number of rows written.
    ///
    /// **Note**: If `sql` targets an unbounded streaming table (e.g. one
    /// registered via [`register_kafka_source`]) this call will never return.
    /// Use it with batch sources or add a `LIMIT` clause.
    pub async fn sql_to_kafka(
        &self,
        sql: impl AsRef<str>,
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
    ) -> SqlResult<u64> {
        use futures::StreamExt;
        use krishiv_connectors::Sink as _;
        use krishiv_connectors::kafka::{KafkaConfig, KafkaSink};

        let config = KafkaConfig {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            group_id: "krishiv-sql-writer".into(),
            auto_commit_interval_ms: None,
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanisms: None,
            enable_idempotence: None,
            transactional_id: None,
        };
        let mut sink = KafkaSink::new(config).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;

        let df = self.sql(sql.as_ref()).await?;
        let mut stream = df.execute_stream().await?;
        let mut total_rows = 0u64;

        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| SqlError::DataFusion { message: e })?;
            if batch.num_rows() > 0 {
                total_rows += batch.num_rows() as u64;
                sink.write_batch(batch)
                    .await
                    .map_err(|e| SqlError::DataFusion {
                        message: e.to_string(),
                    })?;
            }
        }
        sink.flush().await.map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        Ok(total_rows)
    }

    /// Configure this engine with explicit UDF resource limits (Track E).
    /// When set, calls to `sql()` and direct UDF syncs will use these budgets
    /// instead of unlimited defaults. Intended for job-specific engines.
    pub fn with_udf_limits(mut self, limits: krishiv_udf::ResourceLimits) -> Self {
        self.udf_limits = Some(limits);
        self
    }

    /// Returns `true` if `table_name` is registered as an unbounded streaming source.
    pub fn is_streaming_source(&self, table_name: &str) -> bool {
        self.streaming_sources
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(table_name)
    }

    /// Register a table name as a streaming source without creating a live connector.
    ///
    /// This is the test-safe alternative to [`register_kafka_source`]: it marks
    /// `table_name` in the `streaming_sources` set so that `is_streaming_query`
    /// returns `true` for queries that reference it, without constructing any
    /// broker connection. Useful for unit tests where a live Kafka broker is not
    /// available and rdkafka's log subsystem is not initialised.
    /// Returns [`SqlError::EmptyTableName`] if `table_name` is blank.
    pub fn register_streaming_source_name(&self, table_name: impl Into<String>) -> SqlResult<()> {
        let name: String = table_name.into();
        if name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name);
        self.has_streaming_sources.store(true, Ordering::Release);
        Ok(())
    }

    /// Remove a streaming source registration.
    ///
    /// Deregisters the table from DataFusion and removes it from the streaming-
    /// sources set. Invalidates the plan cache. Idempotent — deregistering a
    /// name that was never registered is not an error.
    pub fn deregister_streaming_source(&self, name: &str) -> SqlResult<()> {
        if name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        // Idempotent: ignore the Option return (None when table wasn't registered).
        let _ = self
            .context
            .deregister_table(name)
            .map_err(SqlError::from)?;
        {
            let mut sources = self
                .streaming_sources
                .write()
                .unwrap_or_else(|e| e.into_inner());
            sources.remove(name);
            if sources.is_empty() {
                self.has_streaming_sources.store(false, Ordering::Release);
            }
            // Invalidate while still holding the write lock so there is no window
            // between source removal and cache invalidation where a concurrent
            // is_streaming_query returns false but serves a stale cached plan (N5).
            self.invalidate_plan_cache();
        }
        Ok(())
    }

    /// Register a table UDF backed by a Rust closure.
    ///
    /// The closure receives the arguments passed by the SQL caller (as
    /// `ScalarValue` literals; non-literal args are `ScalarValue::Null`) and
    /// returns an Arrow `RecordBatch`.  `schema` describes the output columns.
    ///
    /// # Example
    /// ```ignore
    /// engine.register_table_udf_fn(
    ///     "generate_ints",
    ///     Schema::new(vec![Field::new("n", DataType::Int64, false)]),
    ///     |args| {
    ///         let count = match args.first() {
    ///             Some(ScalarValue::Int32(n)) => *n as i64,
    ///             _ => 10,
    ///         };
    ///         let arr = Int64Array::from((0..count).collect::<Vec<_>>());
    ///         Ok(RecordBatch::try_from_iter([("n", Arc::new(arr) as _)])?)
    ///     },
    /// )?;
    /// ```
    pub fn register_table_udf_fn(
        &self,
        name: impl Into<String>,
        schema: arrow::datatypes::Schema,
        f: impl Fn(
            &[krishiv_udf::ScalarValue],
        ) -> Result<arrow::record_batch::RecordBatch, krishiv_udf::UdfError>
        + Send
        + Sync
        + 'static,
    ) -> SqlResult<()> {
        let stub =
            create_function_ddl::StubTableUdf::from_ddl(&create_function_ddl::CreateFunctionDdl {
                function_name: name.into(),
                return_columns: schema
                    .fields()
                    .iter()
                    .map(|f| create_function_ddl::ColumnDef {
                        name: f.name().clone(),
                        data_type: f.data_type().clone(),
                    })
                    .collect(),
                language: None,
                body: None,
            })
            .with_body_fn(std::sync::Arc::new(f));
        if let Some(registry) = &self.udf_registry {
            let mut guard = registry.write().map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
            guard.register_table(std::sync::Arc::new(stub.clone()));
        }
        udf::register_single_table_udf(&self.context, std::sync::Arc::new(stub))
            .map_err(SqlError::from)
    }

    /// Returns `true` if any table referenced in `sql` is a registered streaming source.
    pub fn is_streaming_query(&self, sql: &str) -> SqlResult<bool> {
        // Fast-path: avoid the RwLock acquire and SQL parse for the common case
        // where no streaming sources have ever been registered (pure batch engines).
        if !self.has_streaming_sources.load(Ordering::Acquire) {
            return Ok(false);
        }
        let sources = self
            .streaming_sources
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if sources.is_empty() {
            return Ok(false);
        }
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        for stmt in &statements {
            let mut is_streaming = false;
            let _ = visit_relations(stmt, |relation| {
                // relation.to_string() yields the fully-qualified name (e.g. "schema.table").
                // Extract the unqualified table name (last segment after dot).
                let full = relation.to_string();
                let table_name = full.split('.').next_back().unwrap_or(&full);
                if sources.contains(table_name) {
                    is_streaming = true;
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            });
            if is_streaming {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Shared Krishiv catalog backing this engine, if configured.
    pub fn krishiv_catalog(&self) -> Option<&Arc<RwLock<InMemoryCatalog>>> {
        self.krishiv_catalog.as_ref()
    }

    /// Share a session UDF registry so scalar UDFs are visible in SQL.
    #[must_use]
    pub fn with_udf_registry(
        mut self,
        registry: std::sync::Arc<std::sync::RwLock<krishiv_udf::UdfRegistry>>,
    ) -> Self {
        self.udf_registry = Some(registry);
        // Mark UDFs as dirty so the first sql() call syncs them.
        self.bump_udf_version();
        self
    }

    /// Increment the UDF version counter to signal that `sync_all_udfs()` is
    /// needed on the next `sql()` call.
    pub(crate) fn bump_udf_version(&self) {
        self.udf_registry_version.fetch_add(1, Ordering::Release);
    }

    /// Invalidate the plan cache after any schema change. Call this whenever a
    /// table is registered, replaced, or deregistered. Full invalidation is
    /// simpler and safer than per-table tracking: the cache refills quickly on
    /// the next few queries.
    fn invalidate_plan_cache(&self) {
        match self.plan_cache.lock() {
            Ok(mut cache) => cache.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    /// Expose cache invalidation for tests and external callers that register
    /// tables through a different path.
    pub fn clear_plan_cache(&self) {
        self.invalidate_plan_cache();
    }

    /// Register all scalar UDFs from the attached registry with DataFusion.
    /// Uses unlimited defaults (backward compat).
    pub async fn sync_scalar_udfs(&self) -> SqlResult<()> {
        let Some(registry) = &self.udf_registry else {
            return Ok(());
        };
        let guard = registry.read().map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        let limits = self.udf_limits.clone().unwrap_or_default();
        udf::sync_scalar_udfs_with_limits(&self.context, &guard, limits).map_err(|e| {
            SqlError::DataFusion {
                message: e.to_string(),
            }
        })
    }

    /// Register scalar UDFs with explicit ResourceLimits for sandbox enforcement.
    /// Callers that have a job context (scheduler, runner, api session for a job)
    /// should use this and pass limits derived from the JobSpec (memory + time cap).
    /// This is the concrete Track E seam from job limits to UDF execution.
    pub async fn sync_scalar_udfs_with_limits(
        &self,
        limits: krishiv_udf::ResourceLimits,
    ) -> SqlResult<()> {
        let Some(registry) = &self.udf_registry else {
            return Ok(());
        };
        let guard = registry.read().map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        udf::sync_scalar_udfs_with_limits(&self.context, &guard, limits).map_err(|e| {
            SqlError::DataFusion {
                message: e.to_string(),
            }
        })
    }

    /// Register aggregate UDFs from the attached registry (P1-21).
    pub async fn sync_aggregate_udfs(&self) -> SqlResult<()> {
        let Some(registry) = &self.udf_registry else {
            return Ok(());
        };
        let guard = registry.read().map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        udf::sync_aggregate_udfs(&self.context, &guard).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })
    }

    /// Register table UDFs from the attached registry (P1-21).
    pub async fn sync_table_udfs(&self) -> SqlResult<()> {
        let Some(registry) = &self.udf_registry else {
            return Ok(());
        };
        let guard = registry.read().map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        udf::sync_table_udfs(&self.context, &guard).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })
    }

    /// Sync all UDF categories, respecting any limits configured on this engine (Track E).
    pub async fn sync_all_udfs(&self) -> SqlResult<()> {
        self.sync_scalar_udfs().await?;
        self.sync_aggregate_udfs().await?;
        self.sync_table_udfs().await?;
        Ok(())
    }

    /// Attach a [`MaterializedViewRegistry`] so the engine tracks view staleness.
    #[must_use]
    pub fn with_view_registry(
        mut self,
        registry: std::sync::Arc<std::sync::Mutex<MaterializedViewRegistry>>,
    ) -> Self {
        self.view_registry = Some(registry);
        self
    }

    /// Register a local Parquet path as a table.
    pub async fn register_parquet(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<Path>,
    ) -> SqlResult<()> {
        let table_name = table_name.as_ref();
        if table_name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }

        let path = path.as_ref().to_string_lossy().into_owned();
        if self
            .context
            .table_exist(table_name)
            .map_err(SqlError::from)?
        {
            let _ = self
                .context
                .deregister_table(table_name)
                .map_err(SqlError::from)?;
        }
        self.context
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await?;
        if let Some(ref reg) = self.view_registry
            && let Ok(mut r) = reg.lock()
        {
            r.mark_table_committed();
        }
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Create a DataFrame by reading a local Parquet path directly.
    pub async fn read_parquet(&self, path: impl AsRef<Path>) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let dataframe = self
            .context
            .read_parquet(path, ParquetReadOptions::default())
            .await?;
        Ok(SqlDataFrame::new("parquet-read", dataframe))
    }

    /// Register an in-memory table from Arrow record batches.
    ///
    /// The schema is inferred from the first batch. An empty `batches` slice
    /// registers a table with no rows using the provided schema if the batches
    /// are non-empty, or is a no-op if empty.
    pub async fn register_record_batches(
        &self,
        table_name: impl AsRef<str>,
        batches: Vec<RecordBatch>,
    ) -> SqlResult<()> {
        use std::sync::Arc;
        let table_name = table_name.as_ref();
        if table_name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        if batches.is_empty() {
            return Ok(());
        }
        let schema = batches[0].schema();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(|e| {
                SqlError::DataFusion {
                    message: e.to_string(),
                }
            })?;
        if self
            .context
            .table_exist(table_name)
            .map_err(SqlError::from)?
        {
            let _ = self
                .context
                .deregister_table(table_name)
                .map_err(SqlError::from)?;
        }
        self.context
            .register_table(table_name, Arc::new(mem_table))
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
        if let Some(ref reg) = self.view_registry
            && let Ok(mut r) = reg.lock()
        {
            r.mark_table_committed();
        }
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Read a local Delta table directory into a DataFrame.
    pub async fn read_delta(
        &self,
        path: impl AsRef<str>,
        version: Option<i64>,
    ) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref();
        let table = format!("delta_{}", path.replace(['/', '.', '-'], "_"));
        lakehouse::register_delta_uri(&self.context, &table, path, version).await?;
        self.sql(format!("SELECT * FROM {table}")).await
    }

    /// Read a Hudi table directory.
    pub async fn read_hudi(
        &self,
        path: impl AsRef<str>,
        query_type: krishiv_lakehouse::HudiQueryType,
        begin_instant: Option<&str>,
    ) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref();
        let table = format!("hudi_{}", path.replace(['/', '.', '-'], "_"));
        lakehouse::register_hudi_uri(&self.context, &table, path, query_type, begin_instant)
            .await?;
        self.sql(format!("SELECT * FROM {table}")).await
    }

    /// Plan a SQL query with DataFusion.
    pub async fn sql(&self, query: impl AsRef<str>) -> SqlResult<SqlDataFrame> {
        let query = query.as_ref();
        if query.trim().is_empty() {
            return Err(SqlError::EmptyQuery);
        }

        // Lazy UDF sync: only re-sync when the registry has changed since the
        // last sync. Avoids 3 RwLock reads per query when no UDFs are registered
        // or when the UDF set hasn't changed.
        {
            let current = self.udf_registry_version.load(Ordering::Acquire);
            let last = self.udf_last_synced_version.load(Ordering::Relaxed);
            if current != last {
                self.sync_all_udfs().await?;
                self.udf_last_synced_version
                    .store(current, Ordering::Release);
            }
        }

        // ── Intercept CREATE FUNCTION … RETURNS TABLE ────────────────────────
        // DataFusion does not understand this extended DDL syntax.  Parse it
        // here, register a stub UDTF, and return a trivial empty DataFrame so
        // callers see a successful DDL result rather than a parse error.
        if create_function_ddl::is_create_function_returns_table(query) {
            let ddl = create_function_ddl::parse_create_function(query)
                .map_err(|e| SqlError::DataFusion { message: e })?;
            let is_sql_body = ddl.language.as_deref() == Some("sql") && ddl.body.is_some();
            let udf: std::sync::Arc<dyn krishiv_udf::TableUdf> = if is_sql_body {
                // LANGUAGE sql AS '…': register an executable UDF backed by the body.
                let body = ddl.body.as_deref().unwrap_or_default();
                let fields: Vec<_> = ddl
                    .return_columns
                    .iter()
                    .map(|c| arrow::datatypes::Field::new(&c.name, c.data_type.clone(), true))
                    .collect();
                let schema = arrow::datatypes::Schema::new(fields);
                std::sync::Arc::new(create_function_ddl::SqlBodyTableUdf::new(
                    &ddl.function_name,
                    schema,
                    body,
                    std::sync::Arc::new(self.context.clone()),
                ))
            } else {
                if krishiv_common::is_production_mode() {
                    return Err(SqlError::DataFusion {
                        message: format!(
                            "CREATE FUNCTION '{}' with language {:?} is not supported in \
                             production; only LANGUAGE sql AS '...' table functions are allowed",
                            ddl.function_name, ddl.language
                        ),
                    });
                }
                // Other languages: register a stub so the schema resolves at plan time,
                // but calling the function will produce a clear error.
                std::sync::Arc::new(create_function_ddl::StubTableUdf::from_ddl(&ddl))
            };
            if let Some(registry) = &self.udf_registry {
                let mut guard = registry.write().map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })?;
                guard.register_table(std::sync::Arc::clone(&udf));
            }
            udf::register_single_table_udf(&self.context, std::sync::Arc::clone(&udf))
                .map_err(SqlError::from)?;
            // DDL always succeeds — the stub is registered for planning.
            // LANGUAGE sql: the body executes at call time.
            // Other languages: calling the function returns a clear "not implemented" error.
            let empty = self.context.sql("SELECT 1 WHERE FALSE").await?;
            return Ok(SqlDataFrame::new("create-function", empty).with_query(query));
        }

        if query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("MERGE INTO")
        {
            let batches = lakehouse::execute_merge_sql(&self.context, query).await?;
            lakehouse::register_scan_batches(&self.context, "_krishiv_merge_result", batches)
                .await?;
            let dataframe = self
                .context
                .sql("SELECT * FROM _krishiv_merge_result")
                .await?;
            return Ok(SqlDataFrame::new("merge", dataframe).with_query(query));
        }

        // ── Intercept MATCH_RECOGNIZE ─────────────────────────────────────────
        // DataFusion does not parse MATCH_RECOGNIZE. Route it through the CEP
        // path: parse → run PatternMatcher on the source table → return results.
        if query.to_ascii_uppercase().contains(" MATCH_RECOGNIZE ") {
            if let Some(stmt) = cep_sql::parse_match_recognize(query)? {
                let is_streaming = self.is_streaming_source(&stmt.source_table);
                // For streaming sources collect a bounded window of recent events
                // (capped at the configured limit) so the query terminates. The
                // cap is configurable through `KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT`
                // (default 100_000) so users can raise it for high-rate streams
                // or lower it to bound memory on small executors. The truncation
                // is logged at warn level because the result is no longer a
                // complete match over the unbounded stream.
                let streaming_limit = streaming_match_recognize_limit_from_env();
                let source_sql = if is_streaming {
                    format!(
                        "SELECT * FROM {} LIMIT {}",
                        stmt.source_table, streaming_limit
                    )
                } else {
                    format!("SELECT * FROM {}", stmt.source_table)
                };
                let source_df = self.context.sql(&source_sql).await?;
                let source_batches = source_df.collect().await?;
                if is_streaming {
                    tracing::warn!(
                        source = %stmt.source_table,
                        limit = streaming_limit,
                        collected_rows = source_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                        "MATCH_RECOGNIZE executed against a streaming source under \
                         bounded materialisation; results only cover the first {0} rows \
                         of the source. Set KRISHIV_MATCH_RECOGNIZE_STREAMING_LIMIT to a \
                         larger value if your executor has the memory budget.",
                        streaming_limit
                    );
                }
                let results = cep_sql::execute_match_recognize(stmt, &source_batches, false)?;
                lakehouse::register_scan_batches(&self.context, "_krishiv_cep_result", results)
                    .await?;
                let dataframe = self
                    .context
                    .sql("SELECT * FROM _krishiv_cep_result")
                    .await?;
                return Ok(SqlDataFrame::new("cep", dataframe).with_query(query));
            }
        }

        let (rewritten, as_ofs) =
            lakehouse::preprocess_as_of_sql(query).unwrap_or_else(|_| (query.to_string(), vec![]));
        lakehouse::apply_as_of_refs(&self.context, &as_ofs).await?;

        // ── Plan cache ────────────────────────────────────────────────────────
        // Check the cache before sending the query through DataFusion's full
        // parse → analyse → optimise pipeline. Only cache simple queries without
        // DDL or AS-OF refs; DDL side effects must not be bypassed.
        // Single-lock design: lookup and insert share the same Mutex<PlanCache>,
        // eliminating the TOCTOU race of the previous DashMap + VecDeque approach.
        let can_cache = as_ofs.is_empty();
        if can_cache {
            // Scope the guard so it is dropped before any .await point.
            let cached_plan: Option<datafusion::logical_expr::LogicalPlan> = self
                .plan_cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&rewritten)
                .cloned();
            if let Some(plan) = cached_plan {
                let dataframe = self.context.execute_logical_plan(plan).await?;
                return Ok(SqlDataFrame::new("sql-query", dataframe).with_query(rewritten));
            }
        }

        let dataframe = self.context.sql(&rewritten).await?;

        // Cache the logical plan for future repeated calls.
        if can_cache {
            let plan = dataframe.logical_plan().clone();
            match self.plan_cache.lock() {
                Ok(mut cache) => cache.insert(rewritten.clone(), plan),
                Err(poisoned) => poisoned.into_inner().insert(rewritten.clone(), plan),
            }
        }

        Ok(SqlDataFrame::new("sql-query", dataframe).with_query(rewritten))
    }

    /// Execute `query` with materialized view cache lookup.
    ///
    /// If the query targets a registered, fresh view, returns cached batches directly.
    /// Otherwise executes normally and caches the result for `OnCommit` views.
    pub async fn sql_with_view_cache(&self, query: impl AsRef<str>) -> SqlResult<Vec<RecordBatch>> {
        let q = query.as_ref().trim();
        let view_name_candidate = extract_simple_view_name(q);

        if let (Some(reg), Some(name)) = (&self.view_registry, &view_name_candidate)
            && let Ok(r) = reg.lock()
            && let Some(cached) = r.get_if_fresh(name)
        {
            return Ok(cached.clone());
        }

        let df = self.sql(q).await?;
        let batches = df.collect().await?;

        if let (Some(reg), Some(name)) = (&self.view_registry, &view_name_candidate)
            && let Ok(mut r) = reg.lock()
            && let Some(def) = r.definition(name).cloned()
            && def.refresh_policy == RefreshPolicy::OnCommit
        {
            r.set_cached(name, batches.clone());
        }

        Ok(batches)
    }
}

fn extract_simple_view_name(query: &str) -> Option<String> {
    use std::ops::ControlFlow;
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query).ok()?;
    let mut result = None;
    for stmt in &statements {
        let _ = visit_relations(stmt, |relation| {
            if result.is_none() {
                result = Some(relation.to_string());
            }
            ControlFlow::Break(())
        });
        if result.is_some() {
            break;
        }
    }
    result
}

/// Engine-agnostic interface over a prepared query result.
///
/// Hides the concrete [`SqlDataFrame`] (which holds a DataFusion `DataFrame`)
/// behind a stable trait so that `krishiv-api` and other callers are not
/// forced to depend on DataFusion types.  `datafusion` stays an implementation
/// detail inside `krishiv-sql`; a future engine swap only requires a new impl.
#[async_trait::async_trait]
pub trait KrishivDataFrameOps: Send + Sync {
    /// Execute and collect all result batches.
    async fn collect(&self) -> SqlResult<Vec<RecordBatch>>;
    /// Execute, collect results, and return lightweight runtime statistics.
    async fn collect_with_stats(&self) -> SqlResult<(Vec<RecordBatch>, SqlExecutionStats)>;
    /// Explain the physical and logical plan text (does not execute).
    async fn explain(&self) -> SqlResult<String>;
    /// Explain the logical plan text without executing.
    fn explain_logical(&self) -> String;
    /// Build a Krishiv [`LogicalPlan`] wrapper for this DataFrame.
    fn krishiv_logical_plan(&self) -> LogicalPlan;
    /// The original SQL query string, if any.
    fn query(&self) -> Option<&str>;
    /// Execute and return a record batch stream.
    async fn execute_stream(&self) -> SqlResult<krishiv_plan::SendableRecordBatchStream>;
}

/// Krishiv-owned wrapper around a DataFusion DataFrame.
#[derive(Debug, Clone)]
pub struct SqlDataFrame {
    name: String,
    query: Option<String>,
    dataframe: DataFusionDataFrame,
}

impl SqlDataFrame {
    fn new(name: impl Into<String>, dataframe: DataFusionDataFrame) -> Self {
        Self {
            name: name.into(),
            query: None,
            dataframe,
        }
    }

    fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Original SQL query when created from [`SqlEngine::sql`].
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Create a Krishiv logical plan wrapper for this DataFrame.
    pub fn krishiv_logical_plan(&self) -> LogicalPlan {
        let label = self.dataframe.logical_plan().to_string();
        LogicalPlan::new(self.name.clone(), ExecutionKind::Batch).with_node(PlanNode::new(
            "datafusion-logical",
            label,
            ExecutionKind::Batch,
        ))
    }

    /// Explain the logical plan without executing it.
    pub fn explain_logical(&self) -> String {
        self.dataframe.logical_plan().to_string()
    }

    /// Explain logical and physical plan details through DataFusion.
    pub async fn explain(&self) -> SqlResult<String> {
        let batches = self
            .dataframe
            .clone()
            .explain(false, false)?
            .collect()
            .await?;
        pretty_batches(&batches)
    }

    /// Execute and collect this DataFrame.
    pub async fn collect(&self) -> SqlResult<Vec<RecordBatch>> {
        Ok(self.dataframe.clone().collect().await?)
    }

    /// Execute and return a record batch stream.
    pub async fn execute_stream(&self) -> SqlResult<krishiv_plan::SendableRecordBatchStream> {
        let df_stream = self.dataframe.clone().execute_stream().await?;
        use futures::StreamExt;
        let mapped = df_stream.map(|res| res.map_err(|e| e.to_string()));
        Ok(Box::pin(mapped))
    }

    /// Execute and collect this DataFrame, also returning lightweight runtime statistics.
    ///
    /// Collects `output_rows` from DataFusion's execution metrics. `cpu_nanos`
    /// is approximated from `elapsed_compute` when available; other fields default to 0.
    pub async fn collect_with_stats(&self) -> SqlResult<(Vec<RecordBatch>, SqlExecutionStats)> {
        use datafusion::physical_plan::collect as df_collect;

        let df = self.dataframe.clone();
        let task_ctx = df.task_ctx();
        let physical_plan = df.create_physical_plan().await?;

        let batches = df_collect(physical_plan.clone(), task_ctx.into()).await?;

        let mut output_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut cpu_nanos: u64 = 0;

        if let Some(metrics) = physical_plan.metrics() {
            if let Some(v) = metrics.output_rows() {
                output_rows = v as u64;
            }
            if let Some(t) = metrics.elapsed_compute() {
                cpu_nanos = t as u64;
            }
        }

        Ok((
            batches,
            SqlExecutionStats {
                output_rows,
                cpu_nanos,
            },
        ))
    }
}

/// Lightweight execution statistics collected from a DataFusion physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlExecutionStats {
    pub output_rows: u64,
    pub cpu_nanos: u64,
}

#[async_trait::async_trait]
impl KrishivDataFrameOps for SqlDataFrame {
    async fn collect(&self) -> SqlResult<Vec<RecordBatch>> {
        SqlDataFrame::collect(self).await
    }
    async fn collect_with_stats(&self) -> SqlResult<(Vec<RecordBatch>, SqlExecutionStats)> {
        SqlDataFrame::collect_with_stats(self).await
    }
    async fn explain(&self) -> SqlResult<String> {
        SqlDataFrame::explain(self).await
    }
    fn explain_logical(&self) -> String {
        SqlDataFrame::explain_logical(self)
    }
    fn krishiv_logical_plan(&self) -> LogicalPlan {
        SqlDataFrame::krishiv_logical_plan(self)
    }
    fn query(&self) -> Option<&str> {
        SqlDataFrame::query(self)
    }
    async fn execute_stream(&self) -> SqlResult<krishiv_plan::SendableRecordBatchStream> {
        SqlDataFrame::execute_stream(self).await
    }
}

/// Create a Krishiv logical plan wrapper for a SQL query without executing it.
pub fn plan_sql(query: impl Into<String>) -> SqlResult<SqlPlan> {
    let query = query.into();
    if query.trim().is_empty() {
        return Err(SqlError::EmptyQuery);
    }

    if let Some(stmt) = cep_sql::parse_match_recognize(&query)? {
        let logical_plan = cep_sql::plan_match_recognize(stmt, &query);
        let optimized = Optimizer::default().optimize(logical_plan);
        return Ok(SqlPlan {
            query,
            logical_plan: optimized.plan,
        });
    }

    let logical_plan =
        LogicalPlan::new("sql-query", ExecutionKind::Batch).with_node(PlanNode::new(
            "sql",
            format!("sql: {}", query.trim()),
            ExecutionKind::Batch,
        ));

    let optimized = Optimizer::default().optimize(logical_plan);
    Ok(SqlPlan {
        query,
        logical_plan: optimized.plan,
    })
}

/// Create bootstrap `EXPLAIN` text for a SQL query.
pub fn explain_sql(query: impl Into<String>) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    Ok(plan.logical_plan().describe())
}

/// Explain a SQL query including optimizer rule decisions.
///
/// Runs the logical plan through `optimizer` and appends the optimizer
/// summary to the plan description.
pub fn explain_sql_optimized(query: impl Into<String>, optimizer: &Optimizer) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    let result = optimizer.optimize(plan.logical_plan().clone());
    let mut output = result.plan.describe();
    let optimizer_line = result.describe();
    output.push('\n');
    output.push_str(&optimizer_line);
    Ok(output)
}

/// Explain a SQL query and append a cost estimate from the provided cost model.
pub fn explain_sql_with_cost(
    query: impl Into<String>,
    cost_model: &dyn CostModel,
) -> SqlResult<String> {
    let plan = plan_sql(query)?;
    let cost = cost_model.estimate(plan.logical_plan());
    let mut output = plan.logical_plan().describe();
    output.push_str(&format!(
        "\ncost: cpu_nanos={}, memory_bytes={}, network_bytes={}",
        cost.cpu_nanos, cost.memory_bytes, cost.network_bytes
    ));
    Ok(output)
}

/// Return all base table/relation names referenced by `query`.
///
/// This uses the same SQL parser family as DataFusion, so policy checks cover
/// joins, subqueries, CTE bodies, and other nested relation references instead
/// of relying on a single best-effort `FROM` token.
pub fn referenced_table_names(query: impl AsRef<str>) -> SqlResult<Vec<String>> {
    let query = query.as_ref();
    if query.trim().is_empty() {
        return Err(SqlError::EmptyQuery);
    }

    let statements =
        Parser::parse_sql(&GenericDialect {}, query).map_err(|e| SqlError::DataFusion {
            message: format!("SQL parse error: {e}"),
        })?;
    let mut names = BTreeSet::new();
    let _ = visit_relations(&statements, |relation| {
        names.insert(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(names.into_iter().collect())
}

/// Format Arrow batches for CLI and tests.
pub fn pretty_batches(batches: &[RecordBatch]) -> SqlResult<String> {
    Ok(pretty_format_batches(batches)
        .map_err(|error| SqlError::DataFusion {
            message: error.to_string(),
        })?
        .to_string())
}

// ─── Materialized Views Baseline ─────────────────────────────────────────────

/// Materialized view refresh policy.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshPolicy {
    /// Refresh whenever the backing table(s) receive a write commit.
    OnCommit,
    /// Only refresh when explicitly triggered by `MaterializedViewRegistry::refresh()`.
    Manual,
}

/// Declaration of a named materialized view.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct MaterializedViewDefinition {
    /// Unique view name.
    pub name: String,
    /// SQL SELECT query that defines the view.
    pub query: String,
    /// Refresh policy.
    pub refresh_policy: RefreshPolicy,
    /// Partition columns for storage keying (empty = unpartitioned).
    pub partition_columns: Vec<String>,
}

impl MaterializedViewDefinition {
    /// Create a new view definition with OnCommit refresh and no partitioning.
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            refresh_policy: RefreshPolicy::OnCommit,
            partition_columns: Vec::new(),
        }
    }

    /// Set the refresh policy.
    #[must_use]
    pub fn with_refresh_policy(mut self, policy: RefreshPolicy) -> Self {
        self.refresh_policy = policy;
        self
    }

    /// Set partition columns.
    #[must_use]
    pub fn with_partition_columns(mut self, cols: Vec<String>) -> Self {
        self.partition_columns = cols;
        self
    }
}

/// In-memory registry for materialized view definitions and their cached results.
///
/// **Alpha (R10)**: In-memory only. View state is not persisted and resets on process restart.
/// In production, results would be persisted to `RedbStateBackend`.
#[derive(Debug, Default)]
pub struct MaterializedViewRegistry {
    definitions: HashMap<String, MaterializedViewDefinition>,
    /// Cached results keyed by view name → serialized batch (Arrow IPC).
    cache: HashMap<String, Vec<RecordBatch>>,
    /// Current write LSN — incremented on each `mark_table_committed()` call.
    current_lsn: u64,
    /// LSN at which each view was last refreshed.
    view_lsn: HashMap<String, u64>,
}

impl MaterializedViewRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a materialized view definition.
    pub fn register(&mut self, def: MaterializedViewDefinition) {
        self.definitions.insert(def.name.clone(), def);
    }

    /// Mark a table as having received a commit. Increments the current LSN.
    /// All OnCommit views are now stale.
    pub fn mark_table_committed(&mut self) {
        self.current_lsn += 1;
    }

    /// Returns true if the view is stale (backing table committed after last refresh,
    /// or the view has never been cached / is not registered).
    pub fn is_stale(&self, view_name: &str) -> bool {
        // Unregistered or never-cached views are always considered stale.
        if !self.view_lsn.contains_key(view_name) {
            return true;
        }
        let last_refresh = self.view_lsn.get(view_name).copied().unwrap_or(0);
        last_refresh < self.current_lsn
    }

    /// Store refreshed results for a view.
    pub fn set_cached(&mut self, view_name: &str, batches: Vec<RecordBatch>) {
        self.cache.insert(view_name.to_string(), batches);
        self.view_lsn
            .insert(view_name.to_string(), self.current_lsn);
    }

    /// Get cached results if the view is fresh.
    pub fn get_if_fresh(&self, view_name: &str) -> Option<&Vec<RecordBatch>> {
        if self.is_stale(view_name) {
            None
        } else {
            self.cache.get(view_name)
        }
    }

    /// Get the view definition, if registered.
    pub fn definition(&self, view_name: &str) -> Option<&MaterializedViewDefinition> {
        self.definitions.get(view_name)
    }
}

#[cfg(test)]
mod matview_tests {
    use super::*;

    #[test]
    fn fresh_view_returns_cached_results() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        let batch = vec![]; // empty batch for test
        reg.set_cached("v1", batch.clone());
        assert!(reg.get_if_fresh("v1").is_some());
    }

    #[test]
    fn committed_table_marks_view_stale() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        reg.set_cached("v1", vec![]);
        assert!(!reg.is_stale("v1"));
        reg.mark_table_committed();
        assert!(reg.is_stale("v1"));
        assert!(reg.get_if_fresh("v1").is_none());
    }

    #[test]
    fn refresh_after_commit_restores_freshness() {
        let mut reg = MaterializedViewRegistry::new();
        reg.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
        reg.set_cached("v1", vec![]);
        reg.mark_table_committed();
        assert!(reg.is_stale("v1"));
        reg.set_cached("v1", vec![]); // refresh
        assert!(!reg.is_stale("v1"));
    }

    #[test]
    fn unregistered_view_is_stale() {
        let reg = MaterializedViewRegistry::new();
        assert!(reg.is_stale("nonexistent"));
    }

    #[test]
    fn definition_builder_sets_fields() {
        let def = MaterializedViewDefinition::new("sales_summary", "SELECT SUM(amount) FROM sales")
            .with_refresh_policy(RefreshPolicy::Manual)
            .with_partition_columns(vec!["region".into()]);
        assert_eq!(def.name, "sales_summary");
        assert_eq!(def.refresh_policy, RefreshPolicy::Manual);
        assert_eq!(def.partition_columns, vec!["region".to_string()]);
    }
}

#[cfg(test)]
mod view_cache_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn engine_marks_table_committed_after_register() {
        let registry = Arc::new(Mutex::new(MaterializedViewRegistry::new()));
        {
            let mut r = registry.lock().unwrap();
            r.register(MaterializedViewDefinition::new("v1", "SELECT 1"));
            r.set_cached("v1", vec![]);
        }
        assert!(!registry.lock().unwrap().is_stale("v1"));

        let engine = SqlEngine::new().with_view_registry(registry.clone());
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        let col = arrow::array::Int64Array::from(vec![1i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        engine
            .register_record_batches("t1", vec![batch])
            .await
            .unwrap();

        assert!(
            registry.lock().unwrap().is_stale("v1"),
            "commit must mark view stale"
        );
    }

    #[tokio::test]
    async fn sql_with_view_cache_returns_fresh_cache() {
        let registry = Arc::new(Mutex::new(MaterializedViewRegistry::new()));
        let expected_batch = {
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
            ]));
            let col = arrow::array::Int64Array::from(vec![99i64]);
            RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
        };
        {
            let mut r = registry.lock().unwrap();
            r.register(
                MaterializedViewDefinition::new("summary", "SELECT 99 AS v")
                    .with_refresh_policy(RefreshPolicy::OnCommit),
            );
            r.set_cached("summary", vec![expected_batch.clone()]);
        }

        let engine = SqlEngine::new().with_view_registry(registry.clone());
        let batches = engine
            .sql_with_view_cache("SELECT * FROM summary")
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn register_record_batches_overwrites_existing_table() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![3, 4, 5]))],
        )
        .unwrap();

        let engine = SqlEngine::new();
        engine
            .register_record_batches("inventory", vec![first])
            .await
            .unwrap();
        engine
            .register_record_batches("inventory", vec![second])
            .await
            .unwrap();

        let dataframe = engine
            .sql("SELECT count(*) AS n FROM inventory")
            .await
            .unwrap();
        let collected = dataframe.collect().await.unwrap();
        let rows: usize = collected.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 1);
    }
}

#[cfg(test)]
mod tests {
    use krishiv_optimizer::{Cost, CostModel, Optimizer};
    use krishiv_plan::LogicalPlan;

    use super::{
        SqlEngine, SqlError, explain_sql, explain_sql_optimized, explain_sql_with_cost, plan_sql,
        referenced_table_names,
    };

    #[tokio::test]
    async fn catalog_table_resolved_in_sql() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use krishiv_catalog::{
            CatalogField, FieldType, InMemoryCatalog, TableMetadata, TableSchema,
        };
        use std::sync::{Arc, RwLock};

        let catalog = Arc::new(RwLock::new(InMemoryCatalog::new()));
        let schema = TableSchema::new(vec![CatalogField::new("id", FieldType::Int64, false)]);
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values: Vec<Option<i64>> = (0..10).map(Some).collect();
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
        catalog
            .write()
            .unwrap()
            .register_table_with_batches(TableMetadata::new("t", schema), vec![batch])
            .unwrap();

        let engine = SqlEngine::with_in_memory_catalog(catalog).unwrap();
        let df = engine.sql("SELECT * FROM krishiv.public.t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10);
    }

    #[test]
    fn rejects_empty_sql() {
        let error = match plan_sql("   ") {
            Ok(_) => panic!("expected empty query error"),
            Err(error) => error,
        };

        assert_eq!(error, SqlError::EmptyQuery);
    }

    #[test]
    fn referenced_table_names_covers_joins_and_subqueries() {
        let tables = referenced_table_names(
            "SELECT * FROM public JOIN secret ON public.id = secret.id \
             WHERE public.id IN (SELECT id FROM audit)",
        )
        .unwrap();
        assert_eq!(tables, vec!["audit", "public", "secret"]);
    }

    #[test]
    fn explains_non_empty_sql() {
        let explain = match explain_sql("select 1") {
            Ok(explain) => explain,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        assert!(explain.contains("logical plan: sql-query"));
    }

    #[test]
    fn explain_sql_optimized_no_op_optimizer_includes_no_rules_message() {
        let optimizer = Optimizer::new();
        let output = explain_sql_optimized("select 1", &optimizer).unwrap();
        assert!(
            output.contains("optimizer: no rules applied"),
            "output did not contain expected optimizer message: {output}"
        );
    }

    #[test]
    fn explain_sql_with_cost_includes_cost_line() {
        struct ZeroCost;
        impl CostModel for ZeroCost {
            fn estimate(&self, _plan: &LogicalPlan) -> Cost {
                Cost::default()
            }
        }

        let output = explain_sql_with_cost("select 1", &ZeroCost).unwrap();
        assert!(
            output.contains("cost:"),
            "output did not contain cost line: {output}"
        );
        assert!(output.contains("cpu_nanos=0"));
        assert!(output.contains("memory_bytes=0"));
        assert!(output.contains("network_bytes=0"));
    }

    #[tokio::test]
    async fn datafusion_sql_collects_rows() {
        let engine = SqlEngine::new();
        let dataframe = match engine.sql("select 1 as value").await {
            Ok(dataframe) => dataframe,
            Err(error) => panic!("unexpected SQL error: {error}"),
        };

        let batches = match dataframe.collect().await {
            Ok(batches) => batches,
            Err(error) => panic!("unexpected collect error: {error}"),
        };

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }

    // ── GAP-RT-06: collect_with_stats uses the DataFrame's own context ──────────

    #[tokio::test]
    async fn collect_with_stats_uses_registered_table() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let engine = SqlEngine::new();

        // Register a record batch as a table.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let col = Int64Array::from(vec![1i64, 2i64, 3i64]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        engine
            .register_record_batches("rt06_table", vec![batch])
            .await
            .unwrap();

        // Query that table via collect_with_stats.
        let dataframe = engine
            .sql("SELECT id FROM rt06_table")
            .await
            .expect("sql should succeed");
        let (batches, stats) = dataframe
            .collect_with_stats()
            .await
            .expect("collect_with_stats should succeed with registered table");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "expected 3 rows from registered table, got {total_rows} (stats: {stats:?})"
        );
    }
}

#[cfg(test)]
mod udf_sql_tests {
    use std::sync::Arc;

    use krishiv_udf::MultiplyScalarUdf;

    use super::SqlEngine;

    #[tokio::test]
    async fn registered_scalar_udf_visible_in_sql() {
        let registry = Arc::new(std::sync::RwLock::new(krishiv_udf::UdfRegistry::new()));
        registry
            .write()
            .unwrap()
            .register_scalar(Arc::new(MultiplyScalarUdf::new("triple", "x", 3)));
        let engine = SqlEngine::new().with_udf_registry(registry);
        engine
            .register_record_batches(
                "t",
                vec![{
                    use arrow::array::Int64Array;
                    use arrow::datatypes::{DataType, Field, Schema};
                    use arrow::record_batch::RecordBatch;
                    use std::sync::Arc;
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
                    RecordBatch::try_new(
                        schema,
                        vec![Arc::new(Int64Array::from(vec![Some(2), Some(4)]))],
                    )
                    .unwrap()
                }],
            )
            .await
            .unwrap();
        let df = engine.sql("SELECT triple(x) AS y FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 6);
        assert_eq!(col.value(1), 12);
    }
}

#[cfg(test)]
mod udtf_ddl_tests {
    use super::SqlEngine;

    /// All `CREATE FUNCTION … RETURNS TABLE` DDL succeeds — the stub is registered
    /// for plan-time schema resolution regardless of language. Execution errors for
    /// non-SQL languages are deferred to call time.
    #[tokio::test]
    async fn create_function_returns_table_ddl_always_succeeds() {
        let engine = SqlEngine::new();

        // Non-SQL language: DDL registers stub, returns empty DataFrame.
        let rust_result = engine
            .sql(
                "CREATE FUNCTION my_udtf(arg1 INT) \
                 RETURNS TABLE (col1 TEXT, col2 BIGINT) \
                 LANGUAGE RUST \
                 AS 'fn stub() {}'",
            )
            .await;
        assert!(
            rust_result.is_ok(),
            "LANGUAGE RUST DDL should succeed, got {rust_result:?}"
        );

        // SQL language: inline body registered, also returns empty DataFrame on DDL.
        let sql_result = engine
            .sql(
                "CREATE FUNCTION greet(name TEXT) \
                 RETURNS TABLE (msg TEXT) \
                 LANGUAGE SQL \
                 AS 'SELECT ''hello'' AS msg'",
            )
            .await;
        assert!(
            sql_result.is_ok(),
            "LANGUAGE SQL DDL should succeed, got {sql_result:?}"
        );
    }

    // ── Streaming source registration (broker-free path) ─────────────────────
    //
    // register_kafka_source constructs a live KafkaSource whose rdkafka log
    // subsystem aborts in a test binary without proper init. Use the new
    // register_streaming_source_name API for broker-free unit tests.

    #[test]
    fn register_streaming_source_name_marks_table_as_streaming() {
        let engine = SqlEngine::new();
        engine
            .register_streaming_source_name("stream_events")
            .unwrap();
        assert!(
            engine
                .is_streaming_query("SELECT * FROM stream_events")
                .unwrap(),
            "register_streaming_source_name must make the query streaming"
        );
    }

    #[test]
    fn register_streaming_source_name_does_not_affect_other_tables() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("my_stream").unwrap();
        assert!(
            !engine
                .is_streaming_query("SELECT * FROM other_table")
                .unwrap(),
            "only the registered table name must be streaming"
        );
    }

    #[test]
    fn is_streaming_query_false_for_plain_select() {
        let engine = SqlEngine::new();
        assert!(
            !engine.is_streaming_query("SELECT 1 AS n").unwrap(),
            "plain SELECT must not be classified as streaming"
        );
    }

    #[test]
    fn is_streaming_query_true_after_source_registered() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("events").unwrap();
        assert!(
            engine
                .is_streaming_query("SELECT ts, user_id FROM events WHERE ts > 0")
                .unwrap()
        );
    }

    #[test]
    fn multiple_streaming_sources_any_makes_query_streaming() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("s1").unwrap();
        engine.register_streaming_source_name("s2").unwrap();
        assert!(engine.is_streaming_query("SELECT * FROM s1").unwrap());
        assert!(engine.is_streaming_query("SELECT * FROM s2").unwrap());
        assert!(!engine.is_streaming_query("SELECT * FROM s3").unwrap());
    }

    #[test]
    fn is_streaming_query_true_for_table_alias() {
        let engine = SqlEngine::new();
        engine
            .register_streaming_source_name("kafka_source")
            .unwrap();
        // visit_relations must return the base table name, not the alias.
        assert!(
            engine
                .is_streaming_query("SELECT * FROM kafka_source AS k")
                .unwrap(),
            "aliased streaming table must still be classified as streaming"
        );
        assert!(
            engine
                .is_streaming_query(
                    "SELECT * FROM kafka_source AS k JOIN other AS o ON k.id = o.id"
                )
                .unwrap(),
            "JOIN with alias must still detect the streaming source"
        );
    }

    #[test]
    fn register_streaming_source_name_empty_returns_error() {
        let engine = SqlEngine::new();
        assert!(engine.register_streaming_source_name("").is_err());
        assert!(engine.register_streaming_source_name("   ").is_err());
    }

    #[test]
    fn deregister_streaming_source_removes_name() {
        let engine = SqlEngine::new();
        engine.register_streaming_source_name("topic").unwrap();
        assert!(engine.is_streaming_query("SELECT * FROM topic").unwrap());
        engine.deregister_streaming_source("topic").unwrap();
        assert!(
            !engine.is_streaming_query("SELECT * FROM topic").unwrap(),
            "deregistered source must no longer be classified as streaming"
        );
    }

    #[test]
    fn deregister_nonexistent_source_is_ok() {
        let engine = SqlEngine::new();
        // Deregistering a name that was never registered must be idempotent.
        engine
            .deregister_streaming_source("never_registered")
            .expect("deregister of unknown name must not error");
    }

    // ── Plan cache invalidation ───────────────────────────────────────────────

    #[tokio::test]
    async fn plan_cache_cleared_after_table_registration() {
        let engine = SqlEngine::new();
        // Prime the cache with a simple query.
        let _ = engine.sql("SELECT 1 AS n").await.unwrap();
        assert!(
            !engine.plan_cache.lock().unwrap().is_empty(),
            "cache must be non-empty after first query"
        );

        // Registering a table (even parquet) must clear the cache.
        engine.clear_plan_cache();
        assert!(
            engine.plan_cache.lock().unwrap().is_empty(),
            "cache must be empty after clear_plan_cache()"
        );
    }

    #[tokio::test]
    async fn plan_cache_repopulates_after_invalidation() {
        let engine = SqlEngine::new();
        let _ = engine.sql("SELECT 42 AS v").await.unwrap();
        engine.clear_plan_cache();
        // Re-running the same query must succeed and re-populate the cache.
        let df = engine.sql("SELECT 42 AS v").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert!(
            !engine.plan_cache.lock().unwrap().is_empty(),
            "cache must refill after re-query"
        );
    }
}

#[cfg(test)]
mod streaming_match_recognize_limit_tests {
    use crate::resolve_streaming_match_recognize_limit;

    #[test]
    fn default_when_unset() {
        assert_eq!(resolve_streaming_match_recognize_limit(None), 100_000);
    }

    #[test]
    fn default_when_empty() {
        assert_eq!(resolve_streaming_match_recognize_limit(Some("")), 100_000);
    }

    #[test]
    fn parses_valid_value() {
        assert_eq!(resolve_streaming_match_recognize_limit(Some("42")), 42);
    }

    #[test]
    fn falls_back_on_unparseable() {
        assert_eq!(
            resolve_streaming_match_recognize_limit(Some("not-a-number")),
            100_000
        );
    }

    #[test]
    fn rejects_zero() {
        // 0 would mean "scan zero rows" which is meaningless.
        assert_eq!(resolve_streaming_match_recognize_limit(Some("0")), 100_000);
    }
}
pub mod kafka_table;
