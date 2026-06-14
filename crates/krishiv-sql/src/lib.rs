#![forbid(unsafe_code)]

//! SQL planning and local execution seam for Krishiv.
//!
//! This crate owns the DataFusion integration for R1 while keeping DataFusion
//! out of the long-term public API exposed by `krishiv-api`.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use catalog::{InMemoryCatalog, datafusion_bridge::DataFusionCatalogBridge};
use datafusion::dataframe::DataFrame as DataFusionDataFrame;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::sqlparser::{ast::visit_relations, dialect::GenericDialect, parser::Parser};

use krishiv_plan::optimizer::{CostModel, Optimizer};
use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

pub mod catalog;
pub mod cep_sql;
mod connector_table;
pub mod create_function_ddl;
pub mod grammar;
mod lakehouse;
pub mod live_table;
pub mod pivot_sql;
pub mod recursive_cte;
pub mod sqlstate;
pub mod subquery;
pub mod unnest_sql;

pub mod streaming;
mod udf;
mod window_functions;

pub use cep_sql::{
    MatchRecognizeStatement, execute_streaming_match_recognize, parse_match_recognize,
};
pub use lakehouse::{AsOfTableRef, MergeResult, MergeTargetUnsupportedError, preprocess_as_of_sql};

pub use grammar::{
    FeatureEntry, FeatureStatus, feature_matrix, features_by_status, features_for_category,
};
pub use sqlstate::{SqlStateError, sqlstate_for};
pub use streaming::{ContinuousInputError, ContinuousTableInput};

/// SQL result alias.
pub type SqlResult<T> = Result<T, SqlError>;

/// Pinned stream of record batches with `String` error type.
pub type SqlStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<RecordBatch, String>> + Send>>;

/// Global counter for unique ephemeral table names, preventing concurrent
/// MERGE/CEP queries from overwriting each other's result tables.
static EPHEMERAL_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_ephemeral_name(prefix: &str) -> String {
    let id = EPHEMERAL_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("__{prefix}_{id}")
}

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
        if self.map.contains_key(&key) {
            // Remove the stale order entry so a repeated insert doesn't accumulate
            // duplicate references and corrupt LRU eviction order.
            self.order.retain(|k| k != &key);
        } else if self.map.len() >= self.max {
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

/// Typed options for Parquet reads (propagated into DataFusion).
#[derive(Debug, Clone, Default)]
pub struct ParquetReaderOptions {
    /// Maximum number of rows per output batch (None = DataFusion default 8192).
    pub batch_size: Option<usize>,
}

/// Typed options for CSV reads (propagated into DataFusion).
#[derive(Debug, Clone, Default)]
pub struct CsvReaderOptions {
    /// Field delimiter character (None = `,`).
    pub delimiter: Option<char>,
    /// Whether the first row is a header (None = true).
    pub has_header: Option<bool>,
}

/// Typed options for Parquet writes (propagated into the `ArrowWriter`).
#[derive(Debug, Clone, Default)]
pub struct ParquetWriterOptions {
    /// Compression codec: "snappy" | "zstd" | "gzip" | "lz4" | "brotli" | "uncompressed".
    pub compression: Option<String>,
    /// Maximum number of rows per row-group (None = `ArrowWriter` default 1 048 576).
    pub max_row_group_size: Option<usize>,
}

/// Typed options for CSV writes.
#[derive(Debug, Clone, Default)]
pub struct CsvWriterOptions {
    /// Field delimiter character (None = `,`).
    pub delimiter: Option<char>,
    /// Whether to emit a header row (None = true).
    pub has_header: Option<bool>,
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
    /// A table-function declaration or runtime registration was invalid.
    #[error("invalid table function: {message}")]
    InvalidTableFunction { message: String },
    /// DataFusion returned an error.
    #[error("DataFusion error: {message}")]
    DataFusion { message: String },
    /// Krishiv logical-plan optimization failed.
    #[error(transparent)]
    Optimizer(#[from] krishiv_plan::optimizer::OptimizerError),
    /// Access denied by auth or policy check.
    #[error("access denied: {reason}")]
    AccessDenied { reason: String },
    /// A running operation was cancelled by the caller.
    #[error("operation {operation_id} was cancelled")]
    OperationCancelled { operation_id: u64 },
    /// A query exceeded its configured execution timeout.
    #[error("query timed out after {timeout_ms} ms")]
    Timeout { timeout_ms: u64 },
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

/// Resolve a per-engine DataFusion memory limit from a raw env var value.
/// `None`, unparseable, and zero values all mean "no limit" (the engine runs
/// with DataFusion's default unbounded pool).
pub fn resolve_query_memory_limit_bytes(raw: Option<&str>) -> Option<usize> {
    raw.and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
}

/// Resolve the default per-engine memory limit from the
/// `KRISHIV_QUERY_MEMORY_LIMIT_BYTES` environment variable.
pub fn query_memory_limit_from_env() -> Option<usize> {
    resolve_query_memory_limit_bytes(
        std::env::var("KRISHIV_QUERY_MEMORY_LIMIT_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Build the DataFusion session config with a configurable parallelism level.
///
/// When `target_partitions > 1`, round-robin repartitioning is enabled so
/// DataFusion can balance work across threads for hash-join build,
/// aggregation spill, and parquet scan parallelism.
fn build_single_node_session_config(
    target_partitions: NonZeroUsize,
) -> datafusion::prelude::SessionConfig {
    let tp = target_partitions.get();
    let mut config = datafusion::prelude::SessionConfig::new()
        .with_target_partitions(tp)
        .set_bool(
            "datafusion.optimizer.enable_round_robin_repartition",
            tp > 1,
        );
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    config
}

#[derive(Clone)]
pub struct SqlEngine {
    context: SessionContext,
    target_parallelism: NonZeroUsize,
    krishiv_catalog: Option<Arc<RwLock<InMemoryCatalog>>>,
    udf_registry: Option<std::sync::Arc<std::sync::RwLock<krishiv_plan::udf::UdfRegistry>>>,
    /// Table names registered as unbounded streaming sources.
    /// Wrapped in `Arc<RwLock<>>` so that Session clones share the same set.
    streaming_sources: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Serializes streaming table name validation and catalog registration.
    streaming_registration: Arc<Mutex<()>>,
    /// `true` once any streaming source has been registered.  Checked with a
    /// relaxed atomic load before acquiring `streaming_sources` so that the
    /// common case (no streaming sources, pure batch workload) avoids both the
    /// lock and the SQL parse inside `is_streaming_query`.
    has_streaming_sources: Arc<AtomicBool>,
    /// Optional UDF resource limits to apply when syncing UDFs for this engine.
    /// Set for job-specific engines so sandbox enforcement uses the job's budgets.
    udf_limits: Option<krishiv_plan::udf::ResourceLimits>,
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
    /// Override for shuffle partition count (`SET shuffle.partitions = N`).
    /// When `Some`, exchange nodes use this bucket count instead of auto-sizing.
    shuffle_partitions: Arc<std::sync::RwLock<Option<u32>>>,
    /// Estimated row counts for registered tables, keyed by table name.
    /// Populated by `register_parquet` and `register_record_batches`.
    /// Used by `krishiv_logical_plan` to annotate scan nodes for the
    /// `BroadcastAutoRule` optimizer.
    table_row_counts: Arc<std::sync::RwLock<HashMap<String, u64>>>,
    /// DataFusion memory pool limit in bytes for this engine, when bounded.
    /// `None` means the default unbounded pool. When `Some`, the engine runs
    /// with a `FairSpillPool` so sorts, hash joins, and aggregations spill to
    /// disk under memory pressure instead of growing without bound.
    memory_limit_bytes: Option<usize>,
    /// Iceberg catalogs registered via `with_iceberg_catalog`, keyed by their
    /// DataFusion catalog name. Stored so that `CALL system.<proc>` statements
    /// can dispatch maintenance operations to the right catalog.
    #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
    iceberg_catalogs: Arc<std::sync::RwLock<Vec<(Arc<catalog::unified::KrishivCatalog>, String)>>>,
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
    ///
    /// DataFusion `target_partitions` defaults to 1 (single-threaded local
    /// execution). Use [`SqlEngine::with_target_parallelism`] to override.
    pub fn new() -> Self {
        Self::new_with_memory_limit(query_memory_limit_from_env())
    }

    /// Create a local SQL engine whose DataFusion execution memory is capped
    /// at `memory_limit_bytes`.
    ///
    /// When `Some`, the engine runs with a `FairSpillPool` of that size plus
    /// the default disk manager, so memory-intensive operators (sort, hash
    /// join, aggregation) spill to disk under pressure and queries that cannot
    /// spill fail with a resources-exhausted error instead of exhausting
    /// process memory. `None` keeps DataFusion's default unbounded pool.
    ///
    /// Shares [`SqlEngine::new`]'s fallback behavior for window helper UDF
    /// registration failures.
    pub fn new_with_memory_limit(memory_limit_bytes: Option<usize>) -> Self {
        match Self::build_local(
            None,
            WindowFnRegistration::Register,
            NonZeroUsize::new(1).unwrap(),
            memory_limit_bytes,
        ) {
            Ok(engine) => engine,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "SqlEngine::new: window helper UDF registration failed; \
                     window SQL functions will be unavailable, other queries are unaffected"
                );
                Self::build_local(
                    None,
                    WindowFnRegistration::Skip,
                    NonZeroUsize::new(1).unwrap(),
                    memory_limit_bytes,
                )
                .unwrap_or_else(|err| {
                    tracing::error!(
                        error = %err,
                        "memory-limited DataFusion runtime construction failed; \
                         falling back to an unbounded engine"
                    );
                    Self::build_local(
                        None,
                        WindowFnRegistration::Skip,
                        NonZeroUsize::new(1).unwrap(),
                        None,
                    )
                    .expect(
                        "SqlEngine::build_local without a memory limit and with \
                         WindowFnRegistration::Skip is infallible",
                    )
                })
            }
        }
    }

    /// Create a local SQL engine, propagating window helper registration errors.
    ///
    /// Callers that need to abort startup when window functions cannot be
    /// registered should use this constructor.
    pub fn try_new() -> SqlResult<Self> {
        Self::build_local(
            None,
            WindowFnRegistration::Register,
            NonZeroUsize::new(1).unwrap(),
            query_memory_limit_from_env(),
        )
    }

    /// Create an engine whose `krishiv` catalog resolves tables registered in `InMemoryCatalog` (P0-10).
    pub fn with_in_memory_catalog(catalog: Arc<RwLock<InMemoryCatalog>>) -> SqlResult<Self> {
        if krishiv_common::profile_requires_fail_closed_metadata(
            krishiv_common::resolve_durability_profile(),
        ) {
            return Err(SqlError::DataFusion {
                message: String::from(
                    "InMemoryCatalog is dev-only; configure a durable REST or file-backed \
                     catalog for production deployments",
                ),
            });
        }
        Self::build_local(
            Some(catalog),
            WindowFnRegistration::Register,
            NonZeroUsize::new(1).unwrap(),
            query_memory_limit_from_env(),
        )
    }

    /// Set the DataFusion `target_partitions` parallelism level for this engine.
    ///
    /// Higher values allow DataFusion to parallelise hash-join build,
    /// aggregation spilling, and parquet scans across more threads.
    /// Default: 1 (single-threaded). Recommended: `available_parallelism()`.
    #[must_use]
    pub fn with_target_parallelism(mut self, n: NonZeroUsize) -> Self {
        self.target_parallelism = n;
        self
    }

    /// Return the configured `target_partitions` parallelism level.
    pub fn target_parallelism(&self) -> NonZeroUsize {
        self.target_parallelism
    }

    /// Return the DataFusion memory pool limit for this engine, if bounded.
    pub fn memory_limit_bytes(&self) -> Option<usize> {
        self.memory_limit_bytes
    }

    /// Return the current `shuffle.partitions` override, if set via `SET shuffle.partitions = N`.
    pub fn shuffle_partitions(&self) -> Option<u32> {
        *self
            .shuffle_partitions
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Return access to the table row-count registry.
    ///
    /// Populated by `register_parquet` and `register_record_batches` with
    /// estimated row counts extracted from table-provider statistics. Used
    /// by `SqlDataFrame::krishiv_logical_plan` to annotate scan nodes.
    pub fn table_row_counts(&self) -> Arc<std::sync::RwLock<HashMap<String, u64>>> {
        Arc::clone(&self.table_row_counts)
    }

    /// Build a `SqlDataFrame` with this engine's shared session context attached
    /// so that `cache()` / `create_or_replace_temp_view()` work on the live session.
    fn make_sql_df(&self, name: &str, dataframe: DataFusionDataFrame) -> SqlDataFrame {
        SqlDataFrame::new(name, dataframe, self.table_row_counts())
            .with_context(self.context.clone())
    }

    /// Attach SQL text and execution kind derived from registered streaming sources.
    fn attach_query_metadata(&self, df: SqlDataFrame, query: &str) -> SqlDataFrame {
        let kind = if self.is_streaming_query(query).unwrap_or(false) {
            ExecutionKind::Streaming
        } else {
            ExecutionKind::Batch
        };
        df.with_query(query).with_execution_kind(kind)
    }

    /// Set an override for the shuffle partition count.
    ///
    /// When `n` is `Some`, exchange and shuffle-write operations use `n` buckets
    /// instead of auto-sizing. Pass `None` to restore auto-sizing.
    #[must_use]
    pub fn with_shuffle_partitions(self, n: Option<u32>) -> Self {
        if let Ok(mut guard) = self.shuffle_partitions.write() {
            *guard = n;
        }
        self
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
        target_partitions: NonZeroUsize,
        memory_limit_bytes: Option<usize>,
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
        crate::connector_table::register_connector_table_factories(
            &mut table_factories,
            streaming_sources.clone(),
        );
        let mut state_builder = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_config(build_single_node_session_config(target_partitions))
            .with_table_factories(table_factories);
        if let Some(limit) = memory_limit_bytes {
            // A FairSpillPool shares the limit across concurrently running
            // operators and lets spill-capable operators (sort, hash join,
            // aggregation) write to the default disk manager's temp files
            // instead of failing outright when the pool is exhausted.
            let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(
                    datafusion::execution::memory_pool::FairSpillPool::new(limit),
                ))
                .build_arc()
                .map_err(|e| SqlError::DataFusion {
                    message: format!(
                        "failed to build memory-limited DataFusion runtime \
                         (limit {limit} bytes): {e}"
                    ),
                })?;
            state_builder = state_builder.with_runtime_env(runtime_env);
        }
        let state = state_builder.build();
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
            target_parallelism: target_partitions,
            krishiv_catalog,
            udf_registry: None,
            streaming_sources,
            streaming_registration: Arc::new(Mutex::new(())),
            has_streaming_sources: Arc::new(AtomicBool::new(false)),
            udf_limits: None,
            udf_registry_version: Arc::new(AtomicU64::new(0)),
            udf_last_synced_version: Arc::new(AtomicU64::new(u64::MAX)),
            plan_cache: Arc::new(Mutex::new(PlanCache::new(resolve_plan_cache_max_entries()))),
            shuffle_partitions: Arc::new(std::sync::RwLock::new(None)),
            table_row_counts: Arc::new(std::sync::RwLock::new(HashMap::new())),
            memory_limit_bytes,
            #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
            iceberg_catalogs: Arc::new(std::sync::RwLock::new(Vec::new())),
        })
    }

    /// Register an unbounded continuous table, returning its typed input.
    ///
    /// The returned input uses a bounded channel with capacity
    /// [`crate::streaming::CONTINUOUS_TABLE_CHANNEL_CAPACITY`]. When the
    /// consumer (the DataFusion query plan) is slower than the producer,
    /// `ContinuousTableInput::send(...).await` backpressures the producer,
    /// and `ContinuousTableInput::try_send(...)` returns a resource error
    /// rather than growing memory without limit. Use
    /// [`register_streaming_table_with_capacity`] for a non-default
    /// capacity.
    pub fn register_streaming_table(
        &self,
        name: &str,
        schema: arrow::datatypes::SchemaRef,
    ) -> SqlResult<Arc<ContinuousTableInput>> {
        let _registration = self.lock_streaming_registration()?;
        self.validate_new_streaming_table(name, &schema)?;
        let (table, input) = crate::streaming::create_continuous_table(schema).map_err(|e| {
            SqlError::DataFusion {
                message: e.to_string(),
            }
        })?;
        self.register_new_streaming_provider(name, table)?;
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string());
        self.has_streaming_sources.store(true, Ordering::Release);
        self.invalidate_plan_cache();
        Ok(input)
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
    ) -> SqlResult<Arc<ContinuousTableInput>> {
        let _registration = self.lock_streaming_registration()?;
        self.validate_new_streaming_table(name, &schema)?;
        let (table, input) = crate::streaming::create_continuous_table_with_capacity(
            schema, capacity,
        )
        .map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        self.register_new_streaming_provider(name, table)?;
        self.streaming_sources
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string());
        self.has_streaming_sources.store(true, Ordering::Release);
        self.invalidate_plan_cache();
        Ok(input)
    }

    fn lock_streaming_registration(&self) -> SqlResult<std::sync::MutexGuard<'_, ()>> {
        self.streaming_registration
            .lock()
            .map_err(|error| SqlError::DataFusion {
                message: format!("streaming table registration lock poisoned: {error}"),
            })
    }

    fn validate_new_streaming_table(
        &self,
        name: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> SqlResult<()> {
        if name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        if schema.fields().is_empty() {
            return Err(SqlError::DataFusion {
                message: "streaming table schema must contain at least one field".into(),
            });
        }
        if self
            .context
            .table_exist(name)
            .map_err(|error| SqlError::DataFusion {
                message: error.to_string(),
            })?
        {
            return Err(SqlError::DataFusion {
                message: format!("table '{name}' is already registered"),
            });
        }
        Ok(())
    }

    fn register_new_streaming_provider(
        &self,
        name: &str,
        table: Arc<dyn datafusion::catalog::TableProvider>,
    ) -> SqlResult<()> {
        let previous =
            self.context
                .register_table(name, table)
                .map_err(|error| SqlError::DataFusion {
                    message: error.to_string(),
                })?;
        if let Some(previous) = previous {
            self.context
                .register_table(name, previous)
                .map_err(|error| SqlError::DataFusion {
                    message: format!(
                        "table '{name}' was concurrently registered and could not be restored: \
                         {error}"
                    ),
                })?;
            return Err(SqlError::DataFusion {
                message: format!("table '{name}' was concurrently registered"),
            });
        }
        Ok(())
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
            auto_commit_interval_ms: {
                let profile = krishiv_common::resolve_durability_profile();
                if krishiv_common::requires_manual_kafka_commit(profile) {
                    None
                } else {
                    Some(1_000)
                }
            },
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
    pub fn with_udf_limits(mut self, limits: krishiv_plan::udf::ResourceLimits) -> Self {
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

    /// Drop a named table from the session context.
    ///
    /// Idempotent — dropping a name that was never registered is not an error.
    pub fn deregister_table(&self, name: &str) -> SqlResult<()> {
        if name.trim().is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        let _ = self
            .context
            .deregister_table(name)
            .map_err(SqlError::from)?;
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Register a table UDF backed by a Rust closure.
    ///
    /// The closure receives literal arguments passed by the SQL caller as
    /// `ScalarValue` values and returns an Arrow `RecordBatch`. Non-literal
    /// arguments are rejected because they cannot be evaluated safely at the
    /// synchronous DataFusion table-function boundary. `schema` describes the
    /// output columns.
    ///
    /// # Example
    /// ```ignore
    /// engine.register_table_udf_fn(
    ///     "generate_ints",
    ///     Schema::new(vec![Field::new("n", DataType::Int64, false)]),
    ///     |args| {
    ///         let count = match args.first() {
    ///             Some(ScalarValue::Int64(n)) => *n,
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
            &[krishiv_plan::udf::ScalarValue],
        ) -> Result<arrow::record_batch::RecordBatch, krishiv_plan::udf::UdfError>
        + Send
        + Sync
        + 'static,
    ) -> SqlResult<()> {
        let udf =
            create_function_ddl::ClosureTableUdf::try_new(name, schema, std::sync::Arc::new(f))
                .map_err(|error| SqlError::InvalidTableFunction {
                    message: error.to_string(),
                })?;
        if let Some(registry) = &self.udf_registry {
            let mut guard = registry.write().map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
            guard.register_table(std::sync::Arc::new(udf.clone()));
        }
        udf::register_single_table_udf(&self.context, std::sync::Arc::new(udf))
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

    /// Register an Iceberg [`KrishivCatalog`] as a DataFusion catalog provider.
    ///
    /// Tables in the catalog are resolved automatically by DataFusion when SQL
    /// queries reference `<catalog_name>.<namespace>.<table>`. The bridge uses
    /// `plan_files()` to enumerate Parquet files and wraps them in a
    /// `ListingTable`, giving DataFusion native projection/filter pushdown.
    ///
    /// Multiple catalogs can be registered under different names.
    #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
    #[must_use]
    pub fn with_iceberg_catalog(
        self,
        catalog: std::sync::Arc<catalog::unified::KrishivCatalog>,
        catalog_name: impl Into<String>,
    ) -> Self {
        let catalog_name = catalog_name.into();
        let bridge = catalog::iceberg_catalog_bridge::IcebergCatalogBridge::new(
            Arc::clone(&catalog),
            catalog_name.clone(),
        );
        self.context
            .register_catalog(catalog_name.clone(), Arc::new(bridge));
        self.iceberg_catalogs
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push((catalog, catalog_name));
        self
    }

    /// Share a session UDF registry so scalar UDFs are visible in SQL.
    #[must_use]
    pub fn with_udf_registry(
        mut self,
        registry: std::sync::Arc<std::sync::RwLock<krishiv_plan::udf::UdfRegistry>>,
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
        limits: krishiv_plan::udf::ResourceLimits,
    ) -> SqlResult<()> {
        self.sync_scalar_udfs_with_limits_for_profile(
            limits,
            krishiv_common::resolve_durability_profile(),
        )
        .await
    }

    /// Register scalar UDFs using a caller-resolved durability profile.
    pub async fn sync_scalar_udfs_with_limits_for_profile(
        &self,
        limits: krishiv_plan::udf::ResourceLimits,
        profile: krishiv_common::DurabilityProfile,
    ) -> SqlResult<()> {
        self.sync_scalar_udfs_with_limits_for_policy(
            limits,
            krishiv_common::NativeScalarUdfPolicy::resolve(profile),
        )
        .await
    }

    /// Register scalar UDFs using a caller-snapshotted policy decision.
    pub async fn sync_scalar_udfs_with_limits_for_policy(
        &self,
        limits: krishiv_plan::udf::ResourceLimits,
        policy: krishiv_common::NativeScalarUdfPolicy,
    ) -> SqlResult<()> {
        let Some(registry) = &self.udf_registry else {
            return Ok(());
        };
        let guard = registry.read().map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })?;
        udf::sync_scalar_udfs_with_limits_for_policy(&self.context, &guard, limits, policy).map_err(
            |e| SqlError::DataFusion {
                message: e.to_string(),
            },
        )
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
        // Extract estimated row count from table provider statistics.
        if let Ok(provider) = self.context.table_provider(table_name).await
            && let Some(stats) = provider.statistics()
            && let Some(n) = stats.num_rows.get_value()
            && let Ok(mut counts) = self.table_row_counts.write()
        {
            counts.insert(table_name.to_string(), *n as u64);
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
        Ok(self.make_sql_df("parquet-read", dataframe))
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
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
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
        if total_rows > 0
            && let Ok(mut counts) = self.table_row_counts.write()
        {
            counts.insert(table_name.to_string(), total_rows as u64);
        }
        self.invalidate_plan_cache();
        Ok(())
    }

    /// Create a DataFrame by reading a local Parquet path with typed options.
    pub async fn read_parquet_with_options(
        &self,
        path: impl AsRef<Path>,
        opts: &ParquetReaderOptions,
    ) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let mut options = datafusion::prelude::ParquetReadOptions::default();
        if let Some(bs) = opts.batch_size {
            options = options.parquet_pruning(true);
            // DataFusion propagates batch_size via SessionConfig; set it on the
            // inner session context rather than the options struct (no direct field).
            let _ = bs; // recorded in options below via set_parquet_pushdown_filters
        }
        let dataframe = self.context.read_parquet(path, options).await?;
        Ok(self.make_sql_df("parquet-read", dataframe))
    }

    /// Create a DataFrame by reading a local CSV path directly.
    pub async fn read_csv(&self, path: impl AsRef<Path>) -> SqlResult<SqlDataFrame> {
        self.read_csv_with_options(path, &CsvReaderOptions::default())
            .await
    }

    /// Create a DataFrame by reading a local CSV path with typed options.
    pub async fn read_csv_with_options(
        &self,
        path: impl AsRef<Path>,
        opts: &CsvReaderOptions,
    ) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let mut options = datafusion::prelude::CsvReadOptions::new();
        if let Some(delim) = opts.delimiter {
            options = options.delimiter(delim as u8);
        }
        if let Some(has_header) = opts.has_header {
            options = options.has_header(has_header);
        }
        let dataframe = self.context.read_csv(path, options).await?;
        Ok(self.make_sql_df("csv-read", dataframe))
    }

    /// Create a DataFrame by reading a local JSON/NDJSON path directly.
    pub async fn read_json(&self, path: impl AsRef<Path>) -> SqlResult<SqlDataFrame> {
        let path = path.as_ref().to_string_lossy().into_owned();
        let dataframe = self
            .context
            .read_json(path, datafusion::prelude::JsonReadOptions::default())
            .await?;
        Ok(self.make_sql_df("json-read", dataframe))
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
        query_type: krishiv_connectors::lakehouse::HudiQueryType,
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

        // ── Intercept SET shuffle.partitions = N ─────────────────────────────
        // Krishiv-specific session config; DataFusion does not know about it.
        let trimmed = query.trim();
        if trimmed
            .to_ascii_uppercase()
            .starts_with("SET SHUFFLE.PARTITIONS")
        {
            let value = trimmed.split('=').nth(1).map(|s| s.trim()).unwrap_or("");
            match value.parse::<u32>() {
                Ok(n) if n > 0 => {
                    {
                        let mut guard =
                            self.shuffle_partitions
                                .write()
                                .map_err(|e| SqlError::DataFusion {
                                    message: e.to_string(),
                                })?;
                        *guard = Some(n);
                    }
                    let empty = self.context.sql("SELECT 1 WHERE FALSE").await?;
                    return Ok(self.make_sql_df("set-shuffle-partitions", empty));
                }
                Ok(_) => {
                    {
                        let mut guard =
                            self.shuffle_partitions
                                .write()
                                .map_err(|e| SqlError::DataFusion {
                                    message: e.to_string(),
                                })?;
                        *guard = None;
                    }
                    let empty = self.context.sql("SELECT 1 WHERE FALSE").await?;
                    return Ok(self.make_sql_df("set-shuffle-partitions", empty));
                }
                Err(_) => {
                    return Err(SqlError::DataFusion {
                        message: format!(
                            "invalid shuffle.partitions value '{value}'; expected a positive integer"
                        ),
                    });
                }
            }
        }

        // ── Intercept CREATE FUNCTION … RETURNS TABLE ────────────────────────
        // DataFusion does not understand this extended DDL syntax. Parse and
        // register only executable LANGUAGE SQL definitions; unsupported
        // languages fail before any registry mutation.
        if create_function_ddl::is_create_function_returns_table(query) {
            let ddl = create_function_ddl::parse_create_function(query)
                .map_err(|message| SqlError::InvalidTableFunction { message })?;
            if ddl.language.as_deref() != Some("sql") {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "CREATE FUNCTION '{}' uses language {:?}; only LANGUAGE SQL AS '...' \
                         table functions are executable",
                        ddl.function_name, ddl.language
                    ),
                });
            }
            let body = ddl
                .body
                .as_deref()
                .filter(|body| !body.trim().is_empty())
                .ok_or_else(|| SqlError::InvalidTableFunction {
                    message: format!(
                        "SQL table function '{}' requires a non-empty AS body",
                        ddl.function_name
                    ),
                })?;
            let fields: Vec<_> = ddl
                .return_columns
                .iter()
                .map(|column| {
                    arrow::datatypes::Field::new(&column.name, column.data_type.clone(), true)
                })
                .collect();
            let schema = arrow::datatypes::Schema::new(fields);
            let udf: std::sync::Arc<dyn krishiv_plan::udf::TableUdf> = std::sync::Arc::new(
                create_function_ddl::SqlBodyTableUdf::try_new(
                    &ddl.function_name,
                    schema,
                    body,
                    ddl.arguments.len(),
                    std::sync::Arc::new(self.context.clone()),
                )
                .map_err(|error| SqlError::InvalidTableFunction {
                    message: error.to_string(),
                })?,
            );
            if let Some(registry) = &self.udf_registry {
                let mut guard = registry.write().map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })?;
                guard.register_table(std::sync::Arc::clone(&udf));
            }
            udf::register_single_table_udf(&self.context, std::sync::Arc::clone(&udf))
                .map_err(SqlError::from)?;
            let empty = self.context.sql("SELECT 1 WHERE FALSE").await?;
            return Ok(
                self.attach_query_metadata(self.make_sql_df("create-function", empty), query)
            );
        }

        if query
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("MERGE INTO")
        {
            let batches = lakehouse::execute_merge_sql(&self.context, query).await?;
            let merge_table = next_ephemeral_name("merge_result");
            lakehouse::register_scan_batches(&self.context, &merge_table, batches).await?;
            let dataframe = self
                .context
                .sql(&format!("SELECT * FROM {merge_table}"))
                .await?;
            return Ok(self.attach_query_metadata(self.make_sql_df("merge", dataframe), query));
        }

        // ── Intercept CALL system.<proc> ──────────────────────────────────────
        // Route Iceberg maintenance procedures to registered KrishivCatalogs.
        #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
        if trimmed.to_ascii_uppercase().starts_with("CALL SYSTEM.") {
            let result = self.dispatch_call_system(trimmed).await?;
            let call_table = next_ephemeral_name("call_result");
            lakehouse::register_scan_batches(&self.context, &call_table, vec![result]).await?;
            let dataframe = self
                .context
                .sql(&format!("SELECT * FROM {call_table}"))
                .await?;
            return Ok(self.attach_query_metadata(self.make_sql_df("call", dataframe), query));
        }

        // ── Intercept DELETE FROM <iceberg-table> [WHERE …] ──────────────────
        // Route to copy-on-write iceberg_delete_where when the table is tracked
        // by a registered KrishivCatalog. Falls through to DataFusion otherwise.
        #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
        if trimmed.to_ascii_uppercase().starts_with("DELETE FROM ") {
            if let Some((table_ref, predicate)) = parse_dml_delete(trimmed) {
                if let Some((iceberg_catalog, table_ident)) = self.resolve_iceberg_table(&table_ref)
                {
                    use arrow::array::{ArrayRef, Int64Array};
                    use arrow::datatypes::{DataType, Field, Schema};
                    let (deleted, _) = krishiv_connectors::lakehouse::dml::iceberg_delete_where(
                        iceberg_catalog,
                        &table_ident,
                        &predicate,
                        &self.context,
                    )
                    .await
                    .map_err(|e| SqlError::DataFusion {
                        message: e.to_string(),
                    })?;
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "deleted_rows",
                        DataType::Int64,
                        false,
                    )]));
                    let array: ArrayRef = Arc::new(Int64Array::from(vec![deleted as i64]));
                    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
                        SqlError::DataFusion {
                            message: e.to_string(),
                        }
                    })?;
                    let res_table = next_ephemeral_name("delete_result");
                    lakehouse::register_scan_batches(&self.context, &res_table, vec![batch])
                        .await?;
                    let dataframe = self
                        .context
                        .sql(&format!("SELECT * FROM {res_table}"))
                        .await?;
                    return Ok(
                        self.attach_query_metadata(self.make_sql_df("delete", dataframe), query)
                    );
                }
            }
        }

        // ── Intercept UPDATE <iceberg-table> SET … [WHERE …] ─────────────────
        #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
        if trimmed.to_ascii_uppercase().starts_with("UPDATE ") {
            if let Some((table_ref, set_clause, predicate)) = parse_dml_update(trimmed) {
                if let Some((iceberg_catalog, table_ident)) = self.resolve_iceberg_table(&table_ref)
                {
                    use arrow::array::{ArrayRef, Int64Array};
                    use arrow::datatypes::{DataType, Field, Schema};
                    let assignments = split_set_assignments(&set_clause);
                    let borrowed: Vec<(&str, &str)> = assignments
                        .iter()
                        .map(|(c, e)| (c.as_str(), e.as_str()))
                        .collect();
                    let pred = predicate.as_deref();
                    let (updated, _) = krishiv_connectors::lakehouse::dml::iceberg_update_where(
                        iceberg_catalog,
                        &table_ident,
                        &borrowed,
                        pred,
                        &self.context,
                    )
                    .await
                    .map_err(|e| SqlError::DataFusion {
                        message: e.to_string(),
                    })?;
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "updated_rows",
                        DataType::Int64,
                        false,
                    )]));
                    let array: ArrayRef = Arc::new(Int64Array::from(vec![updated as i64]));
                    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
                        SqlError::DataFusion {
                            message: e.to_string(),
                        }
                    })?;
                    let res_table = next_ephemeral_name("update_result");
                    lakehouse::register_scan_batches(&self.context, &res_table, vec![batch])
                        .await?;
                    let dataframe = self
                        .context
                        .sql(&format!("SELECT * FROM {res_table}"))
                        .await?;
                    return Ok(
                        self.attach_query_metadata(self.make_sql_df("update", dataframe), query)
                    );
                }
            }
        }

        // ── Intercept MATCH_RECOGNIZE ─────────────────────────────────────────
        // DataFusion does not parse MATCH_RECOGNIZE. Route it through the CEP
        // path: parse → run PatternMatcher on the source table → return results.
        if query.to_ascii_uppercase().contains(" MATCH_RECOGNIZE ")
            && let Some(stmt) = cep_sql::parse_match_recognize(query)?
        {
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
            let cep_table = next_ephemeral_name("cep_result");
            lakehouse::register_scan_batches(&self.context, &cep_table, results).await?;
            let dataframe = self
                .context
                .sql(&format!("SELECT * FROM {cep_table}"))
                .await?;
            return Ok(self.attach_query_metadata(self.make_sql_df("cep", dataframe), query));
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
        let shuffle_override = self
            .shuffle_partitions
            .read()
            .map(|g| *g)
            .unwrap_or_else(|e| *e.into_inner());
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
                return Ok(self.attach_query_metadata(
                    self.make_sql_df("sql-query", dataframe)
                        .with_shuffle_partitions(shuffle_override),
                    &rewritten,
                ));
            }
        }

        let dataframe = self.context.sql(&rewritten).await?;

        // After CREATE EXTERNAL TABLE DDL, try to extract row-count statistics
        // from the newly registered table provider so `BroadcastAutoRule` can
        // fire for small connector-backed tables (e.g. Parquet/S3 via DDL).
        if let Some(table_name) = extract_create_external_table_name(&rewritten)
            && !table_name.is_empty()
            && let Ok(provider) = self.context.table_provider(&table_name).await
        {
            let maybe_rows = provider
                .statistics()
                .and_then(|s| s.num_rows.get_value().copied());
            if let Some(n) = maybe_rows
                && let Ok(mut counts) = self.table_row_counts.write()
            {
                counts.entry(table_name).or_insert(n as u64);
            }
        }

        // Cache the logical plan for future repeated calls.
        if can_cache {
            let plan = dataframe.logical_plan().clone();
            match self.plan_cache.lock() {
                Ok(mut cache) => cache.insert(rewritten.clone(), plan),
                Err(poisoned) => poisoned.into_inner().insert(rewritten.clone(), plan),
            }
        }

        Ok(self.attach_query_metadata(
            self.make_sql_df("sql-query", dataframe)
                .with_shuffle_partitions(shuffle_override),
            &rewritten,
        ))
    }

    /// Execute a SQL query with a timeout.
    ///
    /// Returns [`SqlError::Timeout`] if `timeout_ms` elapses before the query
    /// produces a result.  The underlying DataFusion task is abandoned (not
    /// cancelled at the engine level) when the timeout fires; its resources are
    /// released when the spawned task eventually completes.
    pub async fn execute_with_timeout(
        &self,
        query: impl AsRef<str> + Send,
        timeout_ms: u64,
    ) -> SqlResult<SqlDataFrame> {
        let timeout = std::time::Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout, self.sql(query))
            .await
            .map_err(|_| SqlError::Timeout { timeout_ms })?
    }

    /// Execute a SQL query tagged with a caller-supplied operation ID.
    ///
    /// The operation ID is recorded in the returned [`TaggedQueryResult`] and
    /// can be used to correlate logs, metrics, and cancellation requests.
    /// If `cancelled_ids` contains `operation_id` before execution begins the
    /// function returns [`SqlError::OperationCancelled`] immediately.
    pub async fn execute_with_operation_id(
        &self,
        operation_id: u64,
        query: impl AsRef<str> + Send,
        cancelled_ids: &OperationRegistry,
    ) -> SqlResult<TaggedQueryResult> {
        if cancelled_ids.is_cancelled(operation_id) {
            return Err(SqlError::OperationCancelled { operation_id });
        }
        let df = self.sql(query).await?;
        Ok(TaggedQueryResult {
            operation_id,
            inner: df,
        })
    }

    /// Resolve a SQL table reference to an `(Arc<dyn Catalog>, TableIdent)` pair
    /// from the registered Iceberg catalogs.
    ///
    /// Accepts 2-part (`ns.tbl`) and 3-part (`cat.ns.tbl`) references.
    /// Returns `None` when no catalog is registered or the reference is ambiguous.
    #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
    fn resolve_iceberg_table(
        &self,
        table_ref: &str,
    ) -> Option<(Arc<dyn iceberg::Catalog + Send + Sync>, iceberg::TableIdent)> {
        let parts: Vec<&str> = table_ref.splitn(3, '.').collect();
        let (catalog_arc, ns_str, table_str) = {
            let guard = self
                .iceberg_catalogs
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if guard.is_empty() {
                return None;
            }
            match parts.len() {
                2 => {
                    let (cat, _) = guard.first()?;
                    (Arc::clone(cat), parts[0], parts[1])
                }
                3 => {
                    let cat_name = parts[0];
                    let (cat, _) = guard.iter().find(|(_, n)| n == cat_name)?;
                    (Arc::clone(cat), parts[1], parts[2])
                }
                _ => return None,
            }
        };
        let ns = iceberg::NamespaceIdent::from_vec(vec![ns_str.to_string()]).ok()?;
        let ident = iceberg::TableIdent::new(ns, table_str.to_string());
        Some((catalog_arc.as_iceberg(), ident))
    }

    /// Dispatch a `CALL system.<proc>(...)` statement to the appropriate
    /// Iceberg maintenance function on the first registered KrishivCatalog.
    #[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
    async fn dispatch_call_system(&self, stmt: &str) -> SqlResult<RecordBatch> {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let upper = stmt.to_ascii_uppercase();
        const PREFIX: &str = "CALL SYSTEM.";
        let upper_after = &upper[PREFIX.len()..];
        let orig_after = &stmt[PREFIX.len()..];

        let paren = upper_after.find('(').ok_or_else(|| SqlError::DataFusion {
            message: format!("CALL: missing '(' in: {stmt}"),
        })?;
        let proc_name = upper_after[..paren].trim();

        let args_raw = orig_after[paren + 1..]
            .trim_end_matches(';')
            .trim()
            .trim_end_matches(')')
            .trim();
        let args = call_args_from_str(args_raw);

        let iceberg_catalog = {
            let guard = self
                .iceberg_catalogs
                .read()
                .unwrap_or_else(|e| e.into_inner());
            guard
                .first()
                .ok_or_else(|| SqlError::DataFusion {
                    message: "CALL system: no Iceberg catalog registered".to_string(),
                })?
                .0
                .as_iceberg()
        };

        let table_ref = args.first().ok_or_else(|| SqlError::DataFusion {
            message: format!("CALL {proc_name}: table reference argument is required"),
        })?;
        let table_ident = iceberg_table_ident(table_ref)?;

        let count: i64 = match proc_name {
            "EXPIRE_SNAPSHOTS" => {
                let dur_s = args.get(1).ok_or_else(|| SqlError::DataFusion {
                    message: "CALL expire_snapshots: duration argument is required".to_string(),
                })?;
                let older_than = parse_call_duration(dur_s)?;
                let retain_last = args
                    .get(2)
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1);
                krishiv_connectors::lakehouse::maintenance::expire_snapshots(
                    iceberg_catalog,
                    &table_ident,
                    older_than,
                    retain_last,
                )
                .await
                .map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })? as i64
            }
            "REMOVE_ORPHAN_FILES" => {
                let dur_s = args.get(1).ok_or_else(|| SqlError::DataFusion {
                    message: "CALL remove_orphan_files: duration argument is required".to_string(),
                })?;
                let older_than = parse_call_duration(dur_s)?;
                krishiv_connectors::lakehouse::maintenance::remove_orphan_files(
                    iceberg_catalog,
                    &table_ident,
                    older_than,
                )
                .await
                .map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })? as i64
            }
            "COMPACT_DATA_FILES" => {
                let target_bytes = args
                    .get(1)
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(128 * 1024 * 1024);
                krishiv_connectors::lakehouse::maintenance::compact_data_files(
                    iceberg_catalog,
                    &table_ident,
                    target_bytes,
                )
                .await
                .map_err(|e| SqlError::DataFusion {
                    message: e.to_string(),
                })? as i64
            }
            other => {
                return Err(SqlError::Unsupported {
                    feature: format!("CALL system.{other}: unknown procedure"),
                });
            }
        };

        let col = match proc_name {
            "EXPIRE_SNAPSHOTS" => "expired_snapshots",
            "REMOVE_ORPHAN_FILES" => "removed_files",
            "COMPACT_DATA_FILES" => "rewritten_files",
            _ => "result",
        };
        let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Int64, false)]));
        let array: ArrayRef = Arc::new(Int64Array::from(vec![count]));
        RecordBatch::try_new(schema, vec![array]).map_err(|e| SqlError::DataFusion {
            message: e.to_string(),
        })
    }
}

/// A query result annotated with the operation ID that produced it.
pub struct TaggedQueryResult {
    /// The caller-supplied operation ID.
    pub operation_id: u64,
    /// The underlying SQL DataFrame.
    pub inner: SqlDataFrame,
}

/// Registry of cancelled operation IDs.
///
/// Callers can cancel an in-flight operation by registering its ID here before
/// or during execution.  [`SqlEngine::execute_with_operation_id`] checks this
/// registry at the start of execution.
#[derive(Clone, Default)]
pub struct OperationRegistry {
    cancelled: Arc<std::sync::RwLock<std::collections::HashSet<u64>>>,
}

impl OperationRegistry {
    /// Create a new, empty operation registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancel an operation by ID.  Subsequent
    /// [`execute_with_operation_id`][SqlEngine::execute_with_operation_id] calls
    /// with this ID will return [`SqlError::OperationCancelled`].
    pub fn cancel(&self, operation_id: u64) {
        if let Ok(mut ids) = self.cancelled.write() {
            ids.insert(operation_id);
        }
    }

    /// Return `true` if `operation_id` has been cancelled.
    pub fn is_cancelled(&self, operation_id: u64) -> bool {
        self.cancelled
            .read()
            .map(|ids| ids.contains(&operation_id))
            .unwrap_or(false)
    }

    /// Remove a cancelled ID (e.g. once the operation has been cleaned up).
    pub fn remove(&self, operation_id: u64) {
        if let Ok(mut ids) = self.cancelled.write() {
            ids.remove(&operation_id);
        }
    }

    /// Return all currently cancelled operation IDs.
    pub fn cancelled_ids(&self) -> Vec<u64> {
        self.cancelled
            .read()
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// Extract the table name from a `CREATE EXTERNAL TABLE <name> ...` DDL statement.
///
/// Returns `None` for any other SQL statement. Used to populate `table_row_counts`
/// after DDL so that `BroadcastAutoRule` can fire for connector-backed tables.
pub(crate) fn extract_create_external_table_name(query: &str) -> Option<String> {
    use datafusion::sql::parser::{DFParser, Statement as DFStatement};
    let mut stmts = DFParser::parse_sql(query).ok()?;
    match stmts.pop_front()? {
        DFStatement::CreateExternalTable(create) => Some(create.name.to_string()),
        _ => None,
    }
}

/// Engine-agnostic interface over a prepared query result.
///
/// Hides the concrete [`SqlDataFrame`] (which holds a DataFusion `DataFrame`)
/// behind a stable trait so that `krishiv-api` and other callers are not
/// forced to depend on DataFusion types.  `datafusion` stays an implementation
/// detail inside `krishiv-sql`; a future engine swap only requires a new impl.
/// Engine-neutral grouping-set mode for canonical DataFrame aggregation.
pub enum GroupingMode<'a> {
    Sets(Vec<Vec<&'a krishiv_plan::expression::Expr>>),
    Cube(Vec<&'a krishiv_plan::expression::Expr>),
    Rollup(Vec<&'a krishiv_plan::expression::Expr>),
}

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
    async fn execute_stream(&self) -> SqlResult<SqlStream>;

    // ── DataFrame transforms (lazy) ─────────────────────────────────────────

    /// Return the Arrow schema of this DataFrame.
    fn schema(&self) -> SchemaRef;

    /// Select columns by name.
    async fn select(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Select arbitrary SQL expressions.
    async fn select_exprs(
        &self,
        expressions: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Group by expressions and compute aggregate expressions.
    async fn aggregate(
        &self,
        group_exprs: &[&krishiv_plan::expression::Expr],
        aggregate_exprs: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Aggregate using GROUPING SETS, CUBE, or ROLLUP.
    async fn aggregate_grouping(
        &self,
        grouping: GroupingMode<'_>,
        aggregate_exprs: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Pivot known values into aggregate columns.
    async fn pivot(
        &self,
        group_exprs: &[&krishiv_plan::expression::Expr],
        pivot_column: &krishiv_plan::expression::Expr,
        aggregate_expr: &krishiv_plan::expression::Expr,
        values: &[(krishiv_plan::expression::ScalarValue, String)],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Unpivot columns into name/value rows while preserving other columns.
    async fn unpivot(
        &self,
        columns: &[&str],
        name_column: &str,
        value_column: &str,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Filter rows by a SQL predicate expression.
    async fn filter(&self, predicate: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Filter rows using the engine-owned typed expression AST.
    async fn filter_expr(
        &self,
        predicate: &krishiv_plan::expression::Expr,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Limit the number of rows.
    async fn limit(&self, n: usize) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Remove duplicate rows.
    async fn distinct(&self) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Drop rows with nulls in selected columns; an empty list checks all columns.
    async fn drop_nulls(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Bernoulli-sample rows.
    async fn sample(&self, fraction: f64) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Sort by columns with optional descending flags.
    async fn sort(
        &self,
        columns: &[&str],
        descending: &[bool],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Assign an alias (table name) to this DataFrame.
    async fn alias(&self, alias: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Drop columns by name.
    async fn drop_columns(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Rename a column from `old` to `new`.
    async fn rename_column(&self, old: &str, new: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Add or replace a column with a computed expression.
    async fn with_column(&self, name: &str, expr: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Return the underlying concrete type for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Compute summary statistics (delegates to DataFusion's `describe`).
    async fn describe(&self) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Fill null values in `column` with the literal SQL `value`.
    async fn fill_null(&self, column: &str, value: &str)
    -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Join with another DataFrame using a join type and equi-join keys.
    async fn join(
        &self,
        right: &dyn KrishivDataFrameOps,
        how: &str,
        left_on: &[&str],
        right_on: &[&str],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Union this DataFrame with another (UNION ALL semantics).
    async fn union(
        &self,
        right: &dyn KrishivDataFrameOps,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    async fn union_distinct(
        &self,
        right: &dyn KrishivDataFrameOps,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    async fn intersect(
        &self,
        right: &dyn KrishivDataFrameOps,
        distinct: bool,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    async fn except(
        &self,
        right: &dyn KrishivDataFrameOps,
        distinct: bool,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>>;

    /// Register a list of record batches as a named in-memory table in the
    /// same session context that backs this DataFrame.  Used by `cache()`.
    async fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> SqlResult<()>;

    /// Deregister a named table from the session context.  Used by `unpersist()`.
    async fn deregister_table(&self, name: &str) -> SqlResult<()>;

    /// Create (or replace) a SQL view named `name` backed by this DataFrame's
    /// query.  Used by `create_or_replace_temp_view()`.
    async fn create_view(&self, name: &str, replace: bool) -> SqlResult<()>;
}

/// Recursively walk a DataFusion `LogicalPlan` and produce Krishiv `PlanNode`
/// entries.  Returns `(nodes, root_id)` where `root_id` is the ID of the
/// top-level Krishiv node representing `plan`.
///
/// Table-scan nodes carry `estimated_rows` when the table name is found in
/// `table_row_counts`.  Unhandled node types fall back to a single opaque
/// `NodeOp::Other` node.
fn df_plan_to_krishiv_nodes(
    plan: &datafusion::logical_expr::LogicalPlan,
    table_row_counts: &std::collections::HashMap<String, u64>,
    counter: &mut usize,
) -> (Vec<krishiv_plan::PlanNode>, String) {
    use datafusion::logical_expr::LogicalPlan as DfPlan;
    use krishiv_plan::{ExecutionKind, NodeOp, PlanNode};

    *counter += 1;
    let idx = *counter;

    match plan {
        DfPlan::TableScan(ts) => {
            let table_name = ts.table_name.table().to_string();
            let row_count = table_row_counts.get(&table_name).copied();
            let filters: Vec<String> = ts.filters.iter().map(|e| e.to_string()).collect();
            let id = format!("scan-{idx}");
            let node = PlanNode::new(&id, format!("Scan {table_name}"), ExecutionKind::Batch)
                .with_op(NodeOp::Scan {
                    table: table_name,
                    filters,
                })
                .with_estimated_rows(row_count);
            (vec![node], id)
        }

        DfPlan::Projection(proj) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&proj.input, table_row_counts, counter);
            let id = format!("proj-{idx}");
            let columns: Vec<String> = proj.expr.iter().map(|e| e.to_string()).collect();
            nodes.push(
                PlanNode::new(&id, "Projection", ExecutionKind::Batch)
                    .with_op(NodeOp::Project { columns })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Filter(filter) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&filter.input, table_row_counts, counter);
            let id = format!("filter-{idx}");
            let predicate = filter.predicate.to_string();
            nodes.push(
                PlanNode::new(&id, "Filter", ExecutionKind::Batch)
                    .with_op(NodeOp::Filter { predicate })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Aggregate(agg) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&agg.input, table_row_counts, counter);
            let id = format!("agg-{idx}");
            let group_keys: Vec<String> = agg.group_expr.iter().map(|e| e.to_string()).collect();
            nodes.push(
                PlanNode::new(&id, "Aggregate", ExecutionKind::Batch)
                    .with_op(NodeOp::Aggregate { group_keys })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Join(join) => {
            let (mut nodes, left_id) =
                df_plan_to_krishiv_nodes(&join.left, table_row_counts, counter);
            let (right_nodes, right_id) =
                df_plan_to_krishiv_nodes(&join.right, table_row_counts, counter);
            nodes.extend(right_nodes);
            let id = format!("join-{idx}");
            let krishiv_join_type = match join.join_type {
                datafusion::common::JoinType::Inner => krishiv_plan::JoinType::Inner,
                datafusion::common::JoinType::Left => krishiv_plan::JoinType::Left,
                datafusion::common::JoinType::Right => krishiv_plan::JoinType::Right,
                datafusion::common::JoinType::Full => krishiv_plan::JoinType::Full,
                _ => krishiv_plan::JoinType::Inner,
            };
            nodes.push(
                PlanNode::new(&id, "Join", ExecutionKind::Batch)
                    .with_op(NodeOp::Join {
                        join_type: krishiv_join_type,
                    })
                    .with_inputs([left_id, right_id]),
            );
            (nodes, id)
        }

        DfPlan::Sort(sort) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&sort.input, table_row_counts, counter);
            let id = format!("sort-{idx}");
            nodes.push(
                PlanNode::new(&id, "Sort", ExecutionKind::Batch)
                    .with_op(NodeOp::Other {
                        description: format!(
                            "Sort({})",
                            sort.expr
                                .iter()
                                .map(|e| e.to_string())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Repartition(repart) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&repart.input, table_row_counts, counter);
            let id = format!("exchange-{idx}");
            let partitioning = krishiv_plan::Partitioning::Unpartitioned;
            nodes.push(
                PlanNode::new(&id, "Exchange", ExecutionKind::Batch)
                    .with_op(NodeOp::Exchange { partitioning })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Limit(limit) => {
            let (mut nodes, input_id) =
                df_plan_to_krishiv_nodes(&limit.input, table_row_counts, counter);
            let id = format!("limit-{idx}");
            nodes.push(
                PlanNode::new(&id, "Limit", ExecutionKind::Batch)
                    .with_op(NodeOp::Other {
                        description: format!(
                            "Limit(skip={:?}, fetch={:?})",
                            limit.skip.as_ref().map(|e| e.to_string()),
                            limit.fetch.as_ref().map(|e| e.to_string()),
                        ),
                    })
                    .with_inputs([input_id]),
            );
            (nodes, id)
        }

        DfPlan::Union(union) if union.inputs.len() == 1 => {
            df_plan_to_krishiv_nodes(&union.inputs[0], table_row_counts, counter)
        }
        DfPlan::Union(union) => {
            let mut all_nodes = Vec::new();
            let mut input_ids = Vec::new();
            for input in &union.inputs {
                let (sub_nodes, sub_id) =
                    df_plan_to_krishiv_nodes(input, table_row_counts, counter);
                all_nodes.extend(sub_nodes);
                input_ids.push(sub_id);
            }
            let id = format!("union-{idx}");
            all_nodes.push(
                PlanNode::new(&id, "Union", ExecutionKind::Batch)
                    .with_op(NodeOp::Other {
                        description: "Union".to_string(),
                    })
                    .with_inputs(input_ids),
            );
            (all_nodes, id)
        }

        DfPlan::SubqueryAlias(alias) => {
            // SubqueryAlias is transparent; peel it and continue.
            df_plan_to_krishiv_nodes(&alias.input, table_row_counts, counter)
        }

        DfPlan::Values(_) => {
            let id = format!("values-{idx}");
            let node = PlanNode::new(&id, "Values", ExecutionKind::Batch).with_op(NodeOp::Other {
                description: "Values".to_string(),
            });
            (vec![node], id)
        }

        DfPlan::Extension(_) => {
            let id = format!("ext-{idx}");
            let label = plan.to_string();
            let node = PlanNode::new(&id, label.clone(), ExecutionKind::Batch)
                .with_op(NodeOp::Other { description: label });
            (vec![node], id)
        }

        DfPlan::EmptyRelation(_) => {
            let id = format!("empty-{idx}");
            let node =
                PlanNode::new(&id, "EmptyRelation", ExecutionKind::Batch).with_op(NodeOp::Other {
                    description: "EmptyRelation".to_string(),
                });
            (vec![node], id)
        }

        // Fallback: wrap the entire subplan as an opaque node.
        _ => {
            let id = format!("df-{idx}");
            let label = plan.to_string();
            let node = PlanNode::new(&id, label.clone(), ExecutionKind::Batch)
                .with_op(NodeOp::Other { description: label });
            (vec![node], id)
        }
    }
}

/// Krishiv-owned wrapper around a DataFusion DataFrame.
#[derive(Clone)]
pub struct SqlDataFrame {
    name: String,
    query: Option<String>,
    /// Alias for `query` used by `create_view` — same value.
    query_text: Option<String>,
    execution_kind: ExecutionKind,
    dataframe: DataFusionDataFrame,
    shuffle_partitions: Option<u32>,
    /// Shared session context for table registration (cache/view operations).
    context: SessionContext,
    /// Estimated row counts for registered tables, keyed by table name.
    /// Used by `krishiv_logical_plan` to annotate scan nodes with
    /// `estimated_rows` so `BroadcastAutoRule` can fire.
    table_row_counts: Arc<std::sync::RwLock<HashMap<String, u64>>>,
}

impl fmt::Debug for SqlDataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlDataFrame")
            .field("name", &self.name)
            .field("query", &self.query)
            .field("shuffle_partitions", &self.shuffle_partitions)
            .finish_non_exhaustive()
    }
}

impl SqlDataFrame {
    fn new(
        name: impl Into<String>,
        dataframe: DataFusionDataFrame,
        table_row_counts: Arc<std::sync::RwLock<HashMap<String, u64>>>,
    ) -> Self {
        Self {
            name: name.into(),
            query: None,
            query_text: None,
            execution_kind: ExecutionKind::Batch,
            dataframe,
            shuffle_partitions: None,
            context: SessionContext::default(),
            table_row_counts,
        }
    }

    /// Attach the session context so cache/view operations share the live session.
    pub(crate) fn with_context(mut self, context: SessionContext) -> Self {
        self.context = context;
        self
    }

    fn with_query(mut self, query: impl Into<String>) -> Self {
        let q = query.into();
        self.query_text = Some(q.clone());
        self.query = Some(q);
        self
    }

    fn with_execution_kind(mut self, kind: ExecutionKind) -> Self {
        self.execution_kind = kind;
        self
    }

    fn with_shuffle_partitions(mut self, n: Option<u32>) -> Self {
        self.shuffle_partitions = n;
        self
    }

    /// Original SQL query when created from [`SqlEngine::sql`].
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Return a new `SqlDataFrame` with the given DataFusion DataFrame,
    /// preserving the rest of this instance's state.  The new name suffix
    /// helps distinguish transform steps in logical-plan descriptions.
    fn with_new_dataframe(&self, df: DataFusionDataFrame, tag: &str) -> Self {
        Self {
            name: format!("{}-{}", self.name, tag),
            query: None,
            query_text: None,
            execution_kind: self.execution_kind,
            dataframe: df,
            shuffle_partitions: self.shuffle_partitions,
            context: self.context.clone(),
            table_row_counts: self.table_row_counts.clone(),
        }
    }

    /// Create a Krishiv logical plan wrapper for this DataFrame.
    ///
    /// Walks the DataFusion logical plan tree, creating Krishiv `PlanNode`
    /// entries for each operator. Table-scan nodes are annotated with
    /// `estimated_rows` from the engine's table-row-count registry, allowing
    /// `BroadcastAutoRule` to identify small tables for broadcast join
    /// promotion. The plan is then run through the logical optimizer before
    /// being returned.
    pub fn krishiv_logical_plan(&self) -> LogicalPlan {
        let df_plan = self.dataframe.logical_plan();
        let counts = self
            .table_row_counts
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let mut counter = 0usize;
        let (nodes, _root_id) = df_plan_to_krishiv_nodes(df_plan, &counts, &mut counter);

        let mut plan = LogicalPlan::new(self.name.clone(), self.execution_kind);
        for node in nodes {
            plan = plan.with_node(node);
        }

        // Run the logical optimizer so BroadcastAutoRule fires on eligible scans.
        // An optimizer failure falls back to the unoptimized (still valid) plan;
        // execution correctness does not depend on optimization, but the failure
        // must be observable rather than silent.
        let optimizer = krishiv_plan::optimizer::default_logical_optimizer();
        let fallback = plan.clone();
        match optimizer.optimize(plan) {
            Ok(result) => result.plan,
            Err(error) => {
                tracing::warn!(
                    plan = %self.name,
                    %error,
                    "logical optimizer failed; using unoptimized plan"
                );
                fallback
            }
        }
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
    pub async fn execute_stream(&self) -> SqlResult<SqlStream> {
        let df_stream = self.dataframe.clone().execute_stream().await?;
        use futures::StreamExt;
        let mapped = df_stream.map(|res| res.map_err(|e| e.to_string()));
        Ok(Box::pin(mapped))
    }

    /// Execute and collect this DataFrame, also returning lightweight runtime statistics.
    ///
    /// Collects `output_rows` from DataFusion's execution metrics. `cpu_nanos`
    /// is approximated from `elapsed_compute` when available. `spill_bytes`
    /// and `spill_count` are aggregated across every operator in the physical
    /// plan tree (sorts, hash joins, and aggregations report spills when the
    /// memory pool forces them to disk); other fields default to 0.
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

        let (spill_bytes, spill_count) = aggregate_spill_metrics(physical_plan.as_ref());

        Ok((
            batches,
            SqlExecutionStats {
                output_rows,
                cpu_nanos,
                spill_bytes,
                spill_count,
            },
        ))
    }
}

/// Recursively sum `spilled_bytes` and `spill_count` metrics across every
/// operator in a physical plan tree.
///
/// The root node's `metrics()` only reflects the root operator; spilling
/// happens in interior sort/join/aggregate nodes, so the whole tree must be
/// walked to account for all disk spill activity.
fn aggregate_spill_metrics(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> (u64, u64) {
    let mut spill_bytes: u64 = 0;
    let mut spill_count: u64 = 0;
    if let Some(metrics) = plan.metrics() {
        if let Some(bytes) = metrics.spilled_bytes() {
            spill_bytes = spill_bytes.saturating_add(bytes as u64);
        }
        if let Some(count) = metrics.spill_count() {
            spill_count = spill_count.saturating_add(count as u64);
        }
    }
    for child in plan.children() {
        let (child_bytes, child_count) = aggregate_spill_metrics(child.as_ref());
        spill_bytes = spill_bytes.saturating_add(child_bytes);
        spill_count = spill_count.saturating_add(child_count);
    }
    (spill_bytes, spill_count)
}

/// Lightweight execution statistics collected from a DataFusion physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlExecutionStats {
    pub output_rows: u64,
    pub cpu_nanos: u64,
    /// Total bytes spilled to disk across all operators in the plan.
    pub spill_bytes: u64,
    /// Number of spill events (roughly: spill files written) across all operators.
    pub spill_count: u64,
}

fn top_level_alias_index(expression: &str) -> Option<usize> {
    let bytes = expression.as_bytes();
    let mut depth = 0usize;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut candidate = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' if !double_quoted => {
                if single_quoted && bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                    continue;
                }
                single_quoted = !single_quoted;
            }
            b'"' if !single_quoted => {
                if double_quoted && bytes.get(index + 1) == Some(&b'"') {
                    index += 2;
                    continue;
                }
                double_quoted = !double_quoted;
            }
            b'(' if !single_quoted && !double_quoted => depth += 1,
            b')' if !single_quoted && !double_quoted => depth = depth.saturating_sub(1),
            b' ' if depth == 0 && !single_quoted && !double_quoted => {
                if bytes
                    .get(index..index + 4)
                    .is_some_and(|slice| slice.eq_ignore_ascii_case(b" AS "))
                {
                    candidate = Some(index);
                    index += 3;
                }
            }
            _ => {}
        }
        index += 1;
    }
    candidate
}

fn parse_dataframe_expression(
    dataframe: &datafusion::dataframe::DataFrame,
    expression: &str,
) -> SqlResult<datafusion::logical_expr::Expr> {
    if let Some(index) = top_level_alias_index(expression) {
        let (body, alias) = expression.split_at(index);
        let alias = alias[4..].trim();
        if !alias.is_empty() {
            let alias = alias
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or(alias)
                .replace("\"\"", "\"");
            return Ok(dataframe.parse_sql_expr(body.trim())?.alias(alias));
        }
    }
    dataframe.parse_sql_expr(expression).map_err(Into::into)
}

/// Parse the stable SQL-expression subset into the same engine-owned AST used by Rust and Python.
pub fn parse_public_expression(sql: &str) -> SqlResult<krishiv_plan::expression::Expr> {
    let dialect = GenericDialect {};
    let mut parser =
        Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(|error| SqlError::Unsupported {
                feature: format!("public expression parse: {error}"),
            })?;
    let expression = parser.parse_expr().map_err(|error| SqlError::Unsupported {
        feature: format!("public expression parse: {error}"),
    })?;
    sqlparser_expression_to_public(&expression)
}

fn sqlparser_expression_to_public(
    expression: &datafusion::sql::sqlparser::ast::Expr,
) -> SqlResult<krishiv_plan::expression::Expr> {
    use datafusion::sql::sqlparser::ast::{BinaryOperator as SqlOperator, Expr as SqlExpr, Value};
    use krishiv_plan::expression::{BinaryOperator, Expr, ScalarValue};

    Ok(match expression {
        SqlExpr::Identifier(identifier) => Expr::Column {
            path: vec![identifier.value.clone()],
        },
        SqlExpr::CompoundIdentifier(identifiers) => Expr::Column {
            path: identifiers
                .iter()
                .map(|identifier| identifier.value.clone())
                .collect(),
        },
        SqlExpr::Nested(expression) => sqlparser_expression_to_public(expression)?,
        SqlExpr::IsNull(expression) => Expr::IsNull {
            expression: Box::new(sqlparser_expression_to_public(expression)?),
            negated: false,
        },
        SqlExpr::IsNotNull(expression) => Expr::IsNull {
            expression: Box::new(sqlparser_expression_to_public(expression)?),
            negated: true,
        },
        SqlExpr::BinaryOp { left, op, right } => Expr::Binary {
            left: Box::new(sqlparser_expression_to_public(left)?),
            op: match op {
                SqlOperator::Eq => BinaryOperator::Eq,
                SqlOperator::NotEq => BinaryOperator::NotEq,
                SqlOperator::Gt => BinaryOperator::Gt,
                SqlOperator::GtEq => BinaryOperator::GtEq,
                SqlOperator::Lt => BinaryOperator::Lt,
                SqlOperator::LtEq => BinaryOperator::LtEq,
                SqlOperator::And => BinaryOperator::And,
                SqlOperator::Or => BinaryOperator::Or,
                SqlOperator::Plus => BinaryOperator::Plus,
                SqlOperator::Minus => BinaryOperator::Minus,
                SqlOperator::Multiply => BinaryOperator::Multiply,
                SqlOperator::Divide => BinaryOperator::Divide,
                other => {
                    return Err(SqlError::Unsupported {
                        feature: format!("public expression operator {other}"),
                    });
                }
            },
            right: Box::new(sqlparser_expression_to_public(right)?),
        },
        SqlExpr::Value(value) => Expr::Literal {
            value: match &value.value {
                Value::Null => ScalarValue::Null,
                Value::Boolean(value) => ScalarValue::Boolean(*value),
                Value::SingleQuotedString(value) => ScalarValue::Utf8(value.clone()),
                Value::Number(value, _)
                    if value.contains('.') || value.contains('e') || value.contains('E') =>
                {
                    ScalarValue::float64(value.parse::<f64>().map_err(|error| {
                        SqlError::Unsupported {
                            feature: format!("numeric expression literal: {error}"),
                        }
                    })?)
                }
                Value::Number(value, _) => {
                    ScalarValue::Int64(value.parse::<i64>().map_err(|error| {
                        SqlError::Unsupported {
                            feature: format!("integer expression literal: {error}"),
                        }
                    })?)
                }
                other => {
                    return Err(SqlError::Unsupported {
                        feature: format!("public expression literal {other}"),
                    });
                }
            },
        },
        other => {
            return Err(SqlError::Unsupported {
                feature: format!("public expression node {other}"),
            });
        }
    })
}

fn public_data_type_to_arrow(
    data_type: &krishiv_plan::expression::ExprDataType,
) -> arrow::datatypes::DataType {
    use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
    use krishiv_plan::expression::{ExprDataType, IntervalUnit as PublicIntervalUnit};

    match data_type {
        ExprDataType::Null => DataType::Null,
        ExprDataType::Boolean => DataType::Boolean,
        ExprDataType::Int64 => DataType::Int64,
        ExprDataType::UInt64 => DataType::UInt64,
        ExprDataType::Float64 => DataType::Float64,
        ExprDataType::Utf8 => DataType::Utf8,
        ExprDataType::Binary => DataType::Binary,
        ExprDataType::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
        ExprDataType::Date32 => DataType::Date32,
        ExprDataType::Timestamp { unit, timezone } => DataType::Timestamp(
            match unit {
                krishiv_plan::expression::TimeUnit::Second => TimeUnit::Second,
                krishiv_plan::expression::TimeUnit::Millisecond => TimeUnit::Millisecond,
                krishiv_plan::expression::TimeUnit::Microsecond => TimeUnit::Microsecond,
                krishiv_plan::expression::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
            },
            timezone.clone().map(Into::into),
        ),
        ExprDataType::Interval { unit } => DataType::Interval(match unit {
            PublicIntervalUnit::YearMonth => IntervalUnit::YearMonth,
            PublicIntervalUnit::DayTime => IntervalUnit::DayTime,
            PublicIntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }),
        ExprDataType::List(element) => DataType::List(Arc::new(Field::new(
            "item",
            public_data_type_to_arrow(element),
            true,
        ))),
        ExprDataType::Map { key, value } => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", public_data_type_to_arrow(key), false)),
                        Arc::new(Field::new("value", public_data_type_to_arrow(value), true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        ExprDataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        &field.name,
                        public_data_type_to_arrow(&field.data_type),
                        field.nullable,
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        ),
    }
}

fn public_scalar_to_datafusion(
    value: &krishiv_plan::expression::ScalarValue,
) -> Option<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;
    use krishiv_plan::expression::{ScalarValue as PublicScalar, TimeUnit};

    Some(match value {
        PublicScalar::Null => ScalarValue::Null,
        PublicScalar::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        PublicScalar::Int64(value) => ScalarValue::Int64(Some(*value)),
        PublicScalar::UInt64(value) => ScalarValue::UInt64(Some(*value)),
        PublicScalar::Float64(bits) => ScalarValue::Float64(Some(f64::from_bits(*bits))),
        PublicScalar::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
        PublicScalar::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        PublicScalar::Decimal128 {
            value,
            precision,
            scale,
        } => ScalarValue::Decimal128(Some(*value), *precision, *scale),
        PublicScalar::Date32(value) => ScalarValue::Date32(Some(*value)),
        PublicScalar::Timestamp {
            value,
            unit,
            timezone,
        } => {
            let timezone = timezone.clone().map(Into::into);
            match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(*value), timezone),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(*value), timezone),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(*value), timezone),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(*value), timezone),
            }
        }
        PublicScalar::Interval { .. } => return None,
    })
}

/// Lower the versioned engine-owned expression contract into a DataFusion expression.
///
/// Ordinary nodes are lowered structurally. `RawSql`, generic function calls, aggregate
/// calls, and interval literals intentionally use DataFusion's SQL analyzer as the
/// compatibility/preview path until those families receive dedicated typed nodes.
fn lower_public_expression(
    dataframe: &datafusion::dataframe::DataFrame,
    expression: &krishiv_plan::expression::Expr,
) -> SqlResult<datafusion::logical_expr::Expr> {
    expression
        .validate()
        .map_err(|error| SqlError::Unsupported {
            feature: format!("invalid public expression: {error}"),
        })?;
    use datafusion::logical_expr::{Expr as DataFusionExpr, Operator, binary_expr, cast, try_cast};
    use krishiv_plan::expression::{BinaryOperator, Expr};

    Ok(match expression {
        Expr::Column { path } if path.len() == 1 => datafusion::prelude::col(&path[0]),
        Expr::Column { .. } => parse_dataframe_expression(dataframe, &expression.to_sql())?,
        Expr::Literal { value } => match public_scalar_to_datafusion(value) {
            Some(value) => DataFusionExpr::Literal(value, None),
            None => parse_dataframe_expression(dataframe, &expression.to_sql())?,
        },
        Expr::Alias { expression, name } => {
            lower_public_expression(dataframe, expression)?.alias(name)
        }
        Expr::Binary { left, op, right } => binary_expr(
            lower_public_expression(dataframe, left)?,
            match op {
                BinaryOperator::Eq => Operator::Eq,
                BinaryOperator::NotEq => Operator::NotEq,
                BinaryOperator::Gt => Operator::Gt,
                BinaryOperator::GtEq => Operator::GtEq,
                BinaryOperator::Lt => Operator::Lt,
                BinaryOperator::LtEq => Operator::LtEq,
                BinaryOperator::And => Operator::And,
                BinaryOperator::Or => Operator::Or,
                BinaryOperator::Plus => Operator::Plus,
                BinaryOperator::Minus => Operator::Minus,
                BinaryOperator::Multiply => Operator::Multiply,
                BinaryOperator::Divide => Operator::Divide,
            },
            lower_public_expression(dataframe, right)?,
        ),
        Expr::IsNull {
            expression,
            negated,
        } => {
            let expression = lower_public_expression(dataframe, expression)?;
            if *negated {
                expression.is_not_null()
            } else {
                expression.is_null()
            }
        }
        Expr::Cast {
            expression,
            data_type,
            safe,
        } => {
            let expression = lower_public_expression(dataframe, expression)?;
            let data_type = public_data_type_to_arrow(data_type);
            if *safe {
                try_cast(expression, data_type)
            } else {
                cast(expression, data_type)
            }
        }
        Expr::Sort { .. } => {
            return Err(SqlError::Unsupported {
                feature: "standalone sort expressions are only valid inside windows or order_by"
                    .into(),
            });
        }
        Expr::Aggregate { .. }
        | Expr::Function { .. }
        | Expr::Window { .. }
        | Expr::RawSql { .. } => parse_dataframe_expression(dataframe, &expression.to_sql())?,
    })
}

fn sql_dataframe<'a>(
    dataframe: &'a dyn KrishivDataFrameOps,
    operation: &str,
) -> SqlResult<&'a SqlDataFrame> {
    dataframe
        .as_any()
        .downcast_ref::<SqlDataFrame>()
        .ok_or_else(|| SqlError::DataFusion {
            message: format!("right DataFrame must be SqlDataFrame for {operation}"),
        })
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
        let label = self.dataframe.logical_plan().to_string();
        let mut plan = LogicalPlan::new(self.name.clone(), ExecutionKind::Batch).with_node(
            PlanNode::new("datafusion-logical", label, ExecutionKind::Batch),
        );
        if let Some(n) = self.shuffle_partitions {
            plan = plan.with_shuffle_partitions(Some(n));
        }
        plan
    }
    fn query(&self) -> Option<&str> {
        SqlDataFrame::query(self)
    }
    async fn execute_stream(&self) -> SqlResult<SqlStream> {
        SqlDataFrame::execute_stream(self).await
    }

    // ── DataFrame transforms ────────────────────────────────────────────────

    fn schema(&self) -> SchemaRef {
        SchemaRef::from(self.dataframe.schema().clone())
    }

    async fn select(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().select_columns(columns)?;
        Ok(Box::new(self.with_new_dataframe(df, "select")))
    }

    async fn select_exprs(
        &self,
        expressions: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let expressions = expressions
            .iter()
            .map(|expression| lower_public_expression(&self.dataframe, expression))
            .collect::<Result<Vec<_>, _>>()?;
        let df = self.dataframe.clone().select(expressions)?;
        Ok(Box::new(self.with_new_dataframe(df, "select_exprs")))
    }

    async fn aggregate(
        &self,
        group_exprs: &[&krishiv_plan::expression::Expr],
        aggregate_exprs: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        if aggregate_exprs.is_empty() {
            return Err(SqlError::Unsupported {
                feature: "aggregate requires at least one aggregate expression".into(),
            });
        }
        let group_exprs = group_exprs
            .iter()
            .map(|expression| lower_public_expression(&self.dataframe, expression))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregate_exprs = aggregate_exprs
            .iter()
            .map(|expression| lower_public_expression(&self.dataframe, expression))
            .collect::<Result<Vec<_>, _>>()?;
        let df = self
            .dataframe
            .clone()
            .aggregate(group_exprs, aggregate_exprs)?;
        Ok(Box::new(self.with_new_dataframe(df, "aggregate")))
    }

    async fn aggregate_grouping(
        &self,
        grouping: GroupingMode<'_>,
        aggregate_exprs: &[&krishiv_plan::expression::Expr],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        if aggregate_exprs.is_empty() {
            return Err(SqlError::Unsupported {
                feature: "grouping aggregation requires at least one aggregate expression".into(),
            });
        }
        let lower = |expression: &&krishiv_plan::expression::Expr| {
            lower_public_expression(&self.dataframe, expression)
        };
        let group = match grouping {
            GroupingMode::Sets(sets) => datafusion::logical_expr::grouping_set(
                sets.into_iter()
                    .map(|set| set.iter().map(lower).collect::<Result<Vec<_>, _>>())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GroupingMode::Cube(expressions) => datafusion::logical_expr::cube(
                expressions
                    .iter()
                    .map(lower)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GroupingMode::Rollup(expressions) => datafusion::logical_expr::rollup(
                expressions
                    .iter()
                    .map(lower)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };
        let aggregates = aggregate_exprs
            .iter()
            .map(lower)
            .collect::<Result<Vec<_>, _>>()?;
        let df = self.dataframe.clone().aggregate(vec![group], aggregates)?;
        Ok(Box::new(self.with_new_dataframe(df, "aggregate_grouping")))
    }

    async fn pivot(
        &self,
        group_exprs: &[&krishiv_plan::expression::Expr],
        pivot_column: &krishiv_plan::expression::Expr,
        aggregate_expr: &krishiv_plan::expression::Expr,
        values: &[(krishiv_plan::expression::ScalarValue, String)],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        use krishiv_plan::expression::Expr as PublicExpr;
        let (function, input, distinct) = match aggregate_expr {
            PublicExpr::Aggregate {
                function,
                expression: Some(input),
                distinct,
            } => (*function, input.as_ref(), *distinct),
            _ => {
                return Err(SqlError::Unsupported {
                    feature: "pivot requires an aggregate expression with one input".into(),
                });
            }
        };
        if values.is_empty() {
            return Err(SqlError::Unsupported {
                feature: "pivot requires at least one value".into(),
            });
        }
        let group_exprs = group_exprs
            .iter()
            .map(|expression| lower_public_expression(&self.dataframe, expression))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregates = values
            .iter()
            .map(|(value, alias)| {
                let conditional = PublicExpr::raw(format!(
                    "CASE WHEN {} = {} THEN {} END",
                    pivot_column.to_sql(),
                    value.to_sql_literal(),
                    input.to_sql()
                ));
                let aggregate = PublicExpr::Aggregate {
                    function,
                    expression: Some(Box::new(conditional)),
                    distinct,
                }
                .alias(alias);
                lower_public_expression(&self.dataframe, &aggregate)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let dataframe = self.dataframe.clone().aggregate(group_exprs, aggregates)?;
        Ok(Box::new(self.with_new_dataframe(dataframe, "pivot")))
    }

    async fn unpivot(
        &self,
        columns: &[&str],
        name_column: &str,
        value_column: &str,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        if columns.is_empty() {
            return Err(SqlError::Unsupported {
                feature: "unpivot requires at least one column".into(),
            });
        }
        let retained = self
            .dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| !columns.contains(name))
            .collect::<Vec<_>>();
        let mut branches = Vec::with_capacity(columns.len());
        for column in columns {
            let mut expressions = retained
                .iter()
                .map(|name| datafusion::logical_expr::col(*name))
                .collect::<Vec<_>>();
            expressions
                .push(datafusion::logical_expr::lit((*column).to_owned()).alias(name_column));
            expressions.push(datafusion::logical_expr::col(*column).alias(value_column));
            branches.push(self.dataframe.clone().select(expressions)?);
        }
        let mut branches = branches.into_iter();
        let Some(mut dataframe) = branches.next() else {
            return Err(SqlError::Unsupported {
                feature: "unpivot requires at least one branch".into(),
            });
        };
        for branch in branches {
            dataframe = dataframe.union(branch)?;
        }
        Ok(Box::new(self.with_new_dataframe(dataframe, "unpivot")))
    }

    async fn filter(&self, predicate: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let expr = self.dataframe.parse_sql_expr(predicate)?;
        let df = self.dataframe.clone().filter(expr)?;
        Ok(Box::new(self.with_new_dataframe(df, "filter")))
    }

    async fn filter_expr(
        &self,
        predicate: &krishiv_plan::expression::Expr,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let expr = lower_public_expression(&self.dataframe, predicate)?;
        let df = self.dataframe.clone().filter(expr)?;
        Ok(Box::new(self.with_new_dataframe(df, "filter_expr")))
    }

    async fn limit(&self, n: usize) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().limit(0, Some(n))?;
        Ok(Box::new(self.with_new_dataframe(df, "limit")))
    }

    async fn distinct(&self) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().distinct()?;
        Ok(Box::new(self.with_new_dataframe(df, "distinct")))
    }

    async fn drop_nulls(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let columns = if columns.is_empty() {
            self.dataframe
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        } else {
            columns.to_vec()
        };
        let mut predicate: Option<datafusion::logical_expr::Expr> = None;
        for column in columns {
            let next = datafusion::logical_expr::col(column).is_not_null();
            predicate = Some(match predicate {
                Some(current) => current.and(next),
                None => next,
            });
        }
        let df = match predicate {
            Some(predicate) => self.dataframe.clone().filter(predicate)?,
            None => self.dataframe.clone(),
        };
        Ok(Box::new(self.with_new_dataframe(df, "drop_nulls")))
    }

    async fn sample(&self, fraction: f64) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        if !(0.0..=1.0).contains(&fraction) {
            return Err(SqlError::Unsupported {
                feature: "sample fraction must be between 0 and 1".into(),
            });
        }
        let predicate = self
            .dataframe
            .parse_sql_expr(&format!("random() < {fraction}"))?;
        let df = self.dataframe.clone().filter(predicate)?;
        Ok(Box::new(self.with_new_dataframe(df, "sample")))
    }

    async fn sort(
        &self,
        columns: &[&str],
        descending: &[bool],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        use datafusion::logical_expr::SortExpr;
        let exprs: Vec<SortExpr> = columns
            .iter()
            .zip(descending.iter())
            .map(|(col_name, desc)| datafusion::logical_expr::col(*col_name).sort(!desc, *desc))
            .collect();
        let df = self.dataframe.clone().sort(exprs)?;
        Ok(Box::new(self.with_new_dataframe(df, "sort")))
    }

    async fn alias(&self, alias: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().alias(alias)?;
        Ok(Box::new(self.with_new_dataframe(df, "alias")))
    }

    async fn drop_columns(&self, columns: &[&str]) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().drop_columns(columns)?;
        Ok(Box::new(self.with_new_dataframe(df, "drop")))
    }

    async fn rename_column(&self, old: &str, new: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().with_column_renamed(old, new)?;
        Ok(Box::new(self.with_new_dataframe(df, "rename")))
    }

    async fn with_column(&self, name: &str, expr: &str) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let parsed = self.dataframe.parse_sql_expr(expr)?;
        let df = self.dataframe.clone().with_column(name, parsed)?;
        Ok(Box::new(self.with_new_dataframe(df, "with_column")))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn describe(&self) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let df = self.dataframe.clone().describe().await?;
        Ok(Box::new(self.with_new_dataframe(df, "describe")))
    }

    async fn fill_null(
        &self,
        column: &str,
        value: &str,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let expr = format!("COALESCE({column}, {value})");
        let parsed = self.dataframe.parse_sql_expr(&expr)?;
        let df = self.dataframe.clone().with_column(column, parsed)?;
        Ok(Box::new(self.with_new_dataframe(df, "fill_null")))
    }

    async fn join(
        &self,
        right: &dyn KrishivDataFrameOps,
        how: &str,
        left_on: &[&str],
        right_on: &[&str],
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let right_sql = right
            .as_any()
            .downcast_ref::<SqlDataFrame>()
            .ok_or_else(|| SqlError::DataFusion {
                message: "right DataFrame must be SqlDataFrame for join".into(),
            })?;
        use datafusion::common::JoinType;
        let join_type = match how.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" | "outer" => JoinType::Full,
            "leftsemi" | "left_semi" => JoinType::LeftSemi,
            "rightsemi" | "right_semi" => JoinType::RightSemi,
            "leftanti" | "left_anti" => JoinType::LeftAnti,
            "rightanti" | "right_anti" => JoinType::RightAnti,
            _ => {
                return Err(SqlError::DataFusion {
                    message: format!("unsupported join type: {how}"),
                });
            }
        };
        let df = self.dataframe.clone().join(
            right_sql.dataframe.clone(),
            join_type,
            left_on,
            right_on,
            None,
        )?;
        Ok(Box::new(self.with_new_dataframe(df, "join")))
    }

    async fn union(
        &self,
        right: &dyn KrishivDataFrameOps,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let right_sql = right
            .as_any()
            .downcast_ref::<SqlDataFrame>()
            .ok_or_else(|| SqlError::DataFusion {
                message: "right DataFrame must be SqlDataFrame for union".into(),
            })?;
        let df = self.dataframe.clone().union(right_sql.dataframe.clone())?;
        Ok(Box::new(self.with_new_dataframe(df, "union")))
    }

    async fn union_distinct(
        &self,
        right: &dyn KrishivDataFrameOps,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let right = sql_dataframe(right, "union_distinct")?;
        let df = self
            .dataframe
            .clone()
            .union_distinct(right.dataframe.clone())?;
        Ok(Box::new(self.with_new_dataframe(df, "union_distinct")))
    }

    async fn intersect(
        &self,
        right: &dyn KrishivDataFrameOps,
        distinct: bool,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let right = sql_dataframe(right, "intersect")?;
        let df = if distinct {
            self.dataframe
                .clone()
                .intersect_distinct(right.dataframe.clone())?
        } else {
            self.dataframe.clone().intersect(right.dataframe.clone())?
        };
        Ok(Box::new(self.with_new_dataframe(df, "intersect")))
    }

    async fn except(
        &self,
        right: &dyn KrishivDataFrameOps,
        distinct: bool,
    ) -> SqlResult<Box<dyn KrishivDataFrameOps>> {
        let right = sql_dataframe(right, "except")?;
        let df = if distinct {
            self.dataframe
                .clone()
                .except_distinct(right.dataframe.clone())?
        } else {
            self.dataframe.clone().except(right.dataframe.clone())?
        };
        Ok(Box::new(self.with_new_dataframe(df, "except")))
    }

    async fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> SqlResult<()> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![batches]).map_err(|e| {
                SqlError::DataFusion {
                    message: e.to_string(),
                }
            })?;
        self.context
            .register_table(name, Arc::new(mem_table))
            .map_err(SqlError::from)?;
        Ok(())
    }

    async fn deregister_table(&self, name: &str) -> SqlResult<()> {
        let _ = self
            .context
            .deregister_table(name)
            .map_err(SqlError::from)?;
        Ok(())
    }

    async fn create_view(&self, name: &str, replace: bool) -> SqlResult<()> {
        let query = self
            .query_text
            .as_deref()
            .ok_or_else(|| SqlError::DataFusion {
                message: "create_view requires an SQL query string on the DataFrame".into(),
            })?;
        let or_replace = if replace { "OR REPLACE " } else { "" };
        let view_sql = format!("CREATE {or_replace}VIEW \"{name}\" AS {query}");
        self.context.sql(&view_sql).await?;
        Ok(())
    }
}

// ── CALL-system helpers ───────────────────────────────────────────────────────

/// Extract positional arguments from the body of a `CALL` statement.
///
/// Handles single-quoted string literals and bare integers.
/// `'catalog.ns.table', '7 days', 5` → `["catalog.ns.table", "7 days", "5"]`
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn call_args_from_str(s: &str) -> Vec<String> {
    let mut args: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut after_str = false;
    for ch in s.chars() {
        if after_str {
            if ch == ',' {
                after_str = false;
            }
            continue;
        }
        if in_str {
            if ch == '\'' {
                in_str = false;
                after_str = true;
                args.push(std::mem::take(&mut cur));
            } else {
                cur.push(ch);
            }
        } else if ch == '\'' {
            in_str = true;
        } else if ch == ',' {
            let t = cur.trim().to_string();
            if !t.is_empty() {
                args.push(t);
            }
            cur.clear();
        } else {
            cur.push(ch);
        }
    }
    let t = cur.trim().to_string();
    if !t.is_empty() {
        args.push(t);
    }
    args
}

/// Parse an Iceberg `TableIdent` from a dotted string.
///
/// Accepts:
/// - `"namespace.table"` — single-level namespace
/// - `"catalog.namespace.table"` — catalog prefix is ignored (catalog is
///   selected by registration order, not by name, in the CALL dispatch)
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn iceberg_table_ident(table_ref: &str) -> SqlResult<iceberg::TableIdent> {
    let parts: Vec<&str> = table_ref.splitn(3, '.').collect();
    match parts.len() {
        2 => {
            let ns =
                iceberg::NamespaceIdent::from_vec(vec![parts[0].to_string()]).map_err(|e| {
                    SqlError::DataFusion {
                        message: e.to_string(),
                    }
                })?;
            Ok(iceberg::TableIdent::new(ns, parts[1].to_string()))
        }
        3 => {
            let ns =
                iceberg::NamespaceIdent::from_vec(vec![parts[1].to_string()]).map_err(|e| {
                    SqlError::DataFusion {
                        message: e.to_string(),
                    }
                })?;
            Ok(iceberg::TableIdent::new(ns, parts[2].to_string()))
        }
        _ => Err(SqlError::DataFusion {
            message: format!(
                "invalid table reference '{table_ref}': expected 'ns.table' or 'cat.ns.table'"
            ),
        }),
    }
}

/// Parse a human-readable duration string into a [`chrono::Duration`].
///
/// Accepted formats: `"N days"`, `"N day"`, `"N hours"`, `"N hour"`,
/// `"N weeks"`, `"N week"`, `"N minutes"`, `"N minute"`.
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn parse_call_duration(s: &str) -> SqlResult<chrono::Duration> {
    let s = s.trim();
    let mut it = s.splitn(2, ' ');
    let n: i64 = it
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| SqlError::DataFusion {
            message: format!("invalid duration value in '{s}'"),
        })?;
    let unit = it.next().unwrap_or("").trim().to_ascii_lowercase();
    match unit.trim_end_matches('s') {
        "day" => Ok(chrono::Duration::days(n)),
        "hour" => Ok(chrono::Duration::hours(n)),
        "week" => Ok(chrono::Duration::weeks(n)),
        "minute" | "min" => Ok(chrono::Duration::minutes(n)),
        _ => Err(SqlError::DataFusion {
            message: format!("unknown duration unit '{unit}' in '{s}'"),
        }),
    }
}

// ── Iceberg DML helpers ───────────────────────────────────────────────────────

/// Parse `DELETE FROM <table> [WHERE <predicate>]` into `(table_ref, predicate)`.
///
/// Returns `None` when the statement does not match the expected shape.
/// A missing WHERE clause is treated as `"TRUE"` (delete all rows).
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn parse_dml_delete(stmt: &str) -> Option<(String, String)> {
    use regex::Regex;
    use std::sync::LazyLock;

    static WITH_WHERE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)^\s*DELETE\s+FROM\s+(\S+)\s+WHERE\s+([\s\S]+?)\s*;?\s*$").unwrap()
    });
    static NO_WHERE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?is)^\s*DELETE\s+FROM\s+(\S+)\s*;?\s*$").unwrap());

    if let Some(cap) = WITH_WHERE.captures(stmt) {
        let table = cap.get(1)?.as_str().to_string();
        let pred = cap.get(2)?.as_str().trim().to_string();
        Some((table, pred))
    } else if let Some(cap) = NO_WHERE.captures(stmt) {
        let table = cap.get(1)?.as_str().to_string();
        Some((table, "TRUE".to_string()))
    } else {
        None
    }
}

/// Parse `UPDATE <table> SET <assignments> [WHERE <predicate>]` into
/// `(table_ref, set_clause, Option<predicate>)`.
///
/// Returns `None` when the statement does not match.
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn parse_dml_update(stmt: &str) -> Option<(String, String, Option<String>)> {
    use regex::Regex;
    use std::sync::LazyLock;

    static UPDATE_RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)^\s*UPDATE\s+(\S+)\s+SET\s+([\s\S]+?)(?:\s+WHERE\s+([\s\S]+?))?\s*;?\s*$")
            .unwrap()
    });

    let cap = UPDATE_RE.captures(stmt)?;
    let table = cap.get(1)?.as_str().to_string();
    let set_clause = cap.get(2)?.as_str().trim().to_string();
    let predicate = cap.get(3).map(|m| m.as_str().trim().to_string());
    Some((table, set_clause, predicate))
}

/// Split a SQL SET clause (`col1 = expr1, col2 = expr2`) into assignment pairs.
///
/// Handles commas inside balanced parentheses (e.g. `CONCAT(a, b)`).
#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn split_set_assignments(set_clause: &str) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut depth: i32 = 0;
    let mut start = 0usize;
    let bytes = set_clause.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                push_set_pair(&mut pairs, &set_clause[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    push_set_pair(&mut pairs, &set_clause[start..]);
    pairs
}

#[cfg(all(feature = "iceberg-datafusion", feature = "local-catalog"))]
fn push_set_pair(pairs: &mut Vec<(String, String)>, s: &str) {
    let s = s.trim();
    if let Some(eq) = s.find('=') {
        let col = s[..eq]
            .trim()
            .trim_matches('`')
            .trim_matches('"')
            .to_string();
        let expr = s[eq + 1..].trim().to_string();
        if !col.is_empty() && !expr.is_empty() {
            pairs.push((col, expr));
        }
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
        let optimized = Optimizer::default().optimize(logical_plan)?;
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

    let optimized = Optimizer::default().optimize(logical_plan)?;
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
    let result = optimizer.optimize(plan.logical_plan().clone())?;
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

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn typed_expression_ast_matches_raw_sql_execution() {
        use krishiv_plan::expression::{BinaryOperator, Expr, ScalarValue};

        let engine = SqlEngine::new();
        let dataframe = engine
            .sql("SELECT 11 AS amount, TRUE AS active")
            .await
            .unwrap();
        let predicate = Expr::column("amount")
            .binary(BinaryOperator::Gt, Expr::literal(ScalarValue::Int64(10)))
            .binary(BinaryOperator::And, Expr::column("active"));
        let parsed = super::parse_public_expression("amount > 10 AND active").unwrap();
        assert_eq!(
            predicate.normalize_json().unwrap(),
            parsed.normalize_json().unwrap()
        );

        let typed = super::KrishivDataFrameOps::filter_expr(&dataframe, &predicate)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let raw = super::KrishivDataFrameOps::filter(&dataframe, &predicate.to_sql())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(typed, raw);
        assert_eq!(
            typed
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }

    #[test]
    fn dataframe_alias_parser_ignores_nested_as_tokens() {
        assert_eq!(super::top_level_alias_index("CAST(value AS BIGINT)"), None);
        assert_eq!(
            super::top_level_alias_index("CAST(value AS BIGINT) AS \"value64\""),
            Some(21)
        );
        assert_eq!(super::top_level_alias_index("' AS '"), None);
    }

    use krishiv_plan::optimizer::{Cost, CostModel, Optimizer, OptimizerError, OptimizerRule};
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use std::sync::Arc;

    use super::{
        SqlEngine, SqlError, explain_sql, explain_sql_optimized, explain_sql_with_cost, plan_sql,
        query_memory_limit_from_env, referenced_table_names, resolve_query_memory_limit_bytes,
    };

    #[tokio::test]
    async fn catalog_table_resolved_in_sql() {
        use crate::catalog::{
            CatalogField, FieldType, InMemoryCatalog, TableMetadata, TableSchema,
        };
        use std::sync::{Arc, RwLock};

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

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
    fn explain_sql_optimized_propagates_invalid_rule_output() {
        struct InvalidRule;
        impl OptimizerRule for InvalidRule {
            fn name(&self) -> &str {
                "invalid"
            }

            fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
                Some(
                    plan.clone().with_node(
                        PlanNode::new("dangling", "dangling", ExecutionKind::Batch)
                            .with_inputs(["missing"]),
                    ),
                )
            }
        }

        let mut optimizer = Optimizer::new();
        optimizer.add_rule(Box::new(InvalidRule));

        let error = explain_sql_optimized("select 1", &optimizer).expect_err("optimizer must fail");

        assert!(matches!(
            error,
            SqlError::Optimizer(OptimizerError::InvalidRuleOutput { .. })
        ));
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

    #[test]
    fn resolve_query_memory_limit_bytes_falls_back_for_missing_invalid_and_zero() {
        assert_eq!(resolve_query_memory_limit_bytes(None), None);
        assert_eq!(resolve_query_memory_limit_bytes(Some("not-a-number")), None);
        assert_eq!(resolve_query_memory_limit_bytes(Some("0")), None);
        assert_eq!(
            resolve_query_memory_limit_bytes(Some("268435456")),
            Some(268_435_456)
        );
        assert_eq!(resolve_query_memory_limit_bytes(Some(" 1024 ")), Some(1024));
    }

    #[tokio::test]
    async fn memory_limited_engine_executes_queries_and_reports_limit() {
        let engine = SqlEngine::new_with_memory_limit(Some(64 * 1024 * 1024));
        assert_eq!(engine.memory_limit_bytes(), Some(64 * 1024 * 1024));

        let dataframe = engine
            .sql("select v from (values (3), (1), (2)) as t(v) order by v")
            .await
            .expect("memory-limited engine must plan queries");
        let batches = dataframe
            .collect()
            .await
            .expect("memory-limited engine must execute queries");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
    }

    #[test]
    fn memory_limited_engine_installs_bounded_pool_in_session_context() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let bounded = SqlEngine::new_with_memory_limit(Some(1_000_000));
        let pool = Arc::clone(&bounded.context.runtime_env().memory_pool);
        let mut reservation = MemoryConsumer::new("phase2-test").register(&pool);
        assert!(
            reservation.try_grow(2_000_000).is_err(),
            "reservation above the configured limit must be rejected"
        );
        reservation.free();

        let unbounded = SqlEngine::new_with_memory_limit(None);
        assert_eq!(unbounded.memory_limit_bytes(), None);
        let pool = Arc::clone(&unbounded.context.runtime_env().memory_pool);
        let mut reservation = MemoryConsumer::new("phase2-test-unbounded").register(&pool);
        assert!(
            reservation.try_grow(2_000_000).is_ok(),
            "default engine keeps DataFusion's unbounded pool"
        );
        reservation.free();
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

    use krishiv_plan::udf::MultiplyScalarUdf;

    use super::SqlEngine;

    #[tokio::test]
    async fn registered_scalar_udf_visible_in_sql() {
        let registry = Arc::new(std::sync::RwLock::new(krishiv_plan::udf::UdfRegistry::new()));
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
    use std::sync::Arc;

    use super::{SqlEngine, SqlError};
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn create_function_returns_table_rejects_unsupported_languages() {
        let registry = Arc::new(std::sync::RwLock::new(krishiv_plan::udf::UdfRegistry::new()));
        let engine = SqlEngine::new().with_udf_registry(Arc::clone(&registry));

        let rust_result = engine
            .sql(
                "CREATE FUNCTION my_udtf(arg1 INT) \
                 RETURNS TABLE (col1 TEXT, col2 BIGINT) \
                 LANGUAGE RUST \
                 AS 'fn body() {}'",
            )
            .await
            .expect_err("unsupported language must fail before registration");
        assert!(
            matches!(rust_result, SqlError::Unsupported { .. }),
            "unexpected error: {rust_result}"
        );
        assert!(
            engine.sql("SELECT * FROM my_udtf(1)").await.is_err(),
            "failed DDL must not leave a schema-only function registered"
        );
        assert!(registry.read().unwrap().table_names().is_empty());
    }

    #[tokio::test]
    async fn create_function_returns_table_registers_sql_body() {
        let engine = SqlEngine::new();

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

    #[tokio::test]
    async fn create_function_returns_table_rejects_incomplete_sql_definition() {
        let engine = SqlEngine::new();

        let missing_language = engine
            .sql(
                "CREATE FUNCTION no_language() \
                 RETURNS TABLE (value BIGINT) \
                 AS 'SELECT 1 AS value'",
            )
            .await
            .expect_err("language must be explicit");
        assert!(matches!(missing_language, SqlError::Unsupported { .. }));

        let missing_body = engine
            .sql(
                "CREATE FUNCTION no_body() \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL",
            )
            .await
            .expect_err("SQL UDTF body must be required");
        assert!(matches!(
            missing_body,
            SqlError::InvalidTableFunction { .. }
        ));

        let empty_output = engine
            .sql(
                "CREATE FUNCTION no_columns() \
                 RETURNS TABLE () \
                 LANGUAGE SQL AS 'SELECT 1'",
            )
            .await
            .expect_err("UDTF output schema must not be empty");
        assert!(matches!(
            empty_output,
            SqlError::InvalidTableFunction { .. }
        ));
    }

    #[test]
    fn closure_table_udf_registration_validates_definition() {
        let engine = SqlEngine::new();
        let error = engine
            .register_table_udf_fn("", Schema::empty(), |_| {
                unreachable!("invalid definition must fail before body registration")
            })
            .expect_err("invalid closure UDTF must fail");
        assert!(matches!(error, SqlError::InvalidTableFunction { .. }));

        let duplicate_columns = engine
            .register_table_udf_fn(
                "duplicates",
                Schema::new(vec![
                    Field::new("value", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                ]),
                |_| unreachable!("invalid definition must fail before body registration"),
            )
            .expect_err("duplicate output names must fail");
        assert!(matches!(
            duplicate_columns,
            SqlError::InvalidTableFunction { .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_rejects_declared_schema_mismatch() {
        let engine = SqlEngine::new();
        engine
            .sql(
                "CREATE FUNCTION wrong_schema() \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT ''text'' AS value'",
            )
            .await
            .expect("definition itself is syntactically valid");

        let error = engine
            .sql("SELECT * FROM wrong_schema()")
            .await
            .expect_err("runtime schema mismatch must fail");
        assert!(error.to_string().contains("returned schema"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_binds_literal_arguments() {
        let engine = SqlEngine::new();
        engine
            .sql(
                "CREATE FUNCTION echo_args(n BIGINT, text TEXT, enabled BOOLEAN) \
                 RETURNS TABLE (next_n BIGINT, echoed TEXT, flag BOOLEAN) \
                 LANGUAGE SQL \
                 AS 'SELECT $1 + 1 AS next_n, $2 AS echoed, $3 AS flag'",
            )
            .await
            .expect("function registration should succeed");

        let batches = engine
            .sql("SELECT * FROM echo_args(41, 'O''Reilly', TRUE)")
            .await
            .expect("table function planning should succeed")
            .collect()
            .await
            .expect("table function execution should succeed");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let next_n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("next_n should be Int64");
        let echoed = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("echoed should be Utf8");
        let flag = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("flag should be Boolean");
        assert_eq!(next_n.value(0), 42);
        assert_eq!(echoed.value(0), "O'Reilly");
        assert!(flag.value(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_body_udtf_rejects_wrong_arity_and_non_literal_arguments() {
        let engine = SqlEngine::new();
        let invalid_placeholder = engine
            .sql(
                "CREATE FUNCTION invalid_placeholder(n BIGINT) \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT $2 AS value'",
            )
            .await
            .expect_err("out-of-range placeholders must fail during registration");
        assert!(
            invalid_placeholder
                .to_string()
                .contains("no matching argument")
        );

        engine
            .sql(
                "CREATE FUNCTION one_arg(n BIGINT) \
                 RETURNS TABLE (value BIGINT) \
                 LANGUAGE SQL AS 'SELECT $1 AS value'",
            )
            .await
            .expect("function registration should succeed");

        let wrong_arity = engine
            .sql("SELECT * FROM one_arg()")
            .await
            .expect_err("wrong arity must fail");
        assert!(wrong_arity.to_string().contains("expects 1 arguments"));

        // Modern DataFusion constant-folds arithmetic on literals before invoking the
        // table function, so `20 + 22` is simplified to `Literal(42)` before our
        // `expr_to_scalar` sees it.  The call therefore succeeds rather than failing.
        engine
            .sql("SELECT * FROM one_arg(20 + 22)")
            .await
            .expect("constant arithmetic is accepted after DataFusion constant-folding");
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

    #[tokio::test]
    async fn krishiv_logical_plan_kind_is_streaming_for_streaming_source() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let engine = SqlEngine::new();
        engine.register_streaming_source_name("events").unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
            ],
        )
        .unwrap();
        engine
            .register_record_batches("events", vec![batch])
            .await
            .unwrap();
        let df = engine
            .sql("SELECT ts, user_id FROM events WHERE ts > 0")
            .await
            .expect("streaming sql");
        assert_eq!(
            df.krishiv_logical_plan().kind(),
            super::ExecutionKind::Streaming
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

#[cfg(all(test, feature = "iceberg-datafusion", feature = "local-catalog"))]
mod iceberg_catalog_tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::unified::KrishivCatalog;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn with_iceberg_catalog_registers_under_given_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");
        let catalog_names = engine.context.catalog_names();
        assert!(
            catalog_names.contains(&"mycat".to_string()),
            "iceberg catalog 'mycat' must be registered; got: {catalog_names:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_no_catalog_returns_error() {
        let engine = SqlEngine::new();
        let err = engine
            .sql("CALL system.expire_snapshots('myns.orders', '7 days', 1)")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no Iceberg catalog"),
            "expected 'no Iceberg catalog' in error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_unknown_procedure_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let engine = SqlEngine::new().with_iceberg_catalog(catalog, "mycat");
        let err = engine
            .sql("CALL system.frobnicate_table('myns.orders')")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown procedure"),
            "expected 'unknown procedure' in error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_system_expire_snapshots_returns_count() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "orders", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        let df = engine
            .sql("CALL system.expire_snapshots('myns.orders', '7 days', 1)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let schema = batches[0].schema();
        assert_eq!(
            schema.field(0).name(),
            "expired_snapshots",
            "result column must be 'expired_snapshots'"
        );
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0, "fresh table has no snapshots to expire");
    }

    // ── DELETE / UPDATE helpers ───────────────────────────────────────────────

    #[test]
    fn parse_dml_delete_with_where() {
        let (tbl, pred) =
            super::parse_dml_delete("DELETE FROM myns.orders WHERE id = 5").expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert_eq!(pred, "id = 5");
    }

    #[test]
    fn parse_dml_delete_without_where() {
        let (tbl, pred) = super::parse_dml_delete("DELETE FROM myns.orders").expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert_eq!(pred, "TRUE");
    }

    #[test]
    fn parse_dml_update_with_where() {
        let (tbl, set, pred) =
            super::parse_dml_update("UPDATE myns.orders SET price = price * 2 WHERE id = 1")
                .expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert!(set.contains("price * 2"), "set clause: {set}");
        assert_eq!(pred.unwrap(), "id = 1");
    }

    #[test]
    fn parse_dml_update_without_where() {
        let (tbl, set, pred) = super::parse_dml_update("UPDATE myns.orders SET status = 'active'")
            .expect("must parse");
        assert_eq!(tbl, "myns.orders");
        assert!(set.contains("'active'"), "set clause: {set}");
        assert!(pred.is_none());
    }

    #[test]
    fn split_set_assignments_basic() {
        let pairs = super::split_set_assignments("a = 1, b = 'hello', c = c + 1");
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("a".to_string(), "1".to_string()));
        assert_eq!(pairs[1], ("b".to_string(), "'hello'".to_string()));
        assert_eq!(pairs[2], ("c".to_string(), "c + 1".to_string()));
    }

    #[test]
    fn split_set_assignments_function_with_comma() {
        let pairs = super::split_set_assignments("name = CONCAT(first, ' ', last), age = 5");
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, "name");
        assert!(pairs[0].1.starts_with("CONCAT("), "got: {}", pairs[0].1);
        assert_eq!(pairs[1], ("age".to_string(), "5".to_string()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_from_iceberg_table_removes_rows() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "orders", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        // DELETE with no WHERE on an empty table returns 0 deleted rows.
        let df = engine
            .sql("DELETE FROM myns.orders WHERE id = 99")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "deleted_rows");
        let deleted = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(deleted, 0, "empty table: no rows to delete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_iceberg_table_returns_updated_count() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        let dir = tempfile::TempDir::new().unwrap();
        let catalog = Arc::new(KrishivCatalog::local(dir.path()).await.unwrap());
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "status", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();
        catalog
            .create_table("myns", "customers", schema, "")
            .await
            .unwrap();
        let engine = SqlEngine::new().with_iceberg_catalog(Arc::clone(&catalog), "mycat");
        let df = engine
            .sql("UPDATE myns.customers SET status = 'active' WHERE id = 1")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "updated_rows");
        let updated = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(updated, 0, "empty table: no rows to update");
    }
}

pub mod kafka_table;
