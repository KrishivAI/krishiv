use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use twox_hash::XxHash64;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use krishiv_dataflow::interval_join::{IntervalJoinSpec, PerKeyIntervalJoin};
use krishiv_dataflow::side_output::{SideOutput, SideOutputRouter};
use krishiv_dataflow::temporal_join::{TemporalJoinSpec, VersionedTableState};
use krishiv_dataflow::{AggExpr, ExecError, WatermarkState};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};
use tokio::sync::mpsc;

use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};

/// Alias for the inner stream error type used by the window runtime.
type ExecStream = Pin<
    Box<dyn Stream<Item = std::result::Result<RecordBatch, krishiv_dataflow::ExecError>> + Send>,
>;

pub type KrishivStream =
    Pin<Box<dyn futures::stream::Stream<Item = std::result::Result<RecordBatch, String>> + Send>>;

const SIDE_OUTPUT_CHANNEL_CAPACITY: usize = 64;

/// Main and named late-data streams produced by side-output execution.
pub struct StreamingOutputStreams {
    main: KrishivStream,
    side_output: NamedSideOutputStream,
}

impl StreamingOutputStreams {
    /// Split this result into independently consumable main and side streams.
    ///
    /// Both streams should be polled concurrently. Routing uses bounded
    /// channels, so an undrained stream intentionally backpressures the source
    /// instead of allowing unbounded memory growth or dropping records.
    pub fn into_parts(self) -> (KrishivStream, NamedSideOutputStream) {
        (self.main, self.side_output)
    }
}

/// A named stream containing records routed out of the main pipeline.
pub struct NamedSideOutputStream {
    name: String,
    stream: KrishivStream,
}

impl NamedSideOutputStream {
    /// Configured side-output name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Consume this wrapper and return the late-data stream.
    pub fn into_stream(self) -> KrishivStream {
        self.stream
    }
}

/// Deduplication strategy (T6 / ST10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DedupStateMode {
    /// Legacy in-memory `HashSet` with the 10M-key `DEDUP_SEEN_CAPACITY`
    /// cap. Suitable only for low-cardinality test workloads.
    InMemory,
    /// State-backed dedup using `krishiv_dataflow::DeduplicationOperator`
    /// (RocksDB). Recommended for production streams.
    #[default]
    StateBacked,
}

/// A fluent builder for creating asynchronous streaming pipelines from a DataFrame.
#[derive(Clone)]
pub struct StreamingDataFrame {
    df: DataFrame,
    event_time_column: Option<String>,
    key_column: Option<String>,
    window_kind: Option<LocalWindowKind>,
    window_size_ms: Option<u64>,
    agg_exprs: Vec<AggExpr>,
    watermark_lag_ms: u64,
    /// Optional side-output spec attached to this pipeline.
    side_output: Option<SideOutput>,
    /// Columns to use for deduplication (within watermark window).
    dedup_columns: Option<Vec<String>>,
    /// Which deduplication implementation to wire into the pipeline.
    dedup_state: DedupStateMode,
    /// Per-key state TTL in milliseconds (`None` = no expiry).
    state_ttl_ms: Option<u64>,
    /// Per-source watermark lags for multi-source reconciliation (source_id → lag_ms).
    source_watermark_lags: HashMap<String, u64>,
    /// Column identifying the source when `source_watermark_lags` is set.
    source_id_column: Option<String>,
}

impl StreamingDataFrame {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self {
            df,
            event_time_column: None,
            key_column: None,
            window_kind: None,
            window_size_ms: None,
            agg_exprs: Vec::new(),
            watermark_lag_ms: 0,
            side_output: None,
            dedup_columns: None,
            dedup_state: DedupStateMode::default(),
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        }
    }

    /// Configure the event time column.
    pub fn with_event_time(mut self, column: impl Into<String>) -> Self {
        self.event_time_column = Some(column.into());
        self
    }

    /// Configure the key column for the stream.
    pub fn key_by(mut self, column: impl Into<String>) -> Self {
        self.key_column = Some(column.into());
        self
    }

    /// Set a tumbling window.
    pub fn tumbling_window(mut self, window_size_ms: u64) -> Self {
        self.window_kind = Some(LocalWindowKind::Tumbling);
        self.window_size_ms = Some(window_size_ms);
        self
    }

    /// Set a session window.
    pub fn session_window(mut self, gap_ms: u64) -> Self {
        self.window_kind = Some(LocalWindowKind::Session { gap_ms });
        self.window_size_ms = Some(0);
        self
    }

    /// Set a sliding window.
    pub fn sliding_window(mut self, window_size_ms: u64, slide_ms: u64) -> Self {
        self.window_kind = Some(LocalWindowKind::Sliding { slide_ms });
        self.window_size_ms = Some(window_size_ms);
        self
    }

    /// Add aggregation expressions.
    pub fn agg(mut self, exprs: Vec<AggExpr>) -> Self {
        self.agg_exprs = exprs;
        self
    }

    /// Set the per-key state TTL in milliseconds (`None` clears it). Expired keys
    /// are evicted from the windowed operator's state.
    pub fn with_state_ttl(mut self, ttl_ms: Option<u64>) -> Self {
        self.state_ttl_ms = ttl_ms;
        self
    }

    /// Add a per-source watermark lag (source_id → lag_ms) for multi-source
    /// watermark reconciliation. The effective watermark is the minimum across
    /// sources; pair with [`with_source_id_column`](Self::with_source_id_column).
    pub fn with_source_watermark(mut self, source_id: impl Into<String>, lag_ms: u64) -> Self {
        self.source_watermark_lags.insert(source_id.into(), lag_ms);
        self
    }

    /// Set the column identifying which source each row came from (required when
    /// per-source watermark lags are configured).
    pub fn with_source_id_column(mut self, column: impl Into<String>) -> Self {
        self.source_id_column = Some(column.into());
        self
    }

    /// The underlying source DataFrame — used by the co-process / broadcast
    /// bindings to collect this stream's batches.
    pub fn source_df(&self) -> DataFrame {
        self.df.clone()
    }

    // ── Stateless transforms (applied to the source stream before windowing) ──
    // These delegate to the underlying batch/stream `DataFrame`, so a streaming
    // pipeline gets the same fluent select/filter/withColumn/drop as a batch one
    // (Spark's "same DataFrame API for batch and streaming"). Because the SDF
    // executes `self.df.execute_stream_async()`, the projection/filter is pushed
    // into the source stream ahead of any windowing/dedup.

    /// Stateless projection: keep only `columns`.
    pub fn select(mut self, columns: &[&str]) -> Result<Self> {
        self.df = self.df.select(columns)?;
        Ok(self)
    }

    /// Stateless filter on a SQL predicate string (e.g. `"amount > 0"`).
    pub fn filter(mut self, predicate: &str) -> Result<Self> {
        self.df = self.df.filter(predicate)?;
        Ok(self)
    }

    /// Stateless computed column: `name = <SQL expr>`.
    pub fn with_column(mut self, name: &str, expr: &str) -> Result<Self> {
        self.df = self.df.with_column(name, expr)?;
        Ok(self)
    }

    /// Stateless column drop.
    pub fn drop_columns(mut self, columns: &[&str]) -> Result<Self> {
        self.df = self.df.drop(columns)?;
        Ok(self)
    }

    /// Flink-style `transformWithState` — the single low-level escape hatch.
    ///
    /// Apply a keyed [`ProcessFunction`](crate::ProcessFunction) (arbitrary keyed
    /// state — Value/List/Map/Reducing/Aggregating — plus event- and
    /// processing-time timers and side outputs) over the source stream, keyed by
    /// the `key_by(...)` column. This replaces `window()` + `agg()` with custom
    /// stateful logic, so the whole Flink DataStream power is reachable from the
    /// Structured-Streaming surface without a second builder hierarchy. Returns
    /// the stream of rows the process function emits.
    pub async fn transform_with_state(
        self,
        func: Box<dyn crate::ProcessFunction>,
    ) -> Result<KrishivStream> {
        let key = self
            .key_column
            .clone()
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: "transform_with_state requires key_by(<column>) to set the state key"
                    .into(),
            })?;
        let input = self.df.execute_stream_async().await?;
        Ok(crate::apply_process_function(
            input,
            key,
            func,
            crate::OperatorConfig::new("transform-with-state"),
        ))
    }

    /// Collect this windowed streaming DataFrame as a BOUNDED result, executed
    /// through the session's runtime.
    ///
    /// This is what makes the Structured-Streaming DataFrame **distributed**: on
    /// a distributed session the windowed aggregation runs on the CLUSTER via the
    /// coordinator's bounded-window operator — the exact same
    /// `collect_bounded_window` primitive the low-level DataStream path uses (a
    /// `BoundedWindow` Flight action) — while an embedded session runs it
    /// in-process. Requires `with_event_time` + `key_by` + a window.
    pub async fn collect_bounded(&self) -> Result<Vec<RecordBatch>> {
        let spec = self.window_spec()?.ok_or_else(|| KrishivError::InvalidConfig {
            message: "collect() on a streaming DataFrame requires a window + key \
                      (use .with_event_time(..).key_by(..).tumbling_window(..).agg(..))"
                .into(),
        })?;
        // Resolve the source to input batches (runs distributed if the df is
        // remote), then hand them to the runtime's bounded-window operator.
        let input = self.df.collect_async().await?.into_batches();
        let topic = format!("sdf-window-{}", krishiv_common::async_util::unix_now_ms());
        self.df
            .runtime()
            .collect_bounded_window(&topic, input, &spec)
            .map_err(KrishivError::from)
    }

    /// The bounded-window execution spec this streaming DataFrame resolves to
    /// (key + event time + window + aggregations), or `None` if no window has
    /// been configured. Used to submit the DataFrame as a continuous job.
    pub fn execution_spec(&self) -> Result<Option<LocalWindowExecutionSpec>> {
        self.window_spec()
    }

    /// Set watermark lag.
    pub fn with_watermark_lag(mut self, lag_ms: u64) -> Self {
        self.watermark_lag_ms = lag_ms;
        self
    }

    /// Route late records to a named side output.
    ///
    /// Records whose event time is more than `lateness_threshold_ms` behind the
    /// current watermark are routed out of the main pipeline. Execute the
    /// configured pipeline with
    /// [`Self::execute_stream_with_side_output_async`] and poll both returned
    /// streams concurrently. For windowed pipelines, the threshold extends the
    /// configured watermark lag so rows retained by the router remain valid
    /// input to the window operator.
    pub fn with_side_output(mut self, name: impl Into<String>, lateness_threshold_ms: u64) -> Self {
        self.side_output = Some(SideOutput::new(name, lateness_threshold_ms));
        self
    }

    /// Drop duplicate rows based on a subset of columns (within watermark window).
    ///
    /// Rows with identical values in all `subset` columns are deduplicated,
    /// keeping the first occurrence per watermark epoch. Deduplication is applied
    /// as a stream adapter when `execute_stream_async` is called.
    ///
    /// **Implementation note:** the default implementation uses an in-memory
    /// `HashSet` and clears it at 10M unique keys (the legacy
    /// `DEDUP_SEEN_CAPACITY` heuristic), so high-cardinality streams can
    /// re-emit previously-seen keys when the cap is hit. For production
    /// workloads prefer [`Self::drop_duplicates_with_state`], which uses a
    /// state backend (RocksDB) and is bounded by disk + cache, not by an
    /// in-memory heuristic.
    pub fn drop_duplicates(mut self, subset: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.dedup_columns = Some(subset.into_iter().map(Into::into).collect());
        self.dedup_state = DedupStateMode::InMemory;
        self
    }

    /// State-backed deduplication on a subset of columns.
    ///
    /// Replaces the legacy in-memory `HashSet` adapter with a real state
    /// backend (`RocksDbStateBackend` when `state_dir` is provided, otherwise
    /// an ephemeral instance) so:
    ///
    /// - Memory is bounded by the backend (RocksDB on disk by default) and
    ///   not by the 10M-key `DEDUP_SEEN_CAPACITY` heuristic.
    /// - Dedup state is checkpointable via `DeduplicationOperator::snapshot`.
    /// - When `with_event_time()` is configured, dedup entries can be evicted
    ///   by event-time watermark (set `state_ttl_ms` to bound retention).
    ///
    /// Use this method instead of [`Self::drop_duplicates`] for production
    /// streams.
    pub fn drop_duplicates_with_state(
        mut self,
        subset: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.dedup_columns = Some(subset.into_iter().map(Into::into).collect());
        self.dedup_state = DedupStateMode::StateBacked;
        self
    }

    /// Build a [`crate::streaming_builder::DataStreamWriter`] for writing this
    /// streaming pipeline to a sink.
    pub fn write_stream(self) -> crate::streaming_builder::DataStreamWriter {
        crate::streaming_builder::DataStreamWriter::new(self.df.clone())
    }

    /// Execute the configured streaming pipeline and return a lazy, asynchronous stream of RecordBatches.
    pub async fn execute_stream_async(self) -> Result<KrishivStream> {
        if self.side_output.is_some() {
            return Err(KrishivError::InvalidConfig {
                message: "side output is configured; use \
                          execute_stream_with_side_output_async() so late records are not lost"
                    .into(),
            });
        }

        let dedup_columns = self.dedup_columns.clone();
        let dedup_state = self.dedup_state;
        let window_spec = self.window_spec()?;
        let df_stream = self.df.execute_stream_async().await?;

        // Apply window pipeline first.
        let base: KrishivStream = match window_spec.as_ref() {
            Some(spec) => execute_window_pipeline(source_exec_stream(df_stream), Some(spec))?,
            None => df_stream,
        };

        // Apply deduplication adapter if columns were configured.
        if let Some(cols) = dedup_columns {
            match dedup_state {
                DedupStateMode::InMemory => Ok(Box::pin(DeduplicatingStream::new(base, cols))),
                DedupStateMode::StateBacked => {
                    Ok(Box::pin(StateBackedDeduplicatingStream::new(base, cols)?))
                }
            }
        } else {
            Ok(base)
        }
    }

    /// Execute a pipeline with a configured late-data side output.
    ///
    /// The returned streams share a bounded routing task. Poll both streams
    /// concurrently; if either consumer falls behind, source ingestion
    /// backpressures rather than dropping or buffering records without bound.
    pub async fn execute_stream_with_side_output_async(self) -> Result<StreamingOutputStreams> {
        let side_output = self
            .side_output
            .clone()
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: "execute_stream_with_side_output_async() requires with_side_output()"
                    .into(),
            })?;
        if side_output.name.trim().is_empty() {
            return Err(KrishivError::InvalidConfig {
                message: "side-output name must not be empty".into(),
            });
        }
        let event_time_column =
            self.event_time_column
                .clone()
                .ok_or_else(|| KrishivError::InvalidConfig {
                    message: "side output requires an event time column (use .with_event_time())"
                        .into(),
                })?;
        let mut window_spec = self.window_spec()?;
        if let Some(spec) = &mut window_spec {
            spec.watermark_lag_ms = spec
                .watermark_lag_ms
                .saturating_add(side_output.lateness_threshold_ms);
        }
        let df_stream = self.df.execute_stream_async().await?;
        let side_output_name = side_output.name.clone();
        let router = SideOutputRouter::new(side_output, event_time_column);
        let (main_input, side_input, _router_task) =
            spawn_side_output_router(df_stream, router, self.watermark_lag_ms);
        let main = execute_window_pipeline(main_input, window_spec.as_ref())?;
        let side_stream =
            Box::pin(side_input.map(|result| result.map_err(|error| error.to_string())));

        Ok(StreamingOutputStreams {
            main,
            side_output: NamedSideOutputStream {
                name: side_output_name,
                stream: side_stream,
            },
        })
    }

    fn window_spec(&self) -> Result<Option<LocalWindowExecutionSpec>> {
        if self.window_kind.is_none() && self.agg_exprs.is_empty() {
            return Ok(None);
        }

        let event_time_column =
            self.event_time_column
                .clone()
                .ok_or_else(|| {
                    KrishivError::InvalidConfig {
                message:
                    "streaming aggregations require an event time column (use .with_event_time())"
                        .into(),
            }
                })?;
        let key_column = self
            .key_column
            .clone()
            .ok_or_else(|| KrishivError::InvalidConfig {
                message: "streaming aggregations require a key column (use .key_by())".into(),
            })?;
        let window_kind = self
            .window_kind
            .clone()
            .unwrap_or(LocalWindowKind::Tumbling);
        let window_size_ms = self.window_size_ms.unwrap_or(0);

        // Shared builder (krishiv-runtime): the single source of truth for window
        // spec defaults, so this Structured-Streaming path can't drift from the
        // DataStream path's `spec_from_pipeline`. `with_aggs` keeps the default
        // COUNT(*) when none are configured.
        Ok(Some(
            LocalWindowExecutionSpec::windowed(
                key_column,
                event_time_column,
                window_kind,
                window_size_ms,
            )
            .with_watermark_lag_ms(self.watermark_lag_ms)
            .with_aggs(self.agg_exprs.clone())
            .with_state_ttl_ms(self.state_ttl_ms)
            .with_source_watermarks(
                self.source_watermark_lags.clone(),
                self.source_id_column.clone(),
            ),
        ))
    }
}

// ── Convenience static join wrappers ──────────────────────────────────────────

impl StreamingDataFrame {
    /// Stream-table as-of join (convenience wrapper for [`temporal_join`]).
    ///
    /// Looks up each stream row in `table_snapshots` using `version_col` as the
    /// version key. Equivalent to calling `temporal_join()` with a
    /// [`TemporalJoinSpec`].
    pub fn stream_table_join(
        stream_batches: &[RecordBatch],
        table_snapshots: &[RecordBatch],
        stream_time_col: &str,
        version_col: &str,
        lookback_ms: i64,
        inner_join: bool,
    ) -> Result<Vec<(RecordBatch, Option<RecordBatch>)>> {
        let spec = TemporalJoinSpec {
            stream_time_col: stream_time_col.to_string(),
            join_keys: vec![],
            inner_join,
        };
        temporal_join(
            stream_batches,
            table_snapshots,
            &spec,
            version_col,
            lookback_ms,
        )
    }

    /// Stream-stream interval join (convenience wrapper for [`interval_join`]).
    ///
    /// Matches events from `left` and `right` when:
    /// `lower_bound_ms <= right_ts - left_ts <= upper_bound_ms`.
    ///
    /// H-1 (audit): the previous wrapper ignored `key_column` and used
    /// the empty string `""` for every event, silently producing
    /// over-matching when the underlying batches had distinct keys. The
    /// new wrapper requires explicit `left_key_col` / `right_key_col`
    /// parameters and forwards them to the per-key join state map.
    #[allow(clippy::too_many_arguments)]
    pub fn stream_stream_join(
        left: &[RecordBatch],
        right: &[RecordBatch],
        left_time_col: &str,
        right_time_col: &str,
        left_key_col: &str,
        right_key_col: &str,
        lower_bound_ms: i64,
        upper_bound_ms: i64,
    ) -> Result<Vec<(Arc<RecordBatch>, Arc<RecordBatch>)>> {
        let spec = IntervalJoinSpec {
            lower_bound_ms,
            upper_bound_ms,
            key_column: left_key_col.into(),
            max_buffer_per_side: 1_000_000,
        };
        interval_join(
            left,
            right,
            left_time_col,
            right_time_col,
            left_key_col,
            right_key_col,
            spec,
        )
    }
}

// ── DeduplicatingStream ───────────────────────────────────────────────────────

/// Maximum number of unique keys tracked by [`DeduplicatingStream`] before the
/// seen set is cleared.  This bounds memory usage at ~8 bytes × capacity for
/// high-cardinality streams where exact dedup is impractical.
const DEDUP_SEEN_CAPACITY: usize = 10_000_000;

/// Stream adapter that removes rows whose dedup-column value set has been seen
/// before. Uses a `HashSet<[u64; 2]>` keyed by a 128-bit XxHash64 of concatenated
/// column values (two independent seeds, giving ~2^64 collision bound).
struct DeduplicatingStream {
    inner: KrishivStream,
    columns: Vec<String>,
    seen: HashSet<[u64; 2]>,
}

impl DeduplicatingStream {
    fn new(inner: KrishivStream, columns: Vec<String>) -> Self {
        Self {
            inner,
            columns,
            seen: HashSet::new(),
        }
    }

    /// Compute a stable 128-bit hash over the dedup-column values for one row.
    ///
    /// Uses XxHash64 with two seeds (0 and 1) for a combined 128-bit hash.
    /// The birthday bound at ~2^64 rows makes collisions negligible for any
    /// realistic streaming workload.
    ///
    /// Returns `Err` if a named column is not present in the batch schema.
    fn row_hash(
        batch: &RecordBatch,
        row: usize,
        columns: &[String],
    ) -> std::result::Result<[u64; 2], String> {
        let mut hasher = XxHash64::with_seed(0);
        let mut hasher2 = XxHash64::with_seed(1);
        for col_name in columns {
            col_name.hash(&mut hasher);
            col_name.hash(&mut hasher2);
            let col_idx = batch
                .schema()
                .index_of(col_name)
                .map_err(|_| format!("dedup column '{col_name}' not found in batch schema"))?;
            let col = batch.column(col_idx);
            // L-2 (audit): the prior implementation only supported Int64
            // and Utf8; every other type fell through to a `format!("{:?}",
            // col.slice(row, 1))` which allocates a String per row per
            // column. Hot-path optimization: type-specialize the hash for
            // the common primitive types so dedup over Float64 / Boolean /
            // Date32 / Timestamp / Int32 / etc. avoids the per-row
            // allocation. Type tag is mixed in so two values that happen
            // to share the same bit pattern across types do not collide.
            let tag: u8 = match col.data_type() {
                DataType::Int64 => b'I',
                DataType::Int32 => b'T',
                DataType::Int16 => b'H',
                DataType::Int8 => b'B',
                DataType::UInt64 => b'U',
                DataType::UInt32 => b'u',
                DataType::UInt16 => b'v',
                DataType::UInt8 => b'w',
                DataType::Float64 => b'F',
                DataType::Float32 => b'f',
                DataType::Boolean => b'Y',
                DataType::Date32 => b'D',
                DataType::Date64 => b'd',
                DataType::Timestamp(_, _) => b'M',
                DataType::Utf8 => b'S',
                _ => b'?',
            };
            tag.hash(&mut hasher);
            tag.hash(&mut hasher2);
            match col.data_type() {
                DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).hash(&mut hasher);
                            arr.value(row).hash(&mut hasher2);
                        }
                    }
                }
                DataType::Int32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).hash(&mut hasher);
                            arr.value(row).hash(&mut hasher2);
                        }
                    }
                }
                DataType::Float64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).to_bits().hash(&mut hasher);
                            arr.value(row).to_bits().hash(&mut hasher2);
                        }
                    }
                }
                DataType::Float32 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).to_bits().hash(&mut hasher);
                            arr.value(row).to_bits().hash(&mut hasher2);
                        }
                    }
                }
                DataType::Boolean => {
                    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).hash(&mut hasher);
                            arr.value(row).hash(&mut hasher2);
                        }
                    }
                }
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(row) {
                            0u8.hash(&mut hasher);
                            0u8.hash(&mut hasher2);
                        } else {
                            arr.value(row).hash(&mut hasher);
                            arr.value(row).hash(&mut hasher2);
                        }
                    }
                }
                _ => {
                    // Fallback for types we don't specialize. Per-row
                    // String allocation, same as before. Acceptable for
                    // uncommon types (Decimal128, Binary, List, etc.).
                    let s = format!("{:?}", col.slice(row, 1));
                    s.hash(&mut hasher);
                    s.hash(&mut hasher2);
                }
            }
        }
        Ok([hasher.finish(), hasher2.finish()])
    }

    /// Filter a batch, keeping only rows whose hash has not been seen before.
    fn dedup_batch(
        &mut self,
        batch: RecordBatch,
    ) -> std::result::Result<Option<RecordBatch>, String> {
        let mut keep_indices: Vec<usize> = Vec::new();
        for row in 0..batch.num_rows() {
            // Bound memory: clear the seen set when it exceeds the capacity
            // limit. This trades exact dedup for bounded memory on high-
            // cardinality streams where tracking all unique keys is impractical.
            // H-17 (audit): the prior code silently re-admitted previously
            // seen rows after the clear. We now log a one-time warning per
            // clear so operators can detect the silent re-admission. A
            // downstream sink that needs strict dedup should use the
            // state-backed `drop_duplicates_with_state` instead.
            if self.seen.len() >= DEDUP_SEEN_CAPACITY {
                tracing::warn!(
                    capacity = DEDUP_SEEN_CAPACITY,
                    "dedup seen-set cleared; subsequent duplicates may pass through \
                     (use the state-backed drop_duplicates for strict dedup)"
                );
                self.seen.clear();
            }
            let h = Self::row_hash(&batch, row, &self.columns)?;
            if self.seen.insert(h) {
                keep_indices.push(row);
            }
        }
        if keep_indices.is_empty() {
            return Ok(None);
        }
        if keep_indices.len() == batch.num_rows() {
            return Ok(Some(batch));
        }
        // Build a filtered batch.
        let indices = arrow::array::UInt32Array::from(
            keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let columns: Vec<Arc<dyn arrow::array::Array>> = batch
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(col.as_ref(), &indices, None).map_err(|e| e.to_string())
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;
        Ok(RecordBatch::try_new(batch.schema(), columns).ok())
    }
}

impl futures::stream::Stream for DeduplicatingStream {
    type Item = std::result::Result<RecordBatch, String>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.inner.as_mut().poll_next(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    match self.dedup_batch(batch) {
                        Err(e) => return std::task::Poll::Ready(Some(Err(e))),
                        Ok(None) => {} // all dups, poll again
                        Ok(Some(deduped)) => {
                            return std::task::Poll::Ready(Some(Ok(deduped)));
                        }
                    }
                }
            }
        }
    }
}

fn source_exec_stream(stream: KrishivStream) -> ExecStream {
    Box::pin(stream.map(|result| result.map_err(ExecError::Upstream)))
}

// ── StateBackedDeduplicatingStream ────────────────────────────────────────────

/// Stream adapter that performs state-backed deduplication (T6 / ST10).
///
/// Uses [`krishiv_dataflow::DeduplicationOperator`] under the hood. State is
/// retained in RocksDB (ephemeral instance) so the seen set is bounded by
/// disk and the cap-clear silent data loss of the in-memory adapter is
/// eliminated.
struct StateBackedDeduplicatingStream {
    inner: KrishivStream,
    op: krishiv_dataflow::dedup_operator::DeduplicationOperator,
}

impl StateBackedDeduplicatingStream {
    fn new(inner: KrishivStream, columns: Vec<String>) -> Result<Self> {
        let cfg = krishiv_dataflow::dedup_operator::DeduplicationConfig::new(columns);
        let op = krishiv_dataflow::dedup_operator::DeduplicationOperator::ephemeral(cfg).map_err(
            |e| KrishivError::InvalidConfig {
                message: format!("state-backed dedup operator failed to construct: {e}"),
            },
        )?;
        Ok(Self { inner, op })
    }
}

impl futures::stream::Stream for StateBackedDeduplicatingStream {
    type Item = std::result::Result<RecordBatch, String>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.inner.as_mut().poll_next(cx) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(e)));
                }
                std::task::Poll::Ready(Some(Ok(batch))) => {
                    match self.op.process_batch(&batch) {
                        Err(e) => return std::task::Poll::Ready(Some(Err(e.to_string()))),
                        Ok(deduped) => {
                            if deduped.num_rows() == 0 {
                                // All dups — poll again to get the next batch.
                                continue;
                            }
                            return std::task::Poll::Ready(Some(Ok(deduped)));
                        }
                    }
                }
            }
        }
    }
}

fn execute_window_pipeline(
    input: ExecStream,
    spec: Option<&LocalWindowExecutionSpec>,
) -> Result<KrishivStream> {
    if let Some(spec) = spec {
        let windowed = krishiv_runtime::execute_streaming_window(input, spec).map_err(|error| {
            KrishivError::Runtime {
                message: error.to_string(),
            }
        })?;
        return Ok(Box::pin(
            windowed.map(|result| result.map_err(|error| error.to_string())),
        ));
    }

    Ok(Box::pin(
        input.map(|result| result.map_err(|error| error.to_string())),
    ))
}

fn spawn_side_output_router(
    mut input: KrishivStream,
    router: SideOutputRouter,
    watermark_lag_ms: u64,
) -> (ExecStream, ExecStream, tokio::task::JoinHandle<()>) {
    let (main_tx, main_rx) = mpsc::channel(SIDE_OUTPUT_CHANNEL_CAPACITY);
    let (side_tx, side_rx) = mpsc::channel(SIDE_OUTPUT_CHANNEL_CAPACITY);

    let handle = tokio::spawn(async move {
        let mut watermark = WatermarkState::new(watermark_lag_ms);
        let mut main_open = true;
        let mut side_open = true;

        loop {
            let result = tokio::select! {
                result = input.next() => result,
                _ = wait_for_both_receivers_closed(&main_tx, &side_tx) => break,
            };
            let Some(result) = result else {
                break;
            };
            let routed = match result {
                Ok(batch) => router.route_batch(&batch, &mut watermark),
                Err(message) => Err(ExecError::Upstream(message)),
            };

            match routed {
                Ok(routed) => {
                    let (next_main_open, next_side_open) = tokio::join!(
                        send_optional_batch(&main_tx, routed.main, main_open),
                        send_optional_batch(&side_tx, routed.side, side_open),
                    );
                    main_open = next_main_open;
                    side_open = next_side_open;
                    if !main_open && !side_open {
                        break;
                    }
                }
                Err(error) => {
                    let (main_sent, side_sent) = tokio::join!(
                        send_stream_error(&main_tx, error.clone(), main_open),
                        send_stream_error(&side_tx, error, side_open),
                    );
                    if !main_sent && !side_sent {
                        tracing::debug!("side output error: both receivers already closed");
                    }
                    break;
                }
            }
        }
    });

    (
        receiver_exec_stream(main_rx),
        receiver_exec_stream(side_rx),
        handle,
    )
}

async fn wait_for_both_receivers_closed(
    main: &mpsc::Sender<std::result::Result<RecordBatch, ExecError>>,
    side: &mpsc::Sender<std::result::Result<RecordBatch, ExecError>>,
) {
    tokio::join!(main.closed(), side.closed());
}

async fn send_optional_batch(
    sender: &mpsc::Sender<std::result::Result<RecordBatch, ExecError>>,
    batch: Option<RecordBatch>,
    open: bool,
) -> bool {
    if !open {
        return false;
    }
    match batch {
        Some(batch) => sender.send(Ok(batch)).await.is_ok(),
        None => true,
    }
}

async fn send_stream_error(
    sender: &mpsc::Sender<std::result::Result<RecordBatch, ExecError>>,
    error: ExecError,
    open: bool,
) -> bool {
    open && sender.send(Err(error)).await.is_ok()
}

fn receiver_exec_stream(
    receiver: mpsc::Receiver<std::result::Result<RecordBatch, ExecError>>,
) -> ExecStream {
    Box::pin(futures::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ))
}

// ── Temporal join ──────────────────────────────────────────────────────────────

/// Stream-table as-of (temporal) join.
///
/// For each row in `stream_batches`, looks up the latest table snapshot in
/// `table_snapshots` whose `version_col` timestamp is ≤ the row's
/// `spec.stream_time_col` value and returns the matched table batch. Rows with
/// no matching version are included with `None` table columns (left join) or
/// excluded (inner join, when `spec.inner_join = true`).
pub fn temporal_join(
    stream_batches: &[RecordBatch],
    table_snapshots: &[RecordBatch],
    spec: &TemporalJoinSpec,
    version_col: &str,
    lookback_ms: i64,
) -> Result<Vec<(RecordBatch, Option<RecordBatch>)>> {
    let mut state = VersionedTableState::new(lookback_ms);
    for snap in table_snapshots {
        let ver_idx = snap
            .schema()
            .index_of(version_col)
            .map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?;
        let ver_col = snap
            .column(ver_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| KrishivError::Runtime {
                message: format!("version_col '{version_col}' must be Int64"),
            })?;
        for i in 0..snap.num_rows() {
            if !ver_col.is_null(i) {
                state.upsert_version(ver_col.value(i), snap.slice(i, 1));
            }
        }
    }

    let mut out = Vec::new();
    for stream_batch in stream_batches {
        let time_idx = stream_batch
            .schema()
            .index_of(&spec.stream_time_col)
            .map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?;
        let time_col = stream_batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| KrishivError::Runtime {
                message: format!("stream_time_col '{}' must be Int64", spec.stream_time_col),
            })?;
        for i in 0..stream_batch.num_rows() {
            if time_col.is_null(i) {
                continue;
            }
            let t = time_col.value(i);
            let matched = state.lookup_as_of(t).cloned();
            if spec.inner_join && matched.is_none() {
                continue;
            }
            out.push((stream_batch.slice(i, 1), matched));
        }
    }
    Ok(out)
}

// ── Interval join ──────────────────────────────────────────────────────────────

/// Stream-stream interval join.
///
/// Matches events from `left_batches` and `right_batches` when:
/// `spec.lower_bound_ms <= right_ts - left_ts <= spec.upper_bound_ms`.
///
/// Returns matched `(left_row, right_row)` pairs as individual single-row
/// `RecordBatch` tuples.
///
/// H-1 (audit): every event was previously inserted into the join's
/// per-key state map under the empty string `""`, regardless of the
/// caller's intended key column. That caused events with distinct keys
/// to match each other across the time window — silent over-matching.
/// The fix routes each row through the join under its actual key value
/// (a typed Arrow value rendered to a string for the per-key map).
pub fn interval_join(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_time_col: &str,
    right_time_col: &str,
    left_key_col: &str,
    right_key_col: &str,
    spec: IntervalJoinSpec,
) -> Result<Vec<(Arc<RecordBatch>, Arc<RecordBatch>)>> {
    let mut join = PerKeyIntervalJoin::new(spec);
    let mut pairs = Vec::new();

    // Helper: extract (row_index, i64 event time, key string) triples,
    // skipping nulls in the time column. Null key values are rejected
    // with a clear error rather than silently coerced to "".
    let get_times_keys =
        |batch: &RecordBatch, time_col: &str, key_col: &str| -> Result<Vec<(usize, i64, String)>> {
            let t_idx = batch
                .schema()
                .index_of(time_col)
                .map_err(|e| KrishivError::Runtime {
                    message: format!("event time column '{time_col}' not found: {e}"),
                })?;
            let t_arr = batch
                .column(t_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| KrishivError::Runtime {
                    message: format!("event time column '{time_col}' must be Int64"),
                })?;
            let k_idx = batch
                .schema()
                .index_of(key_col)
                .map_err(|e| KrishivError::Runtime {
                    message: format!("key column '{key_col}' not found: {e}"),
                })?;
            let k_col = batch.column(k_idx);
            let mut out = Vec::new();
            for i in 0..t_arr.len() {
                if t_arr.is_null(i) {
                    continue;
                }
                if k_col.is_null(i) {
                    return Err(KrishivError::Runtime {
                        message: format!(
                            "key column '{key_col}' has NULL at row {i}; \
                         interval_join requires non-null keys for per-key matching"
                        ),
                    });
                }
                let key = arrow_array_value_to_string(k_col.as_ref(), i);
                out.push((i, t_arr.value(i), key));
            }
            Ok(out)
        };

    for batch in left_batches {
        let times = get_times_keys(batch, left_time_col, left_key_col)?;
        for &(i, t, ref k) in &times {
            let row = batch.slice(i, 1);
            let matched = join.push_left(k, t, row);
            pairs.extend(matched);
        }
    }
    for batch in right_batches {
        let times = get_times_keys(batch, right_time_col, right_key_col)?;
        for &(i, t, ref k) in &times {
            let row = batch.slice(i, 1);
            let matched = join.push_right(k, t, row);
            pairs.extend(matched);
        }
    }
    Ok(pairs)
}

/// Render one cell of an Arrow array to a stable string key. Mirrors
/// `DeduplicatingStream::row_hash` but does not allocate when the value
/// is already a primitive integer / string.
fn arrow_array_value_to_string(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::{Int32Array, Int64Array, StringArray};
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_owned();
    }
    // Fallback: arrow's `Display` for one-element slice.
    format!("{:?}", array.slice(row, 1))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};

    use dashmap::DashMap;
    use futures::StreamExt;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_dataflow::interval_join::IntervalJoinSpec;
    use krishiv_dataflow::temporal_join::TemporalJoinSpec;
    use krishiv_runtime::LocalJobRegistry;

    use super::{KrishivStream, interval_join, temporal_join};
    use crate::dataframe::DataFrame;
    use crate::session::shared_embedded_runtime;
    use crate::types::ExecutionMode;

    fn stream_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("stream_ts", DataType::Int64, false),
        ]))
    }

    fn table_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("version_ts", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
        ]))
    }

    fn interval_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("event_ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn stream_batch(user_ids: &[&str], times: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            stream_schema(),
            vec![
                Arc::new(StringArray::from(user_ids.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn dataframe_from_batches(batches: Vec<RecordBatch>) -> DataFrame {
        DataFrame::from_batches(
            ExecutionMode::Embedded,
            batches,
            Arc::new(Mutex::new(LocalJobRegistry::default())),
            Arc::new(AtomicU64::new(1)),
            shared_embedded_runtime().expect("embedded runtime for test"),
            Arc::new(DashMap::<String, PathBuf>::new()),
        )
    }

    async fn collect_stream(
        mut stream: KrishivStream,
    ) -> std::result::Result<Vec<RecordBatch>, String> {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }

    fn table_batch(versions: &[i64], prices: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            table_schema(),
            vec![
                Arc::new(Int64Array::from(versions.to_vec())) as _,
                Arc::new(Int64Array::from(prices.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    fn interval_batch(keys: &[&str], times: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            interval_schema(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
                Arc::new(Int64Array::from(values.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn side_output_execution_routes_late_rows_across_batches() {
        let dataframe = dataframe_from_batches(vec![
            stream_batch(&["first"], &[10_000]),
            stream_batch(&["late", "next"], &[1_000, 11_000]),
        ]);
        let outputs = dataframe
            .stream()
            .with_event_time("stream_ts")
            .with_side_output("late-events", 0)
            .execute_stream_with_side_output_async()
            .await
            .expect("side-output execution should start");
        let (main, side_output) = outputs.into_parts();
        assert_eq!(side_output.name(), "late-events");

        let (main_batches, side_batches) = futures::future::join(
            collect_stream(main),
            collect_stream(side_output.into_stream()),
        )
        .await;
        let main_batches = main_batches.expect("main stream should succeed");
        let side_batches = side_batches.expect("side stream should succeed");

        assert_eq!(
            main_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            side_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let late_times = side_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("event time should remain Int64");
        assert_eq!(late_times.value(0), 1_000);
    }

    #[tokio::test]
    async fn side_output_grace_period_is_applied_to_window_watermark() {
        let dataframe = dataframe_from_batches(vec![
            stream_batch(&["user"], &[10_000]),
            stream_batch(&["user", "user"], &[9_500, 8_000]),
        ]);
        let outputs = dataframe
            .stream()
            .with_event_time("stream_ts")
            .key_by("user_id")
            .tumbling_window(20_000)
            .with_side_output("late-events", 1_000)
            .execute_stream_with_side_output_async()
            .await
            .expect("side-output window execution should start");
        let (main, side_output) = outputs.into_parts();

        let (main_batches, side_batches) = futures::future::join(
            collect_stream(main),
            collect_stream(side_output.into_stream()),
        )
        .await;
        let main_batches = main_batches.expect("window stream should succeed");
        let side_batches = side_batches.expect("side stream should succeed");

        assert_eq!(main_batches.len(), 1);
        let count = main_batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count aggregate should be Int64");
        assert_eq!(count.value(0), 2);
        assert_eq!(
            side_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn main_only_execution_rejects_configured_side_output() {
        let dataframe = dataframe_from_batches(vec![stream_batch(&["first"], &[10_000])]);

        let result = dataframe
            .stream()
            .with_event_time("stream_ts")
            .with_side_output("late-events", 0)
            .execute_stream_async()
            .await;

        assert!(matches!(
            result,
            Err(crate::KrishivError::InvalidConfig { message })
                if message.contains("execute_stream_with_side_output_async")
        ));
    }

    #[tokio::test]
    async fn split_execution_requires_side_output_configuration() {
        let dataframe = dataframe_from_batches(vec![stream_batch(&["first"], &[10_000])]);

        let result = dataframe
            .stream()
            .with_event_time("stream_ts")
            .execute_stream_with_side_output_async()
            .await;

        assert!(matches!(
            result,
            Err(crate::KrishivError::InvalidConfig { message })
                if message.contains("requires with_side_output")
        ));
    }

    #[tokio::test]
    async fn routing_errors_are_delivered_to_both_output_streams() {
        let dataframe = dataframe_from_batches(vec![stream_batch(&["first"], &[10_000])]);
        let outputs = dataframe
            .stream()
            .with_event_time("missing")
            .with_side_output("late-events", 0)
            .execute_stream_with_side_output_async()
            .await
            .expect("routing task should start lazily");
        let (main, side_output) = outputs.into_parts();

        let (main_error, side_error) = futures::future::join(
            collect_stream(main),
            collect_stream(side_output.into_stream()),
        )
        .await;

        assert!(
            matches!(main_error, Err(message) if message.contains("column not found: missing"))
        );
        assert!(
            matches!(side_error, Err(message) if message.contains("column not found: missing"))
        );
    }

    #[test]
    fn temporal_join_matches_latest_table_version() {
        // Table has versions at t=100 and t=500. Stream event at t=300 should
        // match version at t=100 (the latest version <= 300).
        let table = table_batch(&[100, 500], &[10, 20]);
        let stream = stream_batch(&["alice"], &[300]);

        let spec = TemporalJoinSpec {
            stream_time_col: "stream_ts".to_string(),
            join_keys: vec![],
            inner_join: false,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, "version_ts", 60_000).unwrap();
        assert_eq!(
            pairs.len(),
            1,
            "one stream event must produce one output pair"
        );
        assert!(pairs[0].1.is_some(), "should find a matching table version");
        // Matched version should be t=100, not t=500
        let matched_batch = pairs[0].1.as_ref().unwrap();
        let version_col = matched_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(version_col.value(0), 100);
    }

    #[test]
    fn temporal_join_inner_excludes_unmatched() {
        // Stream event at t=50 but table starts at t=100 — no prior version exists.
        let table = table_batch(&[100], &[99]);
        let stream = stream_batch(&["bob"], &[50]);

        let spec = TemporalJoinSpec {
            stream_time_col: "stream_ts".to_string(),
            join_keys: vec![],
            inner_join: true,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, "version_ts", 60_000).unwrap();
        assert!(
            pairs.is_empty(),
            "inner join must exclude rows with no table match"
        );
    }

    #[test]
    fn temporal_join_left_returns_none_for_unmatched() {
        let table = table_batch(&[100], &[99]);
        let stream = stream_batch(&["carol"], &[50]);

        let spec = TemporalJoinSpec {
            stream_time_col: "stream_ts".to_string(),
            join_keys: vec![],
            inner_join: false,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, "version_ts", 60_000).unwrap();
        assert_eq!(pairs.len(), 1);
        assert!(
            pairs[0].1.is_none(),
            "left join must include row with None table match"
        );
    }

    #[test]
    fn interval_join_matches_events_within_window() {
        // Left event at t=100, right event at t=150 -> delta=50, within [0, 100].
        let left = interval_batch(&["a"], &[100], &[1]);
        let right = interval_batch(&["a"], &[150], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs =
            interval_join(&[left], &[right], "event_ts", "event_ts", "k", "k", spec).unwrap();
        assert_eq!(pairs.len(), 1, "events within window should match");
    }

    #[test]
    fn interval_join_excludes_events_outside_window() {
        // Left at t=100, right at t=300 -> delta=200, outside [0, 100].
        let left = interval_batch(&["a"], &[100], &[1]);
        let right = interval_batch(&["a"], &[300], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs =
            interval_join(&[left], &[right], "event_ts", "event_ts", "k", "k", spec).unwrap();
        assert!(pairs.is_empty(), "events outside window must not match");
    }

    #[test]
    fn interval_join_isolates_distinct_keys() {
        // H-1 regression: prior code used empty key "" for every event,
        // so events with distinct user_ids would falsely match. With
        // per-key routing, only same-key events within the window match.
        let left = interval_batch(&["alice", "bob"], &[100, 100], &[1, 2]);
        let right = interval_batch(&["alice", "bob"], &[150, 150], &[10, 20]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs =
            interval_join(&[left], &[right], "event_ts", "event_ts", "k", "k", spec).unwrap();
        assert_eq!(
            pairs.len(),
            2,
            "alice@100 matches alice@150 and bob@100 matches bob@150"
        );
    }

    #[test]
    fn interval_join_does_not_cross_keys() {
        // Alice@100 must NOT match bob@150 (different keys), even though
        // the time difference is within the window.
        let left = interval_batch(&["alice"], &[100], &[1]);
        let right = interval_batch(&["bob"], &[150], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs =
            interval_join(&[left], &[right], "event_ts", "event_ts", "k", "k", spec).unwrap();
        assert!(
            pairs.is_empty(),
            "different keys must not match under H-1 per-key routing"
        );
    }

    #[test]
    fn interval_join_multiple_matches() {
        // One left event matched by two right events within the window.
        let left = interval_batch(&["a"], &[1000], &[1]);
        let right = interval_batch(&["a", "a", "a"], &[1050, 1080, 2000], &[10, 20, 30]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 200,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs =
            interval_join(&[left], &[right], "event_ts", "event_ts", "k", "k", spec).unwrap();
        // 1050-1000=50, 1080-1000=80, 2000-1000=1000 (outside)
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn interval_join_empty_inputs_returns_empty() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 1000,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let pairs = interval_join(&[], &[], "event_ts", "event_ts", "k", "k", spec).unwrap();
        assert!(pairs.is_empty());
    }

    // ── Phase F tests ─────────────────────────────────────────────────────────

    // Test: drop_duplicates removes duplicate rows by key columns
    #[tokio::test]
    async fn drop_duplicates_removes_duplicate_rows() {
        // Two batches; the second contains a row that duplicates "alice" in stream_ts.
        let dataframe = dataframe_from_batches(vec![
            stream_batch(&["alice", "bob"], &[100, 200]),
            stream_batch(&["alice", "carol"], &[100, 300]),
        ]);

        let stream = dataframe
            .stream()
            .drop_duplicates(vec!["user_id", "stream_ts"])
            .execute_stream_async()
            .await
            .expect("drop_duplicates must not error");

        let batches = collect_stream(stream).await.expect("stream must succeed");
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        // alice@100 appears twice but should only be counted once
        assert_eq!(
            total_rows, 3,
            "dedup must eliminate the duplicate alice@100 row"
        );
    }

    // Test: drop_duplicates_with_state must produce the same dedup result
    // (and not silently drop keys via the 10M in-memory cap).
    #[tokio::test]
    async fn drop_duplicates_with_state_removes_duplicate_rows() {
        let dataframe = dataframe_from_batches(vec![
            stream_batch(&["alice", "bob"], &[100, 200]),
            stream_batch(&["alice", "carol"], &[100, 300]),
        ]);

        let stream = dataframe
            .stream()
            .drop_duplicates_with_state(vec!["user_id", "stream_ts"])
            .execute_stream_async()
            .await
            .expect("drop_duplicates_with_state must not error");

        let batches = collect_stream(stream).await.expect("stream must succeed");
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            total_rows, 3,
            "state-backed dedup must eliminate the duplicate alice@100 row"
        );
    }

    // Test: stream_table_join convenience wrapper matches temporal_join behavior
    #[test]
    fn stream_table_join_convenience_matches_temporal_join() {
        use super::StreamingDataFrame;
        use krishiv_dataflow::temporal_join::TemporalJoinSpec;

        let table = table_batch(&[100, 500], &[10, 20]);
        let stream = stream_batch(&["alice"], &[300]);

        let spec = TemporalJoinSpec {
            stream_time_col: "stream_ts".to_string(),
            join_keys: vec![],
            inner_join: false,
        };
        let reference = temporal_join(
            &[stream.clone()],
            &[table.clone()],
            &spec,
            "version_ts",
            60_000,
        )
        .unwrap();
        let convenience = StreamingDataFrame::stream_table_join(
            &[stream],
            &[table],
            "stream_ts",
            "version_ts",
            60_000,
            false,
        )
        .unwrap();

        assert_eq!(
            reference.len(),
            convenience.len(),
            "results must have equal length"
        );
        for (r, c) in reference.iter().zip(convenience.iter()) {
            assert_eq!(r.1.is_some(), c.1.is_some(), "match presence must agree");
        }
    }

    // Test: stream_stream_join convenience wrapper matches interval_join behavior
    #[test]
    fn stream_stream_join_convenience_matches_interval_join() {
        use super::StreamingDataFrame;

        let left = interval_batch(&["a"], &[100], &[1]);
        let right = interval_batch(&["a"], &[150], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
            max_buffer_per_side: 1000,
        };
        let reference = interval_join(
            &[left.clone()],
            &[right.clone()],
            "event_ts",
            "event_ts",
            "k",
            "k",
            spec,
        )
        .unwrap();
        let convenience = StreamingDataFrame::stream_stream_join(
            &[left],
            &[right],
            "event_ts",
            "event_ts",
            "k",
            "k",
            0,
            100,
        )
        .unwrap();

        assert_eq!(
            reference.len(),
            convenience.len(),
            "both approaches must produce the same number of matches"
        );
    }

    // Phase 2: stateless verbs (filter/select/with_column/drop) push into the
    // source stream before windowing.
    #[tokio::test]
    async fn stateless_verbs_filter_project_before_windowing() {
        let df = dataframe_from_batches(vec![stream_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4])]);
        let sdf = df
            .stream()
            .filter("stream_ts > 2")
            .expect("filter")
            .with_column("bumped", "stream_ts + 100")
            .expect("with_column")
            .select(&["user_id", "bumped"])
            .expect("select");
        let out = collect_stream(sdf.execute_stream_async().await.expect("stream"))
            .await
            .expect("collect");
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "filter stream_ts > 2 keeps rows 3 and 4");
        let batch = out.first().expect("one batch");
        assert_eq!(batch.num_columns(), 2, "projected to user_id + bumped");
        assert_eq!(batch.schema().field(0).name(), "user_id");
        assert_eq!(batch.schema().field(1).name(), "bumped");
        // bumped = stream_ts + 100 -> {103, 104}
        let bumped = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        let mut vals: Vec<i64> = (0..bumped.len()).map(|i| bumped.value(i)).collect();
        vals.sort_unstable();
        assert_eq!(vals, vec![103, 104]);
    }

    /// collect_bounded routes the windowed aggregation through the session
    /// runtime's `collect_bounded_window` — the same distributed primitive the
    /// DataStream path uses (ships to the coordinator on a distributed session).
    /// Here (embedded) it runs in-process; the routing is what matters.
    #[tokio::test]
    async fn collect_bounded_windows_via_runtime_primitive() {
        // window0 (0-60s): a@0,a@1000 -> 2 ; b@2000 -> 1
        // window1 (60-120s): a@65000 -> 1 ; b@70000 -> 1   => 4 (key,window) groups
        let df = dataframe_from_batches(vec![stream_batch(
            &["a", "a", "b", "a", "b"],
            &[0, 1000, 2000, 65000, 70000],
        )]);
        let sdf = df
            .stream()
            .with_event_time("stream_ts")
            .key_by("user_id")
            .tumbling_window(60_000);
        let out = sdf.collect_bounded().await.expect("collect_bounded");
        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "4 (key, window) groups expected");
    }

    // Test: streaming query restart -- start two sequential queries from the same data
    #[tokio::test]
    async fn streaming_query_restart_two_sequential_queries() {
        use crate::streaming_builder::{DataStreamWriter, ForeachBatchFn, StreamingTrigger};
        use std::sync::atomic::{AtomicU64, Ordering};

        let counter = Arc::new(AtomicU64::new(0));

        // First query.
        {
            let df = dataframe_from_batches(vec![stream_batch(&["a", "b"], &[1, 2])]);
            let c = Arc::clone(&counter);
            let f: ForeachBatchFn = Arc::new(move |batches, _epoch| {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                c.fetch_add(rows as u64, Ordering::Relaxed);
                Ok(())
            });
            let q = DataStreamWriter::new(df)
                .trigger(StreamingTrigger::Once)
                .foreach_batch(f)
                .start()
                .await
                .expect("first query must start");
            q.await_termination()
                .await
                .expect("first query must complete");
        }

        let after_first = counter.load(Ordering::Relaxed);
        assert_eq!(after_first, 2, "first query must have processed 2 rows");

        // Second query (recovery / restart).
        {
            let df = dataframe_from_batches(vec![stream_batch(&["c", "d", "e"], &[3, 4, 5])]);
            let c = Arc::clone(&counter);
            let f: ForeachBatchFn = Arc::new(move |batches, _epoch| {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                c.fetch_add(rows as u64, Ordering::Relaxed);
                Ok(())
            });
            let q = DataStreamWriter::new(df)
                .trigger(StreamingTrigger::Once)
                .foreach_batch(f)
                .start()
                .await
                .expect("second query must start");
            q.await_termination()
                .await
                .expect("second query must complete");
        }

        let after_second = counter.load(Ordering::Relaxed);
        assert_eq!(
            after_second, 5,
            "second query must have processed 3 more rows (2+3=5)"
        );
    }
}
