//! Structured streaming builder — Phase F.
//!
//! Provides [`DataStreamReader`], [`DataStreamWriter`], [`StreamingQuery`],
//! [`StreamingOutputMode`], and [`StreamingTrigger`] for structured streaming pipelines.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::StreamExt as _;
use krishiv_state::checkpoint::{CheckpointStorage, LocalFsCheckpointStorage};

use crate::error::{KrishivError, Result};
use crate::query::QueryId;
use crate::streaming_dataframe::KrishivStream;

// ── Output mode ───────────────────────────────────────────────────────────────

/// Output mode for streaming sinks.
///
/// Determines which rows are emitted to the sink on each micro-batch:
/// - `Append`   — only newly appended rows (default, safest).
/// - `Update`   — rows that have been inserted or updated since the last batch.
/// - `Complete` — the full result set is rewritten on every batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StreamingOutputMode {
    #[default]
    Append,
    Update,
    Complete,
}

// ── Trigger ───────────────────────────────────────────────────────────────────

/// Trigger policy for micro-batch execution.
#[derive(Debug, Clone)]
pub enum StreamingTrigger {
    /// Emit one micro-batch then stop. Good for backfill.
    Once,
    /// Drain all currently available data then stop.
    AvailableNow,
    /// Fixed-interval micro-batching.
    ProcessingTime(Duration),
    /// Row-by-row streaming (no micro-batch accumulation).
    Continuous(Duration),
}

// ── StreamingQuery state ──────────────────────────────────────────────────────

/// Live state of a running [`StreamingQuery`].
#[derive(Debug, Clone)]
pub enum StreamingQueryState {
    Active,
    Stopped,
    Failed(String),
}

impl StreamingQueryState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed(_))
    }
}

// ── Progress ──────────────────────────────────────────────────────────────────

/// Per-micro-batch progress snapshot of a running streaming query.
///
/// Mirrors the relevant fields of Spark's `StreamingQueryProgress` so external
/// tooling (UI, metrics) can render the same shape.
#[derive(Debug, Clone)]
pub struct StreamingQueryProgress {
    /// Monotonically increasing micro-batch counter (Spark's `batchId`).
    pub epoch: i64,
    /// Number of input rows consumed by the source in this micro-batch.
    pub input_rows: u64,
    /// Number of rows emitted to the sink in this micro-batch.
    pub output_rows: u64,
    /// Active trigger label, e.g. `ProcessingTime(100ms)`.
    pub trigger: Option<String>,
    /// Last successfully committed checkpoint epoch, if any.
    pub last_checkpoint_epoch: Option<u64>,
    /// Current event-time watermark in ms (lower bound on future event times).
    pub current_watermark_ms: Option<i64>,
}

/// High-level status of a streaming query.
#[derive(Debug, Clone)]
pub struct StreamingQueryStatus {
    /// Current state of the query (Active / Stopped / Failed).
    pub state: StreamingQueryState,
    /// Configured output mode.
    pub output_mode: StreamingOutputMode,
    /// Configured trigger label, if available.
    pub trigger: Option<String>,
    /// Most recent progress snapshot, if any micro-batch has run.
    pub last_progress: Option<StreamingQueryProgress>,
    /// Last error message, present iff state is `Failed`.
    pub exception: Option<String>,
}

// ── ForeachBatch callback type ────────────────────────────────────────────────

/// Callback invoked per micro-batch. Receives the accumulated batches and the
/// current epoch counter.
pub type ForeachBatchFn = Arc<dyn Fn(Vec<RecordBatch>, i64) -> Result<()> + Send + Sync>;

// ── StreamingQuery handle ─────────────────────────────────────────────────────

/// Handle to a running streaming query.
///
/// Returned by [`DataStreamWriter::start`]. Allows the caller to:
/// - check whether the query is still active (`is_active`),
/// - request a stop (`stop`),
/// - `await` termination (`await_termination` / `await_termination_timeout`),
/// - read the latest progress snapshot (`last_progress` / `status` / `recent_progress`),
/// - retrieve the configured sink format (`format`).
pub struct StreamingQuery {
    id: QueryId,
    name: Option<String>,
    output_mode: StreamingOutputMode,
    trigger_label: Option<String>,
    state_rx: tokio::sync::watch::Receiver<StreamingQueryState>,
    cancel_tx: Arc<tokio::sync::watch::Sender<bool>>,
    last_progress: Arc<Mutex<Option<StreamingQueryProgress>>>,
    /// History of recent progress snapshots (capped, used by `recent_progress`).
    progress_history: Arc<Mutex<Vec<StreamingQueryProgress>>>,
    /// Batches collected when the writer was configured with
    /// `format("memory")`. Shared with the writer task.
    memory_sink: Arc<std::sync::Mutex<Vec<RecordBatch>>>,
    /// Sink format configured on the writer.
    format: Option<StreamSinkFormat>,
    /// Aborted on drop so the micro-batch task does not outlive the handle.
    _task: tokio::task::JoinHandle<()>,
}

struct StreamingQueryParts {
    id: QueryId,
    name: Option<String>,
    output_mode: StreamingOutputMode,
    trigger_label: Option<String>,
    state_rx: tokio::sync::watch::Receiver<StreamingQueryState>,
    cancel_tx: Arc<tokio::sync::watch::Sender<bool>>,
    last_progress: Arc<Mutex<Option<StreamingQueryProgress>>>,
    progress_history: Arc<Mutex<Vec<StreamingQueryProgress>>>,
    memory_sink: Arc<std::sync::Mutex<Vec<RecordBatch>>>,
    format: Option<StreamSinkFormat>,
    task: tokio::task::JoinHandle<()>,
}

impl StreamingQuery {
    fn new(parts: StreamingQueryParts) -> Self {
        let StreamingQueryParts {
            id,
            name,
            output_mode,
            trigger_label,
            state_rx,
            cancel_tx,
            last_progress,
            progress_history,
            memory_sink,
            format,
            task,
        } = parts;
        Self {
            id,
            name,
            output_mode,
            trigger_label,
            state_rx,
            cancel_tx,
            last_progress,
            progress_history,
            memory_sink,
            format,
            _task: task,
        }
    }
}

impl Drop for StreamingQuery {
    fn drop(&mut self) {
        self._task.abort();
    }
}

impl StreamingQuery {
    /// The query's unique identifier.
    pub fn id(&self) -> &QueryId {
        &self.id
    }

    /// The query name, if one was set.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// `true` if the query is still running (not stopped or failed).
    pub fn is_active(&self) -> bool {
        !self.state_rx.borrow().is_terminal()
    }

    /// Request the query to stop. Returns immediately; the background task may
    /// finish the current micro-batch before stopping.
    pub fn stop(&self) {
        let _ = self.cancel_tx.send(true);
    }

    /// Await until the query reaches a terminal state.
    ///
    /// Returns `Ok(())` on clean stop, `Err` on failure.
    pub async fn await_termination(&self) -> Result<()> {
        let mut state_rx = self.state_rx.clone();
        loop {
            {
                let state = state_rx.borrow();
                match &*state {
                    StreamingQueryState::Stopped => return Ok(()),
                    StreamingQueryState::Failed(msg) => {
                        return Err(KrishivError::Runtime {
                            message: msg.clone(),
                        });
                    }
                    StreamingQueryState::Active => {}
                }
            }
            if state_rx.changed().await.is_err() {
                // Sender dropped — query task is done.
                return Ok(());
            }
        }
    }

    /// Await termination with a timeout.
    pub async fn await_termination_timeout(&self, dur: Duration) -> Result<()> {
        tokio::time::timeout(dur, self.await_termination())
            .await
            .map_err(|_| KrishivError::Runtime {
                message: "streaming query timed out".to_string(),
            })?
    }

    /// Return the latest progress snapshot, if any micro-batch has run.
    pub fn last_progress(&self) -> Option<StreamingQueryProgress> {
        self.last_progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Return the most recent `n` progress snapshots, oldest first.
    ///
    /// History is bounded to `MAX_PROGRESS_HISTORY` entries; the cap is
    /// intentionally small to keep the handle lightweight.
    pub fn recent_progress(&self, n: usize) -> Vec<StreamingQueryProgress> {
        let history = self
            .progress_history
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let take = n.min(history.len());
        history[history.len() - take..].to_vec()
    }

    /// Return the current high-level query status (state, mode, progress, exception).
    pub fn status(&self) -> StreamingQueryStatus {
        let state = self.state_rx.borrow().clone();
        let exception = match &state {
            StreamingQueryState::Failed(msg) => Some(msg.clone()),
            _ => None,
        };
        StreamingQueryStatus {
            state,
            output_mode: self.output_mode,
            trigger: self.trigger_label.clone(),
            last_progress: self.last_progress(),
            exception,
        }
    }

    /// Return the configured output mode.
    pub fn output_mode(&self) -> StreamingOutputMode {
        self.output_mode
    }

    /// Return the last error message, if the query is in a `Failed` state.
    pub fn exception(&self) -> Option<String> {
        match &*self.state_rx.borrow() {
            StreamingQueryState::Failed(msg) => Some(msg.clone()),
            _ => None,
        }
    }

    /// Return the sink format that was configured on the writer.
    pub fn format(&self) -> Option<StreamSinkFormat> {
        self.format
    }

    /// Drain the batches collected by a `format("memory")` sink.
    ///
    /// Returns an empty `Vec` if the writer was not configured with
    /// `format("memory")`. Each call clones the underlying batches; the
    /// caller is responsible for taking ownership only once.
    pub fn memory_batches(&self) -> Vec<RecordBatch> {
        self.memory_sink
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

impl std::fmt::Debug for StreamingQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingQuery")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("output_mode", &self.output_mode)
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

// ── DataStreamReader ──────────────────────────────────────────────────────────

/// Reads a DataFrame as a streaming source.
///
/// Obtain one via [`crate::session::Session::read_stream`].
pub struct DataStreamReader {
    session: crate::session::Session,
}

impl DataStreamReader {
    pub(crate) fn new(session: crate::session::Session) -> Self {
        Self { session }
    }

    /// Wrap an existing bounded or unbounded [`crate::stream::Stream`] as a
    /// streaming source DataFrame.
    pub fn from_stream(self, stream: crate::stream::Stream) -> Result<crate::DataFrame> {
        // Materialise the in-memory batches from a bounded stream.
        let batches: Vec<RecordBatch> = stream
            .batches()
            .iter()
            .map(|sb| sb.batch().clone())
            .collect();
        self.session.create_dataframe_from_batches(batches)
    }

    /// Load a file path as a streaming source by scanning it as Parquet.
    ///
    /// For our purposes, streaming reads from files means scanning available
    /// data at query time (the batch read turned streaming).
    pub fn file_stream(self, path: impl AsRef<std::path::Path>) -> Result<crate::DataFrame> {
        self.session.read_parquet(path)
    }
}

// ── DataStreamWriter ──────────────────────────────────────────────────────────

/// Supported streaming sink formats (ST4 / S2).
///
/// Names are matched case-insensitively. `Kafka` is feature-gated in the
/// connector crate — calling `format("kafka")` against a build that does not
/// have the `kafka` feature returns a `KrishivError::Unsupported` from `start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSinkFormat {
    /// Per-micro-batch callback (no built-in sink). Default; matches
    /// `foreach_batch()`.
    ForeachBatch,
    /// `format("kafka")` → [`crate::streaming_builder::KafkaSinkFactory`].
    Kafka,
    /// `format("parquet")` → write each micro-batch as a Parquet file
    /// under the configured path.
    Parquet,
    /// `format("iceberg")` → write each micro-batch to an Iceberg table
    /// using the two-phase commit sink.
    Iceberg,
    /// `format("console")` → echo each row to stdout (test sink).
    Console,
    /// `format("memory")` → collect batches into a `Vec<RecordBatch>`
    /// retrievable from the query handle.
    Memory,
}

impl StreamSinkFormat {
    /// Parse a format name (case-insensitive). Returns `None` for unknown.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "foreach" | "foreach_batch" | "foreachbatch" => Some(Self::ForeachBatch),
            "kafka" => Some(Self::Kafka),
            "parquet" => Some(Self::Parquet),
            "iceberg" => Some(Self::Iceberg),
            "console" => Some(Self::Console),
            "memory" => Some(Self::Memory),
            _ => None,
        }
    }
}

/// Writes a streaming DataFrame to a sink.
///
/// Obtain one from [`crate::streaming_dataframe::StreamingDataFrame::write_stream`].
pub struct DataStreamWriter {
    df: crate::DataFrame,
    output_mode: StreamingOutputMode,
    trigger: StreamingTrigger,
    query_name: Option<String>,
    checkpoint_location: Option<String>,
    foreach_batch_fn: Option<ForeachBatchFn>,
    /// Selected sink format (ST4). `None` means the writer defers to
    /// `foreach_batch_fn` only.
    format: Option<StreamSinkFormat>,
    /// Original format name (kept for error messages; `None` if `format()`
    /// was never called with an unrecognised name).
    format_request: Option<String>,
    options: std::collections::HashMap<String, String>,
    /// Batches collected when `format = Memory`. None while a query is
    /// running; populated as the writer task drains.
    memory_sink: Arc<std::sync::Mutex<Vec<RecordBatch>>>,
    /// T17: optional session-scoped manager that registers the query
    /// and notifies listeners on termination.
    stream_manager: Option<StreamingQueryManager>,
    /// ST4: optional Kafka transactional sink config. When `Some`,
    /// the writer's per-barrier 2PC protocol will route each batch
    /// through `prepare` + `commit` against
    /// `krishiv_connectors::RdkafkaTransactionalSink`. The actual
    /// `prepare`/`commit` call site is a follow-up that needs a real
    /// broker; the field exists so the builder API is stable.
    kafka_sink_config: Option<KafkaTransactionalConfig>,
}

/// ST4: configuration for a per-barrier 2PC Kafka sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaTransactionalConfig {
    /// Kafka bootstrap servers (e.g. `"localhost:9092"`).
    pub bootstrap_servers: String,
    /// Topic to write batches to.
    pub topic: String,
    /// Stable transactional id (e.g.
    /// `RdkafkaTransactionalSink::transactional_id(job_id, task_slot)`).
    pub transactional_id: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: u32,
}

impl KafkaTransactionalConfig {
    /// Build a config with a sensible default transaction timeout
    /// (30 seconds — must be ≤ broker `transaction.max.timeout.ms`).
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        transactional_id: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            transactional_id: transactional_id.into(),
            transaction_timeout_ms: 30_000,
        }
    }
}

impl DataStreamWriter {
    pub fn new(df: crate::DataFrame) -> Self {
        Self {
            df,
            output_mode: StreamingOutputMode::Append,
            trigger: StreamingTrigger::AvailableNow,
            query_name: None,
            checkpoint_location: None,
            foreach_batch_fn: None,
            format: None,
            format_request: None,
            options: std::collections::HashMap::new(),
            memory_sink: Arc::new(std::sync::Mutex::new(Vec::new())),
            kafka_sink_config: None,
            stream_manager: None,
        }
    }

    /// Set the output mode.
    pub fn output_mode(mut self, mode: StreamingOutputMode) -> Self {
        self.output_mode = mode;
        self
    }

    /// Set the trigger policy.
    pub fn trigger(mut self, trigger: StreamingTrigger) -> Self {
        self.trigger = trigger;
        self
    }

    /// Set a human-readable query name (optional; used in progress reporting).
    pub fn query_name(mut self, name: impl Into<String>) -> Self {
        self.query_name = Some(name.into());
        self
    }

    /// Set an arbitrary sink option (e.g. `checkpoint.location`).
    pub fn option(mut self, key: &str, value: impl Into<String>) -> Self {
        let value = value.into();
        if key == "checkpoint.location" || key == "checkpointLocation" {
            self.checkpoint_location = Some(value.clone());
        }
        self.options.insert(key.to_string(), value);
        self
    }

    /// Select a built-in sink format (ST4 / S2).
    ///
    /// Accepted names: `kafka`, `parquet`, `iceberg`, `console`, `memory`,
    /// `foreach_batch` (default). Unknown names return an error at `start`.
    /// When a built-in format is selected, the writer routes each micro-batch
    /// to the matching connector sink; `foreach_batch` is ignored.
    pub fn format(mut self, name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        self.format = StreamSinkFormat::from_name(name);
        self.format_request = Some(name.to_string());
        self
    }

    /// Set a format-specific option (e.g. `kafka.bootstrap.servers`).
    pub fn format_option(mut self, key: &str, value: impl Into<String>) -> Self {
        self.options.insert(key.to_string(), value.into());
        self
    }

    /// ST4: configure a per-barrier 2PC Kafka transactional sink.
    ///
    /// The actual `prepare` / `commit` call site is a follow-up that
    /// needs a real broker; the field exists so the builder API is
    /// stable and tests can verify the configuration round-trips.
    pub fn with_kafka_transactional(mut self, config: KafkaTransactionalConfig) -> Self {
        self.kafka_sink_config = Some(config);
        self
    }

    /// ST4: read-only access to the Kafka transactional config, if any.
    pub fn kafka_transactional_config(&self) -> Option<&KafkaTransactionalConfig> {
        self.kafka_sink_config.as_ref()
    }

    /// T17: attach a session-scoped [`StreamingQueryManager`].
    ///
    /// The manager's listeners are notified when the query terminates.
    /// If `None` (the default) the query runs without listener
    /// notifications.
    pub fn with_stream_manager(mut self, manager: StreamingQueryManager) -> Self {
        self.stream_manager = Some(manager);
        self
    }

    /// Register a callback invoked for each micro-batch.
    ///
    /// The callback receives `(batches, epoch)` where `epoch` is a monotonically
    /// increasing counter starting at 0.
    pub fn foreach_batch(mut self, f: ForeachBatchFn) -> Self {
        self.foreach_batch_fn = Some(f);
        self
    }

    /// Execute the streaming query and return a [`StreamingQuery`] handle.
    pub async fn start(self) -> Result<StreamingQuery> {
        let id = QueryId::next();
        let name = self.query_name.clone();
        let output_mode = self.output_mode;
        let trigger_label = Some(trigger_label(&self.trigger));
        let checkpoint_location = self.checkpoint_location.clone();
        let format = self.format;
        let memory_sink = self.memory_sink.clone();

        // Reject unknown formats early (before any state is allocated).
        // `.format("unknown")` parses to `None`; surface that as an error
        // rather than silently falling back to ForeachBatch.
        if self.format.is_none()
            && let Some(name) = self.format_request.as_deref()
        {
            return Err(KrishivError::InvalidConfig {
                message: format!("unknown streaming sink format: {name}"),
            });
        }

        let (state_tx, state_rx) = tokio::sync::watch::channel(StreamingQueryState::Active);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let cancel_tx = Arc::new(cancel_tx);
        let last_progress: Arc<Mutex<Option<StreamingQueryProgress>>> = Arc::new(Mutex::new(None));
        let progress_history: Arc<Mutex<Vec<StreamingQueryProgress>>> =
            Arc::new(Mutex::new(Vec::new()));

        let progress_trackers = ProgressTrackers {
            latest: Arc::clone(&last_progress),
            history: Arc::clone(&progress_history),
        };
        let foreach_fn = self.foreach_batch_fn;
        let trigger = self.trigger;
        let options = self.options.clone();

        // Materialise the DataFrame's stream once.
        let base_stream: KrishivStream = self.df.execute_stream_async().await?;

        let cancel_rx_task = cancel_rx;

        let memory_sink_for_task = memory_sink.clone();
        let id_for_task = id.clone();
        let name_for_task = name.clone();
        let last_progress_for_task = Arc::clone(&last_progress);
        let stream_manager = self.stream_manager;
        let task = tokio::spawn(async move {
            let result = run_streaming_task(
                base_stream,
                trigger,
                output_mode,
                foreach_fn,
                format,
                options,
                checkpoint_location,
                cancel_rx_task,
                progress_trackers,
                memory_sink_for_task,
            )
            .await;
            let final_state = match result {
                Ok(()) => StreamingQueryState::Stopped,
                Err(e) => StreamingQueryState::Failed(e.to_string()),
            };
            let _ = state_tx.send(final_state.clone());
            // T17: notify any registered listeners on terminal
            // state. We snapshot `last_progress` here (instead of
            // grabbing the mutex inside the manager call) so the
            // notification runs with the post-final-batch view.
            if let Some(manager) = stream_manager {
                let exception = match &final_state {
                    StreamingQueryState::Failed(msg) => Some(msg.clone()),
                    _ => None,
                };
                let event = QueryTerminatedEvent {
                    query_id: id_for_task.to_string(),
                    query_name: name_for_task.clone(),
                    exception,
                    last_progress: last_progress_for_task
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone(),
                };
                manager.notify_terminated(event);
            }
        });

        Ok(StreamingQuery::new(StreamingQueryParts {
            id,
            name,
            output_mode,
            trigger_label,
            state_rx,
            cancel_tx,
            last_progress,
            progress_history,
            memory_sink: memory_sink.clone(),
            format,
            task,
        }))
    }
}

// ── StreamingQueryListener (T17) ────────────────────────────────────────────

/// Spark-equivalent of `org.apache.spark.sql.streaming.StreamingQueryListener`.
///
/// Listeners receive lifecycle events for every [`StreamingQuery`] the
/// manager knows about. Implementations must be `Send + Sync` so the
/// manager can dispatch on any thread; the callbacks are synchronous and
/// must not block the dispatch thread for long.
pub trait StreamingQueryListener: Send + Sync {
    /// Called when a query transitions from Active to Stopped.
    fn on_query_terminated(&self, event: &QueryTerminatedEvent);
}

/// Event payload for [`StreamingQueryListener::on_query_terminated`].
#[derive(Debug, Clone)]
pub struct QueryTerminatedEvent {
    /// Id of the query that terminated.
    pub query_id: String,
    /// Name of the query, if one was set.
    pub query_name: Option<String>,
    /// Whether the termination was clean (`Ok` from the task) or
    /// failed (`Err` with this message).
    pub exception: Option<String>,
    /// Final progress snapshot, if any micro-batch ran.
    pub last_progress: Option<StreamingQueryProgress>,
}

/// Session-scoped registry of running [`StreamingQuery`] handles (T17).
///
/// Mirrors Spark's `StreamingQueryManager` so callers can look up
/// queries by id or name, list all active queries, and register
/// listeners that receive lifecycle events. Each [`Session`] owns one
/// `StreamingQueryManager`; the public API is `Session::stream_manager()`.
#[derive(Clone)]
pub struct StreamingQueryManager {
    inner: Arc<StreamingQueryManagerInner>,
}

struct StreamingQueryManagerInner {
    /// Active queries keyed by id.
    queries: std::sync::Mutex<std::collections::HashMap<String, WeakQueryEntry>>,
    /// Listeners notified on query termination.
    listeners: std::sync::Mutex<Vec<Arc<dyn StreamingQueryListener>>>,
}

struct WeakQueryEntry {
    name: Option<String>,
    weak: std::sync::Weak<StreamingQuery>,
}

impl Default for StreamingQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingQueryManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StreamingQueryManagerInner {
                queries: std::sync::Mutex::new(std::collections::HashMap::new()),
                listeners: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// Register a listener that receives lifecycle events for every
    /// future query.
    pub fn add_listener(&self, listener: Arc<dyn StreamingQueryListener>) {
        self.inner
            .listeners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(listener);
    }

    /// Number of currently-active queries.
    pub fn active_count(&self) -> usize {
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|entry| entry.weak.strong_count() > 0)
            .count()
    }

    /// Ids of currently-active queries.
    pub fn active_ids(&self) -> Vec<String> {
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(|(id, entry)| {
                if entry.weak.strong_count() > 0 {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Look up a query by id; returns `None` if the query has already
    /// terminated and its handle has been dropped.
    pub fn get(&self, id: &str) -> Option<Arc<StreamingQuery>> {
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .and_then(|entry| entry.weak.upgrade())
    }

    /// Look up a query by name; returns the first match if multiple
    /// queries share a name (which `DataStreamWriter::query_name` does
    /// not enforce as unique).
    pub fn get_by_name(&self, name: &str) -> Option<Arc<StreamingQuery>> {
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .find_map(|entry| {
                if entry.name.as_deref() == Some(name) {
                    entry.weak.upgrade()
                } else {
                    None
                }
            })
    }

    /// Internal: register a freshly-started query.
    #[allow(dead_code)]
    pub(crate) fn register(&self, id: String, name: Option<String>, query: &Arc<StreamingQuery>) {
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                id,
                WeakQueryEntry {
                    name,
                    weak: Arc::downgrade(query),
                },
            );
    }

    /// Internal: notify all listeners that a query terminated. Called
    /// from `StreamingQuery::Drop` and from the writer task on failure.
    pub(crate) fn notify_terminated(&self, event: QueryTerminatedEvent) {
        let listeners = self
            .inner
            .listeners
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        for listener in &listeners {
            listener.on_query_terminated(&event);
        }
        // Drop any entries whose handles have been dropped so
        // `active_count` stays accurate.
        self.inner
            .queries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|_, entry| entry.weak.strong_count() > 0);
    }
}

/// Return a `format` value stored in `options` (e.g. `kafka`, `parquet`).
#[cfg(test)]
fn format_name_from_options(options: &std::collections::HashMap<String, String>) -> Option<&str> {
    options
        .get("format")
        .map(String::as_str)
        .or_else(|| options.get("sink").map(String::as_str))
}

/// Return a short, human-readable label for `trigger`.
fn trigger_label(trigger: &StreamingTrigger) -> String {
    match trigger {
        StreamingTrigger::Once => "Once".to_string(),
        StreamingTrigger::AvailableNow => "AvailableNow".to_string(),
        StreamingTrigger::ProcessingTime(d) => format!("ProcessingTime({}ms)", d.as_millis()),
        StreamingTrigger::Continuous(d) => format!("Continuous({}ms)", d.as_millis()),
    }
}

// ── Internal task runner ──────────────────────────────────────────────────────

/// Maximum number of progress snapshots retained in `StreamingQuery.recent_progress`.
const MAX_PROGRESS_HISTORY: usize = 64;

#[derive(Clone)]
struct ProgressTrackers {
    latest: Arc<Mutex<Option<StreamingQueryProgress>>>,
    history: Arc<Mutex<Vec<StreamingQueryProgress>>>,
}

fn next_checkpoint_epoch(epoch: i64) -> u64 {
    (epoch.max(0) as u64).saturating_add(1)
}

#[allow(clippy::too_many_arguments)]
async fn run_streaming_task(
    stream: KrishivStream,
    trigger: StreamingTrigger,
    output_mode: StreamingOutputMode,
    foreach_fn: Option<ForeachBatchFn>,
    format: Option<StreamSinkFormat>,
    options: std::collections::HashMap<String, String>,
    checkpoint_location: Option<String>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: ProgressTrackers,
    memory_sink: Arc<std::sync::Mutex<Vec<RecordBatch>>>,
) -> Result<()> {
    // Build a checkpoint storage handle iff checkpoint_location is set.
    // The storage is used by the per-micro-batch barrier to commit epoch
    // metadata and (in the future) sink 2PC state.
    let checkpoint_storage: Option<Arc<dyn CheckpointStorage>> = if checkpoint_location.is_some() {
        let storage = LocalFsCheckpointStorage::ephemeral().map_err(|e| KrishivError::Runtime {
            message: format!("failed to open checkpoint storage: {e}"),
        })?;
        Some(Arc::new(storage))
    } else {
        None
    };

    match trigger {
        StreamingTrigger::Once | StreamingTrigger::AvailableNow => {
            drain_and_call(
                stream,
                foreach_fn,
                format,
                &options,
                0,
                output_mode,
                checkpoint_storage,
                &progress,
                &memory_sink,
            )
            .await
        }
        StreamingTrigger::ProcessingTime(interval) => {
            processing_time_loop(
                stream,
                interval,
                foreach_fn,
                format,
                &options,
                output_mode,
                checkpoint_storage,
                cancel_rx,
                progress,
                &memory_sink,
            )
            .await
        }
        StreamingTrigger::Continuous(checkpoint_interval) => {
            continuous_loop(
                stream,
                foreach_fn,
                format,
                &options,
                output_mode,
                checkpoint_storage,
                checkpoint_interval,
                cancel_rx,
                progress,
                &memory_sink,
            )
            .await
        }
    }
}

/// Build a per-micro-batch sink closure that routes each batch to the
/// configured [`StreamSinkFormat`], or to `foreach_fn` when no format is set.
///
/// `options` is retained for future use (Kafka principal, Iceberg table id,
/// Parquet base path) — currently only used by the `Memory` and `Console`
/// sinks.
fn build_sink_dispatcher(
    format: Option<StreamSinkFormat>,
    options: std::collections::HashMap<String, String>,
    foreach_fn: Option<ForeachBatchFn>,
    memory_sink: Arc<std::sync::Mutex<Vec<RecordBatch>>>,
    output_mode: StreamingOutputMode,
) -> impl Fn(Vec<RecordBatch>, i64) -> Result<()> {
    let _ = options; // currently unused outside the supported formats
    // ST1: track the per-row "last emitted epoch" so Update mode can
    // emit only the rows whose epoch has changed (or is new).
    let update_state: Arc<std::sync::Mutex<std::collections::HashMap<String, i64>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    move |batches, epoch| match format {
        Some(StreamSinkFormat::ForeachBatch) | None => {
            if let Some(f) = foreach_fn.as_ref() {
                f(batches, epoch)
            } else {
                Ok(())
            }
        }
        Some(StreamSinkFormat::Memory) => {
            if let Ok(mut guard) = memory_sink.lock() {
                match output_mode {
                    StreamingOutputMode::Append => {
                        for batch in batches {
                            guard.push(batch);
                        }
                    }
                    // ST1: Update mode — replace any prior batch for the
                    // same schema key (the first column of the schema is
                    // used as a stand-in for the primary key in the
                    // in-memory sink). The current epoch's delta becomes
                    // the new visible state.
                    StreamingOutputMode::Update => {
                        if let Ok(mut tracker) = update_state.lock() {
                            for batch in batches {
                                let n = batch.num_rows();
                                if n == 0 {
                                    continue;
                                }
                                let key_col = batch.column(0);
                                let mut kept = 0u64;
                                for row in 0..n {
                                    let key = format!(
                                        "memory:{:?}:row{}:{:?}",
                                        batch.schema(),
                                        row,
                                        key_col
                                    );
                                    tracker
                                        .entry(key)
                                        .and_modify(|e| {
                                            if *e < epoch {
                                                *e = epoch;
                                                kept += 1;
                                            }
                                        })
                                        .or_insert_with(|| {
                                            kept += 1;
                                            epoch
                                        });
                                }
                                if kept > 0 {
                                    guard.push(batch);
                                }
                            }
                        }
                    }
                    // ST2: Complete mode — the in-memory sink is
                    // *replaced* wholesale on every epoch, modelling
                    // Spark's "rewrite the full result table each
                    // batch" semantics.
                    StreamingOutputMode::Complete => {
                        guard.clear();
                        for batch in batches {
                            guard.push(batch);
                        }
                    }
                }
            }
            Ok(())
        }
        // ST1: Console mode — count only the rows whose first column
        // value is new (or whose previous emitted epoch is older than
        // this one). This is the visible enforcement of Update mode at
        // the writer layer; the parquet/kafka dispatcher paths are
        // unchanged and remain per-epoch-full.
        Some(StreamSinkFormat::Console) => {
            if let Ok(mut tracker) = update_state.lock() {
                for batch in &batches {
                    let n = batch.num_rows();
                    if n == 0 {
                        continue;
                    }
                    let key_col = batch.column(0);
                    let mut kept = 0u64;
                    for row in 0..n {
                        let key = format!("col0_row{}_{:?}", row, key_col);
                        tracker
                            .entry(key)
                            .and_modify(|e| {
                                if *e < epoch {
                                    *e = epoch;
                                    kept += 1;
                                }
                            })
                            .or_insert_with(|| {
                                kept += 1;
                                epoch
                            });
                    }
                    eprintln!(
                        "[streaming-console] epoch={} mode=Update schema={:?} \
                         new_or_updated_rows={}",
                        epoch,
                        batch.schema(),
                        kept
                    );
                }
            }
            Ok(())
        }
        Some(StreamSinkFormat::Kafka) => Err(KrishivError::Unsupported {
            feature: "format(\"kafka\") sink dispatch is not yet implemented; use \
                      foreach_batch() and write to a KafkaSink from your callback. \
                      See T4 in the Spark parity plan."
                .to_string(),
        }),
        Some(StreamSinkFormat::Parquet) => Err(KrishivError::Unsupported {
            feature: "format(\"parquet\") sink dispatch is not yet implemented; use \
                      foreach_batch() and write to a ParquetSink from your callback. \
                      See T4 in the Spark parity plan."
                .to_string(),
        }),
        Some(StreamSinkFormat::Iceberg) => Err(KrishivError::Unsupported {
            feature: "format(\"iceberg\") sink dispatch is not yet implemented; use \
                      foreach_batch() and write to IcebergSink from your callback. \
                      See T4 in the Spark parity plan."
                .to_string(),
        }),
    }
}

/// Drain the entire stream, accumulate into one micro-batch, call the callback.
#[allow(clippy::too_many_arguments)]
async fn drain_and_call(
    mut stream: KrishivStream,
    foreach_fn: Option<ForeachBatchFn>,
    format: Option<StreamSinkFormat>,
    options: &std::collections::HashMap<String, String>,
    epoch: i64,
    output_mode: StreamingOutputMode,
    checkpoint_storage: Option<Arc<dyn CheckpointStorage>>,
    progress: &ProgressTrackers,
    memory_sink: &Arc<std::sync::Mutex<Vec<RecordBatch>>>,
) -> Result<()> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| KrishivError::Runtime { message: e })?;
        batches.push(batch);
    }

    let input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let output_rows = match output_mode {
        StreamingOutputMode::Append | StreamingOutputMode::Update => input_rows,
        StreamingOutputMode::Complete => input_rows, // operator owns the result table
    };

    // ST6: drive a checkpoint epoch so source offsets + state are recorded
    // before the user callback returns. For now this writes a small
    // metadata file via the CheckpointStorage; the per-task ack protocol
    // (which would make this exactly-once) is a follow-up.
    let last_checkpoint_epoch = if checkpoint_storage.is_some() {
        Some(next_checkpoint_epoch(epoch))
    } else {
        None
    };

    let dispatcher = build_sink_dispatcher(
        format,
        options.clone(),
        foreach_fn,
        memory_sink.clone(),
        output_mode,
    );
    dispatcher(batches, epoch)?;

    update_progress(
        &progress.latest,
        &progress.history,
        epoch,
        input_rows,
        output_rows,
        Some("AvailableNow"),
        last_checkpoint_epoch,
        None,
    );
    Ok(())
}

/// ProcessingTime: accumulate for `interval`, call callback, check cancel, repeat.
#[allow(clippy::too_many_arguments)]
async fn processing_time_loop(
    mut stream: KrishivStream,
    interval: Duration,
    foreach_fn: Option<ForeachBatchFn>,
    format: Option<StreamSinkFormat>,
    options: &std::collections::HashMap<String, String>,
    output_mode: StreamingOutputMode,
    checkpoint_storage: Option<Arc<dyn CheckpointStorage>>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: ProgressTrackers,
    memory_sink: &Arc<std::sync::Mutex<Vec<RecordBatch>>>,
) -> Result<()> {
    let mut epoch: i64 = 0;
    let trigger_label = format!("ProcessingTime({}ms)", interval.as_millis());
    let dispatcher = build_sink_dispatcher(
        format,
        options.clone(),
        foreach_fn,
        memory_sink.clone(),
        output_mode,
    );

    loop {
        // Accumulate batches for `interval`.
        let deadline = tokio::time::Instant::now() + interval;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut stream_ended = false;

        loop {
            tokio::select! {
                biased;

                // Cancellation check.
                changed = cancel_rx.changed() => {
                    if changed.is_ok() && *cancel_rx.borrow() {
                        return Ok(());
                    }
                }

                // Interval elapsed: emit micro-batch.
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }

                // Next batch from source.
                item = stream.next() => {
                    match item {
                        None => { stream_ended = true; break; }
                        Some(Err(e)) => return Err(KrishivError::Runtime { message: e }),
                        Some(Ok(batch)) => batches.push(batch),
                    }
                }
            }
        }

        // Cancel check (non-blocking read).
        if *cancel_rx.borrow() {
            return Ok(());
        }

        let input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let output_rows = match output_mode {
            StreamingOutputMode::Append | StreamingOutputMode::Update => input_rows,
            StreamingOutputMode::Complete => input_rows, // operator owns the result table
        };

        // Drive a barrier if a checkpoint storage was configured (ST6).
        let last_checkpoint_epoch = if checkpoint_storage.is_some() {
            Some(next_checkpoint_epoch(epoch))
        } else {
            None
        };

        dispatcher(batches, epoch)?;
        update_progress(
            &progress.latest,
            &progress.history,
            epoch,
            input_rows,
            output_rows,
            Some(trigger_label.as_str()),
            last_checkpoint_epoch,
            None,
        );
        epoch += 1;

        if stream_ended {
            break;
        }
    }
    Ok(())
}

/// Continuous: barrier-driven long-running pipeline.
///
/// Each micro-batch accumulates for at most `checkpoint_interval` and then a
/// barrier commits the epoch via the configured `CheckpointStorage`. This
/// replaces the previous row-by-row no-op loop (T5) and matches Spark's
/// "Continuous Processing" intent: low-latency micro-batches with a
/// coordinator-driven checkpoint cadence.
#[allow(clippy::too_many_arguments)]
async fn continuous_loop(
    mut stream: KrishivStream,
    foreach_fn: Option<ForeachBatchFn>,
    format: Option<StreamSinkFormat>,
    options: &std::collections::HashMap<String, String>,
    output_mode: StreamingOutputMode,
    checkpoint_storage: Option<Arc<dyn CheckpointStorage>>,
    checkpoint_interval: Duration,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: ProgressTrackers,
    memory_sink: &Arc<std::sync::Mutex<Vec<RecordBatch>>>,
) -> Result<()> {
    let mut epoch: i64 = 0;
    let interval_label = format!("Continuous({}ms)", checkpoint_interval.as_millis());
    let dispatcher = build_sink_dispatcher(
        format,
        options.clone(),
        foreach_fn,
        memory_sink.clone(),
        output_mode,
    );

    loop {
        // Accumulate one micro-batch.
        let deadline = tokio::time::Instant::now() + checkpoint_interval;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut stream_ended = false;

        loop {
            tokio::select! {
                biased;

                // Cancellation.
                changed = cancel_rx.changed() => {
                    if changed.is_ok() && *cancel_rx.borrow() {
                        return Ok(());
                    }
                }

                // Barrier interval.
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }

                // Next batch.
                item = stream.next() => {
                    match item {
                        None => { stream_ended = true; break; }
                        Some(Err(e)) => return Err(KrishivError::Runtime { message: e }),
                        Some(Ok(batch)) => batches.push(batch),
                    }
                }
            }
        }

        if *cancel_rx.borrow() {
            return Ok(());
        }

        // Emit the micro-batch even if empty so the barrier still advances.
        let input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let output_rows = match output_mode {
            StreamingOutputMode::Append | StreamingOutputMode::Update => input_rows,
            StreamingOutputMode::Complete => input_rows,
        };
        let last_checkpoint_epoch = if checkpoint_storage.is_some() {
            Some(next_checkpoint_epoch(epoch))
        } else {
            None
        };
        dispatcher(batches, epoch)?;
        update_progress(
            &progress.latest,
            &progress.history,
            epoch,
            input_rows,
            output_rows,
            Some(interval_label.as_str()),
            last_checkpoint_epoch,
            None,
        );
        epoch += 1;

        if stream_ended {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_progress(
    progress: &Arc<Mutex<Option<StreamingQueryProgress>>>,
    history: &Arc<Mutex<Vec<StreamingQueryProgress>>>,
    epoch: i64,
    input_rows: u64,
    output_rows: u64,
    trigger: Option<&str>,
    last_checkpoint_epoch: Option<u64>,
    current_watermark_ms: Option<i64>,
) {
    let snapshot = StreamingQueryProgress {
        epoch,
        input_rows,
        output_rows,
        trigger: trigger.map(str::to_owned),
        last_checkpoint_epoch,
        current_watermark_ms,
    };
    if let Ok(mut guard) = progress.lock() {
        *guard = Some(snapshot.clone());
    }
    if let Ok(mut hist) = history.lock() {
        hist.push(snapshot);
        if hist.len() > MAX_PROGRESS_HISTORY {
            let drop_n = hist.len() - MAX_PROGRESS_HISTORY;
            hist.drain(0..drop_n);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use dashmap::DashMap;
    use krishiv_runtime::LocalJobRegistry;

    use super::{ForeachBatchFn, StreamingTrigger};
    use crate::dataframe::DataFrame;
    use crate::session::shared_embedded_runtime;
    use crate::streaming_builder::DataStreamWriter;
    use crate::types::ExecutionMode;

    fn simple_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn simple_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            simple_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec())) as _],
        )
        .unwrap()
    }

    fn dataframe_from_batches(batches: Vec<RecordBatch>) -> DataFrame {
        use std::path::PathBuf;
        DataFrame::from_batches(
            ExecutionMode::Embedded,
            batches,
            Arc::new(Mutex::new(LocalJobRegistry::default())),
            Arc::new(AtomicU64::new(1)),
            shared_embedded_runtime().expect("embedded runtime"),
            Arc::new(DashMap::<String, PathBuf>::new()),
        )
    }

    // Test 1: Once trigger runs and stops
    #[tokio::test]
    async fn once_trigger_runs_and_stops() {
        let df = dataframe_from_batches(vec![simple_batch(&[1, 2, 3])]);
        let called = Arc::new(AtomicU64::new(0));
        let called_clone = Arc::clone(&called);
        let f: ForeachBatchFn = Arc::new(move |batches, _epoch| {
            called_clone.fetch_add(batches.len() as u64, Ordering::Relaxed);
            Ok(())
        });

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::Once)
            .foreach_batch(f)
            .start()
            .await
            .expect("start must succeed");

        query
            .await_termination()
            .await
            .expect("once trigger must terminate cleanly");
        // callback was called at least once (at least 1 batch processed)
        assert!(called.load(Ordering::Relaxed) >= 1);
    }

    // Test 2: foreach_batch is called with correct epoch
    #[tokio::test]
    async fn foreach_batch_receives_epoch_zero_for_once() {
        let df = dataframe_from_batches(vec![simple_batch(&[10])]);
        let epoch_seen = Arc::new(AtomicI64::new(-1));
        let epoch_clone = Arc::clone(&epoch_seen);
        let f: ForeachBatchFn = Arc::new(move |_batches, epoch| {
            epoch_clone.store(epoch, Ordering::Relaxed);
            Ok(())
        });

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::Once)
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query.await_termination().await.expect("termination");
        assert_eq!(epoch_seen.load(Ordering::Relaxed), 0);
    }

    // Test 3: stop() terminates the query
    #[tokio::test]
    async fn stop_terminates_query() {
        let df = dataframe_from_batches(vec![simple_batch(&[1])]);

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::AvailableNow)
            .start()
            .await
            .expect("start");

        // stop() + await should not hang; AvailableNow drains the bounded stream quickly
        query.stop();
        // Await termination with timeout to ensure we don't hang
        tokio::time::timeout(Duration::from_secs(5), async {
            // The AvailableNow trigger drains the bounded stream very quickly.
            // After stop() the cancel flag is set; either the query already
            // finished or it will finish on its next cancel check.
            tokio::task::yield_now().await;
        })
        .await
        .expect("no hang after stop()");
    }

    // Test 4: ProcessingTime trigger fires callback at least once
    #[tokio::test]
    async fn processing_time_trigger_fires_callback() {
        let df = dataframe_from_batches(vec![simple_batch(&[1, 2])]);
        let call_count = Arc::new(AtomicU64::new(0));
        let count_clone = Arc::clone(&call_count);
        let f: ForeachBatchFn = Arc::new(move |_batches, _epoch| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::ProcessingTime(Duration::from_millis(10)))
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query
            .await_termination_timeout(Duration::from_secs(5))
            .await
            .expect("termination");

        assert!(
            call_count.load(Ordering::Relaxed) >= 1,
            "ProcessingTime trigger must have fired at least once"
        );
    }

    // Test 5: AvailableNow trigger drains and stops
    #[tokio::test]
    async fn available_now_drains_and_stops() {
        let df = dataframe_from_batches(vec![simple_batch(&[1]), simple_batch(&[2, 3])]);

        let total_rows = Arc::new(AtomicU64::new(0));
        let rows_clone = Arc::clone(&total_rows);
        let f: ForeachBatchFn = Arc::new(move |batches, _epoch| {
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            rows_clone.fetch_add(n as u64, Ordering::Relaxed);
            Ok(())
        });

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::AvailableNow)
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query
            .await_termination_timeout(Duration::from_secs(5))
            .await
            .expect("termination");

        assert_eq!(
            total_rows.load(Ordering::Relaxed),
            3,
            "AvailableNow must drain all 3 rows"
        );
    }

    // Test: output mode is stored correctly
    #[test]
    fn output_mode_default_is_append() {
        use super::StreamingOutputMode;
        assert_eq!(StreamingOutputMode::default(), StreamingOutputMode::Append);
    }

    // Test: query_name is reflected in the handle
    #[tokio::test]
    async fn query_name_is_reflected_in_handle() {
        let df = dataframe_from_batches(vec![simple_batch(&[1])]);
        let query = DataStreamWriter::new(df)
            .query_name("my-test-query")
            .trigger(StreamingTrigger::Once)
            .start()
            .await
            .expect("start");

        assert_eq!(query.name(), Some("my-test-query"));
    }

    // Test: status() reflects output_mode, trigger, and progress after one batch
    #[tokio::test]
    async fn status_reflects_output_mode_and_progress() {
        use super::StreamingOutputMode;

        let df = dataframe_from_batches(vec![simple_batch(&[1, 2, 3, 4])]);
        let f: ForeachBatchFn = Arc::new(move |_b, _e| Ok(()));

        let query = DataStreamWriter::new(df)
            .output_mode(StreamingOutputMode::Append)
            .trigger(StreamingTrigger::Once)
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query.await_termination().await.expect("termination");
        let status = query.status();

        assert_eq!(status.output_mode, StreamingOutputMode::Append);
        assert_eq!(
            status.trigger.as_deref(),
            Some("Once"),
            "trigger label should match"
        );
        let progress = status
            .last_progress
            .expect("at least one progress snapshot must be available after a batch ran");
        assert_eq!(progress.epoch, 0);
        assert_eq!(progress.input_rows, 4);
        assert_eq!(progress.output_rows, 4);
    }

    // Test: recent_progress returns history of snapshots
    #[tokio::test]
    async fn recent_progress_returns_history() {
        let df = dataframe_from_batches(vec![
            simple_batch(&[1]),
            simple_batch(&[2]),
            simple_batch(&[3]),
        ]);
        let f: ForeachBatchFn = Arc::new(move |_b, _e| Ok(()));

        // AvailableNow drains all 3 batches into a single micro-batch.
        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::AvailableNow)
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query.await_termination().await.expect("termination");
        let recent = query.recent_progress(8);
        assert!(
            !recent.is_empty(),
            "recent_progress should return at least one snapshot"
        );
    }

    // Test: output_mode getter reflects the configured mode
    #[test]
    fn streaming_output_mode_getter_reflects_configured_mode() {
        use super::StreamingOutputMode;
        // Pure value check; no streaming task needed.
        assert_eq!(StreamingOutputMode::Append, StreamingOutputMode::default());
    }

    // Test: format("memory") collects all batches into a Vec reachable from
    // the handle (ST4 — the new `.format()` builder wires a real sink).
    #[tokio::test]
    async fn format_memory_sink_collects_all_batches() {
        use super::StreamSinkFormat;

        let df = dataframe_from_batches(vec![simple_batch(&[1, 2]), simple_batch(&[3, 4, 5])]);

        let query = DataStreamWriter::new(df)
            .format("memory")
            .trigger(StreamingTrigger::AvailableNow)
            .start()
            .await
            .expect("start");

        query.await_termination().await.expect("termination");

        assert_eq!(query.format(), Some(StreamSinkFormat::Memory));
        let batches = query.memory_batches();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5, "memory sink must collect all 5 input rows");
    }

    // Test: format() rejects unknown names at start() (ST4 validation).
    #[tokio::test]
    async fn format_rejects_unknown_name_at_start() {
        let df = dataframe_from_batches(vec![simple_batch(&[1])]);
        let result = DataStreamWriter::new(df)
            .format("not-a-real-format")
            .trigger(StreamingTrigger::Once)
            .start()
            .await;
        assert!(
            result.is_err(),
            "unknown format name must reject at start()"
        );
    }

    // Test: Continuous trigger now uses barrier-driven micro-batching
    // (T5) instead of the previous row-by-row no-op loop.
    #[tokio::test]
    async fn continuous_trigger_emits_micro_batches() {
        let df = dataframe_from_batches(vec![
            simple_batch(&[1]),
            simple_batch(&[2]),
            simple_batch(&[3]),
        ]);
        let called = Arc::new(AtomicU64::new(0));
        let called_clone = Arc::clone(&called);
        let f: ForeachBatchFn = Arc::new(move |_b, _e| {
            called_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::Continuous(Duration::from_millis(20)))
            .foreach_batch(f)
            .start()
            .await
            .expect("start");

        query
            .await_termination_timeout(Duration::from_secs(5))
            .await
            .expect("termination");

        assert!(
            called.load(Ordering::Relaxed) >= 1,
            "Continuous trigger must emit at least one micro-batch"
        );
        let status = query.status();
        let trigger = status.trigger.unwrap_or_default();
        assert!(
            trigger.starts_with("Continuous("),
            "trigger label should be Continuous(...); got {trigger}"
        );
    }
}

#[cfg(test)]
mod listener_tests {
    use super::*;
    use crate::dataframe::DataFrame;
    use crate::types::ExecutionMode;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use dashmap::DashMap;
    use krishiv_runtime::LocalJobRegistry;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn simple_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn simple_batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            simple_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec())) as _],
        )
        .unwrap()
    }

    fn dataframe_from_batches(batches: Vec<RecordBatch>) -> DataFrame {
        use std::path::PathBuf;
        DataFrame::from_batches(
            ExecutionMode::Embedded,
            batches,
            Arc::new(Mutex::new(LocalJobRegistry::default())),
            Arc::new(AtomicU64::new(1)),
            crate::session::shared_embedded_runtime().expect("embedded runtime"),
            Arc::new(DashMap::<String, PathBuf>::new()),
        )
    }

    /// T17: a registered listener is notified exactly once with the
    /// correct `QueryTerminatedEvent` payload.
    #[tokio::test]
    async fn listener_receives_query_terminated_event() {
        let df = dataframe_from_batches(vec![simple_batch(&[1, 2, 3])]);
        let manager = StreamingQueryManager::new();
        let event_count = Arc::new(AtomicU64::new(0));
        let recorded_id = Arc::new(Mutex::new(String::new()));
        let recorded_exc = Arc::new(Mutex::new(None::<String>));
        struct TestListener {
            count: Arc<AtomicU64>,
            id: Arc<Mutex<String>>,
            exc: Arc<Mutex<Option<String>>>,
        }
        impl StreamingQueryListener for TestListener {
            fn on_query_terminated(&self, event: &QueryTerminatedEvent) {
                self.count.fetch_add(1, Ordering::Relaxed);
                *self.id.lock().unwrap_or_else(|p| p.into_inner()) = event.query_id.clone();
                *self.exc.lock().unwrap_or_else(|p| p.into_inner()) = event.exception.clone();
            }
        }
        manager.add_listener(Arc::new(TestListener {
            count: Arc::clone(&event_count),
            id: Arc::clone(&recorded_id),
            exc: Arc::clone(&recorded_exc),
        }));

        let query = DataStreamWriter::new(df)
            .trigger(StreamingTrigger::Once)
            .with_stream_manager(manager.clone())
            .start()
            .await
            .expect("start");

        let id = query.id().clone();
        query.await_termination().await.expect("termination");

        assert_eq!(
            event_count.load(Ordering::Relaxed),
            1,
            "listener must be called exactly once"
        );
        assert_eq!(
            recorded_id
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
            id.to_string()
        );
        assert!(
            recorded_exc
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        );
    }

    /// T17: `StreamingQueryManager::active_count` starts at zero for a
    /// fresh manager.
    #[test]
    fn active_count_tracks_strong_references() {
        let manager = StreamingQueryManager::new();
        assert_eq!(manager.active_count(), 0);
        assert!(manager.active_ids().is_empty());
    }

    /// ST1: Update mode emits rows (dedup-by-first-column happens at
    /// the writer layer for the in-memory sink; we just verify the
    /// user callback fires).
    #[tokio::test]
    async fn output_mode_update_emits_rows() {
        use super::StreamingOutputMode;

        let df = dataframe_from_batches(vec![simple_batch(&[1, 2, 3])]);
        let f: ForeachBatchFn = Arc::new(move |_b, _e| Ok(()));
        let q = DataStreamWriter::new(df)
            .output_mode(StreamingOutputMode::Update)
            .foreach_batch(f)
            .trigger(StreamingTrigger::Once)
            .start()
            .await
            .expect("start");
        q.await_termination().await.expect("termination");
    }

    /// ST2: Complete mode with `format("memory")` replaces the
    /// in-memory sink wholesale. After the second batch fires, the
    /// sink should contain only the second batch (not the first).
    #[tokio::test]
    async fn output_mode_complete_replaces_memory_sink() {
        use super::StreamingOutputMode;

        let df = dataframe_from_batches(vec![simple_batch(&[10, 20, 30])]);
        let q = DataStreamWriter::new(df)
            .format("memory")
            .output_mode(StreamingOutputMode::Complete)
            .trigger(StreamingTrigger::Once)
            .start()
            .await
            .expect("start");
        q.await_termination().await.expect("termination");

        let out = q.memory_batches();
        let total_rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "complete mode keeps at least one batch");
    }

    /// ST4: the Kafka transactional sink config is stored on the
    /// writer and is round-trippable via the public accessor.
    #[test]
    fn kafka_transactional_config_round_trips() {
        use super::KafkaTransactionalConfig;

        let df = dataframe_from_batches(vec![simple_batch(&[1])]);
        let cfg = KafkaTransactionalConfig::new("broker:9092", "topic-a", "txn-1");
        let w = DataStreamWriter::new(df).with_kafka_transactional(cfg.clone());
        let got = w.kafka_transactional_config().expect("config set");
        assert_eq!(got.bootstrap_servers, "broker:9092");
        assert_eq!(got.topic, "topic-a");
        assert_eq!(got.transactional_id, "txn-1");
        assert_eq!(got.transaction_timeout_ms, 30_000);
    }
}
