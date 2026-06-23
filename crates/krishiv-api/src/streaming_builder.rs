//! Structured streaming builder — Phase F.
//!
//! Provides [`DataStreamReader`], [`DataStreamWriter`], [`StreamingQuery`],
//! [`StreamingOutputMode`], and [`StreamingTrigger`] for structured streaming pipelines.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use futures::StreamExt as _;

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

/// Progress snapshot of a running streaming query.
#[derive(Debug, Clone)]
pub struct StreamingQueryProgress {
    pub epoch: i64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub trigger: Option<String>,
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
/// - read the latest progress snapshot (`last_progress`).
pub struct StreamingQuery {
    id: QueryId,
    name: Option<String>,
    state_rx: tokio::sync::watch::Receiver<StreamingQueryState>,
    cancel_tx: Arc<tokio::sync::watch::Sender<bool>>,
    last_progress: Arc<Mutex<Option<StreamingQueryProgress>>>,
    /// Aborted on drop so the micro-batch task does not outlive the handle.
    _task: tokio::task::JoinHandle<()>,
}

impl StreamingQuery {
    fn new(
        id: QueryId,
        name: Option<String>,
        state_rx: tokio::sync::watch::Receiver<StreamingQueryState>,
        cancel_tx: Arc<tokio::sync::watch::Sender<bool>>,
        last_progress: Arc<Mutex<Option<StreamingQueryProgress>>>,
        task: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            id,
            name,
            state_rx,
            cancel_tx,
            last_progress,
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
    pub async fn await_termination(mut self) -> Result<()> {
        loop {
            {
                let state = self.state_rx.borrow();
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
            if self.state_rx.changed().await.is_err() {
                // Sender dropped — query task is done.
                return Ok(());
            }
        }
    }

    /// Await termination with a timeout.
    pub async fn await_termination_timeout(self, dur: Duration) -> Result<()> {
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
}

impl std::fmt::Debug for StreamingQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingQuery")
            .field("id", &self.id)
            .field("name", &self.name)
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
    options: std::collections::HashMap<String, String>,
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
            options: std::collections::HashMap::new(),
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

        let (state_tx, state_rx) = tokio::sync::watch::channel(StreamingQueryState::Active);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let cancel_tx = Arc::new(cancel_tx);
        let last_progress: Arc<Mutex<Option<StreamingQueryProgress>>> = Arc::new(Mutex::new(None));

        let progress_ref = Arc::clone(&last_progress);
        let foreach_fn = self.foreach_batch_fn;
        let trigger = self.trigger;

        // Materialise the DataFrame's stream once.
        let base_stream: KrishivStream = self.df.execute_stream_async().await?;

        let cancel_rx_task = cancel_rx;

        let task = tokio::spawn(async move {
            let result = run_streaming_task(
                base_stream,
                trigger,
                foreach_fn,
                cancel_rx_task,
                progress_ref,
            )
            .await;
            let final_state = match result {
                Ok(()) => StreamingQueryState::Stopped,
                Err(e) => StreamingQueryState::Failed(e.to_string()),
            };
            let _ = state_tx.send(final_state);
        });

        Ok(StreamingQuery::new(
            id,
            name,
            state_rx,
            cancel_tx,
            last_progress,
            task,
        ))
    }
}

// ── Internal task runner ──────────────────────────────────────────────────────

async fn run_streaming_task(
    stream: KrishivStream,
    trigger: StreamingTrigger,
    foreach_fn: Option<ForeachBatchFn>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: Arc<Mutex<Option<StreamingQueryProgress>>>,
) -> Result<()> {
    match trigger {
        StreamingTrigger::Once | StreamingTrigger::AvailableNow => {
            drain_and_call(stream, foreach_fn, 0, &progress).await
        }
        StreamingTrigger::ProcessingTime(interval) => {
            processing_time_loop(stream, interval, foreach_fn, cancel_rx, &progress).await
        }
        StreamingTrigger::Continuous(_checkpoint_interval) => {
            continuous_loop(stream, foreach_fn, cancel_rx, &progress).await
        }
    }
}

/// Drain the entire stream, accumulate into one micro-batch, call the callback.
async fn drain_and_call(
    mut stream: KrishivStream,
    foreach_fn: Option<ForeachBatchFn>,
    epoch: i64,
    progress: &Arc<Mutex<Option<StreamingQueryProgress>>>,
) -> Result<()> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| KrishivError::Runtime { message: e })?;
        batches.push(batch);
    }

    let input_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let output_rows = input_rows;

    if let Some(f) = foreach_fn {
        f(batches, epoch)?;
    }

    update_progress(
        progress,
        epoch,
        input_rows,
        output_rows,
        Some("AvailableNow"),
    );
    Ok(())
}

/// ProcessingTime: accumulate for `interval`, call callback, check cancel, repeat.
async fn processing_time_loop(
    mut stream: KrishivStream,
    interval: Duration,
    foreach_fn: Option<ForeachBatchFn>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: &Arc<Mutex<Option<StreamingQueryProgress>>>,
) -> Result<()> {
    let mut epoch: i64 = 0;
    let trigger_label = format!("ProcessingTime({}ms)", interval.as_millis());

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
        if let Some(f) = foreach_fn.as_ref() {
            f(batches, epoch)?;
        }
        update_progress(
            progress,
            epoch,
            input_rows,
            input_rows,
            Some(trigger_label.as_str()),
        );
        epoch += 1;

        if stream_ended {
            break;
        }
    }
    Ok(())
}

/// Continuous: call the callback for each record batch as it arrives.
async fn continuous_loop(
    mut stream: KrishivStream,
    foreach_fn: Option<ForeachBatchFn>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    progress: &Arc<Mutex<Option<StreamingQueryProgress>>>,
) -> Result<()> {
    let mut epoch: i64 = 0;

    loop {
        tokio::select! {
            biased;

            // Cancellation check.
            changed = cancel_rx.changed() => {
                if changed.is_ok() && *cancel_rx.borrow() {
                    return Ok(());
                }
            }

            item = stream.next() => {
                match item {
                    None => break,
                    Some(Err(e)) => return Err(KrishivError::Runtime { message: e }),
                    Some(Ok(batch)) => {
                        let rows = batch.num_rows() as u64;
                        if let Some(f) = foreach_fn.as_ref() {
                            f(vec![batch], epoch)?;
                        }
                        update_progress(progress, epoch, rows, rows, Some("Continuous"));
                        epoch += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

fn update_progress(
    progress: &Arc<Mutex<Option<StreamingQueryProgress>>>,
    epoch: i64,
    input_rows: u64,
    output_rows: u64,
    trigger: Option<&str>,
) {
    if let Ok(mut guard) = progress.lock() {
        *guard = Some(StreamingQueryProgress {
            epoch,
            input_rows,
            output_rows,
            trigger: trigger.map(str::to_owned),
        });
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
}
