use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use krishiv_runtime::ExecutionRuntime;
use krishiv_sql::ContinuousTableInput;

use crate::error::{KrishivError, Result};
use crate::types::{ExecutionMode, StreamBatch, StreamMode};
pub use crate::window::KeyedStream;

/// Stream API for R1 local memory streams.
#[derive(Clone)]
pub struct Stream {
    pub(crate) name: String,
    pub(crate) mode: StreamMode,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) coordinator_url: Option<String>,
    pub(crate) state_ttl_ms: Option<u64>,
    pub(crate) batches: Vec<StreamBatch>,
    pub(crate) runtime: Arc<dyn ExecutionRuntime>,
    pub(crate) input: Option<Arc<ContinuousTableInput>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("execution_mode", &self.execution_mode)
            .field("batch_count", &self.batches.len())
            .field("has_input", &self.input.is_some())
            .finish_non_exhaustive()
    }
}

impl Stream {
    /// Create a bounded stream with an explicit execution mode.
    ///
    /// Prefer [`Session::memory_stream`] so the stream inherits the session mode.
    /// Unbounded streams require a registered schema and must be created with
    /// [`Session::unbounded_memory_stream`].
    pub fn new(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
    ) -> Result<Self> {
        if mode == StreamMode::Unbounded {
            return Err(KrishivError::InvalidConfig {
                message: "unbounded streams require Session::unbounded_memory_stream(name, schema)"
                    .into(),
            });
        }
        Ok(Self::for_session(
            name,
            mode,
            batches,
            execution_mode,
            None,
            None,
            crate::session::shared_embedded_runtime(),
        ))
    }

    pub(crate) fn for_session(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
        coordinator_url: Option<String>,
        state_ttl_ms: Option<u64>,
        runtime: Arc<dyn ExecutionRuntime>,
    ) -> Self {
        Self {
            name: name.into(),
            mode,
            execution_mode,
            coordinator_url,
            state_ttl_ms,
            batches,
            runtime,
            input: None,
        }
    }

    pub(crate) fn for_unbounded_session(
        name: impl Into<String>,
        schema_input: Arc<ContinuousTableInput>,
        execution_mode: ExecutionMode,
        coordinator_url: Option<String>,
        state_ttl_ms: Option<u64>,
        runtime: Arc<dyn ExecutionRuntime>,
    ) -> Self {
        Self {
            name: name.into(),
            mode: StreamMode::Unbounded,
            execution_mode,
            coordinator_url,
            state_ttl_ms,
            batches: Vec::new(),
            runtime,
            input: Some(schema_input),
        }
    }

    /// Stream name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set state TTL in milliseconds for stateful operations on this stream.
    pub fn with_state_ttl(mut self, ttl_ms: u64) -> Self {
        self.state_ttl_ms = Some(ttl_ms);
        self
    }

    /// Stream mode.
    pub fn mode(&self) -> StreamMode {
        self.mode
    }

    /// Whether this stream is bounded.
    pub fn is_bounded(&self) -> bool {
        self.mode == StreamMode::Bounded
    }

    /// Borrow local batches.
    pub fn batches(&self) -> &[StreamBatch] {
        &self.batches
    }

    /// Expected Arrow schema for an unbounded input stream.
    pub fn input_schema(&self) -> Option<&SchemaRef> {
        self.input.as_ref().map(|input| input.schema())
    }

    /// Submit an unbounded input batch without waiting for queue capacity.
    pub fn try_push_batch(&self, batch: RecordBatch) -> Result<()> {
        let input = self.unbounded_input()?;
        input.try_send(batch).map_err(KrishivError::from)
    }

    /// Submit an unbounded input batch, waiting asynchronously for capacity.
    pub async fn push_batch_async(&self, batch: RecordBatch) -> Result<()> {
        let input = self.unbounded_input()?;
        input.send(batch).await.map_err(KrishivError::from)
    }

    /// Close an unbounded input after all intended batches are submitted.
    ///
    /// The SQL consumer receives end-of-stream after draining queued batches.
    /// Returns `true` when this call closed the input.
    pub fn close_input(&self) -> Result<bool> {
        self.unbounded_input()?.close().map_err(KrishivError::from)
    }

    /// Whether an unbounded input has been closed.
    pub fn is_input_closed(&self) -> Result<bool> {
        self.unbounded_input()?
            .is_closed()
            .map_err(KrishivError::from)
    }

    /// Collect bounded in-memory stream batches.
    pub fn collect_bounded(&self) -> Result<Vec<StreamBatch>> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream collection requires a streaming runtime",
            ));
        }

        Ok(self.batches.clone())
    }

    /// Execution mode for this stream.
    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Map local stream batches.
    ///
    /// **Local-only**: Applies a transformation to an in-memory `Vec<StreamBatch>`.
    /// Not part of the DAG-planned or distributed execution path.
    pub fn map_batches(&self, mut f: impl FnMut(&StreamBatch) -> StreamBatch) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream mapping requires a streaming runtime",
            ));
        }

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches.iter().map(&mut f).collect(),
            self.execution_mode,
            self.coordinator_url.clone(),
            self.state_ttl_ms,
            self.runtime.clone(),
        ))
    }

    /// Filter local stream batches.
    ///
    /// **Local-only**: Filters an in-memory `Vec<StreamBatch>`.
    /// Not part of the DAG-planned or distributed execution path.
    pub fn filter_batches(&self, mut f: impl FnMut(&StreamBatch) -> bool) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream filtering requires a streaming runtime",
            ));
        }

        Ok(Self::for_session(
            self.name.clone(),
            self.mode,
            self.batches
                .iter()
                .filter(|batch| f(batch))
                .cloned()
                .collect(),
            self.execution_mode,
            self.coordinator_url.clone(),
            self.state_ttl_ms,
            self.runtime.clone(),
        ))
    }

    /// Key the stream by `column`, returning a [`KeyedStream`] that supports
    /// event-time windowing and stateful aggregation.
    ///
    /// **R5.1 Alpha**: Entry point for stateful streaming.
    /// For bounded in-memory streams, window aggregation and `collect()` work.
    /// For unbounded streams, `collect()` always returns an error —
    /// use `Session::submit_stream_job()` for continuous output.
    ///
    /// The same key always routes to the same executor task for the job
    /// lifetime (keyed-distribution stability contract).
    pub fn key_by(self, column: impl Into<String>) -> KeyedStream {
        KeyedStream {
            key_column: column.into(),
            event_time_column: None,
            watermark_spec: None,
            multi_source_watermark: None,
            inner: self,
        }
    }

    fn unbounded_input(&self) -> Result<&Arc<ContinuousTableInput>> {
        self.input.as_ref().ok_or_else(|| {
            KrishivError::unsupported(
                "batch ingestion is only available on unbounded input streams",
            )
        })
    }
}
