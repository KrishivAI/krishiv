use std::fmt;
use std::sync::Arc;

use krishiv_plan::{ExecutionKind, PhysicalPlan};
use krishiv_runtime::ExecutionRuntime;

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
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("name", &self.name)
            .field("mode", &self.mode)
            .field("execution_mode", &self.execution_mode)
            .field("batch_count", &self.batches.len())
            .finish_non_exhaustive()
    }
}

impl Stream {
    /// Create a stream with an explicit execution mode.
    ///
    /// Prefer [`Session::memory_stream`] so the stream inherits the session mode.
    pub fn new(
        name: impl Into<String>,
        mode: StreamMode,
        batches: Vec<StreamBatch>,
        execution_mode: ExecutionMode,
    ) -> Self {
        Self::for_session(
            name,
            mode,
            batches,
            execution_mode,
            None,
            None,
            crate::session::shared_embedded_runtime(),
        )
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
        }
    }

    /// Stream name.
    pub fn name(&self) -> &str {
        &self.name
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

    /// Collect bounded in-memory stream batches.
    pub fn collect_bounded(&self) -> Result<Vec<StreamBatch>> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream collection requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(&self.name, ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;
        Ok(self.batches.clone())
    }

    /// Execution mode for this stream.
    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    /// Map local stream batches.
    pub fn map_batches(&self, mut f: impl FnMut(&StreamBatch) -> StreamBatch) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream mapping requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(format!("{}:map", self.name), ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;

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
    pub fn filter_batches(&self, mut f: impl FnMut(&StreamBatch) -> bool) -> Result<Stream> {
        if !self.is_bounded() {
            return Err(KrishivError::unsupported(
                "unbounded stream filtering requires a streaming runtime",
            ));
        }

        let plan = PhysicalPlan::new(format!("{}:filter", self.name), ExecutionKind::Streaming);
        self.runtime.accept_plan(&plan)?;

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
    /// `key_by` is the entry point for the R5.1 stateful streaming API.
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
}
