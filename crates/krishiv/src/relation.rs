use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krishiv_api::{
    KrishivError, LocalWindowExecutionSpec, LocalWindowKind, QueryResult, StreamBatch,
};

// ── Window specification ──────────────────────────────────────────────────────

/// Window type for streaming aggregation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowSpec {
    /// Non-overlapping windows of fixed duration.
    Tumbling { size_ms: u64 },
    /// Overlapping windows that advance by `slide_ms` every step.
    Sliding { size_ms: u64, slide_ms: u64 },
    /// Activity-based windows closed by inactivity gaps.
    Session { gap_ms: u64 },
}

// ── Emit mode ─────────────────────────────────────────────────────────────────

/// Controls when a streaming relation emits results downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EmitMode {
    /// Emit results only when a batch query terminates (default).
    #[default]
    Batch,
    /// Emit one result record per closed window.
    PerWindow,
    /// Emit continuously as events arrive (requires unbounded sink).
    Continuous,
}

// ── StreamingChain ─────────────────────────────────────────────────────────────

/// Internal builder that accumulates streaming operator configuration.
pub(crate) struct StreamingChain {
    pub(crate) session: krishiv_api::Session,
    pub(crate) source_name: String,
    /// Input batches for bounded (in-memory) streams.
    pub(crate) batches: Vec<StreamBatch>,
    /// True if this chain has a finite set of input batches.
    pub(crate) bounded: bool,
    pub(crate) key_column: Option<String>,
    pub(crate) event_time_column: Option<String>,
    pub(crate) watermark_lag_ms: u64,
    pub(crate) window: Option<WindowSpec>,
    pub(crate) emit_mode: EmitMode,
    /// Custom aggregation expressions; defaults to COUNT(*) when None.
    pub(crate) agg_exprs: Option<Vec<krishiv_api::AggExpr>>,
}

impl StreamingChain {
    /// Build the `LocalWindowExecutionSpec` required for execution.
    fn build_exec_spec(&self) -> krishiv_api::Result<LocalWindowExecutionSpec> {
        let key_column = self.key_column.clone().ok_or_else(|| {
            KrishivError::unsupported("streaming relation requires .key_by() before execute")
        })?;
        let event_time_column = self.event_time_column.clone().ok_or_else(|| {
            KrishivError::unsupported(
                "streaming relation requires .with_event_time() before execute",
            )
        })?;
        let window = self.window.as_ref().ok_or_else(|| {
            KrishivError::unsupported("streaming relation requires .window() before execute")
        })?;

        let (window_kind, window_size_ms) = match window {
            WindowSpec::Tumbling { size_ms } => (LocalWindowKind::Tumbling, *size_ms),
            WindowSpec::Sliding { size_ms, slide_ms } => (
                LocalWindowKind::Sliding {
                    slide_ms: *slide_ms,
                },
                *size_ms,
            ),
            WindowSpec::Session { gap_ms } => {
                (LocalWindowKind::Session { gap_ms: *gap_ms }, *gap_ms)
            }
        };

        let state_ttl_ms = self.session.state_ttl().map(|c| c.ttl_ms());

        Ok(LocalWindowExecutionSpec {
            key_column,
            event_time_column,
            watermark_lag_ms: self.watermark_lag_ms,
            window_kind,
            window_size_ms,
            agg_exprs: self
                .agg_exprs
                .clone()
                .unwrap_or_else(LocalWindowExecutionSpec::default_count_agg),
            state_ttl_ms,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        })
    }

    /// Execute over the bounded in-memory batches.
    fn execute_bounded(&self) -> krishiv_api::Result<Vec<StreamBatch>> {
        let spec = self.build_exec_spec()?;
        let input: Vec<RecordBatch> = self.batches.iter().map(|b| b.batch().clone()).collect();
        // Use execute_windowed_stream so the plan is also registered via accept_plan
        // (matching the behaviour of the older WindowedStream::collect_with_aggs path).
        let output =
            krishiv_api::execute_windowed_stream(input, &spec).map_err(KrishivError::from)?;
        Ok(output
            .into_iter()
            .enumerate()
            .map(|(seq, batch)| StreamBatch::new(seq as u64, batch))
            .collect())
    }
}

// ── RelationKind ──────────────────────────────────────────────────────────────

/// Internal variant that distinguishes batch from streaming relations.
pub(crate) enum RelationKind {
    Batch(krishiv_api::DataFrame),
    Stream(StreamingChain),
}

// ── Relation ──────────────────────────────────────────────────────────────────

/// Unified batch and streaming relation.
///
/// A `Relation` is the single entry point for both batch SQL queries and
/// streaming windowed aggregations.  Use [`crate::SessionExt`] methods such as
/// [`crate::SessionExt::relation`], [`crate::SessionExt::from_parquet`],
/// [`crate::SessionExt::from_source`], and
/// [`crate::SessionExt::from_bounded_stream`] to construct one.
pub struct Relation {
    pub(crate) kind: RelationKind,
}

impl Relation {
    /// Returns `true` when the underlying source is finite (batch or bounded stream).
    pub fn is_bounded(&self) -> bool {
        match &self.kind {
            RelationKind::Batch(_) => true,
            RelationKind::Stream(chain) => chain.bounded,
        }
    }

    /// Return a human-readable description of the execution plan.
    pub fn explain(&self) -> crate::Result<String> {
        match &self.kind {
            RelationKind::Batch(df) => df.explain(),
            RelationKind::Stream(chain) => {
                let window_desc = match &chain.window {
                    Some(WindowSpec::Tumbling { size_ms }) => {
                        format!("tumbling({size_ms}ms)")
                    }
                    Some(WindowSpec::Sliding { size_ms, slide_ms }) => {
                        format!("sliding(size={size_ms}ms, slide={slide_ms}ms)")
                    }
                    Some(WindowSpec::Session { gap_ms }) => {
                        format!("session(gap={gap_ms}ms)")
                    }
                    None => "no-window".to_string(),
                };
                let bounded_label = if chain.bounded {
                    "bounded"
                } else {
                    "unbounded"
                };
                Ok(format!(
                    "Stream[{bounded_label}] source={} key={} event_time={} watermark={}ms window={} emit={:?}",
                    chain.source_name,
                    chain.key_column.as_deref().unwrap_or("<none>"),
                    chain.event_time_column.as_deref().unwrap_or("<none>"),
                    chain.watermark_lag_ms,
                    window_desc,
                    chain.emit_mode,
                ))
            }
        }
    }

    // ── Streaming builder methods ─────────────────────────────────────────────

    /// Key the stream by `column`.  Returns an error when called on a batch relation.
    pub fn key_by(self, column: impl Into<String>) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.key_column = Some(column.into());
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    /// Set the event-time column.  Returns unchanged when called on a batch relation.
    pub fn with_event_time(self, column: impl Into<String>) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.event_time_column = Some(column.into());
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    /// Set the watermark allowed lateness in milliseconds.
    pub fn watermark(self, lag_ms: u64) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.watermark_lag_ms = lag_ms;
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    /// Set the window specification.
    pub fn window(self, spec: WindowSpec) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.window = Some(spec);
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    /// Set the emit mode.
    pub fn emit(self, mode: EmitMode) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.emit_mode = mode;
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    /// Override the default COUNT(*) aggregation with custom expressions.
    ///
    /// ```rust,ignore
    /// use krishiv_api::{AggExpr, AggFunction};
    /// let result = session
    ///     .from_bounded_stream("orders", batches)
    ///     .key_by("customer_id")
    ///     .with_event_time("ts")
    ///     .window(WindowSpec::Tumbling { size_ms: 60_000 })
    ///     .agg(vec![
    ///         AggExpr { function: AggFunction::Sum, input_column: "amount".into(), output_column: "total".into() },
    ///         AggExpr { function: AggFunction::Count, input_column: String::new(), output_column: "cnt".into() },
    ///     ])
    ///     .collect()?;
    /// ```
    pub fn agg(self, exprs: Vec<krishiv_api::AggExpr>) -> Self {
        match self.kind {
            RelationKind::Stream(mut chain) => {
                chain.agg_exprs = Some(exprs);
                Relation {
                    kind: RelationKind::Stream(chain),
                }
            }
            RelationKind::Batch(_) => self,
        }
    }

    // ── Terminal operations ───────────────────────────────────────────────────

    /// Collect the relation into a [`QueryResult`].
    ///
    /// * Batch: executes the SQL query and returns all batches.
    /// * Bounded stream: runs the windowed aggregation in-process.
    /// * Unbounded stream: returns an error — use [`sink_to`] instead.
    pub fn collect(self) -> crate::Result<QueryResult> {
        match self.kind {
            RelationKind::Batch(df) => df.collect(),
            RelationKind::Stream(chain) => {
                if !chain.bounded {
                    return Err(KrishivError::unsupported(
                        "unbounded stream cannot be collected; use .sink_to() for continuous output",
                    ));
                }
                let stream_batches = chain.execute_bounded()?;
                let batches: Vec<RecordBatch> =
                    stream_batches.into_iter().map(|b| b.into_batch()).collect();
                Ok(QueryResult::new(batches))
            }
        }
    }

    /// Write all output to `sink`, returning a [`StreamHandle`].
    ///
    /// * Batch and bounded stream: executes synchronously, writes all batches, and
    ///   returns a completed handle.
    /// * Unbounded stream: submits a continuous job to the runtime, spawns a
    ///   background thread to poll and write, and returns an active handle.
    pub fn sink_to(
        self,
        mut sink: impl krishiv_connectors::DynSink + 'static,
    ) -> crate::Result<crate::StreamHandle> {
        match self.kind {
            RelationKind::Batch(df) => {
                krishiv_async_util::block_on(async {
                    for batch in df.collect()?.into_batches() {
                        sink.write_batch_dyn(batch)
                            .await
                            .map_err(|e| KrishivError::Runtime {
                                message: e.to_string(),
                            })?;
                    }
                    sink.flush_dyn().await.map_err(|e| KrishivError::Runtime {
                        message: e.to_string(),
                    })
                })?;
                Ok(crate::StreamHandle::completed())
            }
            RelationKind::Stream(chain) => {
                if chain.bounded {
                    let stream_batches = chain.execute_bounded()?;
                    krishiv_async_util::block_on(async {
                        for sb in stream_batches {
                            sink.write_batch_dyn(sb.into_batch()).await.map_err(|e| {
                                KrishivError::Runtime {
                                    message: e.to_string(),
                                }
                            })?;
                        }
                        sink.flush_dyn().await.map_err(|e| KrishivError::Runtime {
                            message: e.to_string(),
                        })
                    })?;
                    Ok(crate::StreamHandle::completed())
                } else {
                    // Unbounded: submit continuous streaming job.
                    let spec = chain.build_exec_spec()?;
                    let job_id = chain.session.submit_stream_job(&chain.source_name, spec)?;
                    let handle = crate::StreamHandle::new(job_id.clone(), chain.session.clone());
                    let cancelled_flag = handle.cancelled_flag();
                    let session_clone = chain.session.clone();
                    let job_id_clone = job_id.clone();

                    // Propagate runtime-creation failure back to the caller via a
                    // sync channel before we return the handle.
                    let (ready_tx, ready_rx) =
                        std::sync::mpsc::sync_channel::<Result<(), String>>(1);

                    std::thread::spawn(move || {
                        let rt = match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(rt) => {
                                let _ = ready_tx.send(Ok(()));
                                rt
                            }
                            Err(e) => {
                                let _ = ready_tx.send(Err(e.to_string()));
                                return;
                            }
                        };
                        rt.block_on(async {
                            loop {
                                // Check for cancellation.
                                {
                                    let cancelled =
                                        cancelled_flag.lock().unwrap_or_else(|e| e.into_inner());
                                    if *cancelled {
                                        break;
                                    }
                                }

                                match session_clone.poll_stream_job(&job_id_clone).await {
                                    Ok(batches) => {
                                        for batch in batches {
                                            if sink.write_batch_dyn(batch).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        // Job finished or error — exit the poll loop.
                                        break;
                                    }
                                }

                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                                // Re-check cancellation after sleep.
                                let cancelled =
                                    cancelled_flag.lock().unwrap_or_else(|e| e.into_inner());
                                if *cancelled {
                                    break;
                                }
                            }
                            let _ = sink.flush_dyn().await;
                        });
                    });

                    // Wait for the thread to signal that its runtime started (or failed).
                    ready_rx
                        .recv()
                        .unwrap_or(Err("background thread died before signalling".into()))
                        .map_err(|msg| KrishivError::Runtime { message: msg })?;

                    Ok(handle)
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_api::SessionBuilder;

    use super::*;
    use crate::session_ext::SessionExt;

    fn make_session() -> krishiv_api::Session {
        SessionBuilder::new().build().unwrap()
    }

    #[test]
    fn batch_relation_collect() {
        let session = make_session();
        let result = session
            .relation("SELECT 1 AS n")
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn batch_relation_into_batches() {
        let session = make_session();
        let result = session
            .relation("SELECT 1 AS n")
            .unwrap()
            .collect()
            .unwrap();
        let batches = result.into_batches();
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn batch_relation_is_bounded() {
        let session = make_session();
        let r = session.relation("SELECT 1 AS n").unwrap();
        assert!(r.is_bounded());
    }

    #[test]
    fn stream_relation_is_not_bounded() {
        let session = make_session();
        let r = session.from_source("events");
        assert!(!r.is_bounded());
    }

    #[test]
    fn bounded_stream_relation_collect() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "alice", "alice", "bob",
                ])) as _,
                Arc::new(Int64Array::from(vec![1_000i64, 5_000, 2_000])) as _,
                Arc::new(Int64Array::from(vec![10i64, 20, 30])) as _,
            ],
        )
        .unwrap();
        let batches = vec![StreamBatch::new(0, batch)];
        let session = make_session();
        let result = session
            .from_bounded_stream("test-stream", batches)
            .key_by("user_id")
            .with_event_time("ts")
            .watermark(0)
            .window(WindowSpec::Tumbling { size_ms: 10_000 })
            .collect()
            .unwrap();
        assert!(result.row_count() > 0);
    }

    #[test]
    fn unbounded_stream_collect_errors() {
        let session = make_session();
        let err = session
            .from_source("events")
            .key_by("user_id")
            .with_event_time("ts")
            .watermark(0)
            .window(WindowSpec::Tumbling { size_ms: 60_000 })
            .collect();
        assert!(err.is_err());
    }

    #[test]
    fn query_result_into_iterator() {
        let session = make_session();
        let result = session
            .relation("SELECT 1 AS n")
            .unwrap()
            .collect()
            .unwrap();
        let count = result.into_iter().count();
        assert_eq!(count, 1);
    }

    #[test]
    fn query_result_from_vec() {
        use krishiv_api::QueryResult;
        let qr: QueryResult = vec![].into();
        assert_eq!(qr.row_count(), 0);
    }
}
