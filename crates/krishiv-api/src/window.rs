use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krishiv_dataflow::AggExpr;
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};

use crate::error::{KrishivError, Result};
use crate::stream::Stream;
use crate::types::StreamBatch;

/// Watermark configuration for event-time streaming.
///
/// A fixed-lag watermark declares that no event with `event_time < max_seen − lag`
/// will ever arrive.  This is the only watermark strategy in R5.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkSpec {
    lag_ms: u64,
}

impl WatermarkSpec {
    /// Create a fixed-lag watermark with the given allowed lateness in milliseconds.
    pub fn fixed_lag_ms(lag_ms: u64) -> Self {
        Self { lag_ms }
    }

    /// Allowed lateness in milliseconds.
    pub fn lag_ms(&self) -> u64 {
        self.lag_ms
    }
}

/// A stream keyed by a column value.
///
/// Created by [`Stream::key_by`].  Use the builder methods to configure
/// event-time extraction, watermarking, and windowing before submitting to a
/// distributed runtime.
#[derive(Debug, Clone)]
pub struct KeyedStream {
    pub(crate) inner: Stream,
    pub(crate) key_column: String,
    pub(crate) event_time_column: Option<String>,
    pub(crate) watermark_spec: Option<WatermarkSpec>,
    pub(crate) multi_source_watermark: Option<MultiSourceWatermarkSpec>,
}

impl KeyedStream {
    /// Assign event time from `column` (must be `Int64` milliseconds since epoch).
    #[must_use]
    pub fn with_event_time(mut self, column: impl Into<String>) -> Self {
        self.event_time_column = Some(column.into());
        self
    }

    /// Configure the watermark strategy for late-event handling.
    #[must_use]
    pub fn watermark(mut self, spec: WatermarkSpec) -> Self {
        self.watermark_spec = Some(spec);
        self
    }

    /// Configure multi-source watermark reconciliation.
    ///
    /// **Alpha (R5.2)**: Multi-source watermark configuration. Not yet plumbed
    /// through all execution paths in the Relation API.
    #[must_use]
    pub fn with_multi_source_watermark(mut self, spec: MultiSourceWatermarkSpec) -> Self {
        self.multi_source_watermark = Some(spec);
        self
    }

    /// Multi-source watermark configuration, if set.
    pub fn multi_source_watermark(&self) -> Option<&MultiSourceWatermarkSpec> {
        self.multi_source_watermark.as_ref()
    }

    /// Create a tumbling event-time window of `window_size_ms` milliseconds.
    pub fn tumbling_window(self, window_size_ms: u64) -> WindowedStream {
        WindowedStream {
            keyed: self,
            window_size_ms,
        }
    }

    /// The column used to key the stream.
    pub fn key_column(&self) -> &str {
        &self.key_column
    }

    /// The event-time column, if configured.
    pub fn event_time_column(&self) -> Option<&str> {
        self.event_time_column.as_deref()
    }

    /// The watermark configuration, if set.
    pub fn watermark_spec(&self) -> Option<&WatermarkSpec> {
        self.watermark_spec.as_ref()
    }

    /// The inner stream.
    pub fn inner(&self) -> &Stream {
        &self.inner
    }
}

/// A keyed stream with a tumbling window applied.
///
/// Windowed stream descriptor; call [`WindowedStream::collect`] to execute locally
/// in embedded or single-node mode.
#[derive(Debug, Clone)]
pub struct WindowedStream {
    pub(crate) keyed: KeyedStream,
    pub(crate) window_size_ms: u64,
}

impl WindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Watermark lag in milliseconds (0 if not configured).
    pub fn watermark_lag_ms(&self) -> u64 {
        self.keyed.watermark_spec().map_or(0, WatermarkSpec::lag_ms)
    }

    /// Window size in milliseconds.
    pub fn window_size_ms(&self) -> u64 {
        self.window_size_ms
    }

    /// The underlying keyed stream.
    pub fn keyed_stream(&self) -> &KeyedStream {
        &self.keyed
    }

    /// Execute the tumbling window and collect output batches (embedded / single-node).
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute the tumbling window with custom aggregate expressions.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        let spec = build_tumbling_spec(&self.keyed, self.window_size_ms, agg_exprs)?;
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

fn event_time_column_for_keyed(keyed: &KeyedStream) -> Result<String> {
    keyed.event_time_column.clone().ok_or_else(|| {
        KrishivError::unsupported(
            "windowed stream execution requires with_event_time() before collect",
        )
    })
}

pub(crate) fn ensure_alpha_api(feature: &str) -> Result<()> {
    if krishiv_common::allows_alpha_api() {
        Ok(())
    } else {
        Err(KrishivError::unsupported(format!(
            "{feature} (alpha API disabled in production/durable profiles)"
        )))
    }
}

fn apply_multi_source_watermark(
    keyed: &KeyedStream,
    spec: &mut LocalWindowExecutionSpec,
) -> Result<()> {
    if keyed.multi_source_watermark().is_some() {
        ensure_alpha_api("multi_source_watermark")?;
    }
    if let Some(ms) = keyed.multi_source_watermark() {
        spec.source_watermark_lags = ms
            .source_specs()
            .iter()
            .map(|(id, ws)| (id.clone(), ws.lag_ms()))
            .collect();
        spec.source_id_column = ms.source_id_column().map(|s| s.to_string());
    }
    Ok(())
}

fn build_tumbling_spec(
    keyed: &KeyedStream,
    window_size_ms: u64,
    agg_exprs: Vec<AggExpr>,
) -> Result<LocalWindowExecutionSpec> {
    let event_time = event_time_column_for_keyed(keyed)?;
    let lag = keyed
        .watermark_spec()
        .map(WatermarkSpec::lag_ms)
        .unwrap_or(0);
    let mut spec = LocalWindowExecutionSpec {
            key_column: keyed.key_column.clone(),
            key_column_type: String::from("utf8"),
        event_time_column: event_time,
        watermark_lag_ms: lag,
        window_kind: LocalWindowKind::Tumbling,
        window_size_ms,
        agg_exprs,
        state_ttl_ms: keyed.inner.state_ttl_ms,
        source_watermark_lags: HashMap::new(),
        source_id_column: None,
    };
    apply_multi_source_watermark(keyed, &mut spec)?;
    Ok(spec)
}

fn execute_windowed_inner(
    stream: &Stream,
    spec: LocalWindowExecutionSpec,
) -> Result<Vec<StreamBatch>> {
    if !stream.is_bounded() {
        return Err(KrishivError::unsupported(
            "unbounded stream window execution requires Session::submit_stream_job",
        ));
    }
    let input: Vec<RecordBatch> = stream.batches.iter().map(|b| b.batch().clone()).collect();
    let output = stream
        .runtime
        .collect_bounded_window(stream.name(), input, &spec)
        .map_err(KrishivError::from)?;
    Ok(output
        .into_iter()
        .enumerate()
        .map(|(seq, batch)| StreamBatch::new(seq as u64, batch))
        .collect())
}

/// Multi-source watermark configuration.
///
/// **Alpha (R5.2)**: Multi-source watermark config. May change between minor releases.
/// Each source can have its own fixed-lag watermark.  The effective watermark
/// across all sources is the minimum, so a stalled source blocks all windows.
#[derive(Debug, Clone, Default)]
pub struct MultiSourceWatermarkSpec {
    source_specs: std::collections::HashMap<String, WatermarkSpec>,
    source_id_column: Option<String>,
}

impl MultiSourceWatermarkSpec {
    /// Create an empty multi-source spec.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a watermark spec for `source_id`.
    #[must_use]
    pub fn source(mut self, source_id: impl Into<String>, spec: WatermarkSpec) -> Self {
        self.source_specs.insert(source_id.into(), spec);
        self
    }

    /// Set the column name that identifies the source in each row.
    #[must_use]
    pub fn with_source_id_column(mut self, column: impl Into<String>) -> Self {
        self.source_id_column = Some(column.into());
        self
    }

    /// The configured per-source specs.
    pub fn source_specs(&self) -> &std::collections::HashMap<String, WatermarkSpec> {
        &self.source_specs
    }

    /// The source id column name, if configured.
    pub fn source_id_column(&self) -> Option<&str> {
        self.source_id_column.as_deref()
    }
}

/// State TTL (time-to-live) configuration for streaming operators.
///
/// **Alpha (R5.2)**: State TTL configuration. Not yet enforced in all operator paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateTtlConfig {
    ttl_ms: u64,
}

impl StateTtlConfig {
    /// Create a TTL config with the given duration in milliseconds.
    pub fn new(ttl_ms: u64) -> Self {
        Self { ttl_ms }
    }

    /// TTL duration in milliseconds.
    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// Convert to `krishiv-state` [`TtlConfig`] for state backends.
    pub fn to_ttl_config(self) -> krishiv_state::TtlConfig {
        krishiv_state::TtlConfig::new(self.ttl_ms)
    }
}

/// A keyed stream with a sliding window applied (R5.2).
#[derive(Debug, Clone)]
pub struct SlidingWindowedStream {
    pub(crate) keyed: KeyedStream,
    /// Total window duration in milliseconds.
    pub(crate) window_size_ms: u64,
    /// Slide step in milliseconds.
    pub(crate) slide_ms: u64,
}

impl SlidingWindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Watermark lag in milliseconds.
    pub fn watermark_lag_ms(&self) -> u64 {
        self.keyed.watermark_spec().map_or(0, WatermarkSpec::lag_ms)
    }

    /// Total window size in milliseconds.
    pub fn window_size_ms(&self) -> u64 {
        self.window_size_ms
    }

    /// Slide step in milliseconds.
    pub fn slide_ms(&self) -> u64 {
        self.slide_ms
    }
}

/// A keyed stream with a session window applied (R5.2).
#[derive(Debug, Clone)]
pub struct SessionWindowedStream {
    pub(crate) keyed: KeyedStream,
    /// Inactivity gap that closes a session in milliseconds.
    pub(crate) session_gap_ms: u64,
}

impl SessionWindowedStream {
    /// Key column name.
    pub fn key_column(&self) -> &str {
        self.keyed.key_column()
    }

    /// Event-time column name.
    pub fn event_time_column(&self) -> Option<&str> {
        self.keyed.event_time_column()
    }

    /// Inactivity gap in milliseconds.
    pub fn session_gap_ms(&self) -> u64 {
        self.session_gap_ms
    }

    /// Execute the session window and collect output batches.
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute with custom aggregates.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        ensure_alpha_api("session_window")?;
        let event_time = event_time_column_for_keyed(&self.keyed)?;
        let lag = self
            .keyed
            .watermark_spec()
            .map(WatermarkSpec::lag_ms)
            .unwrap_or(0);
        let mut spec = LocalWindowExecutionSpec {
                key_column: self.keyed.key_column.clone(),
                key_column_type: String::from("utf8"),
            event_time_column: event_time,
            watermark_lag_ms: lag,
            window_kind: LocalWindowKind::Session {
                gap_ms: self.session_gap_ms,
            },
            window_size_ms: self.session_gap_ms,
            agg_exprs,
            state_ttl_ms: self.keyed.inner.state_ttl_ms,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        apply_multi_source_watermark(&self.keyed, &mut spec)?;
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

impl SlidingWindowedStream {
    /// Execute the sliding window and collect output batches.
    pub fn collect(&self) -> Result<Vec<StreamBatch>> {
        self.collect_with_aggs(LocalWindowExecutionSpec::default_count_agg())
    }

    /// Execute with custom aggregates.
    pub fn collect_with_aggs(&self, agg_exprs: Vec<AggExpr>) -> Result<Vec<StreamBatch>> {
        ensure_alpha_api("sliding_window")?;
        let event_time = event_time_column_for_keyed(&self.keyed)?;
        let lag = self
            .keyed
            .watermark_spec()
            .map(WatermarkSpec::lag_ms)
            .unwrap_or(0);
        let mut spec = LocalWindowExecutionSpec {
                key_column: self.keyed.key_column.clone(),
                key_column_type: String::from("utf8"),
            event_time_column: event_time,
            watermark_lag_ms: lag,
            window_kind: LocalWindowKind::Sliding {
                slide_ms: self.slide_ms,
            },
            window_size_ms: self.window_size_ms,
            agg_exprs,
            state_ttl_ms: self.keyed.inner.state_ttl_ms,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        apply_multi_source_watermark(&self.keyed, &mut spec)?;
        execute_windowed_inner(&self.keyed.inner, spec)
    }
}

impl KeyedStream {
    /// Create a sliding event-time window of total size `window_size_ms` advancing
    /// by `slide_ms`.
    ///
    /// **Alpha (R5.2)**: Not yet fully implemented. Bounded streams only; unbounded
    /// streams will error at runtime.
    pub fn sliding_window(self, window_size_ms: u64, slide_ms: u64) -> SlidingWindowedStream {
        SlidingWindowedStream {
            keyed: self,
            window_size_ms,
            slide_ms,
        }
    }

    /// Create a session window that closes after `session_gap_ms` of inactivity.
    ///
    /// **Alpha (R5.2)**: Not yet fully implemented. Bounded streams only; unbounded
    /// streams will error at runtime.
    pub fn session_window(self, session_gap_ms: u64) -> SessionWindowedStream {
        SessionWindowedStream {
            keyed: self,
            session_gap_ms,
        }
    }
}
