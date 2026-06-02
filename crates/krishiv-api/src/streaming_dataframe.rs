use std::collections::HashMap;
use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use krishiv_exec::AggExpr;
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};

use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};

pub type KrishivStream = krishiv_plan::SendableRecordBatchStream;

/// A fluent builder for creating asynchronous streaming pipelines from a DataFrame.
pub struct StreamingDataFrame {
    df: DataFrame,
    event_time_column: Option<String>,
    key_column: Option<String>,
    window_kind: Option<LocalWindowKind>,
    window_size_ms: Option<u64>,
    agg_exprs: Vec<AggExpr>,
    watermark_lag_ms: u64,
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

    /// Set watermark lag.
    pub fn with_watermark_lag(mut self, lag_ms: u64) -> Self {
        self.watermark_lag_ms = lag_ms;
        self
    }

    /// Execute the configured streaming pipeline and return a lazy, asynchronous stream of RecordBatches.
    pub async fn execute_stream_async(self) -> Result<KrishivStream> {
        let df_stream = self.df.execute_stream_async().await?;

        // If no window is configured, just return the underlying stream
        if self.window_kind.is_none() && self.agg_exprs.is_empty() {
            return Ok(df_stream);
        }

        let event_time_column = self.event_time_column.ok_or_else(|| {
            KrishivError::unsupported("streaming aggregations require an event time column (use .with_event_time())")
        })?;

        let key_column = self.key_column.ok_or_else(|| {
            KrishivError::unsupported("streaming aggregations require a key column (use .key_by())")
        })?;

        let window_kind = self.window_kind.unwrap_or(LocalWindowKind::Tumbling);
        let window_size_ms = self.window_size_ms.unwrap_or(0);

        let agg_exprs = if self.agg_exprs.is_empty() {
            LocalWindowExecutionSpec::default_count_agg()
        } else {
            self.agg_exprs
        };

        let spec = LocalWindowExecutionSpec {
            key_column,
            event_time_column,
            watermark_lag_ms: self.watermark_lag_ms,
            window_kind,
            window_size_ms,
            agg_exprs,
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };

        let mapped_input_stream = df_stream.map(|res| res.map_err(|e| krishiv_exec::ExecError::InvalidWindowConfig(e)));

        let windowed = krishiv_runtime::execute_streaming_window(Box::pin(mapped_input_stream), &spec)
            .map_err(|e| KrishivError::Runtime { message: e.to_string() })?;
            
        let mapped_output_stream = windowed.map(|res| res.map_err(|e| e.to_string()));
        Ok(Box::pin(mapped_output_stream))
    }
}
