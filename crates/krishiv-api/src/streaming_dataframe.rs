use std::collections::HashMap;
use std::pin::Pin;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::Stream;
use futures::StreamExt;
use krishiv_exec::AggExpr;
use krishiv_exec::WatermarkState;
use krishiv_exec::interval_join::{IntervalJoinSpec, IntervalJoinState};
use krishiv_exec::side_output::{SideOutput, SideOutputRouter};
use krishiv_exec::temporal_join::{TemporalJoinSpec, VersionedTableState};
use krishiv_runtime::{LocalWindowExecutionSpec, LocalWindowKind};

use crate::dataframe::DataFrame;
use crate::error::{KrishivError, Result};

/// Alias for the inner stream error type used by the window runtime.
type ExecStream =
    Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, krishiv_exec::ExecError>> + Send>>;

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
    /// Optional side-output spec attached to this pipeline.
    side_output: Option<SideOutput>,
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

    /// Route late records to a named side output.
    ///
    /// Records whose event time is more than `lateness_threshold_ms` behind the
    /// current watermark are filtered out of the main pipeline and can be
    /// collected separately. When this is set, `execute_stream_async` only
    /// emits on-time records; late records are silently dropped from the main
    /// stream (collect them via a separate query if needed).
    pub fn with_side_output(mut self, name: impl Into<String>, lateness_threshold_ms: u64) -> Self {
        self.side_output = Some(SideOutput::new(name, lateness_threshold_ms));
        self
    }

    /// Execute the configured streaming pipeline and return a lazy, asynchronous stream of RecordBatches.
    pub async fn execute_stream_async(self) -> Result<KrishivStream> {
        let df_stream = self.df.execute_stream_async().await?;

        // If no window is configured, just return the underlying stream
        if self.window_kind.is_none() && self.agg_exprs.is_empty() {
            return Ok(df_stream);
        }

        let event_time_column = self.event_time_column.clone().ok_or_else(|| {
            KrishivError::unsupported(
                "streaming aggregations require an event time column (use .with_event_time())",
            )
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
            event_time_column: event_time_column.clone(),
            watermark_lag_ms: self.watermark_lag_ms,
            window_kind,
            window_size_ms,
            agg_exprs,
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };

        // When a side output is configured, filter out late records before
        // they reach the window operator. SideOutputRouter.is_late() classifies
        // each row; entire batches that contain any late rows are split.
        let input_stream: ExecStream = if let Some(side_out) = self.side_output {
            let router = SideOutputRouter::new(side_out, event_time_column);
            let watermark_lag = self.watermark_lag_ms;
            Box::pin(df_stream.filter_map(move |res| {
                let result = res
                    .map_err(krishiv_exec::ExecError::InvalidWindowConfig)
                    .map(|batch| filter_on_time_rows(&batch, &router, watermark_lag));
                async move {
                    match result {
                        Ok(Some(b)) => Some(Ok(b)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }))
        } else {
            Box::pin(df_stream.map(|res| res.map_err(krishiv_exec::ExecError::InvalidWindowConfig)))
        };

        let windowed =
            krishiv_runtime::execute_streaming_window(input_stream, &spec).map_err(|e| {
                KrishivError::Runtime {
                    message: e.to_string(),
                }
            })?;

        let mapped_output_stream = windowed.map(|res| res.map_err(|e| e.to_string()));
        Ok(Box::pin(mapped_output_stream))
    }
}

/// Keep only on-time rows from `batch` given a running watermark.
/// Returns `None` when the entire batch is late (common fast path).
fn filter_on_time_rows(
    batch: &RecordBatch,
    router: &SideOutputRouter,
    watermark_lag_ms: u64,
) -> Option<RecordBatch> {
    let col_idx = batch.schema().index_of(&router.event_time_column).ok()?;
    let ts_col = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()?;
    // Build a per-call watermark from the max event time in this batch.
    let mut wm = WatermarkState::new(watermark_lag_ms);
    for i in 0..ts_col.len() {
        if !ts_col.is_null(i) {
            wm.advance(ts_col.value(i));
        }
    }
    let keep: Vec<u32> = (0..batch.num_rows() as u32)
        .filter(|&i| {
            let et = if ts_col.is_null(i as usize) {
                0
            } else {
                ts_col.value(i as usize)
            };
            !router.is_late(&wm, et)
        })
        .collect();
    if keep.is_empty() {
        return None;
    }
    if keep.len() == batch.num_rows() {
        return Some(batch.clone());
    }
    let indices = arrow::array::UInt32Array::from(keep);
    let cols: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<_, _>>()
        .ok()?;
    RecordBatch::try_new(batch.schema(), cols).ok()
}

// ── Temporal join ──────────────────────────────────────────────────────────────

/// Stream-table as-of (temporal) join.
///
/// For each row in `stream_batches`, looks up the latest table snapshot in
/// `table_snapshots` whose `spec.table_version_col` timestamp is ≤ the row's
/// `spec.stream_time_col` value and returns the matched table batch. Rows with
/// no matching version are included with `None` table columns (left join) or
/// excluded (inner join, when `spec.inner_join = true`).
pub fn temporal_join(
    stream_batches: &[RecordBatch],
    table_snapshots: &[RecordBatch],
    spec: &TemporalJoinSpec,
    lookback_ms: i64,
) -> Result<Vec<(RecordBatch, Option<RecordBatch>)>> {
    let mut state = VersionedTableState::new(lookback_ms);
    for snap in table_snapshots {
        let ver_idx = snap
            .schema()
            .index_of(&spec.table_version_col)
            .map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?;
        let ver_col = snap
            .column(ver_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| KrishivError::Runtime {
                message: format!(
                    "table_version_col '{}' must be Int64",
                    spec.table_version_col
                ),
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
pub fn interval_join(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    left_time_col: &str,
    right_time_col: &str,
    spec: IntervalJoinSpec,
) -> Result<Vec<(RecordBatch, RecordBatch)>> {
    let mut state = IntervalJoinState::new(spec);
    let mut pairs = Vec::new();

    // Helper: extract i64 event time from a single-column lookup.
    let get_times = |batch: &RecordBatch, col: &str| -> Result<Vec<i64>> {
        let idx = batch
            .schema()
            .index_of(col)
            .map_err(|e| KrishivError::Runtime {
                message: e.to_string(),
            })?;
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| KrishivError::Runtime {
                message: format!("event time column '{col}' must be Int64"),
            })?;
        Ok((0..arr.len())
            .map(|i| if arr.is_null(i) { 0 } else { arr.value(i) })
            .collect())
    };

    for batch in left_batches {
        let times = get_times(batch, left_time_col)?;
        for (i, &t) in times.iter().enumerate() {
            let row = batch.slice(i, 1);
            let matched = state.push_left(t, row);
            pairs.extend(matched);
        }
    }
    for batch in right_batches {
        let times = get_times(batch, right_time_col)?;
        for (i, &t) in times.iter().enumerate() {
            let row = batch.slice(i, 1);
            let matched = state.push_right(t, row);
            pairs.extend(matched);
        }
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use krishiv_exec::interval_join::IntervalJoinSpec;
    use krishiv_exec::temporal_join::TemporalJoinSpec;

    use super::{interval_join, temporal_join};

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

    fn interval_batch(times: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            interval_schema(),
            vec![
                Arc::new(Int64Array::from(times.to_vec())) as _,
                Arc::new(Int64Array::from(values.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn temporal_join_matches_latest_table_version() {
        // Table has versions at t=100 and t=500. Stream event at t=300 should
        // match version at t=100 (the latest version ≤ 300).
        let table = table_batch(&[100, 500], &[10, 20]);
        let stream = stream_batch(&["alice"], &[300]);

        let spec = TemporalJoinSpec {
            stream_time_col: "stream_ts".to_string(),
            table_version_col: "version_ts".to_string(),
            join_keys: vec![],
            inner_join: false,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, 60_000).unwrap();
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
            table_version_col: "version_ts".to_string(),
            join_keys: vec![],
            inner_join: true,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, 60_000).unwrap();
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
            table_version_col: "version_ts".to_string(),
            join_keys: vec![],
            inner_join: false,
        };

        let pairs = temporal_join(&[stream], &[table], &spec, 60_000).unwrap();
        assert_eq!(pairs.len(), 1);
        assert!(
            pairs[0].1.is_none(),
            "left join must include row with None table match"
        );
    }

    #[test]
    fn interval_join_matches_events_within_window() {
        // Left event at t=100, right event at t=150 → delta=50, within [0, 100].
        let left = interval_batch(&[100], &[1]);
        let right = interval_batch(&[150], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
        };
        let pairs = interval_join(&[left], &[right], "event_ts", "event_ts", spec).unwrap();
        assert_eq!(pairs.len(), 1, "events within window should match");
    }

    #[test]
    fn interval_join_excludes_events_outside_window() {
        // Left at t=100, right at t=300 → delta=200, outside [0, 100].
        let left = interval_batch(&[100], &[1]);
        let right = interval_batch(&[300], &[2]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 100,
            key_column: "k".into(),
        };
        let pairs = interval_join(&[left], &[right], "event_ts", "event_ts", spec).unwrap();
        assert!(pairs.is_empty(), "events outside window must not match");
    }

    #[test]
    fn interval_join_multiple_matches() {
        // One left event matched by two right events within the window.
        let left = interval_batch(&[1000], &[1]);
        let right = interval_batch(&[1050, 1080, 2000], &[10, 20, 30]);

        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 200,
            key_column: "k".into(),
        };
        let pairs = interval_join(&[left], &[right], "event_ts", "event_ts", spec).unwrap();
        // 1050-1000=50 ✓, 1080-1000=80 ✓, 2000-1000=1000 ✗
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn interval_join_empty_inputs_returns_empty() {
        let spec = IntervalJoinSpec {
            lower_bound_ms: 0,
            upper_bound_ms: 1000,
            key_column: "k".into(),
        };
        let pairs = interval_join(&[], &[], "event_ts", "event_ts", spec).unwrap();
        assert!(pairs.is_empty());
    }
}
