//! Shared watermark computation utilities.

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Return the maximum event-time value from `column` in `batch`.
pub fn max_event_time_ms(batch: &RecordBatch, column: &str) -> ExecResult<i64> {
    let idx = batch
        .schema()
        .index_of(column)
        .map_err(|_| ExecError::ColumnNotFound(column.to_string()))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ExecError::UnsupportedType(format!("{column} must be Int64")))?;
    let mut max = i64::MIN;
    for row in 0..arr.len() {
        let v = arr.value(row);
        if v > max {
            max = v;
        }
    }
    Ok(max)
}

/// Return the maximum event-time value from `batch` for a specific `source_id`
/// identified by the `source_col` column.
pub fn max_event_time_ms_for_source(
    batch: &RecordBatch,
    time_col: &str,
    source_col: &str,
    source_id: &str,
) -> ExecResult<i64> {
    let time_idx = batch
        .schema()
        .index_of(time_col)
        .map_err(|_| ExecError::ColumnNotFound(time_col.to_string()))?;
    let source_idx = batch
        .schema()
        .index_of(source_col)
        .map_err(|_| ExecError::ColumnNotFound(source_col.to_string()))?;
    let times = batch
        .column(time_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ExecError::UnsupportedType(format!("{time_col} must be Int64")))?;
    let sources = batch
        .column(source_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ExecError::UnsupportedType(format!("{source_col} must be Utf8")))?;
    let mut max = i64::MIN;
    for row in 0..batch.num_rows() {
        if sources.value(row) == source_id {
            let v = times.value(row);
            if v > max {
                max = v;
            }
        }
    }
    Ok(max)
}

/// Advance the effective watermark for single- or multi-source setups.
///
/// Returns the new effective watermark in milliseconds.
pub fn advance_effective_watermark(
    batch: &RecordBatch,
    event_time_column: &str,
    source_id_column: Option<&str>,
    source_watermark_lags: &std::collections::HashMap<String, u64>,
    single: &mut crate::window::WatermarkState,
    multi: &mut crate::window::MultiSourceWatermarkState,
) -> ExecResult<i64> {
    if source_watermark_lags.is_empty() {
        let max_ts = max_event_time_ms(batch, event_time_column)?;
        if max_ts > i64::MIN {
            single.advance(max_ts);
        }
        return Ok(single.current_watermark_ms());
    }
    let source_col = source_id_column.ok_or_else(|| {
        ExecError::InvalidWindowConfig("multi-source watermark requires source_id_column".into())
    })?;
    for (source_id, lag_ms) in source_watermark_lags {
        let max_ts =
            max_event_time_ms_for_source(batch, event_time_column, source_col, source_id)?;
        if max_ts > i64::MIN {
            let wm = max_ts.saturating_sub(*lag_ms as i64);
            multi.update(source_id, wm);
        }
    }
    Ok(multi.effective_watermark_ms())
}
