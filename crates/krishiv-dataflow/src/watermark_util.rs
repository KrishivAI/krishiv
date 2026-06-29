//! Shared watermark computation utilities.

use arrow::array::{Array, Int64Array, StringArray};
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
        if arr.is_null(row) {
            continue;
        }
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
        if times.is_null(row) {
            continue;
        }
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
        multi.register_source(source_id);
        let max_ts = max_event_time_ms_for_source(batch, event_time_column, source_col, source_id)?;
        if max_ts > i64::MIN {
            let wm = max_ts.saturating_sub(*lag_ms as i64);
            multi.update(source_id, wm);
        }
    }
    Ok(multi.effective_watermark_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn time_batch(times: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(times)) as _]).unwrap()
    }

    fn multi_source_batch(times: Vec<i64>, sources: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("src", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(times)) as _,
                Arc::new(StringArray::from(sources)) as _,
            ],
        )
        .unwrap()
    }

    // ── max_event_time_ms ─────────────────────────────────────────────────────

    #[test]
    fn max_event_time_happy_path() {
        let batch = time_batch(vec![100, 500, 300]);
        assert_eq!(max_event_time_ms(&batch, "ts").unwrap(), 500);
    }

    #[test]
    fn max_event_time_single_row() {
        let batch = time_batch(vec![42]);
        assert_eq!(max_event_time_ms(&batch, "ts").unwrap(), 42);
    }

    #[test]
    fn max_event_time_empty_batch() {
        let batch = time_batch(vec![]);
        assert_eq!(max_event_time_ms(&batch, "ts").unwrap(), i64::MIN);
    }

    #[test]
    fn max_event_time_negative_values() {
        let batch = time_batch(vec![-1000, -500, -2000]);
        assert_eq!(max_event_time_ms(&batch, "ts").unwrap(), -500);
    }

    #[test]
    fn max_event_time_missing_column_returns_error() {
        let batch = time_batch(vec![100]);
        let err = max_event_time_ms(&batch, "nonexistent").unwrap_err();
        assert!(matches!(err, ExecError::ColumnNotFound(_)));
    }

    #[test]
    fn max_event_time_wrong_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["not_a_number"])) as _],
        )
        .unwrap();
        let err = max_event_time_ms(&batch, "ts").unwrap_err();
        assert!(matches!(err, ExecError::UnsupportedType(_)));
    }

    // ── max_event_time_ms_for_source ──────────────────────────────────────────

    #[test]
    fn max_event_time_for_source_happy_path() {
        let batch = multi_source_batch(vec![100, 200, 300, 400], vec!["a", "b", "a", "b"]);
        assert_eq!(
            max_event_time_ms_for_source(&batch, "ts", "src", "a").unwrap(),
            300
        );
        assert_eq!(
            max_event_time_ms_for_source(&batch, "ts", "src", "b").unwrap(),
            400
        );
    }

    #[test]
    fn max_event_time_for_source_no_match_returns_min() {
        let batch = multi_source_batch(vec![100, 200], vec!["a", "a"]);
        assert_eq!(
            max_event_time_ms_for_source(&batch, "ts", "src", "z").unwrap(),
            i64::MIN
        );
    }

    #[test]
    fn max_event_time_for_source_single_row() {
        let batch = multi_source_batch(vec![999], vec!["x"]);
        assert_eq!(
            max_event_time_ms_for_source(&batch, "ts", "src", "x").unwrap(),
            999
        );
    }

    #[test]
    fn max_event_time_for_source_missing_time_col() {
        let batch = multi_source_batch(vec![100], vec!["a"]);
        let err = max_event_time_ms_for_source(&batch, "nope", "src", "a").unwrap_err();
        assert!(matches!(err, ExecError::ColumnNotFound(_)));
    }

    #[test]
    fn max_event_time_for_source_missing_source_col() {
        let batch = multi_source_batch(vec![100], vec!["a"]);
        let err = max_event_time_ms_for_source(&batch, "ts", "nope", "a").unwrap_err();
        assert!(matches!(err, ExecError::ColumnNotFound(_)));
    }

    #[test]
    fn max_event_time_for_source_wrong_time_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Utf8, false),
            Field::new("src", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bad"])) as _,
                Arc::new(StringArray::from(vec!["a"])) as _,
            ],
        )
        .unwrap();
        let err = max_event_time_ms_for_source(&batch, "ts", "src", "a").unwrap_err();
        assert!(matches!(err, ExecError::UnsupportedType(_)));
    }

    #[test]
    fn max_event_time_for_source_wrong_source_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("src", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
        )
        .unwrap();
        let err = max_event_time_ms_for_source(&batch, "ts", "src", "a").unwrap_err();
        assert!(matches!(err, ExecError::UnsupportedType(_)));
    }

    // ── advance_effective_watermark ────────────────────────────────────────────

    #[test]
    fn advance_watermark_single_source() {
        let batch = time_batch(vec![100, 200, 300]);
        let mut single = crate::window::WatermarkState::new(0);
        let mut multi = crate::window::MultiSourceWatermarkState::new();
        let wm = advance_effective_watermark(
            &batch,
            "ts",
            None,
            &HashMap::new(),
            &mut single,
            &mut multi,
        )
        .unwrap();
        assert_eq!(wm, 300);
    }

    #[test]
    fn advance_watermark_single_source_with_lag() {
        let batch = time_batch(vec![1000]);
        let mut single = crate::window::WatermarkState::new(500);
        let mut multi = crate::window::MultiSourceWatermarkState::new();
        let wm = advance_effective_watermark(
            &batch,
            "ts",
            None,
            &HashMap::new(),
            &mut single,
            &mut multi,
        )
        .unwrap();
        assert_eq!(wm, 500); // 1000 - 500
    }

    #[test]
    fn advance_watermark_multi_source() {
        let batch = multi_source_batch(vec![1000, 2000], vec!["src-a", "src-b"]);
        let mut lags = HashMap::new();
        lags.insert("src-a".into(), 100u64);
        lags.insert("src-b".into(), 200u64);
        let mut single = crate::window::WatermarkState::new(0);
        let mut multi = crate::window::MultiSourceWatermarkState::new();
        let wm =
            advance_effective_watermark(&batch, "ts", Some("src"), &lags, &mut single, &mut multi)
                .unwrap();
        // src-a: 1000 - 100 = 900, src-b: 2000 - 200 = 1800
        // effective = min(900, 1800) = 900
        assert_eq!(wm, 900);
    }

    #[test]
    fn advance_watermark_multi_source_missing_configured_source_holds_min() {
        let batch = multi_source_batch(vec![1000], vec!["src-a"]);
        let mut lags = HashMap::new();
        lags.insert("src-a".into(), 100u64);
        lags.insert("src-b".into(), 200u64);
        let mut single = crate::window::WatermarkState::new(0);
        let mut multi = crate::window::MultiSourceWatermarkState::new();

        let wm =
            advance_effective_watermark(&batch, "ts", Some("src"), &lags, &mut single, &mut multi)
                .unwrap();

        assert_eq!(
            wm,
            i64::MIN,
            "configured source src-b has not emitted, so it must hold back the effective watermark"
        );
        assert_eq!(multi.source_count(), 2);
    }

    #[test]
    fn advance_watermark_multi_source_advances_after_all_configured_sources_seen() {
        let mut lags = HashMap::new();
        lags.insert("src-a".into(), 100u64);
        lags.insert("src-b".into(), 200u64);
        let mut single = crate::window::WatermarkState::new(0);
        let mut multi = crate::window::MultiSourceWatermarkState::new();

        let first = multi_source_batch(vec![1000], vec!["src-a"]);
        let first_wm =
            advance_effective_watermark(&first, "ts", Some("src"), &lags, &mut single, &mut multi)
                .unwrap();
        assert_eq!(first_wm, i64::MIN);

        let second = multi_source_batch(vec![2000], vec!["src-b"]);
        let second_wm =
            advance_effective_watermark(&second, "ts", Some("src"), &lags, &mut single, &mut multi)
                .unwrap();

        assert_eq!(second_wm, 900);
    }

    #[test]
    fn advance_watermark_multi_source_missing_column_returns_error() {
        let batch = time_batch(vec![1000]);
        let mut lags = HashMap::new();
        lags.insert("src-a".into(), 0u64);
        let mut single = crate::window::WatermarkState::new(0);
        let mut multi = crate::window::MultiSourceWatermarkState::new();
        let err =
            advance_effective_watermark(&batch, "ts", Some("src"), &lags, &mut single, &mut multi)
                .unwrap_err();
        // Missing source_id column produces a ColumnNotFound error.
        assert!(
            matches!(err, ExecError::ColumnNotFound(_)),
            "expected ColumnNotFound, got: {err:?}"
        );
    }
}
