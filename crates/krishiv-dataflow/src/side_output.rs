//! Late-data side output routing (R16 S3.3).

use arrow::array::{Array, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;

use crate::window::WatermarkState;
use crate::{ExecError, ExecResult};

/// Named side output for late records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideOutput {
    pub name: String,
    pub lateness_threshold_ms: u64,
}

impl SideOutput {
    pub fn new(name: impl Into<String>, lateness_threshold_ms: u64) -> Self {
        Self {
            name: name.into(),
            lateness_threshold_ms,
        }
    }
}

/// Routes batches to main or side output based on event-time vs watermark.
#[derive(Debug, Clone)]
pub struct SideOutputRouter {
    pub spec: SideOutput,
    pub event_time_column: String,
}

/// A micro-batch split into its on-time and late rows.
#[derive(Debug)]
pub struct RoutedBatch {
    /// Rows that remain in the primary pipeline.
    pub main: Option<RecordBatch>,
    /// Rows classified for the named side output.
    pub side: Option<RecordBatch>,
}

impl SideOutputRouter {
    pub fn new(spec: SideOutput, event_time_column: impl Into<String>) -> Self {
        Self {
            spec,
            event_time_column: event_time_column.into(),
        }
    }

    /// Classify `event_time_ms` relative to current watermark.
    pub fn is_late(&self, watermark: &WatermarkState, event_time_ms: i64) -> bool {
        let late_boundary = i128::from(watermark.current_watermark_ms())
            - i128::from(self.spec.lateness_threshold_ms);
        i128::from(event_time_ms) < late_boundary
    }

    /// Split one input batch using the watermark established by earlier batches.
    ///
    /// Classification intentionally happens before advancing `watermark` with
    /// this batch. This matches the window operators' micro-batch contract:
    /// rows in one batch share the previous batch's watermark, then the
    /// high-water mark advances for the next batch.
    pub fn route_batch(
        &self,
        batch: &RecordBatch,
        watermark: &mut WatermarkState,
    ) -> ExecResult<RoutedBatch> {
        let column_index = batch
            .schema()
            .index_of(&self.event_time_column)
            .map_err(|_| ExecError::ColumnNotFound(self.event_time_column.clone()))?;
        let event_times = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "event-time column '{}' must be Int64",
                    self.event_time_column
                ))
            })?;

        let mut main_indices = Vec::with_capacity(batch.num_rows());
        let mut side_indices = Vec::new();
        let mut max_event_time = None;

        for row in 0..batch.num_rows() {
            if event_times.is_null(row) {
                return Err(ExecError::InvalidInput(format!(
                    "event-time column '{}' contains null at row {row}",
                    self.event_time_column
                )));
            }

            let event_time = event_times.value(row);
            max_event_time =
                Some(max_event_time.map_or(event_time, |current: i64| current.max(event_time)));
            let row = u32::try_from(row).map_err(|_| {
                ExecError::InvalidInput(
                    "side-output routing does not support batches with more than u32::MAX rows"
                        .into(),
                )
            })?;
            if self.is_late(watermark, event_time) {
                side_indices.push(row);
            } else {
                main_indices.push(row);
            }
        }

        if let Some(max_event_time) = max_event_time {
            watermark.advance(max_event_time);
        }

        Ok(RoutedBatch {
            main: select_rows(batch, main_indices)?,
            side: select_rows(batch, side_indices)?,
        })
    }
}

fn select_rows(batch: &RecordBatch, indices: Vec<u32>) -> ExecResult<Option<RecordBatch>> {
    if indices.is_empty() {
        return Ok(None);
    }
    if indices.len() == batch.num_rows() {
        return Ok(Some(batch.clone()));
    }

    let indices = UInt32Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(keys: Vec<&str>, times: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(times)),
            ],
        )
        .expect("valid test batch")
    }

    #[test]
    fn late_record_detected_beyond_threshold() {
        let wm = WatermarkState::new(1000);
        let mut state = wm;
        state.advance(10_000);
        let router = SideOutputRouter::new(SideOutput::new("late", 500), "ts");
        assert!(router.is_late(&state, 8_000));
        assert!(!router.is_late(&state, 9_500));
    }

    #[test]
    fn zero_threshold_exact_watermark_boundary() {
        let mut state = WatermarkState::new(0);
        state.advance(1000);
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "ts");
        // event_time < watermark → late
        assert!(router.is_late(&state, 999));
        // event_time == watermark → not late (strict less-than)
        assert!(!router.is_late(&state, 1000));
        // event_time > watermark → not late
        assert!(!router.is_late(&state, 1001));
    }

    #[test]
    fn not_late_before_watermark_advanced() {
        let mut state = WatermarkState::new(0);
        state.advance(500);
        let router = SideOutputRouter::new(SideOutput::new("late", 200), "ts");
        // watermark=500, threshold=200 → effective threshold=300
        // event at 400: 400 < 300? No → not late
        assert!(!router.is_late(&state, 400));
        // event at 200: 200 < 300? Yes → late
        assert!(router.is_late(&state, 200));
    }

    #[test]
    fn high_threshold_prevents_lateness() {
        let mut state = WatermarkState::new(0);
        state.advance(10_000);
        // Very high threshold: 20000
        let router = SideOutputRouter::new(SideOutput::new("late", 20_000), "ts");
        // watermark=10000, threshold=20000 → effective = 10000 - 20000 underflow = 0 (saturating_sub)
        assert!(!router.is_late(&state, 0));
    }

    #[test]
    fn side_output_fields_correctly_set() {
        let so = SideOutput::new("dlq", 5000);
        assert_eq!(so.name, "dlq");
        assert_eq!(so.lateness_threshold_ms, 5000);
    }

    #[test]
    fn router_stores_event_time_column() {
        let router = SideOutputRouter::new(SideOutput::new("late", 100), "event_ts");
        assert_eq!(router.event_time_column, "event_ts");
    }

    #[test]
    fn router_debug_trait() {
        let router = SideOutputRouter::new(SideOutput::new("late", 100), "ts");
        let dbg = format!("{:?}", router);
        assert!(dbg.contains("SideOutputRouter"));
        assert!(dbg.contains("late"));
        assert!(dbg.contains("ts"));
    }

    #[test]
    fn route_batch_uses_watermark_from_previous_batches() {
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "ts");
        let mut watermark = WatermarkState::new(0);

        let first = router
            .route_batch(&batch(vec!["a"], vec![10_000]), &mut watermark)
            .expect("first batch should route");
        assert_eq!(first.main.expect("on-time batch").num_rows(), 1);
        assert!(first.side.is_none());
        assert_eq!(watermark.current_watermark_ms(), 10_000);

        let second = router
            .route_batch(
                &batch(vec!["late", "on-time"], vec![1_000, 11_000]),
                &mut watermark,
            )
            .expect("second batch should route");
        assert_eq!(second.main.expect("on-time batch").num_rows(), 1);
        assert_eq!(second.side.expect("late batch").num_rows(), 1);
        assert_eq!(watermark.current_watermark_ms(), 11_000);
    }

    #[test]
    fn maximum_lateness_threshold_does_not_wrap_negative() {
        let router = SideOutputRouter::new(SideOutput::new("late", u64::MAX), "ts");
        let mut watermark = WatermarkState::new(0);
        watermark.advance(i64::MAX);

        assert!(!router.is_late(&watermark, i64::MIN));
    }

    #[test]
    fn route_batch_rejects_missing_event_time_column() {
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "missing");
        let mut watermark = WatermarkState::new(0);

        assert!(matches!(
            router.route_batch(&batch(vec!["a"], vec![1]), &mut watermark),
            Err(ExecError::ColumnNotFound(column)) if column == "missing"
        ));
    }

    #[test]
    fn route_batch_rejects_non_int64_event_time() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, false)]));
        let string_time_batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["1000"]))])
                .expect("valid string batch");
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "ts");
        let mut watermark = WatermarkState::new(0);

        assert!(matches!(
            router.route_batch(&string_time_batch, &mut watermark),
            Err(ExecError::UnsupportedType(message)) if message.contains("must be Int64")
        ));
    }

    #[test]
    fn route_batch_rejects_null_event_time() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let null_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .expect("valid nullable batch");
        let router = SideOutputRouter::new(SideOutput::new("late", 0), "ts");
        let mut watermark = WatermarkState::new(0);

        assert!(matches!(
            router.route_batch(&null_batch, &mut watermark),
            Err(ExecError::InvalidInput(message)) if message.contains("null at row 1")
        ));
        assert_eq!(watermark.current_watermark_ms(), i64::MIN);
    }
}
