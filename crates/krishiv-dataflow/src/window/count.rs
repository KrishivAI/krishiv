#![forbid(unsafe_code)]

//! E3.1 — Count-based window operator.
//!
//! Emits an aggregate output record every `size` rows per key (hopping count
//! window with configurable slide step).
//!
//! - `size`  — number of input rows per output window.
//! - `slide` — how many rows to advance after each emission.
//!
//! When `slide == size` the operator behaves as a tumbling count window.
//! When `slide < size` windows overlap (hopping).
//!
//! # Algorithm
//!
//! For each key we maintain a `VecDeque` of per-row `AggState` contributions.
//! This lets us slide the window by simply dropping the first `slide` entries
//! from the front and recomputing the aggregate from the remaining entries.
//!
//! **Flush semantics**: on [`CountWindowOperator::flush`], any key buffer that
//! contains at least one row is emitted as a partial window.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::aggregate::{AggExpr, AggFunction, AggState};
use crate::join::extract_agg_key;
use crate::window::tumbling::{
    WindowRecordBatchInput, build_window_output_schema, build_window_record_batch,
};
use crate::{ExecError, ExecResult};

/// Specification for a count-based window operator.
#[derive(Debug, Clone)]
pub struct CountWindowSpec {
    /// Column to group by.
    pub key_column: String,
    /// Arrow type tag: `"utf8"`, `"int32"`, etc.
    pub key_column_type: String,
    /// Number of rows per window.
    pub size: u64,
    /// Slide step (rows to advance after each emission; ≤ `size`).
    pub slide: u64,
    /// Aggregates to compute within each window.
    pub agg_exprs: Vec<AggExpr>,
    /// Per-aggregate float flag: `true` when the aggregate input column is `Float64`.
    pub agg_is_float: Vec<bool>,
}

/// Per-row aggregate contribution (a single-row AggState).
///
/// Stored in the per-key rolling buffer so windows can be recomputed by
/// folding the contributions of the current window's rows.
struct RowContrib {
    agg: AggState,
}

/// Per-key state held between batches.
struct KeyState {
    /// Rolling buffer of individual-row contributions; oldest first.
    buf: VecDeque<RowContrib>,
    /// Global row index of the first row in the buffer.
    window_start_row: u64,
}

/// Count-based window operator.
pub struct CountWindowOperator {
    spec: CountWindowSpec,
    key_states: HashMap<String, KeyState>,
    output_schema: Arc<arrow::datatypes::Schema>,
    global_row: u64,
}

impl CountWindowOperator {
    pub fn new(spec: CountWindowSpec) -> ExecResult<Self> {
        if spec.size == 0 {
            return Err(ExecError::InvalidWindowConfig(
                "count window size must be > 0".into(),
            ));
        }
        if spec.slide == 0 || spec.slide > spec.size {
            return Err(ExecError::InvalidWindowConfig(
                "count window slide must be > 0 and ≤ size".into(),
            ));
        }
        let schema = build_window_output_schema(
            &spec.key_column,
            &spec.key_column_type,
            &spec.agg_exprs,
            &spec.agg_is_float,
        );
        Ok(Self {
            spec,
            key_states: HashMap::new(),
            output_schema: schema,
            global_row: 0,
        })
    }

    /// Feed one batch of rows; returns any completed windows.
    pub fn process_batch(&mut self, batch: &RecordBatch) -> ExecResult<Vec<RecordBatch>> {
        let key_idx = batch
            .schema()
            .index_of(&self.spec.key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.key_column.clone()))?;

        let mut output = Vec::new();

        for row in 0..batch.num_rows() {
            let key = extract_agg_key(batch, key_idx, row)?;
            let key_str = key.to_string();
            let global_row = self.global_row;
            self.global_row += 1;

            let state = self
                .key_states
                .entry(key_str.clone())
                .or_insert_with(|| KeyState {
                    buf: VecDeque::new(),
                    window_start_row: global_row,
                });

            // Compute the single-row contribution and push it.
            let mut contrib = AggState::new(&self.spec.agg_exprs);
            contrib.update(&self.spec.agg_exprs, batch, row)?;
            state.buf.push_back(RowContrib { agg: contrib });

            // If the buffer has reached `size`, emit this window then slide.
            while state.buf.len() >= self.spec.size as usize {
                let window_start = state.window_start_row;
                let window_end = window_start + self.spec.size;
                let merged = fold_agg_states(
                    state
                        .buf
                        .iter()
                        .take(self.spec.size as usize)
                        .map(|c| &c.agg),
                    &self.spec.agg_exprs,
                );
                output.push(build_window_record_batch(WindowRecordBatchInput {
                    schema: &self.output_schema,
                    key_type: &self.spec.key_column_type,
                    key_value: &key_str,
                    window_start_ms: window_start as i64,
                    window_end_ms: window_end as i64,
                    agg_exprs: &self.spec.agg_exprs,
                    state: &merged,
                    agg_is_float: &self.spec.agg_is_float,
                })?);
                // Slide: drop the first `slide` rows from the front.
                for _ in 0..self.spec.slide {
                    state.buf.pop_front();
                }
                state.window_start_row = window_start + self.spec.slide;
            }
        }

        Ok(output)
    }

    /// Flush all per-key buffered rows as a partial window (end-of-stream).
    pub fn flush(&mut self) -> ExecResult<Vec<RecordBatch>> {
        let mut output = Vec::new();
        for (key_str, state) in &self.key_states {
            if state.buf.is_empty() {
                continue;
            }
            let window_start = state.window_start_row;
            let window_end = window_start + state.buf.len() as u64;
            let merged = fold_agg_states(state.buf.iter().map(|c| &c.agg), &self.spec.agg_exprs);
            output.push(build_window_record_batch(WindowRecordBatchInput {
                schema: &self.output_schema,
                key_type: &self.spec.key_column_type,
                key_value: key_str,
                window_start_ms: window_start as i64,
                window_end_ms: window_end as i64,
                agg_exprs: &self.spec.agg_exprs,
                state: &merged,
                agg_is_float: &self.spec.agg_is_float,
            })?);
        }
        self.key_states.clear();
        Ok(output)
    }
}

/// Fold N single-row `AggState` contributions into one merged state.
fn fold_agg_states<'a>(
    iter: impl Iterator<Item = &'a AggState>,
    agg_exprs: &[AggExpr],
) -> AggState {
    let mut merged = AggState::new(agg_exprs);
    for contrib in iter {
        for (i, agg) in agg_exprs.iter().enumerate() {
            match agg.function {
                AggFunction::Count => {
                    merged.values[i] = merged.values[i]
                        .checked_add(contrib.values[i])
                        .unwrap_or(i64::MAX);
                    merged.has_value[i] = true;
                }
                AggFunction::Sum => {
                    merged.values[i] = merged.values[i]
                        .checked_add(contrib.values[i])
                        .unwrap_or(i64::MAX);
                    merged.float_values[i] += contrib.float_values[i];
                    if contrib.has_value[i] {
                        merged.has_value[i] = true;
                    }
                }
                AggFunction::Min => {
                    if contrib.has_value[i] && contrib.values[i] < merged.values[i] {
                        merged.values[i] = contrib.values[i];
                        merged.has_value[i] = true;
                    }
                    if contrib.has_value[i] && contrib.float_values[i] < merged.float_values[i] {
                        merged.float_values[i] = contrib.float_values[i];
                    }
                }
                AggFunction::Max => {
                    if contrib.has_value[i] && contrib.values[i] > merged.values[i] {
                        merged.values[i] = contrib.values[i];
                        merged.has_value[i] = true;
                    }
                    if contrib.has_value[i] && contrib.float_values[i] > merged.float_values[i] {
                        merged.float_values[i] = contrib.float_values[i];
                    }
                }
                AggFunction::Avg => {
                    merged.avg_sums[i] += contrib.avg_sums[i];
                    merged.avg_counts[i] += contrib.avg_counts[i];
                    if contrib.has_value[i] {
                        merged.has_value[i] = true;
                    }
                }
            }
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_spec(size: u64, slide: u64) -> CountWindowSpec {
        CountWindowSpec {
            key_column: "user_id".into(),
            key_column_type: "int32".into(),
            size,
            slide,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: String::new(),
                output_column: "cnt".into(),
            }],
        }
    }

    fn make_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    #[test]
    fn tumbling_count_window_emits_at_size() {
        let mut op = CountWindowOperator::new(make_spec(3, 3)).unwrap();
        let batch = make_batch(&[1, 1, 1, 2, 2, 2]);
        let result = op.process_batch(&batch).unwrap();
        // key 1: rows 0,1,2 → window; key 2: rows 3,4,5 → window
        assert_eq!(result.len(), 2, "one window per key");
        for rb in &result {
            let cnt_col = rb.column_by_name("cnt").unwrap();
            let cnt = cnt_col.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(cnt.value(0), 3, "count should be 3");
        }
    }

    #[test]
    fn hopping_count_window_overlaps() {
        // size=4, slide=2: first window at row 3 (0-3), second at row 5 (2-5).
        let mut op = CountWindowOperator::new(make_spec(4, 2)).unwrap();
        // 6 rows for key 1 → two complete windows
        let batch = make_batch(&[1, 1, 1, 1, 1, 1]);
        let result = op.process_batch(&batch).unwrap();
        assert_eq!(result.len(), 2, "two overlapping windows");
        for rb in &result {
            let cnt = rb.column_by_name("cnt").unwrap();
            let cnt = cnt.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(cnt.value(0), 4, "each window should aggregate 4 rows");
        }
    }

    #[test]
    fn flush_emits_partial_window() {
        let mut op = CountWindowOperator::new(make_spec(5, 5)).unwrap();
        let batch = make_batch(&[1, 1, 1]); // only 3 rows, not a full window
        let mid = op.process_batch(&batch).unwrap();
        assert_eq!(mid.len(), 0, "no complete window yet");
        let final_out = op.flush().unwrap();
        assert_eq!(final_out.len(), 1, "flush emits partial window");
        let cnt = final_out[0].column_by_name("cnt").unwrap();
        let cnt = cnt.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt.value(0), 3, "partial window has 3 rows");
    }

    #[test]
    fn count_window_rejects_zero_size() {
        assert!(CountWindowOperator::new(make_spec(0, 1)).is_err());
    }

    #[test]
    fn count_window_rejects_slide_greater_than_size() {
        assert!(CountWindowOperator::new(make_spec(3, 5)).is_err());
    }
}
