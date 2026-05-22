use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};
use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;

/// Configuration for a session event-time window operator (R5.2).
///
/// A session window opens on the first event for a key and extends as long
/// as events keep arriving within `session_gap_ms` of the previous event.
/// The window closes when the watermark passes `last_event_time + session_gap_ms`.
#[derive(Debug, Clone)]
pub struct SessionWindowSpec {
    /// Column used to key the stream.
    pub key_column: String,
    /// Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Inactivity gap that closes the session in milliseconds.
    pub session_gap_ms: u64,
    /// Aggregate expressions to apply within each session.
    pub agg_exprs: Vec<AggExpr>,
}

pub(crate) struct SessionState {
    pub(crate) session_start_ms: i64,
    pub(crate) last_event_time_ms: i64,
    pub(crate) agg: AggState,
}

/// Session event-time window operator (R5.2).
pub struct SessionWindowOperator {
    spec: SessionWindowSpec,
    // Keyed by serialised key value.
    sessions: HashMap<String, SessionState>,
    prev_watermark_ms: i64,
}

impl SessionWindowOperator {
    /// Create a new session window operator.
    pub fn new(spec: SessionWindowSpec) -> Self {
        Self {
            spec,
            sessions: HashMap::new(),
            prev_watermark_ms: i64::MIN,
        }
    }

    /// Number of open sessions.
    pub fn open_session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Process one `RecordBatch`, returning closed session outputs.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        new_watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        let key_idx = batch
            .schema()
            .index_of(&self.spec.key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.key_column.clone()))?;
        let time_idx = batch
            .schema()
            .index_of(&self.spec.event_time_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.event_time_column.clone()))?;

        let time_arr = batch
            .column(time_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "event_time column '{}' must be Int64",
                    self.spec.event_time_column
                ))
            })?;

        let late_threshold = self.prev_watermark_ms;

        for row in 0..batch.num_rows() {
            let event_time_ms = time_arr.value(row);
            if event_time_ms < late_threshold {
                continue;
            }
            let key = format_key_value(batch, key_idx, row)?;
            let session = self.sessions.entry(key).or_insert_with(|| SessionState {
                session_start_ms: event_time_ms,
                last_event_time_ms: event_time_ms,
                agg: AggState::new(&self.spec.agg_exprs),
            });
            if event_time_ms > session.last_event_time_ms {
                session.last_event_time_ms = event_time_ms;
            }
            session.agg.update(&self.spec.agg_exprs, batch, row)?;
        }

        self.prev_watermark_ms = new_watermark_ms;
        self.flush_closed_sessions(new_watermark_ms)
    }

    /// Flush sessions whose inactivity gap has passed the watermark.
    pub fn flush_closed_sessions(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let gap = self.spec.session_gap_ms as i64;
        let closed: Vec<String> = self
            .sessions
            .keys()
            .filter(|k| self.sessions[*k].last_event_time_ms + gap <= watermark_ms)
            .cloned()
            .collect();
        if closed.is_empty() {
            return Ok(vec![]);
        }
        let mut output = Vec::with_capacity(closed.len());
        for key in closed {
            if let Some(s) = self.sessions.remove(&key) {
                output.push(self.build_output_batch(
                    &key,
                    s.session_start_ms,
                    s.last_event_time_ms + gap,
                    &s.agg,
                )?);
            }
        }
        Ok(output)
    }

    fn build_output_batch(
        &self,
        key_value: &str,
        session_start_ms: i64,
        session_end_ms: i64,
        state: &AggState,
    ) -> ExecResult<RecordBatch> {
        let mut fields = vec![
            Field::new(&self.spec.key_column, DataType::Utf8, false),
            Field::new("session_start_ms", DataType::Int64, false),
            Field::new("session_end_ms", DataType::Int64, false),
        ];
        for agg in &self.spec.agg_exprs {
            fields.push(Field::new(&agg.output_column, DataType::Int64, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![key_value])),
            Arc::new(Int64Array::from(vec![session_start_ms])),
            Arc::new(Int64Array::from(vec![session_end_ms])),
        ];
        for (i, agg) in self.spec.agg_exprs.iter().enumerate() {
            columns.push(Arc::new(Int64Array::from(vec![
                state.finalized_value(i, agg),
            ])));
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}
