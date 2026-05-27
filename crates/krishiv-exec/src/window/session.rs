use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::aggregate::{AggExpr, AggState};
use crate::join::format_key_value;
use crate::{ExecError, ExecResult};

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

    /// Persist open sessions to `StateBackend`.
    ///
    /// Clears the namespace first so that stale entries for already-closed
    /// sessions are removed and cannot be re-opened on checkpoint restore.
    pub fn persist_to_state(
        &self,
        backend: &mut dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        // Remove all previously persisted entries so closed sessions don't
        // survive into the next checkpoint snapshot.
        backend.clear_namespace(namespace)?;

        if self.sessions.is_empty() {
            return Ok(());
        }

        let op_id = namespace.operator_id();
        let name = namespace.state_name();
        let mut state_keys = Vec::with_capacity(self.sessions.len());
        let mut values = Vec::with_capacity(self.sessions.len());
        for (key, session) in &self.sessions {
            let payload = serde_json::json!({
                "session_start_ms": session.session_start_ms,
                "last_event_time_ms": session.last_event_time_ms,
                "values": session.agg.values,
                "has_value": session.agg.has_value,
                "avg_sums": session.agg.avg_sums,
                "avg_counts": session.agg.avg_counts,
            });
            let bytes = serde_json::to_vec(&payload).map_err(|e| StateError::CorruptEntry {
                message: e.to_string(),
            })?;
            // GAP-18: length-prefix encoding.
            // Format: b"ses:" | key_len_le_u32 | key_bytes | session_start_le_i64 | last_event_le_i64
            let key_bytes_slice = key.as_bytes();
            let mut state_key = Vec::with_capacity(4 + 4 + key_bytes_slice.len() + 16);
            state_key.extend_from_slice(b"ses:");
            state_key.extend_from_slice(&(key_bytes_slice.len() as u32).to_le_bytes());
            state_key.extend_from_slice(key_bytes_slice);
            state_key.extend_from_slice(&session.session_start_ms.to_le_bytes());
            state_key.extend_from_slice(&session.last_event_time_ms.to_le_bytes());
            state_keys.push(state_key);
            values.push(bytes);
        }
        let batch_entries: Vec<(&str, &str, &[u8], &[u8])> = state_keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (op_id, name, k.as_slice(), v.as_slice()))
            .collect();
        backend.put_batch(&batch_entries)?;
        Ok(())
    }

    /// Restore open sessions from `StateBackend`.
    pub fn restore_from_state(
        &mut self,
        backend: &dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        let mut restored = HashMap::new();
        for key_bytes in backend.list_keys(namespace)? {
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let parsed: serde_json::Value =
                serde_json::from_slice(&payload).map_err(|e| StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
            let session_start_ms = parsed["session_start_ms"].as_i64().unwrap_or(0);
            let last_event_time_ms = parsed["last_event_time_ms"].as_i64().unwrap_or(0);
            let values: Vec<i64> = parsed["values"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
                .unwrap_or_default();
            let has_value: Vec<bool> = parsed["has_value"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_bool()).collect())
                .unwrap_or_default();
            let avg_sums: Vec<f64> = parsed["avg_sums"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
                .unwrap_or_default();
            let avg_counts: Vec<u64> = parsed["avg_counts"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default();
            if let Some(key) = parse_session_state_key(&key_bytes) {
                restored.insert(
                    key,
                    SessionState {
                        session_start_ms,
                        last_event_time_ms,
                        agg: AggState {
                            values,
                            has_value,
                            avg_sums,
                            avg_counts,
                        },
                    },
                );
            }
        }
        self.sessions = restored;
        Ok(())
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
        let gap = self.spec.session_gap_ms as i64;
        let mut output = Vec::new();

        for row in 0..batch.num_rows() {
            let event_time_ms = time_arr.value(row);
            if event_time_ms < late_threshold {
                continue;
            }
            let key = format_key_value(batch, key_idx, row)?;
            if let Some(existing) = self.sessions.get(&key)
                && event_time_ms > existing.last_event_time_ms.saturating_add(gap)
                && let Some(s) = self.sessions.remove(&key)
            {
                output.push(self.build_output_batch(
                    &key,
                    s.session_start_ms,
                    s.last_event_time_ms + gap,
                    &s.agg,
                )?);
            }
            let session = self.sessions.entry(key).or_insert_with(|| SessionState {
                session_start_ms: event_time_ms,
                last_event_time_ms: event_time_ms,
                agg: AggState::new(&self.spec.agg_exprs),
            });
            if event_time_ms < session.session_start_ms {
                session.session_start_ms = event_time_ms;
            }
            if event_time_ms > session.last_event_time_ms {
                session.last_event_time_ms = event_time_ms;
            }
            session.agg.update(&self.spec.agg_exprs, batch, row)?;
        }

        self.prev_watermark_ms = new_watermark_ms;
        output.extend(self.flush_closed_sessions(new_watermark_ms)?);
        Ok(output)
    }

    /// Flush sessions whose inactivity gap has passed the watermark.
    pub fn flush_closed_sessions(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let gap = self.spec.session_gap_ms as i64;
        // Use saturating_add to prevent i64 overflow when last_event_time_ms is
        // near i64::MAX (e.g. from a malformed event).  An overflow would wrap
        // to a negative value, making every session appear closed spuriously.
        let closed: Vec<String> = self
            .sessions
            .keys()
            .filter(|k| {
                self.sessions[*k]
                    .last_event_time_ms
                    .saturating_add(gap)
                    <= watermark_ms
            })
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

fn parse_session_state_key(bytes: &[u8]) -> Option<String> {
    // GAP-18: length-prefix format.
    // Format: b"ses:" | key_len_le_u32 | key_bytes | session_start_le_i64 | last_event_le_i64
    const PREFIX: &[u8] = b"ses:";
    if !bytes.starts_with(PREFIX) {
        return None;
    }
    let rest = &bytes[PREFIX.len()..];
    let key_len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(rest.get(4..4 + key_len)?).ok()?.to_string();
    Some(key)
}

#[cfg(test)]
mod session_state_tests {
    use super::*;
    use crate::aggregate::AggFunction;
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_state::{InMemoryStateBackend, Namespace};

    #[test]
    fn session_state_persist_and_restore_roundtrip() {
        let spec = SessionWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
        };
        let mut op = SessionWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        op.process_batch(&batch, 300).expect("process");
        assert_eq!(op.open_session_count(), 1);

        let mut backend = InMemoryStateBackend::new();
        let ns = Namespace::new("op-session", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = SessionWindowOperator::new(SessionWindowSpec {
            key_column: "k".into(),
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                input_column: "v".into(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
        });
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert_eq!(restored.open_session_count(), 1);
    }

    #[test]
    fn session_state_parse_key() {
        // GAP-18: use length-prefix encoding
        let key_str = "mykey";
        let key_bytes = key_str.as_bytes();
        let mut key = Vec::from(b"ses:");
        key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&100i64.to_le_bytes());
        key.extend_from_slice(&200i64.to_le_bytes());
        let k = parse_session_state_key(&key).unwrap();
        assert_eq!(k, "mykey");
    }

    #[test]
    fn session_state_parse_key_with_embedded_null() {
        // GAP-18: keys with null bytes must parse correctly.
        let key_str = "user\x00id";
        let key_bytes = key_str.as_bytes();
        let mut key = Vec::from(b"ses:");
        key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(key_bytes);
        key.extend_from_slice(&100i64.to_le_bytes());
        key.extend_from_slice(&200i64.to_le_bytes());
        let k = parse_session_state_key(&key).unwrap();
        assert_eq!(k, "user\x00id");
    }

    #[test]
    fn session_state_parse_key_bad_prefix_returns_none() {
        let key = b"tw:other";
        assert!(parse_session_state_key(key).is_none());
    }
}
