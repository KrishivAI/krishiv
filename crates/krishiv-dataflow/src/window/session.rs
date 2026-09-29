use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_common::MemoryBudget;
use krishiv_state::{Namespace, StateBackend, StateError, StateResult};

use crate::aggregate::{AggEntry, AggExpr, AggFunction, AggState};
use crate::join::extract_agg_key;
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
    /// Arrow type of the key column: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
    /// Defaults to `"utf8"`.
    pub key_column_type: String,
    /// Source columns behind a composite key; empty for a single-column key.
    pub key_parts: Vec<krishiv_plan::window::KeyPart>,
    /// Suppress the key in output: the key exists only to satisfy the keyed
    /// machinery (global aggregation, task #140) and the user never named it.
    pub key_is_synthetic: bool,
    /// Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Inactivity gap that closes the session in milliseconds.
    pub session_gap_ms: u64,
    /// Aggregate expressions to apply within each session.
    pub agg_exprs: Vec<AggExpr>,
    /// Per-aggregate float flag: `true` when the aggregate input column is
    /// `Float64`.  Positions beyond this slice default to `false` (Int64 output).
    pub agg_is_float: Vec<bool>,
}

pub(crate) struct SessionState {
    pub(crate) session_start_ms: i64,
    pub(crate) last_event_time_ms: i64,
    pub(crate) agg: AggState,
}

/// Session event-time window operator (R5.2).
///
/// **Memory bound**: `sessions` holds one open [`SessionState`] per key until
/// inactivity exceeding the session gap closes it (flushed and removed on
/// watermark advance). There is no key-eviction or TTL beyond the session-gap
/// closure itself, so memory is bounded by the number of keys with an
/// in-flight session at any instant. Deployments with very high-cardinality
/// or long-lived keys should choose a session gap and watermark lag that keep
/// this bounded, and pre-aggregate/filter keys upstream where cardinality is
/// unbounded.
pub struct SessionWindowOperator {
    spec: SessionWindowSpec,
    // Keyed by serialised key value; each key's open sessions are disjoint
    // (separated by at least one gap) and sorted by start.
    sessions: HashMap<String, Vec<SessionState>>,
    prev_watermark_ms: i64,
    /// Total late events dropped by this operator since creation.
    pub late_events_dropped: u64,
    /// Output schema, fixed for the operator's lifetime; cached so closed
    /// sessions don't rebuild `Schema`/`Field` vectors per row.
    output_schema: Arc<Schema>,
    memory_budget: Option<Arc<MemoryBudget>>,
    /// Cached key column index (resolved on first batch, reused thereafter).
    cached_key_idx: Option<usize>,
    /// Cached event-time column index (resolved on first batch, reused thereafter).
    cached_time_idx: Option<usize>,
}

fn build_session_output_schema(spec: &SessionWindowSpec) -> Arc<Schema> {
    // A synthetic key was named by nobody and must appear nowhere: a global
    // session aggregate emits session bounds and aggregates only.
    let mut fields: Vec<Field> = if spec.key_is_synthetic {
        Vec::new()
    } else if spec.key_parts.is_empty() {
        vec![Field::new(
            &spec.key_column,
            key_type_to_data_type(&spec.key_column_type),
            false,
        )]
    } else {
        spec.key_parts
            .iter()
            .map(|p| Field::new(&p.name, key_type_to_data_type(&p.type_tag), false))
            .collect()
    };
    fields.extend([
        Field::new("session_start_ms", DataType::Int64, false),
        Field::new("session_end_ms", DataType::Int64, false),
    ]);
    for (i, agg) in spec.agg_exprs.iter().enumerate() {
        let dtype = match agg.function {
            AggFunction::Avg | AggFunction::Stddev => DataType::Float64,
            _ if spec.agg_is_float.get(i).copied().unwrap_or(false) => DataType::Float64,
            _ => DataType::Int64,
        };
        fields.push(Field::new(&agg.output_column, dtype, false));
    }
    Arc::new(Schema::new(fields))
}

impl SessionWindowOperator {
    /// Create a new session window operator.
    pub fn new(spec: SessionWindowSpec) -> Self {
        let output_schema = build_session_output_schema(&spec);
        Self {
            spec,
            sessions: HashMap::new(),
            prev_watermark_ms: i64::MIN,
            late_events_dropped: 0,
            output_schema,
            memory_budget: None,
            cached_key_idx: None,
            cached_time_idx: None,
        }
    }

    /// Seed the late-event threshold from an upstream stage's output watermark
    /// (GAP-WATERMARK).
    ///
    /// Audit: `prev_watermark_ms` starts at `i64::MIN`, so a stage that is not
    /// the first in its job accepted events the upstream stage had already
    /// declared late — it reported "no late events" by construction and
    /// `allowed_lateness` never engaged. Takes the `max` so a watermark
    /// restored from a checkpoint is never walked backwards, and `i64::MIN`
    /// (no hint) is a no-op.
    pub fn seed_initial_watermark(&mut self, watermark_ms: i64) {
        self.prev_watermark_ms = self.prev_watermark_ms.max(watermark_ms);
    }

    /// Attach a shared memory budget.  Each new session entry reserves ~128 bytes;
    /// the reservation is released when the session closes.
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<MemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Number of open sessions.
    pub fn open_session_count(&self) -> usize {
        self.sessions.values().map(Vec::len).sum()
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
        let open = self.open_session_count();
        let mut state_keys = Vec::with_capacity(open);
        let mut values = Vec::with_capacity(open);
        let all_sessions = self
            .sessions
            .iter()
            .flat_map(|(key, list)| list.iter().map(move |session| (key, session)));
        for (key, session) in all_sessions {
            let mut payload = serde_json::json!({
                "session_start_ms": session.session_start_ms,
                "last_event_time_ms": session.last_event_time_ms,
                "values":       session.agg.entries.iter().map(|e| e.value).collect::<Vec<_>>(),
                "has_value":    session.agg.entries.iter().map(|e| e.has_value).collect::<Vec<_>>(),
                "avg_sums":     session.agg.entries.iter().map(|e| e.avg_sum).collect::<Vec<_>>(),
                "avg_counts":   session.agg.entries.iter().map(|e| e.avg_count).collect::<Vec<_>>(),
                "float_values": session.agg.entries.iter().map(|e| e.float_value).collect::<Vec<_>>(),
                "sq_sums":      session.agg.entries.iter().map(|e| e.sq_sum).collect::<Vec<_>>(),
            });
            // COUNT(DISTINCT) sets, written ONLY when one is non-empty — the
            // same opt-in rule as AGG_STATE_BINARY_V2 in state_persistence.rs,
            // and for the same reason: queries that never use DISTINCT keep
            // producing byte-identical checkpoints. Omitting this field was a
            // real defect (register §50): after restore the set came back
            // empty, so the count both under-counted the restored window and
            // re-counted values it had already seen.
            if session.agg.distinct.iter().any(|set| !set.is_empty()) {
                // `payload` is the object literal built just above; the if-let
                // is for clippy's indexing lint, not a reachable branch.
                if let serde_json::Value::Object(map) = &mut payload {
                    map.insert(
                        "distinct".into(),
                        serde_json::json!(
                            session
                                .agg
                                .distinct
                                .iter()
                                .map(|set| set.iter().collect::<Vec<_>>())
                                .collect::<Vec<_>>()
                        ),
                    );
                }
            }
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
        super::state_persistence::persist_operator_watermark_ms(
            backend,
            namespace,
            self.prev_watermark_ms,
        )
    }

    /// Restore open sessions from `StateBackend`.
    pub fn restore_from_state(
        &mut self,
        backend: &dyn StateBackend,
        namespace: &Namespace,
    ) -> StateResult<()> {
        let mut restored: HashMap<String, Vec<SessionState>> = HashMap::new();
        for key_bytes in backend.list_keys(namespace)? {
            if key_bytes.get(..4).is_none_or(|p| p != b"ses:") {
                continue;
            }
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let parsed: serde_json::Value =
                serde_json::from_slice(&payload).map_err(|e| StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
            let session_start_ms = parsed
                .get("session_start_ms")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| StateError::CorruptEntry {
                    message: "missing or invalid session_start_ms".into(),
                })?;
            let last_event_time_ms = parsed
                .get("last_event_time_ms")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| StateError::CorruptEntry {
                    message: "missing or invalid last_event_time_ms".into(),
                })?;
            let values: Vec<i64> = parsed
                .get("values")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_i64()).collect())
                .unwrap_or_default();
            let has_value: Vec<bool> = parsed
                .get("has_value")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_bool()).collect())
                .unwrap_or_default();
            let avg_sums: Vec<f64> = parsed
                .get("avg_sums")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
                .unwrap_or_default();
            let avg_counts: Vec<u64> = parsed
                .get("avg_counts")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
                .unwrap_or_default();
            let float_values: Vec<f64> = parsed
                .get("float_values")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
                .unwrap_or_default();
            let sq_sums: Vec<f64> = parsed
                .get("sq_sums")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_f64()).collect())
                .unwrap_or_default();
            if let Some(key) = parse_session_state_key(&key_bytes) {
                let n = values.len();
                let entries: Vec<AggEntry> = (0..n)
                    .map(|i| AggEntry {
                        value: values.get(i).copied().unwrap_or(0),
                        has_value: has_value.get(i).copied().unwrap_or(false),
                        avg_sum: avg_sums.get(i).copied().unwrap_or(0.0),
                        avg_count: avg_counts.get(i).copied().unwrap_or(0),
                        float_value: float_values.get(i).copied().unwrap_or(0.0),
                        sq_sum: sq_sums.get(i).copied().unwrap_or(0.0),
                    })
                    .collect();
                let mut agg = AggState::from_entries(entries);
                // Absent field = a snapshot from a query with no DISTINCT (or
                // written before the field existed); empty sets are the
                // correct reading. Present-but-malformed is corruption and
                // must fail loudly, not silently degrade to empty sets.
                if let Some(raw) = parsed.get("distinct") {
                    let sets: Vec<std::collections::BTreeSet<String>> =
                        serde_json::from_value(raw.clone()).map_err(|e| {
                            StateError::CorruptEntry {
                                message: format!("invalid distinct sets: {e}"),
                            }
                        })?;
                    if sets.len() != n {
                        return Err(StateError::CorruptEntry {
                            message: format!(
                                "distinct set count {} does not match {} aggregates",
                                sets.len(),
                                n
                            ),
                        });
                    }
                    agg.distinct = sets;
                }
                restored.entry(key).or_default().push(SessionState {
                    session_start_ms,
                    last_event_time_ms,
                    agg,
                });
            }
        }
        for list in restored.values_mut() {
            list.sort_by_key(|session| session.session_start_ms);
        }
        self.sessions = restored;
        if let Some(wm) =
            super::state_persistence::restore_operator_watermark_ms(backend, namespace)?
        {
            self.prev_watermark_ms = wm;
        }
        Ok(())
    }

    /// Process one `RecordBatch`, returning closed session outputs.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        new_watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        // Resolve and cache the column indices on the first call.
        let key_idx = match self.cached_key_idx {
            Some(idx) => idx,
            None => {
                let idx = batch
                    .schema()
                    .index_of(&self.spec.key_column)
                    .map_err(|_| ExecError::ColumnNotFound(self.spec.key_column.clone()))?;
                self.cached_key_idx = Some(idx);
                idx
            }
        };
        let time_idx = match self.cached_time_idx {
            Some(idx) => idx,
            None => {
                let idx = batch
                    .schema()
                    .index_of(&self.spec.event_time_column)
                    .map_err(|_| ExecError::ColumnNotFound(self.spec.event_time_column.clone()))?;
                self.cached_time_idx = Some(idx);
                idx
            }
        };

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
        let gap = i64::try_from(self.spec.session_gap_ms).unwrap_or(i64::MAX);

        // Pre-downcast the aggregate input columns once for the whole batch so
        // the per-row update avoids a `schema().index_of()` + `downcast_ref()`.
        let pre_cols = crate::aggregate::downcast_agg_input_cols(batch, &self.spec.agg_exprs)?;

        // STREAM-2: Sort rows by event time before processing. Correctness no
        // longer depends on it (sessions merge whatever the arrival order),
        // but in-order arrival merges less.
        let mut row_order: Vec<usize> = (0..batch.num_rows()).collect();
        row_order.sort_unstable_by_key(|&r| time_arr.value(r));

        for &row in &row_order {
            let event_time_ms = time_arr.value(row);
            if event_time_ms < late_threshold {
                self.late_events_dropped = self.late_events_dropped.saturating_add(1);
                continue;
            }
            let key = extract_agg_key(batch, key_idx, row)?.to_string();

            // The event's own session, then every open session of this key
            // within one gap of it merged in: an admitted out-of-order event
            // can bridge two sessions, and must not stretch one it is more
            // than a gap away from. Sessions close only when the watermark
            // passes their end (`flush_closed_sessions`), never because a
            // later event arrived — an earlier one may still be admitted.
            let mut merged = SessionState {
                session_start_ms: event_time_ms,
                last_event_time_ms: event_time_ms,
                agg: AggState::new(&self.spec.agg_exprs),
            };
            merged
                .agg
                .update_pre(&self.spec.agg_exprs, &pre_cols, row)?;
            let list = self.sessions.entry(key).or_default();
            let mut absorbed = 0usize;
            let mut kept = Vec::with_capacity(list.len() + 1);
            for session in list.drain(..) {
                let touches = event_time_ms < session.last_event_time_ms.saturating_add(gap)
                    && session.session_start_ms < event_time_ms.saturating_add(gap);
                if touches {
                    merged.session_start_ms = merged.session_start_ms.min(session.session_start_ms);
                    merged.last_event_time_ms =
                        merged.last_event_time_ms.max(session.last_event_time_ms);
                    merged.agg.merge(&session.agg, &self.spec.agg_exprs)?;
                    absorbed += 1;
                } else {
                    kept.push(session);
                }
            }
            kept.push(merged);
            kept.sort_by_key(|session| session.session_start_ms);
            *list = kept;

            // Budget: ~128 bytes per open session.
            if let Some(budget) = &self.memory_budget {
                if absorbed == 0 {
                    if !budget.try_reserve(128) {
                        return Err(ExecError::Oom(format!(
                            "session window exceeded memory budget ({} bytes used, limit {} bytes)",
                            budget.used_bytes(),
                            budget.limit().unwrap_or(0),
                        )));
                    }
                } else if absorbed > 1 {
                    budget.release(128 * (absorbed as u64 - 1));
                }
            }
        }

        let mut output = Vec::new();
        if new_watermark_ms >= self.prev_watermark_ms {
            self.prev_watermark_ms = new_watermark_ms;
        }
        output.extend(self.flush_closed_sessions(new_watermark_ms)?);
        Ok(output)
    }

    /// Flush sessions whose inactivity gap has passed the watermark.
    ///
    /// S-1: emits all closed sessions as a single multi-row RecordBatch, sorted
    /// by `(session_start_ms, key)` for deterministic output.
    pub fn flush_closed_sessions(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let gap = i64::try_from(self.spec.session_gap_ms).unwrap_or(i64::MAX);
        // Use saturating_add to prevent i64 overflow when last_event_time_ms is
        // near i64::MAX (e.g. from a malformed event).  An overflow would wrap
        // to a negative value, making every session appear closed spuriously.
        let is_closed =
            |session: &SessionState| session.last_event_time_ms.saturating_add(gap) <= watermark_ms;
        let mut closed: Vec<(&String, &SessionState)> = self
            .sessions
            .iter()
            .flat_map(|(key, list)| list.iter().map(move |session| (key, session)))
            .filter(|(_, session)| is_closed(session))
            .collect();
        if closed.is_empty() {
            return Ok(vec![]);
        }
        // Sort by (session_start_ms, key) for determinism.
        closed.sort_by(|(ka, a), (kb, b)| {
            a.session_start_ms
                .cmp(&b.session_start_ms)
                .then_with(|| ka.cmp(kb))
        });
        let mut keys = Vec::with_capacity(closed.len());
        let mut starts = Vec::with_capacity(closed.len());
        let mut ends = Vec::with_capacity(closed.len());
        let mut states = Vec::with_capacity(closed.len());
        // STREAM-8: Build the output batch BEFORE removing sessions from state.
        // If batch construction fails, sessions must remain so they aren't lost.
        for (key, session) in &closed {
            ends.push(session.last_event_time_ms.saturating_add(gap));
            starts.push(session.session_start_ms);
            keys.push((*key).clone());
            states.push(&session.agg);
        }
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let batch = self.build_multi_row_output_batch(&key_refs, &starts, &ends, &states)?;
        // Only now that the batch is built, remove the closed sessions.
        let removed = closed.len() as u64;
        for list in self.sessions.values_mut() {
            list.retain(|session| !is_closed(session));
        }
        self.sessions.retain(|_, list| !list.is_empty());
        if let Some(budget) = &self.memory_budget {
            budget.release(128 * removed);
        }
        Ok(vec![batch])
    }

    fn build_multi_row_output_batch(
        &self,
        keys: &[&str],
        session_starts: &[i64],
        session_ends: &[i64],
        states: &[&AggState],
    ) -> ExecResult<RecordBatch> {
        let n = keys.len();
        debug_assert_eq!(session_starts.len(), n);
        debug_assert_eq!(session_ends.len(), n);
        debug_assert_eq!(states.len(), n);

        let schema = Arc::clone(&self.output_schema);
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(3 + self.spec.agg_exprs.len());

        if self.spec.key_is_synthetic {
            // No key columns: the synthetic key exists only inside the
            // accumulator map.
        } else if self.spec.key_parts.is_empty() {
            columns.push(key_values_to_typed_column(
                &self.spec.key_column_type,
                keys,
            )?);
        } else {
            // One encoded key per row becomes N typed columns, transposed.
            let parts = self.spec.key_parts.len();
            let mut per_part: Vec<Vec<String>> = vec![Vec::with_capacity(keys.len()); parts];
            for k in keys {
                let decoded = crate::scalar_expr::split_composite_key(k, parts)?;
                for (slot, v) in per_part.iter_mut().zip(decoded) {
                    slot.push(v);
                }
            }
            for (p, vals) in self.spec.key_parts.iter().zip(per_part) {
                let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
                columns.push(key_values_to_typed_column(&p.type_tag, &refs)?);
            }
        }
        columns.push(Arc::new(Int64Array::from(session_starts.to_vec())));
        columns.push(Arc::new(Int64Array::from(session_ends.to_vec())));

        for (i, agg) in self.spec.agg_exprs.iter().enumerate() {
            let is_float = self.spec.agg_is_float.get(i).copied().unwrap_or(false);
            match agg.function {
                AggFunction::Avg => {
                    let vals: ExecResult<Vec<f64>> =
                        states.iter().map(|s| s.finalized_avg(i)).collect();
                    columns.push(Arc::new(Float64Array::from(vals?)));
                }
                AggFunction::Stddev => {
                    let vals: ExecResult<Vec<f64>> =
                        states.iter().map(|s| s.finalized_stddev(i)).collect();
                    columns.push(Arc::new(Float64Array::from(vals?)));
                }
                _ if is_float => {
                    let vals: ExecResult<Vec<f64>> = states
                        .iter()
                        .map(|s| s.finalized_float_value(i, agg))
                        .collect();
                    columns.push(Arc::new(Float64Array::from(vals?)));
                }
                _ => {
                    let vals: ExecResult<Vec<i64>> =
                        states.iter().map(|s| s.finalized_value(i, agg)).collect();
                    columns.push(Arc::new(Int64Array::from(vals?)));
                }
            }
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
    let rest = bytes.get(PREFIX.len()..)?;
    let key_len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
    let key = std::str::from_utf8(rest.get(4..4 + key_len)?)
        .ok()?
        .to_string();
    Some(key)
}

fn key_type_to_data_type(key_type: &str) -> DataType {
    match key_type {
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn key_values_to_typed_column(
    key_type: &str,
    key_values: &[&str],
) -> crate::ExecResult<Arc<dyn arrow::array::Array>> {
    match key_type {
        "int32" => {
            let vals: ExecResult<Vec<i32>> = key_values
                .iter()
                .map(|v| {
                    v.parse::<i32>().map_err(|_| {
                        ExecError::InvalidInput(format!(
                            "session key '{v}' cannot be parsed as int32"
                        ))
                    })
                })
                .collect();
            Ok(Arc::new(Int32Array::from(vals?)))
        }
        "int64" => {
            let vals: ExecResult<Vec<i64>> = key_values
                .iter()
                .map(|v| {
                    v.parse::<i64>().map_err(|_| {
                        ExecError::InvalidInput(format!(
                            "session key '{v}' cannot be parsed as int64"
                        ))
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(vals?)))
        }
        "float64" => {
            let vals: ExecResult<Vec<f64>> = key_values
                .iter()
                .map(|v| {
                    v.parse::<f64>().map_err(|_| {
                        ExecError::InvalidInput(format!(
                            "session key '{v}' cannot be parsed as float64"
                        ))
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(vals?)))
        }
        "bool" => {
            let vals: ExecResult<Vec<bool>> = key_values
                .iter()
                .map(|v| {
                    v.parse::<bool>().map_err(|_| {
                        ExecError::InvalidInput(format!(
                            "session key '{v}' cannot be parsed as bool"
                        ))
                    })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(vals?)))
        }
        _ => Ok(Arc::new(StringArray::from(key_values.to_vec()))),
    }
}

#[cfg(test)]
mod session_state_tests {
    use super::*;
    use crate::aggregate::AggFunction;
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_state::{Namespace, RocksDbStateBackend};

    #[test]
    fn session_state_persist_and_restore_roundtrip() {
        let spec = SessionWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
            agg_is_float: vec![false],
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

        let mut backend = RocksDbStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-session", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = SessionWindowOperator::new(SessionWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
            agg_is_float: vec![false],
        });
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert_eq!(restored.open_session_count(), 1);
    }

    /// COUNT(DISTINCT) must survive checkpoint/restore with its SET, not just
    /// its count.
    ///
    /// Session windows persist through their own JSON encoder, which predates
    /// the distinct sets and silently dropped them (register §50): the count
    /// restored as N but the set restored empty, so the next already-seen
    /// value RESET the count to 1 — `value` is re-derived as `set.len()` on
    /// every update. This test feeds {x, y}, checkpoints mid-session,
    /// restores, feeds a duplicate x, and demands the final count still be 2.
    /// Against the pre-fix persist path it fails with 1.
    #[test]
    fn session_count_distinct_survives_checkpoint_restore() {
        let make_spec = || SessionWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "distinct_vals".into(),
                function: AggFunction::CountDistinct,
            }],
            agg_is_float: vec![false],
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = |keys: &[&str], ts: &[i64], vals: &[&str]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(keys.to_vec())),
                    Arc::new(Int64Array::from(ts.to_vec())),
                    Arc::new(StringArray::from(vals.to_vec())),
                ],
            )
            .unwrap()
        };

        let mut op = SessionWindowOperator::new(make_spec());
        op.process_batch(&batch(&["a", "a"], &[100, 200], &["x", "y"]), 200)
            .expect("process");

        let mut backend = RocksDbStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-session-distinct", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = SessionWindowOperator::new(make_spec());
        restored.restore_from_state(&backend, &ns).expect("restore");

        // The duplicate: x was already counted before the checkpoint.
        restored
            .process_batch(&batch(&["a"], &[400], &["x"]), 400)
            .expect("duplicate after restore");

        // Advance the watermark past the gap so the session closes and emits.
        let out = restored
            .process_batch(&batch(&["b"], &[5000], &["z"]), 5000)
            .expect("close session");
        let closed: Vec<&RecordBatch> = out.iter().filter(|b| b.num_rows() > 0).collect();
        assert!(
            !closed.is_empty(),
            "advancing the watermark to 5000 must close session 'a'"
        );
        let emitted = closed[0];
        let agg_col_idx = emitted
            .schema()
            .index_of("distinct_vals")
            .expect("output must carry the aggregate column");
        let counts = emitted
            .column(agg_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column is Int64");
        assert_eq!(
            counts.value(0),
            2,
            "{{x, y}} then a duplicate x is 2 distinct values; 1 means the \
             restored set came back empty and the duplicate reset the count"
        );
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

    #[test]
    fn session_gap_ms_max_u64_does_not_panic() {
        // session_gap_ms = u64::MAX overflows on `as i64` cast; try_from saturates to i64::MAX.
        let spec = SessionWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            event_time_column: "ts".into(),
            session_gap_ms: u64::MAX,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: String::new(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
            agg_is_float: vec![false],
        };
        let mut op = SessionWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![100i64])),
            ],
        )
        .unwrap();
        // Must not panic; session gap = i64::MAX so session never closes.
        let out = op.process_batch(&batch, 1000).unwrap();
        assert!(
            out.is_empty(),
            "session with gap=i64::MAX should not close at watermark 1000"
        );
    }

    fn ts_batch(key: &str, event_time_ms: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Int64Array::from(vec![event_time_ms])),
            ],
        )
        .unwrap()
    }

    /// Regression test: a watermark that decreases between batches must not
    /// move the operator's internal late-event threshold (`prev_watermark_ms`)
    /// backwards. If it did, an event that is genuinely late relative to the
    /// high-water mark already observed could be wrongly accepted by a later
    /// batch, corrupting already-closed session state (the Phase 1 bug this
    /// guards against).
    #[test]
    fn session_window_non_monotonic_watermark_does_not_lower_late_threshold() {
        let spec = SessionWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            event_time_column: "ts".into(),
            session_gap_ms: 1000,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: String::new(),
                output_column: "cnt".into(),
                function: AggFunction::Count,
            }],
            agg_is_float: vec![false],
        };
        let mut op = SessionWindowOperator::new(spec);

        // Batch 1: advance the watermark to 5000.
        op.process_batch(&ts_batch("a", 5000), 5000)
            .expect("process batch1");
        assert_eq!(op.late_events_dropped, 0);

        // Batch 2: a DECREASING watermark (100 < 5000) must not move the
        // operator's internal late-event threshold backwards.
        op.process_batch(&ts_batch("a", 5100), 100)
            .expect("process batch2");
        assert_eq!(op.late_events_dropped, 0);

        // Batch 3: an event at ts=4000 is older than the watermark already
        // established in batch 1 (5000). If the decreasing watermark from
        // batch 2 had corrupted the late threshold down to 100, this event
        // would be wrongly accepted instead of dropped as late.
        op.process_batch(&ts_batch("a", 4000), 5000)
            .expect("process batch3");
        assert_eq!(
            op.late_events_dropped, 1,
            "decreasing watermark must not reopen the late-event threshold"
        );
    }
}
