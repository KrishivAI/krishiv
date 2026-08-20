use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::aggregate::{AggExpr, AggFunction, AggState};
use crate::join::extract_agg_key;
use crate::window::LateEventHandler;
use crate::{ExecError, ExecResult};

// ── TumblingWindowSpec ────────────────────────────────────────────────────────

/// Configuration for a tumbling event-time window operator.
#[derive(Debug, Clone, Default)]
pub struct TumblingWindowSpec {
    /// Name of the column to key by.
    pub key_column: String,
    /// Arrow type of the key column: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
    /// Defaults to `"utf8"`.
    pub key_column_type: String,
    /// Source columns behind a composite key; empty for a single-column key.
    pub key_parts: Vec<krishiv_plan::window::KeyPart>,
    /// Suppress the key in output: the key exists only to satisfy the keyed
    /// machinery (global aggregation, task #140) and the user never named it.
    pub key_is_synthetic: bool,
    /// Per-group bounded top-N over raw rows (task #142). When set,
    /// `agg_exprs` is empty and closed windows emit up to `limit` raw rows
    /// per key instead of one aggregate row.
    pub top_n: Option<krishiv_plan::window::TopNSpec>,
    /// Name of the Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Window duration in milliseconds.
    pub window_size_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
    /// Per-aggregate float flag: `true` when the aggregate input column is `Float64`.
    /// Positions beyond this slice default to `false` (Int64 output).
    pub agg_is_float: Vec<bool>,
}

// ── TumblingWindowOperator ────────────────────────────────────────────────────

/// Tumbling event-time window operator backed by an in-memory accumulation map.
///
/// State structure: `(serialised_key, window_start_ms) → AggState`.
/// Windows are closed and flushed when the watermark reaches their end time.
///
/// **Late-event semantics**: an event is late if its `event_time_ms` is
/// strictly less than the watermark from the *previous* batch (stored as
/// `prev_watermark_ms`).  Events in the current batch are never late relative
/// to the watermark they themselves advance — the caller computes the new
/// watermark from this batch and passes it as `new_watermark_ms`.
///
/// Output schema per closed window:
/// `key_column (Utf8), window_start_ms (Int64), window_end_ms (Int64),
///  …agg output columns (Int64)`.
///
/// **Memory bound**: `accumulators` holds one entry per `(key, window_start)`
/// pair until the watermark closes that window, at which point the entry is
/// flushed and removed. There is no key-eviction or TTL on open windows —
/// memory is bounded by `live_key_cardinality × open_window_count`, which the
/// deployment must keep finite by choosing `window_size_ms` and watermark lag
/// appropriate to the expected key cardinality. Pipelines with unbounded or
/// very high-cardinality keys should reduce `window_size_ms` and/or
/// pre-aggregate/filter keys upstream rather than rely on this operator to
/// bound state.
pub struct TumblingWindowOperator {
    spec: TumblingWindowSpec,
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
    pub late_events_dropped: u64,
    late_event_handler: Option<Box<dyn LateEventHandler>>,
    /// Output schema, fixed for the operator's lifetime; cached so closed
    /// windows don't rebuild `Schema`/`Field` vectors per row.
    output_schema: Arc<Schema>,
    /// Cached column index for the key column. Resolved on the first batch and
    /// reused for every subsequent batch (the operator is single-source, so the
    /// schema is fixed for its lifetime). The `None` arm covers the
    /// "schema not yet observed" state at construction time.
    cached_key_idx: Option<usize>,
    /// Cached column index for the event-time column. Same semantics as
    /// `cached_key_idx`.
    cached_time_idx: Option<usize>,
    /// Top-N mode state: per `(key, window_start)` the best rows so far,
    /// kept sorted best-first and capped at `limit` on every insert, so
    /// state is bounded by construction. Disjoint from `accumulators` — a
    /// spec has aggregates or a top-N, never both (validated in the plan).
    topn: HashMap<(String, i64), Vec<TopNRow>>,
    /// Arrival tie-break: equal ordering values keep arrival order, which
    /// makes output deterministic for a deterministic input stream.
    topn_seq: u64,
    /// Column indices, type tags and output schema for top-N mode, resolved
    /// from the first batch (carry-column types are not in the spec).
    topn_runtime: Option<TopNRuntime>,
}

/// One buffered row in top-N mode.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TopNRow {
    order: crate::join::AggKey,
    seq: u64,
    payload: Vec<crate::join::AggKey>,
}

/// Resolved-per-schema half of top-N mode.
struct TopNRuntime {
    order_idx: usize,
    carry_idxs: Vec<usize>,
    carry_tags: Vec<String>,
    output_schema: Arc<Schema>,
}

impl TumblingWindowOperator {
    /// Create a new operator.
    pub fn new(spec: TumblingWindowSpec) -> Self {
        let output_schema = build_window_output_schema(
            &spec.key_column,
            &spec.key_column_type,
            &spec.agg_exprs,
            &spec.agg_is_float,
            &spec.key_parts,
            spec.key_is_synthetic,
        );
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
            late_events_dropped: 0,
            late_event_handler: None,
            output_schema,
            cached_key_idx: None,
            cached_time_idx: None,
            topn: HashMap::new(),
            topn_seq: 0,
            topn_runtime: None,
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

    /// Validate the spec before accepting it.
    pub fn validate_spec(spec: &TumblingWindowSpec) -> ExecResult<()> {
        if spec.window_size_ms == 0 {
            return Err(ExecError::InvalidWindowConfig(
                "tumbling window_size_ms must be non-zero".into(),
            ));
        }
        if spec.window_size_ms > i64::MAX as u64 {
            return Err(ExecError::InvalidWindowConfig(format!(
                "tumbling window_size_ms ({}) exceeds i64::MAX",
                spec.window_size_ms,
            )));
        }
        Ok(())
    }

    fn window_start(event_time_ms: i64, window_size_ms: u64) -> i64 {
        // validate_spec ensures window_size_ms <= i64::MAX, so this cast is safe.
        let size = window_size_ms as i64;
        let q = event_time_ms / size;
        let r = event_time_ms % size;
        // Use saturating_mul to avoid panic in debug and wrapping in release for
        // very large negative timestamps combined with large window sizes.
        if r < 0 {
            q.saturating_sub(1).saturating_mul(size)
        } else {
            q.saturating_mul(size)
        }
    }

    /// Attach a late-event handler that receives each dropped late event.
    pub fn with_late_event_handler(mut self, handler: Box<dyn LateEventHandler>) -> Self {
        self.late_event_handler = Some(handler);
        self
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// Persist open window accumulators to `StateBackend` (GAP-I2).
    ///
    /// Clears the namespace first so that stale entries for windows that have
    /// already been flushed (closed) are removed.  Without this, closed windows
    /// would accumulate in the backend across checkpoint cycles and be
    /// incorrectly re-opened on restore, causing double-emission.
    /// Resolve top-N column indices, carry type tags, and the output schema
    /// from the first observed batch.
    fn ensure_topn_runtime(
        &mut self,
        batch: &RecordBatch,
        top_n: &krishiv_plan::window::TopNSpec,
    ) -> ExecResult<()> {
        if self.topn_runtime.is_some() {
            return Ok(());
        }
        let schema = batch.schema();
        let order_idx = schema
            .index_of(&top_n.order_column)
            .map_err(|_| ExecError::ColumnNotFound(top_n.order_column.clone()))?;
        let mut carry_idxs = Vec::with_capacity(top_n.carry_columns.len());
        let mut carry_tags = Vec::with_capacity(top_n.carry_columns.len());
        let mut carry_fields = Vec::with_capacity(top_n.carry_columns.len());
        for name in &top_n.carry_columns {
            let idx = schema
                .index_of(name)
                .map_err(|_| ExecError::ColumnNotFound(name.clone()))?;
            let dt = schema.field(idx).data_type();
            let tag = crate::stream_driver::key_tag_for_arrow_type(dt).ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "top-N carry column '{name}' has unsupported type {dt}"
                ))
            })?;
            carry_idxs.push(idx);
            carry_tags.push(tag.to_string());
            carry_fields.push(Field::new(name, dt.clone(), false));
        }

        let mut fields: Vec<Field> = if self.spec.key_is_synthetic {
            Vec::new()
        } else if self.spec.key_parts.is_empty() {
            vec![Field::new(
                &self.spec.key_column,
                key_type_to_arrow_data_type(&self.spec.key_column_type),
                false,
            )]
        } else {
            self.spec
                .key_parts
                .iter()
                .map(|p| Field::new(&p.name, key_type_to_arrow_data_type(&p.type_tag), false))
                .collect()
        };
        fields.extend([
            Field::new("window_start_ms", DataType::Int64, false),
            Field::new("window_end_ms", DataType::Int64, false),
        ]);
        fields.extend(carry_fields);
        self.topn_runtime = Some(TopNRuntime {
            order_idx,
            carry_idxs,
            carry_tags,
            output_schema: Arc::new(Schema::new(fields)),
        });
        Ok(())
    }

    /// Insert one row into its group's bounded buffer.
    ///
    /// The buffer is kept sorted best-first and truncated to `limit` on every
    /// insert, so per-group state never exceeds `limit` rows — the bound is
    /// structural, not a cap that something must remember to enforce.
    fn insert_topn_row(
        &mut self,
        batch: &RecordBatch,
        row: usize,
        key: String,
        win_start: i64,
        top_n: &krishiv_plan::window::TopNSpec,
    ) -> ExecResult<()> {
        let rt = self
            .topn_runtime
            .as_ref()
            .ok_or_else(|| ExecError::InvalidInput("top-N runtime not resolved".into()))?;
        let order = extract_agg_key(batch, rt.order_idx, row)?;
        let payload = rt
            .carry_idxs
            .iter()
            .map(|&idx| extract_agg_key(batch, idx, row))
            .collect::<ExecResult<Vec<_>>>()?;
        let descending = top_n.descending;
        let limit = top_n.limit as usize;
        let seq = self.topn_seq;
        self.topn_seq = self.topn_seq.wrapping_add(1);
        let entry = TopNRow {
            order,
            seq,
            payload,
        };
        let rows = self.topn.entry((key, win_start)).or_default();
        // Best-first comparison: DESC = greater order value first; ties keep
        // arrival order.
        let better = |a: &TopNRow, b: &TopNRow| -> std::cmp::Ordering {
            let ord = if descending {
                b.order.cmp(&a.order)
            } else {
                a.order.cmp(&b.order)
            };
            ord.then(a.seq.cmp(&b.seq))
        };
        let pos = rows
            .binary_search_by(|probe| better(probe, &entry))
            .unwrap_or_else(|p| p);
        if pos < limit {
            rows.insert(pos, entry);
            rows.truncate(limit);
        }
        Ok(())
    }

    /// Close and emit top-N buckets whose window end passed the watermark.
    fn flush_closed_topn_windows(
        &mut self,
        watermark_ms: i64,
        size: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        let mut closed: Vec<(String, i64)> = self
            .topn
            .keys()
            .filter(|(_, win_start)| win_start.saturating_add(size) <= watermark_ms)
            .cloned()
            .collect();
        if closed.is_empty() {
            return Ok(vec![]);
        }
        closed.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));
        let mut out = Vec::with_capacity(closed.len());
        for bucket in closed {
            if let Some(rows) = self.topn.remove(&bucket) {
                out.push(self.build_topn_batch(&bucket.0, bucket.1, &rows)?);
            }
        }
        Ok(out)
    }

    /// Build the multi-row output batch for one closed top-N bucket.
    fn build_topn_batch(
        &self,
        key_value: &str,
        window_start_ms: i64,
        rows: &[TopNRow],
    ) -> ExecResult<RecordBatch> {
        let rt = self
            .topn_runtime
            .as_ref()
            .ok_or_else(|| ExecError::InvalidInput("top-N runtime not resolved".into()))?;
        let n = rows.len();
        let window_end_ms = window_start_ms.saturating_add(self.spec.window_size_ms as i64);
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        if self.spec.key_is_synthetic {
            // No key columns.
        } else if self.spec.key_parts.is_empty() {
            let keys: Vec<&str> = std::iter::repeat_n(key_value, n).collect();
            columns.push(key_values_to_typed_column_tumbling(
                &self.spec.key_column_type,
                &keys,
            )?);
        } else {
            let parts = self.spec.key_parts.len();
            let decoded = crate::scalar_expr::split_composite_key(key_value, parts)?;
            for (p, v) in self.spec.key_parts.iter().zip(decoded.iter()) {
                let vals: Vec<&str> = std::iter::repeat_n(v.as_str(), n).collect();
                columns.push(key_values_to_typed_column_tumbling(&p.type_tag, &vals)?);
            }
        }
        columns.push(Arc::new(Int64Array::from(vec![window_start_ms; n])));
        columns.push(Arc::new(Int64Array::from(vec![window_end_ms; n])));

        for (col_pos, tag) in rt.carry_tags.iter().enumerate() {
            let vals: Vec<&crate::join::AggKey> = rows
                .iter()
                .map(|r| {
                    r.payload.get(col_pos).ok_or_else(|| {
                        ExecError::InvalidInput(format!(
                            "top-N buffer corrupt: row has {} payload values, column {col_pos} \
                             requested",
                            r.payload.len()
                        ))
                    })
                })
                .collect::<ExecResult<Vec<_>>>()?;
            columns.push(agg_keys_to_typed_array(tag, &vals)?);
        }
        Ok(RecordBatch::try_new(
            Arc::clone(&rt.output_schema),
            columns,
        )?)
    }

    pub fn persist_to_state(
        &self,
        backend: &mut dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
        super::state_persistence::persist_window_accumulators(
            backend,
            namespace,
            &self.accumulators,
            b"tw:",
        )?;
        // Top-N buffers, written ONLY when present — the same opt-in rule as
        // AGG_STATE_BINARY_V2 and the session `distinct` field (register §51:
        // every persistence path must carry every state field, BY HAND).
        for ((key, win_start), rows) in &self.topn {
            let payload =
                serde_json::to_vec(rows).map_err(|e| krishiv_state::StateError::CorruptEntry {
                    message: e.to_string(),
                })?;
            let key_bytes = key.as_bytes();
            let mut state_key = Vec::with_capacity(3 + 4 + key_bytes.len() + 8);
            state_key.extend_from_slice(b"tn:");
            state_key.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            state_key.extend_from_slice(key_bytes);
            state_key.extend_from_slice(&win_start.to_le_bytes());
            backend.put(namespace, state_key, payload)?;
        }
        super::state_persistence::persist_operator_watermark_ms(
            backend,
            namespace,
            self.prev_watermark_ms,
        )
    }

    /// Restore open window accumulators from `StateBackend` (GAP-I2).
    pub fn restore_from_state(
        &mut self,
        backend: &dyn krishiv_state::StateBackend,
        namespace: &krishiv_state::Namespace,
    ) -> krishiv_state::StateResult<()> {
        self.accumulators =
            super::state_persistence::restore_window_accumulators(backend, namespace, b"tw:")?;
        self.topn.clear();
        for key_bytes in backend.list_keys(namespace)? {
            if key_bytes.get(..3) != Some(b"tn:".as_slice()) {
                continue;
            }
            let Some(payload) = backend.get(namespace, &key_bytes)? else {
                continue;
            };
            let rest = key_bytes.get(3..).unwrap_or_default();
            let (len_bytes, rest) = rest.split_at_checked(4).ok_or_else(|| {
                krishiv_state::StateError::CorruptEntry {
                    message: "truncated top-N state key".into(),
                }
            })?;
            let len_arr: [u8; 4] =
                len_bytes
                    .try_into()
                    .map_err(|_| krishiv_state::StateError::CorruptEntry {
                        message: "top-N state key length prefix malformed".into(),
                    })?;
            let key_len = u32::from_le_bytes(len_arr) as usize;
            let (key_raw, win_bytes) = rest.split_at_checked(key_len).ok_or_else(|| {
                krishiv_state::StateError::CorruptEntry {
                    message: "top-N state key shorter than its declared key length".into(),
                }
            })?;
            let win_arr: [u8; 8] =
                win_bytes
                    .try_into()
                    .map_err(|_| krishiv_state::StateError::CorruptEntry {
                        message: "top-N state key missing window start".into(),
                    })?;
            let key = String::from_utf8(key_raw.to_vec()).map_err(|e| {
                krishiv_state::StateError::CorruptEntry {
                    message: format!("top-N state key not utf8: {e}"),
                }
            })?;
            let rows: Vec<TopNRow> = serde_json::from_slice(&payload).map_err(|e| {
                krishiv_state::StateError::CorruptEntry {
                    message: format!("invalid top-N rows: {e}"),
                }
            })?;
            self.topn.insert((key, i64::from_le_bytes(win_arr)), rows);
        }
        if let Some(wm) =
            super::state_persistence::restore_operator_watermark_ms(backend, namespace)?
        {
            self.prev_watermark_ms = wm;
        }
        Ok(())
    }

    /// Process one `RecordBatch`.
    ///
    /// `new_watermark_ms` is the watermark computed *after* advancing from
    /// this batch's event times.  Events are late only if their
    /// `event_time_ms` is below the watermark from the **previous** batch
    /// (`prev_watermark_ms`).  Windows whose `window_end ≤ new_watermark_ms`
    /// are closed and returned.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        new_watermark_ms: i64,
    ) -> ExecResult<Vec<RecordBatch>> {
        // Resolve and cache the column indices on the first call. The operator
        // is single-source, so the schema is fixed for its lifetime — the
        // cached indices are valid for every subsequent `process_batch`. This
        // saves two `HashMap<String, usize>` lookups per batch (one per
        // `index_of`) and a `&str` comparison, which add up at 10k batches/s.
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

        let time_col = batch.column(time_idx);
        let time_arr = time_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "event_time column '{}' must be Int64",
                    self.spec.event_time_column
                ))
            })?;

        // Use the watermark from the PREVIOUS batch as the late threshold.
        let late_threshold = self.prev_watermark_ms;

        if let Some(top_n) = self.spec.top_n.clone() {
            self.ensure_topn_runtime(batch, &top_n)?;
            for row in 0..batch.num_rows() {
                let event_time_ms = time_arr.value(row);
                if event_time_ms < late_threshold {
                    self.late_events_dropped = self.late_events_dropped.saturating_add(1);
                    let key = extract_agg_key(batch, key_idx, row)
                        .map(|k| k.to_string())
                        .unwrap_or_default();
                    if let Some(ref handler) = self.late_event_handler {
                        handler.on_late_event(&key, event_time_ms, row);
                    }
                    continue;
                }
                let key = extract_agg_key(batch, key_idx, row)?.to_string();
                let win_start = Self::window_start(event_time_ms, self.spec.window_size_ms);
                self.insert_topn_row(batch, row, key, win_start, &top_n)?;
            }
        } else {
            // Pre-downcast the aggregate input columns once for the whole
            // batch so the per-row update avoids a `schema().index_of()` +
            // `downcast_ref()`.
            let pre_cols = crate::aggregate::downcast_agg_input_cols(batch, &self.spec.agg_exprs)?;

            for row in 0..batch.num_rows() {
                let event_time_ms = time_arr.value(row);
                if event_time_ms < late_threshold {
                    self.late_events_dropped = self.late_events_dropped.saturating_add(1);
                    let key = extract_agg_key(batch, key_idx, row)
                        .map(|k| k.to_string())
                        .unwrap_or_default();
                    if let Some(ref handler) = self.late_event_handler {
                        handler.on_late_event(&key, event_time_ms, row);
                    }
                    continue;
                }
                let key = extract_agg_key(batch, key_idx, row)?.to_string();
                let win_start = Self::window_start(event_time_ms, self.spec.window_size_ms);
                let state = self
                    .accumulators
                    .entry((key, win_start))
                    .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
                state.update_pre(&self.spec.agg_exprs, &pre_cols, row)?;
            }
        }

        // Advance internal watermark AFTER accumulating this batch.
        if new_watermark_ms >= self.prev_watermark_ms {
            self.prev_watermark_ms = new_watermark_ms;
        }

        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush all window buckets whose end time is ≤ `watermark_ms`.
    ///
    /// Returns one `RecordBatch` per closed window, sorted by
    /// `(window_start_ms, key)` for deterministic output.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;

        if self.spec.top_n.is_some() {
            return self.flush_closed_topn_windows(watermark_ms, size);
        }

        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, win_start)| win_start.saturating_add(size) <= watermark_ms)
            .cloned()
            .collect();

        if closed.is_empty() {
            return Ok(vec![]);
        }

        // Deterministic output order.
        closed.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));

        // Phase 65: build the per-window output batches on the shared
        // compute pool. Order-preserving by construction — `par_map` keeps
        // input order and the bucket list is sorted BEFORE the parallel
        // region, so output is byte-identical to the serial loop. State
        // removal stays serial (it mutates the map); batch building is
        // `&self`-only and independent per bucket.
        let drained: Vec<((String, i64), AggState)> = closed
            .into_iter()
            .filter_map(|bucket| {
                self.accumulators
                    .remove(&bucket)
                    .map(|state| (bucket, state))
            })
            .collect();
        krishiv_common::compute_pool::par_map(drained, |(bucket, state)| {
            self.build_output_batch(&bucket.0, bucket.1, &state)
        })
        .into_iter()
        .collect()
    }

    /// Early-fire: emit the **current** aggregate of every still-open window
    /// without closing or mutating it.
    ///
    /// These are speculative (non-final) results: the same `(key, window_start)`
    /// will emit its final value via [`flush_closed_windows`] once the watermark
    /// passes the window end. Downstream sinks key on `(key, window_start_ms)`
    /// as an upsert, so each early fire is superseded by the next early fire and
    /// finally by the close. This is the building block for processing-time
    /// early-fire triggers, which cut the latency-to-first-result for long
    /// event-time windows from `window_size` down to the trigger interval.
    ///
    /// State is left untouched — call it as often as the trigger fires.
    pub fn emit_open_windows(&self) -> ExecResult<Vec<RecordBatch>> {
        let mut open: Vec<(String, i64)> = self.accumulators.keys().cloned().collect();
        // Deterministic output order, matching `flush_closed_windows`.
        open.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));
        // Phase 65: same order-preserving pool fan-out as the close path;
        // read-only over `&self`, state untouched (early fires must never
        // mutate).
        krishiv_common::compute_pool::par_map(open, |bucket| match self.accumulators.get(&bucket) {
            Some(state) => self
                .build_output_batch(&bucket.0, bucket.1, state)
                .map(Some),
            None => Ok(None),
        })
        .into_iter()
        .filter_map(Result::transpose)
        .collect()
    }

    fn build_output_batch(
        &self,
        key_value: &str,
        window_start_ms: i64,
        state: &AggState,
    ) -> ExecResult<RecordBatch> {
        let window_end_ms = window_start_ms.saturating_add(self.spec.window_size_ms as i64);
        build_window_record_batch(WindowRecordBatchInput {
            schema: &self.output_schema,
            key_is_synthetic: self.spec.key_is_synthetic,
            key_type: &self.spec.key_column_type,
            key_parts: &self.spec.key_parts,
            key_value,
            window_start_ms,
            window_end_ms,
            agg_exprs: &self.spec.agg_exprs,
            state,
            agg_is_float: &self.spec.agg_is_float,
        })
    }
}

// ── Shared window output builder ──────────────────────────────────────────────

/// Build a single-row `RecordBatch` representing one closed window.
///
/// Used by both `TumblingWindowOperator` and `SlidingWindowOperator` so that
/// the output schema and column layout stay in sync automatically.
/// `key_type` is the Arrow type tag for the key column (`"int32"`, `"int64"`,
/// `"float64"`, `"utf8"`, `"bool"`).
/// Build the (fixed) output schema for a tumbling/sliding window operator.
///
/// The schema depends only on `key_column`, `key_type`, and `agg_exprs`,
/// which are immutable for the operator's lifetime — callers should compute
/// this once (e.g. in `new`) and reuse the cached `Arc<Schema>` for every
/// closed window, rather than rebuilding `Schema`/`Field` vectors per row.
pub(crate) fn build_window_output_schema(
    key_column: &str,
    key_type: &str,
    agg_exprs: &[AggExpr],
    agg_is_float: &[bool],
    key_parts: &[krishiv_plan::window::KeyPart],
    key_is_synthetic: bool,
) -> Arc<Schema> {
    // A composite key is ONE grouping key internally and N columns on the way
    // out: the user wrote `GROUP BY auction, channel` and must get `auction`
    // and `channel` back, not the encoded key they never mentioned. The same
    // rule inverted: a SYNTHETIC key was named by nobody and must appear
    // nowhere — a global aggregate emits window bounds and aggregates only.
    let mut fields: Vec<Field> = if key_is_synthetic {
        Vec::new()
    } else if key_parts.is_empty() {
        vec![Field::new(
            key_column,
            key_type_to_arrow_data_type(key_type),
            false,
        )]
    } else {
        key_parts
            .iter()
            .map(|p| Field::new(&p.name, key_type_to_arrow_data_type(&p.type_tag), false))
            .collect()
    };
    fields.extend([
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("window_end_ms", DataType::Int64, false),
    ]);
    for (i, agg) in agg_exprs.iter().enumerate() {
        let dtype = match agg.function {
            AggFunction::Avg | AggFunction::Stddev => DataType::Float64,
            _ if agg_is_float.get(i).copied().unwrap_or(false) => DataType::Float64,
            _ => DataType::Int64,
        };
        fields.push(Field::new(&agg.output_column, dtype, false));
    }
    Arc::new(Schema::new(fields))
}

pub(crate) struct WindowRecordBatchInput<'a> {
    pub(crate) schema: &'a Arc<Schema>,
    pub(crate) key_is_synthetic: bool,
    pub(crate) key_type: &'a str,
    pub(crate) key_value: &'a str,
    pub(crate) key_parts: &'a [krishiv_plan::window::KeyPart],
    pub(crate) window_start_ms: i64,
    pub(crate) window_end_ms: i64,
    pub(crate) agg_exprs: &'a [AggExpr],
    pub(crate) state: &'a AggState,
    pub(crate) agg_is_float: &'a [bool],
}

pub(crate) fn build_window_record_batch(
    input: WindowRecordBatchInput<'_>,
) -> ExecResult<RecordBatch> {
    let WindowRecordBatchInput {
        schema,
        key_is_synthetic,
        key_type,
        key_value,
        key_parts,
        window_start_ms,
        window_end_ms,
        agg_exprs,
        state,
        agg_is_float,
    } = input;
    let schema = Arc::clone(schema);
    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = if key_is_synthetic {
        Vec::new()
    } else if key_parts.is_empty() {
        vec![key_value_to_typed_array(key_type, key_value)?]
    } else {
        let decoded = crate::scalar_expr::split_composite_key(key_value, key_parts.len())?;
        key_parts
            .iter()
            .zip(decoded.iter())
            .map(|(p, v)| key_value_to_typed_array(&p.type_tag, v))
            .collect::<ExecResult<Vec<_>>>()?
    };
    columns.push(Arc::new(Int64Array::from(vec![window_start_ms])));
    columns.push(Arc::new(Int64Array::from(vec![window_end_ms])));
    for (i, agg) in agg_exprs.iter().enumerate() {
        let is_float = agg_is_float.get(i).copied().unwrap_or(false);
        match agg.function {
            AggFunction::Avg => {
                columns.push(Arc::new(Float64Array::from(vec![state.finalized_avg(i)?])));
            }
            AggFunction::Stddev => {
                columns.push(Arc::new(Float64Array::from(vec![
                    state.finalized_stddev(i)?,
                ])));
            }
            _ if is_float => {
                columns.push(Arc::new(Float64Array::from(vec![
                    state.finalized_float_value(i, agg)?,
                ])));
            }
            _ => {
                columns.push(Arc::new(Int64Array::from(vec![
                    state.finalized_value(i, agg)?,
                ])));
            }
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn key_type_to_arrow_data_type(key_type: &str) -> DataType {
    match key_type {
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint64" => DataType::UInt64,
        "float64" => DataType::Float64,
        "bool" => DataType::Boolean,
        // Includes the unresolved "auto" tag. Reaching emit still unresolved
        // means no batch ever named the key's type, and Utf8 is the historical
        // behaviour — not silently wrong, just unrefined.
        _ => DataType::Utf8,
    }
}

pub(crate) fn key_value_to_typed_array(
    key_type: &str,
    key_value: &str,
) -> Result<Arc<dyn arrow::array::Array>, ExecError> {
    match key_type {
        "int32" => {
            let v = key_value.parse::<i32>().map_err(|e| {
                ExecError::InvalidInput(format!("failed to parse key '{key_value}' as int32: {e}"))
            })?;
            Ok(Arc::new(Int32Array::from(vec![v])))
        }
        "int64" => {
            let v = key_value.parse::<i64>().map_err(|e| {
                ExecError::InvalidInput(format!("failed to parse key '{key_value}' as int64: {e}"))
            })?;
            Ok(Arc::new(Int64Array::from(vec![v])))
        }
        "uint64" => {
            let v = key_value.parse::<u64>().map_err(|e| {
                ExecError::InvalidInput(format!("failed to parse key '{key_value}' as uint64: {e}"))
            })?;
            Ok(Arc::new(arrow::array::UInt64Array::from(vec![v])))
        }
        "float64" => {
            let v = key_value.parse::<f64>().map_err(|e| {
                ExecError::InvalidInput(format!(
                    "failed to parse key '{key_value}' as float64: {e}"
                ))
            })?;
            Ok(Arc::new(Float64Array::from(vec![v])))
        }
        "bool" => {
            let v = key_value.parse::<bool>().map_err(|e| {
                ExecError::InvalidInput(format!("failed to parse key '{key_value}' as bool: {e}"))
            })?;
            Ok(Arc::new(BooleanArray::from(vec![v])))
        }
        _ => Ok(Arc::new(StringArray::from(vec![key_value]))),
    }
}

/// Build a typed column from repeated string key values (top-N emission).
fn key_values_to_typed_column_tumbling(
    key_type: &str,
    values: &[&str],
) -> ExecResult<Arc<dyn arrow::array::Array>> {
    match key_type {
        "int64" => {
            let vals = values
                .iter()
                .map(|v| {
                    v.parse::<i64>().map_err(|_| {
                        ExecError::InvalidInput(format!("key '{v}' cannot be parsed as int64"))
                    })
                })
                .collect::<ExecResult<Vec<i64>>>()?;
            Ok(Arc::new(Int64Array::from(vals)))
        }
        "uint64" => {
            let vals = values
                .iter()
                .map(|v| {
                    v.parse::<u64>().map_err(|_| {
                        ExecError::InvalidInput(format!("key '{v}' cannot be parsed as uint64"))
                    })
                })
                .collect::<ExecResult<Vec<u64>>>()?;
            Ok(Arc::new(arrow::array::UInt64Array::from(vals)))
        }
        "int32" => {
            let vals = values
                .iter()
                .map(|v| {
                    v.parse::<i32>().map_err(|_| {
                        ExecError::InvalidInput(format!("key '{v}' cannot be parsed as int32"))
                    })
                })
                .collect::<ExecResult<Vec<i32>>>()?;
            Ok(Arc::new(Int32Array::from(vals)))
        }
        "float64" => {
            let vals = values
                .iter()
                .map(|v| {
                    v.parse::<f64>().map_err(|_| {
                        ExecError::InvalidInput(format!("key '{v}' cannot be parsed as float64"))
                    })
                })
                .collect::<ExecResult<Vec<f64>>>()?;
            Ok(Arc::new(Float64Array::from(vals)))
        }
        "bool" => {
            let vals = values
                .iter()
                .map(|v| {
                    v.parse::<bool>().map_err(|_| {
                        ExecError::InvalidInput(format!("key '{v}' cannot be parsed as bool"))
                    })
                })
                .collect::<ExecResult<Vec<bool>>>()?;
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        _ => Ok(Arc::new(StringArray::from(values.to_vec()))),
    }
}

/// Build a typed array from `AggKey` values whose declared tag is `tag`.
///
/// A value whose variant disagrees with the tag is an error, not a cast: the
/// tag came from the SAME schema the values were extracted from, so a
/// mismatch means the buffer is corrupt.
fn agg_keys_to_typed_array(
    tag: &str,
    values: &[&crate::join::AggKey],
) -> ExecResult<Arc<dyn arrow::array::Array>> {
    use crate::join::AggKey;
    fn bad(tag: &str, got: &AggKey) -> ExecError {
        ExecError::InvalidInput(format!(
            "top-N buffer corrupt: declared column tag '{tag}' but stored value {got:?}"
        ))
    }
    match tag {
        "int64" => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::Int64(v) => Ok(*v),
                    AggKey::Int32(v) => Ok(i64::from(*v)),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<i64>>>()?;
            Ok(Arc::new(Int64Array::from(vals)))
        }
        "uint64" => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::UInt64(v) => Ok(*v),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<u64>>>()?;
            Ok(Arc::new(arrow::array::UInt64Array::from(vals)))
        }
        "int32" => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::Int32(v) => Ok(*v),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<i32>>>()?;
            Ok(Arc::new(Int32Array::from(vals)))
        }
        "float64" => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::Float64(bits) => Ok(f64::from_bits(*bits)),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<f64>>>()?;
            Ok(Arc::new(Float64Array::from(vals)))
        }
        "bool" => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::Bool(v) => Ok(*v),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<bool>>>()?;
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        _ => {
            let vals = values
                .iter()
                .map(|k| match k {
                    AggKey::Utf8(v) => Ok(v.clone()),
                    other => Err(bad(tag, other)),
                })
                .collect::<ExecResult<Vec<String>>>()?;
            Ok(Arc::new(StringArray::from(vals)))
        }
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;
    use crate::aggregate::AggFunction;
    use krishiv_state::{Namespace, RocksDbStateBackend};

    /// Top-N buffers must survive checkpoint/restore, and a pre-checkpoint
    /// row that is still among the best must beat a worse post-restore row.
    ///
    /// Register §51's standing observation applies: top-N state is a NEW
    /// state field, and every persistence path must carry it by hand. With
    /// the persist hunk reverted, the restored operator has an empty buffer
    /// and the post-restore row (price 50) wrongly wins a top-2 slot that
    /// the checkpointed 900 and 500 should hold.
    #[test]
    fn top_n_buffers_survive_checkpoint_restore() {
        let make_spec = || TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: Some(krishiv_plan::window::TopNSpec {
                order_column: "price".into(),
                descending: true,
                limit: 2,
                carry_columns: vec![String::from("price")],
            }),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![],
            agg_is_float: vec![],
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let batch = |ts: Vec<i64>, price: Vec<i64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a"; ts.len()])),
                    Arc::new(Int64Array::from(ts)),
                    Arc::new(Int64Array::from(price)),
                ],
            )
            .unwrap()
        };

        let mut op = TumblingWindowOperator::new(make_spec());
        op.process_batch(&batch(vec![100, 200], vec![900, 500]), 200)
            .expect("process");

        let mut backend = RocksDbStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-topn", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = TumblingWindowOperator::new(make_spec());
        restored.restore_from_state(&backend, &ns).expect("restore");

        // A worse bid after restore, then close the window.
        restored
            .process_batch(&batch(vec![300], vec![50]), 300)
            .expect("post-restore bid");
        let out = restored.flush_closed_windows(5_000).expect("close");
        let prices: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of("price").expect("price col");
                let arr = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64")
                    .clone();
                (0..b.num_rows())
                    .map(move |i| arr.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            prices,
            vec![900, 500],
            "the checkpointed 900/500 must survive and the post-restore 50 must lose"
        );
    }

    #[test]
    fn tumbling_state_persist_and_restore_roundtrip() {
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
            agg_is_float: vec![false],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![500])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        op.process_batch(&batch, 100).expect("process");
        assert_eq!(op.open_window_count(), 1);

        let mut backend = RocksDbStateBackend::ephemeral().unwrap();
        let ns = Namespace::new("op-1", "windows");
        op.persist_to_state(&mut backend, &ns).expect("persist");

        let mut restored = TumblingWindowOperator::new(TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
            agg_is_float: vec![false],
        });
        restored.restore_from_state(&backend, &ns).expect("restore");
        assert_eq!(restored.open_window_count(), 1);
    }

    /// Regression (Wave 1 — Data Correctness): `validate_spec` must reject
    /// `window_size_ms == 0` and values exceeding `i64::MAX` rather than
    /// letting `window_start`'s `event_time_ms / size` divide by zero or
    /// silently truncate via `as i64`.
    #[test]
    fn validate_spec_rejects_zero_and_overflowing_window_size() {
        let base = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 0,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
            agg_is_float: vec![false],
        };
        assert!(matches!(
            TumblingWindowOperator::validate_spec(&base),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let overflowing = TumblingWindowSpec {
            window_size_ms: i64::MAX as u64 + 1,
            ..base.clone()
        };
        assert!(matches!(
            TumblingWindowOperator::validate_spec(&overflowing),
            Err(ExecError::InvalidWindowConfig(_))
        ));

        let valid = TumblingWindowSpec {
            window_size_ms: 1000,
            ..base
        };
        assert!(TumblingWindowOperator::validate_spec(&valid).is_ok());
    }

    #[test]
    fn emit_open_windows_is_speculative_and_non_mutating() {
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 10_000,
            agg_exprs: vec![AggExpr {
                filter: None,
                input_column: "v".into(),
                output_column: "sum_v".into(),
                function: AggFunction::Sum,
            }],
            agg_is_float: vec![false],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![500, 600])),
                Arc::new(Int64Array::from(vec![10, 32])),
            ],
        )
        .unwrap();
        // Watermark well before the window end (10_000) — nothing is closed yet.
        let closed = op.process_batch(&batch, 100).expect("process");
        assert!(closed.is_empty(), "window must not close before its end");
        assert_eq!(op.open_window_count(), 1);

        // Early-fire emits the current speculative aggregate for the open window.
        let early = op.emit_open_windows().expect("early fire");
        assert_eq!(early.len(), 1, "one open window → one speculative row");
        let sum_col = early[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum_v Int64");
        assert_eq!(sum_col.value(0), 42, "speculative sum = 10 + 32");

        // Early-fire must not mutate state: the window is still open and a second
        // fire returns the same speculative value.
        assert_eq!(op.open_window_count(), 1);
        let early2 = op.emit_open_windows().expect("early fire 2");
        assert_eq!(early2.len(), 1);
        assert_eq!(op.open_window_count(), 1);
    }
}

/// Property-based aggregation-correctness tests for `TumblingWindowOperator`.
///
/// `cargo-fuzz` requires a nightly toolchain and sanitizer support that this
/// workspace does not provision; `proptest` gives equivalent adversarial-input
/// coverage (arbitrary in-order event sequences, shrinking on failure) entirely
/// on stable, so it is the practical choice for exercising window aggregation
/// invariants such as "every accepted row is counted exactly once" and
/// "the windowed sum equals the sum of its inputs".
#[cfg(test)]
mod aggregation_proptests {
    use super::*;
    use crate::aggregate::AggFunction;
    use proptest::prelude::*;

    /// Arbitrary in-order `(event_time_ms, value)` sequences confined to a
    /// single 1000ms window `[0, 1000)`, with values small enough that their
    /// sum cannot overflow `i64`.
    fn arb_single_window_events() -> impl Strategy<Value = Vec<(i64, i64)>> {
        prop::collection::vec((0i64..1000, -10_000i64..10_000), 0..32).prop_map(|mut events| {
            events.sort_by_key(|(ts, _)| *ts);
            events
        })
    }

    fn make_batch(events: &[(i64, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let keys = vec!["k"; events.len()];
        let timestamps: Vec<i64> = events.iter().map(|(ts, _)| *ts).collect();
        let values: Vec<i64> = events.iter().map(|(_, v)| *v).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .expect("schema and array lengths match")
    }

    /// Phase 65: the pooled flush must be byte-order-identical to the serial
    /// contract — one batch per closed window, sorted by
    /// `(window_start_ms, key)` — across many keys and windows. `par_map`
    /// preserves input order and the sort happens before the parallel
    /// region, so any reordering here is a real regression.
    #[test]
    fn parallel_flush_keeps_deterministic_order() {
        let mut op = TumblingWindowOperator::new(spec());
        // 40 keys × 5 windows, inserted in scrambled order.
        let mut rows: Vec<(String, i64, i64)> = Vec::new();
        for k in 0..40 {
            for w in 0..5 {
                rows.push((format!("key-{:02}", (k * 7) % 40), w * 1000 + 17, 1));
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(k, _, _)| k.clone()).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        // Watermark past every window end closes all 5 windows per key.
        let out = op.process_batch(&batch, 10_000).unwrap();
        let mut seen: Vec<(i64, String)> = Vec::new();
        for b in &out {
            assert_eq!(b.num_rows(), 1, "one batch per (key, window) bucket");
            let start = b
                .column_by_name("window_start_ms")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .map(|a| a.value(0))
                .unwrap();
            let key = b
                .column_by_name("k")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .map(|a| a.value(0).to_string())
                .unwrap();
            seen.push((start, key));
        }
        let mut expected = seen.clone();
        expected.sort();
        assert_eq!(
            seen, expected,
            "flush output must stay (window_start, key)-sorted"
        );
        assert_eq!(seen.len(), 40 * 5);
    }

    fn spec() -> TumblingWindowSpec {
        TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![
                AggExpr {
                    filter: None,
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "cnt".into(),
                },
                AggExpr {
                    filter: None,
                    function: AggFunction::Sum,
                    input_column: "v".into(),
                    output_column: "sum_v".into(),
                },
            ],
            agg_is_float: vec![false, false],
        }
    }

    fn read_i64(batch: &RecordBatch, col: &str, row: usize) -> i64 {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
    }

    proptest! {
        /// Every accepted event must be counted exactly once and summed
        /// without loss — the fundamental correctness invariant.
        #[test]
        fn tumbling_window_count_and_sum_are_lossless(events in arb_single_window_events()) {
            let mut op = TumblingWindowOperator::new(spec());
            let batch = make_batch(&events);

            // Watermark = window_size_ms closes the single `[0, 1000)` window.
            let outputs = op.process_batch(&batch, 1000).expect("process_batch");

            if events.is_empty() {
                prop_assert!(outputs.is_empty());
            } else {
                prop_assert_eq!(outputs.len(), 1);
                let out = &outputs[0];
                prop_assert_eq!(out.num_rows(), 1);

                let cnt   = read_i64(out, "cnt",   0);
                let sum_v = read_i64(out, "sum_v", 0);

                prop_assert_eq!(cnt as usize, events.len());
                prop_assert_eq!(sum_v, events.iter().map(|(_, v)| v).sum::<i64>());
            }
            prop_assert_eq!(op.open_window_count(), 0);
        }

        /// Processing N events as a single batch must produce the same count
        /// as processing each event one row at a time (incremental path).
        #[test]
        fn tumbling_batch_vs_incremental_count_equal(events in arb_single_window_events()) {
            // Batch path: all events in one RecordBatch.
            let mut batch_op = TumblingWindowOperator::new(spec());
            let batch = make_batch(&events);
            let batch_out = batch_op.process_batch(&batch, 1000).expect("batch");

            // Incremental path: one row per RecordBatch, watermark = 1000 only on last.
            let mut incr_op = TumblingWindowOperator::new(spec());
            let mut incr_out: Vec<RecordBatch> = vec![];
            for (i, event) in events.iter().enumerate() {
                let single = make_batch(std::slice::from_ref(event));
                let wm = if i + 1 == events.len() { 1000 } else { 0 };
                let mut rows = incr_op.process_batch(&single, wm).expect("incr");
                incr_out.append(&mut rows);
            }

            let batch_cnt: i64 = batch_out.iter()
                .map(|b| read_i64(b, "cnt", 0))
                .sum();
            let incr_cnt: i64 = incr_out.iter()
                .map(|b| read_i64(b, "cnt", 0))
                .sum();
            prop_assert_eq!(batch_cnt, incr_cnt);
        }

        /// `window_start_ms` in the output must always be a multiple of
        /// `window_size_ms` for non-negative timestamps (grid alignment).
        #[test]
        fn tumbling_window_start_is_grid_aligned(ts in 0i64..100_000_000i64) {
            let size_ms: u64 = 1_000;
            let start = TumblingWindowOperator::window_start(ts, size_ms);
            prop_assert_eq!(
                start % size_ms as i64,
                0,
                "window_start({}) = {} not aligned to grid {}", ts, start, size_ms
            );
            prop_assert!(start <= ts, "window_start must be ≤ event time");
            prop_assert!(start + size_ms as i64 > ts, "event must fall within window");
        }

        /// The `min_v` output must be ≤ every input value; `max_v` must be ≥
        /// every input value — both are trivially bounded by the data they saw.
        #[test]
        fn tumbling_min_and_max_are_tight_bounds(events in arb_single_window_events()) {
            if events.is_empty() {
                return Ok(());
            }
            let min_spec = TumblingWindowSpec {
                key_column: "k".into(),
                key_column_type: "utf8".into(),
                key_parts: Vec::new(),
                key_is_synthetic: false,
                top_n: None,
                event_time_column: "ts".into(),
                window_size_ms: 1000,
                agg_exprs: vec![
                    AggExpr { filter: None, function: AggFunction::Min, input_column: "v".into(), output_column: "min_v".into() },
                    AggExpr { filter: None, function: AggFunction::Max, input_column: "v".into(), output_column: "max_v".into() },
                ],
                agg_is_float: vec![false, false],
            };
            let mut op = TumblingWindowOperator::new(min_spec);
            let batch = make_batch(&events);
            let outputs = op.process_batch(&batch, 1000).expect("process_batch");
            prop_assert_eq!(outputs.len(), 1);
            let out = &outputs[0];
            let min_v = read_i64(out, "min_v", 0);
            let max_v = read_i64(out, "max_v", 0);
            let expected_min = events.iter().map(|(_, v)| *v).min().unwrap();
            let expected_max = events.iter().map(|(_, v)| *v).max().unwrap();
            prop_assert_eq!(min_v, expected_min);
            prop_assert_eq!(max_v, expected_max);
        }

        /// Late events (timestamps before `prev_watermark_ms`) must be
        /// excluded from window counts entirely.
        #[test]
        fn tumbling_late_events_not_counted(
            on_time in prop::collection::vec(500i64..1000, 1..16usize),
            late    in prop::collection::vec(0i64..500,   1..8usize),
        ) {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]));
            let spec = TumblingWindowSpec {
                key_column: "k".into(),
                key_column_type: "utf8".into(),
                key_parts: Vec::new(),
                key_is_synthetic: false,
                top_n: None,
                event_time_column: "ts".into(),
                window_size_ms: 1000,
                agg_exprs: vec![AggExpr { filter: None,
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "cnt".into(),
                }],
                agg_is_float: vec![false],
            };
            let mut op = TumblingWindowOperator::new(spec);

            // First batch: on-time events, advances prev_watermark_ms to 500.
            let first: Vec<(i64, i64)> = on_time.iter().map(|&t| (t, 0)).collect();
            let ts_first: Vec<i64> = first.iter().map(|(t, _)| *t).collect();
            let b1 = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(StringArray::from(vec!["k"; first.len()])),
                Arc::new(Int64Array::from(ts_first.clone())),
                Arc::new(Int64Array::from(vec![0i64; first.len()])),
            ]).unwrap();
            // Watermark 500 — doesn't close [0,1000) window yet.
            let _ = op.process_batch(&b1, 500).expect("first");

            // Second batch: late events (ts < prev_watermark_ms = 500).
            let ts_late: Vec<i64> = late.clone();
            let b2 = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(StringArray::from(vec!["k"; ts_late.len()])),
                Arc::new(Int64Array::from(ts_late)),
                Arc::new(Int64Array::from(vec![0i64; late.len()])),
            ]).unwrap();
            // Watermark 1000 — closes [0,1000) window, but late events excluded.
            let outputs = op.process_batch(&b2, 1000).expect("second");

            let cnt = outputs.iter()
                .find(|b| b.schema().index_of("cnt").is_ok())
                .map(|b| read_i64(b, "cnt", 0))
                .unwrap_or(0);

            // Output count must equal ONLY the on-time events (no late events).
            prop_assert_eq!(
                cnt as usize, on_time.len(),
                "late events must be excluded; got {}, expected {}",
                cnt, on_time.len()
            );
            prop_assert_eq!(
                op.late_events_dropped as usize, late.len(),
                "late_events_dropped counter must equal number of late events"
            );
        }
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;
    use krishiv_plan::window::{AggFilterCompareOp, AggFilterValue, WindowAggFilter};

    fn kind_is_edit() -> WindowAggFilter {
        WindowAggFilter::Compare {
            column: "kind".into(),
            op: AggFilterCompareOp::Eq,
            value: AggFilterValue::Utf8("edit".into()),
        }
    }

    /// SQL `FILTER (WHERE …)` semantics on the window accumulators: filtered
    /// SUM only sees matching rows, filtered COUNT counts matching rows, an
    /// unfiltered SUM still sees everything — and NULL inputs feed no
    /// aggregate (they used to enter the accumulator as 0-defaults).
    #[test]
    fn filtered_aggregates_and_null_inputs() {
        let spec = TumblingWindowSpec {
            key_column: "k".into(),
            key_column_type: "utf8".into(),
            key_parts: Vec::new(),
            key_is_synthetic: false,
            top_n: None,
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![
                AggExpr {
                    function: AggFunction::Sum,
                    input_column: "v".into(),
                    output_column: "edit_bytes".into(),
                    filter: Some(kind_is_edit()),
                },
                AggExpr {
                    function: AggFunction::Count,
                    input_column: String::new(),
                    output_column: "edits".into(),
                    filter: Some(kind_is_edit()),
                },
                AggExpr {
                    function: AggFunction::Sum,
                    input_column: "v".into(),
                    output_column: "all_bytes".into(),
                    filter: None,
                },
            ],
            agg_is_float: vec![false, false, false],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("kind", DataType::Utf8, false),
        ]));
        // Window [0, 1000): edit/10, log/5, edit/NULL, edit/7.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "a", "a"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(Int64Array::from(vec![Some(10), Some(5), None, Some(7)])),
                Arc::new(StringArray::from(vec!["edit", "log", "edit", "edit"])),
            ],
        )
        .unwrap();
        assert!(op.process_batch(&batch, 400).unwrap().is_empty());

        // A later event advances the watermark past window_end and closes it.
        let closer = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![5000])),
                Arc::new(Int64Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec!["log"])),
            ],
        )
        .unwrap();
        let closed = op.process_batch(&closer, 5000).unwrap();
        assert_eq!(closed.len(), 1, "exactly the [0,1000) window closes");
        let out = &closed[0];
        let col = |name: &str| -> i64 {
            let idx = out.schema().index_of(name).unwrap();
            out.column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(
            col("edit_bytes"),
            17,
            "10 + 7; log row filtered, NULL skipped"
        );
        assert_eq!(
            col("edits"),
            3,
            "all kind='edit' rows count, NULL v included"
        );
        assert_eq!(col("all_bytes"), 22, "10 + 5 + 7; NULL skipped, no filter");
    }
}
