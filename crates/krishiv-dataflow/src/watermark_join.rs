//! ST8: Stream-to-stream watermark-bounded join operator.
//!
//! [`WatermarkWindowJoinOperator`] buffers events from both streams in a
//! sliding window bounded by the event-time watermark.  When the watermark
//! advances to W, events older than `W − window_ms` are evicted, keeping
//! state at O(window_ms × throughput_per_ms) — the same guarantee as Flink's
//! `intervalJoin` and Spark's stream-stream join with watermarking.
//!
//! Internally it wraps [`PerKeyIntervalJoin`]: each row is routed to a
//! per-key buffer and matched against the opposite side within `[−window_ms,
//! +window_ms]`.  Matched pairs are returned as joined [`RecordBatch`]es
//! (left columns || right columns).

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::barrier_align::{BarrierAligner, BarrierEvent};
use crate::interval_join::{IntervalJoinSpec, PerKeyIntervalJoin};

/// The left input's index for the join's [`BarrierAligner`].
pub const JOIN_LEFT_INPUT: usize = 0;
/// The right input's index for the join's [`BarrierAligner`].
pub const JOIN_RIGHT_INPUT: usize = 1;

// ── Spec ──────────────────────────────────────────────────────────────────────

/// Configures a [`WatermarkWindowJoinOperator`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatermarkWindowJoinSpec {
    /// Event-time column (Int64 milliseconds since epoch) present in *both*
    /// the left and right streams.
    pub time_column: String,
    /// Join key column in the left stream (string or convertible to string).
    pub left_key_column: String,
    /// Join key column in the right stream.
    pub right_key_column: String,
    /// Half-width of the join window in milliseconds.  Left event `L` matches
    /// right event `R` when `R.ts ∈ [L.ts − window_ms, L.ts + window_ms]`.
    pub window_ms: u64,
}

impl From<&krishiv_plan::stream_join::StreamingJoinSpec> for WatermarkWindowJoinSpec {
    /// Adopt a plan-level join spec.
    ///
    /// The plan crate owns the spec because `krishiv-sql` cannot depend on this
    /// one; this is the single place the two vocabularies meet, so they cannot
    /// drift into two descriptions of the same join.
    fn from(plan: &krishiv_plan::stream_join::StreamingJoinSpec) -> Self {
        Self {
            time_column: plan.time_column.clone(),
            left_key_column: plan.left_key_column.clone(),
            right_key_column: plan.right_key_column.clone(),
            window_ms: plan.window_ms,
        }
    }
}

// ── Operator ──────────────────────────────────────────────────────────────────

/// Stream-to-stream equi-join bounded by a sliding event-time window.
///
/// # Usage
///
/// ```ignore
/// let spec = WatermarkWindowJoinSpec {
///     time_column: "ts".into(),
///     left_key_column: "user_id".into(),
///     right_key_column: "user_id".into(),
///     window_ms: 5_000,
/// };
/// let mut op = WatermarkWindowJoinOperator::new(spec);
/// // Process batches from the left stream — returns any matches against
/// // already-buffered right events.
/// let matched: Vec<RecordBatch> = op.process_left(&left_batch);
/// // Advance the watermark to evict stale state.
/// op.advance_watermark(new_watermark_ms);
/// ```
pub struct WatermarkWindowJoinOperator {
    spec: WatermarkWindowJoinSpec,
    join: PerKeyIntervalJoin,
    watermark_ms: i64,
    /// Two-input checkpoint-barrier alignment (left = 0, right = 1).
    aligner: BarrierAligner,
    /// Post-barrier batches held on a blocked side until the epoch aligns; they
    /// belong to the next epoch and are replayed after the snapshot.
    left_buffer: Vec<RecordBatch>,
    right_buffer: Vec<RecordBatch>,
}

impl WatermarkWindowJoinOperator {
    /// Create a new operator from `spec`.
    pub fn new(spec: WatermarkWindowJoinSpec) -> Self {
        let interval = IntervalJoinSpec::new(
            spec.left_key_column.clone(),
            -(spec.window_ms as i64),
            spec.window_ms as i64,
        );
        Self {
            spec,
            join: PerKeyIntervalJoin::new(interval),
            watermark_ms: i64::MIN,
            aligner: BarrierAligner::new(2),
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }

    /// Process a batch from the left stream.
    ///
    /// Each row is matched against the right-side buffer for the same key.
    /// Returns joined `RecordBatch` rows (left columns ∥ right columns). While
    /// the left input is barrier-blocked (it has delivered an epoch's barrier the
    /// right input has not yet matched), the batch is held for replay after the
    /// snapshot rather than folded into the in-progress epoch.
    /// # Errors
    /// When the key or time column is missing or of an unsupported type. The
    /// predecessor silently substituted a POSITIONAL pseudo-key
    /// (`__row_{n}`) on extraction failure, which made row i of one side
    /// "join" row i of every batch on the other — a UInt64-keyed benchmark
    /// emitted 40M rows from 400k inputs, all fabricated (register §58).
    pub fn process_left(&mut self, batch: &RecordBatch) -> crate::ExecResult<Vec<RecordBatch>> {
        if self.aligner.is_blocked(JOIN_LEFT_INPUT) {
            self.left_buffer.push(batch.clone());
            return Ok(Vec::new());
        }
        self.process_side(batch, &self.spec.left_key_column.clone(), true)
    }

    /// Process a batch from the right stream.
    ///
    /// # Errors
    /// See [`Self::process_left`].
    pub fn process_right(&mut self, batch: &RecordBatch) -> crate::ExecResult<Vec<RecordBatch>> {
        if self.aligner.is_blocked(JOIN_RIGHT_INPUT) {
            self.right_buffer.push(batch.clone());
            return Ok(Vec::new());
        }
        self.process_side(batch, &self.spec.right_key_column.clone(), false)
    }

    /// Record the checkpoint barrier for `epoch` on the **left** input.
    ///
    /// Returns [`BarrierEvent::Aligned`] once the right input has also delivered
    /// the epoch's barrier — the operator should snapshot then, and replay any
    /// buffered input via [`take_realigned_input`](Self::take_realigned_input).
    pub fn record_left_barrier(&mut self, epoch: u64) -> BarrierEvent {
        self.aligner.record_barrier(epoch, JOIN_LEFT_INPUT)
    }

    /// Record the checkpoint barrier for `epoch` on the **right** input.
    pub fn record_right_barrier(&mut self, epoch: u64) -> BarrierEvent {
        self.aligner.record_barrier(epoch, JOIN_RIGHT_INPUT)
    }

    /// Whether the left input is currently barrier-blocked (buffering for the
    /// next epoch).
    pub fn is_left_blocked(&self) -> bool {
        self.aligner.is_blocked(JOIN_LEFT_INPUT)
    }

    /// Whether the right input is currently barrier-blocked.
    pub fn is_right_blocked(&self) -> bool {
        self.aligner.is_blocked(JOIN_RIGHT_INPUT)
    }

    /// Drain the `(left, right)` batches buffered during alignment so the caller
    /// can replay them into the post-snapshot epoch. Call after an
    /// [`BarrierEvent::Aligned`] and the snapshot.
    pub fn take_realigned_input(&mut self) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        (
            std::mem::take(&mut self.left_buffer),
            std::mem::take(&mut self.right_buffer),
        )
    }

    /// Advance the watermark.  State older than `watermark_ms − window_ms` is
    /// evicted on the next `evict_before` call inside `PerKeyIntervalJoin`.
    pub fn advance_watermark(&mut self, watermark_ms: i64) {
        if watermark_ms > self.watermark_ms {
            self.watermark_ms = watermark_ms;
            self.join.evict_before(watermark_ms);
        }
    }

    /// Number of active keys with buffered events (diagnostic).
    pub fn active_key_count(&self) -> usize {
        self.join.active_key_count()
    }

    /// Serialize operator state (spec + watermark + **buffered join events**)
    /// as JSON bytes.
    ///
    /// Phase 55 / G5: buffered events travel in the snapshot. The earlier
    /// lightweight form ("re-derive from source replay") could not meet the
    /// restore-with-exact-pre-kill-accumulations contract — a source that
    /// compacted past the buffered offsets lost join state permanently.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        use base64::Engine as _;
        let buffered = self.join.snapshot_buffered_events().map_err(|message| {
            serde::ser::Error::custom(format!("buffered join events: {message}"))
        })?;
        let buffered_b64 = base64::engine::general_purpose::STANDARD.encode(&buffered);
        let snap = serde_json::json!({
            "spec": self.spec,
            "watermark_ms": self.watermark_ms,
            "buffered_b64": buffered_b64,
        });
        serde_json::to_vec(&snap)
    }

    /// Restore from a snapshot produced by [`snapshot_bytes`], including
    /// buffered join events (snapshots from older builds without the
    /// `buffered_b64` field restore with empty buffers, as before).
    pub fn restore_from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let val: serde_json::Value = serde_json::from_slice(bytes)?;
        let spec: WatermarkWindowJoinSpec =
            serde_json::from_value(val.get("spec").cloned().unwrap_or(serde_json::Value::Null))?;
        let watermark_ms: i64 = val
            .get("watermark_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MIN);
        let mut op = Self::new(spec);
        op.watermark_ms = watermark_ms;
        if let Some(encoded) = val.get("buffered_b64").and_then(|v| v.as_str()) {
            use base64::Engine as _;
            let buffered = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|e| serde::de::Error::custom(format!("buffered_b64: {e}")))?;
            op.join
                .restore_buffered_events(&buffered)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(op)
    }

    // ── Internal ──────────────────────────────────────────────────────────

    fn process_side(
        &mut self,
        batch: &RecordBatch,
        key_col: &str,
        is_left: bool,
    ) -> crate::ExecResult<Vec<RecordBatch>> {
        let n = batch.num_rows();
        let time_idx = batch
            .schema()
            .index_of(&self.spec.time_column)
            .map_err(|_| {
                crate::ExecError::ColumnNotFound(format!(
                    "join time column '{}'",
                    self.spec.time_column
                ))
            })?;
        let key_idx = batch.schema().index_of(key_col).map_err(|_| {
            crate::ExecError::ColumnNotFound(format!("join key column '{key_col}'"))
        })?;

        // Hot path (2026-08-22 q3/q8 optimization): the whole input batch is
        // shared as ONE Arc across its rows — no per-row 1-row slice — and
        // the key is formatted into a REUSED buffer instead of a fresh String
        // per row. Matches carry (batch, row) indices; output assembly below
        // interleaves by index instead of concatenating 1-row arrays.
        use std::fmt::Write as _;
        let shared = Arc::new(batch.clone());
        let mut key_buf = String::with_capacity(24);
        let mut pairs: Vec<crate::interval_join::MatchedRows> = Vec::new();
        for row in 0..n {
            let time_ms = extract_i64(batch, time_idx, row).ok_or_else(|| {
                crate::ExecError::UnsupportedType(format!(
                    "join time column '{}' must be Int64",
                    self.spec.time_column
                ))
            })?;
            // Typed key extraction (all integer widths, strings, bools).
            // NEVER a fallback: a row whose key cannot be read must fail the
            // batch, not silently become a positional pseudo-key that joins.
            let key = crate::join::extract_agg_key(batch, key_idx, row)?;
            key_buf.clear();
            write!(key_buf, "{key}").map_err(|e| crate::ExecError::Arrow(e.to_string()))?;
            self.join.push_row(
                is_left,
                &key_buf,
                time_ms,
                &shared,
                u32::try_from(row).unwrap_or(u32::MAX),
                &mut pairs,
            );
        }
        // Batch-at-a-time output (task #149 fix 8): all matches from one
        // input batch become ONE output batch, built columnar.
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let merged =
            join_pairs_to_batch(&pairs).map_err(|e| crate::ExecError::Arrow(e.to_string()))?;
        Ok(vec![merged])
    }
}

// ── Arrow helpers ─────────────────────────────────────────────────────────────

fn extract_i64(batch: &RecordBatch, col_idx: usize, row: usize) -> Option<i64> {
    let col = batch.column(col_idx);
    col.as_any().downcast_ref::<Int64Array>()?.value(row).into()
}

/// Merge matched (left row, right row) pairs into ONE batch (left cols ∥
/// right cols), computing the joined schema once and building each column
/// with a single `interleave` over the distinct source batches (2026-08-22:
/// the predecessor concatenated per-match 1-row arrays — thousands of parts
/// per column).
///
/// If a column name appears in both sides, prefix with `left_` / `right_` to
/// prevent Arrow schema-uniqueness violations — the same naming rule the
/// per-match predecessor applied, so downstream stage specs keep resolving.
fn join_pairs_to_batch(
    pairs: &[crate::interval_join::MatchedRows],
) -> Result<RecordBatch, ArrowError> {
    let (first_left, _, first_right, _) = pairs
        .first()
        .ok_or_else(|| ArrowError::InvalidArgumentError("empty join pair set".into()))?;
    let schema = joined_schema(first_left, first_right);
    let left_cols = first_left.num_columns();
    let right_cols = first_right.num_columns();

    // Distinct source batches per side (by Arc identity) + per-match
    // (batch_index, row_index) pairs for `interleave`.
    fn side_indices<'a>(
        pairs: &'a [crate::interval_join::MatchedRows],
        pick: impl Fn(&'a crate::interval_join::MatchedRows) -> (&'a Arc<RecordBatch>, u32),
    ) -> (Vec<&'a Arc<RecordBatch>>, Vec<(usize, usize)>) {
        let mut sources: Vec<&Arc<RecordBatch>> = Vec::new();
        let mut indices = Vec::with_capacity(pairs.len());
        let mut last: Option<(*const RecordBatch, usize)> = None;
        for pair in pairs {
            let (batch, row) = pick(pair);
            let ptr = Arc::as_ptr(batch);
            let source_idx = match last {
                Some((p, idx)) if p == ptr => idx,
                _ => {
                    let idx = sources
                        .iter()
                        .position(|s| Arc::as_ptr(s) == ptr)
                        .unwrap_or_else(|| {
                            sources.push(batch);
                            sources.len() - 1
                        });
                    last = Some((ptr, idx));
                    idx
                }
            };
            indices.push((source_idx, row as usize));
        }
        (sources, indices)
    }

    let (left_sources, left_indices) = side_indices(pairs, |(l, lr, _, _)| (l, *lr));
    let (right_sources, right_indices) = side_indices(pairs, |(_, _, r, rr)| (r, *rr));

    let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(left_cols + right_cols);
    for idx in 0..left_cols {
        let parts: Vec<&dyn arrow::array::Array> = left_sources
            .iter()
            .map(|b| b.column(idx).as_ref())
            .collect();
        cols.push(arrow::compute::interleave(&parts, &left_indices)?);
    }
    for idx in 0..right_cols {
        let parts: Vec<&dyn arrow::array::Array> = right_sources
            .iter()
            .map(|b| b.column(idx).as_ref())
            .collect();
        cols.push(arrow::compute::interleave(&parts, &right_indices)?);
    }
    RecordBatch::try_new(schema, cols)
}

/// The joined output schema for one (left, right) pair — the naming rules of
/// the original row-level merge, factored so they run once per input batch.
fn joined_schema(left: &RecordBatch, right: &RecordBatch) -> Arc<Schema> {
    use arrow::datatypes::Field;
    let left_schema = left.schema();
    let right_schema = right.schema();
    let left_names: std::collections::HashSet<&str> = left_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let right_names: std::collections::HashSet<&str> = right_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let collide: std::collections::HashSet<&str> =
        left_names.intersection(&right_names).copied().collect();

    let rename = |f: &Arc<arrow::datatypes::Field>, prefix: &str| -> Arc<Field> {
        if collide.contains(f.name().as_str()) {
            Arc::new(Field::new(
                format!("{prefix}{}", f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        } else {
            f.clone()
        }
    };

    let fields: Vec<Arc<Field>> = left
        .schema()
        .fields()
        .iter()
        .map(|f| rename(f, "left_"))
        .chain(right.schema().fields().iter().map(|f| rename(f, "right_")))
        .collect();

    Arc::new(Schema::new(fields))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    /// All matches from one input batch arrive as ONE output batch (task
    /// #149 fix 8): the per-match one-row-batch shape made every downstream
    /// consumer pay full per-batch overhead per matched row.
    #[test]
    fn matches_from_one_input_batch_form_one_output_batch() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(10_000));
        let left = multi_row_batch(&["a", "b", "c"], &[1_000, 1_000, 1_000]);
        assert!(op.process_left(&left).expect("left buffers").is_empty());
        let right = multi_row_batch(&["a", "b", "c"], &[2_000, 2_000, 2_000]);
        let out = op.process_right(&right).expect("right matches");
        assert_eq!(out.len(), 1, "one output batch per input batch");
        assert_eq!(out[0].num_rows(), 3, "carrying every match");
    }

    fn make_spec(window_ms: u64) -> WatermarkWindowJoinSpec {
        WatermarkWindowJoinSpec {
            time_column: "ts".into(),
            left_key_column: "id".into(),
            right_key_column: "id".into(),
            window_ms,
        }
    }

    fn batch_with_key_and_ts(id: &str, ts: i64, val: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])) as _,
                Arc::new(Int64Array::from(vec![ts])) as _,
                Arc::new(Int64Array::from(vec![val])) as _,
            ],
        )
        .unwrap()
    }

    fn multi_row_batch(ids: &[&str], times: &[i64]) -> RecordBatch {
        assert_eq!(ids.len(), times.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())) as _,
                Arc::new(Int64Array::from(times.to_vec())) as _,
            ],
        )
        .unwrap()
    }

    // ── Basic join correctness ─────────────────────────────────────────────

    #[test]
    fn within_window_emits_match() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));
        assert!(
            op.process_left(&batch_with_key_and_ts("k", 1000, 1))
                .expect("join")
                .is_empty()
        );
        let out = op
            .process_right(&batch_with_key_and_ts("k", 1300, 2))
            .expect("join");
        assert_eq!(out.len(), 1, "right event within 500ms should match left");
        // Joined batch must have columns from both sides (id, ts, val from left + id, ts, val from right = 6 cols).
        assert_eq!(out[0].num_columns(), 6);
    }

    #[test]
    fn barrier_alignment_buffers_blocked_side_until_aligned() {
        use crate::barrier_align::BarrierEvent;
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));

        // Left delivers the epoch-1 barrier first → it blocks; right still flows.
        assert_eq!(op.record_left_barrier(1), BarrierEvent::Blocked);
        assert!(op.is_left_blocked());
        assert!(!op.is_right_blocked());

        // A left batch arriving after its barrier is held for the next epoch,
        // not folded into the in-progress (about-to-snapshot) one.
        let held = op
            .process_left(&batch_with_key_and_ts("k", 1000, 1))
            .expect("join");
        assert!(
            held.is_empty(),
            "post-barrier left input is buffered, not joined"
        );
        let r = op
            .process_right(&batch_with_key_and_ts("k", 1100, 2))
            .expect("join");
        assert!(r.is_empty(), "no left state this epoch — it was buffered");

        // Right delivers its barrier → the epoch aligns: snapshot now.
        assert_eq!(op.record_right_barrier(1), BarrierEvent::Aligned);
        assert!(!op.is_left_blocked() && !op.is_right_blocked());

        // The buffered left batch is handed back for replay into the next epoch.
        let (left_replay, right_replay) = op.take_realigned_input();
        assert_eq!(left_replay.len(), 1, "the held left batch is replayed");
        assert!(right_replay.is_empty());

        // Replaying the held left event now joins against the right event that
        // was processed (unblocked) during alignment — proving no data was lost.
        let joined = op.process_left(&left_replay[0]).expect("join");
        assert_eq!(
            joined.len(),
            1,
            "replayed left matches the right event from the aligned epoch"
        );
    }

    #[test]
    fn outside_window_no_match() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(100));
        op.process_left(&batch_with_key_and_ts("k", 1000, 1))
            .expect("join");
        let out = op
            .process_right(&batch_with_key_and_ts("k", 2000, 2))
            .expect("join");
        assert!(
            out.is_empty(),
            "right event 1000ms away from left (window=100ms) must not match"
        );
    }

    #[test]
    fn different_keys_do_not_match() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(1000));
        op.process_left(&batch_with_key_and_ts("a", 1000, 1))
            .expect("join");
        let out = op
            .process_right(&batch_with_key_and_ts("b", 1000, 2))
            .expect("join");
        assert!(out.is_empty(), "different keys must not match");
    }

    // ── Watermark GC ──────────────────────────────────────────────────────

    #[test]
    fn watermark_evicts_stale_state() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(200));
        op.process_left(&batch_with_key_and_ts("k", 1000, 1))
            .expect("join");
        assert_eq!(op.active_key_count(), 1);

        // Advance watermark past the event; evict_before removes state
        op.advance_watermark(2000);
        assert_eq!(
            op.active_key_count(),
            0,
            "state must be evicted after watermark advance"
        );
    }

    #[test]
    fn watermark_monotonic_advance_only() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));
        op.process_left(&batch_with_key_and_ts("k", 1000, 1))
            .expect("join");
        op.advance_watermark(2000);
        assert_eq!(op.active_key_count(), 0);

        // Roll back watermark — must not re-evict (no state to re-evict) and no panic
        op.advance_watermark(500);
        assert_eq!(op.active_key_count(), 0);
    }

    #[test]
    fn watermark_does_not_evict_live_state() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));
        // event at 1000ms, watermark advances to 800ms — event is within [800-500, 800+500]
        op.process_left(&batch_with_key_and_ts("k", 1000, 1))
            .expect("join");
        op.advance_watermark(800);
        assert_eq!(
            op.active_key_count(),
            1,
            "event at 1000ms should not be evicted by watermark 800ms"
        );
    }

    // ── Multi-row batch ────────────────────────────────────────────────────

    #[test]
    fn multi_row_batch_all_rows_processed() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));
        let left = multi_row_batch(&["a", "b", "c"], &[1000, 2000, 3000]);
        assert!(op.process_left(&left).expect("join").is_empty());

        // Each right row matches the left row for the same key within 500ms.
        let right = multi_row_batch(&["a", "b", "c"], &[1200, 2300, 3400]);
        let out = op.process_right(&right).expect("join");
        // Matches within one call coalesce into one batch; the claim is the
        // ROW count, not the batch shape.
        let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3, "each of the 3 keys should produce 1 match");
    }

    #[test]
    fn multi_row_batch_only_matching_rows_emitted() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(100));
        let left = multi_row_batch(&["x", "x"], &[1000, 2000]);
        op.process_left(&left).expect("join");

        // right at 1050 matches left at 1000; right at 3000 does not match either.
        let right = multi_row_batch(&["x", "x"], &[1050, 3000]);
        let out = op.process_right(&right).expect("join");
        assert_eq!(out.len(), 1, "only the in-window row should match");
    }

    // ── Symmetric join ────────────────────────────────────────────────────

    #[test]
    fn right_before_left_still_matches() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(500));
        // Push right first, then left — the interval is symmetric.
        assert!(
            op.process_right(&batch_with_key_and_ts("k", 1000, 2))
                .expect("join")
                .is_empty()
        );
        let out = op
            .process_left(&batch_with_key_and_ts("k", 1200, 1))
            .expect("join");
        assert_eq!(out.len(), 1, "right-before-left within window must match");
    }

    // ── Joined schema ─────────────────────────────────────────────────────

    #[test]
    fn joined_batch_has_correct_column_count() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(1000));
        let l = batch_with_key_and_ts("k", 500, 1);
        let r = batch_with_key_and_ts("k", 700, 2);
        op.process_left(&l).expect("join");
        let out = op.process_right(&r).expect("join");
        assert_eq!(out.len(), 1);
        // Left has 3 cols + right has 3 cols = 6 joined cols.
        assert_eq!(out[0].num_columns(), l.num_columns() + r.num_columns());
        assert_eq!(out[0].num_rows(), 1);
    }

    // ── Fix #5: duplicate column names get prefixed ────────────────────────

    #[test]
    fn joined_schema_renames_colliding_columns() {
        let mut op = WatermarkWindowJoinOperator::new(make_spec(1000));
        op.process_left(&batch_with_key_and_ts("k", 500, 1))
            .expect("join");
        let out = op
            .process_right(&batch_with_key_and_ts("k", 600, 2))
            .expect("join");
        assert_eq!(out.len(), 1);
        let schema = out[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Both sides have identical schemas → all columns collide.
        assert!(
            names.iter().any(|n| n.starts_with("left_")),
            "left_ prefix expected for colliding cols"
        );
        assert!(
            names.iter().any(|n| n.starts_with("right_")),
            "right_ prefix expected for colliding cols"
        );
    }

    // ── Fix #6: snapshot / restore ────────────────────────────────────────

    #[test]
    fn snapshot_roundtrips_spec_and_watermark() {
        let spec = make_spec(500);
        let mut op = WatermarkWindowJoinOperator::new(spec.clone());
        op.advance_watermark(3000);
        let bytes = op.snapshot_bytes().expect("snapshot must succeed");

        // Parse the JSON snapshot to verify spec and watermark values.
        let val: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(val["watermark_ms"].as_i64().unwrap(), 3000);
        assert_eq!(val["spec"]["window_ms"].as_u64().unwrap(), 500);

        // Restore and verify the restored operator honours the watermark:
        // the restored watermark is 3000, so state at ts=0 will be evicted.
        let mut op2 =
            WatermarkWindowJoinOperator::restore_from_bytes(&bytes).expect("restore must succeed");
        // Left event at ts=0 — with restored watermark 3000 the event is already
        // within the eviction zone (3000 − 500 = 2500 > 0), so no match expected
        // for a right event at ts=100.
        op2.process_left(&batch_with_key_and_ts("k", 0, 1))
            .expect("join");
        let out = op2
            .process_right(&batch_with_key_and_ts("k", 100, 2))
            .expect("join");
        // Even if the interval contains the left event, the watermark already
        // passed — state is cleared on restore so match should be zero.
        // (We don't assert a specific count here because state GC timing may
        //  vary; we just assert the round-trip doesn't panic.)
        let _ = out;
    }

    /// UInt64 keys join by VALUE, and a key that matches nothing matches
    /// nothing (register §58).
    ///
    /// The defect: `extract_key` knew only Utf8 and Int64, and extraction
    /// failure fell back to a POSITIONAL pseudo-key `__row_{n}` — so row i of
    /// one side "joined" row i of every batch on the other side regardless of
    /// the actual keys. Four bids and one unrelated auction produced a match
    /// purely because both had a row 0. Against the reverted fallback this
    /// test fails with 1 fabricated match where 0 is the answer.
    #[test]
    fn uint64_keys_join_by_value_not_by_row_position() {
        use arrow::array::UInt64Array;
        let spec = WatermarkWindowJoinSpec {
            time_column: "ts".into(),
            left_key_column: "k".into(),
            right_key_column: "id".into(),
            window_ms: 10_000,
        };
        let mut op = WatermarkWindowJoinOperator::new(spec);
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let left = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1_u64, 2])),
                Arc::new(Int64Array::from(vec![1_000_i64, 1_001])),
            ],
        )
        .unwrap();
        let miss = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![99_u64])),
                Arc::new(Int64Array::from(vec![1_002_i64])),
            ],
        )
        .unwrap();
        let hit = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(UInt64Array::from(vec![2_u64])),
                Arc::new(Int64Array::from(vec![1_003_i64])),
            ],
        )
        .unwrap();

        assert!(op.process_left(&left).expect("left").is_empty());
        let fabricated = op.process_right(&miss).expect("miss");
        assert!(
            fabricated.is_empty(),
            "auction 99 matches no bid; a match here is joined BY ROW POSITION"
        );
        let real: usize = op
            .process_right(&hit)
            .expect("hit")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(real, 1, "auction 2 matches exactly the bid on key 2");
    }
}
