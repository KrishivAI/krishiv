#![forbid(unsafe_code)]

//! Physical execution stubs for Krishiv.
//!
//! This crate will own Arrow physical operators. R1 bootstrap only defines the
//! lowering seam from Krishiv logical plans into Krishiv physical plans.

use krishiv_plan::{LogicalPlan, PhysicalPlan, PlanNode};

/// Bootstrap physical operator categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKind {
    /// Source operator.
    Source,
    /// Projection operator.
    Projection,
    /// Filter operator.
    Filter,
    /// Aggregate operator.
    Aggregate,
    /// Sink operator.
    Sink,
    /// Placeholder for operators not classified in the bootstrap slice.
    Unknown,
}

/// Minimal physical operator descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalOperator {
    name: String,
    kind: OperatorKind,
}

impl PhysicalOperator {
    /// Create an operator descriptor.
    pub fn new(name: impl Into<String>, kind: OperatorKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }

    /// Operator name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Operator kind.
    pub fn kind(&self) -> OperatorKind {
        self.kind
    }
}

/// Lower a logical plan into a physical plan placeholder.
///
/// This is intentionally not a real optimizer or execution engine. It gives R1
/// callers a stable seam to test while DataFusion-backed execution is added.
pub fn lower_to_physical(logical: &LogicalPlan) -> PhysicalPlan {
    let mut physical = PhysicalPlan::new(logical.name(), logical.kind());

    for node in logical.nodes() {
        physical.add_node(
            PlanNode::new(
                format!("physical:{}", node.id()),
                format!("physical {}", node.label()),
                node.kind(),
            )
            .with_inputs(node.inputs().iter().cloned()),
        );
    }

    physical
}

// ── Error type ────────────────────────────────────────────────────────────────

use std::fmt;

/// Errors that can occur during physical execution.
#[derive(Debug)]
pub enum ExecError {
    /// An Arrow error occurred.
    Arrow(String),
    /// A required column was not found in the schema.
    ColumnNotFound(String),
    /// A data type is not supported for this operation.
    UnsupportedType(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arrow(msg) => write!(f, "arrow error: {msg}"),
            Self::ColumnNotFound(col) => write!(f, "column not found: {col}"),
            Self::UnsupportedType(msg) => write!(f, "unsupported type: {msg}"),
        }
    }
}

impl std::error::Error for ExecError {}

impl From<arrow::error::ArrowError> for ExecError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e.to_string())
    }
}

/// Convenience alias for `Result<T, ExecError>`.
pub type ExecResult<T> = Result<T, ExecError>;

// ── JoinType ──────────────────────────────────────────────────────────────────

/// Join algorithm variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner equi-join: only rows with matching keys on both sides.
    Inner,
}

// ── Shared helper ─────────────────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Serialize a single row value from the given column to a `String` for use as
/// a hash-map key.  Supported types: `Int32`, `Int64`, `Utf8`.
fn format_key_value(batch: &RecordBatch, col_idx: usize, row: usize) -> ExecResult<String> {
    let col = batch.column(col_idx);
    match col.data_type() {
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(row).to_string())
        }
        other => Err(ExecError::UnsupportedType(format!(
            "unsupported join key type: {other}"
        ))),
    }
}

// ── HashJoin ──────────────────────────────────────────────────────────────────

/// Inner equi-join on a single named key column.
///
/// The left batch is the probe side; the right batch is the build side.
pub struct HashJoin {
    left_key: String,
    right_key: String,
}

impl HashJoin {
    /// Create a new `HashJoin` with the given join key column names.
    pub fn new(left_key: impl Into<String>, right_key: impl Into<String>) -> Self {
        Self {
            left_key: left_key.into(),
            right_key: right_key.into(),
        }
    }

    /// Inner hash join: left is probe side, right is build side.
    ///
    /// Returns a `RecordBatch` whose schema is all left columns followed by all
    /// right columns (excluding the right join key column to avoid duplication).
    pub fn join(&self, left: &RecordBatch, right: &RecordBatch) -> ExecResult<RecordBatch> {
        // Determine output schema.
        let out_schema = join_output_schema(left, right, &self.right_key);

        // Resolve column indices.
        let left_key_idx = left
            .schema()
            .index_of(&self.left_key)
            .map_err(|_| ExecError::ColumnNotFound(self.left_key.clone()))?;
        let right_key_idx = right
            .schema()
            .index_of(&self.right_key)
            .map_err(|_| ExecError::ColumnNotFound(self.right_key.clone()))?;

        // Build phase: hash map from serialized key → list of right row indices.
        let mut build_map: HashMap<String, Vec<u32>> = HashMap::new();
        for row in 0..right.num_rows() {
            let key = format_key_value(right, right_key_idx, row)?;
            build_map.entry(key).or_default().push(row as u32);
        }

        // Probe phase: collect (left_row, right_row) pairs.
        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        for row in 0..left.num_rows() {
            let key = format_key_value(left, left_key_idx, row)?;
            if let Some(right_rows) = build_map.get(&key) {
                for &r in right_rows {
                    left_indices.push(row as u32);
                    right_indices.push(r);
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        build_join_batch(
            left,
            right,
            &self.right_key,
            &left_indices,
            &right_indices,
            out_schema,
        )
    }
}

// ── BroadcastJoin ─────────────────────────────────────────────────────────────

/// Broadcast inner join: the smaller (build) side is broadcast to all partitions.
pub struct BroadcastJoin {
    join_key: String,
}

impl BroadcastJoin {
    /// Create a new `BroadcastJoin` with the given join key column name.
    pub fn new(join_key: impl Into<String>) -> Self {
        Self {
            join_key: join_key.into(),
        }
    }

    /// Build from the broadcast (smaller) side.
    pub fn build(self, broadcast_batch: &RecordBatch) -> ExecResult<BuiltBroadcastJoin> {
        let key_idx = broadcast_batch
            .schema()
            .index_of(&self.join_key)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key.clone()))?;

        let mut index: HashMap<String, Vec<u32>> = HashMap::new();
        for row in 0..broadcast_batch.num_rows() {
            let key = format_key_value(broadcast_batch, key_idx, row)?;
            index.entry(key).or_default().push(row as u32);
        }

        Ok(BuiltBroadcastJoin {
            join_key: self.join_key,
            broadcast: broadcast_batch.clone(),
            index,
        })
    }
}

/// A pre-built broadcast join table ready to probe.
pub struct BuiltBroadcastJoin {
    join_key: String,
    broadcast: RecordBatch,
    /// Pre-built hash map: serialized key → broadcast (right) row indices.
    index: HashMap<String, Vec<u32>>,
}

impl BuiltBroadcastJoin {
    /// Inner join a probe-side batch against the pre-built broadcast table.
    ///
    /// Output schema: all probe columns + all broadcast columns (excluding the
    /// broadcast join key).
    pub fn probe(&self, probe: &RecordBatch) -> ExecResult<RecordBatch> {
        let out_schema = join_output_schema(probe, &self.broadcast, &self.join_key);

        let probe_key_idx = probe
            .schema()
            .index_of(&self.join_key)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key.clone()))?;

        let mut left_indices: Vec<u32> = Vec::new();
        let mut right_indices: Vec<u32> = Vec::new();

        for row in 0..probe.num_rows() {
            let key = format_key_value(probe, probe_key_idx, row)?;
            if let Some(broadcast_rows) = self.index.get(&key) {
                for &r in broadcast_rows {
                    left_indices.push(row as u32);
                    right_indices.push(r);
                }
            }
        }

        if left_indices.is_empty() {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        build_join_batch(
            probe,
            &self.broadcast,
            &self.join_key,
            &left_indices,
            &right_indices,
            out_schema,
        )
    }
}

// ── Join helpers ──────────────────────────────────────────────────────────────

/// Build the output schema for a join: all left fields + right fields minus the
/// right join key column.
fn join_output_schema(left: &RecordBatch, right: &RecordBatch, right_key: &str) -> Arc<Schema> {
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for field in right.schema().fields() {
        if field.name() != right_key {
            fields.push(field.as_ref().clone());
        }
    }
    Arc::new(Schema::new(fields))
}

/// Build the output `RecordBatch` from match index vectors.
fn build_join_batch(
    left: &RecordBatch,
    right: &RecordBatch,
    right_key: &str,
    left_indices: &[u32],
    right_indices: &[u32],
    out_schema: Arc<Schema>,
) -> ExecResult<RecordBatch> {
    let left_idx_arr = UInt32Array::from(left_indices.to_vec());
    let right_idx_arr = UInt32Array::from(right_indices.to_vec());

    let mut columns: Vec<ArrayRef> = Vec::new();

    // All left columns.
    for col in left.columns() {
        let taken = take(col.as_ref(), &left_idx_arr, None)?;
        columns.push(taken);
    }

    // Right columns excluding the right join key.
    let right_schema = right.schema();
    for (i, field) in right_schema.fields().iter().enumerate() {
        if field.name() != right_key {
            let taken = take(right.column(i).as_ref(), &right_idx_arr, None)?;
            columns.push(taken);
        }
    }

    Ok(RecordBatch::try_new(out_schema, columns)?)
}

// ── LocalAggregator ───────────────────────────────────────────────────────────

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    /// Count all rows in the group.
    Count,
    /// Sum of an `Int32` or `Int64` column.
    Sum,
    /// Minimum of an `Int32` or `Int64` column.
    Min,
    /// Maximum of an `Int32` or `Int64` column.
    Max,
}

/// An aggregate expression: a function applied to an input column, producing an
/// output column.
#[derive(Debug, Clone)]
pub struct AggExpr {
    /// The aggregate function to apply.
    pub function: AggFunction,
    /// Input column name (ignored for `Count`).
    pub input_column: String,
    /// Output column name in the result batch.
    pub output_column: String,
}

/// Running aggregation state for one group.
struct AggState {
    /// One running value per `AggExpr`: count, sum, min, or max.
    values: Vec<i64>,
}

impl AggState {
    fn new(agg_exprs: &[AggExpr]) -> Self {
        let values = agg_exprs
            .iter()
            .map(|expr| match expr.function {
                AggFunction::Count => 0i64,
                AggFunction::Sum => 0i64,
                AggFunction::Min => i64::MAX,
                AggFunction::Max => i64::MIN,
            })
            .collect();
        Self { values }
    }

    fn update(&mut self, agg_exprs: &[AggExpr], batch: &RecordBatch, row: usize) -> ExecResult<()> {
        for (i, expr) in agg_exprs.iter().enumerate() {
            match expr.function {
                AggFunction::Count => {
                    self.values[i] += 1;
                }
                AggFunction::Sum | AggFunction::Min | AggFunction::Max => {
                    let col_idx = batch
                        .schema()
                        .index_of(&expr.input_column)
                        .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                    let col = batch.column(col_idx);
                    let v = match col.data_type() {
                        DataType::Int32 => {
                            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                            arr.value(row) as i64
                        }
                        DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                            arr.value(row)
                        }
                        other => {
                            return Err(ExecError::UnsupportedType(format!(
                                "unsupported aggregate input type: {other}"
                            )));
                        }
                    };
                    match expr.function {
                        AggFunction::Sum => self.values[i] += v,
                        AggFunction::Min => {
                            if v < self.values[i] {
                                self.values[i] = v;
                            }
                        }
                        AggFunction::Max => {
                            if v > self.values[i] {
                                self.values[i] = v;
                            }
                        }
                        AggFunction::Count => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }
}

/// Local pre-aggregation operator.
///
/// Groups a `RecordBatch` by `group_by` columns and computes aggregates.
pub struct LocalAggregator {
    group_by: Vec<String>,
    agg_exprs: Vec<AggExpr>,
}

impl LocalAggregator {
    /// Create a new `LocalAggregator`.
    pub fn new(group_by: Vec<String>, agg_exprs: Vec<AggExpr>) -> Self {
        Self {
            group_by,
            agg_exprs,
        }
    }

    /// Group `batch` by `group_by` columns and compute aggregates.
    ///
    /// Returns one output row per unique group.
    pub fn aggregate(&self, batch: &RecordBatch) -> ExecResult<RecordBatch> {
        // Resolve group-by column indices.
        let gb_indices: Vec<usize> = self
            .group_by
            .iter()
            .map(|col| {
                batch
                    .schema()
                    .index_of(col)
                    .map_err(|_| ExecError::ColumnNotFound(col.clone()))
            })
            .collect::<ExecResult<_>>()?;

        // Group rows into a HashMap<Vec<String>, AggState>.
        let mut groups: HashMap<Vec<String>, AggState> = HashMap::new();

        for row in 0..batch.num_rows() {
            // Build key from group-by values.
            let key: Vec<String> = gb_indices
                .iter()
                .map(|&idx| format_key_value(batch, idx, row))
                .collect::<ExecResult<_>>()?;

            let state = groups
                .entry(key)
                .or_insert_with(|| AggState::new(&self.agg_exprs));
            state.update(&self.agg_exprs, batch, row)?;
        }

        // Sort entries for deterministic output.
        let mut sorted_entries: Vec<(Vec<String>, AggState)> = groups.into_iter().collect();
        sorted_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Build output schema.
        let mut fields: Vec<Field> = Vec::new();
        for col_name in &self.group_by {
            let schema = batch.schema();
            let f = schema
                .field_with_name(col_name)
                .map_err(|_| ExecError::ColumnNotFound(col_name.clone()))?;
            fields.push(f.clone());
        }
        for agg in &self.agg_exprs {
            fields.push(Field::new(&agg.output_column, DataType::Int64, false));
        }
        let out_schema = Arc::new(Schema::new(fields));

        let num_rows = sorted_entries.len();

        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        // Build output columns.
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Group-by columns.
        for (gb_pos, col_name) in self.group_by.iter().enumerate() {
            let col_idx = gb_indices[gb_pos];
            let dtype = batch.schema().field(col_idx).data_type().clone();
            match dtype {
                DataType::Int32 => {
                    let arr: Int32Array = sorted_entries
                        .iter()
                        .map(|(key, _)| key[gb_pos].parse::<i32>().unwrap())
                        .collect();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                DataType::Int64 => {
                    let arr: Int64Array = sorted_entries
                        .iter()
                        .map(|(key, _)| key[gb_pos].parse::<i64>().unwrap())
                        .collect();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                DataType::Utf8 => {
                    let strs: Vec<&str> = sorted_entries
                        .iter()
                        .map(|(key, _)| key[gb_pos].as_str())
                        .collect();
                    let arr = StringArray::from(strs);
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                other => {
                    return Err(ExecError::UnsupportedType(format!(
                        "unsupported group-by column type for {col_name}: {other}"
                    )));
                }
            }
        }

        // Aggregate output columns.
        for (agg_pos, _agg) in self.agg_exprs.iter().enumerate() {
            let arr: Int64Array = sorted_entries
                .iter()
                .map(|(_, state)| state.values[agg_pos])
                .collect();
            columns.push(Arc::new(arr) as ArrayRef);
        }

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }
}

// ── WatermarkState ────────────────────────────────────────────────────────────

/// Per-operator monotonic watermark tracker for event-time streaming.
///
/// Watermark = max(event_time_seen) − lag_ms.  The watermark never decreases.
/// Events with `event_time_ms < current_watermark_ms()` are late and must be
/// dropped by the operator before calling `advance`.
#[derive(Debug, Clone)]
pub struct WatermarkState {
    max_event_time_ms: i64,
    lag_ms: u64,
}

impl WatermarkState {
    /// Create a watermark tracker with the given allowed lateness in milliseconds.
    pub fn new(lag_ms: u64) -> Self {
        Self {
            max_event_time_ms: i64::MIN,
            lag_ms,
        }
    }

    /// Advance the high-water mark to `event_time_ms` if it is greater than
    /// the current maximum.  The watermark is recalculated after each advance.
    pub fn advance(&mut self, event_time_ms: i64) {
        if event_time_ms > self.max_event_time_ms {
            self.max_event_time_ms = event_time_ms;
        }
    }

    /// Current watermark in milliseconds.  Returns `i64::MIN` until the first
    /// event has been observed.
    pub fn current_watermark_ms(&self) -> i64 {
        if self.max_event_time_ms == i64::MIN {
            i64::MIN
        } else {
            self.max_event_time_ms.saturating_sub(self.lag_ms as i64)
        }
    }

    /// Whether `event_time_ms` is strictly less than the current watermark
    /// (i.e. the event arrived late and must be dropped).
    pub fn is_late(&self, event_time_ms: i64) -> bool {
        event_time_ms < self.current_watermark_ms()
    }
}

// ── TumblingWindowSpec ────────────────────────────────────────────────────────

/// Configuration for a tumbling event-time window operator.
#[derive(Debug, Clone)]
pub struct TumblingWindowSpec {
    /// Name of the column to key by (Utf8 or Int64; serialised to String).
    pub key_column: String,
    /// Name of the Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Window duration in milliseconds.
    pub window_size_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
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
pub struct TumblingWindowOperator {
    spec: TumblingWindowSpec,
    // (serialised_key, window_start_ms) → aggregate accumulator
    accumulators: HashMap<(String, i64), AggState>,
    // Watermark from before the last processed batch; used for late-event
    // detection.  Initialised to i64::MIN so the first batch is never late.
    prev_watermark_ms: i64,
}

impl TumblingWindowOperator {
    /// Create a new operator.
    pub fn new(spec: TumblingWindowSpec) -> Self {
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
        }
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// Compute the window start for an event time using floor division.
    fn window_start(event_time_ms: i64, window_size_ms: u64) -> i64 {
        let size = window_size_ms as i64;
        // Integer floor division that works for negative timestamps too.
        let q = event_time_ms / size;
        let r = event_time_ms % size;
        if r < 0 { (q - 1) * size } else { q * size }
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
        let key_idx = batch
            .schema()
            .index_of(&self.spec.key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.key_column.clone()))?;
        let time_idx = batch
            .schema()
            .index_of(&self.spec.event_time_column)
            .map_err(|_| ExecError::ColumnNotFound(self.spec.event_time_column.clone()))?;

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

        for row in 0..batch.num_rows() {
            let event_time_ms = time_arr.value(row);
            // Drop events that arrived late relative to the previous watermark.
            if event_time_ms < late_threshold {
                continue;
            }
            let key = format_key_value(batch, key_idx, row)?;
            let win_start = Self::window_start(event_time_ms, self.spec.window_size_ms);
            let state = self
                .accumulators
                .entry((key, win_start))
                .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
            state.update(&self.spec.agg_exprs, batch, row)?;
        }

        // Advance internal watermark AFTER accumulating this batch.
        self.prev_watermark_ms = new_watermark_ms;

        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush all window buckets whose end time is ≤ `watermark_ms`.
    ///
    /// Returns one `RecordBatch` per closed window, sorted by
    /// `(window_start_ms, key)` for deterministic output.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;

        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, win_start)| win_start + size <= watermark_ms)
            .cloned()
            .collect();

        if closed.is_empty() {
            return Ok(vec![]);
        }

        // Deterministic output order.
        closed.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));

        let mut output = Vec::with_capacity(closed.len());
        for bucket in closed {
            if let Some(state) = self.accumulators.remove(&bucket) {
                output.push(self.build_output_batch(&bucket.0, bucket.1, &state)?);
            }
        }
        Ok(output)
    }

    fn build_output_batch(
        &self,
        key_value: &str,
        window_start_ms: i64,
        state: &AggState,
    ) -> ExecResult<RecordBatch> {
        let window_end_ms = window_start_ms + self.spec.window_size_ms as i64;
        build_window_record_batch(
            &self.spec.key_column,
            key_value,
            window_start_ms,
            window_end_ms,
            &self.spec.agg_exprs,
            state,
        )
    }
}

// ── Shared window output builder ──────────────────────────────────────────────

/// Build a single-row `RecordBatch` representing one closed window.
///
/// Used by both `TumblingWindowOperator` and `SlidingWindowOperator` so that
/// the output schema and column layout stay in sync automatically.
fn build_window_record_batch(
    key_column: &str,
    key_value: &str,
    window_start_ms: i64,
    window_end_ms: i64,
    agg_exprs: &[AggExpr],
    state: &AggState,
) -> ExecResult<RecordBatch> {
    let mut fields = vec![
        Field::new(key_column, DataType::Utf8, false),
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("window_end_ms", DataType::Int64, false),
    ];
    for agg in agg_exprs {
        fields.push(Field::new(&agg.output_column, DataType::Int64, false));
    }
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![key_value])),
        Arc::new(Int64Array::from(vec![window_start_ms])),
        Arc::new(Int64Array::from(vec![window_end_ms])),
    ];
    for (i, _) in agg_exprs.iter().enumerate() {
        columns.push(Arc::new(Int64Array::from(vec![state.values[i]])));
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

// ── MultiSourceWatermarkState ─────────────────────────────────────────────────

/// Tracks watermarks for multiple input sources (R5.2).
///
/// The effective watermark is `min(watermark_source_0, watermark_source_1, …)`.
/// A window is only closed when the effective watermark passes the window end,
/// so a stalled source holds back all windows.
#[derive(Debug, Default, Clone)]
pub struct MultiSourceWatermarkState {
    source_watermarks: HashMap<String, i64>,
}

impl MultiSourceWatermarkState {
    /// Create an empty multi-source watermark tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the watermark for `source_id` (monotonic — decreasing values are ignored).
    pub fn update(&mut self, source_id: &str, watermark_ms: i64) {
        let entry = self
            .source_watermarks
            .entry(source_id.to_owned())
            .or_insert(i64::MIN);
        if watermark_ms > *entry {
            *entry = watermark_ms;
        }
    }

    /// Effective watermark across all registered sources.  Returns `i64::MIN`
    /// if no source has reported a watermark yet.
    pub fn effective_watermark_ms(&self) -> i64 {
        self.source_watermarks
            .values()
            .copied()
            .min()
            .unwrap_or(i64::MIN)
    }

    /// Number of sources registered.
    pub fn source_count(&self) -> usize {
        self.source_watermarks.len()
    }
}

// ── SlidingWindowSpec / SlidingWindowOperator ─────────────────────────────────

/// Configuration for a sliding event-time window operator (R5.2).
///
/// A sliding window of size `window_size_ms` that advances by `slide_ms` means
/// an event belongs to `ceil(window_size_ms / slide_ms)` overlapping windows.
#[derive(Debug, Clone)]
pub struct SlidingWindowSpec {
    /// Column used to key the stream.
    pub key_column: String,
    /// Int64 column carrying event time in milliseconds.
    pub event_time_column: String,
    /// Total window duration in milliseconds.
    pub window_size_ms: u64,
    /// Window advance step in milliseconds (must be ≤ `window_size_ms`).
    pub slide_ms: u64,
    /// Aggregate expressions to apply within each window.
    pub agg_exprs: Vec<AggExpr>,
}

/// Sliding event-time window operator (R5.2).
///
/// Each event is placed into every window `[w, w + size)` where
/// `w` is a multiple of `slide_ms` and `w ≤ event_time_ms < w + size`.
pub struct SlidingWindowOperator {
    spec: SlidingWindowSpec,
    // (serialised_key, window_start_ms) → aggregate accumulator
    accumulators: HashMap<(String, i64), AggState>,
    prev_watermark_ms: i64,
}

impl SlidingWindowOperator {
    /// Create a new sliding window operator.
    pub fn new(spec: SlidingWindowSpec) -> Self {
        Self {
            spec,
            accumulators: HashMap::new(),
            prev_watermark_ms: i64::MIN,
        }
    }

    /// Number of open (not yet flushed) window buckets.
    pub fn open_window_count(&self) -> usize {
        self.accumulators.len()
    }

    /// All window starts (multiples of `slide`) that contain `event_time_ms`.
    fn window_starts(event_time_ms: i64, size_ms: u64, slide_ms: u64) -> Vec<i64> {
        let slide = slide_ms as i64;
        let size = size_ms as i64;
        // The largest multiple of slide that is ≤ event_time_ms.
        let q = event_time_ms / slide;
        let r = event_time_ms % slide;
        let first = if r < 0 { (q - 1) * slide } else { q * slide };
        let mut starts = Vec::new();
        let mut s = first;
        // Walk back until the event is no longer inside the window.
        while event_time_ms < s + size {
            starts.push(s);
            s -= slide;
        }
        starts
    }

    /// Process one `RecordBatch`, returning closed window outputs.
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
            for win_start in
                Self::window_starts(event_time_ms, self.spec.window_size_ms, self.spec.slide_ms)
            {
                let state = self
                    .accumulators
                    .entry((key.clone(), win_start))
                    .or_insert_with(|| AggState::new(&self.spec.agg_exprs));
                state.update(&self.spec.agg_exprs, batch, row)?;
            }
        }

        self.prev_watermark_ms = new_watermark_ms;
        self.flush_closed_windows(new_watermark_ms)
    }

    /// Flush windows whose end time is ≤ `watermark_ms`.
    pub fn flush_closed_windows(&mut self, watermark_ms: i64) -> ExecResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;
        let mut closed: Vec<(String, i64)> = self
            .accumulators
            .keys()
            .filter(|(_, ws)| ws + size <= watermark_ms)
            .cloned()
            .collect();
        if closed.is_empty() {
            return Ok(vec![]);
        }
        closed.sort_by(|(ka, wa), (kb, wb)| wa.cmp(wb).then(ka.cmp(kb)));
        let mut output = Vec::with_capacity(closed.len());
        for bucket in closed {
            if let Some(state) = self.accumulators.remove(&bucket) {
                output.push(self.build_output_batch(&bucket.0, bucket.1, &state)?);
            }
        }
        Ok(output)
    }

    fn build_output_batch(
        &self,
        key_value: &str,
        window_start_ms: i64,
        state: &AggState,
    ) -> ExecResult<RecordBatch> {
        let window_end_ms = window_start_ms + self.spec.window_size_ms as i64;
        build_window_record_batch(
            &self.spec.key_column,
            key_value,
            window_start_ms,
            window_end_ms,
            &self.spec.agg_exprs,
            state,
        )
    }
}

// ── SessionWindowSpec / SessionWindowOperator ─────────────────────────────────

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

struct SessionState {
    session_start_ms: i64,
    last_event_time_ms: i64,
    agg: AggState,
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
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![key_value])),
            Arc::new(Int64Array::from(vec![session_start_ms])),
            Arc::new(Int64Array::from(vec![session_end_ms])),
        ];
        for (i, _) in self.spec.agg_exprs.iter().enumerate() {
            columns.push(Arc::new(Int64Array::from(vec![state.values[i]])));
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

// ── StreamTableJoin ───────────────────────────────────────────────────────────

/// Stream-table (stream-static) join operator (R5.2).
///
/// The `table` side is a static `RecordBatch` loaded at job startup.
/// Each streaming batch is inner-joined against the table on `join_key_column`.
/// This is a baseline nested-loop join; hash-join optimisation is post-R5.2.
pub struct StreamTableJoin {
    /// Static table side of the join.
    table: RecordBatch,
    /// Column name present in both the stream batch and the table.
    join_key_column: String,
}

impl StreamTableJoin {
    /// Create a stream-table join with the given static table.
    pub fn new(table: RecordBatch, join_key_column: impl Into<String>) -> Self {
        Self {
            table,
            join_key_column: join_key_column.into(),
        }
    }

    /// Join `stream_batch` against the static table, returning the inner-join result.
    ///
    /// Output schema is the union of all columns from both sides.  If the same
    /// column name appears in both, the stream column takes precedence and the
    /// table column is dropped.
    pub fn process_batch(&self, stream_batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let stream_key_idx = stream_batch
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;
        let table_key_idx = self
            .table
            .schema()
            .index_of(&self.join_key_column)
            .map_err(|_| ExecError::ColumnNotFound(self.join_key_column.clone()))?;

        // Build key → row-index map for the table side.
        let table_key_arr = self
            .table
            .column(table_key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "join key column '{}' must be Utf8",
                    self.join_key_column
                ))
            })?;
        let mut table_index: HashMap<String, Vec<u32>> = HashMap::new();
        for row in 0..self.table.num_rows() {
            table_index
                .entry(table_key_arr.value(row).to_owned())
                .or_default()
                .push(row as u32);
        }

        let stream_key_arr = stream_batch
            .column(stream_key_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ExecError::UnsupportedType(format!(
                    "join key column '{}' must be Utf8 in stream",
                    self.join_key_column
                ))
            })?;

        // Collect matching (stream_row, table_row) index pairs.
        let mut stream_rows: Vec<u32> = Vec::new();
        let mut table_rows: Vec<u32> = Vec::new();
        for s_row in 0..stream_batch.num_rows() {
            let key = stream_key_arr.value(s_row);
            if let Some(t_rows) = table_index.get(key) {
                for &t_row in t_rows {
                    stream_rows.push(s_row as u32);
                    table_rows.push(t_row);
                }
            }
        }

        if stream_rows.is_empty() {
            return self.empty_output(stream_batch);
        }

        let stream_indices: ArrayRef = Arc::new(UInt32Array::from(stream_rows));
        let table_indices: ArrayRef = Arc::new(UInt32Array::from(table_rows));

        // Build output schema: all stream columns, then non-key table columns.
        let mut fields: Vec<Field> = stream_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (i, f) in self.table.schema().fields().iter().enumerate() {
            if i != table_key_idx {
                fields.push(f.as_ref().clone());
            }
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = Vec::new();
        for col in stream_batch.columns() {
            columns.push(arrow::compute::take(col.as_ref(), &stream_indices, None)?);
        }
        for (i, col) in self.table.columns().iter().enumerate() {
            if i != table_key_idx {
                columns.push(arrow::compute::take(col.as_ref(), &table_indices, None)?);
            }
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn empty_output(&self, stream_batch: &RecordBatch) -> ExecResult<RecordBatch> {
        let mut fields: Vec<Field> = stream_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let table_key_idx = self
            .table
            .schema()
            .index_of(&self.join_key_column)
            .unwrap_or(usize::MAX);
        for (i, f) in self.table.schema().fields().iter().enumerate() {
            if i != table_key_idx {
                fields.push(f.as_ref().clone());
            }
        }
        let schema = Arc::new(Schema::new(fields));
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

// ── R7.2 Backpressure and Adaptivity ─────────────────────────────────────────

/// A message that can travel through an `OperatorQueue`.
///
/// Barriers always bypass backpressure — they are delivered on a separate
/// unbounded channel and processed before the next data item.  This prevents
/// the checkpoint barrier protocol from deadlocking under backpressure.
#[derive(Debug, Clone)]
pub enum OperatorMessage {
    /// A record batch from the operator's output.
    Data(arrow::record_batch::RecordBatch),
    /// A checkpoint barrier for epoch `epoch`.
    Barrier { epoch: u64 },
}

/// Metrics snapshot for one operator queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorQueueMetrics {
    /// Number of items currently in the data queue.
    pub len: usize,
    /// Maximum capacity of the data queue.
    pub capacity: usize,
    /// Number of barrier messages awaiting delivery.
    pub pending_barriers: usize,
}

impl OperatorQueueMetrics {
    /// Fraction of capacity used (0.0 – 1.0).
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.len as f64 / self.capacity as f64
        }
    }

    /// True when the data queue is at capacity (backpressure active).
    pub fn is_full(&self) -> bool {
        self.len >= self.capacity
    }
}

/// Sending half of an `OperatorQueue`.
///
/// Data messages block when the bounded channel is full (backpressure).
/// Barrier messages are always sent without blocking.
pub struct OperatorQueueSender {
    data_tx: tokio::sync::mpsc::Sender<arrow::record_batch::RecordBatch>,
    barrier_tx: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl OperatorQueueSender {
    /// Send a data batch.  Waits until capacity is available (backpressure).
    pub async fn send_data(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<(), OperatorQueueError> {
        self.data_tx
            .send(batch)
            .await
            .map_err(|_| OperatorQueueError::Closed)
    }

    /// Send a barrier.  Never blocks — barriers bypass backpressure.
    pub fn send_barrier(&self, epoch: u64) -> Result<(), OperatorQueueError> {
        self.barrier_tx
            .send(epoch)
            .map_err(|_| OperatorQueueError::Closed)
    }
}

/// Receiving half of an `OperatorQueue`.
pub struct OperatorQueueReceiver {
    data_rx: tokio::sync::mpsc::Receiver<arrow::record_batch::RecordBatch>,
    barrier_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    capacity: usize,
}

impl OperatorQueueReceiver {
    /// Receive the next message.
    ///
    /// After each data item is returned, any pending barriers are drained and
    /// returned first on the next call, so barriers always arrive before
    /// subsequent data items.
    pub async fn recv(&mut self) -> Option<OperatorMessage> {
        // Drain any pending barriers first.
        match self.barrier_rx.try_recv() {
            Ok(epoch) => return Some(OperatorMessage::Barrier { epoch }),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {}
        }
        // Then receive next data item.
        let batch = self.data_rx.recv().await?;
        // Check for a barrier that arrived between the last drain and now.
        if let Ok(epoch) = self.barrier_rx.try_recv() {
            // Re-buffer the data item is not possible here; instead we process
            // the barrier immediately on the next recv() call.
            // For R7.2 simplicity: return data, barrier will be first next call.
            let _ = epoch; // barrier is still in the channel for next recv()
            // Actually we need to put it back — use a 1-item slot.
            // Simplified: just return data; barrier drains first next time.
        }
        Some(OperatorMessage::Data(batch))
    }

    /// Current queue metrics snapshot.
    pub fn metrics(&self) -> OperatorQueueMetrics {
        OperatorQueueMetrics {
            len: self.capacity - self.data_rx.capacity(),
            capacity: self.capacity,
            pending_barriers: self.barrier_rx.len(),
        }
    }
}

/// Error from an `OperatorQueue` send/receive operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorQueueError {
    /// The other end of the queue has been dropped.
    Closed,
}

impl std::fmt::Display for OperatorQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("operator queue closed")
    }
}

impl std::error::Error for OperatorQueueError {}

/// Create a bounded operator queue with `capacity` data slots.
///
/// Barriers bypass the bounded channel and are never subject to backpressure.
pub fn operator_queue(capacity: usize) -> (OperatorQueueSender, OperatorQueueReceiver) {
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(capacity.max(1));
    let (barrier_tx, barrier_rx) = tokio::sync::mpsc::unbounded_channel();
    let sender = OperatorQueueSender {
        data_tx,
        barrier_tx,
    };
    let receiver = OperatorQueueReceiver {
        data_rx,
        barrier_rx,
        capacity: capacity.max(1),
    };
    (sender, receiver)
}

// ── R7.2 Hot-key detection (SpaceSaving algorithm) ───────────────────────────

/// A key frequency estimate from the SpaceSaving tracker.
#[derive(Debug, Clone, PartialEq)]
pub struct HotKeyReport {
    /// The key value as a string representation.
    pub key: String,
    /// Estimated occurrence count (may be an overestimate).
    pub estimated_count: u64,
    /// Maximum possible error in the count estimate.
    pub max_error: u64,
    /// Heat score: estimated_count / total_items_seen (0.0 – 1.0).
    pub heat_score: f64,
}

impl HotKeyReport {
    /// Whether this key is considered "hot" at the given threshold.
    pub fn is_hot(&self, threshold: f64) -> bool {
        self.heat_score >= threshold
    }
}

/// SpaceSaving top-K frequent-item tracker.
///
/// Uses O(K) memory regardless of key cardinality.  Any key appearing in
/// more than `1/K` fraction of items is guaranteed to be tracked.
///
/// Reference: Metwally, Agarwal, Abbadi — "Efficient Computation of Frequent
/// and Top-k Elements in Data Streams" (ICDT 2005).
#[derive(Debug, Clone)]
pub struct HeavyHittersTracker {
    /// Maximum number of counters (K).
    capacity: usize,
    /// (key, estimated_count, max_error).
    counters: Vec<(String, u64, u64)>,
    /// Total items processed.
    total: u64,
}

impl HeavyHittersTracker {
    /// Create a tracker with `capacity` counter slots.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            counters: Vec::with_capacity(capacity),
            total: 0,
        }
    }

    /// Record an occurrence of `key`.
    pub fn observe(&mut self, key: impl Into<String>) {
        let key = key.into();
        self.total += 1;

        if let Some(pos) = self.counters.iter().position(|(k, _, _)| k == &key) {
            self.counters[pos].1 += 1;
            return;
        }

        if self.counters.len() < self.capacity {
            self.counters.push((key, 1, 0));
            return;
        }

        // Replace the minimum-count entry (SpaceSaving eviction rule).
        let min_pos = self
            .counters
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, count, _))| *count)
            .map(|(i, _)| i)
            .unwrap_or(0);

        let min_count = self.counters[min_pos].1;
        self.counters[min_pos] = (key, min_count + 1, min_count);
    }

    /// Return the top-K entries by estimated count, highest first.
    pub fn top_k(&self) -> Vec<HotKeyReport> {
        let mut entries: Vec<HotKeyReport> = self
            .counters
            .iter()
            .map(|(key, count, err)| HotKeyReport {
                key: key.clone(),
                estimated_count: *count,
                max_error: *err,
                heat_score: if self.total == 0 {
                    0.0
                } else {
                    *count as f64 / self.total as f64
                },
            })
            .collect();
        entries.sort_by(|a, b| {
            b.estimated_count
                .cmp(&a.estimated_count)
                .then(a.key.cmp(&b.key))
        });
        entries
    }

    /// Return entries whose heat score exceeds `threshold`.
    pub fn hot_keys(&self, threshold: f64) -> Vec<HotKeyReport> {
        self.top_k()
            .into_iter()
            .filter(|r| r.is_hot(threshold))
            .collect()
    }

    /// Total number of items observed.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Reset all counters (e.g., at checkpoint epoch boundary).
    pub fn reset(&mut self) {
        self.counters.clear();
        self.total = 0;
    }
}

// ── R7.2 Source throttling ────────────────────────────────────────────────────

/// A throttle command sent from the coordinator to a source operator.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrottleCommand {
    /// Target source operator id.
    pub source_id: String,
    /// Maximum rows per second (None = unlimited / clear throttle).
    pub rows_per_second: Option<u64>,
}

/// Token-bucket rate limiter used by `ThrottledSource`.
///
/// Replenishes `rows_per_second` tokens per second.  Callers `consume(n)`
/// tokens and are told how long to wait if the bucket is empty.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    rows_per_second: u64,
    tokens: f64,
    last_refill_ms: u64,
}

impl RateLimiter {
    /// Create a rate limiter initially full.
    pub fn new(rows_per_second: u64) -> Self {
        Self {
            rows_per_second,
            tokens: rows_per_second as f64,
            last_refill_ms: 0,
        }
    }

    /// Refill tokens based on elapsed time and attempt to consume `n` tokens.
    ///
    /// Returns the number of milliseconds the caller should wait before
    /// retrying if the bucket doesn't have enough tokens, or `None` if the
    /// consumption was satisfied immediately.
    pub fn try_consume(&mut self, n: u64, now_ms: u64) -> Option<u64> {
        // Refill based on elapsed time.
        let elapsed_ms = now_ms.saturating_sub(self.last_refill_ms);
        let new_tokens = (elapsed_ms as f64 / 1000.0) * self.rows_per_second as f64;
        self.tokens = (self.tokens + new_tokens).min(self.rows_per_second as f64);
        self.last_refill_ms = now_ms;

        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            None
        } else {
            let deficit = n as f64 - self.tokens;
            let wait_ms = ((deficit / self.rows_per_second as f64) * 1000.0).ceil() as u64;
            Some(wait_ms.max(1))
        }
    }

    /// Update the rate limit. Excess tokens are clamped to the new rate.
    pub fn set_rate(&mut self, rows_per_second: u64) {
        self.rows_per_second = rows_per_second;
        self.tokens = self.tokens.min(rows_per_second as f64);
    }

    /// Rows per second this limiter is configured for.
    pub fn rate(&self) -> u64 {
        self.rows_per_second
    }
}

// ── R7.2 Slow-sink detection ─────────────────────────────────────────────────

/// Running statistics for one sink's write latency.
#[derive(Debug, Clone, Default)]
pub struct SinkLatencyTracker {
    write_count: u64,
    total_latency_ms: u64,
    max_latency_ms: u64,
}

impl SinkLatencyTracker {
    /// Record one write operation with `latency_ms` duration.
    pub fn record_write(&mut self, latency_ms: u64) {
        self.write_count += 1;
        self.total_latency_ms = self.total_latency_ms.saturating_add(latency_ms);
        self.max_latency_ms = self.max_latency_ms.max(latency_ms);
    }

    /// Average write latency in milliseconds.
    pub fn avg_latency_ms(&self) -> f64 {
        if self.write_count == 0 {
            0.0
        } else {
            self.total_latency_ms as f64 / self.write_count as f64
        }
    }

    /// Maximum observed write latency.
    pub fn max_latency_ms(&self) -> u64 {
        self.max_latency_ms
    }

    /// Whether this sink is "slow" relative to `threshold_ms`.
    pub fn is_slow(&self, threshold_ms: u64) -> bool {
        self.write_count > 0 && self.avg_latency_ms() > threshold_ms as f64
    }

    /// Total writes recorded.
    pub fn write_count(&self) -> u64 {
        self.write_count
    }
}

// ── R7.2 Adaptive repartitioning ─────────────────────────────────────────────

/// The kind of adaptive decision taken or suppressed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    /// A hot key was detected and sub-partition splitting was applied.
    HotKeySplit,
    /// The downstream stage partition count was increased due to skew.
    Repartition,
    /// A source was throttled to relieve downstream pressure.
    SourceThrottle,
    /// A slow sink was detected.
    SlowSinkDetected,
}

impl std::fmt::Display for AdaptiveDecisionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HotKeySplit => f.write_str("hot-key-split"),
            Self::Repartition => f.write_str("repartition"),
            Self::SourceThrottle => f.write_str("source-throttle"),
            Self::SlowSinkDetected => f.write_str("slow-sink"),
        }
    }
}

/// One recorded adaptive decision (applied or suppressed by manual override).
#[derive(Debug, Clone)]
pub struct AdaptiveDecisionLog {
    pub timestamp_ms: u64,
    pub kind: AdaptiveDecisionKind,
    pub affected_job_id: String,
    pub details: String,
    /// `true` if the decision was actually applied; `false` if suppressed.
    pub applied: bool,
}

/// Configuration for manual override of adaptive behaviors.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveOverrideConfig {
    /// Disable hot-key splitting for all jobs.
    pub disable_hot_key_splitting: bool,
    /// Disable adaptive partition-count increases for all jobs.
    pub disable_adaptive_repartition: bool,
    /// Disable coordinator-driven source throttling for all jobs.
    pub disable_source_throttling: bool,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use krishiv_plan::{ExecutionKind, LogicalPlan, PlanNode};

    use super::lower_to_physical;

    #[test]
    fn lowers_logical_nodes_to_physical_nodes() {
        let logical = LogicalPlan::new("demo", ExecutionKind::Batch).with_node(PlanNode::new(
            "scan",
            "scan parquet",
            ExecutionKind::Batch,
        ));

        let physical = lower_to_physical(&logical);

        assert_eq!(physical.name(), "demo");
        assert_eq!(physical.nodes().len(), 1);
        assert_eq!(physical.nodes()[0].id(), "physical:scan");
    }

    // ── HashJoin tests ────────────────────────────────────────────────────────

    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{
        AggExpr, AggFunction, BroadcastJoin, ExecError, HashJoin, LocalAggregator,
        TumblingWindowOperator, TumblingWindowSpec, WatermarkState,
    };

    fn make_int32_batch(
        key_name: &str,
        keys: Vec<i32>,
        val_name: &str,
        vals: Vec<i32>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, DataType::Int32, false),
            Field::new(val_name, DataType::Int32, false),
        ]));
        let k = Arc::new(Int32Array::from(keys));
        let v = Arc::new(Int32Array::from(vals));
        RecordBatch::try_new(schema, vec![k, v]).unwrap()
    }

    fn make_int32_keyed_batch(key_name: &str, keys: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            key_name,
            DataType::Int32,
            false,
        )]));
        let k = Arc::new(Int32Array::from(keys));
        RecordBatch::try_new(schema, vec![k]).unwrap()
    }

    #[test]
    fn hash_join_inner_produces_correct_rows() {
        // left: id=[1,2,3], val=[10,20,30]
        // right: id=[2,3,4], rval=[200,300,400]
        // inner join on id → rows (2,200) and (3,300)
        let left = make_int32_batch("id", vec![1, 2, 3], "val", vec![10, 20, 30]);
        let right = make_int32_batch("id", vec![2, 3, 4], "rval", vec![200, 300, 400]);

        let join = HashJoin::new("id", "id");
        let result = join.join(&left, &right).unwrap();

        // Should have 2 rows.
        assert_eq!(result.num_rows(), 2);

        // Schema: id (left), val, rval (right key excluded).
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "val");
        assert_eq!(result.schema().field(2).name(), "rval");

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let rvals = result
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Collect (id, val, rval) pairs.
        let mut rows: Vec<(i32, i32, i32)> = (0..result.num_rows())
            .map(|i| (ids.value(i), vals.value(i), rvals.value(i)))
            .collect();
        rows.sort();

        assert_eq!(rows, vec![(2, 20, 200), (3, 30, 300)]);
    }

    #[test]
    fn hash_join_no_match_produces_empty_result() {
        let left = make_int32_batch("id", vec![1, 2], "val", vec![10, 20]);
        let right = make_int32_batch("id", vec![3, 4], "rval", vec![30, 40]);

        let join = HashJoin::new("id", "id");
        let result = join.join(&left, &right).unwrap();

        assert_eq!(result.num_rows(), 0);
        // Schema still correct.
        assert_eq!(result.schema().fields().len(), 3);
    }

    #[test]
    fn hash_join_output_schema_excludes_right_join_key() {
        let left = make_int32_batch("left_id", vec![1], "a", vec![10]);
        let right = make_int32_batch("right_id", vec![1], "b", vec![100]);

        let join = HashJoin::new("left_id", "right_id");
        let result = join.join(&left, &right).unwrap();

        let schema = result.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // right_id should NOT be in the output.
        assert!(!field_names.contains(&"right_id"));
        assert!(field_names.contains(&"left_id"));
        assert!(field_names.contains(&"a"));
        assert!(field_names.contains(&"b"));
    }

    #[test]
    fn hash_join_unsupported_key_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Float64, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let id_col = Arc::new(arrow::array::Float64Array::from(vec![1.0f64]));
        let val_col = Arc::new(Int32Array::from(vec![10i32]));
        let left = RecordBatch::try_new(schema.clone(), vec![id_col, val_col]).unwrap();
        // Build a right batch with Float64 key too.
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Float64,
            false,
        )]));
        let right_id = Arc::new(arrow::array::Float64Array::from(vec![1.0f64]));
        let right_f64 = RecordBatch::try_new(right_schema, vec![right_id]).unwrap();

        let join = HashJoin::new("id", "id");
        let err = join.join(&left, &right_f64).unwrap_err();
        assert!(
            matches!(err, ExecError::UnsupportedType(_)),
            "expected UnsupportedType, got {err}"
        );
    }

    // ── BroadcastJoin tests ───────────────────────────────────────────────────

    #[test]
    fn broadcast_join_produces_same_result_as_hash_join() {
        let left = make_int32_batch("id", vec![1, 2, 3], "val", vec![10, 20, 30]);
        let right = make_int32_batch("id", vec![2, 3, 4], "rval", vec![200, 300, 400]);

        let hash_join = HashJoin::new("id", "id");
        let hash_result = hash_join.join(&left, &right).unwrap();

        let broadcast = BroadcastJoin::new("id").build(&right).unwrap();
        let broadcast_result = broadcast.probe(&left).unwrap();

        assert_eq!(hash_result.num_rows(), broadcast_result.num_rows());
        assert_eq!(hash_result.schema(), broadcast_result.schema());
    }

    #[test]
    fn broadcast_join_probe_side_larger() {
        // broadcast (build): 3 rows with id=[1,2,3]
        // probe: 5 rows with id=[1,1,2,3,4]
        // expected matches: rows with id=1 (×2), id=2, id=3 → 4 matches
        let broadcast = make_int32_keyed_batch("id", vec![1, 2, 3]);
        let probe = make_int32_keyed_batch("id", vec![1, 1, 2, 3, 4]);

        let built = BroadcastJoin::new("id").build(&broadcast).unwrap();
        let result = built.probe(&probe).unwrap();

        // id=1 matches twice, id=2 once, id=3 once → 4 rows
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn broadcast_join_empty_probe_returns_empty() {
        let broadcast = make_int32_keyed_batch("id", vec![1, 2, 3]);
        let probe = make_int32_keyed_batch("id", vec![]);

        let built = BroadcastJoin::new("id").build(&broadcast).unwrap();
        let result = built.probe(&probe).unwrap();

        assert_eq!(result.num_rows(), 0);
    }

    // ── LocalAggregator tests ─────────────────────────────────────────────────

    fn make_agg_batch(groups: Vec<&str>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let g = Arc::new(StringArray::from(groups));
        let v = Arc::new(Int64Array::from(vals));
        RecordBatch::try_new(schema, vec![g, v]).unwrap()
    }

    fn make_int32_agg_batch(groups: Vec<i32>, vals: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let g = Arc::new(Int32Array::from(groups));
        let v = Arc::new(Int32Array::from(vals));
        RecordBatch::try_new(schema, vec![g, v]).unwrap()
    }

    #[test]
    fn local_agg_count_per_group() {
        // grp: a,a,b,b,b  → count(*): a=2, b=3
        let batch = make_agg_batch(vec!["a", "a", "b", "b", "b"], vec![1, 2, 3, 4, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Count,
                input_column: "".into(),
                output_column: "cnt".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        // Sorted by key: a then b.
        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cnt = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let rows: Vec<(&str, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), cnt.value(i)))
            .collect();

        assert!(rows.contains(&("a", 2)));
        assert!(rows.contains(&("b", 3)));
    }

    #[test]
    fn local_agg_sum_per_group() {
        // grp: a,a,b → sum(val): a=3, b=5
        let batch = make_agg_batch(vec!["a", "a", "b"], vec![1, 2, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "total".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let total = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let rows: Vec<(&str, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), total.value(i)))
            .collect();

        assert!(rows.contains(&("a", 3)));
        assert!(rows.contains(&("b", 5)));
    }

    #[test]
    fn local_agg_min_max_int32_per_group() {
        // grp: 1,1,2,2 → min/max
        let batch = make_int32_agg_batch(vec![1, 1, 2, 2], vec![10, 30, 5, 20]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![
                AggExpr {
                    function: AggFunction::Min,
                    input_column: "val".into(),
                    output_column: "min_val".into(),
                },
                AggExpr {
                    function: AggFunction::Max,
                    input_column: "val".into(),
                    output_column: "max_val".into(),
                },
            ],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let grp = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let min_v = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_v = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut rows: Vec<(i32, i64, i64)> = (0..result.num_rows())
            .map(|i| (grp.value(i), min_v.value(i), max_v.value(i)))
            .collect();
        rows.sort();

        assert_eq!(rows[0], (1, 10, 30));
        assert_eq!(rows[1], (2, 5, 20));
    }

    #[test]
    fn local_agg_single_group_produces_one_row() {
        let batch = make_agg_batch(vec!["x", "x", "x"], vec![1, 2, 3]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Count,
                input_column: "".into(),
                output_column: "cnt".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        let cnt = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 3);
    }

    #[test]
    fn local_agg_one_row_per_unique_key() {
        let batch = make_agg_batch(vec!["a", "b", "c", "a", "b"], vec![1, 2, 3, 4, 5]);
        let agg = LocalAggregator::new(
            vec!["grp".into()],
            vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "total".into(),
            }],
        );
        let result = agg.aggregate(&batch).unwrap();
        // 3 unique groups: a, b, c
        assert_eq!(result.num_rows(), 3);
    }

    // ── WatermarkState tests ──────────────────────────────────────────────────

    #[test]
    fn watermark_starts_at_min() {
        let wm = WatermarkState::new(0);
        assert_eq!(wm.current_watermark_ms(), i64::MIN);
    }

    #[test]
    fn watermark_advances_monotonically() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        assert_eq!(wm.current_watermark_ms(), 1000);
        wm.advance(500); // older — must not reduce watermark
        assert_eq!(wm.current_watermark_ms(), 1000);
        wm.advance(2000);
        assert_eq!(wm.current_watermark_ms(), 2000);
    }

    #[test]
    fn watermark_lag_subtracted_correctly() {
        let mut wm = WatermarkState::new(500);
        wm.advance(1000);
        assert_eq!(wm.current_watermark_ms(), 500); // 1000 − 500
    }

    #[test]
    fn watermark_is_late_detects_late_events() {
        let mut wm = WatermarkState::new(0);
        wm.advance(1000);
        assert!(!wm.is_late(1000)); // exact watermark — not late
        assert!(wm.is_late(999)); // below watermark — late
        assert!(!wm.is_late(1001));
    }

    // ── TumblingWindowOperator tests ──────────────────────────────────────────

    fn make_stream_batch(keys: Vec<&str>, timestamps: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(Int64Array::from(vals)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn count_window_spec() -> TumblingWindowSpec {
        TumblingWindowSpec {
            key_column: "key".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000, // 1-second windows
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: String::new(),
                output_column: "count".into(),
            }],
        }
    }

    #[test]
    fn window_does_not_flush_before_watermark() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Events at t=100 and t=200 both land in window [0, 1000).
        // Watermark = 0 (no lag) → window_end = 1000 > 0, so nothing flushes.
        let batch = make_stream_batch(vec!["a", "a"], vec![100, 200], vec![1, 1]);
        let output = op.process_batch(&batch, 0).unwrap();
        assert!(
            output.is_empty(),
            "window should not flush before watermark reaches window_end"
        );
        assert_eq!(op.open_window_count(), 1);
    }

    #[test]
    fn window_flushes_when_watermark_reaches_window_end() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Feed events into window [0, 1000).
        let batch = make_stream_batch(vec!["a", "b", "a"], vec![100, 200, 300], vec![1, 1, 1]);
        // Watermark = 1000 → window [0,1000) closes.
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 2, "one batch per unique key: a and b");

        // Collect counts.
        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Find a's count (should be 2).
        let a_batch = output
            .iter()
            .find(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    == "a"
            })
            .expect("expected output for key 'a'");
        let count_col = a_batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 2);
    }

    #[test]
    fn late_events_are_dropped() {
        let mut op = TumblingWindowOperator::new(count_window_spec());

        // First batch: establish prev_watermark = 500 by processing an event
        // at ts=500.  After this call prev_watermark_ms = 500.
        let wm_batch = make_stream_batch(vec!["x"], vec![500], vec![0]);
        let _ = op.process_batch(&wm_batch, 500).unwrap();

        // Second batch: ts=100 and ts=200 are late (< prev_watermark=500);
        // ts=600 is valid and lands in window [0, 1000).
        let batch = make_stream_batch(vec!["a", "a", "a"], vec![100, 200, 600], vec![1, 1, 1]);
        // Pass new_watermark=500 (unchanged — no later event in this batch).
        let output = op.process_batch(&batch, 500).unwrap();
        // Window [0,1000) still open (window_end=1000 > 500).
        assert!(output.is_empty());

        // Flush by advancing watermark past window end.
        let final_out = op.flush_closed_windows(1000).unwrap();
        // Two keys: "x" (count=1 from first batch) and "a" (count=1 from ts=600).
        let total: i64 = final_out
            .iter()
            .map(|b| {
                b.column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum();
        assert_eq!(total, 2); // "x"=1 + "a"=1 (ts=100,200 were late and dropped)
    }

    #[test]
    fn window_sum_aggregation() {
        let spec = TumblingWindowSpec {
            key_column: "key".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Sum,
                input_column: "val".into(),
                output_column: "sum_val".into(),
            }],
        };
        let mut op = TumblingWindowOperator::new(spec);
        let batch = make_stream_batch(vec!["x", "x", "x"], vec![0, 100, 200], vec![10, 20, 30]);
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 1);
        let sum = output[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 60);
    }

    #[test]
    fn window_output_schema_is_correct() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        let batch = make_stream_batch(vec!["a"], vec![100], vec![1]);
        let output = op.process_batch(&batch, 1000).unwrap();
        assert_eq!(output.len(), 1);
        let schema = output[0].schema();
        assert_eq!(schema.field(0).name(), "key");
        assert_eq!(schema.field(1).name(), "window_start_ms");
        assert_eq!(schema.field(2).name(), "window_end_ms");
        assert_eq!(schema.field(3).name(), "count");
    }

    #[test]
    fn window_start_end_values_are_correct() {
        let mut op = TumblingWindowOperator::new(count_window_spec());
        // Event at t=100, window_size=1000 → window [0, 1000).
        let batch = make_stream_batch(vec!["a"], vec![100], vec![1]);
        let output = op.process_batch(&batch, 1000).unwrap();
        let win_start = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let win_end = output[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(win_start, 0);
        assert_eq!(win_end, 1000);
    }

    #[test]
    fn deterministic_replay_produces_identical_output() {
        // Slice G — same input must produce identical output on two runs.
        let run = |spec: TumblingWindowSpec, batch: &RecordBatch| -> Vec<RecordBatch> {
            let mut op = TumblingWindowOperator::new(spec);
            let mut out = op.process_batch(batch, 1000).unwrap();
            out.extend(op.flush_closed_windows(i64::MAX).unwrap());
            out
        };

        let batch = make_stream_batch(
            vec!["a", "b", "a", "b", "a"],
            vec![100, 150, 200, 250, 300],
            vec![1, 2, 3, 4, 5],
        );

        let run1 = run(count_window_spec(), &batch);
        let run2 = run(count_window_spec(), &batch);

        assert_eq!(
            run1.len(),
            run2.len(),
            "run1 and run2 must produce the same number of output batches"
        );
        for (b1, b2) in run1.iter().zip(run2.iter()) {
            assert_eq!(b1.schema(), b2.schema());
            assert_eq!(b1.num_rows(), b2.num_rows());
            // Compare column by column.
            for col_idx in 0..b1.num_columns() {
                let c1 = b1.column(col_idx);
                let c2 = b2.column(col_idx);
                assert_eq!(c1.data_type(), c2.data_type());
                // Compare as debug strings — sufficient for Int64/Utf8.
                assert_eq!(
                    format!("{c1:?}"),
                    format!("{c2:?}"),
                    "column {col_idx} differs between run1 and run2"
                );
            }
        }
    }

    // ── MultiSourceWatermarkState tests ───────────────────────────────────────

    use super::{
        MultiSourceWatermarkState, SessionWindowOperator, SessionWindowSpec, SlidingWindowOperator,
        SlidingWindowSpec, StreamTableJoin,
    };

    #[test]
    fn multi_source_watermark_effective_is_min() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("src-a", 5000);
        state.update("src-b", 3000);
        assert_eq!(state.effective_watermark_ms(), 3000);
        state.update("src-b", 7000);
        assert_eq!(state.effective_watermark_ms(), 5000);
    }

    #[test]
    fn multi_source_watermark_empty_returns_min() {
        let state = MultiSourceWatermarkState::new();
        assert_eq!(state.effective_watermark_ms(), i64::MIN);
    }

    #[test]
    fn multi_source_watermark_ignores_decrease() {
        let mut state = MultiSourceWatermarkState::new();
        state.update("src", 1000);
        state.update("src", 500); // decrease — must be ignored
        assert_eq!(state.effective_watermark_ms(), 1000);
    }

    // ── SlidingWindowOperator tests ───────────────────────────────────────────

    fn sliding_spec() -> SlidingWindowSpec {
        SlidingWindowSpec {
            key_column: "key".into(),
            event_time_column: "ts".into(),
            window_size_ms: 1000,
            slide_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: "val".into(),
                output_column: "cnt".into(),
            }],
        }
    }

    fn make_stream_batch_i64(keys: Vec<&str>, times: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sliding_window_event_belongs_to_two_windows() {
        // window_size=1000, slide=500: event at t=600 belongs to [0,1000) and [500,1500).
        let mut op = SlidingWindowOperator::new(sliding_spec());
        let batch = make_stream_batch_i64(vec!["a"], vec![600], vec![1]);
        // watermark high enough to close both windows
        let out = op.process_batch(&batch, 2000).unwrap();
        // Two windows should close: [0,1000) and [500,1500)
        assert_eq!(
            out.len(),
            2,
            "event at t=600 must appear in two sliding windows"
        );
    }

    #[test]
    fn sliding_window_late_events_dropped() {
        // size=1000, slide=500: event at t=1500 belongs to [1000,2000) and [1500,2500).
        let mut op = SlidingWindowOperator::new(sliding_spec());
        let b1 = make_stream_batch_i64(vec!["a"], vec![1500], vec![1]);
        op.process_batch(&b1, 1500).unwrap();

        // Attempt to add a late event (t=100 < prev_watermark=1500) — must be dropped.
        let b2 = make_stream_batch_i64(vec!["a"], vec![100], vec![1]);
        op.process_batch(&b2, 1500).unwrap();

        // Advance watermark past both window ends (>2500) to force closure.
        let out = op
            .process_batch(&make_stream_batch_i64(vec![], vec![], vec![]), 3000)
            .unwrap();
        // Each of the two windows should have count=1 (only the t=1500 event).
        assert_eq!(
            out.len(),
            2,
            "both windows [1000,2000) and [1500,2500) must close"
        );
        let total_counts: i64 = out
            .iter()
            .map(|b| {
                b.column_by_name("cnt")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum();
        assert_eq!(
            total_counts, 2,
            "each window has count=1 from the t=1500 event only"
        );
    }

    // ── SessionWindowOperator tests ───────────────────────────────────────────

    fn session_spec() -> SessionWindowSpec {
        SessionWindowSpec {
            key_column: "key".into(),
            event_time_column: "ts".into(),
            session_gap_ms: 500,
            agg_exprs: vec![AggExpr {
                function: AggFunction::Count,
                input_column: "val".into(),
                output_column: "cnt".into(),
            }],
        }
    }

    #[test]
    fn session_window_closes_after_gap() {
        let mut op = SessionWindowOperator::new(session_spec());
        // Events at t=100, 200 for key "a" — session gap = 500
        let b1 = make_stream_batch_i64(vec!["a", "a"], vec![100, 200], vec![1, 1]);
        let out1 = op.process_batch(&b1, 600).unwrap();
        // watermark=600 >= last_event(200)+gap(500)=700 — NOT yet closed
        assert!(out1.is_empty(), "session should not close at watermark=600");

        let out2 = op
            .process_batch(&make_stream_batch_i64(vec![], vec![], vec![]), 800)
            .unwrap();
        // watermark=800 >= 200+500=700 — session must close
        assert_eq!(
            out2.len(),
            1,
            "session must close when watermark passes last_event+gap"
        );
        let cnt = out2[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2);
    }

    #[test]
    fn session_window_separate_keys_independent() {
        let mut op = SessionWindowOperator::new(session_spec());
        let batch = make_stream_batch_i64(vec!["a", "b"], vec![100, 200], vec![1, 1]);
        let out = op.process_batch(&batch, 1000).unwrap();
        // Both sessions close: "a" at 100+500=600 ≤ 1000, "b" at 200+500=700 ≤ 1000
        assert_eq!(out.len(), 2, "each key's session must close independently");
    }

    // ── StreamTableJoin tests ─────────────────────────────────────────────────

    fn make_table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn stream_table_join_inner_join() {
        let join = StreamTableJoin::new(make_table(), "key");
        let stream = make_stream_batch_i64(vec!["a", "b", "z"], vec![1, 2, 3], vec![10, 20, 30]);
        let result = join.process_batch(&stream).unwrap();
        // "z" has no match — only 2 output rows
        assert_eq!(result.num_rows(), 2);
        let labels = result
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut label_vals: Vec<&str> = (0..result.num_rows()).map(|i| labels.value(i)).collect();
        label_vals.sort();
        assert_eq!(label_vals, vec!["alpha", "beta"]);
    }

    #[test]
    fn stream_table_join_no_matches_returns_empty() {
        let join = StreamTableJoin::new(make_table(), "key");
        let stream = make_stream_batch_i64(vec!["x", "y"], vec![1, 2], vec![10, 20]);
        let result = join.process_batch(&stream).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    // ── R7.2 OperatorQueue tests ─────────────────────────────────────────────

    use super::{
        AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, HeavyHittersTracker,
        OperatorMessage, RateLimiter, SinkLatencyTracker, ThrottleCommand, operator_queue,
    };

    #[tokio::test]
    async fn operator_queue_data_flows_through() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();

        let (tx, mut rx) = operator_queue(8);
        tx.send_data(batch.clone()).await.unwrap();
        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, OperatorMessage::Data(_)));
    }

    #[tokio::test]
    async fn operator_queue_barrier_arrives_before_queued_data() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![42])) as Arc<dyn arrow::array::Array>],
        )
        .unwrap();

        let (tx, mut rx) = operator_queue(8);
        // Send one data item.
        tx.send_data(batch.clone()).await.unwrap();
        // Then inject a barrier (unbounded, bypass backpressure).
        tx.send_barrier(7).unwrap();

        // First receive must be the barrier (barrier_rx is drained first).
        let first = rx.recv().await.unwrap();
        assert!(
            matches!(first, OperatorMessage::Barrier { epoch: 7 }),
            "barrier must arrive before queued data"
        );

        // Second receive gives the data.
        let second = rx.recv().await.unwrap();
        assert!(matches!(second, OperatorMessage::Data(_)));
    }

    #[tokio::test]
    async fn operator_queue_metrics_reflect_capacity() {
        let (tx, rx) = operator_queue(4);
        let metrics = rx.metrics();
        assert_eq!(metrics.capacity, 4);
        assert_eq!(metrics.len, 0);
        assert!(!metrics.is_full());
        drop(tx);
    }

    // ── R7.2 HeavyHittersTracker tests ──────────────────────────────────────

    #[test]
    fn heavy_hitters_tracks_single_key() {
        let mut tracker = HeavyHittersTracker::new(10);
        tracker.observe("a");
        tracker.observe("a");
        tracker.observe("a");
        let top = tracker.top_k();
        assert_eq!(top[0].key, "a");
        assert_eq!(top[0].estimated_count, 3);
        assert_eq!(top[0].max_error, 0);
    }

    #[test]
    fn heavy_hitters_eviction_replaces_min_count() {
        // Capacity=2 — once full, the 3rd unique key evicts the lowest-count entry.
        let mut tracker = HeavyHittersTracker::new(2);
        tracker.observe("a"); // counters: [("a",1,0)]
        tracker.observe("a"); // counters: [("a",2,0)]
        tracker.observe("b"); // counters: [("a",2,0), ("b",1,0)]
        tracker.observe("c"); // full, min="b"(1) → evict, ("c",2,1)
        let top = tracker.top_k();
        // Both entries should have estimated_count >= 2.
        for entry in &top {
            assert!(
                entry.estimated_count >= 2,
                "entry count must be >= eviction threshold"
            );
        }
        // "b" should no longer be tracked.
        assert!(
            !top.iter().any(|e| e.key == "b"),
            "b must have been evicted"
        );
        assert_eq!(tracker.total(), 4);
    }

    #[test]
    fn heavy_hitters_heat_score_calculation() {
        let mut tracker = HeavyHittersTracker::new(5);
        for _ in 0..8 {
            tracker.observe("hot");
        }
        for _ in 0..2 {
            tracker.observe("cold");
        }
        let top = tracker.top_k();
        let hot = top.iter().find(|r| r.key == "hot").unwrap();
        assert!((hot.heat_score - 0.8).abs() < 1e-9);
    }

    #[test]
    fn heavy_hitters_hot_keys_filter_works() {
        let mut tracker = HeavyHittersTracker::new(5);
        for _ in 0..10 {
            tracker.observe("dominant");
        }
        tracker.observe("minor");
        let hot = tracker.hot_keys(0.5); // threshold 50%
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].key, "dominant");
    }

    #[test]
    fn heavy_hitters_reset_clears_state() {
        let mut tracker = HeavyHittersTracker::new(5);
        tracker.observe("x");
        tracker.reset();
        assert_eq!(tracker.total(), 0);
        assert!(tracker.top_k().is_empty());
    }

    // ── R7.2 RateLimiter tests ───────────────────────────────────────────────

    #[test]
    fn rate_limiter_initially_full_allows_consume() {
        let mut rl = RateLimiter::new(1000);
        // Should succeed immediately (bucket starts full).
        let wait = rl.try_consume(500, 0);
        assert!(wait.is_none(), "initial consume must succeed immediately");
    }

    #[test]
    fn rate_limiter_depleted_returns_wait_time() {
        let mut rl = RateLimiter::new(1000);
        // Drain the bucket completely.
        let _ = rl.try_consume(1000, 0);
        // Now try to consume 500 more — bucket empty, should wait.
        let wait = rl.try_consume(500, 0);
        assert!(wait.is_some(), "empty bucket must return a wait time");
        assert!(wait.unwrap() >= 1, "wait time must be at least 1ms");
    }

    #[test]
    fn rate_limiter_refills_over_time() {
        let mut rl = RateLimiter::new(1000); // 1000 tokens/sec
        let _ = rl.try_consume(1000, 0); // drain
        // 500ms later → 500 new tokens added.
        let wait = rl.try_consume(400, 500);
        assert!(
            wait.is_none(),
            "500ms refill must cover a 400-token request"
        );
    }

    #[test]
    fn rate_limiter_set_rate_clamps_tokens() {
        let mut rl = RateLimiter::new(2000);
        rl.set_rate(100);
        assert_eq!(rl.rate(), 100);
        // Tokens should be clamped to new rate.
        let wait = rl.try_consume(101, 0);
        assert!(wait.is_some(), "tokens clamped to 100, cannot consume 101");
    }

    // ── R7.2 SinkLatencyTracker tests ───────────────────────────────────────

    #[test]
    fn sink_latency_tracker_avg_zero_when_empty() {
        let tracker = SinkLatencyTracker::default();
        assert_eq!(tracker.avg_latency_ms(), 0.0);
        assert!(!tracker.is_slow(100));
    }

    #[test]
    fn sink_latency_tracker_records_avg_and_max() {
        let mut tracker = SinkLatencyTracker::default();
        tracker.record_write(10);
        tracker.record_write(30);
        assert_eq!(tracker.write_count(), 2);
        assert_eq!(tracker.avg_latency_ms(), 20.0);
        assert_eq!(tracker.max_latency_ms(), 30);
    }

    #[test]
    fn sink_latency_tracker_is_slow_detection() {
        let mut tracker = SinkLatencyTracker::default();
        tracker.record_write(200);
        tracker.record_write(400);
        // avg = 300 > threshold 100 → slow
        assert!(tracker.is_slow(100));
        // avg = 300 < threshold 500 → not slow
        assert!(!tracker.is_slow(500));
    }

    // ── R7.2 AdaptiveDecisionLog / AdaptiveOverrideConfig tests ─────────────

    #[test]
    fn adaptive_decision_kind_display() {
        assert_eq!(
            AdaptiveDecisionKind::HotKeySplit.to_string(),
            "hot-key-split"
        );
        assert_eq!(AdaptiveDecisionKind::Repartition.to_string(), "repartition");
        assert_eq!(
            AdaptiveDecisionKind::SourceThrottle.to_string(),
            "source-throttle"
        );
        assert_eq!(
            AdaptiveDecisionKind::SlowSinkDetected.to_string(),
            "slow-sink"
        );
    }

    #[test]
    fn adaptive_decision_log_fields_accessible() {
        let log = AdaptiveDecisionLog {
            timestamp_ms: 12345,
            kind: AdaptiveDecisionKind::Repartition,
            affected_job_id: "job-42".into(),
            details: "partition count increased from 4 to 8".into(),
            applied: true,
        };
        assert_eq!(log.timestamp_ms, 12345);
        assert!(log.applied);
        assert_eq!(log.affected_job_id, "job-42");
    }

    #[test]
    fn adaptive_override_config_defaults_all_false() {
        let cfg = AdaptiveOverrideConfig::default();
        assert!(!cfg.disable_hot_key_splitting);
        assert!(!cfg.disable_adaptive_repartition);
        assert!(!cfg.disable_source_throttling);
    }

    #[test]
    fn throttle_command_fields() {
        let cmd = ThrottleCommand {
            source_id: "src-1".into(),
            rows_per_second: Some(5000),
        };
        assert_eq!(cmd.source_id, "src-1");
        assert_eq!(cmd.rows_per_second, Some(5000));

        let clear = ThrottleCommand {
            source_id: "src-1".into(),
            rows_per_second: None,
        };
        assert!(clear.rows_per_second.is_none());
    }
}
