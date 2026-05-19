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
        let size = self.spec.window_size_ms as i64;
        let window_end_ms = window_start_ms + size;

        let mut fields = vec![
            Field::new(&self.spec.key_column, DataType::Utf8, false),
            Field::new("window_start_ms", DataType::Int64, false),
            Field::new("window_end_ms", DataType::Int64, false),
        ];
        for agg in &self.spec.agg_exprs {
            fields.push(Field::new(&agg.output_column, DataType::Int64, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![key_value])),
            Arc::new(Int64Array::from(vec![window_start_ms])),
            Arc::new(Int64Array::from(vec![window_end_ms])),
        ];
        for (i, _) in self.spec.agg_exprs.iter().enumerate() {
            columns.push(Arc::new(Int64Array::from(vec![state.values[i]])));
        }

        Ok(RecordBatch::try_new(schema, columns)?)
    }
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
}
