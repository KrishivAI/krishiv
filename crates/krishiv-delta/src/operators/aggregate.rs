#![forbid(unsafe_code)]

//! Stateful incremental aggregate operators.
//!
//! Supports SUM, COUNT, AVG with correct retraction handling.
//! For each delta row (row, weight):
//!   1. Compute old aggregate for the row's group → emit retraction (-1)
//!   2. Apply delta to running state
//!   3. Compute new aggregate for the row's group → emit insertion (+1)
//!
//! Each aggregation expression has its own state so a `[Count, Sum]` spec
//! does not double-count or cross-contaminate (Sum's `sum` and Count's
//! `count` are distinct fields).

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};

use crate::delta_batch::{DeltaBatch, WEIGHT_COLUMN};
use crate::error::{DeltaError, DeltaResult};

// ── Aggregation specification ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Aggregation {
    Sum {
        input_col: String,
        output_col: String,
    },
    Count {
        output_col: String,
        /// When `Some`, only non-null values of this column are counted
        /// (SQL `COUNT(col)` excludes nulls).  When `None`, counts all rows
        /// (SQL `COUNT(*)`).
        input_col: Option<String>,
    },
    Avg {
        input_col: String,
        output_col: String,
    },
    Min {
        input_col: String,
        output_col: String,
    },
    Max {
        input_col: String,
        output_col: String,
    },
}

impl Aggregation {
    pub fn output_col(&self) -> &str {
        match self {
            Self::Sum { output_col, .. }
            | Self::Count { output_col, .. }
            | Self::Avg { output_col, .. }
            | Self::Min { output_col, .. }
            | Self::Max { output_col, .. } => output_col,
        }
    }

    fn input_col(&self) -> Option<&str> {
        match self {
            Self::Sum { input_col, .. }
            | Self::Avg { input_col, .. }
            | Self::Min { input_col, .. }
            | Self::Max { input_col, .. } => Some(input_col),
            Self::Count { input_col, .. } => input_col.as_deref(),
        }
    }
}

// ── Numeric kind (AUD-3) ────────────────────────────────────────────────────────

/// The accumulation strategy for a numeric aggregate input, decided **once**
/// from the column's Arrow type — not by per-row string sniffing.
///
/// AUD-3: the old code parsed each value's *string form* and picked i64-vs-f64
/// by whether `parse::<i64>()` happened to succeed. Rust renders `10.0_f64` as
/// `"10"`, so a float column would latch the integer AVG path on its whole
/// values and silently corrupt AVG over mixed values like `[10.0, 10.5]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumKind {
    Int,
    Float,
}

/// Map an Arrow type to its numeric accumulation kind, or `None` for a type
/// this operator cannot aggregate numerically (String/Bool/temporal/…). A
/// `None` here makes `IncrementalAggOp::new` error, so the view falls back to
/// DiffBased full recompute rather than producing a silently-wrong `0.0`.
fn num_kind(dt: &DataType) -> Option<NumKind> {
    use DataType::*;
    match dt {
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Some(NumKind::Int),
        Float16 | Float32 | Float64 => Some(NumKind::Float),
        _ => None,
    }
}

/// A typed aggregate output value, so integer aggregates stay exact (no f64
/// round-trip that loses precision above 2^53) and emit the correct Arrow type.
#[derive(Debug, Clone, Copy)]
enum AggScalar {
    I64(i64),
    F64(f64),
}

// ── Per-aggregation state ──────────────────────────────────────────────────────

/// Ordered f64 wrapper for MIN/MAX BTreeMap keys.
///
/// `f64` does not implement `Ord` (NaN). `total_cmp` is used so NaN sorts
/// consistently (after all finite values), keeping the BTreeMap invariants valid.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Default for OrdF64 {
    fn default() -> Self {
        Self(0.0)
    }
}

/// Separate running state for ONE aggregation expression.
/// A group's full state is `Vec<AggState>` indexed by position in `aggregations`.
///
/// `sum` is used by SUM. `avg_sum_i64` + `avg_count_i64` are used by AVG:
/// for integer-typed input columns they accumulate exactly in i64, emitting
/// the quotient as f64 only at output time. For float-typed inputs the caller
/// sets `avg_is_integer = false` and falls back to f64 accumulation in `sum`.
#[derive(Debug, Default, Clone)]
struct AggState {
    /// Weighted sum for SUM / AVG over **float** inputs (f64 accumulation).
    sum: f64,
    /// Weighted sum for SUM over **integer** inputs (exact i64 accumulation,
    /// AUD-3 — avoids the >2^53 precision loss of the old f64-only path).
    sum_i64: i64,
    /// Row count for COUNT / empty-group detection. Also used as the non-null
    /// input count for AVG when inputs are float (avg_is_integer == false).
    count: i64,
    /// Integer-precision weighted sum for AVG over integer-typed inputs.
    avg_sum_i64: i64,
    /// Non-null input count for AVG (separately tracked from `count` so
    /// COUNT and AVG can coexist in a multi-aggregation spec).
    avg_count_i64: i64,
    /// True when the AVG input is an integer column — use i64 accumulation.
    avg_is_integer: bool,
    /// For MIN/MAX: multiset of (value → cumulative weight).
    /// Uses OrdF64 keys so float columns (e.g. Float64) are ordered correctly.
    min_max_set: BTreeMap<OrdF64, i64>,
}

/// One row's typed aggregate input value (AUD-7 / audit §5c): read directly
/// from the Arrow array — no per-row stringify + re-parse. `Null` is a SQL
/// NULL (or a value the safe cast could not represent, e.g. a `UInt64` above
/// `i64::MAX`, which arrow's safe cast nulls out); `None` means the
/// aggregation has no input column (`COUNT(*)`).
#[derive(Debug, Clone, Copy)]
enum AggInput {
    None,
    Null,
    I64(i64),
    F64(f64),
}

impl AggState {
    /// Apply one row's delta. `kind` is `Some` for numeric aggregates
    /// (SUM/AVG/MIN/MAX) and `None` for COUNT, decided from the column's Arrow
    /// type in `IncrementalAggOp::new`. The value arrives typed (AUD-7): the
    /// old string round-trip — and its `.unwrap_or(0.0)` silent-zero bug on
    /// unparseable values — no longer exists.
    fn apply_delta_for_agg(
        &mut self,
        agg: &Aggregation,
        kind: Option<NumKind>,
        value: AggInput,
        weight: i64,
    ) {
        match agg {
            Aggregation::Sum { .. } => {
                match value {
                    // SQL: null inputs are excluded from SUM.
                    AggInput::Null | AggInput::None => return,
                    AggInput::I64(v) => {
                        self.sum_i64 = self.sum_i64.saturating_add(v.saturating_mul(weight));
                    }
                    AggInput::F64(v) => self.sum += v * weight as f64,
                }
                self.count += weight;
            }
            Aggregation::Count { input_col, .. } => {
                // IVM-6: COUNT(col) excludes nulls; COUNT(*) counts all rows.
                if input_col.is_some() && matches!(value, AggInput::Null) {
                    return;
                }
                self.count += weight;
            }
            Aggregation::Avg { .. } => {
                // AUD-3: strategy is fixed by the column's declared type.
                // Integer inputs accumulate exactly in i64; float in f64.
                match value {
                    // SQL: null inputs are excluded from AVG.
                    AggInput::Null | AggInput::None => return,
                    AggInput::I64(v) => {
                        self.avg_is_integer = true;
                        self.avg_sum_i64 =
                            self.avg_sum_i64.saturating_add(v.saturating_mul(weight));
                    }
                    AggInput::F64(v) => {
                        self.avg_is_integer = false;
                        self.sum += v * weight as f64;
                    }
                }
                self.avg_count_i64 += weight;
                self.count += weight;
            }
            Aggregation::Min { .. } | Aggregation::Max { .. } => {
                let v = match value {
                    // SQL: null inputs do not affect MIN/MAX.
                    AggInput::Null | AggInput::None => return,
                    // `kind == Int` values are exact up to 2^53 as f64 keys;
                    // that is fine for ordering.
                    AggInput::I64(v) => v as f64,
                    AggInput::F64(v) => v,
                };
                let _ = kind; // ordering strategy is value-driven now
                let key = OrdF64(v);
                let entry = self.min_max_set.entry(key).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    self.min_max_set.remove(&key);
                }
                self.count += weight;
            }
        }
    }

    fn current_value(&self, agg: &Aggregation, kind: Option<NumKind>) -> Option<AggScalar> {
        match agg {
            Aggregation::Sum { .. } => match kind {
                Some(NumKind::Int) => Some(AggScalar::I64(self.sum_i64)),
                _ => Some(AggScalar::F64(self.sum)),
            },
            Aggregation::Count { .. } => Some(AggScalar::I64(self.count)),
            Aggregation::Avg { .. } => {
                // AVG always yields a floating-point result (SQL semantics).
                if self.avg_count_i64 == 0 {
                    None
                } else if self.avg_is_integer {
                    Some(AggScalar::F64(
                        self.avg_sum_i64 as f64 / self.avg_count_i64 as f64,
                    ))
                } else {
                    Some(AggScalar::F64(self.sum / self.avg_count_i64 as f64))
                }
            }
            Aggregation::Min { .. } => self.min_max_set.keys().next().map(|k| scalar_of(k.0, kind)),
            Aggregation::Max { .. } => self
                .min_max_set
                .keys()
                .next_back()
                .map(|k| scalar_of(k.0, kind)),
        }
    }
}

/// Wrap an f64 min/max key in the correct typed scalar for its column kind.
fn scalar_of(v: f64, kind: Option<NumKind>) -> AggScalar {
    match kind {
        Some(NumKind::Int) => AggScalar::I64(v as i64),
        _ => AggScalar::F64(v),
    }
}

/// `group_key → per-aggregation running state`.
///
/// AUD-7: keys are arrow **row-format** bytes — a single opaque, order-preserving
/// encoding of the group-by columns produced by the op's shared [`RowConverter`],
/// replacing the old `Vec<Option<String>>` that allocated a `String` for every
/// group column of every delta row. `Box<[u8]>` keeps the key heap-compact.
type GroupStateMap = AHashMap<Box<[u8]>, Vec<AggState>>;

/// Before-snapshot map used within a single `apply` tick: `group_key → state as
/// it was before the tick's deltas` (`None` = the group did not exist yet).
type TouchedMap = AHashMap<Box<[u8]>, Option<Vec<AggState>>>;

/// AUD-7: per-aggregation typed column reader. Casts an aggregation's input
/// column to its accumulation array **once per delta batch** (Int64 / Float64),
/// so [`IncrementalAggOp::apply`] reads a typed value per row with no per-row
/// stringify + re-parse (the root of the old `.unwrap_or(0.0)` silent-zero bug).
enum ValueReader {
    /// `COUNT(*)` — no input column; every row contributes.
    NoInput,
    /// Aggregation references a column absent from the delta schema → every row
    /// reads as SQL NULL (excluded from SUM/AVG/MIN/MAX; not counted).
    Missing,
    /// `COUNT(col)`: only nullness matters (`col` may be non-numeric).
    NullMask(ArrayRef),
    /// Numeric input accumulated as i64 (integer-typed column).
    Int(Int64Array),
    /// Numeric input accumulated as f64 (float-typed column).
    Float(Float64Array),
}

impl ValueReader {
    fn build(data: &RecordBatch, agg: &Aggregation, kind: Option<NumKind>) -> DeltaResult<Self> {
        let Some(name) = agg.input_col() else {
            return Ok(ValueReader::NoInput); // COUNT(*)
        };
        let idx = match data.schema().index_of(name) {
            Ok(i) => i,
            Err(_) => return Ok(ValueReader::Missing),
        };
        let col = data.column(idx);
        match kind {
            // COUNT(col): keep the original array, we only probe its null mask.
            None => Ok(ValueReader::NullMask(col.clone())),
            // Cast once per batch. arrow's default (safe) cast nulls out values
            // that don't fit (e.g. UInt64 > i64::MAX), which then read as NULL —
            // strictly better than the old parse path that coerced them to 0.
            Some(NumKind::Int) => {
                let arr = compute::cast(col, &DataType::Int64)?;
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DeltaError::Operator("int64 cast produced wrong type".into()))?
                    .clone();
                Ok(ValueReader::Int(arr))
            }
            Some(NumKind::Float) => {
                let arr = compute::cast(col, &DataType::Float64)?;
                let arr = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DeltaError::Operator("float64 cast produced wrong type".into()))?
                    .clone();
                Ok(ValueReader::Float(arr))
            }
        }
    }

    fn value(&self, row: usize) -> AggInput {
        match self {
            ValueReader::NoInput => AggInput::None,
            ValueReader::Missing => AggInput::Null,
            ValueReader::NullMask(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    // COUNT(col) ignores the magnitude; any non-null marker works.
                    AggInput::I64(0)
                }
            }
            ValueReader::Int(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    AggInput::I64(a.value(row))
                }
            }
            ValueReader::Float(a) => {
                if a.is_null(row) {
                    AggInput::Null
                } else {
                    AggInput::F64(a.value(row))
                }
            }
        }
    }
}

// ── IncrementalAggOp ──────────────────────────────────────────────────────────

/// Stateful incremental aggregate operator.
pub struct IncrementalAggOp {
    group_by: Vec<String>,
    aggregations: Vec<Aggregation>,
    /// AUD-3: per-aggregation numeric kind, decided once from the input schema.
    /// `None` for COUNT (no numeric input to accumulate).
    input_kinds: Vec<Option<NumKind>>,
    output_schema: SchemaRef,
    /// AUD-7: shared row-format encoder for group-by keys, built once from the
    /// group columns' declared types and reused across every tick. Reuse is what
    /// keeps a value's encoding stable when a group column is dictionary-encoded
    /// — a per-tick converter would re-intern and could drift, splitting one
    /// logical group across two keys.
    group_converter: RowConverter,
    /// Declared arrow types of the group-by columns, in order. Used to rebuild
    /// the converter after a restore and to name the reconstructed group columns.
    group_field_types: Vec<DataType>,
    /// state[group_key] → per-aggregation running state (one entry per aggregation)
    state: GroupStateMap,
}

impl IncrementalAggOp {
    pub fn new(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
    ) -> DeltaResult<Self> {
        // Validate group-by columns exist
        for col in &group_by {
            input_schema
                .field_with_name(col)
                .map_err(|_| DeltaError::ColumnNotFound(col.clone()))?;
        }

        // Validate input columns for each agg and decide its numeric kind once
        // from the schema (AUD-3). SUM/AVG/MIN/MAX over a non-numeric column
        // return an error so the caller falls back to DiffBased full recompute
        // rather than the old silent `0.0`.
        let mut input_kinds: Vec<Option<NumKind>> = Vec::with_capacity(aggregations.len());
        for agg in &aggregations {
            let kind = match agg {
                // COUNT only needs a null check; no numeric accumulation.
                Aggregation::Count { input_col, .. } => {
                    if let Some(col) = input_col {
                        input_schema
                            .field_with_name(col)
                            .map_err(|_| DeltaError::ColumnNotFound(col.clone()))?;
                    }
                    None
                }
                Aggregation::Sum { input_col, .. }
                | Aggregation::Avg { input_col, .. }
                | Aggregation::Min { input_col, .. }
                | Aggregation::Max { input_col, .. } => {
                    let field = input_schema
                        .field_with_name(input_col)
                        .map_err(|_| DeltaError::ColumnNotFound(input_col.clone()))?;
                    let k = num_kind(field.data_type()).ok_or_else(|| {
                        DeltaError::Operator(format!(
                            "aggregate '{}' over non-numeric column '{}' ({:?}) is not \
                             supported by the incremental operator; the view falls back to \
                             DiffBased full recompute",
                            agg.output_col(),
                            input_col,
                            field.data_type()
                        ))
                    })?;
                    Some(k)
                }
            };
            input_kinds.push(kind);
        }

        // Build output schema: group-by columns + aggregate output columns.
        let mut out_fields: Vec<_> = group_by
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| Arc::new(f.clone()))
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        for (agg, kind) in aggregations.iter().zip(input_kinds.iter()) {
            // AUD-3: match SQL output types. COUNT → Int64; AVG → Float64 always;
            // SUM/MIN/MAX preserve integer vs float (SUM(Int)→Int64, etc.).
            let output_type = match agg {
                Aggregation::Count { .. } => DataType::Int64,
                Aggregation::Avg { .. } => DataType::Float64,
                Aggregation::Sum { .. } | Aggregation::Min { .. } | Aggregation::Max { .. } => {
                    match kind {
                        Some(NumKind::Int) => DataType::Int64,
                        _ => DataType::Float64,
                    }
                }
            };
            out_fields.push(Arc::new(Field::new(agg.output_col(), output_type, true)));
        }

        let output_schema = Arc::new(Schema::new(out_fields));

        // AUD-7: build the shared row-format encoder for the group-by columns
        // from their declared source types (validated as present above).
        let group_field_types: Vec<DataType> = group_by
            .iter()
            .map(|name| {
                input_schema
                    .field_with_name(name)
                    .map(|f| f.data_type().clone())
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        let group_converter = RowConverter::new(
            group_field_types
                .iter()
                .map(|dt| SortField::new(dt.clone()))
                .collect(),
        )
        .map_err(DeltaError::Arrow)?;

        Ok(Self {
            group_by,
            aggregations,
            input_kinds,
            output_schema,
            group_converter,
            group_field_types,
            state: GroupStateMap::default(),
        })
    }

    /// Like [`new`](Self::new) but adopts the view's **declared** output column
    /// types for the aggregate columns (by name), preserving the operator's
    /// canonical column order (group-by columns first, then aggregates).
    ///
    /// AUD-3: `SUM(Int64)` is SQL-typed `Int64`, but a view may legitimately
    /// declare its output column as `Float64` (or vice-versa). The operator
    /// honors that declaration so the materialized snapshot matches the
    /// registered contract that downstream plans and the DiffBased baseline
    /// diff against — instead of the old behavior of always emitting `Float64`.
    /// Declared aggregate columns must be `Int64`/`Float64`; anything else
    /// errors so the planner falls back to DiffBased.
    pub fn new_with_output_schema(
        input_schema: &SchemaRef,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
        declared: &SchemaRef,
    ) -> DeltaResult<Self> {
        let mut op = Self::new(input_schema, group_by, aggregations)?;
        let n_group = op.group_by.len();
        let mut fields: Vec<Arc<Field>> = op.output_schema.fields().iter().cloned().collect();
        for (i, agg) in op.aggregations.iter().enumerate() {
            if let Ok(df) = declared.field_with_name(agg.output_col()) {
                match df.data_type() {
                    DataType::Int64 | DataType::Float64 => {
                        if let Some(slot) = fields.get_mut(n_group + i) {
                            *slot = Arc::new(Field::new(
                                agg.output_col(),
                                df.data_type().clone(),
                                true,
                            ));
                        }
                    }
                    other => {
                        return Err(DeltaError::Operator(format!(
                            "declared output column '{}' has type {other:?}; the incremental \
                             aggregate emits only Int64/Float64 — view falls back to DiffBased",
                            agg.output_col()
                        )));
                    }
                }
            }
        }
        op.output_schema = Arc::new(Schema::new(fields));
        Ok(op)
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Evict aggregate groups whose event time is below `watermark`.
    ///
    /// Note: the current data model does not carry a per-group event time on
    /// `IncrementalAggOp::state` (groups are keyed by value, not by a typed
    /// timestamp). Until that schema is added, the operator is a no-op here.
    /// The interface exists so the `ViewPlan::Aggregate` arm of
    /// `gc_watermark` is reached; the eviction is wired to no-op pending
    /// schema work. A long-running incremental aggregate over an unbounded
    /// source should add a `TUMBLE/HOP/SESSION` window or filter on
    /// `event_time_col` in the view body so the SQL engine can prune older
    /// partitions.
    pub fn gc_watermark(&mut self, _watermark: i64) -> crate::DeltaResult<usize> {
        Ok(0)
    }

    /// Apply one tick of incremental aggregation.
    ///
    /// For each row in `delta`:
    /// 1. Look up the group's current state (per-aggregation).
    /// 2. Emit retraction of old aggregate output (if group was non-empty).
    /// 3. Apply delta weight to each aggregation's state independently.
    /// 4. Emit insertion of new aggregate output (if group is now non-empty).
    pub fn apply(&mut self, delta: DeltaBatch) -> DeltaResult<DeltaBatch> {
        if delta.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        let data = delta.data_batch();
        let weights = delta.weights();

        let group_col_indices: Vec<usize> = self
            .group_by
            .iter()
            .map(|name| {
                data.schema()
                    .index_of(name)
                    .map_err(|_| DeltaError::ColumnNotFound(name.clone()))
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // AUD-7: encode every group-by column to a single row-format key in one
        // pass (no per-cell String alloc). A global aggregate (no GROUP BY) has
        // one implicit group keyed by the empty byte string.
        let group_rows = if group_col_indices.is_empty() {
            None
        } else {
            let group_arrays: Vec<ArrayRef> = group_col_indices
                .iter()
                .map(|&idx| data.column(idx).clone())
                .collect();
            Some(
                self.group_converter
                    .convert_columns(&group_arrays)
                    .map_err(DeltaError::Arrow)?,
            )
        };

        // AUD-7: cast each aggregation's input column to its typed accumulation
        // array once for the whole batch, replacing the per-row stringify+parse.
        let value_readers: Vec<ValueReader> = self
            .aggregations
            .iter()
            .zip(self.input_kinds.iter())
            .map(|(agg, kind)| ValueReader::build(&data, agg, *kind))
            .collect::<DeltaResult<Vec<_>>>()?;

        // Track which groups were touched and their before-tick state.
        let mut touched: TouchedMap = AHashMap::new();

        for row in 0..data.num_rows() {
            let key: Box<[u8]> = match &group_rows {
                Some(rows) => rows.row(row).as_ref().into(),
                None => Box::<[u8]>::default(),
            };

            // Record state before this row's delta (once per group per tick).
            if !touched.contains_key(&key) {
                let before = self.state.get(&key).cloned();
                touched.insert(key.clone(), before);
            }

            let w = weights.value(row);

            // Apply delta to each aggregation's state independently. Each
            // aggregation has its own AggState, so [Count, Sum] does not
            // double-count and Sum + Min do not cross-contaminate.
            let group_state = self
                .state
                .entry(key.clone())
                .or_insert_with(|| vec![AggState::default(); self.aggregations.len()]);

            // Ensure the state vector matches the aggregation count (handles a
            // new aggregation added after state was created).
            if group_state.len() < self.aggregations.len() {
                group_state.resize(self.aggregations.len(), AggState::default());
            }

            for (((state, agg), kind), reader) in group_state
                .iter_mut()
                .zip(self.aggregations.iter())
                .zip(self.input_kinds.iter())
                .zip(value_readers.iter())
            {
                state.apply_delta_for_agg(agg, *kind, reader.value(row), w);
            }

            // GC empty groups: a group is empty when ALL its per-agg states are.
            if let Some(states) = self.state.get(&key)
                && states.iter().all(|s| s.count == 0)
            {
                self.state.remove(&key);
            }
        }

        // Build output: retract old agg + insert new agg for each touched group.
        let mut out_keys: Vec<Box<[u8]>> = Vec::new();
        let mut out_weights: Vec<i64> = Vec::new();
        let mut agg_values: Vec<Vec<Option<AggScalar>>> = Vec::new();

        for (key, before_states) in &touched {
            let has_before = before_states
                .as_ref()
                .map(|s| s.iter().any(|a| a.count != 0))
                .unwrap_or(false);
            let has_after = self
                .state
                .get(key)
                .map(|s| s.iter().any(|a| a.count != 0))
                .unwrap_or(false);

            if has_before && let Some(states) = before_states.as_ref() {
                let vals = compute_agg_values(states, &self.aggregations, &self.input_kinds);
                out_keys.push(key.clone());
                out_weights.push(-1);
                agg_values.push(vals);
            }
            if has_after && let Some(after_states) = self.state.get(key) {
                let vals = compute_agg_values(after_states, &self.aggregations, &self.input_kinds);
                out_keys.push(key.clone());
                out_weights.push(1);
                agg_values.push(vals);
            }
        }

        if out_keys.is_empty() {
            return DeltaBatch::empty(self.output_schema.clone());
        }

        self.build_output_batch(&out_keys, &out_weights, &agg_values)
    }

    /// Serialize the per-group accumulator state to a self-contained blob.
    ///
    /// This is the piece of an incremental view that a full flow checkpoint
    /// cannot reconstruct from the materialized source or view snapshots: the
    /// source snapshot is a *set* (multiplicity is dropped by `filter_positive`)
    /// and the view snapshot loses the multiset MIN/MAX and the SUM/COUNT split
    /// AVG needs. Persisting the accumulator directly is the only lossless way
    /// to restore an incremental aggregate across a coordinator restart (G6/F4).
    ///
    /// Format **v2** (AUD-7): group keys are now opaque arrow row-format bytes,
    /// which are not stable across arrow encoding changes, so the group *values*
    /// are serialized as a portable Arrow IPC batch of the group columns instead
    /// of raw key bytes. Layout (little-endian):
    ///   `MAGIC "AGGS2" || u8 has_group_cols || u32 n_groups ||
    ///    [ u32 ipc_len || ipc(group columns) ]  (only if has_group_cols && n>0) ||
    ///    (u32 n_states || (state)*){n_groups}`
    /// States are written in the same order as the IPC batch rows. A blob that
    /// does not begin with `MAGIC` fails [`restore_state_bytes`], so an
    /// incompatible/older blob falls back (loudly) to seed-from-snapshots.
    pub fn state_bytes(&self) -> Vec<u8> {
        let entries: Vec<(&[u8], &Vec<AggState>)> =
            self.state.iter().map(|(k, v)| (&k[..], v)).collect();
        let has_group_cols = !self.group_field_types.is_empty();

        // Reconstruct group key columns (portable IPC) when there are group
        // columns AND at least one live group. If reconstruction fails, emit an
        // empty blob so restore falls back to seed-from-snapshots rather than
        // installing wrong state.
        let group_ipc: Option<Vec<u8>> = if has_group_cols && !entries.is_empty() {
            match self
                .group_columns_batch(entries.iter().map(|(k, _)| *k))
                .and_then(|b| encode_batch_ipc(&b))
            {
                Ok(ipc) => Some(ipc),
                Err(_) => {
                    let mut out = Vec::new();
                    out.extend_from_slice(AGG_STATE_MAGIC_V2);
                    out.push(1u8);
                    out.extend_from_slice(&0u32.to_le_bytes());
                    return out;
                }
            }
        } else {
            None
        };

        let mut out = Vec::new();
        out.extend_from_slice(AGG_STATE_MAGIC_V2);
        out.push(has_group_cols as u8);
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        if let Some(ipc) = &group_ipc {
            out.extend_from_slice(&(ipc.len() as u32).to_le_bytes());
            out.extend_from_slice(ipc);
        }
        for (_key, states) in &entries {
            out.extend_from_slice(&(states.len() as u32).to_le_bytes());
            for st in *states {
                st.write_bytes(&mut out);
            }
        }
        out
    }

    /// Replace the accumulator state with one previously produced by
    /// [`state_bytes`](Self::state_bytes). The group-by / aggregation shape is
    /// taken from `self` (rebuilt from the view SQL), so only the running
    /// values are transferred. An unrecognized (non-v2) blob errors so the
    /// caller can fall back to seed-from-snapshots.
    pub fn restore_state_bytes(&mut self, bytes: &[u8]) -> DeltaResult<()> {
        if !bytes.starts_with(AGG_STATE_MAGIC_V2) {
            return Err(DeltaError::Operator(
                "aggregate state blob is not format v2 (AUD-7); restore falls back to \
                 seed-from-snapshots"
                    .into(),
            ));
        }
        let mut pos = AGG_STATE_MAGIC_V2.len();
        let has_group_cols = read_u8(bytes, &mut pos)? == 1;
        let n_groups = read_u32(bytes, &mut pos)? as usize;

        // Rebuild the per-group row keys.
        let keys: Vec<Box<[u8]>> = if !has_group_cols {
            // Global aggregate: 0 or 1 group with the empty key.
            (0..n_groups).map(|_| Box::<[u8]>::default()).collect()
        } else if n_groups == 0 {
            Vec::new()
        } else {
            let ipc_len = read_u32(bytes, &mut pos)? as usize;
            let ipc = bytes
                .get(pos..pos + ipc_len)
                .ok_or_else(|| DeltaError::Operator("agg state truncated (group ipc)".into()))?;
            pos += ipc_len;
            let batch = decode_batch_ipc(ipc)?;
            let rows = self
                .group_converter
                .convert_columns(batch.columns())
                .map_err(DeltaError::Arrow)?;
            (0..batch.num_rows())
                .map(|i| rows.row(i).as_ref().into())
                .collect()
        };

        if keys.len() != n_groups {
            return Err(DeltaError::Operator(
                "agg state group-count mismatch on restore".into(),
            ));
        }

        let mut state: GroupStateMap = AHashMap::with_capacity(n_groups);
        for key in keys {
            let n_states = read_u32(bytes, &mut pos)? as usize;
            let mut states: Vec<AggState> = Vec::with_capacity(n_states);
            for _ in 0..n_states {
                states.push(AggState::read_bytes(bytes, &mut pos)?);
            }
            state.insert(key, states);
        }
        self.state = state;
        Ok(())
    }

    /// Rebuild the group-by columns as a `RecordBatch` from a sequence of
    /// row-format keys, using the shared converter (AUD-7). Shared by
    /// `state_bytes` (for portable serialization) and `build_output_batch`
    /// (for emitting the group columns natively, no string cast).
    fn group_columns_batch<'a>(
        &self,
        keys: impl Iterator<Item = &'a [u8]>,
    ) -> DeltaResult<RecordBatch> {
        let parser = self.group_converter.parser();
        let rows: Vec<_> = keys.map(|k| parser.parse(k)).collect();
        let arrays = self
            .group_converter
            .convert_rows(rows)
            .map_err(DeltaError::Arrow)?;
        let fields: Vec<Field> = self
            .group_by
            .iter()
            .zip(self.group_field_types.iter())
            .map(|(name, dt)| Field::new(name, dt.clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(DeltaError::from)
    }

    /// AUD-7: build the retract/insert output batch, rebuilding the group-by
    /// columns natively from row-format keys (no `String`→cast round trip) and
    /// emitting aggregate columns in the declared output types.
    fn build_output_batch(
        &self,
        group_keys: &[Box<[u8]>],
        weights: &[i64],
        agg_values: &[Vec<Option<AggScalar>>],
    ) -> DeltaResult<DeltaBatch> {
        let n_group = self.group_by.len();

        let mut cols: Vec<ArrayRef> = if n_group == 0 {
            Vec::new()
        } else {
            let batch = self.group_columns_batch(group_keys.iter().map(|k| &k[..]))?;
            // Cast a group column only if the declared output type differs from
            // the source column type (rare; `new_with_output_schema` never
            // re-types group columns, but a view may declare a widened type).
            batch
                .columns()
                .iter()
                .enumerate()
                .map(|(gi, arr)| {
                    let target = self.output_schema.field(gi).data_type();
                    if arr.data_type() == target {
                        Ok(arr.clone())
                    } else {
                        compute::cast(arr, target).map_err(DeltaError::from)
                    }
                })
                .collect::<DeltaResult<Vec<_>>>()?
        };

        // Aggregate columns, typed to the declared output schema (AUD-3):
        // integer SUM/MIN/MAX/COUNT emit Int64 exactly; AVG and float aggregates
        // emit Float64.
        for ai in 0..self.aggregations.len() {
            let target = self.output_schema.field(n_group + ai).data_type();
            let col: ArrayRef = match target {
                DataType::Int64 => {
                    let vals: Int64Array = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|s| match s {
                                AggScalar::I64(v) => v,
                                AggScalar::F64(v) => v as i64,
                            })
                        })
                        .collect();
                    Arc::new(vals)
                }
                _ => {
                    let vals: Float64Array = agg_values
                        .iter()
                        .map(|row| {
                            row.get(ai).copied().flatten().map(|s| match s {
                                AggScalar::I64(v) => v as f64,
                                AggScalar::F64(v) => v,
                            })
                        })
                        .collect();
                    Arc::new(vals)
                }
            };
            cols.push(col);
        }

        // Weight column.
        cols.push(Arc::new(Int64Array::from(weights.to_vec())));

        let mut full_fields: Vec<_> = self.output_schema.fields().iter().cloned().collect();
        full_fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        let full_schema = Arc::new(Schema::new(full_fields));

        let inner = RecordBatch::try_new(full_schema, cols)?;
        DeltaBatch::from_weighted(inner)
    }
}

/// Magic prefix for the version-2 aggregate-state blob (AUD-7).
const AGG_STATE_MAGIC_V2: &[u8; 5] = b"AGGS2";

/// Serialize a `RecordBatch` to a bare Arrow IPC stream (no magic — this is an
/// internal, length-framed payload inside the aggregate-state blob).
fn encode_batch_ipc(batch: &RecordBatch) -> DeltaResult<Vec<u8>> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema())?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

/// Inverse of [`encode_batch_ipc`].
fn decode_batch_ipc(bytes: &[u8]) -> DeltaResult<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    use std::io::Cursor;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    reader
        .next()
        .ok_or_else(|| DeltaError::Operator("empty group-columns IPC stream".into()))?
        .map_err(DeltaError::from)
}

fn read_u8(bytes: &[u8], pos: &mut usize) -> DeltaResult<u8> {
    let b = *bytes
        .get(*pos)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 1;
    Ok(b)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> DeltaResult<u32> {
    let raw = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes(raw.try_into().unwrap_or([0; 4])))
}

fn read_i64(bytes: &[u8], pos: &mut usize) -> DeltaResult<i64> {
    let raw = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 8;
    Ok(i64::from_le_bytes(raw.try_into().unwrap_or([0; 8])))
}

fn read_f64(bytes: &[u8], pos: &mut usize) -> DeltaResult<f64> {
    let raw = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| DeltaError::Operator("agg state truncated".into()))?;
    *pos += 8;
    Ok(f64::from_le_bytes(raw.try_into().unwrap_or([0; 8])))
}

impl AggState {
    fn write_bytes(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.sum.to_le_bytes());
        out.extend_from_slice(&self.sum_i64.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.avg_sum_i64.to_le_bytes());
        out.extend_from_slice(&self.avg_count_i64.to_le_bytes());
        out.push(self.avg_is_integer as u8);
        out.extend_from_slice(&(self.min_max_set.len() as u32).to_le_bytes());
        for (k, w) in &self.min_max_set {
            out.extend_from_slice(&k.0.to_le_bytes());
            out.extend_from_slice(&w.to_le_bytes());
        }
    }

    fn read_bytes(bytes: &[u8], pos: &mut usize) -> DeltaResult<Self> {
        let sum = read_f64(bytes, pos)?;
        let sum_i64 = read_i64(bytes, pos)?;
        let count = read_i64(bytes, pos)?;
        let avg_sum_i64 = read_i64(bytes, pos)?;
        let avg_count_i64 = read_i64(bytes, pos)?;
        let avg_is_integer = read_u8(bytes, pos)? == 1;
        let n_minmax = read_u32(bytes, pos)? as usize;
        let mut min_max_set: BTreeMap<OrdF64, i64> = BTreeMap::new();
        for _ in 0..n_minmax {
            let k = read_f64(bytes, pos)?;
            let w = read_i64(bytes, pos)?;
            min_max_set.insert(OrdF64(k), w);
        }
        Ok(Self {
            sum,
            sum_i64,
            count,
            avg_sum_i64,
            avg_count_i64,
            avg_is_integer,
            min_max_set,
        })
    }
}

fn compute_agg_values(
    states: &[AggState],
    aggregations: &[Aggregation],
    input_kinds: &[Option<NumKind>],
) -> Vec<Option<AggScalar>> {
    states
        .iter()
        .zip(aggregations.iter())
        .zip(input_kinds.iter())
        .map(|((state, agg), kind)| state.current_value(agg, *kind))
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Read the single positive (weight `+1`) row's `col` value as f64 from an
    /// aggregate output whose touched group ended non-empty. AUD-7 tests assert
    /// on the emitted batch instead of reaching into the (now opaque) row keys.
    fn positive_f64(out: &DeltaBatch, col: &str) -> Option<f64> {
        let pos = out.filter_positive().ok()?;
        if pos.num_rows() == 0 {
            return None;
        }
        let arr = pos.column_by_name(col)?;
        if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
            Some(a.value(0))
        } else {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0) as f64)
        }
    }

    fn order_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn order_batch(cids: &[&str], amounts: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            order_schema(),
            vec![
                Arc::new(StringArray::from(cids.to_vec())) as Arc<dyn Array>,
                Arc::new(Float64Array::from(amounts.to_vec())) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[test]
    fn sum_increases_on_insert() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        let delta = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(delta).unwrap();
        // Should have one insertion of sum=100
        assert!(!out.is_empty());
        let positive = out.filter_positive().unwrap();
        assert_eq!(positive.num_rows(), 1);
    }

    #[test]
    fn sum_retracts_on_delete() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Sum {
                input_col: "amount".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();

        // First insert
        let d1 = DeltaBatch::from_inserts(order_batch(&["c1"], &[100.0])).unwrap();
        op.apply(d1).unwrap();

        // Then delete → should emit retraction of sum=100 and insertion of sum=0 (empty group GC'd)
        let d2 = DeltaBatch::from_deletes(order_batch(&["c1"], &[100.0])).unwrap();
        let out = op.apply(d2).unwrap();
        assert!(!out.is_empty());
        // Retraction should appear
        let retractions = out.filter_negative().unwrap();
        assert_eq!(retractions.num_rows(), 1);
    }

    #[test]
    fn count_increments_correctly() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Count {
                output_col: "cnt".into(),
                input_col: None,
            }],
        )
        .unwrap();

        let d1 = DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 20.0])).unwrap();
        let out = op.apply(d1).unwrap();
        // Count for c1 should be 2 (single group → one positive output row).
        assert_eq!(positive_f64(&out, "cnt"), Some(2.0));
    }

    #[test]
    fn min_float_retract_current_min_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Min = 1.2.
        // Retract 1.2. Min must become 2.7 (not 0.0, which the old i64 parse would give).
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Min {
                input_col: "v".into(),
                output_col: "min_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(insert).unwrap();

        // Current min for "g" should be 1.2 (the positive output row).
        let min_val = positive_f64(&out, "min_v");
        assert!(
            (min_val.unwrap_or(f64::NAN) - 1.2).abs() < 1e-9,
            "min before retraction should be 1.2, got {min_val:?}"
        );

        // Retract 1.2
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![1.2])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(retract).unwrap();

        // Min should now be 2.7, not 0.0 (the new positive output row).
        let min_after = positive_f64(&out, "min_v");
        assert!(
            (min_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "min after retracting 1.2 should be 2.7, got {min_after:?}"
        );
    }

    #[test]
    fn max_float_retract_current_max_substitutes_next() {
        // Insert 3.5, 1.2, 2.7 for key "g". Max = 3.5.
        // Retract 3.5. Max must become 2.7.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Float64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Max {
                input_col: "v".into(),
                output_col: "max_v".into(),
            }],
        )
        .unwrap();

        let insert = DeltaBatch::from_inserts(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g", "g", "g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5, 1.2, 2.7])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        op.apply(insert).unwrap();

        // Retract 3.5
        let retract = DeltaBatch::from_deletes(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["g"])) as Arc<dyn Array>,
                    Arc::new(Float64Array::from(vec![3.5])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let out = op.apply(retract).unwrap();

        let max_after = positive_f64(&out, "max_v");
        assert!(
            (max_after.unwrap_or(f64::NAN) - 2.7).abs() < 1e-9,
            "max after retracting 3.5 should be 2.7, got {max_after:?}"
        );
    }

    /// AUD-3: AVG over a **float** column with mixed integer-looking and
    /// fractional values must not latch the i64 path. `[10.0, 10.5]` averages to
    /// 10.25; the old string-sniffing code sent `10.0` (rendered `"10"`) to the
    /// i64 accumulator and `10.5` to the f64 one, then divided one accumulator by
    /// the combined count — a wrong result.
    #[test]
    fn avg_over_float_column_with_integral_values_is_exact() {
        let mut op = IncrementalAggOp::new(
            &order_schema(),
            vec!["customer_id".into()],
            vec![Aggregation::Avg {
                input_col: "amount".into(),
                output_col: "avg_amt".into(),
            }],
        )
        .unwrap();
        let out = op
            .apply(DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[10.0, 10.5])).unwrap())
            .unwrap();
        let avg = positive_f64(&out, "avg_amt");
        assert!(
            (avg.unwrap_or(f64::NAN) - 10.25).abs() < 1e-9,
            "avg should be 10.25, got {avg:?}"
        );
    }

    /// AUD-3: SUM over an integer column emits an Int64 output column (SQL
    /// semantics: `SUM(Int64) → Int64`), not a lossy Float64.
    #[test]
    fn sum_over_integer_column_emits_int64() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut op = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Sum {
                input_col: "v".into(),
                output_col: "total".into(),
            }],
        )
        .unwrap();
        assert_eq!(
            op.output_schema().field(1).data_type(),
            &DataType::Int64,
            "SUM over Int64 must be typed Int64"
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])) as Arc<dyn Array>,
                Arc::new(Int64Array::from(vec![3_000_000_000_i64, 4_000_000_000_i64]))
                    as Arc<dyn Array>,
            ],
        )
        .unwrap();
        let out = op.apply(DeltaBatch::from_inserts(batch).unwrap()).unwrap();
        let data = out.filter_positive().unwrap();
        let total = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("total column must be Int64")
            .value(0);
        assert_eq!(total, 7_000_000_000_i64);
    }

    /// AUD-3: an aggregate over a non-numeric column errors from `new`, so the
    /// planner falls back to DiffBased instead of producing silent zeros.
    #[test]
    fn sum_over_non_numeric_column_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let err = IncrementalAggOp::new(
            &schema,
            vec!["k".into()],
            vec![Aggregation::Sum {
                input_col: "label".into(),
                output_col: "total".into(),
            }],
        );
        assert!(
            err.is_err(),
            "SUM over Utf8 must error (→ DiffBased fallback)"
        );
    }

    /// `state_bytes` → `restore_state_bytes` transfers the accumulator
    /// losslessly, *including* the multiset multiplicity of genuinely-identical
    /// rows — the exact property the materialized source snapshot loses. A fresh
    /// op restored from the bytes then emits the same retract+insert on the next
    /// delta as the original would (G6/F4 lossless restore).
    #[test]
    fn state_bytes_round_trip_preserves_multiset() {
        let group = vec!["customer_id".to_string()];
        let sum = vec![Aggregation::Sum {
            input_col: "amount".into(),
            output_col: "total".into(),
        }];
        let mut op = IncrementalAggOp::new(&order_schema(), group.clone(), sum.clone()).unwrap();
        // Two *identical* rows (c1, 5.0) — a set-based snapshot would collapse
        // these; the accumulator must remember both (sum = 10, count = 2).
        op.apply(DeltaBatch::from_inserts(order_batch(&["c1", "c1"], &[5.0, 5.0])).unwrap())
            .unwrap();

        // Serialize, then restore into a brand-new empty operator.
        let bytes = op.state_bytes();
        let mut restored = IncrementalAggOp::new(&order_schema(), group, sum).unwrap();
        restored.restore_state_bytes(&bytes).unwrap();

        // Retract ONE of the two identical rows on the restored op. If the
        // multiset was preserved, c1 remains present with sum=5 → the op emits
        // retract(total=10) + insert(total=5). If multiplicity had been lost
        // (count=1), the group would vanish → retract(total=5) + nothing.
        let out = restored
            .apply(DeltaBatch::from_deletes(order_batch(&["c1"], &[5.0])).unwrap())
            .unwrap();
        let data = out.data_batch();
        let weights = out.weights();
        let totals = data
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut retract_10 = false;
        let mut insert_5 = false;
        for i in 0..data.num_rows() {
            let w = weights.value(i);
            let t = totals.value(i);
            if w < 0 && (t - 10.0).abs() < 1e-9 {
                retract_10 = true;
            }
            if w > 0 && (t - 5.0).abs() < 1e-9 {
                insert_5 = true;
            }
        }
        assert!(
            retract_10 && insert_5,
            "restored op must retract total=10 and insert total=5 \
             (multiset multiplicity preserved); got {out:?}"
        );
    }
}
