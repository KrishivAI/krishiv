use ahash::AHashMap as HashMap;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use smallvec::SmallVec;

use crate::join::AggKey;
use crate::{ExecError, ExecResult};

/// Pre-downcasted Arrow array reference for fast per-row value extraction.
pub(crate) enum PreDowncastCol<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Bool(&'a BooleanArray),
}

impl<'a> PreDowncastCol<'a> {
    fn downcast(col: &'a ArrayRef) -> ExecResult<Self> {
        match col.data_type() {
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    ExecError::UnsupportedType("declared Int32 column failed downcast".into())
                })?;
                Ok(Self::Int32(arr))
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ExecError::UnsupportedType("declared Int64 column failed downcast".into())
                })?;
                Ok(Self::Int64(arr))
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    ExecError::UnsupportedType("declared Float64 column failed downcast".into())
                })?;
                Ok(Self::Float64(arr))
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ExecError::UnsupportedType("declared Utf8 column failed downcast".into())
                })?;
                Ok(Self::Utf8(arr))
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                    ExecError::UnsupportedType("declared Bool column failed downcast".into())
                })?;
                Ok(Self::Bool(arr))
            }
            other => Err(ExecError::UnsupportedType(format!(
                "unsupported column type for pre-downcast: {other}"
            ))),
        }
    }

    fn extract_agg_key(&self, row: usize) -> AggKey {
        match self {
            Self::Int32(arr) => AggKey::Int32(arr.value(row)),
            Self::Int64(arr) => AggKey::Int64(arr.value(row)),
            Self::Float64(arr) => AggKey::Float64(arr.value(row).to_bits()),
            Self::Utf8(arr) => AggKey::Utf8(arr.value(row).to_string()),
            Self::Bool(arr) => AggKey::Bool(arr.value(row)),
        }
    }

    fn int64_value(&self, row: usize) -> ExecResult<i64> {
        match self {
            Self::Int32(arr) => Ok(arr.value(row) as i64),
            Self::Int64(arr) => Ok(arr.value(row)),
            _ => Err(ExecError::Arrow(
                "int64_value called on unsupported column type".into(),
            )),
        }
    }

    fn float64_value(&self, row: usize) -> ExecResult<f64> {
        match self {
            Self::Int32(arr) => Ok(arr.value(row) as f64),
            Self::Int64(arr) => Ok(arr.value(row) as f64),
            Self::Float64(arr) => Ok(arr.value(row)),
            _ => Err(ExecError::Arrow(
                "float64_value called on unsupported column type".into(),
            )),
        }
    }
}

// ── AggState ──────────────────────────────────────────────────────────────────

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
    /// Average of numeric columns (`Int32`, `Int64`, `Float64`).
    Avg,
    /// Sample standard deviation (Bessel-corrected, denominator `n-1`) of
    /// numeric columns. Reuses the avg accumulators (`avg_sum` = Σx,
    /// `avg_count` = n) plus `sq_sum` = Σx².
    Stddev,
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

/// Per-expression accumulator within one group.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AggEntry {
    /// Integer accumulator (count / sum / min / max).
    pub(crate) value: i64,
    /// True once the first non-null input has been seen (for min/max sentinel logic).
    pub(crate) has_value: bool,
    /// Numerator for avg (all input types promoted to f64).
    pub(crate) avg_sum: f64,
    /// Row count for avg denominator.
    pub(crate) avg_count: u64,
    /// Float64 accumulator for sum/min/max when the input column is Float64.
    pub(crate) float_value: f64,
    /// Sum of squares (Σx²) for stddev/variance.
    pub(crate) sq_sum: f64,
}

/// Running aggregation state for one group — one `AggEntry` per expression.
#[derive(Debug)]
pub struct AggState {
    pub(crate) entries: Vec<AggEntry>,
}

impl AggState {
    pub(crate) fn new(agg_exprs: &[AggExpr]) -> Self {
        let entries = agg_exprs
            .iter()
            .map(|expr| AggEntry {
                value: match expr.function {
                    AggFunction::Min => i64::MAX,
                    AggFunction::Max => i64::MIN,
                    _ => 0,
                },
                has_value: false,
                avg_sum: 0.0,
                avg_count: 0,
                float_value: match expr.function {
                    AggFunction::Min => f64::INFINITY,
                    AggFunction::Max => f64::NEG_INFINITY,
                    _ => 0.0,
                },
                sq_sum: 0.0,
            })
            .collect();
        Self { entries }
    }

    /// Test-only reference path: extract a numeric value with a per-call
    /// downcast. Production aggregation uses the pre-downcast fast path
    /// ([`update_pre`](Self::update_pre)); this remains as a cross-check.
    #[cfg(test)]
    fn numeric_value(col: &ArrayRef, row: usize, input_column: &str) -> ExecResult<f64> {
        match col.data_type() {
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    ExecError::UnsupportedType(
                        "declared Int32 aggregate input failed downcast".into(),
                    )
                })?;
                Ok(arr.value(row) as f64)
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ExecError::UnsupportedType(
                        "declared Int64 aggregate input failed downcast".into(),
                    )
                })?;
                Ok(arr.value(row) as f64)
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    ExecError::UnsupportedType(
                        "declared Float64 aggregate input failed downcast".into(),
                    )
                })?;
                Ok(arr.value(row))
            }
            other => Err(ExecError::UnsupportedType(format!(
                "unsupported aggregate input type for {input_column}: {other}"
            ))),
        }
    }

    /// Test-only reference implementation kept as a cross-check against the
    /// production [`update_pre`](Self::update_pre) fast path. Resolves the input
    /// column and downcasts per row, which is why production code does not use
    /// it on the hot loop.
    #[cfg(test)]
    pub(crate) fn update(
        &mut self,
        agg_exprs: &[AggExpr],
        batch: &RecordBatch,
        row: usize,
    ) -> ExecResult<()> {
        for (entry, expr) in self.entries.iter_mut().zip(agg_exprs.iter()) {
            match expr.function {
                AggFunction::Count => {
                    entry.value = entry.value.checked_add(1).ok_or_else(|| {
                        ExecError::InvalidInput("count overflow: i64::MAX reached".into())
                    })?;
                    entry.has_value = true;
                }
                AggFunction::Sum | AggFunction::Min | AggFunction::Max => {
                    let col_idx = batch
                        .schema()
                        .index_of(&expr.input_column)
                        .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                    let col = batch.column(col_idx);
                    match col.data_type() {
                        DataType::Float64 => {
                            let v = Self::numeric_value(col, row, &expr.input_column)?;
                            match expr.function {
                                AggFunction::Sum => entry.float_value += v,
                                AggFunction::Min if v < entry.float_value => entry.float_value = v,
                                AggFunction::Max if v > entry.float_value => entry.float_value = v,
                                _ => {}
                            }
                            entry.has_value = true;
                        }
                        _ => {
                            let v = match col.data_type() {
                                DataType::Int32 => {
                                    let arr = col
                                        .as_any()
                                        .downcast_ref::<Int32Array>()
                                        .ok_or_else(|| {
                                            ExecError::UnsupportedType(
                                                "declared Int32 aggregate input failed downcast"
                                                    .into(),
                                            )
                                        })?;
                                    arr.value(row) as i64
                                }
                                DataType::Int64 => {
                                    let arr = col
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .ok_or_else(|| {
                                            ExecError::UnsupportedType(
                                                "declared Int64 aggregate input failed downcast"
                                                    .into(),
                                            )
                                        })?;
                                    arr.value(row)
                                }
                                other => {
                                    return Err(ExecError::UnsupportedType(format!(
                                        "unsupported aggregate input type: {other}"
                                    )));
                                }
                            };
                            match expr.function {
                                AggFunction::Sum => {
                                    entry.value = entry.value.checked_add(v).ok_or_else(|| {
                                        ExecError::InvalidInput(format!(
                                            "sum overflow on column '{}': i64::MAX reached",
                                            expr.input_column
                                        ))
                                    })?;
                                }
                                AggFunction::Min => {
                                    if !entry.has_value || v < entry.value {
                                        entry.value = v;
                                    }
                                }
                                AggFunction::Max => {
                                    if !entry.has_value || v > entry.value {
                                        entry.value = v;
                                    }
                                }
                                AggFunction::Count | AggFunction::Avg | AggFunction::Stddev => {
                                    return Err(ExecError::InvalidInput(
                                        "unexpected Count/Avg/Stddev in Sum/Min/Max branch".into(),
                                    ));
                                }
                            }
                            entry.has_value = true;
                        }
                    }
                }
                AggFunction::Avg | AggFunction::Stddev => {
                    let col_idx = batch
                        .schema()
                        .index_of(&expr.input_column)
                        .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                    let col = batch.column(col_idx);
                    let v = Self::numeric_value(col, row, &expr.input_column)?;
                    entry.avg_sum += v;
                    entry.avg_count += 1;
                    entry.sq_sum += v * v;
                    entry.has_value = true;
                }
            }
        }
        Ok(())
    }

    /// Fast per-row update against columns pre-downcast once per batch by
    /// [`downcast_agg_input_cols`]. Equivalent to [`update`](Self::update) but
    /// without the per-row `index_of` / `downcast_ref` cost — this is the path
    /// the streaming window operators take on their hot loop.
    pub(crate) fn update_pre(
        &mut self,
        agg_exprs: &[AggExpr],
        pre_cols: &[Option<PreDowncastCol>],
        row: usize,
    ) -> ExecResult<()> {
        update_agg_state_pre(self, agg_exprs, pre_cols, row)
    }

    pub(crate) fn finalized_value(&self, i: usize, expr: &AggExpr) -> ExecResult<i64> {
        let e = self
            .entries
            .get(i)
            .ok_or_else(|| ExecError::InvalidInput(format!("agg state index {i} out of range")))?;
        Ok(match expr.function {
            AggFunction::Min => {
                if e.has_value {
                    e.value
                } else {
                    i64::MAX
                }
            }
            AggFunction::Max => {
                if e.has_value {
                    e.value
                } else {
                    i64::MIN
                }
            }
            _ => e.value,
        })
    }

    pub(crate) fn finalized_avg(&self, i: usize) -> ExecResult<f64> {
        let e = self
            .entries
            .get(i)
            .ok_or_else(|| ExecError::InvalidInput(format!("agg state index {i} out of range")))?;
        Ok(if e.avg_count == 0 {
            f64::NAN
        } else {
            e.avg_sum / e.avg_count as f64
        })
    }

    pub(crate) fn finalized_stddev(&self, i: usize) -> ExecResult<f64> {
        let e = self
            .entries
            .get(i)
            .ok_or_else(|| ExecError::InvalidInput(format!("agg state index {i} out of range")))?;
        let n = e.avg_count;
        if n < 2 {
            // Sample standard deviation is undefined for fewer than two
            // observations; emit 0.0 rather than NaN into the non-null column.
            return Ok(0.0);
        }
        let n_f = n as f64;
        // Bessel-corrected sample variance via the Σx² / Σx form, guarded against
        // a tiny negative from floating-point cancellation.
        let variance = (e.sq_sum - (e.avg_sum * e.avg_sum) / n_f) / (n_f - 1.0);
        Ok(variance.max(0.0).sqrt())
    }

    pub(crate) fn finalized_float_value(&self, i: usize, expr: &AggExpr) -> ExecResult<f64> {
        let e = self
            .entries
            .get(i)
            .ok_or_else(|| ExecError::InvalidInput(format!("agg state index {i} out of range")))?;
        Ok(match expr.function {
            AggFunction::Min => {
                if e.has_value {
                    e.float_value
                } else {
                    f64::INFINITY
                }
            }
            AggFunction::Max => {
                if e.has_value {
                    e.float_value
                } else {
                    f64::NEG_INFINITY
                }
            }
            _ => e.float_value,
        })
    }
}

/// Pre-downcast the aggregate **input** columns of `batch` once per batch.
///
/// This hoists the per-row `schema().index_of()` lookup and the per-row
/// `as_any().downcast_ref()` out of the hot per-row loop: the window operators
/// call this once before iterating rows, then call [`AggState::update_pre`] per
/// row against the cached columns. `Count` aggregates need no input column and
/// map to `None`. Column-resolution errors (missing column / wrong type)
/// surface here, at batch setup, instead of on every row.
pub(crate) fn downcast_agg_input_cols<'a>(
    batch: &'a RecordBatch,
    agg_exprs: &[AggExpr],
) -> ExecResult<Vec<Option<PreDowncastCol<'a>>>> {
    let mut pre = Vec::with_capacity(agg_exprs.len());
    for expr in agg_exprs {
        if matches!(expr.function, AggFunction::Count) {
            pre.push(None);
        } else {
            let idx = batch
                .schema()
                .index_of(&expr.input_column)
                .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
            pre.push(Some(PreDowncastCol::downcast(batch.column(idx))?));
        }
    }
    Ok(pre)
}

/// Fast per-row aggregate state update using pre-downcasted columns.
fn update_agg_state_pre(
    state: &mut AggState,
    agg_exprs: &[AggExpr],
    pre_cols: &[Option<PreDowncastCol>],
    row: usize,
) -> ExecResult<()> {
    for (i, (entry, (expr, pre_col))) in state
        .entries
        .iter_mut()
        .zip(agg_exprs.iter().zip(pre_cols.iter()))
        .enumerate()
    {
        match expr.function {
            AggFunction::Count => {
                entry.value = entry.value.checked_add(1).ok_or_else(|| {
                    ExecError::InvalidInput("count overflow: i64::MAX reached".into())
                })?;
                entry.has_value = true;
            }
            AggFunction::Sum | AggFunction::Min | AggFunction::Max => {
                let col = pre_col.as_ref().ok_or_else(|| {
                    ExecError::Arrow(format!(
                        "Sum/Min/Max aggregate expr {i} missing input column"
                    ))
                })?;
                if matches!(col, PreDowncastCol::Float64(_)) {
                    let v = col.float64_value(row)?;
                    match expr.function {
                        AggFunction::Sum => entry.float_value += v,
                        AggFunction::Min if v < entry.float_value => entry.float_value = v,
                        AggFunction::Max if v > entry.float_value => entry.float_value = v,
                        _ => {}
                    }
                    entry.has_value = true;
                } else {
                    let v = col.int64_value(row)?;
                    match expr.function {
                        AggFunction::Sum => {
                            entry.value = entry.value.checked_add(v).ok_or_else(|| {
                                ExecError::InvalidInput(
                                    "sum overflow in pre-downcast path: i64::MAX reached".into(),
                                )
                            })?;
                            entry.has_value = true;
                        }
                        AggFunction::Min => {
                            if !entry.has_value || v < entry.value {
                                entry.value = v;
                            }
                            entry.has_value = true;
                        }
                        AggFunction::Max => {
                            if !entry.has_value || v > entry.value {
                                entry.value = v;
                            }
                            entry.has_value = true;
                        }
                        _ => {
                            return Err(ExecError::Arrow(format!(
                                "unexpected aggregate function {:?} for numeric column",
                                expr.function
                            )));
                        }
                    }
                }
            }
            AggFunction::Avg | AggFunction::Stddev => {
                let col = pre_col.as_ref().ok_or_else(|| {
                    ExecError::Arrow(format!(
                        "Avg/Stddev aggregate expr {i} missing input column"
                    ))
                })?;
                let v = col.float64_value(row)?;
                entry.avg_sum += v;
                entry.avg_count += 1;
                entry.sq_sum += v * v;
                entry.has_value = true;
            }
        }
    }
    Ok(())
}

/// Local pre-aggregation operator (test-only).
///
/// Groups a `RecordBatch` by `group_by` columns and computes aggregates.
/// Local in-memory group-by aggregator for testing and single-batch use.
/// Not yet wired into the production window operators (which use streaming
/// incremental aggregation via `AggState`).
///
/// The `#[allow(dead_code)]` propagates to the helpers this struct uses
/// (`PreDowncastCol::Utf8`/`Bool` variants, `extract_agg_key`,
/// `AggKey::cmp`/`discriminant`).  When this gets wired into production,
/// remove the annotation in the same PR and verify the now-used helpers
/// are also marked properly.
#[allow(dead_code)]
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

        // Pre-downcast group-by columns once.
        let pre_gb: Vec<PreDowncastCol> = gb_indices
            .iter()
            .map(|&idx| PreDowncastCol::downcast(batch.column(idx)))
            .collect::<ExecResult<_>>()?;

        // Pre-downcast aggregate input columns once.
        let pre_agg_cols = downcast_agg_input_cols(batch, &self.agg_exprs)?;

        let mut groups: HashMap<SmallVec<[AggKey; 4]>, AggState> = HashMap::new();

        for row in 0..batch.num_rows() {
            let key: SmallVec<[AggKey; 4]> =
                pre_gb.iter().map(|col| col.extract_agg_key(row)).collect();

            let state = groups
                .entry(key)
                .or_insert_with(|| AggState::new(&self.agg_exprs));
            update_agg_state_pre(state, &self.agg_exprs, &pre_agg_cols, row)?;
        }

        let mut sorted_entries: Vec<(SmallVec<[AggKey; 4]>, AggState)> =
            groups.into_iter().collect();
        sorted_entries.sort_by(|(a, _), (b, _)| {
            a.iter()
                .zip(b.iter())
                .map(|(ai, bi)| ai.cmp(bi))
                .find(|&o| o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len()))
        });

        let mut fields: Vec<Field> = Vec::with_capacity(self.group_by.len() + self.agg_exprs.len());
        for col_name in &self.group_by {
            let schema = batch.schema();
            let f = schema
                .field_with_name(col_name)
                .map_err(|_| ExecError::ColumnNotFound(col_name.clone()))?;
            fields.push(f.clone());
        }
        // Determine output dtype per aggregate: Float64 if input column is Float64.
        let agg_out_dtypes: Vec<DataType> = self
            .agg_exprs
            .iter()
            .map(|agg| match agg.function {
                AggFunction::Avg | AggFunction::Stddev => DataType::Float64,
                AggFunction::Count => DataType::Int64,
                _ => batch
                    .schema()
                    .field_with_name(&agg.input_column)
                    .ok()
                    .map(|f| f.data_type().clone())
                    .filter(|dt| matches!(dt, DataType::Float64))
                    .unwrap_or(DataType::Int64),
            })
            .collect();
        for (agg, dtype) in self.agg_exprs.iter().zip(&agg_out_dtypes) {
            fields.push(Field::new(&agg.output_column, dtype.clone(), true));
        }
        let out_schema = Arc::new(Schema::new(fields));

        let num_rows = sorted_entries.len();

        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(self.group_by.len() + self.agg_exprs.len());

        for (gb_pos, (col_idx, col_name)) in gb_indices
            .iter()
            .copied()
            .zip(self.group_by.iter())
            .enumerate()
        {
            let dtype = batch.schema().field(col_idx).data_type().clone();
            match dtype {
                DataType::Int32 => {
                    let values: Vec<i32> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            match key.get(gb_pos).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "group key index {gb_pos} out of range"
                                ))
                            })? {
                                AggKey::Int32(v) => Ok(*v),
                                _ => Err(ExecError::UnsupportedType(format!(
                                    "Int32 group key mismatch for {col_name}"
                                ))),
                            }
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Int32Array::from(values)) as ArrayRef);
                }
                DataType::Int64 => {
                    let values: Vec<i64> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            match key.get(gb_pos).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "group key index {gb_pos} out of range"
                                ))
                            })? {
                                AggKey::Int64(v) => Ok(*v),
                                _ => Err(ExecError::UnsupportedType(format!(
                                    "Int64 group key mismatch for {col_name}"
                                ))),
                            }
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                }
                DataType::Float64 => {
                    let values: Vec<f64> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            match key.get(gb_pos).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "group key index {gb_pos} out of range"
                                ))
                            })? {
                                AggKey::Float64(bits) => Ok(f64::from_bits(*bits)),
                                _ => Err(ExecError::UnsupportedType(format!(
                                    "Float64 group key mismatch for {col_name}"
                                ))),
                            }
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                }
                DataType::Utf8 => {
                    let strs: Vec<&str> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            match key.get(gb_pos).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "group key index {gb_pos} out of range"
                                ))
                            })? {
                                AggKey::Utf8(s) => Ok(s.as_str()),
                                _ => Err(ExecError::UnsupportedType(format!(
                                    "Utf8 group key mismatch for {col_name}"
                                ))),
                            }
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(StringArray::from(strs)) as ArrayRef);
                }
                DataType::Boolean => {
                    let values: Vec<bool> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            match key.get(gb_pos).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "group key index {gb_pos} out of range"
                                ))
                            })? {
                                AggKey::Bool(v) => Ok(*v),
                                _ => Err(ExecError::UnsupportedType(format!(
                                    "Bool group key mismatch for {col_name}"
                                ))),
                            }
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
                }
                other => {
                    return Err(ExecError::UnsupportedType(format!(
                        "unsupported group-by column type for {col_name}: {other}"
                    )));
                }
            }
        }

        for (agg_pos, (agg, out_dtype)) in self.agg_exprs.iter().zip(&agg_out_dtypes).enumerate() {
            match (agg.function, out_dtype) {
                (AggFunction::Avg, _) | (_, DataType::Float64) => {
                    let arr: Float64Array = sorted_entries
                        .iter()
                        .map(|(_, state)| match agg.function {
                            AggFunction::Avg => state.finalized_avg(agg_pos),
                            AggFunction::Stddev => state.finalized_stddev(agg_pos),
                            _ => state.finalized_float_value(agg_pos, agg),
                        })
                        .collect::<ExecResult<Vec<f64>>>()?
                        .into();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                _ => {
                    let arr: Int64Array = sorted_entries
                        .iter()
                        .map(|(_, state)| state.finalized_value(agg_pos, agg))
                        .collect::<ExecResult<Vec<i64>>>()?
                        .into();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
            }
        }

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn count_overflow_returns_error() {
        let exprs = vec![AggExpr {
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: "cnt".into(),
        }];
        let mut state = AggState::new(&exprs);
        state.entries[0].value = i64::MAX;
        state.entries[0].has_value = true;

        let result = update_agg_state_pre(&mut state, &exprs, &[None], 0);
        assert!(
            matches!(result, Err(ExecError::InvalidInput(_))),
            "count at i64::MAX must return Err(InvalidInput), got {result:?}"
        );
    }

    #[test]
    fn sum_overflow_returns_error() {
        let exprs = vec![AggExpr {
            function: AggFunction::Sum,
            input_column: "v".into(),
            output_column: "sum_v".into(),
        }];
        let mut state = AggState::new(&exprs);
        state.entries[0].value = i64::MAX - 5;
        state.entries[0].has_value = true;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10i64]))]).unwrap();

        let result = state.update(&exprs, &batch, 0);
        assert!(
            matches!(result, Err(ExecError::InvalidInput(_))),
            "sum overflow near i64::MAX must return Err(InvalidInput), got {result:?}"
        );
    }

    fn stddev_state_over(values: Vec<i64>) -> AggState {
        let exprs = vec![AggExpr {
            function: AggFunction::Stddev,
            input_column: "v".into(),
            output_column: "sd_v".into(),
        }];
        let mut state = AggState::new(&exprs);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let n = values.len();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
        for row in 0..n {
            state.update(&exprs, &batch, row).unwrap();
        }
        state
    }

    #[test]
    fn stddev_computes_bessel_corrected_sample_deviation() {
        // values [1,2,3]: mean 2, sample variance ((1)+(0)+(1))/2 = 1 → stddev 1.0
        let state = stddev_state_over(vec![1, 2, 3]);
        let sd = state.finalized_stddev(0).unwrap();
        assert!(
            (sd - 1.0).abs() < 1e-9,
            "expected sample stddev 1.0, got {sd}"
        );
    }

    #[test]
    fn stddev_of_fewer_than_two_values_is_zero() {
        // Sample stddev is undefined for n<2; we emit 0.0 into the non-null column.
        let state = stddev_state_over(vec![42]);
        assert_eq!(state.finalized_stddev(0).unwrap(), 0.0);
    }
}
