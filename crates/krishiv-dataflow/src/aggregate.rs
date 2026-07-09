use ahash::AHashMap as HashMap;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use smallvec::SmallVec;

use krishiv_plan::window::{AggFilterCompareOp, AggFilterValue, WindowAggFilter};

use crate::join::AggKey;
use crate::{ExecError, ExecResult};

// ── Aggregate filter masks ────────────────────────────────────────────────────

/// Evaluate a [`WindowAggFilter`] over a whole batch as a boolean mask.
///
/// NULL results (NULL column values under comparison, Kleene combinators)
/// stay NULL in the mask; consumers must treat NULL as "row excluded", which
/// matches SQL `FILTER (WHERE …)` semantics.
pub(crate) fn eval_agg_filter(
    filter: &WindowAggFilter,
    batch: &RecordBatch,
) -> ExecResult<BooleanArray> {
    use arrow::compute::kernels::boolean::{and_kleene, not, or_kleene};
    match filter {
        WindowAggFilter::Compare { column, op, value } => {
            eval_compare(batch, column, *op, value)
        }
        WindowAggFilter::IsNull { column } => {
            let col = filter_column(batch, column)?;
            arrow::compute::is_null(col.as_ref()).map_err(filter_arrow_err)
        }
        WindowAggFilter::IsNotNull { column } => {
            let col = filter_column(batch, column)?;
            arrow::compute::is_not_null(col.as_ref()).map_err(filter_arrow_err)
        }
        WindowAggFilter::And(a, b) => {
            let (a, b) = (eval_agg_filter(a, batch)?, eval_agg_filter(b, batch)?);
            and_kleene(&a, &b).map_err(filter_arrow_err)
        }
        WindowAggFilter::Or(a, b) => {
            let (a, b) = (eval_agg_filter(a, batch)?, eval_agg_filter(b, batch)?);
            or_kleene(&a, &b).map_err(filter_arrow_err)
        }
        WindowAggFilter::Not(inner) => {
            let inner = eval_agg_filter(inner, batch)?;
            not(&inner).map_err(filter_arrow_err)
        }
    }
}

fn filter_arrow_err(e: arrow::error::ArrowError) -> ExecError {
    ExecError::InvalidInput(format!("aggregate filter evaluation failed: {e}"))
}

fn filter_column<'a>(batch: &'a RecordBatch, name: &str) -> ExecResult<&'a ArrayRef> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| ExecError::ColumnNotFound(name.to_string()))?;
    Ok(batch.column(idx))
}

fn eval_compare(
    batch: &RecordBatch,
    column: &str,
    op: AggFilterCompareOp,
    value: &AggFilterValue,
) -> ExecResult<BooleanArray> {
    use arrow::array::Scalar;
    use arrow::compute::kernels::cmp;
    use arrow::datatypes::DataType;

    fn cmp_datum(
        op: AggFilterCompareOp,
        lhs: &dyn arrow::array::Datum,
        rhs: &dyn arrow::array::Datum,
    ) -> Result<BooleanArray, arrow::error::ArrowError> {
        match op {
            AggFilterCompareOp::Eq => cmp::eq(lhs, rhs),
            AggFilterCompareOp::NotEq => cmp::neq(lhs, rhs),
            AggFilterCompareOp::Lt => cmp::lt(lhs, rhs),
            AggFilterCompareOp::LtEq => cmp::lt_eq(lhs, rhs),
            AggFilterCompareOp::Gt => cmp::gt(lhs, rhs),
            AggFilterCompareOp::GtEq => cmp::gt_eq(lhs, rhs),
        }
    }

    let col = filter_column(batch, column)?;
    let result = match (col.data_type(), value) {
        (DataType::Utf8, AggFilterValue::Utf8(s)) => cmp_datum(
            op,
            col,
            &Scalar::new(StringArray::from(vec![s.as_str()])),
        ),
        (DataType::Boolean, AggFilterValue::Bool(b)) => cmp_datum(
            op,
            col,
            &Scalar::new(BooleanArray::from(vec![*b])),
        ),
        (DataType::Int32 | DataType::Int64, AggFilterValue::Int(v)) => {
            let cast = arrow::compute::cast(col, &DataType::Int64).map_err(filter_arrow_err)?;
            cmp_datum(op, &cast, &Scalar::new(Int64Array::from(vec![*v])))
        }
        // Any numeric/float mix compares in f64.
        (
            DataType::Int32 | DataType::Int64 | DataType::Float64,
            AggFilterValue::Int(_) | AggFilterValue::Float(_),
        ) => {
            let lit = match value {
                AggFilterValue::Int(v) => *v as f64,
                AggFilterValue::Float(f) => f.0,
                _ => unreachable!("outer match restricts to numeric literals"),
            };
            let cast = arrow::compute::cast(col, &DataType::Float64).map_err(filter_arrow_err)?;
            cmp_datum(op, &cast, &Scalar::new(Float64Array::from(vec![lit])))
        }
        (other, value) => {
            return Err(ExecError::UnsupportedType(format!(
                "aggregate filter cannot compare column '{column}' of type {other} \
                 against literal {value:?}"
            )));
        }
    };
    result.map_err(filter_arrow_err)
}

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

    /// True when the row's value is NULL. SQL aggregates skip NULL inputs;
    /// without this check `value(row)` reads the type's default (0) into the
    /// accumulator.
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int32(arr) => arr.is_null(row),
            Self::Int64(arr) => arr.is_null(row),
            Self::Float64(arr) => arr.is_null(row),
            Self::Utf8(arr) => arr.is_null(row),
            Self::Bool(arr) => arr.is_null(row),
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
    /// Optional row predicate (SQL `FILTER (WHERE …)` / `CASE WHEN` lowering).
    /// Rows failing it (or where it evaluates to NULL) do not feed this
    /// aggregate. Evaluated once per batch as a boolean mask.
    pub filter: Option<WindowAggFilter>,
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
#[derive(Debug, Clone)]
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
            if let Some(filter) = &expr.filter {
                // Reference path: evaluate the mask per call (tests only).
                let mask = eval_agg_filter(filter, batch)?;
                if !(mask.is_valid(row) && mask.value(row)) {
                    continue;
                }
            }
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
                    if col.is_null(row) {
                        // SQL semantics: NULL inputs do not feed the aggregate.
                        continue;
                    }
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
                    if col.is_null(row) {
                        // SQL semantics: NULL inputs do not feed the aggregate.
                        continue;
                    }
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
        pre: &PreparedAggInputs,
        row: usize,
    ) -> ExecResult<()> {
        update_agg_state_pre(self, agg_exprs, pre, row)
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

/// Per-batch prepared aggregate inputs: pre-downcast value columns plus the
/// evaluated filter mask for each aggregate expression.
pub(crate) struct PreparedAggInputs<'a> {
    pub(crate) cols: Vec<Option<PreDowncastCol<'a>>>,
    /// `Some(mask)` for filtered aggregates. A NULL mask slot excludes the
    /// row, matching SQL `FILTER (WHERE …)` semantics.
    pub(crate) masks: Vec<Option<BooleanArray>>,
}

impl PreparedAggInputs<'_> {
    #[inline]
    fn row_included(&self, i: usize, row: usize) -> bool {
        match self.masks.get(i).and_then(|m| m.as_ref()) {
            Some(mask) => mask.is_valid(row) && mask.value(row),
            None => true,
        }
    }
}

/// Prepare the aggregate **inputs** of `batch` once per batch.
///
/// This hoists the per-row `schema().index_of()` lookup and the per-row
/// `as_any().downcast_ref()` out of the hot per-row loop, and evaluates each
/// aggregate's filter predicate once as a batch-level boolean mask: the window
/// operators call this once before iterating rows, then call
/// [`AggState::update_pre`] per row against the cached columns/masks. `Count`
/// aggregates need no input column and map to `None`. Column-resolution errors
/// (missing column / wrong type) surface here, at batch setup, instead of on
/// every row.
pub(crate) fn downcast_agg_input_cols<'a>(
    batch: &'a RecordBatch,
    agg_exprs: &[AggExpr],
) -> ExecResult<PreparedAggInputs<'a>> {
    let mut cols = Vec::with_capacity(agg_exprs.len());
    let mut masks = Vec::with_capacity(agg_exprs.len());
    for expr in agg_exprs {
        if matches!(expr.function, AggFunction::Count) {
            cols.push(None);
        } else {
            let idx = batch
                .schema()
                .index_of(&expr.input_column)
                .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
            cols.push(Some(PreDowncastCol::downcast(batch.column(idx))?));
        }
        masks.push(match &expr.filter {
            Some(filter) => Some(eval_agg_filter(filter, batch)?),
            None => None,
        });
    }
    Ok(PreparedAggInputs { cols, masks })
}

/// Fast per-row aggregate state update using pre-downcasted columns.
fn update_agg_state_pre(
    state: &mut AggState,
    agg_exprs: &[AggExpr],
    pre: &PreparedAggInputs,
    row: usize,
) -> ExecResult<()> {
    for (i, (entry, expr)) in state.entries.iter_mut().zip(agg_exprs.iter()).enumerate() {
        if !pre.row_included(i, row) {
            continue;
        }
        let pre_col = pre.cols.get(i).and_then(|c| c.as_ref());
        match expr.function {
            AggFunction::Count => {
                entry.value = entry.value.checked_add(1).ok_or_else(|| {
                    ExecError::InvalidInput("count overflow: i64::MAX reached".into())
                })?;
                entry.has_value = true;
            }
            AggFunction::Sum | AggFunction::Min | AggFunction::Max => {
                let col = pre_col.ok_or_else(|| {
                    ExecError::Arrow(format!(
                        "Sum/Min/Max aggregate expr {i} missing input column"
                    ))
                })?;
                if col.is_null(row) {
                    // SQL semantics: NULL inputs do not feed the aggregate.
                    continue;
                }
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
                let col = pre_col.ok_or_else(|| {
                    ExecError::Arrow(format!(
                        "Avg/Stddev aggregate expr {i} missing input column"
                    ))
                })?;
                if col.is_null(row) {
                    // SQL semantics: NULL inputs do not feed the aggregate.
                    continue;
                }
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
        let exprs = vec![AggExpr { filter: None,
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: "cnt".into(),
        }];
        let mut state = AggState::new(&exprs);
        state.entries[0].value = i64::MAX;
        state.entries[0].has_value = true;

        let pre = PreparedAggInputs {
            cols: vec![None],
            masks: vec![None],
        };
        let result = update_agg_state_pre(&mut state, &exprs, &pre, 0);
        assert!(
            matches!(result, Err(ExecError::InvalidInput(_))),
            "count at i64::MAX must return Err(InvalidInput), got {result:?}"
        );
    }

    #[test]
    fn sum_overflow_returns_error() {
        let exprs = vec![AggExpr { filter: None,
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
        let exprs = vec![AggExpr { filter: None,
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
