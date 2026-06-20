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
enum PreDowncastCol<'a> {
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
    /// Average of numeric columns (`Int32`, `Int64`, `Float64`).
    Avg,
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
#[derive(Debug)]
pub struct AggState {
    /// Integer aggregates: count, sum, min, max.
    pub(crate) values: Vec<i64>,
    /// Tracks whether Min/Max has received at least one value.
    pub(crate) has_value: Vec<bool>,
    /// Sum for `Avg` (all numeric types promoted to f64).
    pub(crate) avg_sums: Vec<f64>,
    pub(crate) avg_counts: Vec<u64>,
    /// Float64 accumulator for `Sum|Min|Max` when the input column is Float64.
    pub(crate) float_values: Vec<f64>,
}

impl AggState {
    pub(crate) fn new(agg_exprs: &[AggExpr]) -> Self {
        let values = agg_exprs
            .iter()
            .map(|expr| match expr.function {
                AggFunction::Count => 0i64,
                AggFunction::Sum => 0i64,
                AggFunction::Min => i64::MAX,
                AggFunction::Max => i64::MIN,
                AggFunction::Avg => 0i64,
            })
            .collect();
        let has_value = agg_exprs.iter().map(|_| false).collect();
        let avg_sums = agg_exprs.iter().map(|_| 0.0).collect();
        let avg_counts = agg_exprs.iter().map(|_| 0u64).collect();
        let float_values = agg_exprs
            .iter()
            .map(|expr| match expr.function {
                AggFunction::Min => f64::INFINITY,
                AggFunction::Max => f64::NEG_INFINITY,
                _ => 0.0,
            })
            .collect();
        Self {
            values,
            has_value,
            avg_sums,
            avg_counts,
            float_values,
        }
    }

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

    pub(crate) fn update(
        &mut self,
        agg_exprs: &[AggExpr],
        batch: &RecordBatch,
        row: usize,
    ) -> ExecResult<()> {
        for (i, expr) in agg_exprs.iter().enumerate() {
            match expr.function {
                AggFunction::Count => {
                    self.values[i] = self.values[i].checked_add(1).ok_or_else(|| {
                        ExecError::InvalidInput("count overflow: i64::MAX reached".into())
                    })?;
                    self.has_value[i] = true;
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
                                AggFunction::Sum => self.float_values[i] += v,
                                AggFunction::Min => {
                                    if v < self.float_values[i] {
                                        self.float_values[i] = v;
                                    }
                                }
                                AggFunction::Max => {
                                    if v > self.float_values[i] {
                                        self.float_values[i] = v;
                                    }
                                }
                                _ => {}
                            }
                            self.has_value[i] = true;
                        }
                        _ => {
                            let v = match col.data_type() {
                                DataType::Int32 => {
                                    let arr = col.as_any().downcast_ref::<Int32Array>()
                                        .ok_or_else(|| ExecError::UnsupportedType(
                                            "declared Int32 aggregate input failed downcast".into()))?;
                                    arr.value(row) as i64
                                }
                                DataType::Int64 => {
                                    let arr = col.as_any().downcast_ref::<Int64Array>()
                                        .ok_or_else(|| ExecError::UnsupportedType(
                                            "declared Int64 aggregate input failed downcast".into()))?;
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
                                    self.values[i] = self.values[i].checked_add(v).ok_or_else(|| {
                                        ExecError::InvalidInput(format!(
                                            "sum overflow on column '{}': i64::MAX reached",
                                            expr.input_column
                                        ))
                                    })?;
                                }
                                AggFunction::Min => {
                                    if !self.has_value[i] || v < self.values[i] {
                                        self.values[i] = v;
                                    }
                                }
                                AggFunction::Max => {
                                    if !self.has_value[i] || v > self.values[i] {
                                        self.values[i] = v;
                                    }
                                }
                                AggFunction::Count | AggFunction::Avg => {
                                    return Err(ExecError::InvalidInput(
                                        "unexpected Count/Avg in Sum/Min/Max branch".into(),
                                    ));
                                }
                            }
                            self.has_value[i] = true;
                        }
                    }
                }
                AggFunction::Avg => {
                    let col_idx = batch
                        .schema()
                        .index_of(&expr.input_column)
                        .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                    let col = batch.column(col_idx);
                    let v = Self::numeric_value(col, row, &expr.input_column)?;
                    self.avg_sums[i] += v;
                    self.avg_counts[i] += 1;
                    self.has_value[i] = true;
                }
            }
        }
        Ok(())
    }

    /// Return the finalized integer value for position `i`.
    pub(crate) fn finalized_value(&self, i: usize, expr: &AggExpr) -> i64 {
        match expr.function {
            AggFunction::Min => {
                if self.has_value[i] {
                    self.values[i]
                } else {
                    i64::MAX
                }
            }
            AggFunction::Max => {
                if self.has_value[i] {
                    self.values[i]
                } else {
                    i64::MIN
                }
            }
            _ => self.values[i],
        }
    }

    pub(crate) fn finalized_avg(&self, i: usize) -> f64 {
        if self.avg_counts[i] == 0 {
            f64::NAN
        } else {
            self.avg_sums[i] / self.avg_counts[i] as f64
        }
    }

    /// Return the finalized float value for position `i` (Sum/Min/Max on Float64 columns).
    pub(crate) fn finalized_float_value(&self, i: usize, expr: &AggExpr) -> f64 {
        match expr.function {
            AggFunction::Min => {
                if self.has_value[i] { self.float_values[i] } else { f64::INFINITY }
            }
            AggFunction::Max => {
                if self.has_value[i] { self.float_values[i] } else { f64::NEG_INFINITY }
            }
            _ => self.float_values[i],
        }
    }
}

/// Fast per-row aggregate state update using pre-downcasted columns.
///
/// Avoids the per-row schema lookups and downcasts that `AggState::update`
/// performs.  The caller must pre-downcast all aggregate input columns.
fn update_agg_state_pre(
    state: &mut AggState,
    agg_exprs: &[AggExpr],
    pre_cols: &[Option<PreDowncastCol>],
    row: usize,
) -> ExecResult<()> {
    for (i, expr) in agg_exprs.iter().enumerate() {
        match expr.function {
            AggFunction::Count => {
                state.values[i] = state.values[i].checked_add(1).ok_or_else(|| {
                    ExecError::InvalidInput("count overflow: i64::MAX reached".into())
                })?;
                state.has_value[i] = true;
            }
            AggFunction::Sum | AggFunction::Min | AggFunction::Max => {
                let col = pre_cols[i].as_ref().ok_or_else(|| {
                    ExecError::Arrow(format!(
                        "Sum/Min/Max aggregate expr {} missing input column",
                        i
                    ))
                })?;
                if matches!(col, PreDowncastCol::Float64(_)) {
                    let v = col.float64_value(row)?;
                    match expr.function {
                        AggFunction::Sum => state.float_values[i] += v,
                        AggFunction::Min => {
                            if v < state.float_values[i] { state.float_values[i] = v; }
                        }
                        AggFunction::Max => {
                            if v > state.float_values[i] { state.float_values[i] = v; }
                        }
                        _ => {}
                    }
                    state.has_value[i] = true;
                } else {
                    let v = col.int64_value(row)?;
                    match expr.function {
                        AggFunction::Sum => {
                            state.values[i] = state.values[i].checked_add(v).ok_or_else(|| {
                                ExecError::InvalidInput(
                                    "sum overflow in pre-downcast path: i64::MAX reached".into(),
                                )
                            })?;
                            state.has_value[i] = true;
                        }
                        AggFunction::Min => {
                            if !state.has_value[i] || v < state.values[i] {
                                state.values[i] = v;
                            }
                            state.has_value[i] = true;
                        }
                        AggFunction::Max => {
                            if !state.has_value[i] || v > state.values[i] {
                                state.values[i] = v;
                            }
                            state.has_value[i] = true;
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
            AggFunction::Avg => {
                let col = pre_cols[i].as_ref().ok_or_else(|| {
                    ExecError::Arrow(format!("Avg aggregate expr {} missing input column", i))
                })?;
                let v = col.float64_value(row)?;
                state.avg_sums[i] += v;
                state.avg_counts[i] += 1;
                state.has_value[i] = true;
            }
        }
    }
    Ok(())
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
        let mut pre_agg_cols: Vec<Option<PreDowncastCol>> =
            Vec::with_capacity(self.agg_exprs.len());
        for expr in &self.agg_exprs {
            if matches!(expr.function, AggFunction::Count) {
                pre_agg_cols.push(None);
            } else {
                let col_idx = batch
                    .schema()
                    .index_of(&expr.input_column)
                    .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                let col = PreDowncastCol::downcast(batch.column(col_idx))?;
                pre_agg_cols.push(Some(col));
            }
        }

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
        let agg_out_dtypes: Vec<DataType> = self.agg_exprs.iter().map(|agg| {
            match agg.function {
                AggFunction::Avg => DataType::Float64,
                AggFunction::Count => DataType::Int64,
                _ => {
                    // Sum/Min/Max output type follows the input column type.
                    batch.schema().field_with_name(&agg.input_column)
                        .ok()
                        .map(|f| f.data_type().clone())
                        .filter(|dt| matches!(dt, DataType::Float64))
                        .unwrap_or(DataType::Int64)
                }
            }
        }).collect();
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

        for (gb_pos, col_name) in self.group_by.iter().enumerate() {
            let col_idx = gb_indices[gb_pos];
            let dtype = batch.schema().field(col_idx).data_type().clone();
            match dtype {
                DataType::Int32 => {
                    let values: Vec<i32> = sorted_entries
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int32(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Int32 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Int32Array::from(values)) as ArrayRef);
                }
                DataType::Int64 => {
                    let values: Vec<i64> = sorted_entries
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int64(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Int64 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                }
                DataType::Float64 => {
                    let values: Vec<f64> = sorted_entries
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Float64(bits) => Ok(f64::from_bits(bits)),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Float64 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                }
                DataType::Utf8 => {
                    let strs: Vec<&str> = sorted_entries
                        .iter()
                        .map(|(key, _)| match &key[gb_pos] {
                            AggKey::Utf8(s) => Ok(s.as_str()),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Utf8 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<_>>()?;
                    columns.push(Arc::new(StringArray::from(strs)) as ArrayRef);
                }
                DataType::Boolean => {
                    let values: Vec<bool> = sorted_entries
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Bool(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Bool group key mismatch for {col_name}"
                            ))),
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
                        .map(|(_, state)| {
                            if matches!(agg.function, AggFunction::Avg) {
                                state.finalized_avg(agg_pos)
                            } else {
                                state.finalized_float_value(agg_pos, agg)
                            }
                        })
                        .collect();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                _ => {
                    let arr: Int64Array = sorted_entries
                        .iter()
                        .map(|(_, state)| state.finalized_value(agg_pos, agg))
                        .collect();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
            }
        }

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_overflow_returns_error() {
        // Directly exercise update_agg_state_pre with a state that has count = i64::MAX.
        // The next increment must return Err(InvalidInput) rather than wrapping.
        let exprs = vec![AggExpr {
            function: AggFunction::Count,
            input_column: String::new(),
            output_column: "cnt".into(),
        }];
        let mut state = AggState::new(&exprs);
        state.values[0] = i64::MAX;
        state.has_value[0] = true;

        // pre_cols entry is None for Count (input column is ignored).
        let result = update_agg_state_pre(&mut state, &exprs, &[None], 0);
        assert!(
            matches!(result, Err(ExecError::InvalidInput(_))),
            "count at i64::MAX must return Err(InvalidInput), got {result:?}"
        );
    }

    #[test]
    fn sum_overflow_returns_error() {
        // Regression: a running Sum near i64::MAX must report an error rather
        // than silently wrapping (Phase 1 fix for unchecked accumulation).
        let exprs = vec![AggExpr {
            function: AggFunction::Sum,
            input_column: "v".into(),
            output_column: "sum_v".into(),
        }];
        let mut state = AggState::new(&exprs);
        state.values[0] = i64::MAX - 5;
        state.has_value[0] = true;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10i64]))]).unwrap();

        let result = state.update(&exprs, &batch, 0);
        assert!(
            matches!(result, Err(ExecError::InvalidInput(_))),
            "sum overflow near i64::MAX must return Err(InvalidInput), got {result:?}"
        );
    }
}
