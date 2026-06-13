use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array, Int64Array,
    Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use smallvec::SmallVec;

use crate::join::AggKey;
use crate::spill::SpillFile;
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
        Self {
            values,
            has_value,
            avg_sums,
            avg_counts,
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
                    let v = match col.data_type() {
                        DataType::Int32 => {
                            let arr =
                                col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                                    ExecError::UnsupportedType(
                                        "declared Int32 aggregate input failed downcast".into(),
                                    )
                                })?;
                            arr.value(row) as i64
                        }
                        DataType::Int64 => {
                            let arr =
                                col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                                    ExecError::UnsupportedType(
                                        "declared Int64 aggregate input failed downcast".into(),
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
                            self.values[i] = self.values[i].checked_add(v).ok_or_else(|| {
                                ExecError::InvalidInput(format!(
                                    "sum overflow on column '{}': i64::MAX reached",
                                    expr.input_column
                                ))
                            })?;
                            self.has_value[i] = true;
                        }
                        AggFunction::Min => {
                            if !self.has_value[i] || v < self.values[i] {
                                self.values[i] = v;
                            }
                            self.has_value[i] = true;
                        }
                        AggFunction::Max => {
                            if !self.has_value[i] || v > self.values[i] {
                                self.values[i] = v;
                            }
                            self.has_value[i] = true;
                        }
                        AggFunction::Count | AggFunction::Avg => {
                            return Err(ExecError::InvalidInput(
                                "unexpected Count/Avg in Sum/Min/Max branch".into(),
                            ));
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

        // Pre-resolve aggregate column indices and pre-downcast once.
        let mut pre_agg_indices: Vec<usize> = Vec::with_capacity(self.agg_exprs.len());
        let mut pre_agg_cols: Vec<Option<PreDowncastCol>> =
            Vec::with_capacity(self.agg_exprs.len());
        for expr in &self.agg_exprs {
            if matches!(expr.function, AggFunction::Count) {
                pre_agg_indices.push(0); // unused
                pre_agg_cols.push(None);
            } else {
                let col_idx = batch
                    .schema()
                    .index_of(&expr.input_column)
                    .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                let col = PreDowncastCol::downcast(batch.column(col_idx))?;
                pre_agg_indices.push(col_idx);
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
        for agg in &self.agg_exprs {
            let dtype = match agg.function {
                AggFunction::Avg => DataType::Float64,
                _ => DataType::Int64,
            };
            fields.push(Field::new(&agg.output_column, dtype, true));
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

        for (agg_pos, agg) in self.agg_exprs.iter().enumerate() {
            match agg.function {
                AggFunction::Avg => {
                    let arr: Float64Array = sorted_entries
                        .iter()
                        .map(|(_, state)| state.finalized_avg(agg_pos))
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

// ── ExternalAggregator ────────────────────────────────────────────────────────

/// Memory-bounded aggregation operator that spills partial aggregates to disk.
///
/// Accumulates input batches across multiple `push` calls, tracking estimated
/// HashMap memory usage.  When the in-memory groups exceed the attached budget,
/// the partial aggregates are serialized to Arrow IPC spill files.  `finish`
/// merges all spill runs into the final output batch.
///
/// Without a budget the operator is fully in-memory, identical in semantics to
/// `LocalAggregator::aggregate` called once per batch but accumulated.
pub struct ExternalAggregator {
    group_by: Vec<String>,
    agg_exprs: Vec<AggExpr>,
    memory_budget: Option<Arc<krishiv_common::MemoryBudget>>,
    groups: HashMap<SmallVec<[AggKey; 4]>, AggState>,
    reserved_bytes: u64,
    runs: Vec<SpillFile>,
    spill_bytes: AtomicU64,
    spill_file_count: AtomicU64,
    input_schema: Option<SchemaRef>,
}

impl ExternalAggregator {
    /// Create a new aggregator.
    pub fn new(group_by: Vec<String>, agg_exprs: Vec<AggExpr>) -> Self {
        Self {
            group_by,
            agg_exprs,
            memory_budget: None,
            groups: HashMap::new(),
            reserved_bytes: 0,
            runs: Vec::new(),
            spill_bytes: AtomicU64::new(0),
            spill_file_count: AtomicU64::new(0),
            input_schema: None,
        }
    }

    /// Attach a shared memory budget; exceeding it spills partial aggregates.
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<krishiv_common::MemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Process `batch`, accumulating rows into in-memory partial aggregates.
    pub fn push(&mut self, batch: &RecordBatch) -> ExecResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.input_schema.is_none() {
            self.input_schema = Some(batch.schema());
        }

        // Resolve group-by and aggregate column indices.
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
        let pre_gb: Vec<PreDowncastCol> = gb_indices
            .iter()
            .map(|&idx| PreDowncastCol::downcast(batch.column(idx)))
            .collect::<ExecResult<_>>()?;

        let mut pre_agg_indices: Vec<usize> = Vec::with_capacity(self.agg_exprs.len());
        let mut pre_agg_cols: Vec<Option<PreDowncastCol>> =
            Vec::with_capacity(self.agg_exprs.len());
        for expr in &self.agg_exprs {
            if matches!(expr.function, AggFunction::Count) {
                pre_agg_indices.push(0);
                pre_agg_cols.push(None);
            } else {
                let col_idx = batch
                    .schema()
                    .index_of(&expr.input_column)
                    .map_err(|_| ExecError::ColumnNotFound(expr.input_column.clone()))?;
                pre_agg_cols.push(Some(PreDowncastCol::downcast(batch.column(col_idx))?));
                pre_agg_indices.push(col_idx);
            }
        }

        for row in 0..batch.num_rows() {
            let key: SmallVec<[AggKey; 4]> =
                pre_gb.iter().map(|col| col.extract_agg_key(row)).collect();
            let state = self
                .groups
                .entry(key)
                .or_insert_with(|| AggState::new(&self.agg_exprs));
            update_agg_state_pre(state, &self.agg_exprs, &pre_agg_cols, row)?;
        }

        // Spill if memory budget exceeded.  Estimate: ~256 bytes per group
        // (key + AggState overhead) plus 24 bytes per aggregate expression.
        if let Some(budget) = self.memory_budget.clone() {
            let per_group: u64 = 256 + (24 * self.agg_exprs.len()) as u64;
            let estimated: u64 = (self.groups.len() as u64).saturating_mul(per_group);
            let over_budget = budget
                .limit()
                .map(|limit| estimated > limit.saturating_sub(self.reserved_bytes))
                .unwrap_or(false);
            if over_budget {
                self.spill_partial()?;
            } else {
                // Reserve the incremental delta so budget tracking stays accurate.
                let delta = estimated.saturating_sub(self.reserved_bytes);
                if delta > 0 && !budget.try_reserve(delta) {
                    self.spill_partial()?;
                } else {
                    self.reserved_bytes = estimated;
                }
            }
        }
        Ok(())
    }

    /// Merge all spilled runs with in-memory groups and return the final batch.
    pub fn finish(&mut self) -> ExecResult<RecordBatch> {
        let schema = self.input_schema.clone();

        // Spill remaining in-memory state so all runs are uniform.
        if !self.runs.is_empty() && !self.groups.is_empty() {
            if let Some(ref s) = schema {
                self.spill_partial_with_schema(s.clone())?;
            }
        }

        let spilled = std::mem::take(&mut self.runs);
        // Merge spill runs into groups.
        for file in &spilled {
            for batch in file.read()? {
                self.merge_partial_batch(&batch)?;
            }
        }
        self.release_reserved();
        drop(spilled); // removes temp files

        let input_schema = match &schema {
            Some(s) => s.clone(),
            None => {
                // No rows pushed; return empty batch.
                return Ok(RecordBatch::new_empty(self.empty_output_schema()));
            }
        };
        self.build_output_batch(&input_schema)
    }

    /// Total bytes written to spill files.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes.load(AtomicOrdering::Relaxed)
    }

    /// Number of spill files written.
    pub fn spill_file_count(&self) -> u64 {
        self.spill_file_count.load(AtomicOrdering::Relaxed)
    }

    fn spill_partial(&mut self) -> ExecResult<()> {
        let schema = self.input_schema.clone().ok_or_else(|| {
            ExecError::Spill("spill_partial called before any batch was pushed".into())
        })?;
        self.spill_partial_with_schema(schema)
    }

    fn spill_partial_with_schema(&mut self, input_schema: SchemaRef) -> ExecResult<()> {
        if self.groups.is_empty() {
            return Ok(());
        }
        let partial_schema =
            Self::build_partial_schema(&self.group_by, &self.agg_exprs, &input_schema);
        let batch = self.serialize_partial(&partial_schema, &input_schema)?;
        let (file, bytes) = SpillFile::write("agg", &partial_schema, &[batch])?;
        self.spill_bytes.fetch_add(bytes, AtomicOrdering::Relaxed);
        self.spill_file_count.fetch_add(1, AtomicOrdering::Relaxed);
        let metrics = krishiv_metrics::global_metrics();
        metrics.record_spill(bytes, 1);
        metrics.record_operator_memory("external_aggregate", self.reserved_bytes);
        self.runs.push(file);
        self.groups.clear();
        self.release_reserved();
        Ok(())
    }

    fn release_reserved(&mut self) {
        if let Some(budget) = &self.memory_budget {
            budget.release(self.reserved_bytes);
        }
        self.reserved_bytes = 0;
    }

    /// Build the partial-aggregate RecordBatch schema.
    ///
    /// Layout: group-by columns | per-agg [__pval_i, __phas_i, __pavs_i, __pavc_i]
    fn build_partial_schema(
        group_by: &[String],
        agg_exprs: &[AggExpr],
        input_schema: &Schema,
    ) -> Schema {
        let mut fields: Vec<Field> = Vec::with_capacity(group_by.len() + agg_exprs.len() * 4);
        for col in group_by {
            if let Ok(f) = input_schema.field_with_name(col) {
                fields.push(f.clone());
            }
        }
        for i in 0..agg_exprs.len() {
            fields.push(Field::new(format!("__pval_{i}"), DataType::Int64, false));
            fields.push(Field::new(format!("__phas_{i}"), DataType::Boolean, false));
            fields.push(Field::new(format!("__pavs_{i}"), DataType::Float64, false));
            fields.push(Field::new(format!("__pavc_{i}"), DataType::Int64, false));
        }
        Schema::new(fields)
    }

    /// Serialize the in-memory `groups` HashMap to a partial-aggregate RecordBatch.
    fn serialize_partial(
        &self,
        partial_schema: &Schema,
        input_schema: &SchemaRef,
    ) -> ExecResult<RecordBatch> {
        let mut sorted: Vec<(&SmallVec<[AggKey; 4]>, &AggState)> = self.groups.iter().collect();
        sorted.sort_by(|(a, _), (b, _)| {
            a.iter()
                .zip(b.iter())
                .map(|(ai, bi)| ai.cmp(bi))
                .find(|&o| o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });

        let n = sorted.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(partial_schema.fields().len());

        // Group-by columns.
        for (gb_pos, col_name) in self.group_by.iter().enumerate() {
            let dtype = input_schema
                .field_with_name(col_name)
                .map_err(|_| ExecError::ColumnNotFound(col_name.clone()))?
                .data_type()
                .clone();
            let col: ArrayRef = match dtype {
                DataType::Int32 => Arc::new(Int32Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int32(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType("Int32 key mismatch".into())),
                        })
                        .collect::<ExecResult<Vec<i32>>>()?,
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int64(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType("Int64 key mismatch".into())),
                        })
                        .collect::<ExecResult<Vec<i64>>>()?,
                )),
                DataType::Float64 => Arc::new(Float64Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Float64(bits) => Ok(f64::from_bits(bits)),
                            _ => Err(ExecError::UnsupportedType("Float64 key mismatch".into())),
                        })
                        .collect::<ExecResult<Vec<f64>>>()?,
                )),
                DataType::Utf8 => Arc::new(StringArray::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match &key[gb_pos] {
                            AggKey::Utf8(s) => Ok(s.clone()),
                            _ => Err(ExecError::UnsupportedType("Utf8 key mismatch".into())),
                        })
                        .collect::<ExecResult<Vec<String>>>()?,
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Bool(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType("Bool key mismatch".into())),
                        })
                        .collect::<ExecResult<Vec<bool>>>()?,
                )),
                other => {
                    return Err(ExecError::UnsupportedType(format!(
                        "unsupported group-by type for {col_name}: {other}"
                    )));
                }
            };
            columns.push(col);
        }

        // Partial aggregate columns.
        for i in 0..self.agg_exprs.len() {
            let mut val_b = Int64Builder::with_capacity(n);
            let mut has_b = BooleanBuilder::with_capacity(n);
            let mut avs_b = Float64Builder::with_capacity(n);
            let mut avc_b = Int64Builder::with_capacity(n);
            for (_, state) in &sorted {
                val_b.append_value(state.values[i]);
                has_b.append_value(state.has_value[i]);
                avs_b.append_value(state.avg_sums[i]);
                avc_b.append_value(state.avg_counts[i] as i64);
            }
            columns.push(Arc::new(val_b.finish()) as ArrayRef);
            columns.push(Arc::new(has_b.finish()) as ArrayRef);
            columns.push(Arc::new(avs_b.finish()) as ArrayRef);
            columns.push(Arc::new(avc_b.finish()) as ArrayRef);
        }

        Ok(RecordBatch::try_new(
            Arc::new(partial_schema.clone()),
            columns,
        )?)
    }

    /// Merge one partial-aggregate RecordBatch into `self.groups`.
    fn merge_partial_batch(&mut self, partial: &RecordBatch) -> ExecResult<()> {
        let n = partial.num_rows();
        if n == 0 {
            return Ok(());
        }

        let n_gb = self.group_by.len();
        let n_agg = self.agg_exprs.len();

        // Pre-downcast group-by columns.
        let pre_gb: Vec<PreDowncastCol> = (0..n_gb)
            .map(|i| PreDowncastCol::downcast(partial.column(i)))
            .collect::<ExecResult<_>>()?;

        // Pre-read partial agg columns: 4 columns per agg_expr starting after group-by.
        let base = n_gb;
        let val_cols: Vec<&Int64Array> = (0..n_agg)
            .map(|i| {
                partial
                    .column(base + i * 4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ExecError::Spill("partial val column not Int64".into()))
            })
            .collect::<ExecResult<_>>()?;
        let has_cols: Vec<&BooleanArray> = (0..n_agg)
            .map(|i| {
                partial
                    .column(base + i * 4 + 1)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| ExecError::Spill("partial has column not Boolean".into()))
            })
            .collect::<ExecResult<_>>()?;
        let avs_cols: Vec<&Float64Array> = (0..n_agg)
            .map(|i| {
                partial
                    .column(base + i * 4 + 2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| ExecError::Spill("partial avs column not Float64".into()))
            })
            .collect::<ExecResult<_>>()?;
        let avc_cols: Vec<&Int64Array> = (0..n_agg)
            .map(|i| {
                partial
                    .column(base + i * 4 + 3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| ExecError::Spill("partial avc column not Int64".into()))
            })
            .collect::<ExecResult<_>>()?;

        for row in 0..n {
            let key: SmallVec<[AggKey; 4]> =
                pre_gb.iter().map(|col| col.extract_agg_key(row)).collect();
            let existing = self.groups.entry(key).or_insert_with(|| {
                let n_agg = self.agg_exprs.len();
                AggState {
                    values: vec![0i64; n_agg],
                    has_value: vec![false; n_agg],
                    avg_sums: vec![0.0f64; n_agg],
                    avg_counts: vec![0u64; n_agg],
                }
            });
            for (i, expr) in self.agg_exprs.iter().enumerate() {
                let pval = val_cols[i].value(row);
                let phas = has_cols[i].value(row);
                let pavs = avs_cols[i].value(row);
                let pavc = avc_cols[i].value(row) as u64;
                match expr.function {
                    AggFunction::Count | AggFunction::Sum => {
                        existing.values[i] =
                            existing.values[i].checked_add(pval).ok_or_else(|| {
                                ExecError::InvalidInput(
                                    "aggregate overflow during spill merge".into(),
                                )
                            })?;
                    }
                    AggFunction::Min => {
                        if phas && (!existing.has_value[i] || pval < existing.values[i]) {
                            existing.values[i] = pval;
                        }
                    }
                    AggFunction::Max => {
                        if phas && (!existing.has_value[i] || pval > existing.values[i]) {
                            existing.values[i] = pval;
                        }
                    }
                    AggFunction::Avg => {
                        existing.avg_sums[i] += pavs;
                        existing.avg_counts[i] = existing.avg_counts[i].saturating_add(pavc);
                    }
                }
                existing.has_value[i] = existing.has_value[i] || phas;
            }
        }
        Ok(())
    }

    fn empty_output_schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.group_by
                .iter()
                .map(|c| Field::new(c.as_str(), DataType::Utf8, true))
                .chain(self.agg_exprs.iter().map(|e| {
                    let dtype = if matches!(e.function, AggFunction::Avg) {
                        DataType::Float64
                    } else {
                        DataType::Int64
                    };
                    Field::new(e.output_column.as_str(), dtype, true)
                }))
                .collect::<Vec<_>>(),
        ))
    }

    fn build_output_batch(&self, input_schema: &SchemaRef) -> ExecResult<RecordBatch> {
        let mut sorted: Vec<(&SmallVec<[AggKey; 4]>, &AggState)> = self.groups.iter().collect();
        sorted.sort_by(|(a, _), (b, _)| {
            a.iter()
                .zip(b.iter())
                .map(|(ai, bi)| ai.cmp(bi))
                .find(|&o| o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });

        let n = sorted.len();
        let mut fields: Vec<Field> = Vec::with_capacity(self.group_by.len() + self.agg_exprs.len());
        for col_name in &self.group_by {
            let f = input_schema
                .field_with_name(col_name)
                .map_err(|_| ExecError::ColumnNotFound(col_name.clone()))?;
            fields.push(f.clone());
        }
        for agg in &self.agg_exprs {
            let dtype = if matches!(agg.function, AggFunction::Avg) {
                DataType::Float64
            } else {
                DataType::Int64
            };
            fields.push(Field::new(&agg.output_column, dtype, true));
        }
        let out_schema = Arc::new(Schema::new(fields));

        if n == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let gb_indices: Vec<usize> = self
            .group_by
            .iter()
            .map(|col| {
                input_schema
                    .index_of(col)
                    .map_err(|_| ExecError::ColumnNotFound(col.clone()))
            })
            .collect::<ExecResult<_>>()?;

        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(self.group_by.len() + self.agg_exprs.len());
        for (gb_pos, col_name) in self.group_by.iter().enumerate() {
            let col_idx = gb_indices[gb_pos];
            let dtype = input_schema.field(col_idx).data_type().clone();
            let col: ArrayRef = match dtype {
                DataType::Int32 => Arc::new(Int32Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int32(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Int32 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<Vec<i32>>>()?,
                )),
                DataType::Int64 => Arc::new(Int64Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Int64(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Int64 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<Vec<i64>>>()?,
                )),
                DataType::Float64 => Arc::new(Float64Array::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Float64(bits) => Ok(f64::from_bits(bits)),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Float64 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<Vec<f64>>>()?,
                )),
                DataType::Utf8 => Arc::new(StringArray::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match &key[gb_pos] {
                            AggKey::Utf8(s) => Ok(s.as_str()),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Utf8 group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<Vec<&str>>>()?,
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    sorted
                        .iter()
                        .map(|(key, _)| match key[gb_pos] {
                            AggKey::Bool(v) => Ok(v),
                            _ => Err(ExecError::UnsupportedType(format!(
                                "Bool group key mismatch for {col_name}"
                            ))),
                        })
                        .collect::<ExecResult<Vec<bool>>>()?,
                )),
                other => {
                    return Err(ExecError::UnsupportedType(format!(
                        "unsupported group-by column type for {col_name}: {other}"
                    )));
                }
            };
            columns.push(col);
        }

        for (agg_pos, agg) in self.agg_exprs.iter().enumerate() {
            match agg.function {
                AggFunction::Avg => {
                    let arr: Float64Array = sorted
                        .iter()
                        .map(|(_, state)| state.finalized_avg(agg_pos))
                        .collect();
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                _ => {
                    let arr: Int64Array = sorted
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

impl Drop for ExternalAggregator {
    fn drop(&mut self) {
        self.release_reserved();
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
