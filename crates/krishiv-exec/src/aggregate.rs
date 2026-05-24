use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::join::{AggKey, extract_agg_key};
use crate::{ExecError, ExecResult};

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
pub(crate) struct AggState {
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
                    self.values[i] += 1;
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
                            self.values[i] += v;
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
                        AggFunction::Count | AggFunction::Avg => unreachable!(),
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
            AggFunction::Min | AggFunction::Max => {
                if self.has_value[i] {
                    self.values[i]
                } else {
                    0
                }
            }
            _ => self.values[i],
        }
    }

    pub(crate) fn finalized_avg(&self, i: usize) -> f64 {
        if self.avg_counts[i] == 0 {
            0.0
        } else {
            self.avg_sums[i] / self.avg_counts[i] as f64
        }
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

        let mut groups: HashMap<Vec<AggKey>, AggState> = HashMap::new();

        for row in 0..batch.num_rows() {
            let key: Vec<AggKey> = gb_indices
                .iter()
                .map(|&idx| extract_agg_key(batch, idx, row))
                .collect::<ExecResult<_>>()?;

            let state = groups
                .entry(key)
                .or_insert_with(|| AggState::new(&self.agg_exprs));
            state.update(&self.agg_exprs, batch, row)?;
        }

        let mut sorted_entries: Vec<(Vec<AggKey>, AggState)> = groups.into_iter().collect();
        sorted_entries.sort_by(|(a, _), (b, _)| {
            a.iter()
                .zip(b.iter())
                .map(|(ai, bi)| ai.cmp(bi))
                .find(|&o| o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len()))
        });

        let mut fields: Vec<Field> = Vec::new();
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
            fields.push(Field::new(&agg.output_column, dtype, false));
        }
        let out_schema = Arc::new(Schema::new(fields));

        let num_rows = sorted_entries.len();

        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

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
