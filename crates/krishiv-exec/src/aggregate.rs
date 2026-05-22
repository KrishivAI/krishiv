use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};
use crate::join::{compare_key_parts, format_key_value};

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
#[derive(Debug)]
pub(crate) struct AggState {
    /// One running value per `AggExpr`: count, sum, min, or max.
    pub(crate) values: Vec<i64>,
    /// Tracks whether Min/Max has received at least one value.
    pub(crate) has_value: Vec<bool>,
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
            })
            .collect();
        let has_value = agg_exprs.iter().map(|_| false).collect();
        Self { values, has_value }
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
                        AggFunction::Count => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the finalized value for position `i`. For Min/Max with no data,
    /// returns 0 rather than the sentinel.
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

        // Sort entries for deterministic output using numeric-aware key comparison.
        let mut sorted_entries: Vec<(Vec<String>, AggState)> = groups.into_iter().collect();
        sorted_entries.sort_by(|(a, _), (b, _)| {
            a.iter()
                .zip(b.iter())
                .map(|(ai, bi)| compare_key_parts(ai, bi))
                .find(|&o| o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len()))
        });

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
                    let values: Vec<i32> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            key[gb_pos].parse::<i32>().map_err(|e| {
                                ExecError::UnsupportedType(format!(
                                    "failed to rebuild Int32 group key '{}': {e}",
                                    key[gb_pos]
                                ))
                            })
                        })
                        .collect::<ExecResult<_>>()?;
                    let arr = Int32Array::from(values);
                    columns.push(Arc::new(arr) as ArrayRef);
                }
                DataType::Int64 => {
                    let values: Vec<i64> = sorted_entries
                        .iter()
                        .map(|(key, _)| {
                            key[gb_pos].parse::<i64>().map_err(|e| {
                                ExecError::UnsupportedType(format!(
                                    "failed to rebuild Int64 group key '{}': {e}",
                                    key[gb_pos]
                                ))
                            })
                        })
                        .collect::<ExecResult<_>>()?;
                    let arr = Int64Array::from(values);
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
        for (agg_pos, agg) in self.agg_exprs.iter().enumerate() {
            let arr: Int64Array = sorted_entries
                .iter()
                .map(|(_, state)| state.finalized_value(agg_pos, agg))
                .collect();
            columns.push(Arc::new(arr) as ArrayRef);
        }

        Ok(RecordBatch::try_new(out_schema, columns)?)
    }
}
