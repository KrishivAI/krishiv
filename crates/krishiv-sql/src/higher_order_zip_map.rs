#![forbid(unsafe_code)]
//! Spark's two-parameter lambda functions: `zip_with` over two arrays and the
//! map lambdas `map_filter`, `transform_keys` and `transform_values`.
//!
//! All four evaluate their lambda the way [`crate::higher_order_functions`]'
//! `aggregate` does: position by position across the whole batch, so the
//! lambda always sees row-aligned columns and may reference outer columns
//! without any re-alignment. Position `k` of a row that has fewer than `k + 1`
//! elements is evaluated on NULLs and its result discarded.
//!
//! Spark semantics kept exactly:
//! - `zip_with` pads the shorter array with NULLs and returns NULL when either
//!   array is NULL.
//! - `map_filter` keeps an entry only when the predicate is a definite `true`.
//! - `transform_keys` fails on a NULL key and on a duplicate key within one
//!   map (Spark's default `EXCEPTION` dedup policy).
//! - A NULL map yields NULL.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Builder, ListArray, MapArray, StructArray, UInt64Array,
    new_empty_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{interleave, take};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::row::{RowConverter, SortField};
use datafusion::common::utils::take_function_args;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaArgument, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion::prelude::SessionContext;

type DFResult<T> = Result<T, DataFusionError>;

/// Register `zip_with`, `map_filter`, `transform_keys` and `transform_values`.
pub fn register_zip_and_map_lambda_functions(ctx: &SessionContext) -> DFResult<()> {
    ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(ZipWith::new())));
    for kind in [
        MapLambdaKind::Filter,
        MapLambdaKind::TransformKeys,
        MapLambdaKind::TransformValues,
    ] {
        ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(
            MapLambda::new(kind),
        )));
    }
    Ok(())
}

fn plan_err<T>(message: String) -> DFResult<T> {
    Err(DataFusionError::Plan(message))
}

fn exec_err<T>(message: String) -> DFResult<T> {
    Err(DataFusionError::Execution(message))
}

/// Offsets (as `i64`, into `values`) and the flat child of a list array.
fn list_parts(array: &ArrayRef) -> DFResult<(Vec<i64>, ArrayRef)> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            Ok((
                list.offsets().iter().map(|o| i64::from(*o)).collect(),
                Arc::clone(list.values()),
            ))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            Ok((
                list.offsets().iter().copied().collect(),
                Arc::clone(list.values()),
            ))
        }
        other => exec_err(format!("expected a list, got {other}")),
    }
}

/// Per-row element counts; a NULL row counts as empty whatever its offsets say.
fn row_lengths(offsets: &[i64], nulls: Option<&NullBuffer>) -> Vec<i64> {
    offsets
        .windows(2)
        .enumerate()
        .map(|(row, window)| match window {
            [start, end] if nulls.is_none_or(|n| n.is_valid(row)) => *end - *start,
            _ => 0,
        })
        .collect()
}

/// The `k`-th element of every row, NULL where the row is shorter.
fn kth_elements(
    values: &dyn Array,
    offsets: &[i64],
    lengths: &[i64],
    k: i64,
) -> DFResult<ArrayRef> {
    let mut indices = Int64Builder::with_capacity(lengths.len());
    for (offset, len) in offsets.iter().zip(lengths) {
        if k < *len {
            indices.append_value(*offset + k);
        } else {
            indices.append_null();
        }
    }
    Ok(take(values, &indices.finish(), None)?)
}

/// Evaluate a two-parameter lambda on row-aligned argument columns.
fn evaluate_pair(
    lambda: &LambdaArgument,
    first: &ArrayRef,
    second: &ArrayRef,
    num_rows: usize,
) -> DFResult<ArrayRef> {
    let first_fn: &dyn Fn() -> DFResult<ArrayRef> = &|| Ok(Arc::clone(first));
    let second_fn: &dyn Fn() -> DFResult<ArrayRef> = &|| Ok(Arc::clone(second));
    lambda
        .evaluate(&[first_fn, second_fn], |arrays| Ok(arrays.to_vec()))?
        .into_array(num_rows)
}

/// Lay per-position result columns out row by row: row `r` contributes
/// `per_position[k][r]` for every `k < lengths[r]`.
fn gather_row_major(
    per_position: &[ArrayRef],
    lengths: &[i64],
    element_type: &DataType,
) -> DFResult<ArrayRef> {
    if per_position.is_empty() {
        return Ok(new_empty_array(element_type));
    }
    let mut indices = Vec::new();
    for (row, len) in lengths.iter().enumerate() {
        for k in 0..usize::try_from(*len).unwrap_or(0) {
            indices.push((k, row));
        }
    }
    let arrays: Vec<&dyn Array> = per_position.iter().map(AsRef::as_ref).collect();
    Ok(interleave(&arrays, &indices)?)
}

fn offsets_from_lengths(lengths: &[i64]) -> DFResult<OffsetBuffer<i32>> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    let mut total = 0_i64;
    offsets.push(0_i32);
    for len in lengths {
        total += *len;
        offsets.push(i32::try_from(total).map_err(|_| {
            DataFusionError::Execution(String::from("lambda result exceeds the 2^31 element limit"))
        })?);
    }
    Ok(OffsetBuffer::new(offsets.into()))
}

fn element_field(list: &FieldRef, function: &str) -> DFResult<FieldRef> {
    match list.data_type() {
        DataType::List(field) | DataType::LargeList(field) => {
            // The shorter array is padded with NULLs, so the parameter is
            // nullable whatever the element field says.
            Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
        }
        other => plan_err(format!("{function} expected a list, got {other}")),
    }
}

/// Spark `zip_with(left, right, (x, y) -> expr)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ZipWith {
    signature: HigherOrderSignature,
}

impl Default for ZipWith {
    fn default() -> Self {
        Self::new()
    }
}

impl ZipWith {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Value(()),
                    ValueOrLambda::Lambda(()),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl HigherOrderUDFImpl for ZipWith {
    fn name(&self) -> &str {
        "zip_with"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [left, right] = take_function_args(self.name(), arg_types)?;
        let as_list = |data_type: &DataType| match data_type {
            DataType::List(_) | DataType::LargeList(_) => Ok(data_type.clone()),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                Ok(DataType::List(Arc::clone(field)))
            }
            DataType::LargeListView(field) => Ok(DataType::LargeList(Arc::clone(field))),
            other => plan_err(format!("{} expected two arrays, got {other}", self.name())),
        };
        Ok(vec![as_list(left)?, as_list(right)?])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [left, right, _lambda] = take_function_args(self.name(), fields)?;
        let (ValueOrLambda::Value(left), ValueOrLambda::Value(right)) = (left, right) else {
            return plan_err(format!(
                "{} expects two arrays before the lambda",
                self.name()
            ));
        };
        Ok(LambdaParametersProgress::Complete(vec![vec![
            element_field(left, self.name())?,
            element_field(right, self.name())?,
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        let [_left, _right, lambda] = take_function_args(self.name(), args.arg_fields)?;
        let ValueOrLambda::Lambda(result) = lambda else {
            return plan_err(format!(
                "{} expects a lambda as its third argument",
                self.name()
            ));
        };
        let item = Field::new("item", result.data_type().clone(), true);
        Ok(Arc::new(Field::new(
            "",
            DataType::List(Arc::new(item)),
            true,
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let [left, right, lambda] = take_function_args(self.name(), &args.args)?;
        let (
            ValueOrLambda::Value(left),
            ValueOrLambda::Value(right),
            ValueOrLambda::Lambda(lambda),
        ) = (left, right, lambda)
        else {
            return exec_err(format!("{} expects (array, array, lambda)", self.name()));
        };
        let DataType::List(item) = args.return_field.data_type() else {
            return exec_err(format!("{} must return a list", self.name()));
        };

        let left = left.to_array(num_rows)?;
        let right = right.to_array(num_rows)?;
        let (left_offsets, left_values) = list_parts(&left)?;
        let (right_offsets, right_values) = list_parts(&right)?;
        let left_lengths = row_lengths(&left_offsets, left.nulls());
        let right_lengths = row_lengths(&right_offsets, right.nulls());
        // NULL when either side is NULL, so such a row contributes nothing.
        let nulls = NullBuffer::union(left.nulls(), right.nulls());
        let lengths: Vec<i64> = left_lengths
            .iter()
            .zip(&right_lengths)
            .enumerate()
            .map(|(row, (l, r))| {
                if nulls.as_ref().is_none_or(|n| n.is_valid(row)) {
                    (*l).max(*r)
                } else {
                    0
                }
            })
            .collect();
        let max_len = lengths.iter().copied().max().unwrap_or(0);

        let mut per_position = Vec::new();
        for k in 0..max_len {
            let x = kth_elements(left_values.as_ref(), &left_offsets, &left_lengths, k)?;
            let y = kth_elements(right_values.as_ref(), &right_offsets, &right_lengths, k)?;
            per_position.push(evaluate_pair(lambda, &x, &y, num_rows)?);
        }
        let values = gather_row_major(&per_position, &lengths, item.data_type())?;
        let list = ListArray::try_new(
            Arc::clone(item),
            offsets_from_lengths(&lengths)?,
            values,
            nulls,
        )?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MapLambdaKind {
    Filter,
    TransformKeys,
    TransformValues,
}

/// Spark `map_filter`, `transform_keys` and `transform_values`: a
/// `(key, value) -> expr` lambda over each entry of a map.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapLambda {
    kind: MapLambdaKind,
    signature: HigherOrderSignature,
}

impl MapLambda {
    fn new(kind: MapLambdaKind) -> Self {
        Self {
            kind,
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
        }
    }
}

/// The entries field of a map type and its `(key, value)` fields.
fn map_fields(data_type: &DataType, function: &str) -> DFResult<(FieldRef, FieldRef, FieldRef)> {
    let DataType::Map(entries, _) = data_type else {
        return plan_err(format!("{function} expected a map, got {data_type}"));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return plan_err(format!("{function}: malformed map type {data_type}"));
    };
    match fields.iter().collect::<Vec<_>>().as_slice() {
        [key, value] => Ok((Arc::clone(entries), Arc::clone(key), Arc::clone(value))),
        _ => plan_err(format!("{function}: malformed map type {data_type}")),
    }
}

impl HigherOrderUDFImpl for MapLambda {
    fn name(&self) -> &str {
        match self.kind {
            MapLambdaKind::Filter => "map_filter",
            MapLambdaKind::TransformKeys => "transform_keys",
            MapLambdaKind::TransformValues => "transform_values",
        }
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [map] = take_function_args(self.name(), arg_types)?;
        map_fields(map, self.name())?;
        Ok(vec![map.clone()])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [map, _lambda] = take_function_args(self.name(), fields)?;
        let ValueOrLambda::Value(map) = map else {
            return plan_err(format!("{} expects a map before the lambda", self.name()));
        };
        let (_entries, key, value) = map_fields(map.data_type(), self.name())?;
        // Positions past the end of a shorter map are evaluated on NULLs.
        Ok(LambdaParametersProgress::Complete(vec![vec![
            Arc::new(key.as_ref().clone().with_nullable(true)),
            Arc::new(value.as_ref().clone().with_nullable(true)),
        ]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        let [map, lambda] = take_function_args(self.name(), args.arg_fields)?;
        let (ValueOrLambda::Value(map), ValueOrLambda::Lambda(result)) = (map, lambda) else {
            return plan_err(format!("{} expects (map, lambda)", self.name()));
        };
        let DataType::Map(_, sorted) = map.data_type() else {
            return plan_err(format!("{} expected a map", self.name()));
        };
        let (entries, key, value) = map_fields(map.data_type(), self.name())?;
        let data_type = match self.kind {
            MapLambdaKind::Filter => {
                if result.data_type() != &DataType::Boolean {
                    return plan_err(format!(
                        "{} expects a boolean lambda, got {}",
                        self.name(),
                        result.data_type()
                    ));
                }
                map.data_type().clone()
            }
            MapLambdaKind::TransformKeys => {
                let key = Field::new(key.name(), result.data_type().clone(), false);
                let fields = Fields::from(vec![Arc::new(key), value]);
                let entries = Field::new(entries.name(), DataType::Struct(fields), false);
                // New keys are in no particular order.
                DataType::Map(Arc::new(entries), false)
            }
            MapLambdaKind::TransformValues => {
                let value = Field::new(value.name(), result.data_type().clone(), true);
                let fields = Fields::from(vec![key, Arc::new(value)]);
                let entries = Field::new(entries.name(), DataType::Struct(fields), false);
                DataType::Map(Arc::new(entries), *sorted)
            }
        };
        Ok(Arc::new(Field::new("", data_type, true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;
        let [map, lambda] = take_function_args(self.name(), &args.args)?;
        let (ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)) = (map, lambda) else {
            return exec_err(format!("{} expects (map, lambda)", self.name()));
        };
        let return_type = args.return_field.data_type();
        let DataType::Map(_, sorted) = return_type else {
            return exec_err(format!("{} must return a map", self.name()));
        };
        let (out_entries, out_key, out_value) = map_fields(return_type, self.name())?;

        let map = map.to_array(num_rows)?;
        let DataType::Map(_, _) = map.data_type() else {
            return exec_err(format!(
                "{} expected a map, got {}",
                self.name(),
                map.data_type()
            ));
        };
        let map_array = map.as_map();
        let offsets: Vec<i64> = map_array.offsets().iter().map(|o| i64::from(*o)).collect();
        let lengths = row_lengths(&offsets, map_array.nulls());
        let max_len = lengths.iter().copied().max().unwrap_or(0);
        let keys = map_array.keys();
        let values = map_array.values();

        let mut per_position = Vec::new();
        for k in 0..max_len {
            let key = kth_elements(keys.as_ref(), &offsets, &lengths, k)?;
            let value = kth_elements(values.as_ref(), &offsets, &lengths, k)?;
            per_position.push(evaluate_pair(lambda, &key, &value, num_rows)?);
        }

        // Entry indexes of the input, row by row, for the columns kept as-is.
        let all_entries = || {
            let mut indices = Vec::new();
            for (offset, len) in offsets.iter().zip(&lengths) {
                for k in 0..*len {
                    indices.push(u64::try_from(*offset + k).unwrap_or(0));
                }
            }
            UInt64Array::from(indices)
        };

        let (new_keys, new_values, out_lengths) = match self.kind {
            MapLambdaKind::Filter => {
                let mut kept = Vec::new();
                let mut out_lengths = Vec::with_capacity(lengths.len());
                for (row, (offset, len)) in offsets.iter().zip(&lengths).enumerate() {
                    let mut count = 0_i64;
                    for k in 0..usize::try_from(*len).unwrap_or(0) {
                        let keep = per_position.get(k).is_some_and(|predicate| {
                            let predicate = predicate.as_boolean();
                            predicate.is_valid(row) && predicate.value(row)
                        });
                        if keep {
                            let index = *offset + i64::try_from(k).unwrap_or(0);
                            kept.push(u64::try_from(index).unwrap_or(0));
                            count += 1;
                        }
                    }
                    out_lengths.push(count);
                }
                let kept = UInt64Array::from(kept);
                (
                    take(keys.as_ref(), &kept, None)?,
                    take(values.as_ref(), &kept, None)?,
                    out_lengths,
                )
            }
            MapLambdaKind::TransformValues => (
                take(keys.as_ref(), &all_entries(), None)?,
                gather_row_major(&per_position, &lengths, out_value.data_type())?,
                lengths.clone(),
            ),
            MapLambdaKind::TransformKeys => {
                let new_keys = gather_row_major(&per_position, &lengths, out_key.data_type())?;
                check_map_keys(new_keys.as_ref(), &lengths, self.name())?;
                (
                    new_keys,
                    take(values.as_ref(), &all_entries(), None)?,
                    lengths.clone(),
                )
            }
        };

        let DataType::Struct(entry_fields) = out_entries.data_type() else {
            return exec_err(format!("{}: malformed map type", self.name()));
        };
        let entries = StructArray::try_new(entry_fields.clone(), vec![new_keys, new_values], None)?;
        let result = MapArray::try_new(
            out_entries,
            offsets_from_lengths(&out_lengths)?,
            entries,
            map_array.nulls().cloned(),
            *sorted,
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Reject a NULL key or a key repeated within one map.
fn check_map_keys(keys: &dyn Array, lengths: &[i64], function: &str) -> DFResult<()> {
    if keys.null_count() > 0 {
        return exec_err(format!("{function}: cannot use NULL as a map key"));
    }
    let converter = RowConverter::new(vec![SortField::new(keys.data_type().clone())])?;
    let rows = converter.convert_columns(&[arrow::array::make_array(keys.to_data())])?;
    let mut start = 0_usize;
    for len in lengths {
        let len = usize::try_from(*len).unwrap_or(0);
        let mut seen = HashSet::with_capacity(len);
        for index in start..start + len {
            if !seen.insert(rows.row(index)) {
                return exec_err(format!(
                    "{function}: duplicate map key produced by the lambda"
                ));
            }
        }
        start += len;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;

    async fn run(sql: &str) -> String {
        let batches = crate::SqlEngine::new()
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        pretty_format_batches(&batches).expect("format").to_string()
    }

    async fn error(sql: &str) -> String {
        let engine = crate::SqlEngine::new();
        match engine.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("must fail").to_string(),
        }
    }

    /// One result cell, from a single-row single-column query.
    async fn cell(sql: &str) -> String {
        let text = run(sql).await;
        let row = text.lines().nth(3).expect("a data row");
        row.trim_matches('|').trim().to_string()
    }

    #[tokio::test]
    async fn zip_with_combines_position_by_position() {
        assert_eq!(
            cell("SELECT zip_with([1, 2, 3], [10, 20, 30], (x, y) -> x + y) AS z").await,
            "[11, 22, 33]"
        );
        // The shorter array is padded with NULLs, not truncated.
        assert_eq!(
            cell("SELECT zip_with([1, 2, 3], [10], (x, y) -> coalesce(y, 0) + x) AS z").await,
            "[11, 2, 3]"
        );
        assert_eq!(
            cell("SELECT zip_with([1], [10, 20], (x, y) -> x + y) AS z").await,
            "[11, ]"
        );
        // The result type is the lambda's, not the inputs'.
        assert_eq!(
            cell(
                "SELECT zip_with(['a', 'b'], [1, 2], (s, n) -> concat(s, cast(n AS VARCHAR))) AS z"
            )
            .await,
            "[a1, b2]"
        );
    }

    #[tokio::test]
    async fn zip_with_handles_rows_of_different_lengths_and_nulls() {
        let text = run(
            "SELECT id, zip_with(a, b, (x, y) -> x * y) AS z FROM (VALUES \
               (1, [1, 2], [3, 4]), \
               (2, [5], [6, 7, 8]), \
               (3, NULL, [1]), \
               (4, arrow_cast([], 'List(Int64)'), arrow_cast([], 'List(Int64)'))) AS t(id, a, b) \
             ORDER BY id",
        )
        .await;
        let cells: Vec<&str> = text
            .lines()
            .skip(3)
            .filter(|line| line.starts_with('|'))
            .map(|line| line.trim_matches('|').split('|').nth(1).expect("z").trim())
            .collect();
        assert_eq!(cells, vec!["[3, 8]", "[30, , ]", "", "[]"], "{text}");
    }

    #[tokio::test]
    async fn zip_with_lambda_can_use_an_outer_column() {
        let text = run(
            "SELECT zip_with(a, b, (x, y) -> (x + y) * k) AS z FROM (VALUES \
               ([1, 2], [1, 1], 10), ([3], [4], 100)) AS t(a, b, k) ORDER BY k",
        )
        .await;
        assert!(
            text.contains("[20, 30]") && text.contains("[700]"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn map_filter_keeps_only_definite_matches() {
        assert_eq!(
            cell("SELECT map_filter(map(['a', 'b', 'c'], [1, 2, 3]), (k, v) -> v > 1) AS m").await,
            "{b: 2, c: 3}"
        );
        assert_eq!(
            cell("SELECT map_filter(map(['a', 'b'], [1, 2]), (k, v) -> k = 'z') AS m").await,
            "{}"
        );
        // A NULL predicate result drops the entry.
        assert_eq!(
            cell("SELECT map_filter(map(['a', 'b'], [1, NULL]), (k, v) -> v > 0) AS m").await,
            "{a: 1}"
        );
        assert!(
            error("SELECT map_filter(map(['a'], [1]), (k, v) -> v + 1)")
                .await
                .contains("boolean")
        );
    }

    #[tokio::test]
    async fn transform_values_and_keys() {
        assert_eq!(
            cell("SELECT transform_values(map(['a', 'b'], [1, 2]), (k, v) -> v * 10) AS m").await,
            "{a: 10, b: 20}"
        );
        // The value type follows the lambda.
        assert_eq!(
            cell("SELECT transform_values(map(['a', 'b'], [1, 2]), (k, v) -> concat(k, '!')) AS m")
                .await,
            "{a: a!, b: b!}"
        );
        assert_eq!(
            cell("SELECT transform_keys(map(['a', 'b'], [1, 2]), (k, v) -> upper(k)) AS m").await,
            "{A: 1, B: 2}"
        );
        assert_eq!(
            cell("SELECT transform_keys(map(['a', 'b'], [1, 2]), (k, v) -> v + 100) AS m").await,
            "{101: 1, 102: 2}"
        );
    }

    #[tokio::test]
    async fn transform_keys_refuses_duplicate_and_null_keys() {
        let duplicate =
            error("SELECT transform_keys(map(['a', 'b'], [1, 2]), (k, v) -> 'same')").await;
        assert!(duplicate.contains("duplicate map key"), "{duplicate}");
        let null = error(
            "SELECT transform_keys(map(['a', 'b'], [1, 2]), (k, v) -> CASE WHEN v = 1 THEN NULL ELSE k END)",
        )
        .await;
        assert!(null.contains("NULL as a map key"), "{null}");
    }

    #[tokio::test]
    async fn map_lambdas_work_per_row_with_maps_of_different_sizes() {
        let text = run("SELECT id, map_filter(m, (k, v) -> v >= lo) AS kept, \
                    transform_values(m, (k, v) -> v + lo) AS shifted \
             FROM (VALUES \
               (1, map(['a', 'b', 'c'], [1, 2, 3]), 2), \
               (2, map(['x'], [9]), 100)) AS t(id, m, lo) ORDER BY id")
        .await;
        assert!(text.contains("{b: 2, c: 3}"), "{text}");
        assert!(text.contains("{a: 3, b: 4, c: 5}"), "{text}");
        assert!(text.contains("{x: 109}"), "{text}");
        // Row 2 keeps nothing: 9 < 100.
        let second = text
            .lines()
            .find(|line| line.trim_start_matches('|').trim_start().starts_with('2'));
        assert!(second.is_some_and(|line| line.contains("{}")), "{text}");
    }
}
