#![forbid(unsafe_code)]
//! Spark-reference higher-order (lambda) array functions (Phase 60).
//!
//! DataFusion 54 ships native lambda support (`Expr::Lambda`,
//! `Expr::HigherOrderFunction`) and three higher-order array functions in
//! `datafusion-functions-nested`: `array_transform`, `array_filter`, and
//! `array_any_match`. Spark's surface names for these are `transform`,
//! `filter`, and `exists`; we register those names as **aliases onto the exact
//! DataFusion implementations** (via `HigherOrderUDF::with_aliases`, which
//! delegates every trait method) so semantics are byte-for-byte identical —
//! honouring the phase's exact-or-absent rule for the alias layer.
//!
//! Note: `exists(array, lambda)` is registered as an alias but is **not
//! reachable via SQL text** — sqlparser parses a leading `EXISTS(` as a
//! subquery predicate, not a function call. The reachable spelling of the same
//! (byte-identical) implementation is `any_match` / `array_any_match`. This is
//! a documented dialect difference on the Krishiv-vs-Spark honesty page.
//!
//! Spark's `forall` (all-elements-match) has no DataFusion equivalent, so it is
//! implemented here as [`ArrayAllMatch`], mirroring `array_any_match`'s
//! three-valued-logic and slice/null handling exactly, but with all-match range
//! semantics (empty array ⇒ true; a definite `false` dominates; otherwise a
//! `NULL` predicate result poisons the row to `NULL`).
//!
//! The remaining Spark HOFs — `aggregate`/`reduce` (two-lambda fold),
//! `zip_with` (binary lambda over two arrays), and the map lambdas
//! (`map_filter`, `transform_keys`, `transform_values`) — require the
//! multi-step lambda-parameter protocol / map-lambda machinery and are tracked
//! as `Planned` in the feature matrix rather than shipped approximately.

use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::compute::take_arrays;
use arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion::common::utils::{
    adjust_offsets_for_slice, list_values, list_values_row_number, take_function_args,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion_functions_nested::array_any_match::ArrayAnyMatch;
use datafusion_functions_nested::array_filter::ArrayFilter;
use datafusion_functions_nested::array_transform::ArrayTransform;

type DFResult<T> = Result<T, DataFusionError>;

/// Register the Spark-parity higher-order array functions on `ctx`.
///
/// - `transform` → `array_transform` (alias, exact)
/// - `filter`    → `array_filter`    (alias, exact)
/// - `exists`    → `array_any_match` (alias, exact)
/// - `forall`    → [`ArrayAllMatch`] (new, exact all-match semantics)
pub fn register_higher_order_spark_functions(ctx: &SessionContext) -> DFResult<()> {
    // Aliases delegate through DataFusion's own `AliasedHigherOrderUDFImpl`,
    // which forwards every trait method — so the Spark name is the exact same
    // implementation, not a re-derivation. Re-registering the base name is
    // idempotent (it replaces the default registration with the same impl plus
    // the extra alias), so `array_transform` / `list_transform` keep working.
    ctx.register_higher_order_function(Arc::new(
        HigherOrderUDF::new_from_impl(ArrayTransform::new()).with_aliases(["transform"]),
    ));
    ctx.register_higher_order_function(Arc::new(
        HigherOrderUDF::new_from_impl(ArrayFilter::new()).with_aliases(["filter"]),
    ));
    ctx.register_higher_order_function(Arc::new(
        HigherOrderUDF::new_from_impl(ArrayAnyMatch::new()).with_aliases(["exists"]),
    ));
    ctx.register_higher_order_function(Arc::new(HigherOrderUDF::new_from_impl(
        ArrayAllMatch::new(),
    )));
    Ok(())
}

/// Spark `forall(array, predicate)` — returns whether *every* element of the
/// array matches the predicate.
///
/// Three-valued logic (matches Spark / mirrors `array_any_match`):
/// - empty array ⇒ `true`
/// - any element for which the predicate is a definite `false` ⇒ `false`
/// - otherwise, any element for which the predicate is `NULL` ⇒ `NULL`
/// - all elements `true` ⇒ `true`
///
/// The predicate is never evaluated on elements behind a `NULL` list row or
/// before a slice offset (unreachable values), exactly as `array_any_match`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAllMatch {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for ArrayAllMatch {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAllMatch {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("forall"), String::from("array_forall")],
        }
    }
}

/// `Some(false)` if any element in `[start, end)` is a definite false,
/// `None` if none are false but some are null, `Some(true)` otherwise
/// (all true, or an empty range).
fn all_match_for_range(predicate: &BooleanArray, start: usize, end: usize) -> Option<bool> {
    let any_false = (start..end).any(|j| predicate.is_valid(j) && !predicate.value(j));
    if any_false {
        return Some(false);
    }
    let any_null = (start..end).any(|j| predicate.is_null(j));
    if any_null { None } else { Some(true) }
}

impl HigherOrderUDFImpl for ArrayAllMatch {
    fn name(&self) -> &str {
        "array_all_match"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [list] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "{} requires 1 value argument, got {}",
                self.name(),
                arg_types.len()
            )));
        };
        let coerced = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{} expected a list as first argument, got {other}",
                    self.name()
                )));
            }
        };
        Ok(vec![coerced])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> DFResult<LambdaParametersProgress> {
        let [list, _] = take_function_args(self.name(), fields)?;
        let ValueOrLambda::Value(list) = list else {
            return Err(DataFusionError::Plan(format!(
                "{} expects a value as first argument",
                self.name()
            )));
        };
        let field = match list.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field,
            other => {
                return Err(DataFusionError::Plan(format!("expected list, got {other}")));
            }
        };
        Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
            field,
        )]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> DFResult<Arc<Field>> {
        let [ValueOrLambda::Value(list), _] = take_function_args(self.name(), args.arg_fields)?
        else {
            return Err(DataFusionError::Plan(format!(
                "{} expects a value as first argument",
                self.name()
            )));
        };
        Ok(Arc::new(Field::new("", DataType::Boolean, list.is_nullable())))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> DFResult<ColumnarValue> {
        let [ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), &args.args)?
        else {
            return Err(DataFusionError::Execution(format!(
                "{} expects a value followed by a lambda",
                self.name()
            )));
        };

        let list_array = list.to_array(args.number_rows)?;

        // Fully-null input: every row is NULL (also the only correct path for a
        // fully-null FixedSizeList, which the range logic below can't address).
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                list_array.len(),
            )));
        }

        let list_values = list_values(&list_array)?;
        let values_param = || Ok(Arc::clone(&list_values));

        let predicate_results = lambda
            .evaluate(&[&values_param], |arrays| {
                let indices = list_values_row_number(&list_array)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(list_values.len())?;

        let predicate_bool =
            predicate_results
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{} predicate must return a boolean array",
                        self.name()
                    ))
                })?;

        let mut values = BooleanBuilder::with_capacity(list_array.len());
        macro_rules! process_list {
            ($list_typed:expr) => {{
                let offsets = adjust_offsets_for_slice($list_typed);
                for i in 0..$list_typed.len() {
                    let start = offsets[i].as_usize();
                    let end = offsets[i + 1].as_usize();
                    values.append_option(all_match_for_range(predicate_bool, start, end));
                }
            }};
        }
        match list_array.data_type() {
            DataType::List(_) => process_list!(list_array.as_list::<i32>()),
            DataType::LargeList(_) => process_list!(list_array.as_list::<i64>()),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "expected list, got {other}"
                )));
            }
        }

        let (boolean_buffer, predicate_nulls) = values.finish().into_parts();
        // A row is NULL if the input list row was NULL or the predicate poisoned it.
        let nulls = NullBuffer::union(list_array.nulls(), predicate_nulls.as_ref());
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            boolean_buffer,
            nulls,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Int64Array};

    /// transform/filter/exists/forall are all reachable and correct through the
    /// real SQL front door (proves registration + Spark aliasing + `forall`).
    async fn run(sql: &str) -> Vec<arrow::array::RecordBatch> {
        crate::SqlEngine::new()
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    #[tokio::test]
    async fn spark_transform_alias_doubles_elements() {
        let b = run("SELECT transform([1, 2, 3], x -> x * 2) AS r").await;
        let list = b[0].column(0).as_any().downcast_ref::<arrow::array::ListArray>();
        let list = list.expect("list");
        let vals = list.value(0);
        let vals = vals.as_any().downcast_ref::<Int64Array>().expect("i64");
        assert_eq!(vals.values(), &[2, 4, 6]);
    }

    #[tokio::test]
    async fn spark_filter_alias_keeps_matching() {
        let b = run("SELECT filter([1, 2, 3, 4], x -> x % 2 = 0) AS r").await;
        let list = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .expect("list");
        let vals = list.value(0);
        let vals = vals.as_any().downcast_ref::<Int64Array>().expect("i64");
        assert_eq!(vals.values(), &[2, 4]);
    }

    #[tokio::test]
    async fn spark_exists_and_forall() {
        // `exists(arr, lambda)` is unreachable via SQL text — sqlparser treats
        // `EXISTS(` as a subquery predicate — so the reachable spelling of the
        // same (aliased) impl is `any_match`. `forall` has no keyword clash.
        // The empty array is built by filtering everything out (an untyped `[]`
        // literal has no element type to check the predicate against).
        let b = run(
            "SELECT any_match([1, 2, 3], x -> x > 2) AS any_gt2, \
                    forall([2, 4, 6], x -> x % 2 = 0) AS all_even, \
                    forall([2, 3, 6], x -> x % 2 = 0) AS not_all_even, \
                    forall(filter([1], x -> x > 100), x -> x > 0) AS empty_all",
        )
        .await;
        let row = &b[0];
        let col = |i: usize| {
            row.column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool")
                .value(0)
        };
        assert!(col(0), "exists any > 2");
        assert!(col(1), "forall even");
        assert!(!col(2), "not all even");
        assert!(col(3), "forall over empty array is true");
    }

    #[tokio::test]
    async fn forall_null_semantics() {
        // A NULL predicate result with no definite false poisons the row to NULL;
        // a definite false still dominates a NULL.
        let b = run(
            "SELECT forall([2, 4], x -> CASE WHEN x = 4 THEN NULL ELSE x % 2 = 0 END) AS poisoned, \
                    forall([3, 4], x -> CASE WHEN x = 4 THEN NULL ELSE x % 2 = 0 END) AS false_wins",
        )
        .await;
        let row = &b[0];
        let poisoned = row.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let false_wins = row.column(1).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(poisoned.is_null(0), "NULL result with no false ⇒ NULL");
        assert!(!false_wins.is_null(0) && !false_wins.value(0), "false dominates NULL");
    }

    #[tokio::test]
    async fn all_match_range_helper_direct() {
        use super::all_match_for_range;
        let p = BooleanArray::from(vec![Some(true), Some(true), Some(false), None]);
        assert_eq!(all_match_for_range(&p, 0, 2), Some(true));
        assert_eq!(all_match_for_range(&p, 0, 3), Some(false)); // definite false
        assert_eq!(all_match_for_range(&p, 0, 4), Some(false)); // false dominates null
        assert_eq!(all_match_for_range(&p, 3, 4), None); // only null
        assert_eq!(all_match_for_range(&p, 1, 1), Some(true)); // empty range
    }
}
