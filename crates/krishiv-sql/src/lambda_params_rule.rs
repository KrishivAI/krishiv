#![forbid(unsafe_code)]
//! Works around a DataFusion 54 defect in lambdas with more than one parameter.
//!
//! DataFusion renumbers the variables inside a lambda body to the positions of
//! the ones the body *uses*, but then evaluates the body against a batch that
//! holds *every* parameter. A lambda that uses a later parameter and not an
//! earlier one — `(k, v) -> v > 1`, `(acc, x) -> x` — therefore fails at run
//! time with "Field of physical LambdaVariable … doesn't match batch field".
//! (DataFusion 55 tracks used parameters by name and does not have this.)
//!
//! [`BindAllLambdaParams`] makes the two agree: it wraps the body of every
//! multi-parameter lambda in [`LAMBDA_BODY_UDF`], a function that returns its
//! first argument and takes each parameter as a further argument. Every
//! parameter is then "used", so the renumbering is the identity. The wrapper
//! adds no work — it hands its first argument straight back.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::datatype::FieldExt;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

/// Name of the pass-through function the rule wraps lambda bodies in.
pub const LAMBDA_BODY_UDF: &str = "krishiv_lambda_body";

/// Register the pass-through function and the rule that inserts it.
pub fn register_lambda_params_workaround(ctx: &SessionContext) {
    let udf = Arc::new(ScalarUDF::new_from_impl(LambdaBody::new()));
    ctx.register_udf(udf.as_ref().clone());
    ctx.add_analyzer_rule(Arc::new(BindAllLambdaParams { udf }));
}

/// `krishiv_lambda_body(body, params…)` evaluates to `body`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LambdaBody {
    signature: Signature,
}

impl LambdaBody {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LambdaBody {
    fn name(&self) -> &str {
        LAMBDA_BODY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "{LAMBDA_BODY_UDF} needs the lambda body as its first argument"
            ))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        args.arg_fields.first().cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "{LAMBDA_BODY_UDF} needs the lambda body as its first argument"
            ))
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "{LAMBDA_BODY_UDF} needs the lambda body as its first argument"
            ))
        })
    }
}

/// Wraps multi-parameter lambda bodies so that every parameter is referenced.
#[derive(Debug)]
pub struct BindAllLambdaParams {
    udf: Arc<ScalarUDF>,
}

impl AnalyzerRule for BindAllLambdaParams {
    fn name(&self) -> &str {
        "bind_all_lambda_params"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| self.rewrite_node(node))
            .map(|transformed| transformed.data)
    }
}

impl BindAllLambdaParams {
    fn rewrite_node(&self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // What the node's expressions can see: its inputs, and for a leaf its
        // own output.
        let mut schema = merge_schema(&node.inputs());
        schema.merge(node.schema());
        let preserver = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let saved = preserver.save(&expr);
            let rewritten = expr.transform_up(|e| self.rewrite_expr(e, &schema))?;
            Ok(rewritten.update_data(|e| saved.restore(e)))
        })
    }

    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        let Expr::HigherOrderFunction(function) = &expr else {
            return Ok(Transformed::no(expr));
        };
        let needs_wrapping = function.args.iter().any(|arg| match arg {
            Expr::Lambda(lambda) => lambda.params.len() > 1 && !self.is_wrapped(&lambda.body),
            _ => false,
        });
        if !needs_wrapping {
            return Ok(Transformed::no(expr));
        }
        // If the parameter types cannot be worked out here, leave the lambda
        // alone: it then behaves exactly as it would without this rule.
        let Ok(parameters) = function.lambda_parameters(schema) else {
            return Ok(Transformed::no(expr));
        };
        let Expr::HigherOrderFunction(HigherOrderFunction { func, args }) = expr else {
            return Ok(Transformed::no(expr));
        };

        let mut lambda_fields = parameters.into_iter();
        let args = args
            .into_iter()
            .map(|arg| match arg {
                Expr::Lambda(lambda) => {
                    let fields = lambda_fields.next().unwrap_or_default();
                    Expr::Lambda(self.wrap(lambda, &fields))
                }
                other => other,
            })
            .collect();
        Ok(Transformed::yes(Expr::HigherOrderFunction(
            HigherOrderFunction::new(func, args),
        )))
    }

    fn is_wrapped(&self, body: &Expr) -> bool {
        matches!(body, Expr::ScalarFunction(call) if call.func.name() == LAMBDA_BODY_UDF)
    }

    fn wrap(&self, lambda: Lambda, fields: &[FieldRef]) -> Lambda {
        if lambda.params.len() < 2
            || self.is_wrapped(&lambda.body)
            || fields.len() < lambda.params.len()
        {
            return lambda;
        }
        let mut call_args = Vec::with_capacity(lambda.params.len() + 1);
        call_args.push(*lambda.body);
        for (name, field) in lambda.params.iter().zip(fields) {
            let field = Arc::clone(field).renamed(name);
            call_args.push(Expr::LambdaVariable(LambdaVariable::new(
                name.clone(),
                Some(field),
            )));
        }
        let body = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&self.udf), call_args));
        Lambda::new(lambda.params, body)
    }
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

    /// The failing shape: a later parameter used, an earlier one not.
    #[tokio::test]
    async fn a_lambda_may_ignore_its_first_parameter() {
        let text = run("SELECT aggregate([1, 2, 3], 0, (acc, x) -> x) AS last").await;
        assert!(text.contains("| 3 "), "{text}");
    }

    #[tokio::test]
    async fn a_lambda_may_ignore_its_first_parameter_and_use_an_outer_column() {
        let text = run("SELECT aggregate(a, 0, (acc, x) -> x + k) AS v \
             FROM (VALUES ([1, 2], 10), ([5], 100)) AS t(a, k) ORDER BY k")
        .await;
        assert!(text.contains("| 12 ") && text.contains("| 105 "), "{text}");
    }

    /// The wrapper must not leak into a result column's name.
    #[tokio::test]
    async fn the_wrapper_does_not_rename_the_output_column() {
        let text = run("SELECT aggregate([1, 2, 3], 0, (acc, x) -> acc + x)").await;
        assert!(!text.contains(super::LAMBDA_BODY_UDF), "{text}");
        assert!(text.contains("| 6 "), "{text}");
    }

    #[tokio::test]
    async fn nested_multi_parameter_lambdas_still_work() {
        let text =
            run("SELECT transform([[1, 2], [3]], a -> aggregate(a, 0, (acc, x) -> x)) AS lasts")
                .await;
        assert!(text.contains("[2, 3]"), "{text}");
    }
}
