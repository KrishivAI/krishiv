//! Evaluation of pre-window scalar expressions.
//!
//! The window operators aggregate a *named column*. An aggregate over an
//! expression — `SUM(price * 908 / 1000)`, NEXMark Q1's currency conversion —
//! therefore needs the expression materialised into a column before the window
//! sees the batch. The SQL compiler lowers such an argument into a
//! [`DerivedColumn`]; this module computes it.
//!
//! Deliberately arithmetic-only. This is not a general expression engine and
//! must not become one: the dataflow crate has no SQL parser and evaluates a
//! small serializable IR with Arrow kernels, exactly as it does for
//! `WindowAggFilter`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_plan::window::{DerivedColumn, ScalarBinaryOp, WindowScalarExpr};

use crate::{ExecError, ExecResult};

/// Is this type evaluated in floating point rather than as an integer?
fn is_float(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// The type an operand pair is computed in.
///
/// Float wins if either side is float; otherwise `Int64`. Integer division then
/// truncates, which is SQL's own behaviour for integer operands and is what
/// NEXMark Q1 expects — it is not an approximation of float division.
fn common_type(left: &DataType, right: &DataType) -> DataType {
    if is_float(left) || is_float(right) {
        DataType::Float64
    } else {
        DataType::Int64
    }
}

/// Evaluate one expression against a batch, producing a full-length array.
///
/// # Errors
/// Returns an error when a referenced column is absent, a cast fails, or the
/// arithmetic kernel rejects the operand types (integer divide by zero, for
/// instance, which fails rather than producing a silent null).
pub fn eval_scalar_expr(expr: &WindowScalarExpr, batch: &RecordBatch) -> ExecResult<ArrayRef> {
    let rows = batch.num_rows();
    match expr {
        WindowScalarExpr::Column(name) => {
            let idx = batch.schema().index_of(name).map_err(|_| {
                ExecError::InvalidInput(format!(
                    "derived column expression references '{name}', which the source batch does \
                     not have"
                ))
            })?;
            Ok(Arc::clone(batch.column(idx)))
        }
        WindowScalarExpr::Int(v) => Ok(Arc::new(Int64Array::from(vec![*v; rows])) as ArrayRef),
        WindowScalarExpr::Float(v) => Ok(Arc::new(Float64Array::from(vec![v.0; rows])) as ArrayRef),
        WindowScalarExpr::Binary { left, op, right } => {
            let l = eval_scalar_expr(left, batch)?;
            let r = eval_scalar_expr(right, batch)?;
            let target = common_type(l.data_type(), r.data_type());
            let l = cast(&l, &target).map_err(|e| {
                ExecError::Arrow(format!("derived column cast of left operand: {e}"))
            })?;
            let r = cast(&r, &target).map_err(|e| {
                ExecError::Arrow(format!("derived column cast of right operand: {e}"))
            })?;
            let out = match op {
                ScalarBinaryOp::Add => numeric::add(&l, &r),
                ScalarBinaryOp::Sub => numeric::sub(&l, &r),
                ScalarBinaryOp::Mul => numeric::mul(&l, &r),
                ScalarBinaryOp::Div => numeric::div(&l, &r),
                ScalarBinaryOp::Mod => numeric::rem(&l, &r),
            };
            out.map_err(|e| {
                ExecError::Arrow(format!(
                    "derived column '{}' {} '{}': {e}",
                    left,
                    op.symbol(),
                    right
                ))
            })
        }
    }
}

/// Append every derived column to a batch, in order.
///
/// Later expressions may reference earlier derived columns, so each is appended
/// before the next is evaluated.
///
/// # Errors
/// Propagates evaluation failures, and rejects a derived name that already
/// exists in the batch rather than shadowing a source column.
pub fn append_derived_columns(
    batch: &RecordBatch,
    derived: &[DerivedColumn],
) -> ExecResult<RecordBatch> {
    if derived.is_empty() {
        return Ok(batch.clone());
    }

    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().map(Arc::clone).collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut current = batch.clone();

    for d in derived {
        if current.schema().index_of(&d.name).is_ok() {
            return Err(ExecError::InvalidInput(format!(
                "derived column '{}' collides with an existing column; generated names use the \
                 __krishiv_expr_N form precisely so this cannot happen from SQL",
                d.name
            )));
        }
        let array = eval_scalar_expr(&d.expr, &current)?;
        fields.push(Arc::new(Field::new(
            &d.name,
            array.data_type().clone(),
            true,
        )));
        columns.push(array);
        current = RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), columns.clone())?;
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_plan::window::FloatLiteral;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_000_i64, 2_500]))],
        )
        .expect("batch")
    }

    fn col(name: &str) -> Box<WindowScalarExpr> {
        Box::new(WindowScalarExpr::Column(name.to_owned()))
    }

    /// NEXMark Q1's currency conversion, evaluated exactly.
    #[test]
    fn evaluates_nexmark_q1_currency_conversion_with_integer_semantics() {
        // price * 908 / 1000
        let expr = WindowScalarExpr::Binary {
            left: Box::new(WindowScalarExpr::Binary {
                left: col("price"),
                op: ScalarBinaryOp::Mul,
                right: Box::new(WindowScalarExpr::Int(908)),
            }),
            op: ScalarBinaryOp::Div,
            right: Box::new(WindowScalarExpr::Int(1_000)),
        };
        let out = eval_scalar_expr(&expr, &batch()).expect("evaluates");
        let vals = out.as_any().downcast_ref::<Int64Array>().expect("Int64");
        // 1000*908/1000 = 908 ; 2500*908/1000 = 2270 (integer division truncates)
        assert_eq!(vals.value(0), 908);
        assert_eq!(vals.value(1), 2_270);
    }

    /// A float literal anywhere promotes the whole computation to f64.
    #[test]
    fn a_float_operand_promotes_the_computation() {
        let expr = WindowScalarExpr::Binary {
            left: col("price"),
            op: ScalarBinaryOp::Mul,
            right: Box::new(WindowScalarExpr::Float(FloatLiteral(0.908))),
        };
        let out = eval_scalar_expr(&expr, &batch()).expect("evaluates");
        let vals = out
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert!((vals.value(0) - 908.0).abs() < 1e-9);
    }

    #[test]
    fn a_missing_column_is_named_in_the_error() {
        let expr = WindowScalarExpr::Column("nope".to_owned());
        let err = eval_scalar_expr(&expr, &batch()).expect_err("must fail");
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    #[test]
    fn derived_columns_are_appended_in_order_and_can_chain() {
        let derived = vec![
            DerivedColumn {
                name: "__krishiv_expr_0".to_owned(),
                expr: WindowScalarExpr::Binary {
                    left: col("price"),
                    op: ScalarBinaryOp::Mul,
                    right: Box::new(WindowScalarExpr::Int(2)),
                },
            },
            // references the previous derived column
            DerivedColumn {
                name: "__krishiv_expr_1".to_owned(),
                expr: WindowScalarExpr::Binary {
                    left: col("__krishiv_expr_0"),
                    op: ScalarBinaryOp::Add,
                    right: Box::new(WindowScalarExpr::Int(1)),
                },
            },
        ];
        let out = append_derived_columns(&batch(), &derived).expect("appends");
        assert_eq!(out.num_columns(), 3);
        let second = out
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(second.value(0), 2_001);
    }
}
