//! Scalar SQL-expression user functions, expanded (inlined) into native SQL
//! before planning.
//!
//! A scalar SQL function such as `CREATE FUNCTION tax(x DOUBLE) RETURNS DOUBLE
//! RETURN x * 1.1` is stored as a parsed body expression plus its parameter
//! names. When a query references it (`SELECT tax(amount) FROM sales`), every
//! call is replaced with the body — arguments substituted for parameters — so
//! the query becomes pure native SQL (`SELECT (amount * 1.1) FROM sales`).
//!
//! Because the result is ordinary SQL with no UDF reference, it plans and runs
//! anywhere the engine runs, INCLUDING distributed execution on the Rust
//! executors — no Python interpreter and no per-executor function registration
//! required. This is the portable, distributable counterpart to Python-callable
//! UDFs, which can only run in the embedded (in-process) engine.

use std::collections::HashMap;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, visit_expressions_mut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// A scalar SQL function definition: its (lower-cased) name, ordered parameter
/// names, and the parsed body expression.
#[derive(Clone, Debug)]
pub struct ScalarSqlFunction {
    pub name: String,
    pub params: Vec<String>,
    body: Expr,
}

impl ScalarSqlFunction {
    /// Build from a name, parameter names, and a body SQL expression (e.g.
    /// `"x * 1.1"`). Parameter/function names are matched case-insensitively.
    pub fn new(name: &str, params: &[String], body_sql: &str) -> Result<Self, String> {
        let dialect = GenericDialect {};
        let body = Parser::new(&dialect)
            .try_with_sql(body_sql)
            .and_then(|mut p| p.parse_expr())
            .map_err(|e| format!("invalid scalar function body '{body_sql}': {e}"))?;
        Ok(Self {
            name: name.trim().to_lowercase(),
            params: params.iter().map(|p| p.trim().to_lowercase()).collect(),
            body,
        })
    }
}

fn object_name_lower(name: &ObjectName) -> String {
    name.to_string().to_lowercase()
}

fn unnamed_args(args: &FunctionArguments) -> Option<Vec<Expr>> {
    match args {
        FunctionArguments::List(list) => {
            let mut out = Vec::with_capacity(list.args.len());
            for arg in &list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => out.push(expr.clone()),
                    _ => return None,
                }
            }
            Some(out)
        }
        _ => None,
    }
}

/// Replace every parameter identifier in `body` with the matching argument.
fn substitute(body: &mut Expr, params: &[String], args: &[Expr]) {
    let _: ControlFlow<()> = visit_expressions_mut(body, |expr| {
        if let Expr::Identifier(ident) = expr {
            let name = ident.value.to_lowercase();
            if let Some(pos) = params.iter().position(|p| *p == name)
                && let Some(arg) = args.get(pos)
            {
                *expr = arg.clone();
            }
        }
        ControlFlow::Continue(())
    });
}

/// Inline every call to a registered scalar SQL function in `sql`, returning
/// pure native SQL. No-op (returns `sql` unchanged) when `funcs` is empty or the
/// query references none of them. Errors only if the query cannot be parsed.
pub fn expand_scalar_sql_functions(
    sql: &str,
    funcs: &HashMap<String, ScalarSqlFunction>,
) -> Result<String, String> {
    if funcs.is_empty() {
        return Ok(sql.to_string());
    }
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("cannot parse query for scalar-UDF expansion: {e}"))?;

    // Bounded fixpoint so a function body that itself calls another registered
    // function is also expanded; the bound guards against a mutually-recursive
    // definition looping forever.
    let mut any = false;
    for _ in 0..32 {
        let mut changed = false;
        let _: ControlFlow<()> = visit_expressions_mut(&mut statements, |expr| {
            if let Expr::Function(func) = expr {
                let fname = object_name_lower(&func.name);
                if let Some(def) = funcs.get(&fname)
                    && let Some(args) = unnamed_args(&func.args)
                    && args.len() == def.params.len()
                {
                    let mut body = def.body.clone();
                    substitute(&mut body, &def.params, &args);
                    // Parenthesize to preserve precedence at the call site.
                    *expr = Expr::Nested(Box::new(body));
                    changed = true;
                }
            }
            ControlFlow::Continue(())
        });
        any |= changed;
        if !changed {
            break;
        }
    }

    if !any {
        return Ok(sql.to_string());
    }
    Ok(statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(defs: &[(&str, &[&str], &str)]) -> HashMap<String, ScalarSqlFunction> {
        defs.iter()
            .map(|(n, p, b)| {
                let params: Vec<String> = p.iter().map(|s| s.to_string()).collect();
                (n.to_lowercase(), ScalarSqlFunction::new(n, &params, b).unwrap())
            })
            .collect()
    }

    fn norm(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_lowercase()
    }

    #[test]
    fn inlines_single_call() {
        let funcs = reg(&[("tax", &["x"], "x * 1.1")]);
        let out = expand_scalar_sql_functions("SELECT tax(amount) FROM sales", &funcs).unwrap();
        assert_eq!(norm(&out), norm("SELECT (amount * 1.1) FROM sales"));
    }

    #[test]
    fn inlines_multiple_params_and_calls() {
        let funcs = reg(&[("disc", &["p", "d"], "p * (1 - d)")]);
        let out = expand_scalar_sql_functions(
            "SELECT disc(price, rate), disc(unit_price, 0.2) FROM t WHERE disc(price, rate) > 10",
            &funcs,
        )
        .unwrap();
        assert!(norm(&out).contains(&norm("(price * (1 - rate))")));
        assert!(norm(&out).contains(&norm("(unit_price * (1 - 0.2))")));
    }

    #[test]
    fn inlines_nested_functions() {
        let funcs = reg(&[("a", &["x"], "x + 1"), ("b", &["y"], "a(y) * 2")]);
        let out = expand_scalar_sql_functions("SELECT b(v) FROM t", &funcs).unwrap();
        // b(v) -> (a(v) * 2) -> ((v + 1) * 2)
        assert!(norm(&out).contains(&norm("((v + 1) * 2)")));
    }

    #[test]
    fn leaves_unrelated_and_builtin_calls_untouched() {
        let funcs = reg(&[("tax", &["x"], "x * 1.1")]);
        let out = expand_scalar_sql_functions(
            "SELECT SUM(amount), UPPER(region) FROM sales",
            &funcs,
        )
        .unwrap();
        assert_eq!(norm(&out), norm("SELECT SUM(amount), UPPER(region) FROM sales"));
    }

    #[test]
    fn empty_registry_is_noop() {
        let funcs = HashMap::new();
        let sql = "SELECT tax(amount) FROM sales";
        assert_eq!(expand_scalar_sql_functions(sql, &funcs).unwrap(), sql);
    }
}
