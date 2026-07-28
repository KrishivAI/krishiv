//! E5.3 — Recursive CTE: iterative fixpoint execution.
//!
//! DataFusion does not support `WITH RECURSIVE` natively. This module adds:
//!
//! 1. **Detection**: parse `WITH RECURSIVE name AS (base UNION ALL recursive)`.
//! 2. **Rewriter**: expand a recursive CTE into a `NodeOp::RecursiveCte` plan node.
//! 3. **Iterative executor**: given a `SqlEngine`, execute base + recursive rounds
//!    until fixpoint or `max_iterations`.
//!
//! # Execution model
//!
//! ```text
//! working = accumulator = execute(base_query)
//! for i in 0..max_iterations:
//!     delta = execute(recursive_query with cte_name = working)
//!     if delta is empty: break (fixpoint)
//!     accumulator = accumulator UNION ALL delta
//!     working = delta
//! return accumulator
//! ```
//!
//! Each iteration materialises `delta` fully before the next starts. This is
//! the semi-naïve strategy, suitable for transitive-closure and tree-traversal
//! queries on bounded datasets.
//!
//! The recursive self-reference resolves to the **previous iteration's output**
//! — the "working table" — not to everything accumulated so far. That is what
//! SQL specifies, and it is not a refinement: substituting the accumulation
//! re-derives every earlier row on every pass, so `SELECT n + 1 FROM cte WHERE
//! n < 3` seeded with `{1}` yields `{2}`, then `{2, 3}`, then `{2, 3, 3, 4}` —
//! duplicates under `UNION ALL`, and no fixpoint.
//!
//! # Status
//!
//! Nothing calls into this module yet: [`parse_recursive_cte`] has no callers
//! and [`krishiv_plan::NodeOp::RecursiveCte`] is never constructed outside
//! tests, so `WITH RECURSIVE` reaches DataFusion unmodified today. The
//! semantics above are written down so that wiring it up is a wiring job.

use arrow::record_batch::RecordBatch;
use datafusion::sql::sqlparser::ast::{Query, SetExpr, SetOperator, SetQuantifier, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use krishiv_plan::NodeOp;

use crate::{SqlError, SqlResult};

/// Default maximum recursion depth for `WITH RECURSIVE`.
pub const DEFAULT_MAX_ITERATIONS: u32 = 100;

// ── Detection ─────────────────────────────────────────────────────────────────

/// A parsed `WITH RECURSIVE` statement ready for iterative execution.
#[derive(Debug, Clone)]
pub struct RecursiveCteStatement {
    /// The CTE name used in the recursive branch.
    pub name: String,
    /// SQL text for the non-recursive seed query.
    pub base_query: String,
    /// SQL text for the recursive branch (references `name`).
    pub recursive_query: String,
    /// Hard upper bound on iterations.
    pub max_iterations: u32,
}

/// Attempt to parse `sql` as a `WITH RECURSIVE` statement.
///
/// Returns `Ok(Some(...))` when the SQL starts with `WITH RECURSIVE`.
/// Returns `Ok(None)` for any other SQL (not a recursive CTE).
/// Returns `Err` when the SQL is syntactically invalid.
pub fn parse_recursive_cte(sql: &str) -> SqlResult<Option<RecursiveCteStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');

    // Cheap reject only. This used to be `starts_with("WITH RECURSIVE")`,
    // which is a claim about *formatting*, not about the statement: a leading
    // comment, a newline between the two words, or two spaces all made a
    // perfectly ordinary recursive CTE invisible. Whether the statement is
    // recursive is decided below by `with.recursive`, which is the parser's
    // answer rather than ours.
    if !trimmed.to_ascii_uppercase().contains("RECURSIVE") {
        return Ok(None);
    }

    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, trimmed).map_err(|e| SqlError::Unsupported {
        feature: format!("WITH RECURSIVE parse error: {e}"),
    })?;

    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "WITH RECURSIVE produced no statement".into(),
        })?;

    extract_recursive_cte(stmt)
}

fn extract_recursive_cte(stmt: Statement) -> SqlResult<Option<RecursiveCteStatement>> {
    let Statement::Query(q) = stmt else {
        return Ok(None);
    };
    let Some(with) = &q.with else {
        return Ok(None);
    };
    if !with.recursive {
        return Ok(None);
    }

    let cte = with
        .cte_tables
        .first()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "WITH RECURSIVE requires at least one CTE".into(),
        })?;

    let name = cte.alias.name.value.clone();

    let (base_query, recursive_query) =
        split_union_all(&cte.query).ok_or_else(|| SqlError::Unsupported {
            feature: format!(
                "WITH RECURSIVE '{name}': body must be `base_query UNION ALL recursive_query`"
            ),
        })?;

    Ok(Some(RecursiveCteStatement {
        name,
        base_query,
        recursive_query,
        max_iterations: DEFAULT_MAX_ITERATIONS,
    }))
}

/// Split a `SetExpr` that is `left UNION ALL right` into `(left_sql, right_sql)`.
fn split_union_all(query: &Query) -> Option<(String, String)> {
    match query.body.as_ref() {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left,
            right,
        } => {
            let left_sql = format!("SELECT * FROM ({left})");
            let right_sql = format!("SELECT * FROM ({right})");
            Some((left_sql, right_sql))
        }
        _ => None,
    }
}

// ── NodeOp builder ────────────────────────────────────────────────────────────

/// Build a `NodeOp::RecursiveCte` from a parsed `RecursiveCteStatement`.
pub fn build_recursive_cte_op(stmt: &RecursiveCteStatement) -> NodeOp {
    NodeOp::RecursiveCte {
        name: stmt.name.clone(),
        base_query: stmt.base_query.clone(),
        recursive_query: stmt.recursive_query.clone(),
        max_iterations: stmt.max_iterations,
    }
}

// ── Iterative executor ────────────────────────────────────────────────────────

/// Result of a recursive CTE execution.
#[derive(Debug)]
pub struct RecursiveCteResult {
    /// Collected batches from all iterations (base + recursive rounds).
    pub batches: Vec<RecordBatch>,
    /// Number of recursive iterations actually executed (0 = only base ran).
    pub iterations: u32,
    /// `true` when execution stopped because `max_iterations` was reached.
    pub hit_limit: bool,
}

/// Execute a recursive CTE using a `SqlEngine`-like executor callback.
///
/// `execute_fn` is called with a SQL string and the name of the current
/// "working table" (a registered view containing the current accumulator rows).
/// It must return the resulting batches or an error.
///
/// `register_batches_fn` is called to register each iteration's accumulator as
/// a temporary view under `cte_name` so the recursive branch can reference it.
pub fn execute_recursive_cte<E, R>(
    stmt: &RecursiveCteStatement,
    mut execute_fn: E,
    mut register_batches_fn: R,
) -> SqlResult<RecursiveCteResult>
where
    E: FnMut(&str) -> SqlResult<Vec<RecordBatch>>,
    R: FnMut(&str, &[RecordBatch]) -> SqlResult<()>,
{
    // Hard row cap to prevent divergent recursive CTEs from consuming unbounded
    // memory while appearing to respect max_iterations.
    const MAX_ACCUMULATED_ROWS: usize = 10_000_000;

    // Seed: execute the base query. The seed is both the first working table
    // and the first contents of the result.
    let base_batches = execute_fn(&stmt.base_query)?;
    let mut working = base_batches.clone();
    let mut accumulator = base_batches;

    let mut iterations = 0u32;
    let mut hit_limit = false;

    loop {
        if iterations >= stmt.max_iterations {
            hit_limit = true;
            break;
        }

        let acc_rows: usize = accumulator.iter().map(|b| b.num_rows()).sum();
        if acc_rows >= MAX_ACCUMULATED_ROWS {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "WITH RECURSIVE: accumulated row count ({acc_rows}) exceeded limit of {MAX_ACCUMULATED_ROWS}"
                ),
            });
        }

        // The self-reference resolves to the *working table* — the previous
        // iteration's output — not to everything accumulated so far. Binding
        // it to the accumulation re-derives every earlier row on every pass,
        // which under UNION ALL duplicates rows and never reaches a fixpoint.
        register_batches_fn(&stmt.name, &working)?;

        let delta = execute_fn(&stmt.recursive_query)?;
        let delta_rows: usize = delta.iter().map(|b| b.num_rows()).sum();

        if delta_rows == 0 {
            break; // fixpoint reached
        }

        accumulator.extend(delta.iter().cloned());
        working = delta;
        iterations += 1;
    }

    Ok(RecursiveCteResult {
        batches: accumulator,
        iterations,
        hit_limit,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_with_recursive_union_all() {
        let sql = "\
            WITH RECURSIVE cte AS (\
              SELECT 1 AS n \
              UNION ALL \
              SELECT n + 1 FROM cte WHERE n < 5\
            ) SELECT * FROM cte";
        let result = parse_recursive_cte(sql).unwrap();
        assert!(result.is_some());
        let stmt = result.unwrap();
        assert_eq!(stmt.name, "cte");
        assert!(stmt.base_query.contains("SELECT 1"));
        assert!(stmt.recursive_query.to_ascii_uppercase().contains("CTE"));
        assert_eq!(stmt.max_iterations, DEFAULT_MAX_ITERATIONS);
    }

    #[test]
    fn returns_none_for_non_recursive_cte() {
        let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
        let result = parse_recursive_cte(sql).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn returns_none_for_plain_select() {
        let sql = "SELECT * FROM t WHERE x = 1";
        let result = parse_recursive_cte(sql).unwrap();
        assert!(result.is_none());
    }

    /// Plain `UNION` is not the supported body and must be refused.
    ///
    /// The previous version of this test accepted `Ok(Some(_))`, `Ok(None)`
    /// *and* `Err(_)` — every possible outcome — so it could not fail, and it
    /// justified that with a comment claiming "sqlparser parses UNION and
    /// UNION ALL identically at the AST level". It does not: the two differ by
    /// `SetQuantifier`, which is exactly what `split_union_all` matches on.
    #[test]
    fn rejects_non_union_all_body() {
        let sql = "\
            WITH RECURSIVE cte AS (\
              SELECT 1 AS n \
              UNION \
              SELECT n + 1 FROM cte\
            ) SELECT * FROM cte";
        assert!(
            parse_recursive_cte(sql).is_err(),
            "UNION (without ALL) is not the recursive-CTE body this supports"
        );
    }

    /// Detection must survive ordinary formatting.
    #[test]
    fn detection_survives_comments_and_whitespace() {
        for sql in [
            "-- build the closure\nWITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL \
             SELECT n + 1 FROM cte WHERE n < 3) SELECT * FROM cte",
            "WITH\n  RECURSIVE cte AS (SELECT 1 AS n UNION ALL \
             SELECT n + 1 FROM cte WHERE n < 3) SELECT * FROM cte",
            "  with recursive cte as (select 1 as n union all \
             select n + 1 from cte where n < 3) select * from cte",
        ] {
            assert!(
                parse_recursive_cte(sql).unwrap().is_some(),
                "formatting hid a recursive CTE:\n{sql}"
            );
        }
    }

    /// The self-reference must resolve to the previous iteration's output.
    ///
    /// This is the semantics SQL specifies, and the reason it matters is
    /// concrete: binding the self-reference to the whole accumulation
    /// re-derives every earlier row on every pass. The mock below actually
    /// evaluates `n + 1 WHERE n < 4` against whatever was registered, so it
    /// reproduces that divergence rather than returning canned rows — the two
    /// pre-existing executor tests ignore the registered table entirely and
    /// therefore cannot tell the two behaviours apart.
    #[test]
    fn the_working_table_is_the_previous_delta_not_the_accumulation() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::cell::RefCell;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let registered: RefCell<Vec<i32>> = RefCell::new(Vec::new());

        let make = |values: Vec<i32>| -> Vec<RecordBatch> {
            if values.is_empty() {
                return vec![];
            }
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                    .expect("batch"),
            ]
        };
        let read = |batches: &[RecordBatch]| -> Vec<i32> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("int32")
                        .values()
                        .to_vec()
                })
                .collect()
        };

        let stmt = RecursiveCteStatement {
            name: "cte".into(),
            base_query: "SELECT 1 AS n".into(),
            recursive_query: "SELECT n + 1 FROM cte WHERE n < 4".into(),
            max_iterations: DEFAULT_MAX_ITERATIONS,
        };

        let execute = |sql: &str| -> SqlResult<Vec<RecordBatch>> {
            if sql.contains("SELECT 1") {
                return Ok(make(vec![1]));
            }
            // Evaluate the recursive term against the registered working table.
            let out: Vec<i32> = registered
                .borrow()
                .iter()
                .filter(|n| **n < 4)
                .map(|n| n + 1)
                .collect();
            Ok(make(out))
        };
        let register = |_name: &str, batches: &[RecordBatch]| -> SqlResult<()> {
            *registered.borrow_mut() = read(batches);
            Ok(())
        };

        let result = execute_recursive_cte(&stmt, execute, register).expect("run");
        let rows = read(&result.batches);

        assert_eq!(
            rows,
            vec![1, 2, 3, 4],
            "each value must appear exactly once; duplicates mean the \
             self-reference saw the accumulation instead of the working table"
        );
        assert!(!result.hit_limit, "this reaches a fixpoint");
        assert_eq!(result.iterations, 3);
    }

    #[test]
    fn build_recursive_cte_op_returns_correct_variant() {
        let stmt = RecursiveCteStatement {
            name: "tree".into(),
            base_query: "SELECT id FROM nodes WHERE parent_id IS NULL".into(),
            recursive_query: "SELECT n.id FROM nodes n JOIN tree t ON n.parent_id = t.id".into(),
            max_iterations: 50,
        };
        let op = build_recursive_cte_op(&stmt);
        match op {
            NodeOp::RecursiveCte {
                name,
                max_iterations,
                ..
            } => {
                assert_eq!(name, "tree");
                assert_eq!(max_iterations, 50);
            }
            _ => panic!("expected RecursiveCte"),
        }
    }

    #[test]
    fn iterative_executor_stops_at_fixpoint() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));

        let stmt = RecursiveCteStatement {
            name: "cte".into(),
            base_query: "SELECT 1 AS n".into(),
            recursive_query: "SELECT n + 1 FROM cte WHERE n < 3".into(),
            max_iterations: DEFAULT_MAX_ITERATIONS,
        };

        // Simulate execution: base returns [{n:1}], then recursive returns
        // [{n:2}], [{n:3}], then empty (fixpoint).
        let mut call_count = 0u32;
        let schema_clone = schema.clone();
        let execute = |sql: &str| -> SqlResult<Vec<RecordBatch>> {
            call_count += 1;
            let values: Vec<i32> = if sql.contains("SELECT 1") {
                vec![1]
            } else {
                // Recursive call: simulate returning empty after 2 rounds.
                match call_count {
                    2 => vec![2],
                    3 => vec![3],
                    _ => vec![],
                }
            };
            if values.is_empty() {
                return Ok(vec![]);
            }
            let batch = RecordBatch::try_new(
                schema_clone.clone(),
                vec![Arc::new(Int32Array::from(values))],
            )
            .map_err(|e| SqlError::Unsupported {
                feature: e.to_string(),
            })?;
            Ok(vec![batch])
        };

        let register = |_name: &str, _batches: &[RecordBatch]| -> SqlResult<()> { Ok(()) };

        let result = execute_recursive_cte(&stmt, execute, register).unwrap();
        assert!(!result.hit_limit);
        assert!(result.iterations <= 3);
        let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0);
    }

    #[test]
    fn iterative_executor_respects_max_iterations() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));

        let stmt = RecursiveCteStatement {
            name: "inf".into(),
            base_query: "SELECT 0 AS n".into(),
            recursive_query: "SELECT n + 1 FROM inf".into(),
            max_iterations: 5,
        };

        let schema_clone = schema.clone();
        let execute = |_sql: &str| -> SqlResult<Vec<RecordBatch>> {
            let batch = RecordBatch::try_new(
                schema_clone.clone(),
                vec![Arc::new(Int32Array::from(vec![42i32]))],
            )
            .map_err(|e| SqlError::Unsupported {
                feature: e.to_string(),
            })?;
            Ok(vec![batch])
        };

        let register = |_: &str, _: &[RecordBatch]| -> SqlResult<()> { Ok(()) };

        let result = execute_recursive_cte(&stmt, execute, register).unwrap();
        assert!(result.hit_limit, "should have hit max_iterations");
        assert_eq!(result.iterations, 5);
    }
}
