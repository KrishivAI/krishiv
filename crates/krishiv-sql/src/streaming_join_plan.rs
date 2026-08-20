//! Compile a stream-to-stream interval join from SQL text.
//!
//! Recognises the form Flink and Spark users already write — an equi-join with
//! an event-time band in the `ON` clause:
//!
//! ```sql
//! SELECT *
//! FROM bid JOIN auction
//!   ON bid.auction = auction.id
//!  AND bid.ts BETWEEN auction.ts - 5000 AND auction.ts + 5000
//! ```
//!
//! The executing operator implements a **symmetric** window, so asymmetric
//! bounds are refused rather than quietly rounded to something the engine would
//! do differently. Every other unsupported shape is refused by name, following
//! the rule the windowed compiler already follows (§39 A1): a clause this
//! compiler cannot honour is an error, never a silent omission.

use krishiv_plan::stream_join::StreamingJoinSpec;
use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, Select, SetExpr, Statement,
    TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::{SqlError, SqlResult};

fn unsupported(msg: impl Into<String>) -> SqlError {
    SqlError::Unsupported {
        feature: msg.into(),
    }
}

/// A compiled streaming join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingJoinPlan {
    pub spec: StreamingJoinSpec,
}

/// Does this SQL look like a streaming interval join?
///
/// Used for routing, and deliberately cheap: it answers "is this the join
/// shape" without deciding whether the join is *valid*, so a malformed join
/// still reaches the compiler and produces the compiler's error rather than
/// falling through to a different engine whose error would describe a different
/// problem. This is the routing lesson from f022220.
#[must_use]
pub fn looks_like_streaming_join(sql: &str) -> bool {
    let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let has_join =
        select.from.len() == 1 && select.from.first().is_some_and(|f| !f.joins.is_empty());
    if !has_join {
        return false;
    }
    // The event-time BETWEEN band is what makes a join a STREAM-STREAM
    // interval join. A join without one is a different shape — a side-input
    // join against bounded reference data, which the stateless per-batch path
    // handles (task #143) — and routing it here would refuse a query that
    // runs fine elsewhere. Still shape, not validity: a malformed band gets
    // THIS compiler's error about the band, not another engine's error about
    // an unknown table.
    select
        .from
        .first()
        .map(|f| &f.joins)
        .into_iter()
        .flatten()
        .any(|j| match &j.join_operator {
            // ANY join kind counts: a banded LEFT JOIN must be claimed so the
            // "INNER only" refusal a user sees is THIS compiler's, not a
            // stateless-path error about an unknown table.
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::Left(JoinConstraint::On(expr))
            | JoinOperator::Right(JoinConstraint::On(expr))
            | JoinOperator::Semi(JoinConstraint::On(expr))
            | JoinOperator::Anti(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
            | JoinOperator::RightOuter(JoinConstraint::On(expr))
            | JoinOperator::FullOuter(JoinConstraint::On(expr))
            | JoinOperator::LeftSemi(JoinConstraint::On(expr))
            | JoinOperator::RightSemi(JoinConstraint::On(expr))
            | JoinOperator::LeftAnti(JoinConstraint::On(expr))
            | JoinOperator::RightAnti(JoinConstraint::On(expr)) => expr_contains_between(expr),
            _ => false,
        })
}

/// Does the expression tree contain a BETWEEN anywhere?
fn expr_contains_between(expr: &Expr) -> bool {
    match expr {
        Expr::Between { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_between(left) || expr_contains_between(right)
        }
        Expr::Nested(inner) => expr_contains_between(inner),
        _ => false,
    }
}

/// Compile the join form.
///
/// # Errors
/// Returns [`SqlError::Unsupported`] for any shape this compiler cannot
/// represent, naming what it refused.
pub fn compile_streaming_join_sql(sql: &str) -> SqlResult<StreamingJoinPlan> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| unsupported(format!("streaming join parse error: {e}")))?;
    let Some(Statement::Query(query)) = statements.first() else {
        return Err(unsupported("streaming join expects a single SELECT query"));
    };
    let select = extract_select(query.as_ref())?;

    // Fail closed on clauses this compiler does not lower (§39 A1): a clause
    // accepted-but-ignored is a silent wrong answer, the defect class the
    // register exists to remove.
    if select.selection.is_some() {
        return Err(unsupported(
            "streaming joins do not apply WHERE yet: the predicate would be silently \
             dropped and every non-matching row would join anyway. Filter the stream \
             before the join (stateless path) or filter the join output",
        ));
    }
    if query.order_by.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return Err(unsupported(
            "streaming joins do not support ORDER BY / LIMIT / FETCH",
        ));
    }
    if query.with.is_some() {
        return Err(unsupported("streaming joins do not support WITH / CTEs"));
    }

    if select.from.len() != 1 {
        return Err(unsupported(
            "streaming joins support exactly one FROM item with a JOIN; comma-separated \
             tables are not a join this compiler can bound in time",
        ));
    }
    let from = select
        .from
        .first()
        .ok_or_else(|| unsupported("streaming join needs a FROM clause"))?;
    if from.joins.len() != 1 {
        return Err(unsupported(format!(
            "streaming joins support exactly two streams; this query joins {}",
            from.joins.len() + 1
        )));
    }
    let join = from
        .joins
        .first()
        .ok_or_else(|| unsupported("streaming join needs a JOIN clause"))?;

    match &join.join_operator {
        JoinOperator::Inner(_) | JoinOperator::Join(_) => {}
        other => {
            return Err(unsupported(format!(
                "streaming joins support INNER JOIN only; `{other:?}` would need to emit rows \
                 for events whose partner may still arrive, which a bounded window cannot \
                 promise"
            )));
        }
    }

    let left_source = table_name(&from.relation)?;
    let right_source = table_name(&join.relation)?;

    let constraint = match &join.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => c,
        _ => unreachable!("checked above"),
    };
    let JoinConstraint::On(on) = constraint else {
        return Err(unsupported(
            "streaming joins need an ON clause naming the join key and the event-time band; \
             USING and NATURAL do not carry the time bound",
        ));
    };

    let mut equi: Option<(String, String)> = None;
    let mut band: Option<TimeBand> = None;
    for term in flatten_and(on) {
        if let Some(pair) = as_equi_key(term) {
            if equi.is_some() {
                return Err(unsupported(
                    "streaming joins support a single equi-key; combine the columns into one \
                     key upstream",
                ));
            }
            equi = Some(pair);
        } else if let Some(b) = as_time_band(term)? {
            if band.is_some() {
                return Err(unsupported("streaming joins accept one event-time band"));
            }
            band = Some(b);
        } else {
            return Err(unsupported(format!(
                "streaming join ON clause supports `left.key = right.key` and a BETWEEN band \
                 over the event-time column; got `{term}`"
            )));
        }
    }

    let (left_key_column, right_key_column) = equi.ok_or_else(|| {
        unsupported(
            "streaming joins need an equi-key in the ON clause (`left.key = right.key`); \
             without one every left row matches every right row in the window",
        )
    })?;
    let band = band.ok_or_else(|| {
        unsupported(
            "streaming joins need an event-time band in the ON clause, e.g. \
             `l.ts BETWEEN r.ts - 5000 AND r.ts + 5000`; without one the join state is \
             unbounded",
        )
    })?;

    let spec = StreamingJoinSpec {
        left_source,
        right_source,
        time_column: band.time_column,
        left_key_column,
        right_key_column,
        window_ms: band.window_ms,
    };
    spec.validate()
        .map_err(|e| unsupported(format!("streaming join is not valid: {e}")))?;
    Ok(StreamingJoinPlan { spec })
}

fn extract_select(query: &Query) -> SqlResult<&Select> {
    if query.with.is_some() {
        return Err(unsupported(
            "streaming joins do not support WITH clauses; inline the subquery",
        ));
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => Ok(select),
        _ => Err(unsupported("streaming join expects a plain SELECT")),
    }
}

fn table_name(factor: &TableFactor) -> SqlResult<String> {
    match factor {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .map(|p| p.to_string().trim_matches('"').to_owned())
            .ok_or_else(|| unsupported("streaming join source has no name")),
        other => Err(unsupported(format!(
            "streaming joins read plain tables; got `{other}`"
        ))),
    }
}

/// Flatten `a AND b AND c` into its terms so ON-clause order does not matter.
fn flatten_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut out = flatten_and(left);
            out.extend(flatten_and(right));
            out
        }
        Expr::Nested(inner) => flatten_and(inner),
        other => vec![other],
    }
}

/// `left.key = right.key` → (left column, right column).
fn as_equi_key(expr: &Expr) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    Some((column_of(left)?, column_of(right)?))
}

fn column_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        Expr::Nested(inner) => column_of(inner),
        _ => None,
    }
}

struct TimeBand {
    time_column: String,
    window_ms: u64,
}

/// `l.ts BETWEEN r.ts - N AND r.ts + N` → a symmetric band.
///
/// Asymmetric bounds are an error, not a rounding: the operator's window is
/// symmetric, so accepting `BETWEEN r.ts - 1000 AND r.ts + 5000` would run a
/// join the query did not describe.
fn as_time_band(expr: &Expr) -> SqlResult<Option<TimeBand>> {
    let Expr::Between {
        expr: subject,
        negated,
        low,
        high,
    } = expr
    else {
        return Ok(None);
    };
    if *negated {
        return Err(unsupported(
            "streaming joins do not support NOT BETWEEN over event time: it describes \
             everything outside the window, which is unbounded state",
        ));
    }
    let time_column = column_of(subject).ok_or_else(|| {
        unsupported("the BETWEEN subject in a streaming join must be the event-time column")
    })?;

    let low = offset_of(low, "lower")?;
    let high = offset_of(high, "upper")?;
    if low.column != high.column {
        return Err(unsupported(format!(
            "the streaming join band must be measured from one column; got `{}` and `{}`",
            low.column, high.column
        )));
    }
    if low.offset_ms != -high.offset_ms {
        return Err(unsupported(format!(
            "streaming joins run a symmetric window, but this band is {} ms before and {} ms \
             after. Use the same magnitude on both sides, or pre-shift the event time upstream",
            -low.offset_ms, high.offset_ms
        )));
    }
    let window_ms = u64::try_from(high.offset_ms)
        .map_err(|_| unsupported("streaming join window must be a positive number of ms"))?;
    Ok(Some(TimeBand {
        time_column,
        window_ms,
    }))
}

struct Offset {
    column: String,
    offset_ms: i64,
}

/// `r.ts - 5000` / `r.ts + 5000` → the column and its signed offset.
fn offset_of(expr: &Expr, which: &str) -> SqlResult<Offset> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Err(unsupported(format!(
            "the {which} bound of a streaming join band must be `column ± milliseconds`; \
             got `{expr}`"
        )));
    };
    let column = column_of(left).ok_or_else(|| {
        unsupported(format!(
            "the {which} bound must offset from a column; got `{left}`"
        ))
    })?;
    let Expr::Value(v) = right.as_ref() else {
        return Err(unsupported(format!(
            "the {which} bound must offset by a literal number of milliseconds; got `{right}`"
        )));
    };
    let Value::Number(n, _) = &v.value else {
        return Err(unsupported(format!(
            "the {which} bound must offset by a number; got `{}`",
            v.value
        )));
    };
    let magnitude: i64 = n.parse().map_err(|_| {
        unsupported(format!(
            "the {which} bound offset `{n}` is not a whole number of ms"
        ))
    })?;
    let offset_ms = match op {
        BinaryOperator::Plus => magnitude,
        BinaryOperator::Minus => -magnitude,
        other => {
            return Err(unsupported(format!(
                "the {which} bound of a streaming join band must use + or -; got `{other}`"
            )));
        }
    };
    Ok(Offset { column, offset_ms })
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q3: &str = "SELECT b.auction, a.category \
                      FROM bid b JOIN auction a \
                        ON b.auction = a.id \
                       AND b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000";

    #[test]
    fn compiles_the_nexmark_q3_join_shape() {
        let plan = compile_streaming_join_sql(Q3).expect("Q3 join must compile");
        assert_eq!(plan.spec.left_source, "bid");
        assert_eq!(plan.spec.right_source, "auction");
        assert_eq!(plan.spec.left_key_column, "auction");
        assert_eq!(plan.spec.right_key_column, "id");
        assert_eq!(plan.spec.time_column, "dateTime");
        assert_eq!(plan.spec.window_ms, 5_000);
    }

    /// ON-clause term order must not change the result.
    #[test]
    fn the_band_may_precede_the_equi_key() {
        let swapped = "SELECT * FROM bid b JOIN auction a \
                       ON b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000 \
                      AND b.auction = a.id";
        let plan = compile_streaming_join_sql(swapped).expect("compiles");
        assert_eq!(plan.spec.left_key_column, "auction");
        assert_eq!(plan.spec.window_ms, 5_000);
    }

    /// An asymmetric band is refused, quoting both magnitudes.
    ///
    /// The operator's window is symmetric. Accepting this and using one side
    /// would run a join the query did not describe — silently, and only for
    /// events near the window edge, which is the hardest kind of wrong to
    /// notice.
    #[test]
    fn an_asymmetric_band_is_refused_with_both_magnitudes() {
        let sql = "SELECT * FROM bid b JOIN auction a \
                   ON b.auction = a.id \
                  AND b.dateTime BETWEEN a.dateTime - 1000 AND a.dateTime + 5000";
        let err = compile_streaming_join_sql(sql)
            .expect_err("an asymmetric band must be refused")
            .to_string();
        assert!(
            err.contains("1000"),
            "must quote the before magnitude: {err}"
        );
        assert!(
            err.contains("5000"),
            "must quote the after magnitude: {err}"
        );
    }

    /// Without a time band the join state is unbounded, so it is refused.
    #[test]
    fn a_join_without_a_time_band_is_refused() {
        let sql = "SELECT * FROM bid b JOIN auction a ON b.auction = a.id";
        let err = compile_streaming_join_sql(sql)
            .expect_err("an unbounded join must be refused")
            .to_string();
        assert!(err.contains("unbounded"), "got: {err}");
    }

    /// Without an equi-key the join is a cross product within the window.
    #[test]
    fn a_join_without_an_equi_key_is_refused() {
        let sql = "SELECT * FROM bid b JOIN auction a \
                   ON b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000";
        let err = compile_streaming_join_sql(sql)
            .expect_err("a keyless join must be refused")
            .to_string();
        assert!(err.contains("equi-key"), "got: {err}");
    }

    /// Outer joins are refused by name rather than run as inner joins.
    #[test]
    fn an_outer_join_is_refused_rather_than_run_as_an_inner_join() {
        let sql = "SELECT * FROM bid b LEFT JOIN auction a \
                   ON b.auction = a.id \
                  AND b.dateTime BETWEEN a.dateTime - 5000 AND a.dateTime + 5000";
        let err = compile_streaming_join_sql(sql)
            .expect_err("LEFT JOIN must be refused")
            .to_string();
        assert!(err.contains("INNER JOIN only"), "got: {err}");
    }

    /// Three streams are refused, saying how many were seen.
    #[test]
    fn a_three_stream_join_is_refused() {
        let sql = "SELECT * FROM bid b \
                   JOIN auction a ON b.auction = a.id \
                   JOIN person p ON a.seller = p.id";
        let err = compile_streaming_join_sql(sql)
            .expect_err("three streams must be refused")
            .to_string();
        assert!(err.contains('3'), "must say how many streams: {err}");
    }

    /// The routing predicate recognises the join shape but does not judge it.
    ///
    /// A malformed join must still reach this compiler and produce ITS error;
    /// routing on validity would send it to another engine whose message would
    /// describe a different problem. That is the f022220 lesson.
    ///
    /// The shape discriminator is the event-time BETWEEN band (task #144): a
    /// join WITHOUT one is the side-input shape the stateless path handles
    /// (task #143), so it must NOT be claimed here — that is asserted below,
    /// not merely tolerated.
    #[test]
    fn routing_recognises_a_join_it_would_refuse() {
        // Malformed for THIS compiler (LEFT is refused), but it carries the
        // band, so the shape is a streaming join and the refusal must be ours.
        let malformed = "SELECT * FROM bid b LEFT JOIN auction a ON b.auction = a.id \
                         AND b.ts BETWEEN a.ts - 1000 AND a.ts + 1000";
        assert!(
            looks_like_streaming_join(malformed),
            "routing must claim this query so its own error is the one the user sees"
        );
        assert!(compile_streaming_join_sql(malformed).is_err());

        // A band-less join is the SIDE-INPUT shape and belongs to the
        // stateless path; claiming it here would refuse a query that runs
        // fine there.
        assert!(
            !looks_like_streaming_join("SELECT b.v, s.label FROM bid b JOIN side s ON b.v = s.k"),
            "a join with no event-time band is not a stream-stream join"
        );

        assert!(
            !looks_like_streaming_join(
                "SELECT k, COUNT(*) FROM TUMBLE(TABLE t, DESCRIPTOR(ts), 1000) GROUP BY k"
            ),
            "a windowed aggregate is not a join"
        );
    }
}
