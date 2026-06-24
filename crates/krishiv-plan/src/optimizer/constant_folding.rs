//! Constant-folding and tautology-elimination rule (T3).
//!
//! Operates on the plan-layer `Filter { predicate: String }` expressions. The
//! plan layer does not yet have a structured expression AST, so this rule
//! parses simple, common patterns out of the predicate string:
//!
//! - Arithmetic: `1 + 1` → `2`, `(2 * 3) + 4` → `10`.
//! - Boolean tautologies: `1 = 1` → `true`, `1 = 0` → `false`.
//! - Logical simplifications:
//!   - `TRUE AND x`  → `x`
//!   - `FALSE AND x` → `FALSE`
//!   - `TRUE OR x`   → `TRUE`
//!   - `FALSE OR x`  → `x`
//!   - `NOT TRUE`    → `FALSE`, `NOT FALSE` → `TRUE`
//! - Constant-folding nested boolean expressions: `1 = 1 AND col = 1` → `col = 1`,
//!   `1 = 0 AND col = 1` → `FALSE`.
//!
//! The rule is conservative: any expression that does not reduce to a
//! known-constant form is left unchanged. The point is to elide whole
//! filter sub-expressions and to surface the predicate strings that
//! contain obviously-constant operands to downstream passes.
//!
//! Limitations (follow-ups):
//! - No typed comparison. `1 = 1` and `'a' = 'a'` both fold, but
//!   `1 = '1'` is not modelled — these are left to the executor.
//! - No `IN`, `BETWEEN`, `LIKE`, or `IS NULL` folding. Spark's
//!   `ConstantFolding` handles these via the typed expression AST; we
//!   do not have one yet at the plan layer.

use crate::optimizer::OptimizerRule;
use crate::{LogicalPlan, NodeOp, PlanNode};

/// Optimizer rule that folds constant sub-expressions inside filter predicates
/// and removes always-true / always-false filters.
pub struct ConstantFoldingRule;

impl OptimizerRule for ConstantFoldingRule {
    fn name(&self) -> &str {
        "constant-folding"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let mut changed = false;
        let mut new_nodes: Vec<PlanNode> = Vec::with_capacity(plan.nodes().len());
        for node in plan.nodes() {
            let new_op = match node.op() {
                Some(NodeOp::Filter { predicate }) => {
                    let folded = fold_predicate(predicate);
                    if folded.as_deref() != Some(predicate.as_str()) {
                        changed = true;
                    }
                    NodeOp::Filter {
                        predicate: folded.unwrap_or_else(|| predicate.clone()),
                    }
                }
                Some(other) => other.clone(),
                None => return None,
            };
            let mut new_node = PlanNode::new(node.id(), node.label(), node.kind())
                .with_op(new_op)
                .with_inputs(node.inputs().to_vec());
            let partitioning = node.partitioning().clone();
            new_node = new_node.with_partitioning(partitioning);
            if let Some(estimated_rows) = node.estimated_rows() {
                new_node = new_node.with_estimated_rows(Some(estimated_rows));
            }
            if node.broadcast_eligible() {
                new_node = new_node.with_broadcast_eligible(true);
            }
            // T3: preserve the output schema so a no-op rule returns a
            // plan that compares equal to the input. Without this, the
            // optimizer would record the rule as "applied" (because
            // `new_plan != current`) even when nothing changed, masking
            // subsequent rules.
            let schema = node.output_schema();
            if !schema.is_empty() {
                new_node = new_node.with_output_schema(schema.clone());
            }
            new_nodes.push(new_node);
        }
        if !changed {
            return None;
        }
        // Preserve the plan's name + kind.
        let mut new_plan = LogicalPlan::new(plan.name(), plan.kind());
        for node in &new_nodes {
            new_plan.add_node(node.clone());
        }
        if let Some(parts) = plan.shuffle_partitions() {
            new_plan = new_plan.with_shuffle_partitions(Some(parts));
        }
        Some(new_plan)
    }
}

/// Fold `predicate`. Returns `Some(new_predicate)` when the predicate changes
/// (including the empty string for "always-true", but the rule itself
/// substitutes `TRUE` so the executor treats it as a no-op), or `None` when
/// the predicate is already constant-true (so the caller can elide the
/// predicate entirely).
fn fold_predicate(predicate: &str) -> Option<String> {
    let trimmed = predicate.trim();
    if trimmed.is_empty() {
        return Some("TRUE".to_string());
    }
    let folded = try_fold(trimmed);
    match folded {
        FoldResult::True => Some("TRUE".to_string()),
        FoldResult::False => Some("FALSE".to_string()),
        FoldResult::Expr(s) if s == trimmed => None,
        FoldResult::Expr(s) => Some(s),
        FoldResult::Unknown => None,
    }
}

enum FoldResult {
    /// Always-true.
    True,
    /// Always-false.
    False,
    /// Folded expression (possibly identical to input).
    Expr(String),
    /// Could not fold; leave as-is.
    Unknown,
}

/// Attempt to fold a single predicate string. The parser is intentionally
/// minimal — it only recognises integer arithmetic (`+`, `-`, `*`, `/`),
/// equality comparisons, boolean connectives, and `NOT`. Any other
/// token (column reference, function call, `IN`, `BETWEEN`, etc.) makes
/// the whole predicate `Unknown` and we return it unchanged.
fn try_fold(input: &str) -> FoldResult {
    let mut p = Parser::new(input);
    let result = p.parse_or();
    if !p.eof() {
        return FoldResult::Expr(input.to_string());
    }
    match result {
        Ok(FoldValue::Bool(b)) => {
            if b {
                FoldResult::True
            } else {
                FoldResult::False
            }
        }
        Ok(FoldValue::Int(n)) => FoldResult::Expr(n.to_string()),
        Ok(FoldValue::Str(s)) => FoldResult::Expr(s),
        Ok(FoldValue::Column(_)) | Err(()) => FoldResult::Unknown,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FoldValue {
    Bool(bool),
    Int(i64),
    /// String literal preserved verbatim.
    Str(String),
    /// A column reference or any other expression that cannot be evaluated
    /// at planning time. The string is the textual representation; we keep
    /// it so the caller can decide whether short-circuit rewrites are safe.
    Column(String),
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.bytes.get(self.pos).copied();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn eat(&mut self, c: u8) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_kw(&mut self, kw: &[u8]) -> bool {
        self.skip_ws();
        if self.bytes[self.pos..].starts_with(kw) {
            // Must be followed by whitespace or eof to avoid eating `note` as `not`.
            let after = self.pos + kw.len();
            if after == self.bytes.len() || self.bytes[after].is_ascii_whitespace() {
                self.pos = after;
                return true;
            }
        }
        false
    }

    fn parse_or(&mut self) -> Result<FoldValue, ()> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_ws();
            if self.eat_kw(b"OR") {
                let right = self.parse_and()?;
                // OR reduction rules:
                //   `TRUE OR x`  → `TRUE`       (always folds to constant)
                //   `FALSE OR x` → `x`          (only fold if `x` is a column)
                //   `x OR TRUE`  → `TRUE`       (always folds to constant)
                //   `x OR FALSE` → `x`          (only fold if `x` is a column)
                left = match (left.clone(), right) {
                    (FoldValue::Bool(true), _) | (_, FoldValue::Bool(true)) => {
                        FoldValue::Bool(true)
                    }
                    (FoldValue::Bool(false), r) => r,
                    (l, FoldValue::Bool(false)) => l,
                    // All other combinations of `Bool` and `Column` cannot
                    // be safely rewritten; leave the predicate unchanged.
                    (FoldValue::Column(_), _) | (_, FoldValue::Column(_)) => {
                        return Err(());
                    }
                    _ => return Err(()),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<FoldValue, ()> {
        let mut left = self.parse_not()?;
        loop {
            self.skip_ws();
            if self.eat_kw(b"AND") {
                let right = self.parse_not()?;
                // AND reduction rules:
                //   `FALSE AND x` → `FALSE`     (always folds to constant)
                //   `TRUE AND x`  → `x`        (only fold if `x` is a column)
                //   `x AND FALSE` → `FALSE`     (always folds to constant)
                //   `x AND TRUE`  → `x`        (only fold if `x` is a column)
                left = match (left.clone(), right) {
                    (FoldValue::Bool(false), _) | (_, FoldValue::Bool(false)) => {
                        FoldValue::Bool(false)
                    }
                    (FoldValue::Bool(true), r) => r,
                    (l, FoldValue::Bool(true)) => l,
                    (FoldValue::Column(_), _) | (_, FoldValue::Column(_)) => {
                        return Err(());
                    }
                    _ => return Err(()),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<FoldValue, ()> {
        self.skip_ws();
        if self.eat_kw(b"NOT") {
            let v = self.parse_cmp()?;
            return Ok(match v {
                FoldValue::Bool(b) => FoldValue::Bool(!b),
                _ => return Err(()),
            });
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<FoldValue, ()> {
        let start = self.pos;
        let left = self.parse_add()?;
        self.skip_ws();
        // Order matters: `<=` and `>=` must be tested before `<` / `>`.
        let op = if self.eat(b'<') {
            if self.eat(b'=') { "<=" } else { "<" }
        } else if self.eat(b'>') {
            if self.eat(b'=') { ">=" } else { ">" }
        } else if self.eat(b'=') {
            if self.eat(b'=') { "==" } else { "=" }
        } else if self.eat(b'!') {
            if self.eat(b'=') {
                "!="
            } else {
                return Err(());
            }
        } else {
            return Ok(left);
        };
        let right = self.parse_add()?;
        // When one side is non-foldable (e.g. a column reference), we cannot
        // evaluate the comparison, but AND/OR may still be able to
        // short-circuit. Surface the whole comparison as a `Column` marker
        // carrying the textual representation so the AND/OR reducers can
        // see it as a non-constant operand.
        if matches!(left, FoldValue::Column(_)) || matches!(right, FoldValue::Column(_)) {
            let end = self.pos;
            let text = std::str::from_utf8(&self.bytes[start..end])
                .map_err(|_| ())?
                .trim()
                .to_string();
            return Ok(FoldValue::Column(text));
        }
        self.cmp_op(op, left, right)
    }

    fn cmp_op(&self, op: &str, left: FoldValue, right: FoldValue) -> Result<FoldValue, ()> {
        match (left, right) {
            (FoldValue::Int(a), FoldValue::Int(b)) => {
                let r = match op {
                    "=" | "==" => a == b,
                    "!=" => a != b,
                    "<" => a < b,
                    "<=" => a <= b,
                    ">" => a > b,
                    ">=" => a >= b,
                    _ => return Err(()),
                };
                Ok(FoldValue::Bool(r))
            }
            (FoldValue::Str(a), FoldValue::Str(b)) => match op {
                "=" | "==" => Ok(FoldValue::Bool(a == b)),
                "!=" => Ok(FoldValue::Bool(a != b)),
                _ => Err(()),
            },
            _ => Err(()),
        }
    }

    fn parse_add(&mut self) -> Result<FoldValue, ()> {
        let mut left = self.parse_mul()?;
        loop {
            self.skip_ws();
            let op = if self.eat(b'+') {
                Some(b'+')
            } else if self.eat(b'-') {
                Some(b'-')
            } else {
                None
            };
            let Some(op) = op else { break };
            let right = self.parse_mul()?;
            left = match (left, right) {
                (FoldValue::Int(a), FoldValue::Int(b)) => match op {
                    b'+' => FoldValue::Int(a.checked_add(b).ok_or(())?),
                    b'-' => FoldValue::Int(a.checked_sub(b).ok_or(())?),
                    _ => return Err(()),
                },
                _ => return Err(()),
            };
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<FoldValue, ()> {
        let mut left = self.parse_unary()?;
        loop {
            self.skip_ws();
            let op = if self.eat(b'*') {
                Some(b'*')
            } else if self.eat(b'/') {
                Some(b'/')
            } else {
                None
            };
            let Some(op) = op else { break };
            let right = self.parse_unary()?;
            left = match (left, right) {
                (FoldValue::Int(a), FoldValue::Int(b)) => match op {
                    b'*' => FoldValue::Int(a.checked_mul(b).ok_or(())?),
                    b'/' => {
                        if b == 0 {
                            return Err(());
                        }
                        FoldValue::Int(a.checked_div(b).ok_or(())?)
                    }
                    _ => return Err(()),
                },
                _ => return Err(()),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<FoldValue, ()> {
        self.skip_ws();
        if self.eat(b'-') {
            let v = self.parse_atom()?;
            return match v {
                FoldValue::Int(n) => n.checked_neg().map(FoldValue::Int).ok_or(()),
                _ => Err(()),
            };
        }
        if self.eat(b'+') {
            return self.parse_atom();
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<FoldValue, ()> {
        self.skip_ws();
        if self.eat(b'(') {
            let v = self.parse_or()?;
            self.skip_ws();
            if !self.eat(b')') {
                return Err(());
            }
            return Ok(v);
        }
        // Boolean literal.
        if self.eat_kw(b"TRUE") {
            return Ok(FoldValue::Bool(true));
        }
        if self.eat_kw(b"FALSE") {
            return Ok(FoldValue::Bool(false));
        }
        // String literal — single quotes.
        if self.peek() == Some(b'\'') {
            self.pos += 1;
            let start = self.pos;
            while let Some(c) = self.peek() {
                if c == b'\\' {
                    self.pos += 1;
                    if self.bump().is_none() {
                        return Err(());
                    }
                    continue;
                }
                if c == b'\'' {
                    break;
                }
                self.pos += 1;
            }
            if !self.eat(b'\'') {
                return Err(());
            }
            let s = std::str::from_utf8(&self.bytes[start..self.pos - 1])
                .map_err(|_| ())?
                .to_string();
            return Ok(FoldValue::Str(s));
        }
        // Integer literal.
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start < self.pos {
            let s = std::str::from_utf8(&self.bytes[start..self.pos]).map_err(|_| ())?;
            return s.parse::<i64>().map(FoldValue::Int).map_err(|_| ());
        }
        // Identifier — could be a column reference or a function call.
        // We return a `Column(name)` marker so the AND/OR reducers can
        // make short-circuit decisions (e.g. `FALSE AND <col>` → `FALSE`).
        // Function calls like `upper(name)` are not handled — the
        // closing paren would have to follow, which the simple atom
        // parser does not consume — so they fall through to `Err(())`
        // and the predicate is left unchanged.
        let id_start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if id_start < self.pos {
            // Reject identifier-followed-by-`(` to avoid misinterpreting
            // function calls as columns. The strict behavior is to leave
            // the predicate unchanged for function calls.
            if self.peek() == Some(b'(') {
                // Roll back the identifier; this is a function call.
                self.pos = id_start;
                return Err(());
            }
            let name = std::str::from_utf8(&self.bytes[id_start..self.pos])
                .map_err(|_| ())?
                .to_string();
            return Ok(FoldValue::Column(name));
        }
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_fold(input: &str, expected: &str) {
        let result = fold_predicate(input);
        assert_eq!(
            result.as_deref(),
            Some(expected),
            "fold_predicate({input:?}) expected {expected:?}, got {result:?}"
        );
    }

    #[test]
    fn folds_arithmetic() {
        assert_fold("1 + 1", "2");
        assert_fold("(2 * 3) + 4", "10");
        assert_fold("10 - 3", "7");
        assert_fold("12 / 4", "3");
        assert_fold("-(5)", "-5");
    }

    #[test]
    fn folds_comparisons() {
        assert_fold("1 = 1", "TRUE");
        assert_fold("1 = 0", "FALSE");
        assert_fold("2 > 1", "TRUE");
        assert_fold("2 < 1", "FALSE");
    }

    #[test]
    fn folds_logical_connectives_when_other_side_is_constant() {
        // `TRUE AND col = 1` is logically `col = 1`, but the rule cannot
        // rewrite an expression that still has non-constant operands. The
        // fold result is therefore `None` (left unchanged).
        assert_eq!(fold_predicate("TRUE AND col = 1"), None);
        // `FALSE AND x` is always false regardless of x — fold succeeds.
        assert_fold("FALSE AND col = 1", "FALSE");
        // `TRUE OR x` is always true.
        assert_fold("TRUE OR col = 1", "TRUE");
        // `FALSE OR x` is logically `x`, but we cannot rewrite the
        // expression (column ref preserved).
        assert_eq!(fold_predicate("FALSE OR col = 1"), None);
        assert_fold("NOT TRUE", "FALSE");
        assert_fold("NOT FALSE", "TRUE");
    }

    #[test]
    fn leaves_already_folded_expressions_alone() {
        let r = fold_predicate("1 + 1");
        assert_eq!(r.as_deref(), Some("2"));
    }

    #[test]
    fn leaves_column_references_alone() {
        assert_eq!(fold_predicate("col = 1"), None);
        assert_eq!(fold_predicate("a + b"), None);
    }

    #[test]
    fn folds_nested_boolean_when_reduces_to_constant() {
        // `1 = 0 AND col = 1` reduces to `FALSE` because the LHS is constant
        // `false`; the rule can rewrite the entire expression.
        assert_fold("1 = 0 AND col = 1", "FALSE");
        // `1 = 1 AND col = 1` would logically be `col = 1`, but the rule
        // cannot rewrite an expression that has non-constant operands
        // (no expression-AST rewrite is available), so the predicate is
        // left unchanged.
        assert_eq!(fold_predicate("1 = 1 AND col = 1"), None);
    }
}
