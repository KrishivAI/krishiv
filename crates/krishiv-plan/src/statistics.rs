//! Column and table-level statistics for cardinality estimation.
//!
//! These types capture per-column and per-table statistics collected from data
//! source metadata (Parquet row-group footers, catalog entries, etc.) and use
//! them to drive [`CardinalityEstimator`], which annotates a [`LogicalPlan`]
//! with estimated row counts so that join-ordering and broadcast rules can make
//! better decisions without runtime feedback.

use std::collections::HashMap;

use crate::{JoinType, LogicalPlan, NodeOp, PlanNode};

// ── ColumnStats ───────────────────────────────────────────────────────────────

/// Per-column statistics sourced from data source metadata (e.g. Parquet footer).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats {
    /// Total number of rows in this column's source.
    pub row_count: u64,
    /// Fraction of rows that are `NULL`. Range: `[0.0, 1.0]`.
    pub null_fraction: f64,
    /// Estimated number of distinct non-null values (NDV).
    pub distinct_count_estimate: u64,
    /// Serialised minimum value, if available (as a string for type-agnostic storage).
    pub min_value: Option<String>,
    /// Serialised maximum value, if available.
    pub max_value: Option<String>,
}

impl ColumnStats {
    /// Create minimal stats with only a row count.
    ///
    /// Null fraction defaults to `0.0` and distinct count equals `row_count`.
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            null_fraction: 0.0,
            distinct_count_estimate: row_count.max(1),
            min_value: None,
            max_value: None,
        }
    }

    /// Override the null fraction. Clamped to `[0.0, 1.0]`.
    #[must_use]
    pub fn with_null_fraction(mut self, fraction: f64) -> Self {
        self.null_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Set the estimated distinct value count.
    #[must_use]
    pub fn with_distinct_count(mut self, count: u64) -> Self {
        self.distinct_count_estimate = count.max(1);
        self
    }

    /// Attach the serialised min/max range.
    #[must_use]
    pub fn with_range(mut self, min: impl Into<String>, max: impl Into<String>) -> Self {
        self.min_value = Some(min.into());
        self.max_value = Some(max.into());
        self
    }

    /// Selectivity of a single-value equality predicate (`col = value`).
    ///
    /// Uses `1 / NDV`. Returns `1.0` when no distinct count is available.
    pub fn equality_selectivity(&self) -> f64 {
        if self.distinct_count_estimate == 0 {
            return 1.0;
        }
        1.0 / self.distinct_count_estimate as f64
    }

    /// Selectivity of a range predicate (`col < value`, `col BETWEEN …`).
    ///
    /// Without a histogram, defaults to the classic `1/3` heuristic.
    pub fn range_selectivity(&self) -> f64 {
        1.0 / 3.0
    }
}

// ── TableStats ────────────────────────────────────────────────────────────────

/// Table-level statistics: total row count plus optional per-column detail.
#[derive(Debug, Clone, PartialEq)]
pub struct TableStats {
    /// Total number of rows in the table.
    pub row_count: u64,
    /// Per-column statistics keyed by column name (lower-case, unqualified).
    columns: HashMap<String, ColumnStats>,
}

impl TableStats {
    /// Create table stats with a known row count and no column-level detail.
    pub fn new(row_count: u64) -> Self {
        Self {
            row_count,
            columns: HashMap::new(),
        }
    }

    /// Attach column-level statistics.
    #[must_use]
    pub fn with_column(mut self, name: impl Into<String>, stats: ColumnStats) -> Self {
        self.columns.insert(name.into().to_lowercase(), stats);
        self
    }

    /// Return the stats for a named column, if available.
    pub fn column(&self, name: &str) -> Option<&ColumnStats> {
        self.columns.get(&name.to_lowercase())
    }

    /// Estimate the output row count after applying `filters`.
    ///
    /// Each filter is a string predicate of the form `column OP value`
    /// (e.g. `"amount > 100"`, `"status = 'active'"`, `"id IS NULL"`).
    /// Predicates that cannot be matched to a known column fall back to
    /// a conservative 0.5 selectivity. Multiple predicates are assumed
    /// independent and their selectivities are multiplied together.
    pub fn estimate_after_filters(&self, filters: &[String]) -> u64 {
        if filters.is_empty() {
            return self.row_count;
        }
        let mut selectivity: f64 = 1.0;
        for pred in filters {
            selectivity *= self.predicate_selectivity(pred);
        }
        (self.row_count as f64 * selectivity).ceil() as u64
    }

    fn predicate_selectivity(&self, predicate: &str) -> f64 {
        let pred = predicate.trim();
        let upper = pred.to_uppercase();

        if upper.contains("IS NULL") {
            if let Some(col) = pred.split_whitespace().next() {
                let col = col.trim_matches(|c: char| c == '"' || c == '`' || c == '\'');
                let col = col.rsplit('.').next().unwrap_or(col);
                if let Some(stats) = self.columns.get(&col.to_lowercase()) {
                    return stats.null_fraction.max(0.001);
                }
            }
            return 0.01;
        }

        if upper.contains("IS NOT NULL") {
            if let Some(col) = pred.split_whitespace().next() {
                let col = col.trim_matches(|c: char| c == '"' || c == '`' || c == '\'');
                let col = col.rsplit('.').next().unwrap_or(col);
                if let Some(stats) = self.columns.get(&col.to_lowercase()) {
                    return (1.0 - stats.null_fraction).max(0.001);
                }
            }
            return 0.99;
        }

        // Equality: "col = value" or "col = ?"
        // Match first `=` not preceded by `!`, `<`, or `>`
        if let Some(eq_col) = find_eq_column(pred) {
            return self
                .columns
                .get(&eq_col)
                .map(|c| c.equality_selectivity())
                .unwrap_or(0.1);
        }

        // Range: "col < value", "col > value", "col <= value", "col >= value"
        if pred.contains('<') || pred.contains('>') {
            let col = pred
                .split(|c: char| c == '<' || c == '>')
                .next()
                .unwrap_or("")
                .trim();
            let col = col.rsplit('.').next().unwrap_or(col);
            let col = col.trim_matches(|c: char| c == '"' || c == '`' || c == '\'');
            return self
                .columns
                .get(&col.to_lowercase())
                .map(|c| c.range_selectivity())
                .unwrap_or(1.0 / 3.0);
        }

        // Unknown predicate — conservative 50 % selectivity
        0.5
    }
}

/// Find the column name in an equality predicate `col = value`.
///
/// Returns `None` if the first `=` is `!=`, `<=`, or `>=`.
fn find_eq_column(pred: &str) -> Option<String> {
    let bytes = pred.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'=' {
            // Skip compound operators
            if i > 0 && matches!(bytes[i - 1], b'!' | b'<' | b'>') {
                continue;
            }
            // Skip `==` (treat as equality too, but skip the double `=` case)
            let col_raw = pred[..i].trim();
            let col = col_raw.rsplit('.').next().unwrap_or(col_raw);
            let col = col.trim_matches(|c: char| c == '"' || c == '`' || c == '\'');
            return Some(col.to_lowercase());
        }
    }
    None
}

// ── CardinalityEstimator ──────────────────────────────────────────────────────

/// Annotates a [`LogicalPlan`] with `estimated_rows` using table statistics.
///
/// The estimator walks plan nodes in topological order (inputs before consumers)
/// and fills in `estimated_rows` for any node that does not already have one.
/// Pre-existing estimates set by the query planner (e.g. from Parquet footer
/// statistics) are respected and not overridden.
///
/// ## Join cardinality heuristics
///
/// | Join type | Estimate |
/// |-----------|---------|
/// | Inner     | `sqrt(left × right)`, capped at `min(left, right)` |
/// | Left      | `left` |
/// | Right     | `right` |
/// | Full      | `left + right` |
/// | Semi      | `min(left, right)` |
/// | Anti      | `left` |
/// | Cross     | `left × right` |
/// | NestedLoop| same as Inner |
pub struct CardinalityEstimator {
    table_stats: HashMap<String, TableStats>,
}

impl CardinalityEstimator {
    /// Create an estimator with no table statistics.
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    /// Register statistics for a table.
    #[must_use]
    pub fn with_table(mut self, table: impl Into<String>, stats: TableStats) -> Self {
        self.table_stats.insert(table.into(), stats);
        self
    }

    /// Return the registered stats for a table, if available.
    pub fn table_stats(&self, table: &str) -> Option<&TableStats> {
        self.table_stats.get(table)
    }

    /// Estimate the output row count for a scan of `table` with optional filters.
    ///
    /// Returns `None` when no statistics are registered for the table.
    pub fn estimate_scan(&self, table: &str, filters: &[String]) -> Option<u64> {
        let stats = self.table_stats.get(table)?;
        Some(stats.estimate_after_filters(filters))
    }

    /// Estimate join output cardinality from input sizes and join type.
    pub fn estimate_join(&self, left_rows: u64, right_rows: u64, join_type: &JoinType) -> u64 {
        match join_type {
            JoinType::Inner | JoinType::NestedLoop => {
                // Geometric mean heuristic: avoids the full cross-product over-estimate
                // while producing an output larger than either input alone for small tables.
                let product = (left_rows as f64 * right_rows as f64).sqrt().ceil() as u64;
                product.max(1).min(left_rows.saturating_add(right_rows))
            }
            JoinType::Left => left_rows,
            JoinType::Right => right_rows,
            JoinType::Full => left_rows.saturating_add(right_rows),
            JoinType::Semi => left_rows.min(right_rows),
            JoinType::Anti => left_rows,
            JoinType::Cross => left_rows.saturating_mul(right_rows),
        }
    }

    /// Walk `plan` and fill in `estimated_rows` on every node that lacks one.
    ///
    /// Nodes with pre-existing `estimated_rows` are left unchanged and their
    /// estimates propagate downstream to feed into derived estimates.
    ///
    /// Returns a new plan with updated estimates, or the original plan if no
    /// node needed annotation.
    pub fn annotate_plan(&self, plan: LogicalPlan) -> LogicalPlan {
        let nodes = plan.nodes();
        let mut row_map: HashMap<String, u64> = HashMap::with_capacity(nodes.len());
        let mut new_nodes: Vec<PlanNode> = Vec::with_capacity(nodes.len());
        let mut changed = false;

        for node in nodes {
            let rows = self.estimate_node(node, &row_map);

            if let Some(rows) = rows {
                row_map.insert(node.id().to_string(), rows);
                if node.estimated_rows() != Some(rows) {
                    changed = true;
                    new_nodes.push(node.clone().with_estimated_rows(Some(rows)));
                } else {
                    new_nodes.push(node.clone());
                }
            } else {
                if let Some(existing) = node.estimated_rows() {
                    row_map.insert(node.id().to_string(), existing);
                }
                new_nodes.push(node.clone());
            }
        }

        if !changed {
            return plan;
        }

        let mut new_plan = LogicalPlan::new(plan.name(), plan.kind());
        for n in new_nodes {
            new_plan = new_plan.with_node(n);
        }
        new_plan
    }

    fn estimate_node(&self, node: &PlanNode, row_map: &HashMap<String, u64>) -> Option<u64> {
        // Respect pre-existing estimates — only fill in what's missing.
        if let Some(existing) = node.estimated_rows() {
            return Some(existing);
        }

        match node.op() {
            Some(NodeOp::Scan { table, filters }) => {
                self.estimate_scan(table, filters).or(Some(DEFAULT_ROWS))
            }

            Some(NodeOp::Filter { .. }) => {
                // Default 50 % selectivity without column-level detail.
                let input_rows = first_input_rows(node, row_map).unwrap_or(DEFAULT_ROWS);
                Some((input_rows as f64 * 0.5).ceil() as u64)
            }

            Some(NodeOp::Join { join_type }) => {
                let inputs = node.inputs();
                if inputs.len() == 2 {
                    let left = row_map.get(&inputs[0]).copied().unwrap_or(DEFAULT_ROWS);
                    let right = row_map.get(&inputs[1]).copied().unwrap_or(DEFAULT_ROWS);
                    Some(self.estimate_join(left, right, join_type))
                } else {
                    None
                }
            }

            Some(NodeOp::SortMergeJoin { join_type, .. }) => {
                let inputs = node.inputs();
                if inputs.len() == 2 {
                    let left = row_map.get(&inputs[0]).copied().unwrap_or(DEFAULT_ROWS);
                    let right = row_map.get(&inputs[1]).copied().unwrap_or(DEFAULT_ROWS);
                    Some(self.estimate_join(left, right, join_type))
                } else {
                    None
                }
            }

            Some(NodeOp::Aggregate { group_keys }) => {
                let input_rows = first_input_rows(node, row_map).unwrap_or(DEFAULT_ROWS);
                let rows = if group_keys.is_empty() {
                    1
                } else {
                    // Conservative: aggregation reduces to ~10 % of input.
                    (input_rows / 10).max(1)
                };
                Some(rows)
            }

            Some(
                NodeOp::Project { .. }
                | NodeOp::Exchange { .. }
                | NodeOp::CoalescePartitions { .. }
                | NodeOp::GlobalSort { .. },
            ) => first_input_rows(node, row_map),

            Some(NodeOp::Unnest { .. }) => {
                // Conservative: each row expands to ~5 elements on average.
                let input_rows = first_input_rows(node, row_map).unwrap_or(DEFAULT_ROWS);
                Some(input_rows.saturating_mul(5))
            }

            _ => None,
        }
    }
}

impl Default for CardinalityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_ROWS: u64 = 10_000;

fn first_input_rows(node: &PlanNode, row_map: &HashMap<String, u64>) -> Option<u64> {
    node.inputs()
        .first()
        .and_then(|id| row_map.get(id))
        .copied()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionKind, LogicalPlan, NodeOp, PlanNode};

    fn make_scan(id: &str, table: &str, rows: u64) -> PlanNode {
        PlanNode::new(id, format!("scan {table}"), ExecutionKind::Batch)
            .with_op(NodeOp::Scan {
                table: table.to_string(),
                filters: vec![],
            })
            .with_estimated_rows(Some(rows))
    }

    fn make_scan_no_estimate(id: &str, table: &str) -> PlanNode {
        PlanNode::new(id, format!("scan {table}"), ExecutionKind::Batch).with_op(NodeOp::Scan {
            table: table.to_string(),
            filters: vec![],
        })
    }

    fn make_join(id: &str, left: &str, right: &str) -> PlanNode {
        PlanNode::new(id, "join", ExecutionKind::Batch)
            .with_inputs([left, right])
            .with_op(NodeOp::Join {
                join_type: JoinType::Inner,
            })
    }

    // ── ColumnStats ───────────────────────────────────────────────────────────

    #[test]
    fn column_stats_new_defaults() {
        let cs = ColumnStats::new(1000);
        assert_eq!(cs.row_count, 1000);
        assert_eq!(cs.null_fraction, 0.0);
        assert_eq!(cs.distinct_count_estimate, 1000);
        assert!(cs.min_value.is_none());
        assert!(cs.max_value.is_none());
    }

    #[test]
    fn column_stats_equality_selectivity() {
        let cs = ColumnStats::new(1000).with_distinct_count(100);
        assert!((cs.equality_selectivity() - 0.01).abs() < 1e-9);
    }

    #[test]
    fn column_stats_equality_selectivity_zero_ndv() {
        let mut cs = ColumnStats::new(0);
        cs.distinct_count_estimate = 0;
        assert_eq!(cs.equality_selectivity(), 1.0);
    }

    #[test]
    fn column_stats_range_selectivity_is_one_third() {
        let cs = ColumnStats::new(1000);
        assert!((cs.range_selectivity() - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn column_stats_null_fraction_clamped() {
        let cs = ColumnStats::new(100).with_null_fraction(2.5);
        assert_eq!(cs.null_fraction, 1.0);
        let cs2 = ColumnStats::new(100).with_null_fraction(-0.5);
        assert_eq!(cs2.null_fraction, 0.0);
    }

    // ── TableStats ────────────────────────────────────────────────────────────

    #[test]
    fn table_stats_no_filters_returns_row_count() {
        let ts = TableStats::new(5000);
        assert_eq!(ts.estimate_after_filters(&[]), 5000);
    }

    #[test]
    fn table_stats_equality_filter_with_column() {
        let ts = TableStats::new(1000).with_column(
            "status",
            ColumnStats::new(1000).with_distinct_count(10),
        );
        let rows = ts.estimate_after_filters(&["status = 'active'".to_string()]);
        // selectivity = 1/10 = 0.1 → 100 rows
        assert_eq!(rows, 100);
    }

    #[test]
    fn table_stats_range_filter_fallback() {
        let ts = TableStats::new(900);
        let rows = ts.estimate_after_filters(&["amount > 100".to_string()]);
        // selectivity = 1/3 → ceil(300) = 300
        assert_eq!(rows, 300);
    }

    #[test]
    fn table_stats_unknown_predicate_uses_half_selectivity() {
        let ts = TableStats::new(1000);
        let rows = ts.estimate_after_filters(&["LIKE '%foo%'".to_string()]);
        assert_eq!(rows, 500);
    }

    #[test]
    fn table_stats_column_lookup_case_insensitive() {
        let ts = TableStats::new(100).with_column(
            "Amount",
            ColumnStats::new(100).with_distinct_count(50),
        );
        assert!(ts.column("amount").is_some());
        assert!(ts.column("AMOUNT").is_some());
    }

    // ── CardinalityEstimator ──────────────────────────────────────────────────

    #[test]
    fn estimator_scan_from_table_stats() {
        let est = CardinalityEstimator::new()
            .with_table("orders", TableStats::new(10_000));
        assert_eq!(est.estimate_scan("orders", &[]), Some(10_000));
    }

    #[test]
    fn estimator_scan_unknown_table_returns_none() {
        let est = CardinalityEstimator::new();
        assert_eq!(est.estimate_scan("unknown", &[]), None);
    }

    #[test]
    fn estimator_estimate_inner_join() {
        let est = CardinalityEstimator::new();
        // sqrt(100 * 1000) = ~316
        let rows = est.estimate_join(100, 1000, &JoinType::Inner);
        assert!(rows > 1 && rows <= 1100, "inner join estimate={rows}");
    }

    #[test]
    fn estimator_estimate_left_join_preserves_left() {
        let est = CardinalityEstimator::new();
        assert_eq!(est.estimate_join(500, 100, &JoinType::Left), 500);
    }

    #[test]
    fn estimator_estimate_cross_join() {
        let est = CardinalityEstimator::new();
        assert_eq!(est.estimate_join(10, 20, &JoinType::Cross), 200);
    }

    #[test]
    fn estimator_annotate_plan_fills_missing_scan_estimate() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan_no_estimate("s", "orders"));

        let est = CardinalityEstimator::new()
            .with_table("orders", TableStats::new(50_000));
        let annotated = est.annotate_plan(plan);
        let scan = annotated.nodes().iter().find(|n| n.id() == "s").unwrap();
        assert_eq!(scan.estimated_rows(), Some(50_000));
    }

    #[test]
    fn estimator_annotate_plan_respects_existing_estimate() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan("s", "orders", 999));

        let est = CardinalityEstimator::new()
            .with_table("orders", TableStats::new(50_000));
        let annotated = est.annotate_plan(plan.clone());
        // Pre-existing estimate must not be replaced.
        let scan = annotated.nodes().iter().find(|n| n.id() == "s").unwrap();
        assert_eq!(scan.estimated_rows(), Some(999));
    }

    #[test]
    fn estimator_annotate_plan_propagates_through_join() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan("a", "t_a", 100))
            .with_node(make_scan("b", "t_b", 1000))
            .with_node(make_join("j", "a", "b"));

        let est = CardinalityEstimator::new();
        let annotated = est.annotate_plan(plan);
        let join_node = annotated.nodes().iter().find(|n| n.id() == "j").unwrap();
        // Inner join estimate is populated.
        assert!(join_node.estimated_rows().is_some());
    }

    #[test]
    fn estimator_annotate_plan_noop_when_all_estimates_present() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan("s", "t", 100));

        let est = CardinalityEstimator::new();
        let annotated = est.annotate_plan(plan.clone());
        // Plan should be structurally identical — same estimated_rows value.
        assert_eq!(annotated.nodes()[0].estimated_rows(), Some(100));
    }

    #[test]
    fn estimator_aggregate_global_estimates_one_row() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan("s", "t", 10_000))
            .with_node(
                PlanNode::new("agg", "aggregate", ExecutionKind::Batch)
                    .with_inputs(["s"])
                    .with_op(NodeOp::Aggregate { group_keys: vec![] }),
            );

        let est = CardinalityEstimator::new();
        let annotated = est.annotate_plan(plan);
        let agg = annotated.nodes().iter().find(|n| n.id() == "agg").unwrap();
        assert_eq!(agg.estimated_rows(), Some(1));
    }

    #[test]
    fn estimator_aggregate_grouped_estimates_reduction() {
        let plan = LogicalPlan::new("q", ExecutionKind::Batch)
            .with_node(make_scan("s", "t", 1000))
            .with_node(
                PlanNode::new("agg", "aggregate", ExecutionKind::Batch)
                    .with_inputs(["s"])
                    .with_op(NodeOp::Aggregate {
                        group_keys: vec!["city".to_string()],
                    }),
            );

        let est = CardinalityEstimator::new();
        let annotated = est.annotate_plan(plan);
        let agg = annotated.nodes().iter().find(|n| n.id() == "agg").unwrap();
        // 10 % of 1000 = 100
        assert_eq!(agg.estimated_rows(), Some(100));
    }
}
