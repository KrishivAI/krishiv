//! Predicate push-down logical optimizer rule.

use crate::{LogicalPlan, NodeOp};

use super::OptimizerRule;

/// Push `Filter` predicates down into `TableScan` nodes.
///
/// Walks the logical plan looking for `Filter` nodes and decomposes each
/// filter's predicate into AND-conjuncts. Conjuncts that reference only
/// columns present in one scan's output schema are pushed into that scan
/// node's `filters` list. If all conjuncts are pushed the `Filter` node is
/// removed; remaining cross-join conjuncts stay in place.
///
/// Two patterns are handled:
/// - **Filter-above-Scan**: filter's direct input is a scan.
/// - **Filter-above-Join**: filter sits above a join; each conjunct is tested
///   against the left and right scan inputs independently and pushed as far
///   down as it can go. Cross-join predicates (referencing both sides) remain
///   in the filter.
pub struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "predicate-pushdown"
    }

    fn apply(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let nodes = plan.nodes().to_vec();
        let id_to_idx: std::collections::HashMap<&str, usize> =
            nodes.iter().enumerate().map(|(i, n)| (n.id(), i)).collect();

        // Collect pushdown candidates: filter nodes whose input is a scan.
        struct FilterPushdown {
            filter_idx: usize,
            scan_pushes: Vec<(usize, Vec<String>)>,
            remaining: Vec<String>,
        }

        let mut pushdowns: Vec<FilterPushdown> = Vec::new();

        for (i, node) in nodes.iter().enumerate() {
            let predicate = match node.op() {
                Some(NodeOp::Filter { predicate }) => predicate.clone(),
                _ => continue,
            };

            // Collect all scan nodes reachable in one or two hops from this
            // filter. One hop covers Filter-above-Scan; two hops covers
            // Filter-above-Join-above-Scan so each side of the join can
            // independently receive the conjuncts that belong to it.
            let direct_inputs: Vec<usize> = node
                .inputs()
                .iter()
                .filter_map(|input_id| id_to_idx.get(input_id.as_str()).copied())
                .collect();

            let mut scan_indices: Vec<usize> = direct_inputs
                .iter()
                .copied()
                .filter(|&idx| nodes.get(idx).is_some_and(|n| matches!(n.op(), Some(NodeOp::Scan { .. }))))
                .collect();

            // Filter-above-Join: descend through join nodes to collect
            // both left and right scan inputs for per-side pushdown.
            for join_idx in direct_inputs.iter().copied().filter(|&idx| {
                nodes.get(idx).is_some_and(|n| matches!(n.op(), Some(NodeOp::Join { join_type: crate::JoinType::Inner })))
            }) {
                let join_inputs: Vec<String> = nodes.get(join_idx).map(|n| n.inputs().to_vec()).unwrap_or_default();
                for child_id in &join_inputs {
                    if let Some(&child_idx) = id_to_idx.get(child_id.as_str())
                        && nodes.get(child_idx).is_some_and(|n| matches!(n.op(), Some(NodeOp::Scan { .. })))
                    {
                        scan_indices.push(child_idx);
                    }
                }
            }
            scan_indices.sort_unstable();
            scan_indices.dedup();

            if scan_indices.is_empty() {
                continue;
            }

            // C5: Use sqlparser to split predicate conjuncts properly
            // instead of naively splitting on the literal string " AND ".
            let conjuncts = split_predicate_conjuncts(&predicate);

            if conjuncts.is_empty() {
                continue;
            }

            let scan_contracts = scan_indices
                .iter()
                .filter_map(|&scan_idx| {
                    let scan_node = nodes.get(scan_idx)?;
                    let columns = scan_node
                        .output_schema()
                        .fields()
                        .iter()
                        .map(|field| field.name())
                        .collect::<Vec<_>>();
                    let table = match scan_node.op() {
                        Some(NodeOp::Scan { table, .. }) => table.as_str(),
                        _ => "",
                    };
                    Some((scan_idx, table, columns))
                })
                .collect::<Vec<_>>();
            let mut scan_pushes = std::collections::HashMap::<usize, Vec<String>>::new();
            let mut remaining = Vec::new();

            for conjunct in conjuncts {
                let columns = extract_column_refs(&conjunct);
                let matching_scans = scan_contracts
                    .iter()
                    .filter_map(|(scan_idx, table, scan_columns)| {
                        (!columns.is_empty()
                            && columns
                                .iter()
                                .all(|column| column_belongs_to_scan(column, table, scan_columns)))
                        .then_some(*scan_idx)
                    })
                    .collect::<Vec<_>>();
                if let [scan_idx] = matching_scans.as_slice() {
                    scan_pushes.entry(*scan_idx).or_default().push(conjunct);
                } else {
                    remaining.push(conjunct);
                }
            }

            if !scan_pushes.is_empty() {
                let mut scan_pushes = scan_pushes.into_iter().collect::<Vec<_>>();
                scan_pushes.sort_by_key(|(scan_idx, _)| *scan_idx);
                pushdowns.push(FilterPushdown {
                    filter_idx: i,
                    scan_pushes,
                    remaining,
                });
            }
        }

        if pushdowns.is_empty() {
            return None;
        }

        let mut new_nodes = nodes.clone();
        let mut to_remove: Vec<usize> = Vec::new();

        for pd in &pushdowns {
            for (scan_idx, pushable) in &pd.scan_pushes {
                if let Some(node) = new_nodes.get(*scan_idx)
                    && let Some(NodeOp::Scan { table, filters }) = node.op()
                {
                    let table = table.clone();
                    let mut new_filters = filters.clone();
                    new_filters.extend(pushable.iter().cloned());
                    if let Some(n) = new_nodes.get_mut(*scan_idx) {
                        *n = n.clone().with_op(NodeOp::Scan { table, filters: new_filters });
                    }
                }
            }

            if pd.remaining.is_empty() {
                to_remove.push(pd.filter_idx);
            } else if let Some(n) = new_nodes.get(pd.filter_idx) {
                let updated = n.clone().with_op(NodeOp::Filter {
                    predicate: pd.remaining.join(" AND "),
                });
                if let Some(slot) = new_nodes.get_mut(pd.filter_idx) {
                    *slot = updated;
                }
            }
        }

        // Remove filter nodes and rewire downstream node inputs.
        for &idx in to_remove.iter().rev() {
            let (filter_id, filter_inputs) = new_nodes.get(idx).map(|n| (n.id().to_string(), n.inputs().to_vec())).unwrap_or_default();
            new_nodes.remove(idx);

            for node in &mut new_nodes {
                let inputs: Vec<String> = node.inputs().to_vec();
                if inputs.contains(&filter_id) {
                    let new_inputs: Vec<String> = inputs
                        .iter()
                        .flat_map(|input| {
                            if input == &filter_id {
                                filter_inputs.clone()
                            } else {
                                vec![input.clone()]
                            }
                        })
                        .collect();
                    *node = node.clone().with_inputs(new_inputs);
                }
            }
        }

        let mut out = LogicalPlan::new(plan.name(), plan.kind());
        for node in new_nodes {
            out.add_node(node);
        }
        Some(out)
    }
}

/// Extract likely column-name identifiers from a predicate expression string.
///
/// Skips string literals and function names, retaining unquoted and quoted
/// identifier paths such as `column` and `table.column`.
pub(super) fn extract_column_refs(predicate: &str) -> Vec<String> {
    const SQL_KEYWORDS: &[&str] = &[
        "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE", "WHERE", "SELECT", "FROM", "AS",
        "ON", "BETWEEN", "LIKE", "EXISTS", "HAVING", "GROUP", "ORDER", "BY", "ASC", "DESC",
        "LIMIT", "OFFSET", "DISTINCT", "ALL", "ANY", "SOME", "CASE", "WHEN", "THEN", "ELSE", "END",
        "CAST",
    ];

    let chars = predicate.char_indices().collect::<Vec<_>>();
    let mut refs = Vec::new();
    let mut cursor = 0usize;
    while cursor < chars.len() {
        let Some(&(_, ch)) = chars.get(cursor) else { break; };
        if ch == '\'' {
            cursor += 1;
            while cursor < chars.len() {
                if chars.get(cursor).is_some_and(|(_, c)| *c == '\'') {
                    if cursor + 1 < chars.len() && chars.get(cursor + 1).is_some_and(|(_, c)| *c == '\'') {
                        cursor += 2;
                        continue;
                    }
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
            continue;
        }
        if ch == '"' || ch == '`' {
            let quote = ch;
            let start = chars.get(cursor).map_or(predicate.len(), |(o, _)| *o + ch.len_utf8());
            cursor += 1;
            while cursor < chars.len() && chars.get(cursor).is_none_or(|(_, c)| *c != quote) {
                cursor += 1;
            }
            let end = chars
                .get(cursor)
                .map_or(predicate.len(), |(offset, _)| *offset);
            if end > start {
                refs.push(predicate.get(start..end).unwrap_or("").to_string());
            }
            cursor = cursor.saturating_add(1);
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = chars.get(cursor).map_or(predicate.len(), |(o, _)| *o);
            cursor += 1;
            while cursor < chars.len()
                && chars.get(cursor).is_some_and(|(_, c)| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            {
                cursor += 1;
            }
            let end = chars
                .get(cursor)
                .map_or(predicate.len(), |(offset, _)| *offset);
            let token = predicate.get(start..end).unwrap_or("");
            let next_non_whitespace = chars.get(cursor..).unwrap_or(&[])
                .iter()
                .find_map(|(_, next)| (!next.is_whitespace()).then_some(*next));
            if next_non_whitespace != Some('(')
                && !SQL_KEYWORDS.contains(&token.to_uppercase().as_str())
                && !refs.iter().any(|existing| existing == token)
            {
                refs.push(token.to_string());
            }
            continue;
        }
        cursor += 1;
    }
    refs
}

/// C5: Split a SQL predicate string into conjuncts using sqlparser for correct
/// AND splitting.  Respects quoted strings, nested expressions, etc.
pub(super) fn split_predicate_conjuncts(predicate: &str) -> Vec<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let expression = predicate
        .strip_prefix("WHERE ")
        .or_else(|| predicate.strip_prefix("where "))
        .unwrap_or(predicate);
    let statement = format!("SELECT * FROM __krishiv_predicate WHERE {expression}");
    let Ok(mut stmts) = Parser::parse_sql(&dialect, &statement) else {
        return Vec::new();
    };
    let Some(stmt) = stmts.pop() else {
        return vec![predicate.to_string()];
    };
    // Extract the expression and split on top-level AND.
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return vec![predicate.to_string()];
    };
    let Some(select_body) = query.body.as_select() else {
        return vec![predicate.to_string()];
    };
    let Some(selection) = &select_body.selection else {
        return vec![predicate.to_string()];
    };
    collect_binary_conjuncts(selection, "AND")
}

/// Recursively collect top-level conjuncts from a binary expression tree.
pub(super) fn collect_binary_conjuncts(expr: &sqlparser::ast::Expr, op: &str) -> Vec<String> {
    match expr {
        sqlparser::ast::Expr::BinaryOp {
            left,
            op: bin_op,
            right,
        } if bin_op.to_string().to_uppercase() == op => {
            let mut left_conjuncts = collect_binary_conjuncts(left, op);
            let right_conjuncts = collect_binary_conjuncts(right, op);
            left_conjuncts.extend(right_conjuncts);
            left_conjuncts
        }
        other => vec![other.to_string()],
    }
}

/// Check whether `col` (possibly qualified like `"t.id"`) belongs to `scan_table`
/// with the given column names.  C5: When a column reference has an explicit
/// qualifier, require an exact case-insensitive table match. Aliases are not
/// represented in `PlanNode`, so guessing them would permit unsafe pushdown.
pub(super) fn column_belongs_to_scan(col: &str, scan_table: &str, scan_columns: &[&str]) -> bool {
    if let Some(dot_pos) = col.rfind('.') {
        let qualifier = &col[..dot_pos];
        let unqualified = &col[dot_pos + 1..];
        if !qualifier.is_empty() {
            let scan_lower = scan_table.to_ascii_lowercase();
            let qual_lower = qualifier.to_ascii_lowercase();
            if qual_lower == scan_lower {
                return scan_columns.contains(&unqualified);
            }
            // Reject qualification that doesn't match this table at all.
            return false;
        }
        return scan_columns.contains(&unqualified);
    }
    scan_columns.contains(&col)
}
