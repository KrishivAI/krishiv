//! WINDOW-1: rewrite `TUMBLE(TABLE t, DESCRIPTOR(col), size)` into standard
//! SQL a batch planner accepts.
//!
//! A tumbling window over a **materialized view** is not a streaming trigger —
//! it is a grouping key. `window_start` is `col - col % size`, a plain
//! derived column, so the whole TVF rewrites to a derived table:
//!
//! ```sql
//! FROM (SELECT *, col - col % size          AS window_start,
//!               col - col % size + size     AS window_end
//!       FROM t) AS t
//! ```
//!
//! after which the ordinary machinery takes over — the computed columns become
//! a map hop, `window_start` a plain group key, and the view maintains O(Δ)
//! with late rows updating their window (which is exactly what a materialized
//! view should do; emission-on-watermark is the STREAMING engine's contract,
//! not this one's).
//!
//! Deliberately narrow: `TUMBLE` only, integer window sizes only (an integer
//! event-time column is what the rewrite's arithmetic assumes; a real
//! timestamp column fails to plan and the view stays exactly as unplannable
//! as it was). `HOP` fans each row across several windows — a 1:N rewrite
//! this cannot express — and `SESSION` windows are stateful merges; both are
//! left untouched, as is `PROCTIME()`, which has no meaning in delta-batch.

/// Rewrite every integer-size `TUMBLE(TABLE …, DESCRIPTOR(…), n)` in `sql`.
/// Returns `None` when there is nothing to rewrite (the common case, kept
/// allocation-free for every ordinary view).
pub fn rewrite_tumble_tvfs(sql: &str) -> Option<String> {
    let mut current = sql.to_owned();
    let mut rewrote = false;
    // Bounded: malformed input must not loop forever.
    for _ in 0..16 {
        let Some((start, end, table, column, size)) = find_tumble(&current) else {
            break;
        };
        let replacement = format!(
            "(SELECT *, {column} - {column} % {size} AS window_start, \
             {column} - {column} % {size} + {size} AS window_end FROM {table}) AS {alias}",
            alias = alias_of(&table),
        );
        let (Some(before), Some(after)) = (current.get(..start), current.get(end..)) else {
            break;
        };
        let mut next = before.to_owned();
        next.push_str(&replacement);
        next.push_str(after);
        current = next;
        rewrote = true;
    }
    rewrote.then_some(current)
}

/// The alias the derived table wears: the table's bare (unqualified) name, so
/// existing qualified references in the query keep resolving.
fn alias_of(table: &str) -> &str {
    table.rsplit('.').next().unwrap_or(table)
}

/// Find the next `TUMBLE(TABLE <t>, DESCRIPTOR(<c>), <int>)`, returning
/// `(start, end, table, column, size)`. Case-insensitive on keywords;
/// identifiers are taken verbatim (quoting preserved).
fn find_tumble(sql: &str) -> Option<(usize, usize, String, String, u64)> {
    find_tvf(sql, "TUMBLE").and_then(|(start, end, args)| {
        parse_tumble_args(&args).map(|(t, c, n)| (start, end, t, c, n))
    })
}

/// HOP-1: rewrite every integer `HOP(TABLE t, DESCRIPTOR(col), slide, size)`
/// into a UNION ALL of `size / slide` phase-shifted TUMBLE-style derived
/// tables — a row at time `t` belongs to exactly the windows starting at
/// `t - t % slide - k*slide` for `k in 0..size/slide` (each start is ≤ t and
/// `t - start = t % slide + k*slide < size`), so the union IS the hopping
/// window relation with no filter and no duplicates. Union-all is linear over
/// Z-sets, which is what lets the fan-out maintain O(Δ) as a FlatMap.
///
/// Deliberately narrow, like TUMBLE: integer slide/size only, and `size`
/// must be a positive multiple of `slide` (a non-multiple hop needs a window
/// calendar, not a phase shift). Anything else is left untouched — the query
/// stays as unplannable as it was, which is honest.
pub fn rewrite_hop_tvfs(sql: &str) -> Option<String> {
    let mut current = sql.to_owned();
    let mut rewrote = false;
    for _ in 0..16 {
        let Some((start, end, args)) = find_tvf(&current, "HOP") else {
            break;
        };
        let Some((table, column, slide, size)) = parse_hop_args(&args) else {
            break;
        };
        let branches: Vec<String> = (0..size / slide)
            .map(|k| {
                let shift = k * slide;
                let ws = if shift == 0 {
                    format!("{column} - {column} % {slide}")
                } else {
                    format!("{column} - {column} % {slide} - {shift}")
                };
                format!("SELECT *, {ws} AS window_start, {ws} + {size} AS window_end FROM {table}")
            })
            .collect();
        let replacement = format!(
            "({}) AS {alias}",
            branches.join(" UNION ALL "),
            alias = alias_of(&table),
        );
        let (Some(before), Some(after)) = (current.get(..start), current.get(end..)) else {
            break;
        };
        let mut next = before.to_owned();
        next.push_str(&replacement);
        next.push_str(after);
        current = next;
        rewrote = true;
    }
    rewrote.then_some(current)
}

fn parse_hop_args(args: &str) -> Option<(String, String, u64, u64)> {
    let parts = split_top_level_args(args)?;
    if parts.len() != 4 {
        return None;
    }
    let (table, column) = parse_table_and_descriptor(parts.first()?, parts.get(1)?)?;
    let slide: u64 = parts.get(2)?.parse().ok()?;
    let size: u64 = parts.get(3)?.parse().ok()?;
    // A zero slide never advances; a non-multiple size has no phase-shift
    // decomposition; and an absurd fan-out (size/slide branches) would plan
    // a query no one meant — 64 copies is already generous.
    if slide == 0 || size == 0 || !size.is_multiple_of(slide) || size / slide > 64 {
        return None;
    }
    Some((table, column, slide, size))
}

/// SESSION-1: rewrite `SESSION(TABLE t, DESCRIPTOR(col), gap)` into the
/// standard-SQL sessionization cascade — LAG marks a row whose distance to
/// its key-predecessor is `>= gap` (the streaming engine's own boundary
/// convention: `event_time >= last + gap` starts a new session), a framed
/// SUM turns the marks into a per-key session ordinal, and MIN/MAX over
/// (key, ordinal) become `window_start` / `window_end = last + gap`:
///
/// ```sql
/// (SELECT *, MIN(col) OVER (PARTITION BY k, __ivm_sid) AS window_start,
///            MAX(col) OVER (PARTITION BY k, __ivm_sid) + gap AS window_end
///  FROM (SELECT *, SUM(__ivm_snew) OVER (PARTITION BY k ORDER BY col
///          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS __ivm_sid
///        FROM (SELECT *, CASE WHEN col - LAG(col) OVER (PARTITION BY k
///                ORDER BY col) >= gap THEN 1 ELSE 0 END AS __ivm_snew
///              FROM t) AS __ivm_s1) AS __ivm_s2) AS t
/// ```
///
/// The session PARTITION KEY is not in the TVF — the streaming surface
/// sessionizes per GROUP BY key — so the rewrite parses the query with the
/// TVF replaced by a placeholder and reads the outer GROUP BY: its bare
/// columns minus `window_start`/`window_end` are the keys. DataFusion
/// executes the cascade whole (the DiffBased oracle computes real
/// sessions), and the `__ivm_` marker names are what the O(Δ) recognizer
/// keys on — user SQL cannot produce them. Integer gaps only, one SESSION
/// per query, and a GROUP BY that is anything but bare columns carrying
/// both window bounds leaves the query untouched.
pub fn rewrite_session_tvfs(sql: &str) -> Option<String> {
    use sqlparser::ast::{Expr as SqlExpr, GroupByExpr, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let (start, end, args) = find_tvf(sql, "SESSION")?;
    let parts = split_top_level_args(&args)?;
    if parts.len() != 3 {
        return None;
    }
    let (table, column) = parse_table_and_descriptor(parts.first()?, parts.get(1)?)?;
    let gap: u64 = parts.get(2)?.parse().ok()?;
    if gap == 0 {
        return None;
    }
    // One SESSION only: a second TVF anywhere means this narrow rewrite does
    // not understand the query.
    if find_tvf(sql.get(end..)?, "SESSION").is_some() {
        return None;
    }

    // Parse with a placeholder standing in for the TVF to read the GROUP BY.
    let placeholder = {
        let (before, after) = (sql.get(..start)?, sql.get(end..)?);
        format!("{before}__ivm_session_ph{after}")
    };
    let statements = Parser::parse_sql(&GenericDialect {}, &placeholder).ok()?;
    let [Statement::Query(q)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    let bare_name = |e: &SqlExpr| -> Option<String> {
        match e {
            SqlExpr::Identifier(id) => Some(id.to_string()),
            SqlExpr::CompoundIdentifier(ids) => ids.last().map(|id| id.to_string()),
            _ => None,
        }
    };
    let GroupByExpr::Expressions(group_exprs, modifiers) = &sel.group_by else {
        return None;
    };
    if !modifiers.is_empty() {
        return None;
    }
    let mut keys: Vec<String> = Vec::new();
    let mut saw_start = false;
    let mut saw_end = false;
    for g in group_exprs {
        let n = bare_name(g)?;
        if n.eq_ignore_ascii_case("window_start") {
            saw_start = true;
        } else if n.eq_ignore_ascii_case("window_end") {
            saw_end = true;
        } else {
            keys.push(n);
        }
    }
    if !saw_start || !saw_end || keys.is_empty() {
        return None;
    }
    // The projection must not reference the marker columns (impossible for
    // user SQL, checked anyway so the namespace claim is enforced, not
    // assumed).
    for item in &sel.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item
            && let Some(n) = bare_name(e)
            && n.starts_with("__ivm_")
        {
            return None;
        }
    }

    let key_list = keys.join(", ");
    let cascade = format!(
        "(SELECT *, MIN({column}) OVER (PARTITION BY {key_list}, __ivm_sid) AS window_start, MAX({column}) OVER (PARTITION BY {key_list}, __ivm_sid) + {gap} AS window_end FROM (SELECT *, SUM(__ivm_snew) OVER (PARTITION BY {key_list} ORDER BY {column} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS __ivm_sid FROM (SELECT *, CASE WHEN {column} - LAG({column}) OVER (PARTITION BY {key_list} ORDER BY {column}) >= {gap} THEN 1 ELSE 0 END AS __ivm_snew FROM {table}) AS __ivm_s1) AS __ivm_s2) AS {alias}",
        alias = alias_of(&table),
    );
    let (before, after) = (sql.get(..start)?, sql.get(end..)?);
    Some(format!("{before}{cascade}{after}"))
}

/// TOPNK-1: reinterpret the STREAMING dialect's per-key ranking idiom as
/// standard SQL. The streaming surface spells "top n rows per group" as
///
/// ```sql
/// SELECT auction, bidder, price FROM <windowed>
/// GROUP BY auction, window_start, window_end ORDER BY price DESC LIMIT 10
/// ```
///
/// — which standard SQL REJECTS (`bidder`/`price` are neither grouped nor
/// aggregated), so the rewrite can never change the meaning of a query the
/// batch planner accepts: it claims only what was unplannable. The same
/// relation in standard SQL is
///
/// ```sql
/// SELECT auction, bidder, price FROM <windowed>
/// QUALIFY ROW_NUMBER() OVER (PARTITION BY auction, window_start,
///                            window_end ORDER BY price DESC) <= 10
/// ```
///
/// Deliberately narrow: the whole query must be one plain SELECT whose
/// projection, GROUP BY and ORDER BY are all bare column references, the
/// GROUP BY must carry `window_start` AND `window_end` (the marker that the
/// query came through a window TVF — this dialect exists nowhere else), the
/// LIMIT must be a positive integer, and at least one projected column must
/// be absent from the GROUP BY (the proof the query is NOT standard SQL).
/// Anything else is left untouched.
pub fn rewrite_streaming_topn(sql: &str) -> Option<String> {
    use sqlparser::ast::{
        Expr as SqlExpr, GroupByExpr, LimitClause, OrderByKind, SelectItem, SetExpr, Statement,
        Value,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    let [Statement::Query(q)] = statements.as_slice() else {
        return None;
    };
    if q.with.is_some() || q.fetch.is_some() {
        return None;
    }
    let SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    if sel.distinct.is_some()
        || sel.having.is_some()
        || sel.qualify.is_some()
        || sel.top.is_some()
        || !sel.lateral_views.is_empty()
        || !sel.named_window.is_empty()
        || !sel.cluster_by.is_empty()
        || !sel.distribute_by.is_empty()
        || !sel.sort_by.is_empty()
        || sel.from.len() != 1
    {
        return None;
    }
    // A bare (possibly qualified) column reference, by its unqualified name.
    let bare_name = |e: &SqlExpr| -> Option<String> {
        match e {
            SqlExpr::Identifier(id) => Some(id.value.clone()),
            SqlExpr::CompoundIdentifier(ids) => ids.last().map(|id| id.value.clone()),
            _ => None,
        }
    };
    let GroupByExpr::Expressions(group_exprs, modifiers) = &sel.group_by else {
        return None;
    };
    if group_exprs.is_empty() || !modifiers.is_empty() {
        return None;
    }
    let group_names = group_exprs
        .iter()
        .map(bare_name)
        .collect::<Option<Vec<_>>>()?;
    if !group_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case("window_start"))
        || !group_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case("window_end"))
    {
        return None;
    }
    let order_by = q.order_by.as_ref()?;
    let OrderByKind::Expressions(order_exprs) = &order_by.kind else {
        return None;
    };
    if order_exprs.is_empty() || order_by.interpolate.is_some() {
        return None;
    }
    for oe in order_exprs {
        bare_name(&oe.expr)?;
    }
    let LimitClause::LimitOffset {
        limit: Some(limit_expr),
        offset: None,
        limit_by,
    } = q.limit_clause.as_ref()?
    else {
        return None;
    };
    if !limit_by.is_empty() {
        return None;
    }
    let SqlExpr::Value(v) = limit_expr else {
        return None;
    };
    let Value::Number(n, _) = &v.value else {
        return None;
    };
    let k: u64 = n.parse().ok()?;
    if k == 0 {
        return None;
    }
    // Every projected item must be a bare column; at least one must be
    // OUTSIDE the GROUP BY, or the query was standard SQL all along and is
    // not this rewrite's to claim.
    let mut ungrouped = false;
    for item in &sel.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return None,
        };
        let name = bare_name(expr)?;
        if !group_names.iter().any(|g| g == &name) {
            ungrouped = true;
        }
    }
    if !ungrouped {
        return None;
    }

    let projection = sel
        .projection
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let from = sel
        .from
        .iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = sel
        .selection
        .as_ref()
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();
    let partition = group_exprs
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let ranking = order_exprs
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "SELECT {projection} FROM {from}{where_clause} QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {ranking}) <= {k}"
    ))
}

/// Find the next `<name>(...)` TVF call at a word boundary, returning
/// `(start, one_past_close, args)`.
fn find_tvf(sql: &str, name: &str) -> Option<(usize, usize, String)> {
    let upper = sql.to_uppercase();
    let bytes = sql.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = upper.get(search_from..).and_then(|s| s.find(name)) {
        let start = search_from + rel;
        search_from = start + name.len();
        // Word boundary on the left (not e.g. `my_tumble`).
        if start > 0
            && let Some(&prev) = bytes.get(start - 1)
            && ((prev as char).is_alphanumeric() || prev == b'_')
        {
            continue;
        }
        // Opening paren (allow whitespace).
        let mut i = start + name.len();
        while bytes.get(i).is_some_and(|b| (*b as char).is_whitespace()) {
            i += 1;
        }
        if bytes.get(i) != Some(&b'(') {
            continue;
        }
        // Balanced-paren scan for the argument list.
        let args_start = i + 1;
        let mut depth = 1usize;
        let mut j = args_start;
        while depth > 0 {
            match bytes.get(j) {
                Some(b'(') => depth += 1,
                Some(b')') => depth -= 1,
                Some(_) => {}
                None => return None,
            }
            j += 1;
        }
        let end = j; // one past the closing paren
        let args = sql.get(args_start..end.checked_sub(1)?)?;
        // An unsupported argument shape (interval string, PROCTIME) is left
        // in place — the query stays as unplannable as it was, which is
        // honest — so the caller decides parseability, not the finder.
        return Some((start, end, args.to_owned()));
    }
    None
}

fn parse_tumble_args(args: &str) -> Option<(String, String, u64)> {
    let parts = split_top_level_args(args)?;
    if parts.len() != 3 {
        return None;
    }
    let (table, column) = parse_table_and_descriptor(parts.first()?, parts.get(1)?)?;
    let size: u64 = parts.get(2)?.parse().ok()?;
    if size == 0 {
        return None;
    }
    Some((table, column, size))
}

/// Split on top-level commas only (DESCRIPTOR(...) contains none today,
/// but stay paren-aware anyway).
fn split_top_level_args(args: &str) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for ch in args.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                cur.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(cur.trim().to_owned());
                cur = String::new();
            }
            _ => cur.push(ch),
        }
    }
    parts.push(cur.trim().to_owned());
    Some(parts)
}

fn parse_table_and_descriptor(table_arg: &str, descriptor: &str) -> Option<(String, String)> {
    let table = table_arg
        .strip_prefix("TABLE ")
        .or_else(|| table_arg.strip_prefix("table "))?
        .trim()
        .to_owned();
    let upper = descriptor.to_uppercase();
    if !upper.starts_with("DESCRIPTOR") {
        return None;
    }
    let open = descriptor.find('(')?;
    let close = descriptor.rfind(')')?;
    let column = descriptor.get(open + 1..close)?.trim().to_owned();
    if column.is_empty() || column.to_uppercase().starts_with("PROCTIME") {
        return None;
    }
    // Quote the column unless it already is: the TVF dialect preserves
    // identifier case, but the batch planner lowercases unquoted identifiers —
    // an unquoted `dateTime` would silently become `datetime` and miss the
    // column. Quoting an already-lowercase name is a no-op.
    let column = if column.starts_with('"') {
        column
    } else {
        format!("\"{column}\"")
    };
    Some((table, column))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tumble_rewrites_to_a_derived_table() {
        let sql = "SELECT auction, COUNT(*) AS c \
                   FROM TUMBLE(TABLE bid, DESCRIPTOR(\"dateTime\"), 10000) \
                   GROUP BY auction, window_start, window_end";
        let out = rewrite_tumble_tvfs(sql).expect("rewritten");
        assert!(
            out.contains(
                "(SELECT *, \"dateTime\" - \"dateTime\" % 10000 AS window_start, \
                 \"dateTime\" - \"dateTime\" % 10000 + 10000 AS window_end FROM bid) AS bid"
            ),
            "{out}"
        );
        assert!(out.contains("GROUP BY auction, window_start, window_end"));
    }

    #[test]
    fn plain_sql_is_untouched() {
        assert!(rewrite_tumble_tvfs("SELECT a FROM t WHERE tumbler = 1").is_none());
    }

    #[test]
    fn hop_session_and_proctime_are_left_alone() {
        assert!(
            rewrite_tumble_tvfs("SELECT k FROM HOP(TABLE t, DESCRIPTOR(ts), 2000, 10000)")
                .is_none()
        );
        assert!(
            rewrite_tumble_tvfs("SELECT k FROM SESSION(TABLE t, DESCRIPTOR(ts), 10000)").is_none()
        );
        assert!(rewrite_tumble_tvfs("SELECT k FROM TUMBLE(TABLE t, PROCTIME(), 60000)").is_none());
        assert!(
            rewrite_tumble_tvfs("SELECT k FROM TUMBLE(TABLE t, DESCRIPTOR(ts), '1 minute')")
                .is_none()
        );
    }

    #[test]
    fn an_interval_string_size_is_refused_not_mangled() {
        let sql = "SELECT k FROM TUMBLE(TABLE t, DESCRIPTOR(ts), '10 seconds') GROUP BY k";
        assert!(rewrite_tumble_tvfs(sql).is_none());
    }

    #[test]
    fn hop_fans_into_phase_shifted_union_branches() {
        let sql = "SELECT auction, COUNT(*) AS c \
                   FROM HOP(TABLE bid, DESCRIPTOR(\"dateTime\"), 2000, 10000) \
                   GROUP BY auction, window_start, window_end";
        let out = rewrite_hop_tvfs(sql).expect("rewrites");
        assert_eq!(out.matches("UNION ALL").count(), 4, "5 branches: {out}");
        assert!(out.contains("\"dateTime\" - \"dateTime\" % 2000 AS window_start"));
        assert!(out.contains("\"dateTime\" - \"dateTime\" % 2000 - 8000 AS window_start"));
        assert!(out.contains("- 8000 + 10000 AS window_end"));
        assert!(out.contains(") AS bid"), "wears the table alias: {out}");
    }

    #[test]
    fn hop_refuses_a_non_multiple_size_and_proctime() {
        assert!(
            rewrite_hop_tvfs("SELECT 1 FROM HOP(TABLE t, DESCRIPTOR(ts), 3000, 10000)").is_none(),
            "size not a multiple of slide has no phase-shift decomposition"
        );
        assert!(rewrite_hop_tvfs("SELECT 1 FROM HOP(TABLE t, PROCTIME(), 2000, 10000)").is_none());
        assert!(rewrite_hop_tvfs("SELECT 1 FROM my_hop(x)").is_none());
    }

    #[test]
    fn streaming_topn_rewrites_to_qualify() {
        // q18's shape after the TUMBLE rewrite: a derived table + the
        // GROUP BY … ORDER BY … LIMIT ranking idiom.
        let sql = "SELECT bidder, auction, price FROM (SELECT *, \
                   \"dateTime\" - \"dateTime\" % 10000 AS window_start, \
                   \"dateTime\" - \"dateTime\" % 10000 + 10000 AS window_end FROM bid) AS bid \
                   GROUP BY bidder, auction, window_start, window_end \
                   ORDER BY \"dateTime\" DESC LIMIT 1";
        let out = rewrite_streaming_topn(sql).expect("rewrites");
        assert!(
            out.contains(
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY bidder, auction, window_start, window_end ORDER BY \"dateTime\" DESC) <= 1"
            ),
            "{out}"
        );
        assert!(
            !out.contains("GROUP BY"),
            "the idiom clauses are consumed: {out}"
        );
        assert!(!out.contains("LIMIT"), "{out}");
    }

    #[test]
    fn streaming_topn_leaves_standard_sql_alone() {
        // q5's shape: every projected column is grouped or aggregated —
        // standard SQL, not this rewrite's to claim.
        assert!(rewrite_streaming_topn(
            "SELECT auction, COUNT(*) AS c FROM b GROUP BY auction, window_start, window_end              ORDER BY auction LIMIT 5"
        )
        .is_none());
        // No window marker in the GROUP BY: not the streaming idiom.
        assert!(
            rewrite_streaming_topn("SELECT a, b FROM t GROUP BY a ORDER BY b LIMIT 1").is_none()
        );
    }

    #[test]
    fn session_rewrites_to_the_lag_cascade_with_the_group_key() {
        let sql = "SELECT bidder, COUNT(*) AS c \
                   FROM SESSION(TABLE bid, DESCRIPTOR(\"dateTime\"), 10000) \
                   GROUP BY bidder, window_start, window_end";
        let out = rewrite_session_tvfs(sql).expect("rewrites");
        assert!(out.contains("PARTITION BY bidder, __ivm_sid"), "{out}");
        assert!(
            out.contains(
                "LAG(\"dateTime\") OVER (PARTITION BY bidder ORDER BY \"dateTime\") >= 10000"
            ),
            "the engine boundary convention is diff >= gap splits: {out}"
        );
        assert!(out.contains("+ 10000 AS window_end"), "{out}");
        assert!(out.contains(") AS bid"), "{out}");
    }

    #[test]
    fn session_refuses_without_a_key_or_with_proctime() {
        assert!(rewrite_session_tvfs(
            "SELECT COUNT(*) FROM SESSION(TABLE b, DESCRIPTOR(ts), 10)              GROUP BY window_start, window_end"
        )
        .is_none(), "no session key");
        assert!(rewrite_session_tvfs("SELECT 1 FROM SESSION(TABLE b, PROCTIME(), 10)").is_none());
    }
}
