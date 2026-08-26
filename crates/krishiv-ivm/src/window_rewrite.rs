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
    let upper = sql.to_uppercase();
    let bytes = sql.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = upper.get(search_from..).and_then(|s| s.find("TUMBLE")) {
        let start = search_from + rel;
        search_from = start + 6;
        // Word boundary on the left (not e.g. `my_tumble`).
        if start > 0
            && let Some(&prev) = bytes.get(start - 1)
            && ((prev as char).is_alphanumeric() || prev == b'_')
        {
            continue;
        }
        // Opening paren (allow whitespace).
        let mut i = start + 6;
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
        if let Some(parsed) = parse_tumble_args(args) {
            return Some((start, end, parsed.0, parsed.1, parsed.2));
        }
        // Unsupported argument shape (interval string, PROCTIME): leave it —
        // the query stays as unplannable as it was, which is honest.
    }
    None
}

fn parse_tumble_args(args: &str) -> Option<(String, String, u64)> {
    // Split on top-level commas only (DESCRIPTOR(...) contains none today,
    // but stay paren-aware anyway).
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
    if parts.len() != 3 {
        return None;
    }

    let table = parts
        .first()?
        .strip_prefix("TABLE ")
        .or_else(|| parts.first()?.strip_prefix("table "))?
        .trim()
        .to_owned();
    let descriptor = parts.get(1)?;
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
    let size: u64 = parts.get(2)?.parse().ok()?;
    if size == 0 {
        return None;
    }
    Some((table, column, size))
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
}
