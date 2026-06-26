#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Streaming window table-valued functions: TUMBLE, HOP, SESSION.
//!
//! Rewrites Flink-SQL-style window TVF syntax into standard SQL that uses
//! the existing `tumble_start`/`tumble_end`/`hop_start`/`hop_end`/`session_start`/
//! `session_end` scalar UDFs registered by `window_functions.rs`.
//!
//! # Syntax (FROM clause)
//!
//! ```sql
//! -- Tumbling window: each event appears in exactly one window
//! SELECT key, window_start, window_end, COUNT(*)
//! FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000)
//! GROUP BY key, window_start, window_end
//!
//! -- Hopping / sliding window: each event appears in (size/slide) windows
//! SELECT key, window_start, window_end, COUNT(*)
//! FROM HOP(TABLE events, DESCRIPTOR(ts), 30000, 60000)
//! GROUP BY key, window_start, window_end
//!
//! -- Session window: gaps between events delimit window boundaries
//! SELECT key, window_start, window_end, COUNT(*)
//! FROM SESSION(TABLE events, DESCRIPTOR(ts), 5000)
//! GROUP BY key, window_start, window_end
//! ```
//!
//! # Interval expressions
//!
//! The size / slide / gap argument can be:
//! - An integer literal (milliseconds): `60000`
//! - A SQL interval string: `'1 minute'`, `'30 seconds'`, `'1 hour'` → converted to ms
//!
//! # Rewrite output
//!
//! ```sql
//! -- TUMBLE → subquery with window_start / window_end columns:
//! SELECT key, window_start, window_end, COUNT(*)
//! FROM (
//!   SELECT *, tumble_start(ts, 60000) AS window_start,
//!             tumble_end(ts, 60000)   AS window_end
//!   FROM events
//! ) AS _tvf_window
//! GROUP BY key, window_start, window_end
//! ```

use std::fmt::Write as FmtWrite;

/// Parse a quoted interval string to milliseconds.
/// Supports `'N unit'` where unit is one of: millisecond(s), second(s),
/// minute(s), hour(s), day(s).  Returns `None` if unparseable.
fn interval_str_to_ms(s: &str) -> Option<i64> {
    let inner = s.trim().trim_matches('\'').trim();
    let mut parts = inner.splitn(2, ' ');
    let n: i64 = parts.next()?.trim().parse().ok()?;
    let unit = parts.next()?.trim().to_ascii_lowercase();
    let ms = match unit.trim_end_matches('s') {
        "millisecond" => n,
        "second" => n * 1_000,
        "minute" => n * 60_000,
        "hour" => n * 3_600_000,
        "day" => n * 86_400_000,
        _ => return None,
    };
    Some(ms)
}

/// Convert a TVF interval argument (integer literal or quoted string) to a
/// millisecond `i64` string suitable for embedding in SQL.
fn normalise_interval_arg(arg: &str) -> String {
    let trimmed = arg.trim();
    // Already an integer literal.
    if trimmed.parse::<i64>().is_ok() {
        return trimmed.to_owned();
    }
    // Quoted interval string.
    if let Some(ms) = interval_str_to_ms(trimmed) {
        return ms.to_string();
    }
    // Return as-is and let DataFusion complain if invalid.
    trimmed.to_owned()
}

/// State for the TVF argument scanner (handles nested parentheses).
struct ArgScanner<'a> {
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    src: &'a str,
}

impl<'a> ArgScanner<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            chars: src.char_indices().peekable(),
            src,
        }
    }

    /// Consume past leading whitespace.
    fn skip_ws(&mut self) {
        while self.chars.peek().map(|(_, c)| c.is_ascii_whitespace()) == Some(true) {
            self.chars.next();
        }
    }

    /// Read until a `,` or `)` at depth 0, handling nested parens and quotes.
    /// Returns the argument string (trimmed) and the terminator char.
    fn next_arg(&mut self) -> Option<(&'a str, char)> {
        self.skip_ws();
        let (start, _) = self.chars.peek().copied()?;
        let mut depth = 0i32;
        let mut in_quote = false;
        let mut quote_char = '\0';
        let mut end = start;

        loop {
            match self.chars.next() {
                None => break,
                Some((i, c)) => {
                    end = i + c.len_utf8();
                    if in_quote {
                        if c == quote_char {
                            in_quote = false;
                        }
                    } else {
                        match c {
                            '\'' | '"' => {
                                in_quote = true;
                                quote_char = c;
                            }
                            '(' => depth += 1,
                            ')' => {
                                if depth == 0 {
                                    return Some((&self.src[start..i].trim(), c));
                                }
                                depth -= 1;
                            }
                            ',' if depth == 0 => {
                                return Some((&self.src[start..i].trim(), c));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        if end > start {
            Some((&self.src[start..end].trim(), '\0'))
        } else {
            None
        }
    }
}

/// Parsed form of a window TVF call.
#[derive(Debug, PartialEq)]
pub enum WindowTvf<'a> {
    Tumble {
        source: &'a str,
        ts_col: &'a str,
        size_ms: String,
    },
    Hop {
        source: &'a str,
        ts_col: &'a str,
        slide_ms: String,
        size_ms: String,
    },
    Session {
        source: &'a str,
        ts_col: &'a str,
        gap_ms: String,
    },
}

/// Try to parse `DESCRIPTOR(col_name)` and return `col_name`.
fn parse_descriptor(s: &str) -> Option<&str> {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    let inner = lower.strip_prefix("descriptor(")?;
    let inner = inner.strip_suffix(')')?;
    // Map back to original-case span by offset.
    let prefix_len = "descriptor(".len();
    Some(s[prefix_len..prefix_len + inner.len()].trim())
}

/// Try to parse `TABLE name` and return `name`.
fn parse_table_ref(s: &str) -> Option<&str> {
    let s = s.trim();
    let rest = s
        .strip_prefix("TABLE ")
        .or_else(|| s.strip_prefix("table "))
        .or_else(|| {
            let lower = s.to_ascii_lowercase();
            if lower.starts_with("table ") || lower.starts_with("table\t") {
                Some(&s[6..])
            } else {
                None
            }
        })?;
    Some(rest.trim())
}

/// Scan `sql` for the first occurrence of a window TVF in a FROM clause and
/// return `(pre, tvf, post)` where `pre + rewrite(tvf) + post` produces the
/// final SQL.  Returns `None` if no TVF is found.
pub fn find_window_tvf(sql: &str) -> Option<(usize, WindowTvf<'_>, usize)> {
    let lower = sql.to_ascii_lowercase();

    for kw in ["tumble", "hop", "session"] {
        let mut search_start = 0;
        while let Some(pos) = lower[search_start..].find(kw) {
            let abs = search_start + pos;
            // Must be preceded by whitespace, comma, or start-of-string (not part of identifier).
            let preceded_ok = abs == 0
                || sql[..abs]
                    .chars()
                    .last()
                    .map(|c| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(true);
            // Must be followed by '(' optionally with whitespace.
            let after = abs + kw.len();
            let followed_ok = sql[after..].trim_start().starts_with('(');

            if preceded_ok && followed_ok {
                // Find the opening paren.
                let paren_pos = after + sql[after..].find('(')?;
                let inner_start = paren_pos + 1;
                let mut scanner = ArgScanner::new(&sql[inner_start..]);

                let tvf = match kw {
                    "tumble" => {
                        let (a0, _) = scanner.next_arg()?;
                        let (a1, _) = scanner.next_arg()?;
                        let (a2, term) = scanner.next_arg()?;
                        if term != ')' && term != ',' {
                            search_start = abs + 1;
                            continue;
                        }
                        let source = parse_table_ref(a0)?;
                        let ts_col = parse_descriptor(a1)?;
                        let size_ms = normalise_interval_arg(a2);
                        WindowTvf::Tumble {
                            source,
                            ts_col,
                            size_ms,
                        }
                    }
                    "hop" => {
                        let (a0, _) = scanner.next_arg()?;
                        let (a1, _) = scanner.next_arg()?;
                        let (a2, _) = scanner.next_arg()?;
                        let (a3, term) = scanner.next_arg()?;
                        if term != ')' && term != ',' {
                            search_start = abs + 1;
                            continue;
                        }
                        let source = parse_table_ref(a0)?;
                        let ts_col = parse_descriptor(a1)?;
                        let slide_ms = normalise_interval_arg(a2);
                        let size_ms = normalise_interval_arg(a3);
                        WindowTvf::Hop {
                            source,
                            ts_col,
                            slide_ms,
                            size_ms,
                        }
                    }
                    "session" => {
                        let (a0, _) = scanner.next_arg()?;
                        let (a1, _) = scanner.next_arg()?;
                        let (a2, term) = scanner.next_arg()?;
                        if term != ')' && term != ',' {
                            search_start = abs + 1;
                            continue;
                        }
                        let source = parse_table_ref(a0)?;
                        let ts_col = parse_descriptor(a1)?;
                        let gap_ms = normalise_interval_arg(a2);
                        WindowTvf::Session {
                            source,
                            ts_col,
                            gap_ms,
                        }
                    }
                    _ => unreachable!(),
                };

                // The end of the TVF call is right after the closing ')' consumed
                // by scanner (scanner consumed inner content; paren_pos is the '(').
                // We need to find where in the original sql the scanner stopped.
                let consumed = scanner
                    .chars
                    .next()
                    .map(|(i, _)| i)
                    .unwrap_or(sql[inner_start..].len());
                let tvf_end = inner_start + consumed;
                return Some((abs, tvf, tvf_end));
            }
            search_start = abs + 1;
        }
    }
    None
}

/// Emit SQL for a window TVF call.
fn emit_tvf_subquery(tvf: &WindowTvf<'_>) -> String {
    let mut out = String::new();
    match tvf {
        WindowTvf::Tumble {
            source,
            ts_col,
            size_ms,
        } => {
            write!(
                out,
                "(SELECT *, tumble_start({ts_col}, {size_ms}) AS window_start, \
                 tumble_end({ts_col}, {size_ms}) AS window_end FROM {source}) AS _tvf_window"
            )
            .unwrap();
        }
        WindowTvf::Hop {
            source,
            ts_col,
            slide_ms,
            size_ms,
        } => {
            write!(
                out,
                "(SELECT *, hop_start({ts_col}, {slide_ms}, {size_ms}) AS window_start, \
                 hop_end({ts_col}, {slide_ms}, {size_ms}) AS window_end FROM {source}) AS _tvf_window"
            )
            .unwrap();
        }
        WindowTvf::Session {
            source,
            ts_col,
            gap_ms,
        } => {
            write!(
                out,
                "(SELECT *, session_start({ts_col}, {gap_ms}) AS window_start, \
                 session_end({ts_col}, {gap_ms}) AS window_end FROM {source}) AS _tvf_window"
            )
            .unwrap();
        }
    }
    out
}

/// Rewrite all window TVF calls in `sql` to subquery form.
/// Iterates until no more TVFs are found (handles multiple TVFs in one query).
pub fn rewrite_window_tvfs(sql: &str) -> String {
    let mut current = sql.to_owned();
    // Limit iterations to avoid infinite loop on malformed input.
    for _ in 0..16 {
        match find_window_tvf(&current) {
            None => break,
            Some((start, tvf, end)) => {
                let subq = emit_tvf_subquery(&tvf);
                let mut next = current[..start].to_owned();
                next.push_str(&subq);
                next.push_str(&current[end..]);
                current = next;
            }
        }
    }
    current
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn interval_str_seconds() {
        assert_eq!(interval_str_to_ms("'30 seconds'"), Some(30_000));
        assert_eq!(interval_str_to_ms("'1 minute'"), Some(60_000));
        assert_eq!(interval_str_to_ms("'2 hours'"), Some(7_200_000));
        assert_eq!(interval_str_to_ms("'1 day'"), Some(86_400_000));
    }

    #[test]
    fn tumble_rewrite_integer_interval() {
        let sql = "SELECT key, COUNT(*) FROM TUMBLE(TABLE events, DESCRIPTOR(ts), 60000) GROUP BY key, window_start, window_end";
        let out = rewrite_window_tvfs(sql);
        assert!(
            out.contains("tumble_start(ts, 60000) AS window_start"),
            "{out}"
        );
        assert!(out.contains("tumble_end(ts, 60000) AS window_end"), "{out}");
        assert!(out.contains("FROM events"), "{out}");
        assert!(out.contains("_tvf_window"), "{out}");
    }

    #[test]
    fn tumble_rewrite_interval_string() {
        let sql = "SELECT key FROM TUMBLE(TABLE clicks, DESCRIPTOR(event_ts), '1 minute') GROUP BY key, window_start, window_end";
        let out = rewrite_window_tvfs(sql);
        assert!(out.contains("tumble_start(event_ts, 60000)"), "{out}");
    }

    #[test]
    fn hop_rewrite() {
        let sql = "SELECT key FROM HOP(TABLE events, DESCRIPTOR(ts), 30000, 60000) GROUP BY key, window_start, window_end";
        let out = rewrite_window_tvfs(sql);
        assert!(
            out.contains("hop_start(ts, 30000, 60000) AS window_start"),
            "{out}"
        );
        assert!(
            out.contains("hop_end(ts, 30000, 60000) AS window_end"),
            "{out}"
        );
    }

    #[test]
    fn session_rewrite() {
        let sql = "SELECT key FROM SESSION(TABLE events, DESCRIPTOR(ts), 5000) GROUP BY key, window_start, window_end";
        let out = rewrite_window_tvfs(sql);
        assert!(
            out.contains("session_start(ts, 5000) AS window_start"),
            "{out}"
        );
        assert!(out.contains("session_end(ts, 5000) AS window_end"), "{out}");
    }

    #[test]
    fn no_tvf_is_identity() {
        let sql = "SELECT * FROM events WHERE ts > 0";
        assert_eq!(rewrite_window_tvfs(sql), sql);
    }

    #[test]
    fn lowercase_keywords_work() {
        let sql = "SELECT key FROM tumble(TABLE events, DESCRIPTOR(ts), 60000) GROUP BY key";
        let out = rewrite_window_tvfs(sql);
        assert!(out.contains("tumble_start"), "{out}");
    }

    #[test]
    fn interval_normalisation() {
        assert_eq!(normalise_interval_arg("60000"), "60000");
        assert_eq!(normalise_interval_arg("'1 minute'"), "60000");
        assert_eq!(normalise_interval_arg("'30 seconds'"), "30000");
    }
}
