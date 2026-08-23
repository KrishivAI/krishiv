#![forbid(unsafe_code)]

//! `CREATE INCREMENTAL VIEW` and `DECLARE RECURSIVE VIEW` SQL extensions.
//!
//! **These statements only *declare* a view.** Executing one writes an entry
//! into the SQL-layer [`IncrementalViewRegistry`] and does nothing else: no
//! operator state is built, no data is read, and no view is maintained. This
//! crate has no incremental engine — it is a parser and a metadata registry.
//! In particular there is **no** `INSERT INTO base` → view-maintenance path:
//! writing to a base table never updates a declared view.
//!
//! A declared view only becomes live when a pipeline that references it is
//! started, which needs all four statements below. `START PIPELINE` is executed
//! by `krishiv-api`'s `Session` (the layer that owns the IVM engine), not here:
//!
//! ```sql
//! CREATE SOURCE orders AS SELECT * FROM orders_raw;
//!
//! -- Declares the view. Maintains nothing on its own.
//! CREATE INCREMENTAL VIEW revenue AS
//!   SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
//!   LATENESS event_ts INTERVAL '5' MINUTE;
//!
//! CREATE SINK out FROM revenue;
//! START PIPELINE out;   -- ← the only statement that runs the IVM engine
//! ```
//!
//! Other accepted declaration forms:
//!
//! ```sql
//! -- Materialized variant (the engine keeps a full snapshot in memory)
//! CREATE MATERIALIZED INCREMENTAL VIEW revenue AS ...;
//!
//! -- ANSI/Spark spelling of the same thing, for JDBC/BI clients
//! CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] revenue AS ...;
//!
//! -- Recursive view (fixed-point iteration)
//! DECLARE RECURSIVE VIEW reachable AS
//!   SELECT dst FROM edges WHERE src = 0
//!   UNION
//!   SELECT e.dst FROM edges e JOIN reachable r ON e.src = r.dst;
//!
//! -- Remove the declaration (and, once started, its cached Trace state)
//! DROP INCREMENTAL VIEW revenue;
//! ```
//!
//! Two deliberate non-features, both of which used to be documented as working:
//!
//! * **No auto-DISTINCT on `DECLARE RECURSIVE VIEW`.** Nothing rewrites the
//!   body, so a `UNION ALL` recursion over a cyclic input does not converge; it
//!   runs until the engine's fixpoint iteration cap. Write the body
//!   set-semantically (`UNION`, or an explicit `DISTINCT`) yourself.
//! * **`REFRESH INCREMENTAL VIEW` / `REFRESH MATERIALIZED VIEW` are rejected.**
//!   No code path re-runs a view from them; they used to return an empty result
//!   set that the caller could not distinguish from success. Re-run the
//!   pipeline instead: `START PIPELINE <sink>` or `REFRESH PIPELINE <sink>`.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::{SqlError, SqlResult};

// ── LATENESS spec ─────────────────────────────────────────────────────────────

/// One LATENESS annotation: `LATENESS <column> INTERVAL '<n>' <unit>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatenessAnnotation {
    pub column: String,
    pub lateness_ms: u64,
}

// ── Parsed DDL statement ───────────────────────────────────────────────────────

/// Parsed incremental-view DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalViewStatement {
    Create {
        name: String,
        body_sql: String,
        is_materialized: bool,
        lateness: Vec<LatenessAnnotation>,
        /// `CREATE … IF NOT EXISTS`: leave an already-declared view alone
        /// instead of replacing its definition.
        if_not_exists: bool,
    },
    DeclareRecursive {
        name: String,
        body_sql: String,
        /// `LATENESS` annotations are accepted on a recursive declaration too
        /// and are carried into the registry entry like any other view's.
        lateness: Vec<LatenessAnnotation>,
    },
    Refresh {
        name: String,
    },
    Drop {
        name: String,
    },
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Metadata stored for one registered incremental view.
#[derive(Debug, Clone)]
pub struct IncrementalViewEntry {
    pub body_sql: String,
    pub is_materialized: bool,
    pub is_recursive: bool,
    pub lateness: Vec<LatenessAnnotation>,
}

/// Registry of active incremental views (SQL metadata layer).
///
/// This is the SQL-layer registry — it stores the DDL metadata for each view.
/// The `krishiv-api` layer bridges this to the `krishiv-delta` incremental
/// operator pipeline.
#[derive(Debug, Default)]
pub struct IncrementalViewRegistry {
    views: RwLock<HashMap<String, IncrementalViewEntry>>,
}

impl IncrementalViewRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, name: impl Into<String>, entry: IncrementalViewEntry) -> SqlResult<()> {
        let mut views = self.views.write().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        views.insert(name.into(), entry);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> SqlResult<bool> {
        let mut views = self.views.write().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        Ok(views.remove(name).is_some())
    }

    pub fn get(&self, name: &str) -> SqlResult<Option<IncrementalViewEntry>> {
        let views = self.views.read().map_err(|_| SqlError::DataFusion {
            message: "incremental view registry lock poisoned".into(),
        })?;
        Ok(views.get(name).cloned())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.views
            .read()
            .map(|v| v.contains_key(name))
            .unwrap_or(false)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse incremental-view DDL statements from a SQL string.
///
/// Returns `Ok(None)` if the statement is not an incremental-view DDL.
pub fn parse_incremental_view_statement(sql: &str) -> SqlResult<Option<IncrementalViewStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();

    // CREATE [MATERIALIZED] INCREMENTAL VIEW <name> AS <body>
    // [LATENESS <col> INTERVAL '<n>' <unit>]
    let is_materialized = upper.starts_with("CREATE MATERIALIZED INCREMENTAL VIEW ");
    if is_materialized || upper.starts_with("CREATE INCREMENTAL VIEW ") {
        let prefix = if is_materialized {
            "CREATE MATERIALIZED INCREMENTAL VIEW "
        } else {
            "CREATE INCREMENTAL VIEW "
        };
        let rest = trimmed
            .get(prefix.len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "CREATE INCREMENTAL VIEW".into(),
            })?;
        let (if_not_exists, rest) = split_if_not_exists(rest);
        let (name, body_with_lateness) = split_name_and_body(rest)?;
        let (body_sql, lateness) = split_body_and_lateness(&body_with_lateness)?;
        return Ok(Some(IncrementalViewStatement::Create {
            name,
            body_sql,
            is_materialized,
            lateness,
            if_not_exists,
        }));
    }

    // CREATE [OR REPLACE] MATERIALIZED VIEW <name> AS <body> [LATENESS …]
    //
    // The ANSI/Spark spelling of an incremental materialized view — a
    // SQL-standard front door onto the same IVM engine as
    // `CREATE MATERIALIZED INCREMENTAL VIEW`, so a plain Flight SQL / JDBC / BI
    // client can create one without the Krishiv-specific `INCREMENTAL` keyword.
    let mv_prefix = if upper.starts_with("CREATE OR REPLACE MATERIALIZED VIEW ") {
        Some("CREATE OR REPLACE MATERIALIZED VIEW ")
    } else if upper.starts_with("CREATE MATERIALIZED VIEW ") {
        Some("CREATE MATERIALIZED VIEW ")
    } else {
        None
    };
    if let Some(prefix) = mv_prefix {
        let or_replace = prefix.starts_with("CREATE OR REPLACE");
        let rest = trimmed
            .get(prefix.len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "CREATE MATERIALIZED VIEW".into(),
            })?;
        let (if_not_exists, rest) = split_if_not_exists(rest);
        if or_replace && if_not_exists {
            return Err(SqlError::Unsupported {
                feature: "CREATE OR REPLACE MATERIALIZED VIEW … IF NOT EXISTS: OR REPLACE \
                          (always replace) and IF NOT EXISTS (never replace) are mutually \
                          exclusive — pick one"
                    .into(),
            });
        }
        let (name, body_with_lateness) = split_name_and_body(rest)?;
        let (body_sql, lateness) = split_body_and_lateness(&body_with_lateness)?;
        return Ok(Some(IncrementalViewStatement::Create {
            name,
            body_sql,
            is_materialized: true,
            lateness,
            if_not_exists,
        }));
    }

    // DECLARE RECURSIVE VIEW <name> AS <body>
    if upper.starts_with("DECLARE RECURSIVE VIEW ") {
        let rest = trimmed
            .get("DECLARE RECURSIVE VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DECLARE RECURSIVE VIEW".into(),
            })?;
        let (name, body_sql) = split_name_and_body(rest)?;
        let (body_sql, lateness) = split_body_and_lateness(&body_sql)?;
        return Ok(Some(IncrementalViewStatement::DeclareRecursive {
            name,
            body_sql,
            lateness,
        }));
    }

    // REFRESH INCREMENTAL VIEW <name>
    if upper.starts_with("REFRESH INCREMENTAL VIEW ") {
        let name = trimmed
            .get("REFRESH INCREMENTAL VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "REFRESH INCREMENTAL VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Refresh { name }));
    }

    // DROP INCREMENTAL VIEW <name>
    if upper.starts_with("DROP INCREMENTAL VIEW ") {
        let name = trimmed
            .get("DROP INCREMENTAL VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DROP INCREMENTAL VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Drop { name }));
    }

    // REFRESH MATERIALIZED VIEW <name>  (ANSI/Spark synonym)
    if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
        let name = trimmed
            .get("REFRESH MATERIALIZED VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "REFRESH MATERIALIZED VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Refresh { name }));
    }

    // DROP MATERIALIZED VIEW <name>  (ANSI/Spark synonym)
    if upper.starts_with("DROP MATERIALIZED VIEW ") {
        let name = trimmed
            .get("DROP MATERIALIZED VIEW ".len()..)
            .ok_or_else(|| SqlError::Unsupported {
                feature: "DROP MATERIALIZED VIEW".into(),
            })?
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(SqlError::EmptyTableName);
        }
        return Ok(Some(IncrementalViewStatement::Drop { name }));
    }

    Ok(None)
}

/// Result of executing an incremental-view DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalViewResult {
    /// View was created or replaced.
    Created(String),
    /// `CREATE … IF NOT EXISTS` matched an existing declaration, which was left
    /// untouched. The statement succeeded; nothing changed.
    Existed(String),
    /// View was dropped.
    Dropped(String),
    /// A recursive view was created.
    Recursive(String),
}

/// Apply a parsed incremental-view DDL statement to the registry.
///
/// Returns `Ok(Some(_))` if the statement was an incremental-view DDL (so the
/// caller knows to return an empty DDL result rather than forwarding to
/// DataFusion), `Ok(None)` if the SQL was not an incremental-view DDL, and
/// `Err` if it was one this engine cannot honour.
pub fn execute_incremental_view_ddl(
    registry: &IncrementalViewRegistry,
    sql: &str,
) -> SqlResult<Option<IncrementalViewResult>> {
    let Some(stmt) = parse_incremental_view_statement(sql)? else {
        return Ok(None);
    };

    match stmt {
        IncrementalViewStatement::Create {
            ref name,
            ref body_sql,
            is_materialized,
            ref lateness,
            if_not_exists,
        } => {
            // Honoured, not merely parsed: an existing definition survives and
            // the statement still succeeds.
            if if_not_exists && registry.contains(name) {
                return Ok(Some(IncrementalViewResult::Existed(name.clone())));
            }
            registry.register(
                name.clone(),
                IncrementalViewEntry {
                    body_sql: body_sql.clone(),
                    is_materialized,
                    is_recursive: false,
                    lateness: lateness.clone(),
                },
            )?;
            Ok(Some(IncrementalViewResult::Created(name.clone())))
        }

        IncrementalViewStatement::DeclareRecursive {
            ref name,
            ref body_sql,
            ref lateness,
        } => {
            registry.register(
                name.clone(),
                IncrementalViewEntry {
                    body_sql: body_sql.clone(),
                    is_materialized: false,
                    is_recursive: true,
                    // IVM-AUD-DDL-B3: the parser binds the clause and this
                    // used to hardcode `vec![]` — LATENESS on a recursive view
                    // was accepted, validated, and thrown away. It travels
                    // with the spec like every other view's now.
                    lateness: lateness.clone(),
                },
            )?;
            Ok(Some(IncrementalViewResult::Recursive(name.clone())))
        }

        // IVM-AUD-DDL-F2: REFRESH is rejected rather than accepted-and-ignored.
        // Nothing in the engine re-runs a view because of this statement — the
        // arm used to return an empty result set indistinguishable from a
        // successful refresh, so a BI client's "refresh my materialized view"
        // silently did nothing. (An incremental view is maintained by feeding
        // its sources and stepping; there is no separate refresh action to
        // perform, which is why the honest answer is an error naming the real
        // mechanism rather than a no-op that looks like success.)
        IncrementalViewStatement::Refresh { ref name } => Err(SqlError::Unsupported {
            feature: format!(
                "REFRESH INCREMENTAL VIEW {name}: an incremental view is maintained by \
                 feeding its sources and stepping the flow (START PIPELINE, or \
                 IncrementalDataFrame::apply/step), not by a refresh statement; this \
                 statement previously succeeded while doing nothing"
            ),
        }),

        IncrementalViewStatement::Drop { ref name } => {
            registry.remove(name)?;
            Ok(Some(IncrementalViewResult::Dropped(name.clone())))
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Split `<name> AS <body>` into `(name, body)`.
fn split_name_and_body(rest: &str) -> SqlResult<(String, String)> {
    // ASCII folding: `as_pos` indexes `rest`, so the folded copy must keep the
    // same byte length. Unicode folding does not (U+FB01 -> "FI"), which
    // truncated the view name and could slice a character in half.
    let upper = rest.to_ascii_uppercase();
    let as_pos = upper.find(" AS ").ok_or_else(|| SqlError::Unsupported {
        feature: "CREATE INCREMENTAL VIEW / DECLARE RECURSIVE VIEW requires AS <query>".into(),
    })?;
    let name = rest[..as_pos].trim().to_string();
    let body = rest[as_pos + 4..].trim().to_string();
    if name.is_empty() {
        return Err(SqlError::EmptyTableName);
    }
    if body.is_empty() {
        return Err(SqlError::EmptyQuery);
    }
    Ok((name, body))
}

/// Strip a leading `IF NOT EXISTS` from `rest`, returning whether it was there.
///
/// Without this, `CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS …` registered a
/// view literally named `"IF NOT EXISTS mv"` — on the branch advertised as the
/// SQL-standard front door for JDBC/BI clients.
fn split_if_not_exists(rest: &str) -> (bool, &str) {
    // IVM-AUD-DDL-B5: this used to discard the strip and return `(false, rest)`,
    // so `CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS …` registered a view
    // literally named "IF NOT EXISTS mv" — on the branch advertised as the
    // SQL-standard front door for JDBC/BI clients, where that spelling is the
    // most common one a tool emits.
    match strip_leading_words(rest, &["IF", "NOT", "EXISTS"]) {
        Some(remainder) => (true, remainder),
        None => (false, rest),
    }
}

/// Consume `words` (ASCII, case-insensitive, whitespace-separated) from the
/// front of `rest`, returning the remainder. `None` if they are not all there.
fn strip_leading_words<'a>(rest: &'a str, words: &[&str]) -> Option<&'a str> {
    let mut rest = rest;
    for word in words {
        let trimmed = rest.trim_start();
        let head = trimmed.get(..word.len())?;
        if !head.eq_ignore_ascii_case(word) {
            return None;
        }
        let tail = trimmed.get(word.len()..)?;
        // The word must end here, not merely prefix a longer identifier.
        if !tail.starts_with(char::is_whitespace) {
            return None;
        }
        rest = tail;
    }
    Some(rest.trim_start())
}

/// Split the view body from trailing `LATENESS` annotations.
///
/// Grammar: `<body_sql> LATENESS <col> INTERVAL '<n>' <unit> [, ...]`
/// where unit is SECOND | MINUTE | HOUR | DAY.
///
/// If no LATENESS clause is found, returns `(body, vec![])`.
fn split_body_and_lateness(
    body_with_lateness: &str,
) -> SqlResult<(String, Vec<LatenessAnnotation>)> {
    // ASCII folding: `lat_pos` indexes `body_with_lateness`. The body is
    // arbitrary user SQL and may contain non-ASCII string literals.
    let upper = body_with_lateness.to_ascii_uppercase();

    // Find the FIRST top-level LATENESS *clause*: the clause list is the tail
    // of the grammar, so everything from there on belongs to
    // `parse_lateness_clauses`, which walks the remaining `, LATENESS …`
    // clauses itself.
    let Some(lat_pos) = find_lateness_clause_start(&upper) else {
        return Ok((body_with_lateness.trim().to_string(), vec![]));
    };

    let body_sql = body_with_lateness[..lat_pos].trim().to_string();
    let lateness_str = &body_with_lateness[lat_pos..];
    let lateness = parse_lateness_clauses(lateness_str)?;
    Ok((body_sql, lateness))
}

/// Find the byte offset of the first top-level LATENESS keyword in `upper`.
#[allow(clippy::indexing_slicing)]
fn find_lateness_clause_start(upper: &str) -> Option<usize> {
    // IVM-AUD-DDL-B4. The previous scan walked raw bytes tracking only paren
    // depth, so `SELECT lateness FROM t` truncated the body to `"SELECT"` and
    // returned Ok, and `WHERE msg = 'LATENESS …'` cut a string literal in half.
    // Two rules fix the whole class:
    //
    //   1. Skip regions where SQL keywords are not syntax — single-quoted
    //      literals (with '' escaping), double-quoted identifiers, `--` line
    //      comments and `/* */` block comments.
    //   2. Commit only on a LATENESS occurrence that is actually FOLLOWED by a
    //      clause (`<ident> INTERVAL`). The keyword alone is ambiguous — it is
    //      a legal column name — so the lookahead, not the keyword, is what
    //      distinguishes syntax from data. That is also why a column literally
    //      called `lateness` can still carry a LATENESS clause.
    //
    // `_` counts as a word byte, so `max_lateness` is one identifier — without
    // that, `WHERE max_lateness BETWEEN INTERVAL '1' DAY AND INTERVAL '2' DAY`
    // matches the `<ident> INTERVAL` lookahead and truncates the body at
    // `max_`.
    //
    // One ambiguity is irreducible: a column named exactly `lateness` used as
    // `WHERE lateness BETWEEN INTERVAL '1' DAY AND …` is indistinguishable from
    // a clause by any local rule. It is rejected with "unexpected tokens after
    // a LATENESS clause" rather than silently mis-parsed; quoting the column
    // (`"lateness"`) takes the quoted-identifier path above and is exact.
    let bytes = upper.as_bytes();
    let keyword = b"LATENESS";
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let Some(&b) = bytes.get(i) else { break };
        // ── skip non-syntax regions ──
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes.get(i) == Some(&b'\'') {
                    // '' is an escaped quote, not the end of the literal.
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if b == b'"' {
            i += 1;
            while i < bytes.len() && bytes.get(i) != Some(&b'"') {
                i += 1;
            }
            i += 1;
            continue;
        }
        if b == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes.get(i) != Some(&b'\n') {
                i += 1;
            }
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i < bytes.len()
                && !(bytes.get(i) == Some(&b'*') && bytes.get(i + 1) == Some(&b'/'))
            {
                i += 1;
            }
            i += 2;
            continue;
        }
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            depth = depth.saturating_sub(1);
            i += 1;
            continue;
        }
        // ── candidate keyword at depth 0 ──
        if depth == 0 && bytes.get(i..).is_some_and(|s| s.starts_with(keyword)) {
            let before_ok = i == 0 || bytes.get(i - 1).is_none_or(|&b| !is_word(b));
            let after = i + keyword.len();
            let after_ok = bytes.get(after).is_none_or(|&b| !is_word(b));
            if before_ok && after_ok && lookahead_is_clause(upper.get(after..).unwrap_or("")) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// True when the text right after a `LATENESS` keyword looks like the rest of
/// a clause: `<identifier> INTERVAL`. Keying on INTERVAL (rather than on the
/// column's spelling) is what lets `LATENESS lateness INTERVAL '1' DAY` parse
/// while `SELECT lateness FROM t` does not.
fn lookahead_is_clause(after_keyword: &str) -> bool {
    let mut tokens = after_keyword.split_whitespace();
    let Some(column) = tokens.next() else {
        return false;
    };
    // The column must be shaped like an identifier. Without this,
    // `WHERE d > lateness + INTERVAL '1' DAY` — a column named `lateness`
    // compared against an interval literal — reads as `LATENESS + INTERVAL`
    // and truncates the body at `lateness`.
    let identifier_shaped = column
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '"');
    if !identifier_shaped {
        return false;
    }
    tokens
        .next()
        .is_some_and(|kw| kw.eq_ignore_ascii_case("INTERVAL"))
}

/// Split off the next token. Commas are their own token so a clause list can
/// be walked without guessing where one clause ends and the next begins.
fn next_token(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    if let Some(rest) = s.strip_prefix(',') {
        return Some((",", rest));
    }
    let end = s
        .find(|c: char| c.is_whitespace() || c == ',')
        .unwrap_or(s.len());
    Some((s.get(..end)?, s.get(end..)?))
}

/// Parse one or more `LATENESS <col> INTERVAL '<n>' <unit>` clauses.
///
/// IVM-AUD-DDL-B4: the previous implementation walked the text with
/// `split_whitespace()` and a `break` on anything it did not recognise, then
/// re-found the *next* clause with `find("LATENESS")` from offset 1 — so a
/// clause missing its unit, a second malformed clause, and arbitrary trailing
/// SQL all parsed as success while the body had already been truncated at the
/// keyword. Every exit from this loop is now either a complete clause or an
/// error.
fn parse_lateness_clauses(lateness_str: &str) -> SqlResult<Vec<LatenessAnnotation>> {
    let mut result = Vec::new();
    let mut rest = lateness_str.trim();

    while !rest.trim().is_empty() {
        // Optional comma separating this clause from the previous one.
        if let Some((",", tail)) = next_token(rest) {
            rest = tail;
        }
        let incomplete = |what: &str| SqlError::Unsupported {
            feature: format!(
                "incomplete LATENESS clause: expected {what} in \
                 LATENESS <column> INTERVAL '<n>' <unit>"
            ),
        };

        let Some((keyword, tail)) = next_token(rest) else {
            break;
        };
        if !keyword.eq_ignore_ascii_case("LATENESS") {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "unexpected tokens after a LATENESS clause: '{}'",
                    rest.trim()
                ),
            });
        }
        let (col, tail) = next_token(tail).ok_or_else(|| incomplete("a column name"))?;
        if col == "," {
            return Err(incomplete("a column name"));
        }
        let (interval_kw, tail) = next_token(tail).ok_or_else(|| incomplete("INTERVAL"))?;
        if !interval_kw.eq_ignore_ascii_case("INTERVAL") {
            return Err(SqlError::Unsupported {
                feature: format!(
                    "LATENESS {col} expects the INTERVAL keyword before the value, \
                     got '{interval_kw}'"
                ),
            });
        }
        let (value_tok, tail) = next_token(tail).ok_or_else(|| incomplete("an interval value"))?;
        let (unit_tok, tail) = next_token(tail).ok_or_else(|| incomplete("an interval unit"))?;
        if value_tok == "," || unit_tok == "," {
            return Err(incomplete("an interval value and unit"));
        }

        let interval_str = value_tok.trim_matches('\'');
        let n: u64 = interval_str.parse().map_err(|_| SqlError::Unsupported {
            feature: format!("LATENESS INTERVAL value '{interval_str}' is not a valid integer"),
        })?;
        let ms = match unit_tok.trim_matches('\'').to_ascii_uppercase().as_str() {
            "SECOND" | "SECONDS" => n.saturating_mul(1000),
            "MINUTE" | "MINUTES" => n.saturating_mul(60_000),
            "HOUR" | "HOURS" => n.saturating_mul(3_600_000),
            "DAY" | "DAYS" => n.saturating_mul(86_400_000),
            "MILLISECOND" | "MILLISECONDS" | "MS" => n,
            other => {
                return Err(SqlError::Unsupported {
                    feature: format!(
                        "LATENESS interval unit '{other}' is not supported \
                         (expected SECOND, MINUTE, HOUR, DAY, or MILLISECOND)"
                    ),
                });
            }
        };

        result.push(LatenessAnnotation {
            column: col.trim_matches('"').to_string(),
            lateness_ms: ms,
        });
        rest = tail;

        // Only another clause may follow. Anything else is trailing SQL that
        // the body no longer contains — rejecting it is the whole point.
        let peek = rest.trim_start();
        if peek.is_empty() {
            break;
        }
        let is_next_clause = peek.starts_with(',')
            || peek
                .get(.."LATENESS".len())
                .is_some_and(|w| w.eq_ignore_ascii_case("LATENESS"));
        if !is_next_clause {
            return Err(SqlError::Unsupported {
                feature: format!("unexpected tokens after a LATENESS clause: '{peek}'"),
            });
        }
    }

    Ok(result)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod folding_and_whitespace_tests {
    use super::*;

    /// The name offset is found in an uppercased copy and applied to the
    /// original, so the two must have equal byte length. Unicode folding does
    /// not preserve it (U+FB01 -> "FI", 3 bytes to 2).
    #[test]
    fn a_view_name_whose_uppercase_is_shorter_is_not_truncated() {
        let stmt =
            parse_incremental_view_statement("CREATE INCREMENTAL VIEW \u{FB01}x AS SELECT 1")
                .unwrap()
                .unwrap();
        match stmt {
            IncrementalViewStatement::Create { name, body_sql, .. } => {
                assert_eq!(name, "\u{FB01}x", "the whole name must survive");
                assert_eq!(body_sql, "SELECT 1");
            }
            other => panic!("expected create, got {other:?}"),
        }
    }

    /// Extra whitespace between LATENESS tokens must not shift the fields.
    /// `splitn(_, char::is_whitespace)` does not collapse runs, so a double
    /// space made the INTERVAL token land where the value was expected.
    #[test]
    fn lateness_tolerates_repeated_whitespace() {
        let stmt = parse_incremental_view_statement(
            "CREATE INCREMENTAL VIEW v AS SELECT 1 LATENESS ts  INTERVAL '5' MINUTE",
        )
        .unwrap()
        .unwrap();
        match stmt {
            IncrementalViewStatement::Create { lateness, .. } => {
                assert_eq!(lateness.len(), 1, "one annotation");
                let a = lateness.first().expect("one");
                assert_eq!(a.column, "ts");
                assert_eq!(a.lateness_ms, 300_000, "5 minutes");
            }
            other => panic!("expected create, got {other:?}"),
        }
    }

    /// The token after the column must actually be INTERVAL; the code carried a
    /// comment saying so but never checked.
    ///
    /// The check bites once the scanner has committed to a clause list, which
    /// it does on the unambiguous `LATENESS <col> INTERVAL` shape. A *first*
    /// clause whose keyword is mistyped is not recognised as a clause at all —
    /// see `a_mistyped_interval_keyword_leaves_the_body_intact`; guessing at
    /// near-misses is exactly what truncated bodies containing a column named
    /// `lateness`.
    #[test]
    fn lateness_requires_the_interval_keyword() {
        let err = parse_incremental_view_statement(
            "CREATE INCREMENTAL VIEW v AS SELECT 1 \
             LATENESS a INTERVAL '5' MINUTE, LATENESS b NOTINTERVAL '2' HOUR",
        )
        .expect_err("a missing INTERVAL keyword must be rejected");
        assert!(err.to_string().contains("INTERVAL"), "{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_incremental_view() {
        let sql = "CREATE INCREMENTAL VIEW revenue AS SELECT SUM(amount) FROM orders";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create { ref name, is_materialized: false, .. }
            if name == "revenue"
        ));
    }

    #[test]
    fn parse_create_materialized_incremental_view() {
        let sql = "CREATE MATERIALIZED INCREMENTAL VIEW snap AS SELECT * FROM t";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create {
                is_materialized: true,
                ..
            }
        ));
    }

    #[test]
    fn parse_create_materialized_view_maps_to_ivm() {
        // The ANSI/Spark `CREATE MATERIALIZED VIEW` maps onto the same IVM view
        // as `CREATE MATERIALIZED INCREMENTAL VIEW`.
        let sql = "CREATE MATERIALIZED VIEW revenue AS SELECT SUM(amount) AS t FROM orders";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create { ref name, body_sql: ref body, is_materialized: true, .. }
            if name == "revenue" && body.starts_with("SELECT SUM(amount)")
        ));
    }

    #[test]
    fn parse_create_or_replace_materialized_view() {
        let sql = "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT * FROM t";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create { ref name, is_materialized: true, .. } if name == "mv"
        ));
    }

    #[test]
    fn parse_create_materialized_view_with_lateness() {
        let sql = "CREATE MATERIALIZED VIEW ev AS SELECT * FROM s \
                   LATENESS event_ts INTERVAL '5' MINUTE";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        match stmt {
            IncrementalViewStatement::Create {
                is_materialized,
                lateness,
                body_sql,
                ..
            } => {
                assert!(is_materialized);
                assert_eq!(lateness.len(), 1, "LATENESS annotation is parsed");
                assert!(!body_sql.to_uppercase().contains("LATENESS"));
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn parse_refresh_and_drop_materialized_view() {
        let refresh = parse_incremental_view_statement("REFRESH MATERIALIZED VIEW revenue")
            .unwrap()
            .unwrap();
        assert!(matches!(
            refresh,
            IncrementalViewStatement::Refresh { ref name } if name == "revenue"
        ));
        let drop = parse_incremental_view_statement("DROP MATERIALIZED VIEW revenue;")
            .unwrap()
            .unwrap();
        assert!(matches!(
            drop,
            IncrementalViewStatement::Drop { ref name } if name == "revenue"
        ));
    }

    #[test]
    fn materialized_view_does_not_shadow_materialized_incremental_view() {
        // The two-token `MATERIALIZED VIEW` matcher must not swallow the
        // three-token `MATERIALIZED INCREMENTAL VIEW` form.
        let sql = "CREATE MATERIALIZED INCREMENTAL VIEW snap AS SELECT * FROM t";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Create { ref name, is_materialized: true, .. } if name == "snap"
        ));
    }

    #[test]
    fn parse_declare_recursive_view() {
        let sql = "DECLARE RECURSIVE VIEW reach AS SELECT dst FROM edges WHERE src = 0";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::DeclareRecursive { ref name, .. } if name == "reach"
        ));
    }

    #[test]
    fn parse_refresh_incremental_view() {
        let sql = "REFRESH INCREMENTAL VIEW revenue";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Refresh { ref name } if name == "revenue"
        ));
    }

    #[test]
    fn parse_drop_incremental_view() {
        let sql = "DROP INCREMENTAL VIEW revenue;";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        assert!(matches!(
            stmt,
            IncrementalViewStatement::Drop { ref name } if name == "revenue"
        ));
    }

    #[test]
    fn non_incremental_sql_returns_none() {
        let sql = "SELECT 1";
        assert!(parse_incremental_view_statement(sql).unwrap().is_none());
    }

    #[test]
    fn parse_create_with_lateness() {
        let sql =
            "CREATE INCREMENTAL VIEW ev AS SELECT * FROM s LATENESS event_ts INTERVAL '5' MINUTE";
        let stmt = parse_incremental_view_statement(sql).unwrap().unwrap();
        if let IncrementalViewStatement::Create { lateness, .. } = stmt {
            assert_eq!(lateness.len(), 1);
            assert_eq!(lateness[0].column, "event_ts");
            assert_eq!(lateness[0].lateness_ms, 5 * 60_000);
        } else {
            panic!("expected Create");
        }
    }

    #[test]
    fn registry_register_and_get() {
        let reg = IncrementalViewRegistry::new();
        reg.register(
            "v1",
            IncrementalViewEntry {
                body_sql: "SELECT 1".into(),
                is_materialized: false,
                is_recursive: false,
                lateness: vec![],
            },
        )
        .unwrap();
        assert!(reg.contains("v1"));
        let entry = reg.get("v1").unwrap().unwrap();
        assert_eq!(entry.body_sql, "SELECT 1");
    }

    #[test]
    fn execute_ddl_create_and_drop() {
        let reg = IncrementalViewRegistry::new();
        let result =
            execute_incremental_view_ddl(&reg, "CREATE INCREMENTAL VIEW v AS SELECT 1").unwrap();
        assert!(matches!(result, Some(IncrementalViewResult::Created(_))));
        assert!(reg.contains("v"));

        execute_incremental_view_ddl(&reg, "DROP INCREMENTAL VIEW v").unwrap();
        assert!(!reg.contains("v"));
    }

    /// DDL-F2: REFRESH used to return an empty result set indistinguishable
    /// from a real refresh, for a statement no engine path acts on. It must
    /// fail, and the failure must name the door that does work.
    #[test]
    fn execute_ddl_refresh_is_rejected_and_names_the_pipeline_alternative() {
        let reg = IncrementalViewRegistry::new();
        execute_incremental_view_ddl(&reg, "CREATE INCREMENTAL VIEW v AS SELECT 1").unwrap();
        for sql in [
            "REFRESH INCREMENTAL VIEW v",
            "REFRESH MATERIALIZED VIEW v",
            // …registered or not: there is nothing to refresh either way.
            "REFRESH INCREMENTAL VIEW nonexistent",
        ] {
            let err = execute_incremental_view_ddl(&reg, sql)
                .expect_err("REFRESH must be rejected, not silently accepted");
            let msg = err.to_string();
            assert!(
                msg.contains("START PIPELINE"),
                "the error must name the working alternative; got {msg}"
            );
        }
    }
}

// ── Regression tests for the delta-batch DDL audit ────────────────────────────

#[cfg(test)]
mod ivm_audit_regression_tests {
    use super::*;

    fn create_stmt(sql: &str) -> IncrementalViewStatement {
        parse_incremental_view_statement(sql)
            .expect("parses")
            .expect("is incremental-view DDL")
    }

    fn body_and_lateness(sql: &str) -> (String, Vec<LatenessAnnotation>) {
        match create_stmt(sql) {
            IncrementalViewStatement::Create {
                body_sql, lateness, ..
            } => (body_sql, lateness),
            other => panic!("expected Create, got {other:?}"),
        }
    }

    // ── DDL-B4: the LATENESS scanner ──────────────────────────────────────────

    /// A column *named* `lateness` is not a LATENESS clause. The raw-byte scan
    /// truncated the body to `"SELECT"` here and returned `Ok`.
    #[test]
    fn a_column_named_lateness_does_not_truncate_the_body() {
        let (body, lateness) =
            body_and_lateness("CREATE INCREMENTAL VIEW v AS SELECT lateness FROM t");
        assert_eq!(
            body, "SELECT lateness FROM t",
            "the body must survive whole"
        );
        assert!(lateness.is_empty(), "no annotation was written");
    }

    /// `_` is a word byte: `max_lateness` is one identifier, not a keyword.
    #[test]
    fn an_identifier_ending_in_lateness_is_not_a_clause() {
        let (body, lateness) =
            body_and_lateness("CREATE INCREMENTAL VIEW v AS SELECT max_lateness FROM t");
        assert_eq!(body, "SELECT max_lateness FROM t");
        assert!(lateness.is_empty());
    }

    /// The keyword inside a string literal is data, not syntax — even when the
    /// literal contains a whole well-formed-looking clause.
    #[test]
    fn lateness_inside_a_string_literal_is_not_a_clause() {
        let sql = "CREATE INCREMENTAL VIEW v AS \
                   SELECT * FROM t WHERE msg = 'LATENESS ts INTERVAL x'";
        let (body, lateness) = body_and_lateness(sql);
        assert_eq!(
            body, "SELECT * FROM t WHERE msg = 'LATENESS ts INTERVAL x'",
            "the literal must not be cut in half"
        );
        assert!(lateness.is_empty());
    }

    /// …and a doubled quote inside a literal must not end it early.
    #[test]
    fn lateness_after_an_escaped_quote_inside_a_literal_is_not_a_clause() {
        let sql = "CREATE INCREMENTAL VIEW v AS \
                   SELECT * FROM t WHERE msg = 'it''s LATENESS ts INTERVAL x'";
        let (body, lateness) = body_and_lateness(sql);
        assert_eq!(
            body,
            "SELECT * FROM t WHERE msg = 'it''s LATENESS ts INTERVAL x'"
        );
        assert!(lateness.is_empty());
    }

    /// A quoted identifier is the same story.
    #[test]
    fn lateness_as_a_quoted_identifier_is_not_a_clause() {
        let sql = "CREATE INCREMENTAL VIEW v AS SELECT \"LATENESS ts INTERVAL x\" FROM t";
        let (body, lateness) = body_and_lateness(sql);
        assert_eq!(body, "SELECT \"LATENESS ts INTERVAL x\" FROM t");
        assert!(lateness.is_empty());
    }

    /// A commented-out clause is not a clause.
    #[test]
    fn lateness_inside_a_comment_is_not_a_clause() {
        let line = "CREATE INCREMENTAL VIEW v AS SELECT 1 -- LATENESS ts INTERVAL '5' MINUTE";
        let (body, lateness) = body_and_lateness(line);
        assert_eq!(body, "SELECT 1 -- LATENESS ts INTERVAL '5' MINUTE");
        assert!(lateness.is_empty());

        let block = "CREATE INCREMENTAL VIEW v AS \
                     SELECT 1 /* LATENESS ts INTERVAL '5' MINUTE */ FROM t";
        let (body, lateness) = body_and_lateness(block);
        assert_eq!(
            body,
            "SELECT 1 /* LATENESS ts INTERVAL '5' MINUTE */ FROM t"
        );
        assert!(lateness.is_empty());
    }

    /// The real clause still parses, and still leaves the body clean.
    /// Revert-proof for the `_`-is-a-word-byte rule: `BETWEEN INTERVAL` is a
    /// real interval comparison, and satisfies the `<ident> INTERVAL`
    /// lookahead. Only the word boundary tells `max_lateness` from a clause.
    #[test]
    fn an_identifier_ending_in_lateness_before_an_interval_literal_is_not_a_clause() {
        let sql = "CREATE INCREMENTAL VIEW v AS \
             SELECT * FROM t WHERE max_lateness BETWEEN INTERVAL '1' DAY AND INTERVAL '2' DAY";
        let (body, lateness) = body_and_lateness(sql);
        assert_eq!(
            body,
            "SELECT * FROM t WHERE max_lateness BETWEEN INTERVAL '1' DAY AND INTERVAL '2' DAY",
            "the body must survive intact"
        );
        assert!(lateness.is_empty(), "got {lateness:?}");
    }

    /// Revert-proof for the identifier-shape rule: a column named `lateness`
    /// compared against an interval literal puts `+` where the column would go.
    #[test]
    fn a_lateness_column_added_to_an_interval_literal_is_not_a_clause() {
        let sql =
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t WHERE d > lateness + INTERVAL '1' DAY";
        let (body, lateness) = body_and_lateness(sql);
        assert_eq!(
            body, "SELECT * FROM t WHERE d > lateness + INTERVAL '1' DAY",
            "the body must survive intact"
        );
        assert!(lateness.is_empty(), "got {lateness:?}");
    }

    #[test]
    fn a_real_lateness_clause_is_still_found() {
        let (body, lateness) = body_and_lateness(
            "CREATE INCREMENTAL VIEW v AS SELECT lateness FROM t \
             LATENESS event_ts INTERVAL '5' MINUTE",
        );
        assert_eq!(body, "SELECT lateness FROM t");
        assert_eq!(lateness.len(), 1);
        let a = lateness.first().expect("one");
        assert_eq!(a.column, "event_ts");
        assert_eq!(a.lateness_ms, 300_000);
    }

    /// Two comma-separated clauses.
    #[test]
    fn two_lateness_clauses_are_both_parsed() {
        let (body, lateness) = body_and_lateness(
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t \
             LATENESS a INTERVAL '5' MINUTE, LATENESS b INTERVAL '2' HOUR",
        );
        assert_eq!(body, "SELECT * FROM t");
        assert_eq!(lateness.len(), 2, "both annotations survive");
        assert_eq!(lateness.first().expect("first").column, "a");
        assert_eq!(lateness.get(1).expect("second").column, "b");
        assert_eq!(lateness.get(1).expect("second").lateness_ms, 7_200_000);
    }

    /// A clause with a column even called `lateness` still parses — the
    /// lookahead keys on the INTERVAL keyword, not on the column's spelling.
    #[test]
    fn a_lateness_clause_over_a_column_named_lateness_parses() {
        let (body, lateness) = body_and_lateness(
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t LATENESS lateness INTERVAL '1' DAY",
        );
        assert_eq!(body, "SELECT * FROM t");
        assert_eq!(lateness.len(), 1);
        let a = lateness.first().expect("one");
        assert_eq!(a.column, "lateness");
        assert_eq!(a.lateness_ms, 86_400_000);
    }

    /// The scanner commits only on `LATENESS <col> INTERVAL`. A mistyped
    /// keyword is therefore not a clause, and the body keeps every byte the
    /// user wrote rather than being cut at the guess. (Body SQL is validated
    /// when a pipeline runs it, not here — a nonsense body has always been
    /// registered as written.)
    #[test]
    fn a_mistyped_interval_keyword_leaves_the_body_intact() {
        let (body, lateness) = body_and_lateness(
            "CREATE INCREMENTAL VIEW v AS SELECT 1 LATENESS ts NOTINTERVAL '5' MINUTE",
        );
        assert_eq!(
            body, "SELECT 1 LATENESS ts NOTINTERVAL '5' MINUTE",
            "no guessing: the body is not truncated at a near-miss"
        );
        assert!(lateness.is_empty());
    }

    /// DDL-B4, second half: a malformed clause used to `break` — fail OPEN —
    /// while its two sibling branches returned errors. It must fail closed.
    #[test]
    fn an_incomplete_lateness_clause_is_rejected() {
        let err = parse_incremental_view_statement(
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t LATENESS ts INTERVAL '5'",
        )
        .expect_err("a clause missing its unit must be rejected");
        assert!(err.to_string().contains("incomplete"), "{err}");
    }

    /// Junk after a well-formed clause is a syntax error, not something to
    /// silently drop off the end of the body.
    #[test]
    fn trailing_junk_after_a_lateness_clause_is_rejected() {
        let err = parse_incremental_view_statement(
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t \
             LATENESS ts INTERVAL '5' MINUTE GROUP BY x",
        )
        .expect_err("trailing tokens must be rejected");
        assert!(err.to_string().contains("unexpected"), "{err}");
    }

    /// A second clause that is malformed must not be silently forgotten.
    #[test]
    fn a_malformed_second_lateness_clause_is_rejected() {
        let err = parse_incremental_view_statement(
            "CREATE INCREMENTAL VIEW v AS SELECT * FROM t \
             LATENESS a INTERVAL '5' MINUTE, LATENESS b INTERVAL '2'",
        )
        .expect_err("an incomplete second clause must be rejected");
        assert!(err.to_string().contains("incomplete"), "{err}");
    }

    // ── DDL-B5: IF NOT EXISTS ─────────────────────────────────────────────────

    /// The name used to come out as `"IF NOT EXISTS mv"` — on the branch sold
    /// as the SQL-standard front door for JDBC/BI clients.
    #[test]
    fn create_materialized_view_if_not_exists_parses_the_bare_name() {
        match create_stmt("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1") {
            IncrementalViewStatement::Create {
                name,
                if_not_exists,
                is_materialized,
                ..
            } => {
                assert_eq!(
                    name, "mv",
                    "the IF NOT EXISTS clause is not part of the name"
                );
                assert!(if_not_exists, "the clause must be recorded, not dropped");
                assert!(is_materialized);
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn create_incremental_view_if_not_exists_parses_the_bare_name() {
        match create_stmt("CREATE INCREMENTAL VIEW IF NOT EXISTS v AS SELECT 1") {
            IncrementalViewStatement::Create {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "v");
                assert!(if_not_exists);
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    /// IF NOT EXISTS is honoured, not merely parsed: the existing definition
    /// survives, and no view is registered under the clause text.
    #[test]
    fn if_not_exists_keeps_the_existing_definition() {
        let reg = IncrementalViewRegistry::new();
        execute_incremental_view_ddl(&reg, "CREATE MATERIALIZED VIEW mv AS SELECT 1 AS a").unwrap();

        let result = execute_incremental_view_ddl(
            &reg,
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 2 AS b",
        )
        .unwrap();

        assert!(
            matches!(result, Some(IncrementalViewResult::Existed(ref n)) if n == "mv"),
            "an existing view must be reported as untouched"
        );
        assert!(
            reg.get("IF NOT EXISTS mv").unwrap().is_none(),
            "nothing may be registered under the clause text"
        );
        assert_eq!(
            reg.get("mv").unwrap().expect("mv is registered").body_sql,
            "SELECT 1 AS a",
            "IF NOT EXISTS must not replace the existing definition"
        );
    }

    /// …and it still creates the view when there is none.
    #[test]
    fn if_not_exists_creates_when_absent() {
        let reg = IncrementalViewRegistry::new();
        let result = execute_incremental_view_ddl(
            &reg,
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1",
        )
        .unwrap();
        assert!(matches!(result, Some(IncrementalViewResult::Created(_))));
        assert!(reg.contains("mv"));
    }

    /// A view is not named `if`-something just because the clause is absent.
    #[test]
    fn a_view_named_if_is_not_mistaken_for_if_not_exists() {
        match create_stmt("CREATE INCREMENTAL VIEW if AS SELECT 1") {
            IncrementalViewStatement::Create {
                name,
                if_not_exists,
                ..
            } => {
                assert_eq!(name, "if");
                assert!(!if_not_exists);
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn or_replace_with_if_not_exists_is_rejected() {
        let err = parse_incremental_view_statement(
            "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1",
        )
        .expect_err("the two clauses contradict each other");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    // ── DDL-B3: LATENESS on DECLARE RECURSIVE VIEW ────────────────────────────

    /// The annotation was parsed into `_lateness` and thrown away, and the
    /// registry entry hardcoded `lateness: vec![]`.
    #[test]
    fn lateness_on_a_recursive_view_reaches_the_registry() {
        let reg = IncrementalViewRegistry::new();
        execute_incremental_view_ddl(
            &reg,
            "DECLARE RECURSIVE VIEW reach AS SELECT dst FROM edges \
             LATENESS event_ts INTERVAL '10' SECOND",
        )
        .unwrap();

        let entry = reg.get("reach").unwrap().expect("reach is registered");
        assert!(entry.is_recursive);
        assert_eq!(entry.body_sql, "SELECT dst FROM edges");
        assert_eq!(
            entry.lateness,
            vec![LatenessAnnotation {
                column: "event_ts".into(),
                lateness_ms: 10_000,
            }],
            "a recursive view's LATENESS must not be discarded"
        );
    }
}
