//! `MATCH_RECOGNIZE` SQL extension planning (R16 S2).

use std::time::Duration;

use krishiv_cep::{CompiledPattern, Pattern};
use krishiv_plan::{ExecutionKind, LogicalPlan, NodeOp, PlanNode};

use crate::{SqlError, SqlResult};

/// Parsed `MATCH_RECOGNIZE` statement.
#[derive(Debug, Clone)]
pub struct MatchRecognizeStatement {
    pub source_table: String,
    pub key_column: String,
    pub event_time_column: String,
    pub pattern: CompiledPattern,
}

/// Parse the supported R16 subset:
///
/// `SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts PATTERN (A B) WITHIN 10 SECONDS)`
pub fn parse_match_recognize(sql: &str) -> SqlResult<Option<MatchRecognizeStatement>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();
    let Some(mr_pos) = upper.find(" MATCH_RECOGNIZE ") else {
        return Ok(None);
    };
    let from_pos = upper.find(" FROM ").ok_or_else(|| SqlError::Unsupported {
        feature: "MATCH_RECOGNIZE requires SELECT ... FROM <table>".into(),
    })?;
    let source_table = trimmed[from_pos + 6..mr_pos].trim().to_string();
    if source_table.is_empty() {
        return Err(SqlError::EmptyTableName);
    }

    let body_start = trimmed[mr_pos..]
        .find('(')
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE requires parenthesized body".into(),
        })?
        + mr_pos
        + 1;
    let body_end = trimmed.rfind(')').ok_or_else(|| SqlError::Unsupported {
        feature: "MATCH_RECOGNIZE requires closing ')'".into(),
    })?;
    let body = &trimmed[body_start..body_end];
    let body_upper = body.to_ascii_uppercase();

    let key_column = extract_after_keyword(body, &body_upper, "PARTITION BY", "ORDER BY")?;
    let event_time_column = extract_after_keyword(body, &body_upper, "ORDER BY", "PATTERN")?;
    let pattern_body = extract_parenthesized_after(body, &body_upper, "PATTERN")?;
    let stages = pattern_body
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if stages.is_empty() {
        return Err(SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE PATTERN must contain at least one stage".into(),
        });
    }
    let mut pattern = Pattern::begin(stages[0]);
    for stage in stages.iter().skip(1) {
        pattern = pattern.followed_by(*stage);
    }
    if let Some(window_ms) = parse_within_ms(body, &body_upper)? {
        pattern = pattern.within(Duration::from_millis(window_ms));
    }
    let pattern = pattern.compile().map_err(|e| SqlError::Unsupported {
        feature: format!("MATCH_RECOGNIZE pattern: {e}"),
    })?;

    Ok(Some(MatchRecognizeStatement {
        source_table,
        key_column,
        event_time_column,
        pattern,
    }))
}

/// Build a Krishiv streaming logical plan for `MATCH_RECOGNIZE`.
pub fn plan_match_recognize(stmt: MatchRecognizeStatement, query: &str) -> LogicalPlan {
    let stage_names = stmt
        .pattern
        .stages
        .iter()
        .map(|stage| stage.name.clone())
        .collect::<Vec<_>>();
    LogicalPlan::new("match-recognize", ExecutionKind::Streaming).with_node(
        PlanNode::new(
            "match-recognize",
            format!(
                "MATCH_RECOGNIZE source={} partition_by={} order_by={} pattern=({}) within_ms={}",
                stmt.source_table,
                stmt.key_column,
                stmt.event_time_column,
                stage_names.join(" "),
                stmt.pattern.window_ms
            ),
            ExecutionKind::Streaming,
        )
        .with_op(NodeOp::Other {
            description: format!("cep:{query}"),
        }),
    )
}

fn extract_after_keyword(
    body: &str,
    body_upper: &str,
    start_keyword: &str,
    end_keyword: &str,
) -> SqlResult<String> {
    let start = body_upper
        .find(start_keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {start_keyword}"),
        })?
        + start_keyword.len();
    let end = body_upper[start..]
        .find(end_keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {end_keyword}"),
        })?
        + start;
    let value = body[start..end].trim().to_string();
    if value.is_empty() {
        return Err(SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE empty {start_keyword}"),
        });
    }
    Ok(value)
}

fn extract_parenthesized_after(body: &str, body_upper: &str, keyword: &str) -> SqlResult<String> {
    let start = body_upper
        .find(keyword)
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE requires {keyword}"),
        })?
        + keyword.len();
    let open = body[start..]
        .find('(')
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE {keyword} requires '('"),
        })?
        + start;
    let close = body[open + 1..]
        .find(')')
        .ok_or_else(|| SqlError::Unsupported {
            feature: format!("MATCH_RECOGNIZE {keyword} requires ')'"),
        })?
        + open
        + 1;
    Ok(body[open + 1..close].trim().to_string())
}

fn parse_within_ms(body: &str, body_upper: &str) -> SqlResult<Option<u64>> {
    let Some(start) = body_upper.find("WITHIN") else {
        return Ok(None);
    };
    let mut parts = body[start + "WITHIN".len()..].split_whitespace();
    let value = parts
        .next()
        .ok_or_else(|| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE WITHIN requires a value".into(),
        })?
        .parse::<u64>()
        .map_err(|_| SqlError::Unsupported {
            feature: "MATCH_RECOGNIZE WITHIN value must be an integer".into(),
        })?;
    let unit = parts.next().unwrap_or("MILLISECONDS").to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "MILLISECOND" | "MILLISECONDS" | "MS" => 1,
        "SECOND" | "SECONDS" | "S" => 1_000,
        "MINUTE" | "MINUTES" | "M" => 60_000,
        other => {
            return Err(SqlError::Unsupported {
                feature: format!("MATCH_RECOGNIZE unsupported WITHIN unit {other}"),
            });
        }
    };
    Ok(Some(value.saturating_mul(multiplier)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_match_recognize_subset() {
        let stmt = parse_match_recognize(
            "SELECT * FROM events MATCH_RECOGNIZE (PARTITION BY user_id ORDER BY ts PATTERN (A B) WITHIN 10 SECONDS)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(stmt.source_table, "events");
        assert_eq!(stmt.key_column, "user_id");
        assert_eq!(stmt.event_time_column, "ts");
        assert_eq!(stmt.pattern.stages.len(), 2);
        assert_eq!(stmt.pattern.window_ms, 10_000);
    }
}
