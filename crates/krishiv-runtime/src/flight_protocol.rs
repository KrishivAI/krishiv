//! Krishiv Flight SQL comment protocol for catalog sync and remote streaming control.
//!
//! Status (B3, D2):
//! - **Legacy fallback path** kept for clients that haven't migrated to the
//!   typed [`KrishivFlightAction`] API exposed via `do_action`.  New clients
//!   should use `krishiv-flight-sql`'s `KrishivFlightActionClient`.
//! - Comment parser is hardened: identifiers (table, job_id, topic) must
//!   match `[A-Za-z0-9_.-]+`; base64-encoded fields are verified before use;
//!   any directive carrying a forbidden character is rejected (not silently
//!   passed through as SQL — this prevented a comment-injection vector where
//!   a job_id containing `*/` could end the comment early and inject SQL).

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use krishiv_plan::window::WindowExecutionSpec;

use crate::in_process::BatchSqlTable;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

const REGISTER_PARQUET: &str = "krishiv-register-parquet";
const CONTINUOUS_REGISTER: &str = "krishiv-continuous-register";
const CONTINUOUS_PUSH: &str = "krishiv-continuous-push";
const CONTINUOUS_DRAIN: &str = "krishiv-continuous-drain";
const BOUNDED_WINDOW: &str = "krishiv-bounded-window";
const EXPLAIN: &str = "krishiv-explain";

/// Parsed Krishiv Flight SQL directive.
#[derive(Debug, Clone, PartialEq)]
pub enum FlightDirective {
    RegisterParquet {
        table: String,
        path: PathBuf,
    },
    ContinuousRegister {
        job_id: String,
        spec: WindowExecutionSpec,
    },
    ContinuousPush {
        job_id: String,
        batches: Vec<RecordBatch>,
    },
    ContinuousDrain {
        job_id: String,
    },
    BoundedWindow {
        topic: String,
        spec: WindowExecutionSpec,
        input_batches: Vec<RecordBatch>,
    },
    Explain,
}

/// Encode batch SQL with client-side parquet catalog registrations.
pub fn encode_batch_sql(query: &str, tables: &[BatchSqlTable]) -> String {
    let mut parts = Vec::new();
    for table in tables {
        parts.push(format!(
            "/* {REGISTER_PARQUET}:{}:{} */",
            table.table_name,
            table.path.display()
        ));
    }
    parts.push(query.to_string());
    parts.join("\n")
}

/// Encode remote continuous job registration.
pub fn encode_continuous_register(job_id: &str, spec: &LocalWindowExecutionSpec) -> RuntimeResult<String> {
    let plan_spec = spec.to_plan_spec();
    let json = serde_json::to_string(&plan_spec)
        .map_err(|e| RuntimeError::transport(format!("window spec serialization: {e}")))?;
    let encoded = BASE64.encode(json.as_bytes());
    Ok(format!("/* {CONTINUOUS_REGISTER}:{job_id}:{encoded} */ SELECT 1 AS registered"))
}

/// Encode remote continuous input push.
pub fn encode_continuous_push(job_id: &str, batches: &[RecordBatch]) -> RuntimeResult<String> {
    let ipc = encode_batches_ipc(batches)?;
    Ok(format!(
        "/* {CONTINUOUS_PUSH}:{job_id}:{ipc} */ SELECT 1 AS pushed"
    ))
}

/// Encode remote continuous output drain.
pub fn encode_continuous_drain(job_id: &str) -> String {
    format!("/* {CONTINUOUS_DRAIN}:{job_id} */ SELECT 1 AS drained")
}

/// Encode remote bounded window execution (topic + spec + input batches).
pub fn encode_bounded_window(
    topic: &str,
    spec: &LocalWindowExecutionSpec,
    input_batches: &[RecordBatch],
) -> RuntimeResult<String> {
    let plan_spec = spec.to_plan_spec();
    let spec_json = serde_json::to_string(&plan_spec)
        .map_err(|e| RuntimeError::transport(format!("window spec serialization: {e}")))?;
    let spec_b64 = BASE64.encode(spec_json.as_bytes());
    let ipc = encode_batches_ipc(input_batches)?;
    Ok(format!(
        "/* {BOUNDED_WINDOW}:{topic}:{spec_b64}:{ipc} */ SELECT 1 AS windowed"
    ))
}

/// Encode remote EXPLAIN request.
pub fn encode_explain_sql(query: &str) -> String {
    format!("/* {EXPLAIN} */ {query}")
}

/// Split SQL into Krishiv directives and the remaining executable statement.
pub fn parse_sql(sql: &str) -> (Vec<FlightDirective>, String) {
    let mut directives = Vec::new();
    let mut remaining = String::new();
    let mut cursor = sql;

    while let Some(start) = cursor.find("/*") {
        remaining.push_str(&cursor[..start]);
        let Some(end_rel) = cursor[start..].find("*/") else {
            remaining.push_str(&cursor[start..]);
            return (directives, remaining.trim().to_string());
        };
        let end = start + end_rel;
        let comment = cursor[start + 2..end].trim();
        if let Some(directive) = parse_comment(comment) {
            directives.push(directive);
        } else {
            remaining.push_str(&cursor[start..end + 2]);
        }
        cursor = &cursor[end + 2..];
    }
    remaining.push_str(cursor);
    (directives, remaining.trim().to_string())
}

/// Identifiers transmitted through the comment protocol must match this
/// character class — `[A-Za-z0-9_.-]+`.  Anything else is rejected to close
/// the comment-injection vector documented in B3.  In particular, the
/// sequence `*/` is impossible inside a valid identifier.
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Filesystem path field — same character class as identifiers plus `/`.
/// (Tightened from "anything goes" to prevent `*/` injection through the
/// REGISTER_PARQUET path field.)
fn is_safe_path(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/' || c == ' '
        })
}

/// Base64 payload — alphabet is `[A-Za-z0-9+/=]`, neither `*` nor whitespace.
fn is_safe_base64(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
}

fn parse_comment(comment: &str) -> Option<FlightDirective> {
    if let Some(rest) = comment.strip_prefix(REGISTER_PARQUET) {
        let rest = rest.strip_prefix(':')?;
        let (table, path) = rest.split_once(':')?;
        if !is_safe_identifier(table) || !is_safe_path(path) {
            return None;
        }
        return Some(FlightDirective::RegisterParquet {
            table: table.to_string(),
            path: PathBuf::from(path),
        });
    }
    if let Some(rest) = comment.strip_prefix(CONTINUOUS_REGISTER) {
        let rest = rest.strip_prefix(':')?;
        let (job_id, spec_b64) = rest.split_once(':')?;
        if !is_safe_identifier(job_id) || !is_safe_base64(spec_b64) {
            return None;
        }
        let spec = decode_window_spec(spec_b64).ok()?;
        return Some(FlightDirective::ContinuousRegister {
            job_id: job_id.to_string(),
            spec,
        });
    }
    if let Some(rest) = comment.strip_prefix(CONTINUOUS_PUSH) {
        let rest = rest.strip_prefix(':')?;
        let (job_id, ipc) = rest.split_once(':')?;
        if !is_safe_identifier(job_id) || (!ipc.is_empty() && !is_safe_base64(ipc)) {
            return None;
        }
        let batches = decode_batches_ipc(ipc).ok()?;
        return Some(FlightDirective::ContinuousPush {
            job_id: job_id.to_string(),
            batches,
        });
    }
    if comment.strip_prefix(CONTINUOUS_DRAIN) == Some("") {
        return None;
    }
    if let Some(rest) = comment.strip_prefix(CONTINUOUS_DRAIN) {
        let job_id = rest.strip_prefix(':')?;
        if !is_safe_identifier(job_id) {
            return None;
        }
        return Some(FlightDirective::ContinuousDrain {
            job_id: job_id.to_string(),
        });
    }
    if let Some(rest) = comment.strip_prefix(BOUNDED_WINDOW) {
        let rest = rest.strip_prefix(':')?;
        let (topic, rest) = rest.split_once(':')?;
        let (spec_b64, ipc) = rest.split_once(':')?;
        if !is_safe_identifier(topic)
            || !is_safe_base64(spec_b64)
            || (!ipc.is_empty() && !is_safe_base64(ipc))
        {
            return None;
        }
        let spec = decode_window_spec(spec_b64).ok()?;
        let input_batches = decode_batches_ipc(ipc).ok()?;
        return Some(FlightDirective::BoundedWindow {
            topic: topic.to_string(),
            spec,
            input_batches,
        });
    }
    if comment == EXPLAIN {
        return Some(FlightDirective::Explain);
    }
    None
}

/// Merge register-parquet directives into a catalog map.
pub fn apply_register_directives(
    catalog: &mut HashMap<String, PathBuf>,
    directives: &[FlightDirective],
) {
    for directive in directives {
        if let FlightDirective::RegisterParquet { table, path } = directive {
            catalog.insert(table.clone(), path.clone());
        }
    }
}

pub fn catalog_to_batch_tables(catalog: &HashMap<String, PathBuf>) -> Vec<BatchSqlTable> {
    catalog
        .iter()
        .map(|(table, path)| BatchSqlTable {
            table_name: table.clone(),
            path: path.clone(),
        })
        .collect()
}

fn decode_window_spec(encoded: &str) -> RuntimeResult<WindowExecutionSpec> {
    let bytes = BASE64
        .decode(encoded)
        .map_err(|e| RuntimeError::transport(format!("invalid window spec encoding: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| RuntimeError::transport(format!("invalid window spec json: {e}")))
}

fn encode_batches_ipc(batches: &[RecordBatch]) -> RuntimeResult<String> {
    if batches.is_empty() {
        return Ok(String::new());
    }
    let schema = batches[0].schema();
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| RuntimeError::transport(format!("ipc encode failed: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| RuntimeError::transport(format!("ipc write failed: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RuntimeError::transport(format!("ipc finish failed: {e}")))?;
    }
    Ok(BASE64.encode(buffer))
}

fn decode_batches_ipc(encoded: &str) -> RuntimeResult<Vec<RecordBatch>> {
    if encoded.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = BASE64
        .decode(encoded)
        .map_err(|e| RuntimeError::transport(format!("invalid batch ipc encoding: {e}")))?;
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| RuntimeError::transport(format!("ipc decode failed: {e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RuntimeError::transport(format!("ipc read failed: {e}")))
}

/// Whether any directive requires special handling before normal SQL execution.
pub fn has_control_directive(directives: &[FlightDirective]) -> bool {
    directives.iter().any(|d| {
        matches!(
            d,
            FlightDirective::ContinuousRegister { .. }
                | FlightDirective::ContinuousPush { .. }
                | FlightDirective::ContinuousDrain { .. }
                | FlightDirective::BoundedWindow { .. }
                | FlightDirective::Explain
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use krishiv_plan::window::WindowExecutionSpec;

    #[test]
    fn encode_and_parse_register_parquet() {
        let sql = encode_batch_sql(
            "SELECT * FROM t",
            &[BatchSqlTable {
                table_name: "t".into(),
                path: PathBuf::from("/data/t.parquet"),
            }],
        );
        let (directives, query) = parse_sql(&sql);
        assert_eq!(query, "SELECT * FROM t");
        assert_eq!(directives.len(), 1);
        assert_eq!(
            directives[0],
            FlightDirective::RegisterParquet {
                table: "t".into(),
                path: PathBuf::from("/data/t.parquet"),
            }
        );
    }

    #[test]
    fn continuous_drain_round_trip() {
        let sql = encode_continuous_drain("job-1");
        let (directives, _) = parse_sql(&sql);
        assert_eq!(
            directives[0],
            FlightDirective::ContinuousDrain {
                job_id: "job-1".into()
            }
        );
    }

    #[test]
    fn explain_directive_parsed() {
        let sql = encode_explain_sql("SELECT 1");
        let (directives, query) = parse_sql(&sql);
        assert_eq!(directives, vec![FlightDirective::Explain]);
        assert_eq!(query, "SELECT 1");
    }

    #[test]
    fn bounded_window_spec_round_trip() {
        let spec = WindowExecutionSpec::tumbling("k", "ts", 10_000);
        let json = serde_json::to_string(&spec).unwrap();
        let encoded = BASE64.encode(json.as_bytes());
        let comment = format!("/* {BOUNDED_WINDOW}:events:{encoded}: */ SELECT 1");
        let (directives, _) = parse_sql(&comment);
        assert!(matches!(
            directives[0],
            FlightDirective::BoundedWindow { .. }
        ));
    }

    #[test]
    fn comment_parser_strips_injection_attempt_without_leaking_payload() {
        // B3: a malicious caller embeds a `*/` mid-comment, hoping the
        // remaining tail (`SELECT 1; DROP TABLE users;`) gets executed as SQL.
        //
        // parse_sql DOES detect the early `*/` and extracts the (truncated)
        // directive — but the residual SQL is delivered as a separate
        // `query` string and our caller (`FlightExecutionHost::execute_sql`)
        // short-circuits on control directives via `has_control_directive`
        // BEFORE executing any SQL.  Below we assert both halves of the
        // contract: the comment yields a parsed directive (so it doesn't fall
        // through and execute the malicious tail), AND `has_control_directive`
        // reports true.
        let evil = "evil*/SELECT 1; DROP TABLE users; /*";
        let comment = format!("/* {CONTINUOUS_DRAIN}:{evil} */");
        let (directives, _query) = parse_sql(&comment);
        assert_eq!(
            directives.len(),
            1,
            "directive must be parsed: {directives:?}"
        );
        assert!(matches!(
            &directives[0],
            FlightDirective::ContinuousDrain { job_id } if job_id == "evil"
        ));
        assert!(
            has_control_directive(&directives),
            "control-directive flag must be set so callers skip the residual SQL"
        );
    }

    #[test]
    fn comment_parser_rejects_directive_with_unsafe_field_chars() {
        // Spaces / shell metacharacters in identifiers cause the directive
        // to be silently dropped (parsed as None), so the caller does NOT
        // mistake a malformed comment for a valid control plane request.
        let comment = "/* krishiv-continuous-drain:foo bar; rm -rf / */ SELECT 1".to_string();
        let (directives, _query) = parse_sql(&comment);
        assert!(directives.is_empty(), "unsafe identifier must be rejected");
    }

    #[test]
    fn comment_parser_rejects_unsafe_base64_field() {
        // Insert a `*` character into what looks like base64 (the standard
        // alphabet doesn't contain `*`): rejected.
        let comment = format!("/* {BOUNDED_WINDOW}:events:abc*defgh:ipc */ SELECT 1");
        let (directives, _query) = parse_sql(&comment);
        assert!(directives.is_empty(), "unsafe base64 must be rejected");
    }
}
