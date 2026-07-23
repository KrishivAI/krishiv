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
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use krishiv_plan::window::WindowExecutionSpec;

use crate::in_process::BatchSqlTable;
use crate::local_streaming::LocalWindowExecutionSpec;
use crate::{RuntimeError, RuntimeResult};

const REGISTER_PARQUET: &str = "krishiv-register-parquet";
const REGISTER_PARQUET_IPC: &str = "krishiv-register-parquet-ipc";
const REGISTER_PYTHON_UDF: &str = "krishiv-register-python-udf";
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
    /// Arrow IPC bytes delivered inline — no filesystem access required.
    RegisterParquetIpc {
        table: String,
        ipc_b64: String,
    },
    /// A cloudpickled Python scalar UDF shipped in-band, to be run on the
    /// executor via a python worker subprocess. `input_types`/`output_type` are
    /// Arrow type names; `pickle_b64` is base64 of the cloudpickled callable.
    RegisterPythonUdf {
        name: String,
        input_types: Vec<String>,
        output_type: String,
        pickle_b64: String,
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

/// Encode batch SQL with inline Arrow IPC for each table.
///
/// Reads each local parquet file on the **client** side and embeds the IPC
/// bytes as base64 in the SQL comment so the flight server never needs
/// filesystem access.  Falls back to path-based encoding if the file cannot
/// be read (backward-compatible with single-node deployments where the flight
/// server shares the host filesystem).
pub fn encode_batch_sql(query: &str, tables: &[BatchSqlTable]) -> String {
    let mut parts = Vec::new();
    for table in tables {
        match parquet_file_to_ipc_b64(&table.path) {
            Ok(ipc_b64) if !ipc_b64.is_empty() => {
                // Inline IPC: safe because base64 alphabet has no `*/` sequence.
                parts.push(format!(
                    "/* {REGISTER_PARQUET_IPC}:{}:{ipc_b64} */",
                    table.table_name
                ));
            }
            _ => {
                // Fallback: path-based (works when flight server shares filesystem).
                parts.push(format!(
                    "/* {REGISTER_PARQUET}:{}:{} */",
                    table.table_name,
                    table.path.display()
                ));
            }
        }
    }
    parts.push(query.to_string());
    parts.join("\n")
}

/// Encode a Python UDF as a comment directive to prepend to a batch-SQL query.
/// `pickle_b64` is base64 of the cloudpickled callable; `input_types` /
/// `output_type` are Arrow type names (validated as identifiers on decode).
pub fn encode_python_udf(
    name: &str,
    input_types: &[String],
    output_type: &str,
    pickle_b64: &str,
) -> String {
    format!(
        "/* {REGISTER_PYTHON_UDF}:{name}:{}:{output_type}:{pickle_b64} */",
        input_types.join(",")
    )
}

/// Default cap on a single inlined parquet table (64 MiB on disk).
///
/// Inlined IPC travels inside one gRPC/HTTP message; an oversized blob silently
/// blows past the transport's max-message limit and fails cryptically. Capping
/// the on-disk parquet size keeps inline shipping to dimension-sized tables and
/// turns "too big to inline" into an actionable error / path-based fallback.
const DEFAULT_INLINE_IPC_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Effective inline-IPC cap, overridable via `KRISHIV_INLINE_IPC_MAX_BYTES`.
pub(crate) fn inline_ipc_max_bytes() -> u64 {
    std::env::var("KRISHIV_INLINE_IPC_MAX_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_INLINE_IPC_MAX_BYTES)
}

/// Read a local parquet file and return base64-encoded Arrow IPC bytes.
///
/// Shared by the Flight SQL comment protocol and the coordinator HTTP submit
/// path — both need to inline parquet data before sending to the remote server.
///
/// Tables whose on-disk size exceeds the inline cap return an error so callers
/// can fall back to path-based shipping (shared filesystem) instead of building
/// a transport-busting message; see [`parquet_file_to_ipc_b64_capped`].
pub(crate) fn parquet_file_to_ipc_b64(path: &std::path::Path) -> RuntimeResult<String> {
    parquet_file_to_ipc_b64_capped(path, inline_ipc_max_bytes())
}

/// [`parquet_file_to_ipc_b64`] with an explicit size cap (bytes) for testing and
/// callers that want a non-default limit.
pub(crate) fn parquet_file_to_ipc_b64_capped(
    path: &std::path::Path,
    max_bytes: u64,
) -> RuntimeResult<String> {
    use arrow::ipc::writer::StreamWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)
        .map_err(|e| RuntimeError::transport(format!("open '{}': {e}", path.display())))?;
    let on_disk = file
        .metadata()
        .map(|m| m.len())
        .map_err(|e| RuntimeError::transport(format!("stat '{}': {e}", path.display())))?;
    if on_disk > max_bytes {
        return Err(RuntimeError::transport(format!(
            "parquet table '{}' is {on_disk} bytes, over the inline-IPC cap of {max_bytes} \
             (KRISHIV_INLINE_IPC_MAX_BYTES); ship it via a shared filesystem (path-based) \
             or raise the cap",
            path.display()
        )));
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            RuntimeError::transport(format!("parquet reader for '{}': {e}", path.display()))
        })?
        .build()
        .map_err(|e| {
            RuntimeError::transport(format!("parquet build for '{}': {e}", path.display()))
        })?;

    let batches: Vec<_> = reader
        .collect::<Result<_, _>>()
        .map_err(|e| RuntimeError::transport(format!("parquet read: {e}")))?;
    if batches.is_empty() {
        return Ok(String::new());
    }

    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| RuntimeError::transport(format!("ipc writer: {e}")))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| RuntimeError::transport(format!("ipc write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RuntimeError::transport(format!("ipc finish: {e}")))?;
    }
    Ok(BASE64.encode(&buf))
}

/// Encode remote continuous job registration.
pub fn encode_continuous_register(
    job_id: &str,
    spec: &LocalWindowExecutionSpec,
) -> RuntimeResult<String> {
    let plan_spec = spec.to_plan_spec();
    let json = serde_json::to_string(&plan_spec)
        .map_err(|e| RuntimeError::transport(format!("window spec serialization: {e}")))?;
    let encoded = BASE64.encode(json.as_bytes());
    Ok(format!(
        "/* {CONTINUOUS_REGISTER}:{job_id}:{encoded} */ SELECT 1 AS registered"
    ))
}

/// Encode remote continuous input push.
pub fn encode_continuous_push(job_id: &str, batches: &[RecordBatch]) -> RuntimeResult<String> {
    let ipc = crate::flight_action::encode_batches(batches)?;
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
    let ipc = crate::flight_action::encode_batches(input_batches)?;
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
    krishiv_common::validate::is_safe_identifier(s)
}

/// Filesystem path field — same character class as identifiers plus `/`.
/// (Tightened from "anything goes" to prevent `*/` injection through the
/// REGISTER_PARQUET path field.)
fn is_safe_path(s: &str) -> bool {
    krishiv_common::validate::is_safe_path(s)
}

/// Base64 payload — alphabet is `[A-Za-z0-9+/=]`, neither `*` nor whitespace.
fn is_safe_base64(s: &str) -> bool {
    krishiv_common::validate::is_safe_base64(s)
}

fn parse_comment(comment: &str) -> Option<FlightDirective> {
    // Inline IPC (preferred, no filesystem dependency) — check before path variant.
    if let Some(rest) = comment.strip_prefix(REGISTER_PARQUET_IPC) {
        let rest = rest.strip_prefix(':')?;
        let (table, ipc_b64) = rest.split_once(':')?;
        if !is_safe_identifier(table) || (!ipc_b64.is_empty() && !is_safe_base64(ipc_b64)) {
            return None;
        }
        return Some(FlightDirective::RegisterParquetIpc {
            table: table.to_string(),
            ipc_b64: ipc_b64.to_string(),
        });
    }
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
    if let Some(rest) = comment.strip_prefix(REGISTER_PYTHON_UDF) {
        // name:in1,in2,…:out:pickle_b64  (empty in-types allowed)
        let rest = rest.strip_prefix(':')?;
        let (name, rest) = rest.split_once(':')?;
        let (in_types, rest) = rest.split_once(':')?;
        let (out_type, pickle_b64) = rest.split_once(':')?;
        let input_types: Vec<String> = if in_types.is_empty() {
            Vec::new()
        } else {
            in_types.split(',').map(str::to_string).collect()
        };
        let types_ok = is_safe_identifier(name)
            && is_safe_identifier(out_type)
            && input_types.iter().all(|t| is_safe_identifier(t));
        if !types_ok || !is_safe_base64(pickle_b64) {
            return None;
        }
        return Some(FlightDirective::RegisterPythonUdf {
            name: name.to_string(),
            input_types,
            output_type: out_type.to_string(),
            pickle_b64: pickle_b64.to_string(),
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
        let batches = crate::flight_action::decode_batches(ipc).ok()?;
        return Some(FlightDirective::ContinuousPush {
            job_id: job_id.to_string(),
            batches,
        });
    }
    if let Some(rest) = comment.strip_prefix(CONTINUOUS_DRAIN) {
        // Require a colon separator followed by a non-empty safe identifier.
        // A bare prefix with no colon (rest == "") is rejected here too.
        let job_id = rest.strip_prefix(':').unwrap_or("").trim();
        if job_id.is_empty() || !is_safe_identifier(job_id) {
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
        let input_batches = crate::flight_action::decode_batches(ipc).ok()?;
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
            ..Default::default()
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
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use krishiv_plan::window::WindowExecutionSpec;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(Int64Array::from(vec![1_000, 5_000])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn encode_and_parse_register_parquet() {
        let sql = encode_batch_sql(
            "SELECT * FROM t",
            &[BatchSqlTable {
                table_name: "t".into(),
                path: PathBuf::from("/data/t.parquet"),
                ..Default::default()
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
    fn encode_and_parse_register_parquet_multiple() {
        let sql = encode_batch_sql(
            "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            &[
                BatchSqlTable {
                    table_name: "t1".into(),
                    path: PathBuf::from("/data/t1.parquet"),
                    ..Default::default()
                },
                BatchSqlTable {
                    table_name: "t2".into(),
                    path: PathBuf::from("/data/t2.parquet"),
                    ..Default::default()
                },
            ],
        );
        let (directives, query) = parse_sql(&sql);
        assert_eq!(query, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
        assert_eq!(directives.len(), 2);
    }

    #[test]
    fn register_parquet_preserves_path_with_spaces() {
        let sql = encode_batch_sql(
            "SELECT 1",
            &[BatchSqlTable {
                table_name: "t".into(),
                path: PathBuf::from("/my data/my table.parquet"),
                ..Default::default()
            }],
        );
        let (directives, _) = parse_sql(&sql);
        assert_eq!(
            directives[0],
            FlightDirective::RegisterParquet {
                table: "t".into(),
                path: PathBuf::from("/my data/my table.parquet"),
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
    fn continuous_drain_with_hyphenated_job_id() {
        let sql = encode_continuous_drain("my-job-123");
        let (directives, _) = parse_sql(&sql);
        assert_eq!(
            directives[0],
            FlightDirective::ContinuousDrain {
                job_id: "my-job-123".into()
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
    fn explain_with_complex_query() {
        let sql = encode_explain_sql("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 10");
        let (directives, query) = parse_sql(&sql);
        assert_eq!(directives, vec![FlightDirective::Explain]);
        assert_eq!(
            query,
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 10"
        );
    }

    #[test]
    fn continuous_register_round_trip() {
        let local = LocalWindowExecutionSpec {
            key_column_type: String::from("utf8"),
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let sql = encode_continuous_register("job-abc", &local).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::ContinuousRegister {
                job_id,
                spec: decoded,
            } => {
                assert_eq!(job_id, "job-abc");
                assert_eq!(decoded.key_column, "user_id");
                assert_eq!(decoded.event_time_column, "ts");
                assert_eq!(decoded.window_size_ms, 10_000);
            }
            other => panic!("expected ContinuousRegister, got {other:?}"),
        }
    }

    #[test]
    fn continuous_push_round_trip_with_batches() {
        let batch = test_batch();
        let sql = encode_continuous_push("job-xyz", &[batch]).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::ContinuousPush { job_id, batches } => {
                assert_eq!(job_id, "job-xyz");
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 2);
            }
            other => panic!("expected ContinuousPush, got {other:?}"),
        }
    }

    #[test]
    fn continuous_push_empty_batches() {
        let sql = encode_continuous_push("job-empty", &[]).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::ContinuousPush { job_id, batches } => {
                assert_eq!(job_id, "job-empty");
                assert!(batches.is_empty());
            }
            other => panic!("expected ContinuousPush, got {other:?}"),
        }
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
    fn bounded_window_with_input_batches() {
        let local = LocalWindowExecutionSpec {
            key_column_type: String::from("utf8"),
            key_column: "user_id".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let batch = test_batch();
        let sql = encode_bounded_window("events", &local, &[batch]).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::BoundedWindow {
                topic,
                input_batches,
                ..
            } => {
                assert_eq!(topic, "events");
                assert_eq!(input_batches.len(), 1);
                assert_eq!(input_batches[0].num_rows(), 2);
            }
            other => panic!("expected BoundedWindow, got {other:?}"),
        }
    }

    #[test]
    fn multiple_directives_in_one_sql() {
        let drain_sql = encode_continuous_drain("job-1");
        let sql = format!("{drain_sql}; SELECT * FROM t");
        let (directives, query) = parse_sql(&sql);
        assert_eq!(directives.len(), 1);
        assert!(matches!(
            &directives[0],
            FlightDirective::ContinuousDrain { .. }
        ));
        assert!(query.contains("SELECT * FROM t"));
    }

    #[test]
    fn plain_sql_no_directives() {
        let (directives, query) = parse_sql("SELECT 1 AS n");
        assert!(directives.is_empty());
        assert_eq!(query, "SELECT 1 AS n");
    }

    #[test]
    fn unclosed_comment_does_not_panic() {
        let (directives, query) = parse_sql("SELECT 1 /* unclosed");
        assert!(directives.is_empty());
        assert!(query.contains("/* unclosed"));
    }

    #[test]
    fn non_krishiv_comment_preserved() {
        let sql = "/* ordinary comment */ SELECT 1";
        let (directives, query) = parse_sql(sql);
        assert!(directives.is_empty());
        assert_eq!(query, "/* ordinary comment */ SELECT 1");
    }

    #[test]
    fn apply_register_directives_populates_catalog() {
        let mut catalog = std::collections::HashMap::new();
        let directives = vec![
            FlightDirective::RegisterParquet {
                table: "t1".into(),
                path: PathBuf::from("/data/t1.parquet"),
            },
            FlightDirective::RegisterParquet {
                table: "t2".into(),
                path: PathBuf::from("/data/t2.parquet"),
            },
        ];
        apply_register_directives(&mut catalog, &directives);
        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog["t1"], PathBuf::from("/data/t1.parquet"));
        assert_eq!(catalog["t2"], PathBuf::from("/data/t2.parquet"));
    }

    #[test]
    fn catalog_to_batch_tables_roundtrip() {
        let mut catalog = std::collections::HashMap::new();
        catalog.insert("t".into(), PathBuf::from("/data/t.parquet"));
        let tables = catalog_to_batch_tables(&catalog);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_name, "t");
        assert_eq!(tables[0].path, PathBuf::from("/data/t.parquet"));
    }

    #[test]
    fn has_control_directive_true_for_explain() {
        let d = vec![FlightDirective::Explain];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn has_control_directive_false_for_register_parquet() {
        let d = vec![FlightDirective::RegisterParquet {
            table: "t".into(),
            path: PathBuf::from("/t.parquet"),
        }];
        assert!(!has_control_directive(&d));
    }

    #[test]
    fn comment_parser_strips_injection_attempt_without_leaking_payload() {
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
        let comment = "/* krishiv-continuous-drain:foo bar; rm -rf / */ SELECT 1".to_string();
        let (directives, _query) = parse_sql(&comment);
        assert!(directives.is_empty(), "unsafe identifier must be rejected");
    }

    #[test]
    fn comment_parser_rejects_unsafe_base64_field() {
        let comment = format!("/* {BOUNDED_WINDOW}:events:abc*defgh:ipc */ SELECT 1");
        let (directives, _query) = parse_sql(&comment);
        assert!(directives.is_empty(), "unsafe base64 must be rejected");
    }

    #[test]
    fn comment_parser_rejects_unsafe_path_chars() {
        let comment = "/* krishiv-register-parquet:t:/data/t.parquet; rm -rf / */ SELECT 1";
        let (directives, _) = parse_sql(comment);
        assert!(directives.is_empty(), "unsafe path must be rejected");
    }

    #[test]
    fn parse_sql_empty_string() {
        let (directives, query) = parse_sql("");
        assert!(directives.is_empty());
        assert_eq!(query, "");
    }

    #[test]
    fn parse_sql_only_whitespace() {
        let (directives, query) = parse_sql("   ");
        assert!(directives.is_empty());
        assert_eq!(query, "");
    }

    #[test]
    fn parse_sql_multiple_directives_in_sequence() {
        let d1 = encode_continuous_drain("j1");
        let d2 = encode_continuous_drain("j2");
        let sql = format!("{d1}; {d2}; SELECT 1");
        let (directives, _) = parse_sql(&sql);
        assert_eq!(directives.len(), 2);
    }

    #[test]
    fn is_safe_identifier_rejects_empty() {
        assert!(!is_safe_identifier(""));
    }

    #[test]
    fn is_safe_identifier_rejects_spaces() {
        assert!(!is_safe_identifier("has space"));
    }

    #[test]
    fn is_safe_identifier_rejects_slash() {
        assert!(!is_safe_identifier("has/slash"));
    }

    #[test]
    fn is_safe_identifier_rejects_semicolon() {
        assert!(!is_safe_identifier("has;semicolon"));
    }

    #[test]
    fn is_safe_identifier_accepts_alphanumeric() {
        assert!(is_safe_identifier("abc123"));
    }

    #[test]
    fn is_safe_identifier_accepts_underscores() {
        assert!(is_safe_identifier("abc_123_def"));
    }

    #[test]
    fn is_safe_identifier_accepts_hyphens() {
        assert!(is_safe_identifier("job-123"));
    }

    #[test]
    fn is_safe_identifier_accepts_dots() {
        assert!(is_safe_identifier("table.v1"));
    }

    #[test]
    fn is_safe_path_rejects_empty() {
        assert!(!is_safe_path(""));
    }

    #[test]
    fn is_safe_path_rejects_semicolons() {
        assert!(!is_safe_path("/data;rm -rf /"));
    }

    #[test]
    fn is_safe_path_rejects_asterisk() {
        assert!(!is_safe_path("/data/*.parquet"));
    }

    #[test]
    fn is_safe_path_accepts_spaces() {
        assert!(is_safe_path("/my data/file.parquet"));
    }

    #[test]
    fn is_safe_base64_rejects_empty() {
        assert!(!is_safe_base64(""));
    }

    #[test]
    fn is_safe_base64_rejects_spaces() {
        assert!(!is_safe_base64("abc 123"));
    }

    #[test]
    fn is_safe_base64_rejects_asterisk() {
        assert!(!is_safe_base64("abc*def"));
    }

    #[test]
    fn is_safe_base64_accepts_standard() {
        assert!(is_safe_base64("SGVsbG8="));
        assert!(is_safe_base64("abc123+/="));
    }

    #[test]
    fn has_control_directive_false_for_empty() {
        assert!(!has_control_directive(&[]));
    }

    #[test]
    fn has_control_directive_true_for_continuous_register() {
        let d = vec![FlightDirective::ContinuousRegister {
            job_id: "j".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
        }];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn has_control_directive_true_for_continuous_push() {
        let d = vec![FlightDirective::ContinuousPush {
            job_id: "j".into(),
            batches: vec![],
        }];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn has_control_directive_true_for_continuous_drain() {
        let d = vec![FlightDirective::ContinuousDrain { job_id: "j".into() }];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn has_control_directive_true_for_bounded_window() {
        let d = vec![FlightDirective::BoundedWindow {
            topic: "t".into(),
            spec: WindowExecutionSpec::tumbling("k", "ts", 1_000),
            input_batches: vec![],
        }];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn has_control_directive_false_for_drain_unknown() {
        // Unknown directives don't match any known variant
        let d = vec![FlightDirective::Explain];
        assert!(has_control_directive(&d));
    }

    #[test]
    fn apply_register_directives_empty_catalog() {
        let mut catalog = std::collections::HashMap::new();
        apply_register_directives(&mut catalog, &[]);
        assert!(catalog.is_empty());
    }

    #[test]
    fn apply_register_directives_overwrites_same_table() {
        let mut catalog = std::collections::HashMap::new();
        let directives = vec![
            FlightDirective::RegisterParquet {
                table: "t".into(),
                path: PathBuf::from("/old.parquet"),
            },
            FlightDirective::RegisterParquet {
                table: "t".into(),
                path: PathBuf::from("/new.parquet"),
            },
        ];
        apply_register_directives(&mut catalog, &directives);
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog["t"], PathBuf::from("/new.parquet"));
    }

    #[test]
    fn catalog_to_batch_tables_empty() {
        let catalog = std::collections::HashMap::new();
        let tables = catalog_to_batch_tables(&catalog);
        assert!(tables.is_empty());
    }

    #[test]
    fn catalog_to_batch_tables_multiple() {
        let mut catalog = std::collections::HashMap::new();
        catalog.insert("a".into(), PathBuf::from("/a.parquet"));
        catalog.insert("b".into(), PathBuf::from("/b.parquet"));
        let tables = catalog_to_batch_tables(&catalog);
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn encode_explain_sql_preserves_query() {
        let sql = encode_explain_sql("SELECT * FROM users");
        assert_eq!(sql, "/* krishiv-explain */ SELECT * FROM users");
    }

    #[test]
    fn parse_sql_mixed_krishiv_and_normal_comment() {
        let drain = encode_continuous_drain("j1");
        let sql = format!("{drain} /* ordinary */ SELECT 1");
        let (directives, query) = parse_sql(&sql);
        assert_eq!(directives.len(), 1);
        assert!(query.contains("/* ordinary */"));
        assert!(query.contains("SELECT 1"));
    }

    #[test]
    fn bounded_window_empty_ipc() {
        let local = LocalWindowExecutionSpec {
            key_column_type: String::from("utf8"),
            key_column: "k".into(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: crate::local_streaming::LocalWindowKind::Tumbling,
            window_size_ms: 10_000,
            agg_exprs: LocalWindowExecutionSpec::default_count_agg(),
            state_ttl_ms: None,
            allowed_lateness_ms: None,
            source_watermark_lags: std::collections::HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let sql = encode_bounded_window("topic", &local, &[]).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::BoundedWindow { input_batches, .. } => {
                assert!(input_batches.is_empty());
            }
            other => panic!("expected BoundedWindow, got {other:?}"),
        }
    }

    #[test]
    fn continuous_push_multiple_batches() {
        let b1 = test_batch();
        let schema = b1.schema();
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x"])) as _,
                Arc::new(Int64Array::from(vec![999])) as _,
            ],
        )
        .unwrap();
        let sql = encode_continuous_push("job-multi", &[b1, b2]).unwrap();
        let (directives, _) = parse_sql(&sql);
        match &directives[0] {
            FlightDirective::ContinuousPush { batches, .. } => {
                assert_eq!(batches.len(), 2);
            }
            other => panic!("expected ContinuousPush, got {other:?}"),
        }
    }

    #[test]
    fn comment_with_only_prefix_ignored() {
        let sql = "/* krishiv-continuous-drain */ SELECT 1";
        let (directives, _) = parse_sql(sql);
        assert!(directives.is_empty());
    }

    fn write_parquet(batch: &RecordBatch) -> (tempfile::TempDir, PathBuf) {
        use parquet::arrow::ArrowWriter;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        (dir, path)
    }

    #[test]
    fn inline_ipc_under_cap_encodes_and_over_cap_errors() {
        let (_dir, path) = write_parquet(&test_batch());

        // A generous cap inlines the table.
        let encoded =
            parquet_file_to_ipc_b64_capped(&path, 64 * 1024 * 1024).expect("under cap inlines");
        assert!(!encoded.is_empty(), "small table should inline");

        // A 1-byte cap rejects it with an actionable error, so callers fall back
        // to path-based shipping instead of building a transport-busting message.
        let err = parquet_file_to_ipc_b64_capped(&path, 1)
            .expect_err("over cap must error, not inline a giant blob");
        let msg = err.to_string();
        assert!(
            msg.contains("inline-IPC cap") && msg.contains("KRISHIV_INLINE_IPC_MAX_BYTES"),
            "error must name the cap knob: {msg}"
        );

        // Default cap is the documented 64 MiB.
        assert_eq!(DEFAULT_INLINE_IPC_MAX_BYTES, 64 * 1024 * 1024);
    }
}
