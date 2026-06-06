//! Streaming window plan configuration and fragment encoding (unified execution).

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Window operator kind for streaming execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowKind {
    Tumbling,
    Sliding,
    Session,
}

/// Aggregate function in a streaming window plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowAggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// One aggregate in a windowed stream plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowAgg {
    pub kind: WindowAggKind,
    pub input_column: String,
    pub output_column: String,
}

impl WindowAgg {
    pub fn count(output_column: impl Into<String>) -> Self {
        Self {
            kind: WindowAggKind::Count,
            input_column: String::new(),
            output_column: output_column.into(),
        }
    }
}

/// Full specification for a keyed, windowed streaming operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowExecutionSpec {
    pub key_column: String,
    /// Arrow type of the key column as a simple string tag: `"int32"`,
    /// `"int64"`, `"float64"`, `"utf8"`, or `"bool"`.  Defaults to `"utf8"`.
    #[serde(default = "default_key_type")]
    pub key_column_type: String,
    pub event_time_column: String,
    pub watermark_lag_ms: u64,
    pub window_kind: WindowKind,
    pub window_size_ms: u64,
    /// Slide step for sliding windows.
    pub slide_ms: Option<u64>,
    /// Session gap for session windows.
    pub session_gap_ms: Option<u64>,
    pub agg_exprs: Vec<WindowAgg>,
    pub state_ttl_ms: Option<u64>,
    /// Per-source fixed-lag watermark (ms). When non-empty, effective watermark is the
    /// minimum across all configured sources (R5.2 multi-source reconciliation).
    #[serde(default)]
    pub source_watermark_lags: HashMap<String, u64>,
    /// Column identifying the input source for multi-source watermark propagation.
    #[serde(default)]
    pub source_id_column: Option<String>,
}

fn default_key_type() -> String {
    String::from("utf8")
}

impl WindowExecutionSpec {
    pub fn default_count_agg() -> Vec<WindowAgg> {
        vec![WindowAgg::count("count")]
    }

    pub fn tumbling(
        key_column: impl Into<String>,
        event_time_column: impl Into<String>,
        window_size_ms: u64,
    ) -> Self {
        Self {
            key_column: key_column.into(),
            key_column_type: default_key_type(),
            event_time_column: event_time_column.into(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: Self::default_count_agg(),
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        }
    }
}

/// Versioned prefix for lossless JSON-encoded [`WindowExecutionSpec`] values.
pub const WINDOW_EXECUTION_SPEC_PREFIX: &str = "stream:spec:v1:";

/// Validate and losslessly encode a window execution specification.
///
/// An empty aggregate list retains the historical default-count behavior and
/// is normalized to one `count` aggregate in the encoded representation.
pub fn encode_window_execution_spec(spec: &WindowExecutionSpec) -> Result<String, PlanError> {
    let mut normalized = spec.clone();
    if normalized.agg_exprs.is_empty() {
        normalized.agg_exprs = WindowExecutionSpec::default_count_agg();
    }
    validate_window_execution_spec(&normalized)?;
    let json =
        serde_json::to_string(&normalized).map_err(|error| PlanError::Encode(error.to_string()))?;
    Ok(format!("{WINDOW_EXECUTION_SPEC_PREFIX}{json}"))
}

/// Decode a lossless window specification, accepting legacy compact fragments
/// for backward compatibility.
pub fn decode_window_execution_spec(encoded: &str) -> Result<WindowExecutionSpec, PlanError> {
    if let Some(json) = encoded.strip_prefix(WINDOW_EXECUTION_SPEC_PREFIX) {
        let spec: WindowExecutionSpec =
            serde_json::from_str(json).map_err(|error| PlanError::Parse(error.to_string()))?;
        validate_window_execution_spec(&spec)?;
        return Ok(spec);
    }

    let parsed = parse_stream_fragment(encoded)?;
    let (slide_ms, session_gap_ms) = match parsed.window_kind {
        WindowKind::Tumbling => (None, None),
        WindowKind::Sliding => (parsed.slide_ms, None),
        WindowKind::Session => (None, parsed.session_gap_ms),
    };
    let spec = WindowExecutionSpec {
        key_column: parsed.key_col,
        key_column_type: String::from("utf8"),
        event_time_column: parsed.time_col,
        watermark_lag_ms: parsed.lag_ms,
        window_kind: parsed.window_kind,
        window_size_ms: parsed.window_ms,
        slide_ms,
        session_gap_ms,
        agg_exprs: vec![parsed.agg],
        state_ttl_ms: parsed.ttl_ms,
        source_watermark_lags: parsed.source_watermark_lags,
        source_id_column: parsed.source_id_column,
    };
    validate_window_execution_spec(&spec)?;
    Ok(spec)
}

/// Validate invariants required by all continuous window executors.
pub fn validate_window_execution_spec(spec: &WindowExecutionSpec) -> Result<(), PlanError> {
    if spec.key_column.trim().is_empty() {
        return Err(PlanError::Validation(String::from(
            "window key_column must not be empty",
        )));
    }
    if spec.event_time_column.trim().is_empty() {
        return Err(PlanError::Validation(String::from(
            "window event_time_column must not be empty",
        )));
    }
    if spec.window_size_ms == 0 {
        return Err(PlanError::Validation(String::from(
            "window_size_ms must be greater than zero",
        )));
    }
    if spec.window_kind == WindowKind::Sliding && spec.slide_ms.unwrap_or(spec.window_size_ms) == 0
    {
        return Err(PlanError::Validation(String::from(
            "sliding window slide_ms must be greater than zero",
        )));
    }
    if spec.window_kind == WindowKind::Session
        && spec.session_gap_ms.unwrap_or(spec.window_size_ms) == 0
    {
        return Err(PlanError::Validation(String::from(
            "session window session_gap_ms must be greater than zero",
        )));
    }
    if spec.state_ttl_ms == Some(0) {
        return Err(PlanError::Validation(String::from(
            "window state_ttl_ms must be greater than zero",
        )));
    }
    if spec.agg_exprs.is_empty() {
        return Err(PlanError::Validation(String::from(
            "window execution requires at least one aggregate",
        )));
    }

    let mut output_columns = HashSet::with_capacity(spec.agg_exprs.len());
    for aggregate in &spec.agg_exprs {
        if aggregate.output_column.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "window aggregate output_column must not be empty",
            )));
        }
        if aggregate.kind != WindowAggKind::Count && aggregate.input_column.trim().is_empty() {
            return Err(PlanError::Validation(format!(
                "{:?} window aggregate requires a non-empty input_column",
                aggregate.kind
            )));
        }
        if !output_columns.insert(aggregate.output_column.as_str()) {
            return Err(PlanError::Validation(format!(
                "duplicate window aggregate output_column '{}'",
                aggregate.output_column
            )));
        }
    }

    if let Some(source_id_column) = &spec.source_id_column
        && source_id_column.trim().is_empty()
    {
        return Err(PlanError::Validation(String::from(
            "source_id_column must not be empty when configured",
        )));
    }
    if !spec.source_watermark_lags.is_empty() && spec.source_id_column.is_none() {
        return Err(PlanError::Validation(String::from(
            "source_id_column is required when source_watermark_lags are configured",
        )));
    }
    if spec
        .source_watermark_lags
        .keys()
        .any(|source_id| source_id.trim().is_empty())
    {
        return Err(PlanError::Validation(String::from(
            "source_watermark_lags contains an empty source id",
        )));
    }
    Ok(())
}

/// Escape `:` and `\` in compact-fragment values so that the colon separator
/// cannot be confused with literal characters inside column names or source ids.
fn escape_compact_value(s: &str) -> String {
    s.replace('\\', "\\\\").replace(':', "\\:")
}

/// Reverse [`escape_compact_value`].
fn unescape_compact_value(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(&next) = chars.peek() {
                if next == ':' || next == '\\' {
                    chars.next();
                    result.push(next);
                    continue;
                }
            }
        }
        result.push(ch);
    }
    result
}

/// Encode a window spec as an executor plan fragment description.
pub fn encode_stream_fragment(spec: &WindowExecutionSpec) -> String {
    let aggs: Vec<String> = if spec.agg_exprs.is_empty() {
        vec!["agg=count".to_string()]
    } else {
        spec.agg_exprs.iter().map(encode_agg).collect()
    };
    let agg = aggs.join(";");

    let prefix = match spec.window_kind {
        WindowKind::Tumbling => "stream:tw",
        WindowKind::Sliding => "stream:sw",
        WindowKind::Session => "stream:ses",
    };

    let extra = match spec.window_kind {
        WindowKind::Tumbling => String::new(),
        WindowKind::Sliding => format!(
            ":slide={}",
            spec.slide_ms.unwrap_or(spec.window_size_ms)
        ),
        WindowKind::Session => format!(
            ":gap={}",
            spec.session_gap_ms.unwrap_or(spec.window_size_ms)
        ),
    };

    let ttl = spec
        .state_ttl_ms
        .map(|ms| format!(":ttl={ms}"))
        .unwrap_or_default();

    // Encode multi-source watermark fields: srcid=<col> and srcs=id1:lag1,id2:lag2.
    // These are omitted when not configured so the fragment stays compact for the
    // common single-source case.
    let srcid = spec
        .source_id_column
        .as_deref()
        .map(|c| format!(":srcid={}", escape_compact_value(c)))
        .unwrap_or_default();

    let srcs = if spec.source_watermark_lags.is_empty() {
        String::new()
    } else {
        // Sort by key for deterministic encoding (HashMap iteration order is unspecified).
        let mut pairs: Vec<_> = spec.source_watermark_lags.iter().collect();
        pairs.sort_by_key(|(k, _)| k.as_str());
        let encoded: Vec<String> = pairs
            .iter()
            .map(|(id, lag)| {
                format!("{}:{lag}", escape_compact_value(id))
            })
            .collect();
        format!(":srcs={}", encoded.join(","))
    };

    format!(
        "{prefix}:key={}:time={}:win={}:lag={}:{agg}{extra}{ttl}{srcid}{srcs}",
        escape_compact_value(&spec.key_column),
        escape_compact_value(&spec.event_time_column),
        spec.window_size_ms,
        spec.watermark_lag_ms,
    )
}

fn encode_agg(agg: &WindowAgg) -> String {
    match agg.kind {
        WindowAggKind::Count => "agg=count".to_string(),
        WindowAggKind::Sum => format!("agg=sum:col={}", agg.input_column),
        WindowAggKind::Min => format!("agg=min:col={}", agg.input_column),
        WindowAggKind::Max => format!("agg=max:col={}", agg.input_column),
        WindowAggKind::Avg => format!("agg=avg:col={}", agg.input_column),
    }
}

/// Parsed streaming window fragment (all window kinds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedStreamFragment {
    pub window_kind: WindowKind,
    pub key_col: String,
    pub time_col: String,
    pub window_ms: u64,
    pub lag_ms: u64,
    pub slide_ms: Option<u64>,
    pub session_gap_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub agg: WindowAgg,
    /// Source-id column name for multi-source watermark tracking.
    pub source_id_column: Option<String>,
    /// Per-source fixed watermark lags: source_id → lag_ms.
    pub source_watermark_lags: HashMap<String, u64>,
}

use crate::PlanError;

/// Parse `stream:tw|sw|ses:...` fragment strings.
pub fn parse_stream_fragment(fragment: &str) -> Result<ParsedStreamFragment, PlanError> {
    let (window_kind, payload) = if let Some(p) = fragment.strip_prefix("stream:tw:") {
        (WindowKind::Tumbling, p)
    } else if let Some(p) = fragment.strip_prefix("stream:sw:") {
        (WindowKind::Sliding, p)
    } else if let Some(p) = fragment.strip_prefix("stream:ses:") {
        (WindowKind::Session, p)
    } else {
        return Err(PlanError::Parse(format!(
            "streaming fragment must start with stream:tw:, stream:sw:, or stream:ses:; got: {fragment}"
        )));
    };

    let mut key_col = None;
    let mut time_col = None;
    let mut window_ms = None;
    let mut lag_ms = None;
    let mut slide_ms = None;
    let mut session_gap_ms = None;
    let mut ttl_ms = None;
    let mut agg_kind: Option<String> = None;
    let mut agg_col: Option<String> = None;
    let mut source_id_column: Option<String> = None;
    let mut source_watermark_lags: HashMap<String, u64> = HashMap::new();

    for part in split_stream_fields(payload) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').ok_or_else(|| {
            PlanError::Parse(format!(
                "streaming fragment field must be k=v; got '{part}'"
            ))
        })?;
        match k.trim() {
            "key" => key_col = Some(unescape_compact_value(v.trim())),
            "time" => time_col = Some(unescape_compact_value(v.trim())),
            "win" => {
                window_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| PlanError::Parse(format!("invalid win value '{v}': {e}")))?,
                );
            }
            "lag" => {
                lag_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| PlanError::Parse(format!("invalid lag value '{v}': {e}")))?,
                );
            }
            "slide" => {
                slide_ms =
                    Some(v.trim().parse::<u64>().map_err(|e| {
                        PlanError::Parse(format!("invalid slide value '{v}': {e}"))
                    })?);
            }
            "gap" => {
                session_gap_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| PlanError::Parse(format!("invalid gap value '{v}': {e}")))?,
                );
            }
            "ttl" => {
                ttl_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| PlanError::Parse(format!("invalid ttl value '{v}': {e}")))?,
                );
            }
            "agg" => agg_kind = Some(v.trim().to_owned()),
            "col" => agg_col = Some(v.trim().to_owned()),
            "srcid" => source_id_column = Some(unescape_compact_value(v.trim())),
            "srcs" => {
                // Format: id1:lag1,id2:lag2  (ids may contain escaped colons)
                for pair in v.split(',') {
                    let pair = pair.trim();
                    if pair.is_empty() {
                        continue;
                    }
                    // Split on the first non-escaped colon.
                    let split_idx = pair
                        .char_indices()
                        .find(|(idx, ch)| {
                            *ch == ':' && !is_escaped_colon(pair, *idx)
                        })
                        .map(|(idx, _)| idx);
                    let split_idx = split_idx.ok_or_else(|| {
                        PlanError::Parse(format!(
                            "srcs entry must be id:lag_ms; got '{pair}'"
                        ))
                    })?;
                    let id = unescape_compact_value(&pair[..split_idx]);
                    let lag_str = &pair[split_idx + ':'.len_utf8()..];
                    let lag: u64 = lag_str.trim().parse().map_err(|e| {
                        PlanError::Parse(format!(
                            "invalid lag in srcs entry '{pair}': {e}"
                        ))
                    })?;
                    source_watermark_lags.insert(id.trim().to_owned(), lag);
                }
            }
            _ => {}
        }
    }

    let agg = match agg_kind.as_deref() {
        None | Some("count") => WindowAgg::count("count"),
        Some("sum") => WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=sum requires col=<column>",
                ))
            })?,
            output_column: format!("sum_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("min") => WindowAgg {
            kind: WindowAggKind::Min,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=min requires col=<column>",
                ))
            })?,
            output_column: format!("min_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("max") => WindowAgg {
            kind: WindowAggKind::Max,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=max requires col=<column>",
                ))
            })?,
            output_column: format!("max_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("avg") => WindowAgg {
            kind: WindowAggKind::Avg,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=avg requires col=<column>",
                ))
            })?,
            output_column: format!("avg_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some(other) => {
            return Err(PlanError::Parse(format!(
                "unknown streaming aggregate '{other}', expected count|sum|min|max|avg"
            )));
        }
    };

    Ok(ParsedStreamFragment {
        window_kind,
        key_col: key_col
            .ok_or_else(|| PlanError::Parse(String::from("stream fragment missing key=<col>")))?,
        time_col: time_col
            .ok_or_else(|| PlanError::Parse(String::from("stream fragment missing time=<col>")))?,
        window_ms: {
            let ms = window_ms.ok_or_else(|| {
                PlanError::Parse(String::from("stream fragment missing win=<ms>"))
            })?;
            if ms == 0 {
                return Err(PlanError::Parse(String::from(
                    "stream fragment window size must be > 0",
                )));
            }
            ms
        },
        lag_ms: lag_ms.unwrap_or(0),
        slide_ms,
        session_gap_ms,
        ttl_ms,
        agg,
        source_id_column,
        source_watermark_lags,
    })
}

const STREAM_FIELD_PREFIXES: &[&str] = &[
    "key=", "time=", "win=", "lag=", "slide=", "gap=", "ttl=", "agg=", "col=", "srcid=", "srcs=",
];

/// Return `true` if the colon at byte position `idx` is escaped by an odd
/// number of consecutive backslashes immediately preceding it.
fn is_escaped_colon(payload: &str, idx: usize) -> bool {
    let mut backslash_count = 0usize;
    let mut i = idx;
    while i > 0 {
        i -= 1;
        if payload.as_bytes()[i] == b'\\' {
            backslash_count += 1;
        } else {
            break;
        }
    }
    backslash_count % 2 == 1
}

fn split_stream_fields(payload: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut field_start = 0usize;

    for (idx, ch) in payload.char_indices() {
        if ch != ':' || idx == field_start {
            continue;
        }
        // Skip escaped colons so values like `key=col\:name` are not split.
        if is_escaped_colon(payload, idx) {
            continue;
        }
        let after_colon = &payload[idx + ch.len_utf8()..];
        if STREAM_FIELD_PREFIXES
            .iter()
            .any(|prefix| after_colon.starts_with(prefix))
        {
            fields.push(&payload[field_start..idx]);
            field_start = idx + ch.len_utf8();
        }
    }

    fields.push(&payload[field_start..]);
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_tumbling_fragment() {
        let spec = WindowExecutionSpec {
            key_column: "user_id".into(),
            key_column_type: default_key_type(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 1000,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 60_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![WindowAgg::count("count")],
            state_ttl_ms: Some(30_000),
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        let frag = encode_stream_fragment(&spec);
        let parsed = parse_stream_fragment(&frag).expect("parse");
        assert_eq!(parsed.window_kind, WindowKind::Tumbling);
        assert_eq!(parsed.window_ms, 60_000);
        assert_eq!(parsed.lag_ms, 1000);
        assert_eq!(parsed.ttl_ms, Some(30_000));
    }

    #[test]
    fn lossless_window_spec_roundtrip_preserves_all_aggregates() {
        let mut source_watermark_lags = HashMap::new();
        source_watermark_lags.insert(String::from("orders"), 1_000);
        source_watermark_lags.insert(String::from("payments"), 2_000);
        let spec = WindowExecutionSpec {
            key_column: String::from("customer_id"),
            key_column_type: default_key_type(),
            event_time_column: String::from("event_ts"),
            watermark_lag_ms: 250,
            window_kind: WindowKind::Sliding,
            window_size_ms: 60_000,
            slide_ms: Some(5_000),
            session_gap_ms: None,
            agg_exprs: vec![
                WindowAgg::count("event_count"),
                WindowAgg {
                    kind: WindowAggKind::Sum,
                    input_column: String::from("amount"),
                    output_column: String::from("gross_amount"),
                },
            ],
            state_ttl_ms: Some(600_000),
            source_watermark_lags,
            source_id_column: Some(String::from("source")),
        };

        let encoded = encode_window_execution_spec(&spec).unwrap();
        assert!(encoded.starts_with(WINDOW_EXECUTION_SPEC_PREFIX));
        assert_eq!(decode_window_execution_spec(&encoded).unwrap(), spec);
    }

    #[test]
    fn lossless_window_spec_normalizes_empty_aggregate_to_count() {
        let mut spec = WindowExecutionSpec::tumbling("key", "ts", 1_000);
        spec.agg_exprs.clear();

        let decoded =
            decode_window_execution_spec(&encode_window_execution_spec(&spec).unwrap()).unwrap();

        assert_eq!(decoded.agg_exprs, WindowExecutionSpec::default_count_agg());
    }

    #[test]
    fn window_spec_validation_rejects_invalid_execution_contracts() {
        let mut spec = WindowExecutionSpec::tumbling("", "ts", 1_000);
        assert!(validate_window_execution_spec(&spec).is_err());

        spec.key_column = String::from("key");
        spec.window_size_ms = 0;
        assert!(validate_window_execution_spec(&spec).is_err());

        spec.window_size_ms = 1_000;
        spec.source_watermark_lags
            .insert(String::from("orders"), 100);
        assert!(validate_window_execution_spec(&spec).is_err());

        spec.source_id_column = Some(String::from("source"));
        spec.agg_exprs.push(WindowAgg::count("count"));
        assert!(validate_window_execution_spec(&spec).is_err());
    }

    #[test]
    fn parse_sliding_fragment() {
        let frag = "stream:sw:key=key:time=ts:win=10000:lag=0:slide=5000:agg=count";
        let p = parse_stream_fragment(frag).expect("parse");
        assert_eq!(p.window_kind, WindowKind::Sliding);
        assert_eq!(p.slide_ms, Some(5000));
    }

    #[test]
    fn roundtrip_multi_source_watermark_fragment() {
        let mut source_watermark_lags = HashMap::new();
        source_watermark_lags.insert("orders".to_string(), 1_000);
        source_watermark_lags.insert("payments".to_string(), 2_500);
        let spec = WindowExecutionSpec {
            key_column: "customer_id".into(),
            key_column_type: default_key_type(),
            event_time_column: "event_ts".into(),
            watermark_lag_ms: 100,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 60_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![WindowAgg::count("count")],
            state_ttl_ms: Some(600_000),
            source_watermark_lags,
            source_id_column: Some("source_id".into()),
        };

        let fragment = encode_stream_fragment(&spec);
        assert!(
            fragment.contains("srcs=orders:1000,payments:2500"),
            "multi-source encoding should remain deterministic: {fragment}"
        );

        let parsed = parse_stream_fragment(&fragment).expect("parse multi-source fragment");
        assert_eq!(parsed.source_id_column.as_deref(), Some("source_id"));
        assert_eq!(parsed.source_watermark_lags.get("orders"), Some(&1_000));
        assert_eq!(parsed.source_watermark_lags.get("payments"), Some(&2_500));
        assert_eq!(parsed.source_watermark_lags.len(), 2);
    }

    #[test]
    fn parse_multi_source_watermark_with_colon_values() {
        let fragment =
            "stream:tw:key=k:time=ts:win=1000:lag=0:agg=count:srcid=source:srcs=a:10,b:20";
        let parsed = parse_stream_fragment(fragment).expect("parse");
        assert_eq!(parsed.source_watermark_lags.get("a"), Some(&10));
        assert_eq!(parsed.source_watermark_lags.get("b"), Some(&20));
    }

    #[test]
    fn parse_invalid_multi_source_lag_errors() {
        let fragment = "stream:tw:key=k:time=ts:win=1000:lag=0:agg=count:srcs=a:not-a-number";
        let err = parse_stream_fragment(fragment).expect_err("invalid lag should fail");
        assert!(
            err.to_string().contains("invalid lag in srcs entry"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn roundtrip_fragment_with_colon_in_column_name() {
        let spec = WindowExecutionSpec {
            key_column: "ns:user_id".into(),
            key_column_type: default_key_type(),
            event_time_column: "ts:ms".into(),
            watermark_lag_ms: 100,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 5_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![WindowAgg::count("count")],
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        let frag = encode_stream_fragment(&spec);
        let parsed = parse_stream_fragment(&frag).expect("parse escaped fragment");
        assert_eq!(parsed.key_col, "ns:user_id");
        assert_eq!(parsed.time_col, "ts:ms");
    }

    #[test]
    fn roundtrip_fragment_with_backslash_in_column_name() {
        let spec = WindowExecutionSpec {
            key_column: "path\\to".into(),
            key_column_type: default_key_type(),
            event_time_column: "ts".into(),
            watermark_lag_ms: 0,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 1_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![WindowAgg::count("count")],
            state_ttl_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
        };
        let frag = encode_stream_fragment(&spec);
        let parsed = parse_stream_fragment(&frag).expect("parse escaped backslash");
        assert_eq!(parsed.key_col, "path\\to");
    }

    #[test]
    fn roundtrip_multi_source_with_colon_in_source_id() {
        let mut source_watermark_lags = HashMap::new();
        source_watermark_lags.insert("ns:orders".to_string(), 1_000);
        let spec = WindowExecutionSpec {
            key_column: "customer_id".into(),
            key_column_type: default_key_type(),
            event_time_column: "event_ts".into(),
            watermark_lag_ms: 100,
            window_kind: WindowKind::Tumbling,
            window_size_ms: 60_000,
            slide_ms: None,
            session_gap_ms: None,
            agg_exprs: vec![WindowAgg::count("count")],
            state_ttl_ms: None,
            source_watermark_lags,
            source_id_column: Some("src:col".into()),
        };
        let frag = encode_stream_fragment(&spec);
        let parsed = parse_stream_fragment(&frag).expect("parse escaped multi-source");
        assert_eq!(parsed.source_id_column.as_deref(), Some("src:col"));
        assert_eq!(parsed.source_watermark_lags.get("ns:orders"), Some(&1_000));
    }
}
