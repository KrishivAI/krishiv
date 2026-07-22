//! Streaming window plan configuration and fragment encoding (unified execution).

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Window operator kind for streaming execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowKind {
    Tumbling,
    Sliding,
    Session,
    /// E3.1 — count-based window: closes every `size` rows, slides every `slide` rows.
    Count {
        size: u64,
        slide: u64,
    },
}

/// Aggregate function in a streaming window plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowAggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Sample standard deviation (Bessel-corrected, denominator `n-1`).
    Stddev,
}

/// Comparison operator inside a [`WindowAggFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFilterCompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A float literal with bitwise equality so filter ASTs stay `Eq`-comparable.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FloatLiteral(pub f64);

impl PartialEq for FloatLiteral {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for FloatLiteral {}

/// Literal value a filter compares a column against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFilterValue {
    Int(i64),
    Float(FloatLiteral),
    Utf8(String),
    Bool(bool),
}

/// Typed per-aggregate row predicate for streaming windows.
///
/// This is the engine-internal lowering target for SQL
/// `AGG(x) FILTER (WHERE …)` and the `AGG(CASE WHEN … THEN x END)` idiom: a
/// small, serializable predicate AST the dataflow operators can evaluate with
/// plain Arrow compute kernels (the dataflow crate deliberately has no SQL or
/// DataFusion dependency). Rows failing the predicate (or where it evaluates
/// to NULL) do not feed the aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowAggFilter {
    /// `column <op> literal`. NULL column values compare as "no match".
    Compare {
        column: String,
        op: AggFilterCompareOp,
        value: AggFilterValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And(Box<WindowAggFilter>, Box<WindowAggFilter>),
    Or(Box<WindowAggFilter>, Box<WindowAggFilter>),
    Not(Box<WindowAggFilter>),
}

impl WindowAggFilter {
    /// Every column name the predicate references (for validation).
    pub fn columns(&self) -> Vec<&str> {
        match self {
            WindowAggFilter::Compare { column, .. }
            | WindowAggFilter::IsNull { column }
            | WindowAggFilter::IsNotNull { column } => vec![column.as_str()],
            WindowAggFilter::And(a, b) | WindowAggFilter::Or(a, b) => {
                let mut cols = a.columns();
                cols.extend(b.columns());
                cols
            }
            WindowAggFilter::Not(inner) => inner.columns(),
        }
    }
}

/// One aggregate in a windowed stream plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowAgg {
    pub kind: WindowAggKind,
    pub input_column: String,
    pub output_column: String,
    /// Optional row predicate (`FILTER (WHERE …)` / `CASE WHEN` lowering);
    /// rows failing it do not feed this aggregate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<WindowAggFilter>,
}

impl WindowAgg {
    pub fn count(output_column: impl Into<String>) -> Self {
        Self {
            kind: WindowAggKind::Count,
            input_column: String::new(),
            output_column: output_column.into(),
            filter: None,
        }
    }

    /// Attach a row predicate to this aggregate.
    #[must_use]
    pub fn with_filter(mut self, filter: WindowAggFilter) -> Self {
        self.filter = Some(filter);
        self
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
    /// ST11: events arriving within `[watermark, watermark + allowed_lateness_ms)`
    /// are kept for late-firing instead of being dropped. Defaults to
    /// `None` (no lateness — events past the watermark are dropped).
    /// When `Some(0)` the behaviour is identical to `None` (drop on
    /// arrival past the watermark).
    #[serde(default)]
    pub allowed_lateness_ms: Option<u64>,
    /// Per-source fixed-lag watermark (ms). When non-empty, effective watermark is the
    /// minimum across all configured sources (R5.2 multi-source watermark reconciliation).
    #[serde(default)]
    pub source_watermark_lags: HashMap<String, u64>,
    /// Column identifying the input source for multi-source watermark propagation.
    #[serde(default)]
    pub source_id_column: Option<String>,
    /// Optional timezone for SQL civil-time window bucketing (e.g. "America/New_York").
    ///
    /// This is only used for SQL window TVFs (`TUMBLE`, `HOP`, `SESSION`) when the
    /// user specifies `WITH TIMEZONE`. It affects how event timestamps are bucketed
    /// into civil-time windows (e.g., daily windows in a specific timezone).
    /// Watermark comparison and checkpoint ordering are always UTC and are NOT
    /// affected by this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_timezone: Option<String>,
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
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
            window_timezone: None,
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
        WindowKind::Count { .. } => (None, None),
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
        allowed_lateness_ms: None,
        source_watermark_lags: parsed.source_watermark_lags,
        source_id_column: parsed.source_id_column,
        window_timezone: None,
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
    if spec.window_kind == WindowKind::Sliding {
        match spec.slide_ms {
            None => {
                return Err(PlanError::Validation(String::from(
                    "sliding window requires explicit slide_ms",
                )));
            }
            Some(0) => {
                return Err(PlanError::Validation(String::from(
                    "sliding window slide_ms must be greater than zero",
                )));
            }
            Some(_) => {}
        }
    }
    if spec.window_kind == WindowKind::Session {
        match spec.session_gap_ms {
            None => {
                return Err(PlanError::Validation(String::from(
                    "session window requires explicit session_gap_ms",
                )));
            }
            Some(0) => {
                return Err(PlanError::Validation(String::from(
                    "session window session_gap_ms must be greater than zero",
                )));
            }
            Some(_) => {}
        }
    }
    if let WindowKind::Count { size, slide } = spec.window_kind {
        if size == 0 {
            return Err(PlanError::Validation(String::from(
                "count window size must be greater than zero",
            )));
        }
        if slide == 0 {
            return Err(PlanError::Validation(String::from(
                "count window slide must be greater than zero",
            )));
        }
        if slide > size {
            return Err(PlanError::Validation(String::from(
                "count window slide must be ≤ size",
            )));
        }
    }
    if spec.state_ttl_ms == Some(0) {
        return Err(PlanError::Validation(String::from(
            "window state_ttl_ms must be greater than zero",
        )));
    }
    // ST11: `allowed_lateness_ms = Some(0)` is equivalent to `None` and
    // is rejected here so the spec round-trips cleanly. `Some(n)` for
    // `n > 0` is accepted; values larger than `u64::MAX / 2` are
    // rejected to keep `watermark + allowed_lateness` additions safe.
    if let Some(0) = spec.allowed_lateness_ms {
        return Err(PlanError::Validation(String::from(
            "window allowed_lateness_ms must be greater than zero (use None to disable)",
        )));
    }
    if let Some(lat) = spec.allowed_lateness_ms
        && lat > u64::MAX / 2
    {
        return Err(PlanError::Validation(String::from(
            "window allowed_lateness_ms is implausibly large",
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
        if let Some(filter) = &aggregate.filter
            && filter.columns().iter().any(|c| c.trim().is_empty())
        {
            return Err(PlanError::Validation(format!(
                "window aggregate '{}' filter references an empty column name",
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
        if ch == '\\'
            && let Some(&next) = chars.peek()
            && (next == ':' || next == '\\')
        {
            chars.next();
            result.push(next);
            continue;
        }
        result.push(ch);
    }
    result
}

/// Encode a window spec as an executor plan fragment description.
///
/// Single-aggregate specs use the compact text format for backward
/// compatibility.  Multi-aggregate specs delegate to the lossless JSON
/// format because the compact format's `:` field delimiter conflicts with
/// agg parameter syntax (`agg=sum:col=amount`).
pub fn encode_stream_fragment(spec: &WindowExecutionSpec) -> Result<String, PlanError> {
    // Filtered aggregates also need the lossless JSON format: the compact
    // text format has no filter syntax.
    if spec.agg_exprs.len() > 1 || spec.agg_exprs.iter().any(|a| a.filter.is_some()) {
        return encode_window_execution_spec(spec);
    }
    let agg = if spec.agg_exprs.is_empty() {
        "agg=count".to_string()
    } else {
        spec.agg_exprs.first().map(encode_agg).unwrap_or_default()
    };

    let prefix = match spec.window_kind {
        WindowKind::Tumbling => "stream:tw",
        WindowKind::Sliding => "stream:sw",
        WindowKind::Session => "stream:ses",
        WindowKind::Count { .. } => "stream:cw",
    };

    let extra = match spec.window_kind {
        WindowKind::Tumbling => String::new(),
        WindowKind::Sliding => format!(":slide={}", spec.slide_ms.unwrap_or(spec.window_size_ms)),
        WindowKind::Session => format!(
            ":gap={}",
            spec.session_gap_ms.unwrap_or(spec.window_size_ms)
        ),
        WindowKind::Count { size, slide } => format!(":csize={size}:cslide={slide}"),
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
            .map(|(id, lag)| format!("{}:{lag}", escape_compact_value(id)))
            .collect();
        format!(":srcs={}", encoded.join(","))
    };

    Ok(format!(
        "{prefix}:key={}:time={}:win={}:lag={}:{agg}{extra}{ttl}{srcid}{srcs}",
        escape_compact_value(&spec.key_column),
        escape_compact_value(&spec.event_time_column),
        spec.window_size_ms,
        spec.watermark_lag_ms,
    ))
}

fn encode_agg(agg: &WindowAgg) -> String {
    match agg.kind {
        WindowAggKind::Count => "agg=count".to_string(),
        WindowAggKind::Sum => format!("agg=sum:col={}", agg.input_column),
        WindowAggKind::Min => format!("agg=min:col={}", agg.input_column),
        WindowAggKind::Max => format!("agg=max:col={}", agg.input_column),
        WindowAggKind::Avg => format!("agg=avg:col={}", agg.input_column),
        WindowAggKind::Stddev => format!("agg=stddev:col={}", agg.input_column),
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
    let (window_kind_tag, payload) = if let Some(p) = fragment.strip_prefix("stream:tw:") {
        ("tw", p)
    } else if let Some(p) = fragment.strip_prefix("stream:sw:") {
        ("sw", p)
    } else if let Some(p) = fragment.strip_prefix("stream:ses:") {
        ("ses", p)
    } else if let Some(p) = fragment.strip_prefix("stream:cw:") {
        ("cw", p)
    } else {
        return Err(PlanError::Parse(format!(
            "streaming fragment must start with stream:tw:, stream:sw:, stream:ses:, or stream:cw:; got: {fragment}"
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
    let mut count_size: Option<u64> = None;
    let mut count_slide: Option<u64> = None;

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
            "csize" => {
                count_size =
                    Some(v.trim().parse::<u64>().map_err(|e| {
                        PlanError::Parse(format!("invalid csize value '{v}': {e}"))
                    })?);
            }
            "cslide" => {
                count_slide =
                    Some(v.trim().parse::<u64>().map_err(|e| {
                        PlanError::Parse(format!("invalid cslide value '{v}': {e}"))
                    })?);
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
                        .find(|(idx, ch)| *ch == ':' && !is_escaped_colon(pair, *idx))
                        .map(|(idx, _)| idx);
                    let split_idx = split_idx.ok_or_else(|| {
                        PlanError::Parse(format!("srcs entry must be id:lag_ms; got '{pair}'"))
                    })?;
                    let id = unescape_compact_value(&pair[..split_idx]);
                    let lag_str = &pair[split_idx + ':'.len_utf8()..];
                    let lag: u64 = lag_str.trim().parse().map_err(|e| {
                        PlanError::Parse(format!("invalid lag in srcs entry '{pair}': {e}"))
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
            filter: None,
            kind: WindowAggKind::Sum,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=sum requires col=<column>",
                ))
            })?,
            output_column: format!("sum_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("min") => WindowAgg {
            filter: None,
            kind: WindowAggKind::Min,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=min requires col=<column>",
                ))
            })?,
            output_column: format!("min_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("max") => WindowAgg {
            filter: None,
            kind: WindowAggKind::Max,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=max requires col=<column>",
                ))
            })?,
            output_column: format!("max_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("avg") => WindowAgg {
            filter: None,
            kind: WindowAggKind::Avg,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=avg requires col=<column>",
                ))
            })?,
            output_column: format!("avg_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("stddev") => WindowAgg {
            filter: None,
            kind: WindowAggKind::Stddev,
            input_column: agg_col.clone().ok_or_else(|| {
                PlanError::Parse(String::from(
                    "stream fragment with agg=stddev requires col=<column>",
                ))
            })?,
            output_column: format!("stddev_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some(other) => {
            return Err(PlanError::Parse(format!(
                "unknown streaming aggregate '{other}', expected count|sum|min|max|avg|stddev"
            )));
        }
    };

    let window_kind = match window_kind_tag {
        "tw" => WindowKind::Tumbling,
        "sw" => WindowKind::Sliding,
        "ses" => WindowKind::Session,
        "cw" => WindowKind::Count {
            size: count_size.ok_or_else(|| {
                PlanError::Parse(String::from("count-window fragment missing csize=<n>"))
            })?,
            slide: count_slide.ok_or_else(|| {
                PlanError::Parse(String::from("count-window fragment missing cslide=<n>"))
            })?,
        },
        _ => {
            return Err(PlanError::Parse(format!(
                "unknown window kind tag '{window_kind_tag}'"
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
    "csize=", "cslide=",
];

/// Return `true` if the colon at byte position `idx` is escaped by an odd
/// number of consecutive backslashes immediately preceding it.
fn is_escaped_colon(payload: &str, idx: usize) -> bool {
    let mut backslash_count = 0usize;
    let mut i = idx;
    while i > 0 {
        i -= 1;
        if payload.as_bytes().get(i).is_some_and(|&b| b == b'\\') {
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

    #[test]
    fn filtered_agg_fragment_round_trips_via_json() {
        let mut spec = WindowExecutionSpec::tumbling("k", "ts", 60_000);
        spec.agg_exprs = vec![WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: "size".into(),
            output_column: "edit_bytes".into(),
            filter: Some(WindowAggFilter::And(
                Box::new(WindowAggFilter::Compare {
                    column: "kind".into(),
                    op: AggFilterCompareOp::Eq,
                    value: AggFilterValue::Utf8("edit".into()),
                }),
                Box::new(WindowAggFilter::IsNotNull {
                    column: "size".into(),
                }),
            )),
        }];
        let encoded = encode_stream_fragment(&spec).unwrap();
        assert!(
            encoded.starts_with(WINDOW_EXECUTION_SPEC_PREFIX),
            "filtered aggregates must take the lossless JSON fragment format, got: {encoded}"
        );
        let decoded = decode_window_execution_spec(&encoded).unwrap();
        assert_eq!(decoded, spec, "filter survives the fragment round trip");
    }

    #[test]
    fn unfiltered_agg_json_omits_filter_field_for_wire_compat() {
        let spec = WindowExecutionSpec::tumbling("k", "ts", 60_000);
        let json = serde_json::to_string(&spec).unwrap();
        assert!(
            !json.contains("\"filter\""),
            "unfiltered aggregates must serialize byte-identically to the pre-filter format"
        );
    }
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
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let frag = encode_stream_fragment(&spec).unwrap();
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
                    filter: None,
                    kind: WindowAggKind::Sum,
                    input_column: String::from("amount"),
                    output_column: String::from("gross_amount"),
                },
            ],
            state_ttl_ms: Some(600_000),
            allowed_lateness_ms: None,
            source_watermark_lags,
            source_id_column: Some(String::from("source")),
            window_timezone: None,
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
            allowed_lateness_ms: None,
            source_watermark_lags,
            source_id_column: Some("source_id".into()),
            window_timezone: None,
        };

        let fragment = encode_stream_fragment(&spec).unwrap();
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
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let frag = encode_stream_fragment(&spec).unwrap();
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
            allowed_lateness_ms: None,
            source_watermark_lags: HashMap::new(),
            source_id_column: None,
            window_timezone: None,
        };
        let frag = encode_stream_fragment(&spec).unwrap();
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
            allowed_lateness_ms: None,
            source_watermark_lags,
            source_id_column: Some("src:col".into()),
            window_timezone: None,
        };
        let frag = encode_stream_fragment(&spec).unwrap();
        let parsed = parse_stream_fragment(&frag).expect("parse escaped multi-source");
        assert_eq!(parsed.source_id_column.as_deref(), Some("src:col"));
        assert_eq!(parsed.source_watermark_lags.get("ns:orders"), Some(&1_000));
    }

    // ── Fuzz-style adversarial validation (Phase 5: no fuzz coverage for
    // validate_window_execution_spec) ──────────────────────────────────────
    //
    // `cargo-fuzz` requires a nightly toolchain and sanitizer support that
    // this workspace does not provision; `proptest` gives equivalent
    // adversarial-input coverage (arbitrary/edge-case generation, shrinking
    // on failure) entirely on stable, so it is the practical choice here.
    mod adversarial_validation {
        use super::*;
        use proptest::prelude::*;

        fn arb_window_kind() -> impl Strategy<Value = WindowKind> {
            prop_oneof![
                Just(WindowKind::Tumbling),
                Just(WindowKind::Sliding),
                Just(WindowKind::Session),
            ]
        }

        fn arb_agg_kind() -> impl Strategy<Value = WindowAggKind> {
            prop_oneof![
                Just(WindowAggKind::Count),
                Just(WindowAggKind::Sum),
                Just(WindowAggKind::Min),
                Just(WindowAggKind::Max),
                Just(WindowAggKind::Avg),
                Just(WindowAggKind::Stddev),
            ]
        }

        fn arb_agg() -> impl Strategy<Value = WindowAgg> {
            (arb_agg_kind(), "[a-zA-Z0-9_ ]{0,8}", "[a-zA-Z0-9_ ]{0,8}").prop_map(
                |(kind, input_column, output_column)| WindowAgg {
                    filter: None,
                    kind,
                    input_column,
                    output_column,
                },
            )
        }

        fn arb_spec() -> impl Strategy<Value = WindowExecutionSpec> {
            (
                "[a-zA-Z0-9_ ]{0,8}",
                "[a-zA-Z0-9_ ]{0,8}",
                any::<u64>(),
                arb_window_kind(),
                any::<u64>(),
                proptest::option::of(any::<u64>()),
                proptest::option::of(any::<u64>()),
                proptest::collection::vec(arb_agg(), 0..4),
                proptest::option::of(any::<u64>()),
            )
                .prop_map(
                    |(
                        key_column,
                        event_time_column,
                        watermark_lag_ms,
                        window_kind,
                        window_size_ms,
                        slide_ms,
                        session_gap_ms,
                        agg_exprs,
                        state_ttl_ms,
                    )| WindowExecutionSpec {
                        key_column,
                        key_column_type: default_key_type(),
                        event_time_column,
                        watermark_lag_ms,
                        window_kind,
                        window_size_ms,
                        slide_ms,
                        session_gap_ms,
                        agg_exprs,
                        state_ttl_ms,
                        allowed_lateness_ms: None,
                        source_watermark_lags: HashMap::new(),
                        source_id_column: None,
                        window_timezone: None,
                    },
                )
        }

        proptest! {
            /// Adversarial inputs (empty/whitespace names, zero/huge durations,
            /// missing slide/gap, duplicate output columns, …) must always be
            /// rejected or accepted cleanly — never panic.
            #[test]
            fn validate_window_execution_spec_never_panics(spec in arb_spec()) {
                let _ = validate_window_execution_spec(&spec);
            }

            /// A spec that validates Ok must satisfy the invariants the
            /// validator is supposed to enforce, regardless of how the
            /// arbitrary input was shaped.
            #[test]
            fn validated_spec_satisfies_invariants(spec in arb_spec()) {
                if validate_window_execution_spec(&spec).is_ok() {
                    prop_assert!(!spec.key_column.trim().is_empty());
                    prop_assert!(!spec.event_time_column.trim().is_empty());
                    prop_assert!(spec.window_size_ms > 0);
                    prop_assert!(!spec.agg_exprs.is_empty());
                    if spec.window_kind == WindowKind::Sliding {
                        prop_assert!(matches!(spec.slide_ms, Some(s) if s > 0));
                    }
                    if spec.window_kind == WindowKind::Session {
                        prop_assert!(matches!(spec.session_gap_ms, Some(g) if g > 0));
                    }
                    prop_assert_ne!(spec.state_ttl_ms, Some(0));
                    let mut seen = HashSet::with_capacity(spec.agg_exprs.len());
                    for agg in &spec.agg_exprs {
                        prop_assert!(!agg.output_column.trim().is_empty());
                        prop_assert!(seen.insert(agg.output_column.clone()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod allowed_lateness_tests {
    use super::*;

    /// ST11: `allowed_lateness_ms` defaults to `None` (no lateness) on
    /// the `tumbling` constructor and is accepted as a positive value
    /// by the validator.
    #[test]
    fn allowed_lateness_defaults_to_none_and_validates_positive_value() {
        let spec = WindowExecutionSpec::tumbling("k", "ts", 1_000);
        assert_eq!(spec.allowed_lateness_ms, None);
        validate_window_execution_spec(&spec).expect("default spec is valid");

        let mut with_lat = spec.clone();
        with_lat.allowed_lateness_ms = Some(2_000);
        validate_window_execution_spec(&with_lat).expect("positive allowed_lateness_ms is valid");
    }

    /// ST11: `Some(0)` is rejected by the validator so a round-trip
    /// through `encode_window_execution_spec` cannot store a zero
    /// lateness that would be indistinguishable from `None` on read.
    #[test]
    fn allowed_lateness_zero_is_rejected() {
        let mut spec = WindowExecutionSpec::tumbling("k", "ts", 1_000);
        spec.allowed_lateness_ms = Some(0);
        let err = validate_window_execution_spec(&spec).unwrap_err();
        assert!(format!("{err}").contains("allowed_lateness_ms"));
    }
}
