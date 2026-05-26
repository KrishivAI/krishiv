//! Streaming window plan configuration and fragment encoding (unified execution).

use std::collections::HashMap;

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

/// Encode a window spec as an executor plan fragment description.
pub fn encode_stream_fragment(spec: &WindowExecutionSpec) -> String {
    let agg = spec
        .agg_exprs
        .first()
        .map(encode_agg)
        .unwrap_or_else(|| "agg=count".to_string());

    let prefix = match spec.window_kind {
        WindowKind::Tumbling => "stream:tw",
        WindowKind::Sliding => "stream:sw",
        WindowKind::Session => "stream:ses",
    };

    let extra = match spec.window_kind {
        WindowKind::Tumbling => String::new(),
        WindowKind::Sliding => format!(":slide={}", spec.slide_ms.unwrap_or(spec.window_size_ms)),
        WindowKind::Session => format!(
            ":gap={}",
            spec.session_gap_ms.unwrap_or(spec.window_size_ms)
        ),
    };

    let ttl = spec
        .state_ttl_ms
        .map(|ms| format!(":ttl={ms}"))
        .unwrap_or_default();

    format!(
        "{prefix}:key=key:time=ts:win={}:lag={}:{agg}{extra}{ttl}",
        spec.window_size_ms, spec.watermark_lag_ms,
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
}

/// Parse `stream:tw|sw|ses:...` fragment strings.
pub fn parse_stream_fragment(fragment: &str) -> Result<ParsedStreamFragment, String> {
    let (window_kind, payload) = if let Some(p) = fragment.strip_prefix("stream:tw:") {
        (WindowKind::Tumbling, p)
    } else if let Some(p) = fragment.strip_prefix("stream:sw:") {
        (WindowKind::Sliding, p)
    } else if let Some(p) = fragment.strip_prefix("stream:ses:") {
        (WindowKind::Session, p)
    } else {
        return Err(format!(
            "streaming fragment must start with stream:tw:, stream:sw:, or stream:ses:; got: {fragment}"
        ));
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

    for part in payload.split(':') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| format!("streaming fragment field must be k=v; got '{part}'"))?;
        match k.trim() {
            "key" => key_col = Some(v.trim().to_owned()),
            "time" => time_col = Some(v.trim().to_owned()),
            "win" => {
                window_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| format!("invalid win value '{v}': {e}"))?,
                );
            }
            "lag" => {
                lag_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| format!("invalid lag value '{v}': {e}"))?,
                );
            }
            "slide" => {
                slide_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| format!("invalid slide value '{v}': {e}"))?,
                );
            }
            "gap" => {
                session_gap_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| format!("invalid gap value '{v}': {e}"))?,
                );
            }
            "ttl" => {
                ttl_ms = Some(
                    v.trim()
                        .parse::<u64>()
                        .map_err(|e| format!("invalid ttl value '{v}': {e}"))?,
                );
            }
            "agg" => agg_kind = Some(v.trim().to_owned()),
            "col" => agg_col = Some(v.trim().to_owned()),
            _ => {}
        }
    }

    let agg = match agg_kind.as_deref() {
        None | Some("count") => WindowAgg::count("count"),
        Some("sum") => WindowAgg {
            kind: WindowAggKind::Sum,
            input_column: agg_col.clone().ok_or_else(|| {
                String::from("stream fragment with agg=sum requires col=<column>")
            })?,
            output_column: format!("sum_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("min") => WindowAgg {
            kind: WindowAggKind::Min,
            input_column: agg_col.clone().ok_or_else(|| {
                String::from("stream fragment with agg=min requires col=<column>")
            })?,
            output_column: format!("min_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("max") => WindowAgg {
            kind: WindowAggKind::Max,
            input_column: agg_col.clone().ok_or_else(|| {
                String::from("stream fragment with agg=max requires col=<column>")
            })?,
            output_column: format!("max_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some("avg") => WindowAgg {
            kind: WindowAggKind::Avg,
            input_column: agg_col.clone().ok_or_else(|| {
                String::from("stream fragment with agg=avg requires col=<column>")
            })?,
            output_column: format!("avg_{}", agg_col.as_deref().unwrap_or("val")),
        },
        Some(other) => {
            return Err(format!(
                "unknown streaming aggregate '{other}', expected count|sum|min|max|avg"
            ));
        }
    };

    Ok(ParsedStreamFragment {
        window_kind,
        key_col: key_col.ok_or_else(|| String::from("stream fragment missing key=<col>"))?,
        time_col: time_col.ok_or_else(|| String::from("stream fragment missing time=<col>"))?,
        window_ms: window_ms.ok_or_else(|| String::from("stream fragment missing win=<ms>"))?,
        lag_ms: lag_ms.unwrap_or(0),
        slide_ms,
        session_gap_ms,
        ttl_ms,
        agg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_tumbling_fragment() {
        let spec = WindowExecutionSpec {
            key_column: "user_id".into(),
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
    fn parse_sliding_fragment() {
        let frag = "stream:sw:key=key:time=ts:win=10000:lag=0:slide=5000:agg=count";
        let p = parse_stream_fragment(frag).expect("parse");
        assert_eq!(p.window_kind, WindowKind::Sliding);
        assert_eq!(p.slide_ms, Some(5000));
    }
}
