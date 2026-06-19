//! Shared streaming pipeline descriptor carried by Stream / KeyedStream / WindowedStream.

use std::collections::HashMap;
use std::sync::Arc;

use krishiv_api::Session;

use crate::agg::AggDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowKind {
    Tumbling,
    Sliding,
    Session,
}

#[derive(Debug, Clone)]
pub struct WindowDescriptor {
    pub kind: WindowKind,
    pub size_ms: u64,
    pub slide_ms: Option<u64>,
    pub gap_ms: Option<u64>,
}

/// Mutable streaming plan metadata assembled by the Python transformation chain.
#[derive(Debug, Clone)]
pub struct StreamPipeline {
    pub session: Arc<Session>,
    pub source_id: String,
    /// True for bounded (in-memory / SQL query) streams; false for unbounded sources.
    pub bounded: bool,
    pub watermark_column: String,
    pub max_lateness_ms: u64,
    pub key_columns: Vec<String>,
    pub event_time_column: Option<String>,
    pub window: Option<WindowDescriptor>,
    pub aggregations: Vec<AggDescriptor>,
    /// Per-source watermark lags for multi-source joins (source_id → lag_ms).
    pub source_watermarks: HashMap<String, u64>,
    /// Column name that identifies the source for multi-source watermark reconciliation.
    pub source_id_column: Option<String>,
    /// Optional state TTL override (ms). Overrides the session-level TTL when set.
    pub state_ttl_ms: Option<u64>,
}

impl StreamPipeline {
    pub fn new(
        session: Arc<Session>,
        source_id: String,
        watermark_column: String,
        max_lateness_ms: u64,
    ) -> Self {
        Self {
            session,
            source_id,
            bounded: false,
            watermark_column,
            max_lateness_ms,
            key_columns: Vec::new(),
            event_time_column: None,
            window: None,
            aggregations: Vec::new(),
            source_watermarks: HashMap::new(),
            source_id_column: None,
            state_ttl_ms: None,
        }
    }

    pub fn with_watermark(&self, column: String, max_lateness_ms: u64) -> Self {
        let mut next = self.clone();
        next.watermark_column = column;
        next.max_lateness_ms = max_lateness_ms;
        next
    }

    pub fn with_keys(&self, columns: Vec<String>) -> Self {
        let mut next = self.clone();
        next.key_columns = columns;
        next
    }

    pub fn with_window(&self, window: WindowDescriptor) -> Self {
        let mut next = self.clone();
        if next.event_time_column.is_none() && !next.watermark_column.is_empty() {
            next.event_time_column = Some(next.watermark_column.clone());
        }
        next.window = Some(window);
        next
    }

    pub fn with_aggregations(&self, aggs: Vec<AggDescriptor>) -> Self {
        let mut next = self.clone();
        next.aggregations = aggs;
        next
    }

    /// Override state TTL for this stream (milliseconds).
    pub fn with_state_ttl(&self, ttl_ms: u64) -> Self {
        let mut next = self.clone();
        next.state_ttl_ms = Some(ttl_ms);
        next
    }

    /// Add a per-source watermark lag for multi-source join pipelines.
    pub fn with_source_watermark(&self, source_id: String, lag_ms: u64) -> Self {
        let mut next = self.clone();
        next.source_watermarks.insert(source_id, lag_ms);
        next
    }

    /// Set the source-id column used for multi-source watermark reconciliation.
    pub fn with_source_id_column(&self, column: String) -> Self {
        let mut next = self.clone();
        next.source_id_column = Some(column);
        next
    }

    pub fn repr_html(&self) -> String {
        let keys = if self.key_columns.is_empty() {
            "—".to_string()
        } else {
            self.key_columns.join(", ")
        };
        let window = self
            .window
            .as_ref()
            .map(|w| format!("{:?} {}ms", w.kind, w.size_ms))
            .unwrap_or_else(|| "—".to_string());
        let aggs = if self.aggregations.is_empty() {
            "—".to_string()
        } else {
            self.aggregations
                .iter()
                .map(|a| a.output_name.clone())
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "<table><tbody>\
             <tr><th>Source</th><td><code>{}</code></td></tr>\
             <tr><th>Watermark</th><td>{} ({} ms)</td></tr>\
             <tr><th>Keys</th><td>{keys}</td></tr>\
             <tr><th>Window</th><td>{window}</td></tr>\
             <tr><th>Aggregations</th><td>{aggs}</td></tr>\
             </tbody></table>",
            html_escape(&self.source_id),
            html_escape(&self.watermark_column),
            self.max_lateness_ms,
        )
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
