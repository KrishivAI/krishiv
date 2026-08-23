//! In-process ring buffer of recent tracing events, servable over HTTP.
//!
//! Every real debugging session ends in "tail the daemon log"; the console
//! shows state and the event log, but not the WARN/ERROR narrative that
//! explains transitions. This layer captures the last [`MAX_ENTRIES`]
//! INFO-and-above events (with their real wall-clock timestamps — tracing
//! has them, unlike the metadata store's event log) into a global bounded
//! ring that `/logs`-style endpoints read.
//!
//! Deliberately NOT log aggregation: recent history since process start,
//! in memory, capped — the archive belongs to stdout shipping (kubectl
//! logs / Loki). DEBUG/TRACE are excluded so a debug-level filter cannot
//! flood the ring into uselessness.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use tracing::Level;
use tracing::field::{Field, Visit};

/// Ring capacity in entries. At a typical <200 bytes per formatted entry
/// this bounds the ring under ~1 MiB.
pub const MAX_ENTRIES: usize = 2000;
/// Per-entry message cap: one pathological error string must not consume
/// the whole budget.
const MAX_MESSAGE_BYTES: usize = 2048;

/// One captured log event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Wall-clock UNIX ms when the event fired (real, from capture time).
    pub at_ms: u64,
    /// `ERROR` | `WARN` | `INFO`.
    pub level: String,
    /// The tracing target (module path).
    pub target: String,
    /// The event message plus formatted fields.
    pub message: String,
}

fn ring() -> &'static Mutex<VecDeque<LogEntry>> {
    static RING: OnceLock<Mutex<VecDeque<LogEntry>>> = OnceLock::new();
    RING.get_or_init(|| Mutex::new(VecDeque::with_capacity(256)))
}

/// Push an entry (also the test seam for endpoint tests).
pub fn push(entry: LogEntry) {
    let mut guard = ring().lock().unwrap_or_else(|p| p.into_inner());
    if guard.len() >= MAX_ENTRIES {
        guard.pop_front();
    }
    guard.push_back(entry);
}

/// The most recent `limit` entries at or above `min_level`
/// (`"error"`/`"warn"`/`"info"`, default info), newest last.
pub fn recent(limit: usize, min_level: Option<&str>) -> Vec<LogEntry> {
    let keep: &[&str] = match min_level.map(str::to_ascii_lowercase).as_deref() {
        Some("error") => &["ERROR"],
        Some("warn") => &["ERROR", "WARN"],
        _ => &["ERROR", "WARN", "INFO"],
    };
    let guard = ring().lock().unwrap_or_else(|p| p.into_inner());
    guard
        .iter()
        .filter(|e| keep.contains(&e.level.as_str()))
        .rev()
        .take(limit)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            if !self.fields.is_empty() {
                self.fields.push(' ');
            }
            let _ = write!(self.fields, "{}={value:?}", field.name());
        }
    }
}

/// Tracing layer feeding the global ring. INFO and above only.
pub struct LogRingLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogRingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let level = *event.metadata().level();
        if level > Level::INFO {
            return;
        }
        let mut visitor = MessageVisitor {
            message: String::new(),
            fields: String::new(),
        };
        event.record(&mut visitor);
        let mut message = visitor.message;
        if !visitor.fields.is_empty() {
            if !message.is_empty() {
                message.push(' ');
            }
            message.push_str(&visitor.fields);
        }
        message.truncate(MAX_MESSAGE_BYTES);
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        push(LogEntry {
            at_ms,
            level: level.as_str().to_owned(),
            target: event.metadata().target().to_owned(),
            message,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt as _;

    /// Revert-proof: remove the `.with(LogRingLayer)` wiring (or the layer's
    /// push) and capture stops — WARN present, DEBUG excluded.
    #[test]
    fn captures_warn_and_excludes_debug() {
        let subscriber = tracing_subscriber::registry().with(LogRingLayer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(job_id = "job-ring", "ring capture check");
            tracing::debug!("must not be captured");
        });
        let entries = recent(50, Some("warn"));
        let hit = entries
            .iter()
            .find(|e| e.message.contains("ring capture check"))
            .expect("warn event must be captured");
        assert_eq!(hit.level, "WARN");
        assert!(hit.message.contains("job_id=\"job-ring\""));
        assert!(hit.at_ms > 0);
        assert!(
            !entries
                .iter()
                .any(|e| e.message.contains("must not be captured"))
        );
    }

    #[test]
    fn ring_is_bounded() {
        for i in 0..(MAX_ENTRIES + 100) {
            push(LogEntry {
                at_ms: 1,
                level: "INFO".into(),
                target: "t".into(),
                message: format!("bound-{i}"),
            });
        }
        let guard = super::ring().lock().unwrap();
        assert!(guard.len() <= MAX_ENTRIES);
    }
}
