#![forbid(unsafe_code)]

//! LATENESS annotation and watermark-based state GC.
//!
//! Each incremental source can annotate one timestamp column with a LATENESS
//! bound. Records arriving with `ts < watermark` (where
//! `watermark = max_observed_ts - lateness_ms`) are dropped at ingestion.
//! Stateful operators (join Traces, aggregate state) can call
//! `gc_below_watermark` to free entries older than the watermark.

/// LATENESS annotation on one source column.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LatenessSpec {
    /// Name of the timestamp column (must be Int64 milliseconds or Timestamp).
    pub column: String,
    /// Maximum tolerated out-of-orderness in milliseconds.
    pub lateness_ms: i64,
}

impl LatenessSpec {
    pub fn new(column: impl Into<String>, lateness_ms: i64) -> Self {
        Self { column: column.into(), lateness_ms }
    }

    /// Parse a human-readable duration string like "5 minutes", "1 hour", "30 seconds".
    pub fn parse_duration(s: &str) -> Option<i64> {
        let s = s.trim();
        if let Some(n) = s.strip_suffix(" minutes").or_else(|| s.strip_suffix(" minute")) {
            return n.trim().parse::<i64>().ok().map(|v| v * 60_000);
        }
        if let Some(n) = s.strip_suffix(" hours").or_else(|| s.strip_suffix(" hour")) {
            return n.trim().parse::<i64>().ok().map(|v| v * 3_600_000);
        }
        if let Some(n) = s.strip_suffix(" seconds").or_else(|| s.strip_suffix(" second")) {
            return n.trim().parse::<i64>().ok().map(|v| v * 1_000);
        }
        if let Some(n) = s.strip_suffix("ms") {
            return n.trim().parse::<i64>().ok();
        }
        // bare number → milliseconds
        s.parse::<i64>().ok()
    }
}

/// Tracks the high-water mark for one source's timestamp column.
#[derive(Debug, Clone)]
pub struct WatermarkTracker {
    spec: LatenessSpec,
    /// Maximum timestamp (ms) observed across all ingested records.
    max_observed_ts: i64,
}

impl WatermarkTracker {
    pub fn new(spec: LatenessSpec) -> Self {
        Self { max_observed_ts: i64::MIN, spec }
    }

    /// Update the high-water mark given a new observed timestamp.
    pub fn observe(&mut self, ts_ms: i64) {
        if ts_ms > self.max_observed_ts {
            self.max_observed_ts = ts_ms;
        }
    }

    /// The current watermark: records below this are late and should be dropped.
    pub fn watermark(&self) -> i64 {
        if self.max_observed_ts == i64::MIN {
            i64::MIN
        } else {
            self.max_observed_ts - self.spec.lateness_ms
        }
    }

    /// Return true if `ts_ms` is too late to process (below watermark).
    pub fn is_late(&self, ts_ms: i64) -> bool {
        ts_ms < self.watermark()
    }

    pub fn lateness_column(&self) -> &str {
        &self.spec.column
    }

    pub fn max_observed_ts(&self) -> i64 {
        self.max_observed_ts
    }
}

/// Per-source ordinal watermark for skip-if-unchanged optimization.
///
/// When the source offset has not advanced since the last tick, the scheduler
/// can skip assigning tasks for that source's downstream operators.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceOrdinal {
    pub source_name: String,
    /// Opaque offset bytes (e.g., Kafka offset, Iceberg snapshot ID, mtime).
    pub last_processed: Vec<u8>,
}

impl SourceOrdinal {
    pub fn new(source_name: impl Into<String>, offset: Vec<u8>) -> Self {
        Self { source_name: source_name.into(), last_processed: offset }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_advances_with_max_ts() {
        let spec = LatenessSpec::new("ts", 5_000);
        let mut tracker = WatermarkTracker::new(spec);
        tracker.observe(100_000);
        assert_eq!(tracker.watermark(), 95_000);
        tracker.observe(200_000);
        assert_eq!(tracker.watermark(), 195_000);
    }

    #[test]
    fn records_below_watermark_are_late() {
        let spec = LatenessSpec::new("ts", 10_000);
        let mut tracker = WatermarkTracker::new(spec);
        tracker.observe(100_000);
        assert!(tracker.is_late(89_999));
        assert!(!tracker.is_late(90_000));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(LatenessSpec::parse_duration("5 minutes"), Some(300_000));
        assert_eq!(LatenessSpec::parse_duration("1 minute"), Some(60_000));
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(LatenessSpec::parse_duration("2 hours"), Some(7_200_000));
    }

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(LatenessSpec::parse_duration("30 seconds"), Some(30_000));
    }

    #[test]
    fn watermark_min_when_no_observations() {
        let spec = LatenessSpec::new("ts", 1_000);
        let tracker = WatermarkTracker::new(spec);
        assert_eq!(tracker.watermark(), i64::MIN);
        assert!(!tracker.is_late(0));
    }
}
