//! P11: Materialized Table API — Spark 4.0-style materialized tables with
//! managed refresh lifecycle.
//!
//! A materialized table is a named dataset whose contents are defined by a
//! query and kept up-to-date via scheduled refreshes. It supports:
//!
//! - **Full refresh**: re-executes the defining query and replaces the data.
//! - **Incremental refresh**: appends only new/changed rows (requires a
//!   monotonically-increasing column or a change-tracking predicate).
//! - **Manual refresh**: triggered by the user via `refresh()`.
//! - **Periodic refresh**: driven by a `RefreshSchedule` (interval-based).
//! - **Lineage tracking**: records each refresh's timestamp, row count delta,
//!   and optional label.
//!
//! # Example
//!
//! ```ignore
//! use krishiv_api::materialized_table::*;
//!
//! let table = MaterializedTable::new("daily_revenue", "SELECT date, SUM(amount) FROM orders GROUP BY date")
//!     .with_refresh_mode(RefreshMode::Full)
//!     .with_schedule(RefreshSchedule::interval(std::time::Duration::from_secs(3600)))
//!     .build();
//!
//! table.refresh(&session).await?;
//! let batch = table.read(&session).await?;
//! ```

use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── Refresh mode ─────────────────────────────────────────────────────────────

/// How a materialized table's data is refreshed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RefreshMode {
    /// Re-execute the defining query and replace all data.
    #[default]
    Full,
    /// Execute the query with an incremental predicate and append results.
    Incremental {
        /// Column name used to detect new rows (must be monotonically increasing,
        /// e.g. an event timestamp or auto-increment id).
        change_column: String,
    },
}

// ── Refresh schedule ─────────────────────────────────────────────────────────

/// When the next automatic refresh should occur.
#[derive(Debug, Clone, Default)]
pub enum RefreshSchedule {
    /// Refresh at a fixed interval from the last successful refresh.
    Interval(Duration),
    /// Refresh once at a specific absolute time (epoch millis).
    OnceAt(u64),
    /// No automatic schedule; manual refresh only.
    #[default]
    Manual,
}

impl RefreshSchedule {
    /// Convenience constructor for [`RefreshSchedule::Interval`].
    pub fn interval(d: Duration) -> Self {
        Self::Interval(d)
    }
}

// ── Refresh metadata ─────────────────────────────────────────────────────────

/// Metadata about a single refresh operation.
#[derive(Debug, Clone)]
pub struct RefreshRecord {
    /// Epoch-millis timestamp when the refresh completed.
    pub completed_at_ms: u64,
    /// Number of rows added (for incremental) or total rows (for full).
    pub rows_affected: u64,
    /// Duration of the refresh in milliseconds.
    pub duration_ms: u64,
    /// Optional user-provided label for this refresh.
    pub label: Option<String>,
    /// The number of rows in the materialized table after this refresh.
    pub total_rows: u64,
}

// ── Materialized table definition ────────────────────────────────────────────

/// Configuration for a materialized table.
#[derive(Debug, Clone)]
pub struct MaterializedTableConfig {
    /// The table name.
    pub name: String,
    /// The SQL query that defines the table's contents.
    pub query: String,
    /// Refresh strategy.
    pub refresh_mode: RefreshMode,
    /// Automatic refresh schedule.
    pub schedule: RefreshSchedule,
    /// Maximum number of `RefreshRecord`s to retain (for lineage).
    pub max_history: usize,
    /// Optional sink path for persistent storage (e.g. Parquet directory).
    pub sink_path: Option<String>,
}

impl MaterializedTableConfig {
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            refresh_mode: RefreshMode::default(),
            schedule: RefreshSchedule::default(),
            max_history: 64,
            sink_path: None,
        }
    }

    /// Return a builder for the config. Sugar so the test code
    /// (and user code) can chain `.with_schedule(...).with_mode(...)`
    /// without a separate `MaterializedTableConfigBuilder` type.
    pub fn builder(
        name: impl Into<String>,
        query: impl Into<String>,
    ) -> MaterializedTableConfigBuilder {
        MaterializedTableConfigBuilder::new(name, query)
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Builder for `MaterializedTableConfig`.
pub struct MaterializedTableConfigBuilder {
    config: MaterializedTableConfig,
}

impl MaterializedTableConfigBuilder {
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            config: MaterializedTableConfig::new(name, query),
        }
    }

    pub fn with_refresh_mode(mut self, mode: RefreshMode) -> Self {
        self.config.refresh_mode = mode;
        self
    }

    pub fn with_schedule(mut self, schedule: RefreshSchedule) -> Self {
        self.config.schedule = schedule;
        self
    }

    pub fn with_max_history(mut self, max: usize) -> Self {
        self.config.max_history = max;
        self
    }

    pub fn with_sink_path(mut self, path: impl Into<String>) -> Self {
        self.config.sink_path = Some(path.into());
        self
    }

    pub fn build(self) -> MaterializedTableConfig {
        self.config
    }
}

// ── Materialized table handle ────────────────────────────────────────────────

/// A materialized table with refresh lifecycle management.
///
/// Stores the table's configuration, refresh history, and current state.
/// The actual data is held externally (in a session or state backend); this
/// struct manages the metadata and scheduling logic.
pub struct MaterializedTable {
    config: MaterializedTableConfig,
    history: RwLock<Vec<RefreshRecord>>,
    last_refresh_epoch_ms: RwLock<Option<u64>>,
}

impl MaterializedTable {
    /// Create a new materialized table handle.
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            config: MaterializedTableConfig::new(name, query),
            history: RwLock::new(Vec::new()),
            last_refresh_epoch_ms: RwLock::new(None),
        }
    }

    /// Create from an existing config.
    pub fn from_config(config: MaterializedTableConfig) -> Self {
        Self {
            config,
            history: RwLock::new(Vec::new()),
            last_refresh_epoch_ms: RwLock::new(None),
        }
    }

    /// Access the table configuration.
    pub fn config(&self) -> &MaterializedTableConfig {
        &self.config
    }

    /// Return the table name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Return the defining query.
    pub fn query(&self) -> &str {
        &self.config.query
    }

    /// Record a completed refresh. This updates the history and last-refresh
    /// timestamp. The caller is responsible for actually executing the query.
    pub fn record_refresh(
        &self,
        rows_affected: u64,
        duration_ms: u64,
        total_rows: u64,
        label: Option<String>,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let record = RefreshRecord {
            completed_at_ms: now,
            rows_affected,
            duration_ms,
            label,
            total_rows,
        };

        if let Ok(mut history) = self.history.write() {
            history.push(record);
            let max = self.config.max_history;
            if history.len() > max {
                let excess = history.len() - max;
                history.drain(..excess);
            }
        }

        if let Ok(mut last) = self.last_refresh_epoch_ms.write() {
            *last = Some(now);
        }
    }

    /// Check if a refresh is due based on the configured schedule.
    pub fn is_refresh_due(&self) -> bool {
        match &self.config.schedule {
            RefreshSchedule::Manual => false,
            RefreshSchedule::OnceAt(target) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                now >= *target
            }
            RefreshSchedule::Interval(interval) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let last = self
                    .last_refresh_epoch_ms
                    .read()
                    .ok()
                    .and_then(|v| *v)
                    .unwrap_or(0);
                now.saturating_sub(last) >= interval.as_millis() as u64
            }
        }
    }

    /// Return the epoch-millis of the last successful refresh, or `None`.
    pub fn last_refresh_ms(&self) -> Option<u64> {
        self.last_refresh_epoch_ms.read().ok().and_then(|v| *v)
    }

    /// Return the refresh history (most recent last).
    pub fn history(&self) -> Vec<RefreshRecord> {
        self.history
            .read()
            .ok()
            .map(|h| h.clone())
            .unwrap_or_default()
    }

    /// Build the incremental predicate for the given change column and
    /// watermark. Returns `None` for full-refresh mode.
    #[allow(unused_variables)]
    pub fn incremental_predicate(&self, change_column: &str, after_value: i64) -> Option<String> {
        match &self.config.refresh_mode {
            RefreshMode::Full => None,
            RefreshMode::Incremental { change_column: col } => {
                Some(format!("{col} > {after_value}"))
            }
        }
    }

    /// Return the configured change column for incremental refresh, if any.
    pub fn change_column(&self) -> Option<&str> {
        match &self.config.refresh_mode {
            RefreshMode::Full => None,
            RefreshMode::Incremental { change_column } => Some(change_column),
        }
    }
}

// ── Convenience builder ──────────────────────────────────────────────────────

impl MaterializedTable {
    /// Start building a materialized table with the given name and query.
    pub fn builder(
        name: impl Into<String>,
        query: impl Into<String>,
    ) -> MaterializedTableConfigBuilder {
        MaterializedTableConfigBuilder::new(name, query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_manual_full() {
        let config = MaterializedTableConfig::new("t", "SELECT 1");
        assert_eq!(config.refresh_mode, RefreshMode::Full);
        assert!(matches!(config.schedule, RefreshSchedule::Manual));
    }

    #[test]
    fn builder_sets_refresh_mode() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_refresh_mode(RefreshMode::Incremental {
                change_column: "ts".into(),
            })
            .build();
        assert!(matches!(
            config.refresh_mode,
            RefreshMode::Incremental { .. }
        ));
    }

    #[test]
    fn builder_sets_schedule() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_schedule(RefreshSchedule::interval(Duration::from_secs(60)))
            .build();
        assert!(matches!(config.schedule, RefreshSchedule::Interval(_)));
    }

    #[test]
    fn builder_sets_sink_path() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_sink_path("/data/t")
            .build();
        assert_eq!(config.sink_path.as_deref(), Some("/data/t"));
    }

    #[test]
    fn record_refresh_stores_history() {
        let table = MaterializedTable::new("t", "SELECT 1");
        table.record_refresh(100, 50, 100, Some("init".into()));
        table.record_refresh(10, 10, 110, None);

        let history = table.history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].rows_affected, 100);
        assert_eq!(history[1].rows_affected, 10);
        assert_eq!(history[1].total_rows, 110);
        assert!(history[0].label.as_deref() == Some("init"));
        assert!(history[1].label.is_none());
    }

    #[test]
    fn history_capped_at_max() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_max_history(3)
            .build();
        let table = MaterializedTable::from_config(config);

        for i in 0..10 {
            table.record_refresh(i, 1, i, None);
        }

        let history = table.history();
        assert_eq!(history.len(), 3);
        // Last 3 records: 7, 8, 9
        assert_eq!(history[0].rows_affected, 7);
        assert_eq!(history[2].rows_affected, 9);
    }

    #[test]
    fn is_refresh_due_manual_is_never() {
        let table = MaterializedTable::new("t", "SELECT 1");
        assert!(!table.is_refresh_due());
    }

    #[test]
    fn is_refresh_due_interval_first_call() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_schedule(RefreshSchedule::interval(Duration::from_secs(60)))
            .build();
        let table = MaterializedTable::from_config(config);
        // Never refreshed → due
        assert!(table.is_refresh_due());
    }

    #[test]
    fn is_refresh_due_interval_not_yet() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_schedule(RefreshSchedule::interval(Duration::from_secs(3600)))
            .build();
        let table = MaterializedTable::from_config(config);
        table.record_refresh(10, 1, 10, None);
        // Just refreshed → not due
        assert!(!table.is_refresh_due());
    }

    #[test]
    fn incremental_predicate_generation() {
        let config = MaterializedTableConfig::builder("t", "SELECT * FROM orders")
            .with_refresh_mode(RefreshMode::Incremental {
                change_column: "event_ts".into(),
            })
            .build();
        let table = MaterializedTable::from_config(config);

        let pred = table.incremental_predicate("event_ts", 1000);
        assert_eq!(pred.as_deref(), Some("event_ts > 1000"));

        assert_eq!(table.change_column(), Some("event_ts"));
    }

    #[test]
    fn full_refresh_no_predicate() {
        let table = MaterializedTable::new("t", "SELECT 1");
        assert!(table.incremental_predicate("ts", 0).is_none());
        assert!(table.change_column().is_none());
    }

    #[test]
    fn name_and_query_accessors() {
        let table = MaterializedTable::new("revenue", "SELECT SUM(amount) FROM orders");
        assert_eq!(table.name(), "revenue");
        assert_eq!(table.query(), "SELECT SUM(amount) FROM orders");
    }

    #[test]
    fn last_refresh_ms_returns_none_initially() {
        let table = MaterializedTable::new("t", "SELECT 1");
        assert!(table.last_refresh_ms().is_none());
    }

    #[test]
    fn last_refresh_ms_returns_some_after_record() {
        let table = MaterializedTable::new("t", "SELECT 1");
        table.record_refresh(5, 10, 5, None);
        assert!(table.last_refresh_ms().is_some());
    }

    #[test]
    fn once_at_schedule_is_due_after_target() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_schedule(RefreshSchedule::OnceAt(0)) // epoch 0 = always due
            .build();
        let table = MaterializedTable::from_config(config);
        assert!(table.is_refresh_due());
    }

    #[test]
    fn from_config_preserves_fields() {
        let config = MaterializedTableConfig::builder("t", "SELECT 1")
            .with_refresh_mode(RefreshMode::Incremental {
                change_column: "id".into(),
            })
            .with_sink_path("/data/t")
            .with_max_history(100)
            .build();
        let table = MaterializedTable::from_config(config);
        assert_eq!(table.name(), "t");
        assert_eq!(table.config().sink_path.as_deref(), Some("/data/t"));
        assert_eq!(table.config().max_history, 100);
    }
}
