//! Time-travel specification for lakehouse scans (R18 S4).

use chrono::{DateTime, Utc};

/// Snapshot or version qualifier for table scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsOfSpec {
    /// Iceberg/Delta version number.
    Version(i64),
    /// Wall-clock timestamp (resolved to a snapshot by the catalog).
    Timestamp(DateTime<Utc>),
}

impl AsOfSpec {
    /// Parse user-facing `as_of` strings: integer version or ISO-8601 timestamp.
    pub fn parse(value: &str) -> Result<Self, String> {
        if let Ok(v) = value.parse::<i64>() {
            return Ok(Self::Version(v));
        }
        let ts = DateTime::parse_from_rfc3339(value)
            .map_err(|e| e.to_string())?
            .with_timezone(&Utc);
        Ok(Self::Timestamp(ts))
    }
}
