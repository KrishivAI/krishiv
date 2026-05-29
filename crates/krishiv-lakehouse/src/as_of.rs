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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_integer_version() {
        let spec = AsOfSpec::parse("42").unwrap();
        assert_eq!(spec, AsOfSpec::Version(42));
    }

    #[test]
    fn parse_negative_version() {
        let spec = AsOfSpec::parse("-1").unwrap();
        assert_eq!(spec, AsOfSpec::Version(-1));
    }

    #[test]
    fn parse_zero_version() {
        let spec = AsOfSpec::parse("0").unwrap();
        assert_eq!(spec, AsOfSpec::Version(0));
    }

    #[test]
    fn parse_rfc3339_timestamp() {
        let spec = AsOfSpec::parse("2024-06-15T10:30:00Z").unwrap();
        match spec {
            AsOfSpec::Timestamp(ts) => {
                assert_eq!(
                    ts.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    "2024-06-15T10:30:00Z"
                );
            }
            _ => panic!("expected Timestamp variant"),
        }
    }

    #[test]
    fn parse_rfc3339_with_offset() {
        let spec = AsOfSpec::parse("2024-01-01T00:00:00+05:30").unwrap();
        match spec {
            AsOfSpec::Timestamp(ts) => {
                assert_eq!(
                    ts.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    "2023-12-31T18:30:00Z"
                );
            }
            _ => panic!("expected Timestamp variant"),
        }
    }

    #[test]
    fn parse_invalid_string_fails() {
        assert!(AsOfSpec::parse("not-a-number").is_err());
        assert!(AsOfSpec::parse("2024-13-01T00:00:00Z").is_err());
        assert!(AsOfSpec::parse("").is_err());
    }

    #[test]
    fn equality_version_variants() {
        assert_eq!(AsOfSpec::Version(1), AsOfSpec::Version(1));
        assert_ne!(AsOfSpec::Version(1), AsOfSpec::Version(2));
    }

    #[test]
    fn equality_timestamp_variants() {
        let ts1 = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let ts2 = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(AsOfSpec::Timestamp(ts1), AsOfSpec::Timestamp(ts2));
    }

    #[test]
    fn inequality_across_variants() {
        let ts = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_ne!(AsOfSpec::Version(1), AsOfSpec::Timestamp(ts));
    }

    #[test]
    fn debug_format_version() {
        let spec = AsOfSpec::Version(5);
        let dbg = format!("{:?}", spec);
        assert!(dbg.contains("Version"));
        assert!(dbg.contains("5"));
    }

    #[test]
    fn debug_format_timestamp() {
        let ts = DateTime::parse_from_rfc3339("2024-06-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let spec = AsOfSpec::Timestamp(ts);
        let dbg = format!("{:?}", spec);
        assert!(dbg.contains("Timestamp"));
    }

    #[test]
    fn clone_preserves_value() {
        let spec = AsOfSpec::Version(99);
        let cloned = spec.clone();
        assert_eq!(spec, cloned);
    }

    #[test]
    fn large_version_number() {
        let spec = AsOfSpec::parse("999999999999").unwrap();
        assert_eq!(spec, AsOfSpec::Version(999999999999));
    }
}
