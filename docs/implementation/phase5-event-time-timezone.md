# Phase 5: Event Time, Timezone, and SQL Semantics

## Goal

Make event-time semantics correct without contaminating checkpoint ordering by normalizing to UTC and adding timezone-aware SQL bucketing.

## Design

### 1. UTC Normalization

```rust
// In krishiv-plan/src/window.rs

/// Timezone-aware timestamp for event time.
#[derive(Debug, Clone, Copy)]
pub struct EventTime {
    /// UTC epoch milliseconds.
    pub epoch_ms: i64,
    
    /// Optional timezone for display/bucketing (not for ordering).
    pub tz: Option<TimeZoneRef>,
}

/// Timezone reference for SQL semantics.
#[derive(Debug, Clone)]
pub enum TimeZoneRef {
    /// UTC timezone.
    Utc,
    
    /// Fixed offset from UTC.
    Fixed(i32), // offset in seconds
    
    /// Named timezone (e.g., "America/New_York").
    Named(String),
}

impl EventTime {
    /// Create a new event time in UTC.
    pub fn utc(epoch_ms: i64) -> Self {
        Self {
            epoch_ms,
            tz: None,
        }
    }
    
    /// Create a new event time with timezone for display.
    pub fn with_tz(epoch_ms: i64, tz: TimeZoneRef) -> Self {
        Self {
            epoch_ms,
            tz: Some(tz),
        }
    }
    
    /// Get epoch milliseconds (always UTC for ordering).
    pub fn epoch_ms(&self) -> i64 {
        self.epoch_ms
    }
    
    /// Get timezone for display/bucketing.
    pub fn tz(&self) -> Option<&TimeZoneRef> {
        self.tz.as_ref()
    }
}
```

### 2. Window Timezone Support

```rust
// In krishiv-plan/src/window.rs

/// Window specification with timezone support.
pub struct WindowSpec {
    /// Window kind (tumbling, sliding, session).
    pub kind: WindowKind,
    
    /// Event time column.
    pub time_column: String,
    
    /// Optional timezone for civil-time bucketing.
    /// Only used for SQL window functions that need timezone-aware bucketing.
    pub timezone: Option<TimeZoneRef>,
}

impl WindowSpec {
    /// Create a tumbling window.
    pub fn tumbling(size_ms: u64, time_column: &str) -> Self {
        Self {
            kind: WindowKind::Tumbling { size_ms },
            time_column: time_column.to_string(),
            timezone: None,
        }
    }
    
    /// Create a tumbling window with timezone for civil-time bucketing.
    pub fn tumbling_with_tz(size_ms: u64, time_column: &str, tz: TimeZoneRef) -> Self {
        Self {
            kind: WindowKind::Tumbling { size_ms },
            time_column: time_column.to_string(),
            timezone: Some(tz),
        }
    }
}
```

### 3. SQL Timezone Functions

```sql
-- Timezone conversion for display
SELECT event_time AT TIME ZONE 'America/New_York' AS local_time
FROM events;

-- Civil-time tumbling window
SELECT * FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '1 minute')
    WITH TIMEZONE 'America/New_York'
);

-- Civil-time sliding window
SELECT * FROM TABLE(
    HOP(TABLE events, DESCRIPTOR(event_time), INTERVAL '5 seconds', INTERVAL '1 minute')
    WITH TIMEZONE 'America/New_York'
);

-- Civil-time session window
SELECT * FROM TABLE(
    SESSION(TABLE events, DESCRIPTOR(event_time), INTERVAL '30 minutes')
    WITH TIMEZONE 'America/New_York'
);
```

## Files to Modify

| File | Change |
|------|--------|
| `crates/krishiv-plan/src/window.rs` | Add `EventTime`, `TimeZoneRef`, timezone-aware `WindowSpec` |
| `crates/krishiv-sql/src/streaming_tvf.rs` | Add `WITH TIMEZONE` syntax for window TVFs |
| `crates/krishiv-sql/src/window_functions.rs` | Add timezone conversion functions |
| `crates/krishiv-api/src/window.rs` | Expose timezone options in API |
| `crates/krishiv-python/src/stream.rs` | Add Python bindings for timezone options |

## Acceptance Tests

1. UTC watermarks are monotonic across timezone conversions
2. Civil-time tumbling windows around DST transitions behave according to configured timezone
3. Existing timestamp-without-timezone behavior remains backward-compatible
