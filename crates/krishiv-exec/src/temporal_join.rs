//! Stream-table temporal (as-of) join (R16 S3.1).

use std::collections::BTreeMap;

use arrow::record_batch::RecordBatch;

/// Specification for a stream-table temporal join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalJoinSpec {
    pub stream_time_col: String,
    pub table_version_col: String,
    pub join_keys: Vec<String>,
    pub inner_join: bool,
}

/// Versioned table state per join key.
#[derive(Debug, Default)]
pub struct VersionedTableState {
    /// version_ms -> batch snapshot for that version
    versions: BTreeMap<i64, RecordBatch>,
    lookback_ms: i64,
}

impl VersionedTableState {
    pub fn new(lookback_ms: i64) -> Self {
        Self {
            versions: BTreeMap::new(),
            lookback_ms,
        }
    }

    pub fn upsert_version(&mut self, version_ms: i64, batch: RecordBatch) {
        self.versions.insert(version_ms, batch);
        let min_version = version_ms - self.lookback_ms;
        while let Some((&k, _)) = self.versions.first_key_value() {
            if k < min_version {
                self.versions.pop_first();
            } else {
                break;
            }
        }
    }

    /// Latest table version where `version_ms <= stream_time_ms`.
    pub fn lookup_as_of(&self, stream_time_ms: i64) -> Option<&RecordBatch> {
        self.versions
            .range(..=stream_time_ms)
            .next_back()
            .map(|(_, b)| b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn version_batch(v: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![v]))],
        )
        .unwrap()
    }

    #[test]
    fn as_of_lookup_returns_latest_valid_version() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        assert_eq!(
            state
                .lookup_as_of(2500)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert_eq!(
            state
                .lookup_as_of(1500)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(state.lookup_as_of(500).is_none());
    }

    #[test]
    fn upsert_evicts_old_versions_beyond_lookback() {
        let mut state = VersionedTableState::new(5000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        state.upsert_version(7000, version_batch(3));
        // lookback=5000, version 7000: min_version=2000
        // version 1000 < 2000 → evicted
        assert!(state.lookup_as_of(1000).is_none());
        assert!(state.lookup_as_of(2000).is_some());
        assert!(state.lookup_as_of(7000).is_some());
    }

    #[test]
    fn upsert_exact_lookback_boundary() {
        let mut state = VersionedTableState::new(1000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        // min_version = 2000 - 1000 = 1000
        // version 1000 is NOT < 1000 → not evicted
        assert!(state.lookup_as_of(1000).is_some());
    }

    #[test]
    fn lookup_as_of_exact_version_match() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(5000, version_batch(42));
        assert_eq!(
            state
                .lookup_as_of(5000)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[test]
    fn lookup_as_of_between_versions_returns_previous() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(10));
        state.upsert_version(3000, version_batch(30));
        // query at 2000 → should return version at 1000
        assert_eq!(
            state
                .lookup_as_of(2000)
                .unwrap()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
    }

    #[test]
    fn lookup_as_of_empty_state_returns_none() {
        let state = VersionedTableState::new(10_000);
        assert!(state.lookup_as_of(1000).is_none());
    }

    #[test]
    fn upsert_replaces_same_version() {
        let mut state = VersionedTableState::new(10_000);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(1000, version_batch(99));
        let val = state
            .lookup_as_of(1000)
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 99);
    }

    #[test]
    fn large_lookback_keeps_all_versions() {
        let mut state = VersionedTableState::new(i64::MAX as u64 as i64);
        state.upsert_version(0, version_batch(0));
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(100_000, version_batch(2));
        assert!(state.lookup_as_of(0).is_some());
        assert!(state.lookup_as_of(1000).is_some());
        assert!(state.lookup_as_of(100_000).is_some());
    }

    #[test]
    fn zero_lookback_evicts_all_except_latest() {
        let mut state = VersionedTableState::new(0);
        state.upsert_version(1000, version_batch(1));
        state.upsert_version(2000, version_batch(2));
        // min_version = 2000 - 0 = 2000
        // version 1000 < 2000 → evicted
        assert!(state.lookup_as_of(1000).is_none());
        assert!(state.lookup_as_of(2000).is_some());
    }

    #[test]
    fn temporal_join_spec_fields() {
        let spec = TemporalJoinSpec {
            stream_time_col: "event_ts".into(),
            table_version_col: "version".into(),
            join_keys: vec!["id".into()],
            inner_join: true,
        };
        assert_eq!(spec.stream_time_col, "event_ts");
        assert_eq!(spec.table_version_col, "version");
        assert_eq!(spec.join_keys, vec!["id"]);
        assert!(spec.inner_join);
    }
}
