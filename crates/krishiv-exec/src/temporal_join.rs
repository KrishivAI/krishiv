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
}
