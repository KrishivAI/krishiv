//! Exactly-once CDC → Iceberg certification (R14 S4.4, in-process).

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use krishiv_connectors::ConnectorError;
use krishiv_connectors::cdc::{CdcEventSource, build_batch_from_events, parse_debezium_envelope};
use krishiv_connectors::lakehouse::{
    IcebergScanOptions, IcebergTableRef, IcebergTwoPhaseCommit, LakehouseTable,
    MemoryIcebergTwoPhaseCommit, MemoryLakehouseTable, SchemaField, SchemaVersion,
};
use krishiv_connectors::transactional::InMemoryTransactionalProducer;

struct JsonCdcSource {
    events: Vec<String>,
    cursor: usize,
}

impl CdcEventSource for JsonCdcSource {
    fn poll_events(&mut self, max: usize) -> Result<Vec<String>, ConnectorError> {
        let end = (self.cursor + max).min(self.events.len());
        if self.cursor >= end {
            return Ok(Vec::new());
        }
        let chunk = self
            .events
            .get(self.cursor..end)
            .unwrap_or_default()
            .to_vec();
        self.cursor = end;
        Ok(chunk)
    }
}

fn table() -> Arc<MemoryLakehouseTable> {
    let schema = SchemaVersion {
        schema_id: 1,
        fields: vec![SchemaField {
            id: 1,
            name: "id".to_string(),
            required: true,
            data_type: "long".to_string(),
        }],
    };
    Arc::new(MemoryLakehouseTable::new(
        IcebergTableRef::new("cat", "ns", "orders"),
        schema,
    ))
}

/// One exactly-once iteration: the producer/offset commit is coupled to the
/// two-phase sink commit — offsets are staged inside an open transaction and
/// committed only after `tpc.commit` succeeds.
async fn process_batch(
    tpc: &MemoryIcebergTwoPhaseCommit,
    txn: &mut InMemoryTransactionalProducer,
    raw: &[String],
    start_offset: usize,
    end_offset: usize,
) -> Result<usize, ConnectorError> {
    let parsed: Vec<_> = raw
        .iter()
        .enumerate()
        .filter_map(|(i, j)| parse_debezium_envelope(j, 0, (start_offset + i) as i64).ok())
        .collect();
    let batch = build_batch_from_events(&parsed).unwrap();
    let rows = batch.num_rows();
    let staged = tpc.prepare(vec![batch]).await.unwrap();

    txn.begin_transaction()?;
    txn.stage_offset("orders-0", end_offset as i64)?;
    // Two-phase sink commit FIRST; only then are the staged offsets committed.
    let offsets: BTreeMap<String, i64> = [("orders-0".to_string(), end_offset as i64)]
        .into_iter()
        .collect();
    tpc.commit(staged, offsets)
        .await
        .map_err(|e| ConnectorError::Cdc(format!("iceberg commit failed: {e}")))?;
    txn.commit_transaction(txn.metadata.committed_offsets.clone())?;
    Ok(rows)
}

#[tokio::test]
async fn exactly_once_ten_thousand_rows_after_crash() {
    let events: Vec<String> = (0..10_000)
        .map(|i| format!(r#"{{"op":"c","after":{{"id":"{i}"}},"source":{{"table":"orders"}}}}"#))
        .collect();

    let lake = table();
    let tpc = MemoryIcebergTwoPhaseCommit::new(lake.clone());
    let mut source = JsonCdcSource { events, cursor: 0 };
    let mut txn = InMemoryTransactionalProducer::new();
    txn.init_transactions().unwrap();

    let mut rows_committed = 0usize;
    let batch_size = 1000usize;

    // Phase 1: process until the simulated crash. The crash happens after the
    // sixth batch is staged (sink prepare + offsets staged in an open
    // transaction) but BEFORE either the sink commit or the offset commit.
    let crashed_staged = loop {
        let start = source.cursor;
        let raw = source.poll_events(batch_size).unwrap();
        if raw.is_empty() {
            break None;
        }
        if rows_committed == 5000 {
            // Stage but crash before commit: neither the snapshot nor the
            // offsets become durable.
            let parsed: Vec<_> = raw
                .iter()
                .enumerate()
                .filter_map(|(i, j)| parse_debezium_envelope(j, 0, (start + i) as i64).ok())
                .collect();
            let batch = build_batch_from_events(&parsed).unwrap();
            let staged = tpc.prepare(vec![batch]).await.unwrap();
            txn.begin_transaction().unwrap();
            txn.stage_offset("orders-0", source.cursor as i64).unwrap();
            break Some(staged);
        }
        rows_committed += process_batch(&tpc, &mut txn, &raw, start, source.cursor)
            .await
            .unwrap();
    };

    // Recovery: abort the in-flight transaction (drops the staged, uncommitted
    // offsets) and the staged-but-uncommitted snapshot, then resume from the
    // offsets the durable sink actually recorded — no hardcoded cursor.
    txn.abort_transaction().unwrap();
    if let Some(staged) = crashed_staged {
        tpc.abort(staged).await.unwrap();
    }
    let recovered_offsets = tpc.committed_kafka_offsets().await;
    let resume_cursor = *recovered_offsets
        .get("orders-0")
        .expect("committed offsets must be recoverable from the sink");
    assert_eq!(
        resume_cursor, 5000,
        "recovery must resume at the last SINK-committed offset (offsets staged \
         before the crash must not have advanced it)"
    );
    source.cursor = usize::try_from(resume_cursor).unwrap();

    // Phase 2: re-run the correct protocol to the end.
    loop {
        let start = source.cursor;
        let raw = source.poll_events(batch_size).unwrap();
        if raw.is_empty() {
            break;
        }
        rows_committed += process_batch(&tpc, &mut txn, &raw, start, source.cursor)
            .await
            .unwrap();
    }

    // No loss AND no duplication: every source row exactly once.
    let scanned = lake.scan(&IcebergScanOptions::default()).await.unwrap();
    let total: usize = scanned.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 10_000, "exactly one row per source event");
    let mut ids = std::collections::HashSet::new();
    for batch in &scanned {
        let idx = batch.schema().index_of("id").unwrap();
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..col.len() {
            assert!(
                ids.insert(col.value(i).to_string()),
                "duplicate row id {} — replayed batch was not deduplicated by the protocol",
                col.value(i)
            );
        }
    }
    assert_eq!(ids.len(), 10_000);
    assert_eq!(rows_committed, 10_000);
}
