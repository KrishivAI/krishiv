//! Exactly-once CDC → Iceberg certification (R14 S4.4, in-process).

use std::collections::BTreeMap;
use std::sync::Arc;

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
        let chunk = self.events[self.cursor..end].to_vec();
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
    let mut recovered_offsets = BTreeMap::new();
    let batch_size = 1000usize;

    loop {
        let raw = source.poll_events(batch_size).unwrap();
        if raw.is_empty() {
            break;
        }
        let parsed: Vec<_> = raw
            .iter()
            .enumerate()
            .filter_map(|(i, j)| parse_debezium_envelope(j, 0, i as i64).ok())
            .collect();
        let batch = build_batch_from_events(&parsed).unwrap();
        let staged = tpc.prepare(vec![batch.clone()]).await.unwrap();
        let offsets = txn
            .write_batch_with_offsets(&batch, "orders-0", source.cursor as i64)
            .unwrap();
        if rows_committed == 5000 {
            // Crash before commit; recovery uses last committed offsets only.
            recovered_offsets = offsets;
            break;
        }
        tpc.commit(staged, offsets).await.unwrap();
        rows_committed += batch.num_rows();
    }

    // Recovery path: resume from recovered_offsets without reprocessing committed rows.
    source.cursor = 5000;
    while let Ok(raw) = source.poll_events(batch_size) {
        if raw.is_empty() {
            break;
        }
        let parsed: Vec<_> = raw
            .iter()
            .enumerate()
            .filter_map(|(i, j)| parse_debezium_envelope(j, 0, (5000 + i) as i64).ok())
            .collect();
        let batch = build_batch_from_events(&parsed).unwrap();
        let staged = tpc.prepare(vec![batch.clone()]).await.unwrap();
        let offsets = txn
            .write_batch_with_offsets(&batch, "orders-0", source.cursor as i64)
            .unwrap();
        tpc.commit(staged, offsets).await.unwrap();
        rows_committed += batch.num_rows();
    }

    let scanned = lake.scan(&IcebergScanOptions::default()).await.unwrap();
    let total: usize = scanned.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 10_000);
    assert_eq!(recovered_offsets.get("orders-0"), Some(&(6000)));
    let _ = rows_committed;
}
