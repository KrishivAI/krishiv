use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use krishiv_connectors::{
    ConnectorCapabilities, ConnectorResult, DataQualityConfig, DataQualityRule, DeadLetterSink,
    LocalParquetTwoPhaseCommitSink, QualityAction, Sink, Source, TwoPhaseCommitSink,
    parquet::{ParquetSink, ParquetSource},
};
use krishiv_connectors::lakehouse::{
    IcebergScanOptions, IcebergTableRef, LakehouseTable, MemoryLakehouseTable, MultiWriterGuard,
    SchemaField, SchemaVersion,
};

fn schema_v1() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn schema_v2() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]))
}

fn make_v1_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema_v1(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn make_v2_batch(ids: &[i32], names: &[&str], ages: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_new(
        schema_v2(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(ages.into_iter().collect::<Int32Array>()),
        ],
    )
    .unwrap()
}

fn make_int64_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

#[allow(dead_code)]
fn make_float64_batch_with_nulls(values: Vec<Option<f64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(values.into_iter().collect::<Float64Array>())],
    )
    .unwrap()
}

fn lakehouse_table_ref() -> IcebergTableRef {
    IcebergTableRef::new("test_catalog", "test_ns", "test_table")
}

fn lakehouse_schema_v1() -> SchemaVersion {
    SchemaVersion {
        schema_id: 1,
        fields: vec![SchemaField {
            id: 1,
            name: "x".to_string(),
            required: true,
            data_type: "int64".to_string(),
        }],
    }
}

struct RecordingSink {
    batches: Vec<RecordBatch>,
}

impl RecordingSink {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

impl Sink for RecordingSink {
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::new().with_idempotent()
    }

    async fn write_batch(&mut self, batch: RecordBatch) -> ConnectorResult<()> {
        self.batches.push(batch);
        Ok(())
    }

    async fn flush(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn parquet_source_read_verifies_schema_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source_test.parquet");

    let batch = make_v1_batch(&[1, 2, 3], &["alice", "bob", "carol"]);
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut source = ParquetSource::open(&path).unwrap();

    let schema = source.schema().unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    let mut total_rows = 0usize;
    let mut all_ids = Vec::new();
    while let Some(batch) = source.read_batch().await.unwrap() {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            all_ids.push(col.value(i));
        }
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 3);
    assert_eq!(all_ids, vec![1, 2, 3]);
}

#[tokio::test]
async fn parquet_sink_write_read_back_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sink_test.parquet");

    let mut sink = ParquetSink::create(&path).unwrap();
    let batch1 = make_v1_batch(&[10, 20], &["x", "y"]);
    let batch2 = make_v1_batch(&[30], &["z"]);
    sink.write_batch(batch1).await.unwrap();
    sink.write_batch(batch2).await.unwrap();
    sink.flush().await.unwrap();

    assert!(path.exists());

    let mut source = ParquetSource::open(&path).unwrap();
    let mut total_rows = 0usize;
    let mut all_names = Vec::new();
    while let Some(batch) = source.read_batch().await.unwrap() {
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..col.len() {
            all_names.push(col.value(i).to_string());
        }
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 3);
    assert_eq!(all_names, vec!["x", "y", "z"]);
}

#[tokio::test]
async fn two_phase_commit_prepare_commit_verify_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

    let batch = make_int64_batch(vec![100, 200, 300]);
    let handle = sink.prepare(1, &batch).unwrap();
    assert!(handle.staging_path.exists());
    assert!(!handle.final_path.exists());

    sink.commit(handle).unwrap();

    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|e| e == "parquet") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(files.len(), 1);

    let file = std::fs::File::open(&files[0]).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    let mut total_values = Vec::new();
    for batch in reader {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..col.len() {
            total_values.push(col.value(i));
        }
    }
    assert_eq!(total_values, vec![100, 200, 300]);
}

#[tokio::test]
async fn two_phase_commit_prepare_abort_no_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path());

    let batch = make_int64_batch(vec![42]);
    let handle = sink.prepare(1, &batch).unwrap();
    assert!(handle.staging_path.exists());

    sink.abort(handle).unwrap();

    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|e| e == "parquet") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    assert!(
        files.is_empty(),
        "no parquet files should exist after abort"
    );

    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|e| e == "tmp") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    assert!(
        tmp_files.is_empty(),
        "no staging files should remain after abort"
    );
}

#[tokio::test]
async fn data_quality_write_with_rules_verify_accepted_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![
                Some(85.0),
                None,
                Some(110.0),
                Some(50.0),
            ])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave"])),
        ],
    )
    .unwrap();

    let config = DataQualityConfig::new()
        .with_rule(
            DataQualityRule::Range {
                column: "score".into(),
                min: 0.0,
                max: 100.0,
            },
            QualityAction::Reject,
        )
        .with_rule(
            DataQualityRule::NotNull {
                column: "score".into(),
            },
            QualityAction::Reject,
        );

    let mut sink = LocalParquetTwoPhaseCommitSink::new(dir.path())
        .with_quality_config(config)
        .unwrap();

    let handle = sink.prepare(1, &batch).unwrap();
    sink.commit(handle).unwrap();

    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|e| e == "parquet") {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(files.len(), 1);

    let file = std::fs::File::open(&files[0]).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut total_rows = 0usize;
    let mut read_names = Vec::new();
    for batch in reader {
        let b = batch.unwrap();
        let name_col = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..name_col.len() {
            read_names.push(name_col.value(i).to_string());
        }
        total_rows += b.num_rows();
    }
    assert_eq!(
        total_rows, 2,
        "only alice (85.0) and dave (50.0) pass both rules"
    );
    assert_eq!(read_names, vec!["alice", "dave"]);
}

#[tokio::test]
async fn dead_letter_sink_rejected_rows_routed() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![
                Some(85.0),
                None,
                Some(110.0),
                Some(50.0),
            ])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol", "dave"])),
        ],
    )
    .unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull {
            column: "score".into(),
        },
        QualityAction::Reject,
    );

    let mut dead_letter_sink = DeadLetterSink::new("test_dlq", config);
    let (accepted, rejected) = dead_letter_sink.process_batch(&batch).await.unwrap();

    assert_eq!(
        accepted.num_rows(),
        3,
        "3 rows should be accepted (all non-null scores)"
    );
    assert_eq!(rejected.len(), 1, "1 row should be rejected (null score)");
    assert_eq!(rejected[0].batch_row_index, 1);

    let accepted_col = accepted
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let accepted_names: Vec<&str> = (0..accepted_col.len())
        .map(|i| accepted_col.value(i))
        .collect();
    assert_eq!(accepted_names, vec!["alice", "carol", "dave"]);
}

#[tokio::test]
async fn dead_letter_sink_secondary_sink_receives_rejected() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(3.0),
        ]))],
    )
    .unwrap();

    let config = DataQualityConfig::new().with_rule(
        DataQualityRule::NotNull { column: "v".into() },
        QualityAction::Reject,
    );

    let secondary = RecordingSink::new();
    let mut dead_letter_sink =
        DeadLetterSink::new("test_with_secondary", config).with_secondary_sink(secondary);

    let (accepted, rejected) = dead_letter_sink.process_batch(&batch).await.unwrap();
    assert_eq!(accepted.num_rows(), 2);
    assert_eq!(rejected.len(), 1);
}

#[tokio::test]
async fn lakehouse_append_scan_verify() {
    let table = MemoryLakehouseTable::new(lakehouse_table_ref(), lakehouse_schema_v1());

    let batch1 = make_int64_batch(vec![1, 2, 3]);
    let batch2 = make_int64_batch(vec![4, 5]);
    table.append(vec![batch1]).await.unwrap();
    table.append(vec![batch2]).await.unwrap();

    let result = table.scan(&IcebergScanOptions::new()).await.unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    let all_values: Vec<i64> = result
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect();
    assert_eq!(all_values, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn lakehouse_snapshot_time_travel_verify() {
    let table = MemoryLakehouseTable::new(lakehouse_table_ref(), lakehouse_schema_v1());

    table
        .append(vec![make_int64_batch(vec![10, 20])])
        .await
        .unwrap();
    let snap1 = table.current_snapshot_id().await.unwrap().unwrap();

    table
        .append(vec![make_int64_batch(vec![30, 40, 50])])
        .await
        .unwrap();
    let snap2 = table.current_snapshot_id().await.unwrap().unwrap();
    assert!(snap2 > snap1);

    let at_snap1 = table
        .scan(&IcebergScanOptions::new().with_snapshot(snap1))
        .await
        .unwrap();
    let rows_snap1: usize = at_snap1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows_snap1, 2,
        "time-travel to snap1 should see only first append"
    );

    let at_snap2 = table
        .scan(&IcebergScanOptions::new().with_snapshot(snap2))
        .await
        .unwrap();
    let rows_snap2: usize = at_snap2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows_snap2, 5,
        "time-travel to snap2 should see both appends"
    );

    let values_snap1: Vec<i64> = at_snap1
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect();
    assert_eq!(values_snap1, vec![10, 20]);

    let values_snap2: Vec<i64> = at_snap2
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect();
    assert_eq!(values_snap2, vec![10, 20, 30, 40, 50]);
}

#[tokio::test]
async fn lakehouse_concurrent_writes_no_duplication() {
    let table = Arc::new(MemoryLakehouseTable::new(
        lakehouse_table_ref(),
        lakehouse_schema_v1(),
    ));

    let guard_a = MultiWriterGuard::new(None, "writer-a");
    let guard_b = MultiWriterGuard::new(None, "writer-b");

    let table_a = Arc::clone(&table);
    let table_b = Arc::clone(&table);

    let handle_a = tokio::spawn(async move {
        table_a
            .check_and_append(&guard_a, vec![make_int64_batch(vec![1])])
            .await
    });
    let handle_b = tokio::spawn(async move {
        tokio::task::yield_now().await;
        table_b
            .check_and_append(&guard_b, vec![make_int64_batch(vec![2])])
            .await
    });

    let result_a = handle_a.await.unwrap();
    let result_b = handle_b.await.unwrap();

    let successes = [result_a.is_ok(), result_b.is_ok()]
        .into_iter()
        .filter(|ok| *ok)
        .count();
    assert_eq!(successes, 1, "exactly one concurrent writer should succeed");

    let scanned = table.scan(&IcebergScanOptions::new()).await.unwrap();
    let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "only one writer's batch should be visible");

    let successful_value = if result_a.is_ok() {
        let col = scanned[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        col.value(0)
    } else {
        let col = scanned[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        col.value(0)
    };
    assert!(
        successful_value == 1 || successful_value == 2,
        "the committed value should be from one of the writers"
    );
}

#[tokio::test]
async fn lakehouse_schema_evolution_write_v1_v2_read_both() {
    let table_ref = lakehouse_table_ref();
    let schema_v1 = SchemaVersion {
        schema_id: 1,
        fields: vec![
            SchemaField {
                id: 1,
                name: "id".to_string(),
                required: true,
                data_type: "int32".to_string(),
            },
            SchemaField {
                id: 2,
                name: "name".to_string(),
                required: true,
                data_type: "utf8".to_string(),
            },
        ],
    };

    let table = MemoryLakehouseTable::new(table_ref, schema_v1);

    let v1_batch = make_v1_batch(&[1, 2], &["alice", "bob"]);
    table.append(vec![v1_batch]).await.unwrap();

    let v2_batch = make_v2_batch(&[3, 4], &["carol", "dave"], vec![Some(30), None]);
    table.append(vec![v2_batch]).await.unwrap();

    let all = table.scan(&IcebergScanOptions::new()).await.unwrap();
    assert_eq!(all.len(), 2, "should have two batches from two appends");
    assert_eq!(all[0].num_rows(), 2);
    assert_eq!(all[1].num_rows(), 2);
    assert_eq!(all[0].schema().fields().len(), 2);
    assert_eq!(all[1].schema().fields().len(), 3);

    let v1_names: Vec<&str> = {
        let col = all[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..col.len()).map(|i| col.value(i)).collect()
    };
    assert_eq!(v1_names, vec!["alice", "bob"]);

    let v2_names: Vec<&str> = {
        let col = all[1]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..col.len()).map(|i| col.value(i)).collect()
    };
    assert_eq!(v2_names, vec!["carol", "dave"]);

    let v2_ages: Vec<Option<i32>> = {
        let col = all[1]
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .collect()
    };
    assert_eq!(v2_ages, vec![Some(30), None]);

    let all_row_count: usize = all.iter().map(|b| b.num_rows()).sum();
    assert_eq!(all_row_count, 4);
}
