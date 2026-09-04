//! Does registering a Parquet table put its row count in the engine's registry?
//!
//! `SqlEngine::register_parquet_with_primary_key` ends with
//!
//! ```text
//!     if let Ok(provider) = self.context.table_provider(table_name).await
//!         && let Some(stats) = provider.statistics()
//! ```
//!
//! and `TableProvider::statistics()` is a trait method whose default is `None`.
//! DataFusion's `ListingTable` — the provider behind every `--parquet` table
//! and every `CREATE EXTERNAL TABLE … STORED AS PARQUET` — does not implement
//! it. So the `&&` chain stops at the second link for exactly the tables the
//! registry exists to describe, `table_row_counts` stays empty, and
//! `BroadcastAutoRule`, which reads it, can never fire for a Parquet table.
//!
//! The row counts do exist: `collect_statistics` is on in
//! `build_single_node_session_config`, so `ListingTable` gathers them from the
//! Parquet footers when a scan is planned. They are reachable through
//! `partition_statistics` on the planned scan, which is where
//! `spillable_join` and `join_estimates` already read estimates.
//!
//! This test asserts the count is recorded, and asserts it is *exact* — an
//! `Absent` estimate silently coerced to 0 would satisfy a mere "is present".

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use krishiv_sql::SqlEngine;
use std::sync::Arc;

const ROWS: usize = 512;

fn write_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Int64Array = (0..ROWS as i64).collect::<Vec<_>>().into();
    let names: StringArray = (0..ROWS)
        .map(|i| format!("row-{i}"))
        .collect::<Vec<_>>()
        .into();
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(names)]).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn registering_a_parquet_table_records_its_row_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.parquet");
    write_parquet(&path);

    let engine = SqlEngine::new();
    engine.register_parquet("rows", &path).await.unwrap();

    let counts = engine.table_row_counts();
    let recorded = counts.read().unwrap().get("rows").copied();

    assert_eq!(
        recorded,
        Some(ROWS as u64),
        "registering a Parquet table must record its row count; \
         `TableProvider::statistics()` returns None for ListingTable, so the \
         count has to come from the planned scan's `partition_statistics`"
    );
}
