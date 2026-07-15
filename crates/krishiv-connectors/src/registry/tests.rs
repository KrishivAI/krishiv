//! Registry integration tests.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use crate::config::ConnectorConfig;
use crate::registry::{
    ConnectorKind, ConnectorRegistry, ConnectorRole, default_registry,
    drivers::{ParquetSinkDriver, ParquetSourceDriver},
};

fn parquet_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap()
}

#[tokio::test]
async fn default_registry_opens_parquet_source_and_sink() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");
    {
        let mut file = std::fs::File::create(&path).unwrap();
        let batch = parquet_batch();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let registry = default_registry();
    let source_config =
        ConnectorConfig::new("src", "parquet").with_property("path", path.display().to_string());
    let mut source = registry.open_source(&source_config).await.unwrap();
    let batch = source.read_batch_dyn().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let sink_path = dir.path().join("out.parquet");
    let sink_config = ConnectorConfig::new("sink", "parquet")
        .with_property("path", sink_path.display().to_string());
    let mut sink = registry.open_sink(&sink_config).await.unwrap();
    sink.write_batch_dyn(parquet_batch()).await.unwrap();
    sink.flush_dyn().await.unwrap();
    assert!(sink_path.is_file());
}

#[test]
fn connector_kind_parse_is_case_insensitive() {
    assert_eq!(
        ConnectorKind::parse("PARQUET").unwrap(),
        ConnectorKind::Parquet
    );
    assert_eq!(ConnectorKind::parse("s3").unwrap(), ConnectorKind::S3);
}

#[test]
fn custom_driver_registration() {
    let mut registry = ConnectorRegistry::new();
    registry.register_source(Arc::new(ParquetSourceDriver));
    registry.register_sink(Arc::new(ParquetSinkDriver));
    assert!(registry.has_driver(ConnectorKind::Parquet, ConnectorRole::Source));
    assert!(registry.has_driver(ConnectorKind::Parquet, ConnectorRole::Sink));
}

/// Phase 31 ingest breadth: the JDBC driver's incremental-cursor options are
/// validated fail-fast (no live database needed) — `cursor.after` needs
/// `cursor.column` and must be an Int64.
#[cfg(feature = "jdbc")]
#[test]
fn jdbc_source_validate_rejects_bad_cursor_options() {
    let registry = default_registry();

    let ok = ConnectorConfig::new("t", "jdbc")
        .with_property("url", "postgres://u:p@127.0.0.1:1/db")
        .with_property("table", "public.orders")
        .with_property("cursor.column", "id")
        .with_property("cursor.after", "42");
    registry.validate_source(&ok).expect("valid cursor config");

    let dangling = ConnectorConfig::new("t", "jdbc")
        .with_property("url", "postgres://u:p@127.0.0.1:1/db")
        .with_property("table", "public.orders")
        .with_property("cursor.after", "42");
    let err = registry.validate_source(&dangling).unwrap_err();
    assert!(
        err.to_string().contains("cursor.after requires cursor.column"),
        "{err}"
    );

    let non_int = ConnectorConfig::new("t", "jdbc")
        .with_property("url", "postgres://u:p@127.0.0.1:1/db")
        .with_property("table", "public.orders")
        .with_property("cursor.column", "id")
        .with_property("cursor.after", "2026-07-15");
    let err = registry.validate_source(&non_int).unwrap_err();
    assert!(err.to_string().contains("64-bit integer"), "{err}");
}
