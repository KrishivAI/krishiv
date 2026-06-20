#![forbid(unsafe_code)]

use std::fs::File;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_krishiv"))
        .args(args)
        .output()
        .expect("failed to run krishiv binary");
    (
        out.status.code().unwrap_or(1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn parquet_projection_filter_aggregate_limit_matches_golden_output() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let parquet_arg = format!("people={}", parquet_path.display());

    let (code, stdout, stderr) = run(&[
        "sql",
        "--parquet",
        &parquet_arg,
        "--query",
        "select city, count(*) as count from people where id > 1 group by city order by city limit 2",
    ]);

    assert_eq!(code, 0, "{stderr}");
    assert_eq!(
        normalize(&stdout),
        include_str!("../../../tests/golden/r1-sql-parquet-aggregate.txt")
    );
}

#[test]
fn invalid_sql_returns_error() {
    let (code, _stdout, stderr) = run(&["sql", "--query", "select from"]);
    assert_eq!(code, 1);
    assert!(stderr.contains("DataFusion error"));
}

#[test]
fn missing_parquet_file_returns_error() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_arg = format!("people={}", temp.path().join("missing.parquet").display());
    let (code, _stdout, stderr) = run(&[
        "sql",
        "--parquet",
        &parquet_arg,
        "--query",
        "select * from people",
    ]);

    assert_eq!(code, 1);
    assert!(stderr.contains("DataFusion error"));
}

fn write_people_parquet(path: &std::path::Path) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("city", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["London", "Paris", "London"])),
        ],
    )
    .unwrap_or_else(|error| panic!("unexpected record batch error: {error}"));
    let file = File::create(path)
        .unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .unwrap_or_else(|error| panic!("unexpected parquet writer error: {error}"));
    writer
        .write(&batch)
        .unwrap_or_else(|error| panic!("unexpected parquet write error: {error}"));
    writer
        .close()
        .unwrap_or_else(|error| panic!("unexpected parquet close error: {error}"));
}

fn normalize(value: &str) -> String {
    value.replace("\r\n", "\n")
}
