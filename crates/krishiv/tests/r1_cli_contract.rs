#![forbid(unsafe_code)]

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
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

/// `--primary-key` reaches the engine and does not change the answer.
///
/// The declaration licenses the optimizer to drop grouping columns and
/// re-fetch them (`krishiv_sql::late_materialize`), so the property that
/// matters is that a query returns the same rows either way. Without this the
/// embedded path could declare a key and silently plan something wrong, which
/// is exactly the failure the distributed side has a 22-query gate for.
#[test]
fn a_declared_primary_key_does_not_change_the_answer() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let parquet_arg = format!("people={}", parquet_path.display());
    let query = "select id, city, count(*) as n from people group by id, city \
                 order by n desc, id limit 10";

    let (plain_code, plain_out, plain_err) =
        run(&["sql", "--parquet", &parquet_arg, "--query", query]);
    let (keyed_code, keyed_out, keyed_err) = run(&[
        "sql",
        "--parquet",
        &parquet_arg,
        "--primary-key",
        "people=id",
        "--query",
        query,
    ]);

    assert_eq!(plain_code, 0, "{plain_err}");
    assert_eq!(keyed_code, 0, "{keyed_err}");
    assert_eq!(
        normalize(&plain_out),
        normalize(&keyed_out),
        "declaring a primary key changed the result"
    );
}

/// A key naming a column the file does not have must fail loudly. A silent
/// no-op would surface only as "the optimization mysteriously does nothing".
#[test]
fn an_unknown_primary_key_column_is_rejected() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let parquet_arg = format!("people={}", parquet_path.display());
    let (code, _stdout, stderr) = run(&[
        "sql",
        "--parquet",
        &parquet_arg,
        "--primary-key",
        "people=not_a_column",
        "--query",
        "select * from people",
    ]);
    assert_eq!(code, 1);
    assert!(
        stderr.contains("not_a_column"),
        "the error must name the offending column: {stderr}"
    );
}

/// A key for a table no `--parquet` registered is a typo, and one whose only
/// symptom would otherwise be an optimization that never applies.
#[test]
fn a_primary_key_for_an_unregistered_table_is_rejected() {
    let temp = tempdir().unwrap_or_else(|error| panic!("unexpected tempdir error: {error}"));
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path);
    let parquet_arg = format!("people={}", parquet_path.display());
    let (code, _stdout, stderr) = run(&[
        "sql",
        "--parquet",
        &parquet_arg,
        "--primary-key",
        "peple=id",
        "--query",
        "select * from people",
    ]);
    // 2, not 1: this is caught while parsing arguments, so it is a usage error
    // rather than a query error — the same exit code any other malformed flag
    // produces.
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains("peple"),
        "the error must name the unregistered table: {stderr}"
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
    let file =
        File::create(path).unwrap_or_else(|error| panic!("unexpected parquet file error: {error}"));
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
