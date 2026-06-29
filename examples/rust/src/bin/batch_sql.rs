#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let parquet_path = temp.path().join("people.parquet");
    write_people_parquet(&parquet_path)?;

    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;
    session.register_parquet("people", &parquet_path)?;

    let result = session
        .sql("select city, count(*) as count from people group by city order by city")?
        .collect()?;

    println!("{}", result.pretty()?);
    Ok(())
}

fn write_people_parquet(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["London", "Paris", "London"])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
