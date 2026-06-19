#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let parquet_path = temp.path().join("retail_orders.parquet");
    write_retail_orders(&parquet_path)?;

    let session = Session::builder()
        .with_execution_mode(ExecutionMode::Embedded)
        .build()?;
    session.register_parquet("retail_orders", &parquet_path)?;

    let result = session
        .sql(
            "select region, channel, count(*) as orders, sum(amount_cents) as revenue_cents \
             from retail_orders \
             group by region, channel \
             order by region, channel",
        )?
        .collect()?;

    println!("Embedded batch: retail revenue by region and channel");
    println!("{}", result.pretty()?);
    Ok(())
}

fn write_retail_orders(path: &Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1001, 1002, 1003, 1004, 1005, 1006])),
            Arc::new(StringArray::from(vec![
                "north", "north", "south", "south", "west", "west",
            ])),
            Arc::new(StringArray::from(vec![
                "store", "web", "web", "store", "store", "web",
            ])),
            Arc::new(Int64Array::from(vec![
                12_900, 8_450, 22_100, 6_775, 9_999, 18_250,
            ])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
