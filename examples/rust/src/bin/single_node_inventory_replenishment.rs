#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session, SessionBuilder};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let parquet_path = temp.path().join("warehouse_inventory.parquet");
    write_inventory_snapshot(&parquet_path)?;

    let builder = match std::env::var("KRISHIV_COORDINATOR") {
        Ok(flight_url) => SessionBuilder::new()
            .with_local_cluster(flight_url)
            .with_remote_execution(true),
        Err(_) => Session::builder().with_execution_mode(ExecutionMode::SingleNode),
    };
    let session = builder.build()?;
    session.register_parquet("inventory", &parquet_path)?;

    let result = session
        .sql(
            "select warehouse, sku, on_hand, reorder_point, daily_demand, \
                    (reorder_point - on_hand) as units_to_reorder \
             from inventory \
             where on_hand < reorder_point \
             order by warehouse, sku",
        )?
        .collect()?;

    println!("Single-node batch: replenishment candidates");
    println!("{}", result.pretty()?);
    Ok(())
}

fn write_inventory_snapshot(path: &Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("warehouse", DataType::Utf8, false),
        Field::new("sku", DataType::Utf8, false),
        Field::new("on_hand", DataType::Int64, false),
        Field::new("reorder_point", DataType::Int64, false),
        Field::new("daily_demand", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "iad-1", "iad-1", "dfw-2", "dfw-2", "sfo-1",
            ])),
            Arc::new(StringArray::from(vec![
                "coffee", "filters", "tea", "cups", "sugar",
            ])),
            Arc::new(Int64Array::from(vec![42, 120, 18, 900, 24])),
            Arc::new(Int64Array::from(vec![80, 100, 30, 500, 50])),
            Arc::new(Int64Array::from(vec![14, 12, 6, 80, 10])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
