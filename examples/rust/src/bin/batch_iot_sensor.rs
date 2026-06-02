//! IoT Sensor metrics aggregation from local Parquet files using SQL.
//! Run with: `cargo run -p krishiv-rust-examples --bin batch_iot_sensor`

#![forbid(unsafe_code)]

use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    // 1. Create a temporary Parquet file with mock sensor logs
    let temp = tempdir()?;
    let parquet_path = temp.path().join("sensors.parquet");
    write_sensor_parquet(&parquet_path)?;

    // 2. Build the session
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;

    // 3. Register the local Parquet file as a table
    session.register_parquet("sensor_logs", &parquet_path)?;

    // 4. Run aggregate SQL query
    let df = session.sql(
        "SELECT device_id, \
                AVG(temperature) as avg_temp, \
                MAX(humidity) as max_humidity, \
                COUNT(*) as reading_count \
         FROM sensor_logs \
         GROUP BY device_id \
         ORDER BY device_id",
    )?;

    // 5. Collect and print formatted results
    let result = df.collect()?;
    println!("{}", result.pretty()?);

    Ok(())
}

fn write_sensor_parquet(path: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("humidity", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "device-1", "device-2", "device-1", "device-2",
            ])),
            Arc::new(Float64Array::from(vec![22.5, 18.0, 24.1, 19.5])),
            Arc::new(Float64Array::from(vec![55.0, 62.1, 54.2, 60.8])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
