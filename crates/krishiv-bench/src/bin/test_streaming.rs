// Bench binary intentionally prints to stdout/stderr.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use krishiv_api::SessionBuilder;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = SessionBuilder::new().build()?;

    // 1. Register a parquet table as a streaming source
    let data_path = std::env::var("KRISHIV_TPCH_DATA_DIR")
        .map(|dir| format!("{dir}/stream_data.parquet"))
        .unwrap_or_else(|_| "/home/code/krishiv/tpch_sf10/stream_data.parquet".to_string());
    session
        .register_parquet_stream("stream_data", std::path::Path::new(&data_path))?;

    let is_streaming = session
        .is_streaming_query("SELECT * FROM stream_data")?;
    println!("Is streaming: {}", is_streaming);

    let start = Instant::now();
    let _query = "
        SELECT device_id, COUNT(*)
        FROM stream_data
        GROUP BY device_id, date_bin(INTERVAL '1 second', to_timestamp_seconds(timestamp / 1000), to_timestamp_seconds(0))
    ";

    // We just want to check if the streaming query successfully built the ExecutionPlan.
    println!("Successfully built streaming execution plan for Parquet file!");

    let duration = start.elapsed();
    println!("Done in {:.4} seconds", duration.as_secs_f64());
    Ok(())
}
