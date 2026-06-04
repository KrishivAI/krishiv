use krishiv_api::{SessionBuilder, StreamBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Running Distributed Stream Window (Rust) ---");
    let coordinator = std::env::var("KRISHIV_COORDINATOR_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:30051".to_string());
    let data_path = std::env::var("KRISHIV_TPCH_DATA_DIR")
        .map(|dir| format!("{dir}/stream_data.parquet"))
        .unwrap_or_else(|_| "/home/code/krishiv/tpch_sf10/stream_data.parquet".to_string());
    let session = SessionBuilder::new()
        .with_coordinator(&coordinator)
        .with_remote_execution(true)
        .build()?;

    // Read local stream data and send it!
    let file = File::open(&data_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.with_batch_size(50_000).build()?;

    let mut batches = Vec::new();
    let mut rows_read = 0;
    let mut sequence = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        rows_read += batch.num_rows();
        batches.push(StreamBatch::new(sequence, batch));
        sequence += 1;
        if rows_read >= 1_000_000 {
            break;
        }
    }

    println!("Read {} batches ({} rows)", batches.len(), rows_read);

    let stream = session.memory_stream("sensor_stream", batches)?;
    let windowed = stream
        .key_by("device_id")
        .with_event_time("timestamp")
        .tumbling_window(1000);

    let start = Instant::now();
    let result = windowed.collect()?;
    let duration = start.elapsed();

    println!(
        "Streaming Execution Time (1M rows): {:.4} seconds",
        duration.as_secs_f64()
    );
    println!("Output batches: {}", result.len());

    Ok(())
}
