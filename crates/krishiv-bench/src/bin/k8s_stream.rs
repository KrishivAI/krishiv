use krishiv_api::SessionBuilder;
use krishiv_api::Batch;
use std::time::Instant;
use arrow::record_batch::RecordBatch;
use std::fs::File;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Running Distributed Stream Window (Rust) ---");
    let session = SessionBuilder::new()
        .with_coordinator("http://127.0.0.1:30051") // Use Flight server address
        .with_remote_execution(true)
        .build()?;
    
    // Read local stream data and send it!
    let file = File::open("/home/code/krishiv/tpch_sf10/stream_data.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.with_batch_size(50_000).build()?;
    
    let mut batches = Vec::new();
    let mut rows_read = 0;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        rows_read += batch.num_rows();
        batches.push(Batch::new(batch));
        if rows_read >= 1_000_000 {
            break;
        }
    }
    
    println!("Read {} batches ({} rows)", batches.len(), rows_read);
    
    let stream = session.from_bounded_stream(
        "sensor_stream",
        batches,
        "timestamp".to_string(),
        1000,
    )?;
    
    let windowed = stream
        .key_by(vec!["device_id".to_string()])?
        .tumbling_window(1000)?;
        
    let start = Instant::now();
    let result = windowed.collect().await?;
    let duration = start.elapsed();
    
    println!("Streaming Execution Time (1M rows): {:.4} seconds", duration.as_secs_f64());
    
    Ok(())
}
