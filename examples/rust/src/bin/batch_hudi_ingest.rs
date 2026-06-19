//! Local Apache Hudi Copy-On-Write ingestion and SQL read batch example.
//! Run with: `cargo run -p krishiv-rust-examples --bin batch_hudi_ingest`

#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use krishiv::{DataType, ExecutionMode, Field, RecordBatch, Schema, Session};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let hudi_path = temp.path().to_path_buf();

    // 1. Prepare some mock users to ingest
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // 2. Initialize the Hudi writer and write to the local directory
    {
        let writer = krishiv_connectors::lakehouse::HudiCowWriter::open(&hudi_path);
        writer.append(batch)?;
    }

    // 3. Build the session
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;

    // 4. Read the Hudi table locally
    let df = session
        .read_hudi_async(
            hudi_path.to_string_lossy(),
            krishiv_connectors::lakehouse::HudiQueryType::Snapshot,
            None,
        )
        .await?;

    // 5. Collect and print the results
    let result = df.collect_async().await?;
    println!("{}", result.pretty()?);

    Ok(())
}
