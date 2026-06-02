#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::Int64Array;
use krishiv::{
    DataType, ExecutionMode, Field, QueryResult, RecordBatch, Schema, Session, StreamBatch,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = Session::builder();
    if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
        builder = builder.with_local_cluster(url);
    } else {
        builder = builder.with_execution_mode(ExecutionMode::Embedded);
    }
    let session = builder.build()?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])?;

    let stream = session
        .memory_stream("numbers", vec![StreamBatch::new(0, batch)])
        .unwrap();
    let filtered = stream.filter_batches(|batch| batch.sequence() == 0)?;
    let batches = filtered
        .collect_bounded()?
        .into_iter()
        .map(|batch| batch.batch().clone())
        .collect();

    println!("{}", QueryResult::new(batches).pretty()?);
    Ok(())
}
