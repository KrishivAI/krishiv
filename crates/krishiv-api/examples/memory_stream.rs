#![forbid(unsafe_code)]

use std::error::Error;
use std::sync::Arc;

use arrow::array::Int64Array;
use krishiv_api::{DataType, Field, QueryResult, RecordBatch, Schema, Session, StreamBatch};

fn main() -> Result<(), Box<dyn Error>> {
    let session = Session::builder().build()?;
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
