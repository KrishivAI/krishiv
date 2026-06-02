use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use krishiv_api::Session;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let coordinator_url = std::env::var("KRISHIV_COORDINATOR").unwrap_or_else(|_| "http://krishiv-coordinator.default.svc.cluster.local:50051".to_string());
    println!("Connecting to Krishiv coordinator at {}", coordinator_url);
    
    let session = krishiv_api::SessionBuilder::new().with_coordinator(coordinator_url).build().unwrap();
    
    println!("=====================================");
    println!("1. BATCH SQL EXAMPLE");
    println!("=====================================");
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("action", DataType::Utf8, false),
    ]));
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2])),
            Arc::new(StringArray::from(vec!["login", "login", "view", "view", "logout"])),
        ],
    ).unwrap();
    
    session.register_memory_stream("batch_events", vec![batch]).unwrap();
    
    let df_batch = session.sql("SELECT user_id, count(action) as action_count FROM batch_events GROUP BY user_id ORDER BY user_id");
    let result = df_batch.collect_async().await.unwrap();
    
    println!("Batch Query Result:");
    for batch in result {
        println!("{:?}", batch);
    }
    
    println!("=====================================");
    println!("2. CONTINUOUS STREAMING SQL EXAMPLE");
    println!("=====================================");
    
    let stream_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    
    session.register_unbounded("live_events", stream_schema.clone()).unwrap();
    
    let session_clone = session.clone();
    let stream_schema_clone = stream_schema.clone();
    
    tokio::spawn(async move {
        for i in 1..=5 {
            sleep(Duration::from_millis(500)).await;
            let stream_batch = RecordBatch::try_new(
                stream_schema_clone.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![i])),
                    Arc::new(Int64Array::from(vec![i * 10])),
                ],
            ).unwrap();
            
            session_clone.push_stream_job_input("live_events", vec![stream_batch]).unwrap();
            println!("Pushed event_id = {}", i);
        }
    });
    
    let df_stream = session.sql("SELECT event_id, value * 2 as doubled_value FROM live_events");
    let mut stream = df_stream.execute_stream_async().await.unwrap();
    
    let mut count = 0;
    while let Some(Ok(batch)) = stream.next().await {
        println!("Received streaming batch: {:?}", batch);
        count += 1;
        if count >= 5 {
            break;
        }
    }
    
    println!("Successfully completed both Batch and Streaming SQL on Krishiv K8s cluster!");
}
