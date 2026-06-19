import krishiv as ks
import time
import os
import pyarrow as pa

def run_complex_batch():
    print("--- Starting Complex Batch Job (with Shuffle) ---")
    start = time.time()

    # In k8s-operator mode, from_env() automatically connects to the coordinator.
    session = ks.Session.from_env()

    # Distributed GROUP BY forces a shuffle across executor nodes.
    df = session.sql("""
        SELECT
            value % 100 AS category,
            COUNT(value)  AS total_count,
            SUM(value)    AS total_sum
        FROM generate_series(1, 1000000)
        GROUP BY value % 100
        ORDER BY total_sum DESC
        LIMIT 10
    """)

    result = df.collect()
    end = time.time()

    print(result)
    print(f"Batch Execution Time: {end - start:.4f} seconds")


def run_complex_streaming():
    print("\n--- Starting Complex Streaming Job ---")
    session = ks.Session.from_env()

    # Build a bounded synthetic batch that represents one micro-batch of events.
    schema = pa.schema([
        pa.field("user_id", pa.utf8()),
        pa.field("ts",      pa.int64()),
        pa.field("value",   pa.int64()),
    ])
    batch = pa.record_batch(
        {
            "user_id": [f"user_{i % 20}" for i in range(1000)],
            "ts":      [i * 100 for i in range(1000)],   # 0 … 99 900 ms
            "value":   [i % 7 for i in range(1000)],
        },
        schema=schema,
    )

    # Register the name so is_streaming_query() classifies it correctly, then
    # submit as a bounded-window pipeline using the actual streaming API.
    stream = session.stream(pa.RecordBatchReader.from_batches(schema, [batch]))
    pipeline = (
        stream
        .key_by("user_id")
        .tumbling_window(window_ms=5_000, event_time_column="ts")
        .aggregate("value", "sum", output_column="total_value")
    )

    job_id = session.submit_stream_job(pipeline)
    print(f"Streaming job submitted: {job_id}")

    # Poll once for demonstration; in production loop until done.
    time.sleep(0.5)
    results = session.poll_stream_job(job_id)
    print(f"Streaming results ({len(results)} batches): {results}")


if __name__ == "__main__":
    run_complex_batch()
    # run_complex_streaming()  # Uncomment to run the streaming demo
