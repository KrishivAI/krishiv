#!/usr/bin/env python3
"""Continuous unbounded streaming job submission, live data pushing, and window polling in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build session
    session = ks.Session.from_env()

    # 2. Build stream and window pipeline representing the continuous job query
    # We use a placeholder query selects from the registered stream name "alerts_stream"
    stream = session.stream(
        "SELECT timestamp, user_id FROM alerts_stream",
        watermark_column="timestamp",
        max_lateness_ms=1000
    )
    windowed = (
        stream
        .key_by("user_id")
        .tumbling_window(10) # 10 seconds tumbling window
    )

    # 3. Submit the continuous job
    job_id = session.submit_stream_job("alerts_stream", windowed)
    print(f"Submitted continuous stream job ID: {job_id}")

    # 4. Prepare and dynamically push a real-time record batch
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("user_id", pa.string()),
    ])
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 2000],
        "user_id": ["Alice", "Bob"],
    }, schema=schema)

    session.push_stream_job_input(job_id, [ks.Batch(batch)])

    # 5. Poll for active window outputs emitted by the running job
    results = session.poll_stream_job(job_id)
    print(f"Polled {len(results)} batches from continuous stream job")

    if results:
        pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
        table = pa.Table.from_batches(pa_batches)
        print(table)

if __name__ == "__main__":
    main()
