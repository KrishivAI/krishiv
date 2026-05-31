#!/usr/bin/env python3
"""Real-time transaction count using a Tumbling Event-Time Window in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build an embedded in-process session
    session = ks.Session.embedded()

    # 2. Prepare streaming mock transaction batches (timestamp, user_id)
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("user_id", pa.string()),
    ])
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 2000, 61000, 62000],
        "user_id": ["Alice", "Bob", "Alice", "Alice"],
    }, schema=schema)

    # 3. Register as a bounded memory stream (watermark_column="timestamp", max_lateness_ms=5000)
    stream = session.memory_stream(
        "transactions",
        [ks.Batch(batch)],
        watermark_column="timestamp",
        max_lateness_ms=5000
    )

    # 4. Declare event-time windowing via the fluent Python API
    windowed = (
        stream
        .key_by("user_id")
        .tumbling_window(60) # 60 seconds tumbling window size (1 minute)
    )

    # 5. Execute in-process and collect output stream batches
    results = windowed.collect()
    
    # 6. Convert PyBatches to pyarrow Table and print formatted output
    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    print(table)

if __name__ == "__main__":
    main()
