#!/usr/bin/env python3
"""Real-time user session grouping using Session Windows in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build session
    session = ks.Session.from_env()

    # Alice has a 12-second gap between the third and fourth interactions (triggers session split)
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("user_id", pa.string()),
    ])
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 5000, 8000, 20000],
        "user_id": ["Alice", "Alice", "Alice", "Alice"],
    }, schema=schema)

    # 2. Register memory stream with 2s lateness watermark
    stream = session.memory_stream(
        "clicks",
        [ks.Batch(batch)],
        watermark_column="timestamp",
        max_lateness_ms=2000
    )

    # 3. Apply session window (10-second inactivity gap)
    session_windowed = (
        stream
        .key_by("user_id")
        .session_window(10000)
    )

    # 4. Collect and print results
    results = session_windowed.collect()
    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    print(table)

if __name__ == "__main__":
    main()
