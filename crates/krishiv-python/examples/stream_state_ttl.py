#!/usr/bin/env python3
"""Stateful streaming with event-time state TTL eviction in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build session
    session = ks.Session.from_env()

    # 2. Two Alice events 14 seconds apart — TTL of 5s causes first window's
    #    state to be evicted before the second event arrives.
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("user_id", pa.string()),
    ])
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 15000],
        "user_id": ["Alice", "Alice"],
    }, schema=schema)

    # 3. Register stream and set state TTL of 5s on the stream
    stream = (
        session.memory_stream(
            "user_txs",
            [ks.Batch(batch)],
            watermark_column="timestamp",
            max_lateness_ms=1000,
        )
        .with_state_ttl(5000)  # 5-second state TTL
    )

    # 4. 2-second tumbling window
    windowed = (
        stream
        .key_by("user_id")
        .tumbling_window(2000)  # 2 seconds
    )

    # 5. Collect and print
    results = windowed.collect()
    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    print(table)

if __name__ == "__main__":
    main()
