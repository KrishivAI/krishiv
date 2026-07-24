#!/usr/bin/env python3
"""Real-time sliding window with multi-source watermark synchronization in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build session
    session = ks.Session.from_env()

    # 2. Prepare multi-device sensor events
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("device_id", pa.string()),
    ])
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 2000, 3000, 8000],
        "device_id": ["device-1", "device-2", "device-1", "device-2"],
    }, schema=schema)

    # 3. Register as a bounded streaming relation (returns PyRelation with full pipeline API)
    relation = session.from_bounded_stream(
        "sensor_stream",
        [ks.Batch(batch)],
        watermark_column="timestamp",
        max_lateness_ms=1000,
    )

    # 4. Apply per-source watermarks and sliding window
    windowed = (
        relation
        .with_source_id_column("device_id")   # column that identifies each source
        .with_source_watermark("device-1", 1000)
        .with_source_watermark("device-2", 2000)
        .key_by("device_id")
        .sliding_window(10000, 5000)  # 10s window, 5s slide
    )

    # 5. Collect and print
    results = windowed.collect()
    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    print(table)

if __name__ == "__main__":
    main()
