#!/usr/bin/env python3
"""In-memory bounded stream collect and sequence-filter example in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    session = ks.Session.from_env()

    schema = pa.schema([("value", pa.int64())])
    batch = pa.RecordBatch.from_pydict({"value": [1, 2, 3]}, schema=schema)

    # Collect the stream directly (no windowing — raw batch collection)
    results = session.memory_stream_collect(
        "numbers",
        [ks.Batch(batch)],
    )

    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    print(table)

if __name__ == "__main__":
    main()
