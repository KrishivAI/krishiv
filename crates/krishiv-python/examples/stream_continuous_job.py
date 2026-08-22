#!/usr/bin/env python3
"""Continuous unbounded streaming job submission, live data pushing, and window polling in Python."""

import pyarrow as pa
import krishiv as ks

def main():
    # 1. Build session
    session = ks.Session.from_env()

    # 2. Register the source stream table up front so the job query can be
    # planned; live rows are fed later via push_stream_job_input. (The unified
    # StreamingDataFrame plans its SQL eagerly, so the source must exist first.)
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("user_id", pa.string()),
    ])
    # A one-row placeholder just gives the source its schema so the query plans;
    # submit_stream_job reads only the window spec, not these rows.
    seed = pa.RecordBatch.from_pydict({"timestamp": [0], "user_id": ["_seed"]}, schema=schema)
    session.register_record_batches("alerts_stream", [ks.Batch(seed)])

    # 3. Build the windowed pipeline over the source stream.
    windowed = (
        session.stream(
            "SELECT timestamp, user_id FROM alerts_stream",
            watermark_column="timestamp",
            max_lateness_ms=1000,
        )
        .key_by("user_id")
        .tumbling_window(10000)  # 10 seconds tumbling window
    )

    # 4. Start the continuous job through the ONE write terminal; the
    # returned StreamingJob handle owns push/drain/flush/stop identically on
    # embedded, single-node, and distributed sessions.
    job = windowed.write().trigger("continuous", 1000).start(session, "alerts_stream")
    print(f"Started continuous stream job: {job.id}")

    # 5. Prepare and dynamically push a real-time record batch
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 2000],
        "user_id": ["Alice", "Bob"],
    }, schema=schema)

    job.push([ks.Batch(batch)])

    # 5. Poll for active window outputs emitted by the running job
    results = job.drain()
    print(f"Polled {len(results)} batches from continuous stream job")

    if results:
        pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
        table = pa.Table.from_batches(pa_batches)
        print(table)

if __name__ == "__main__":
    main()
