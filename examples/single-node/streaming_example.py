#!/usr/bin/env python3
"""Streaming Example: Real-time IoT Sensor anomaly detection."""

import os
import pyarrow as pa
import pyarrow.compute as pc
import krishiv as ks

def main():
    if "KRISHIV_COORDINATOR_URL" not in os.environ:
        print("Warning: KRISHIV_COORDINATOR_URL is not set. Running in embedded mode.")
        
    session = ks.Session.from_env()

    schema = pa.schema([
        ("sensor_id", pa.string()), 
        ("temperature", pa.float64())
    ])
    
    batch1 = pa.RecordBatch.from_pydict({"sensor_id": ["S1", "S2"], "temperature": [22.5, 89.1]}, schema=schema)
    batch2 = pa.RecordBatch.from_pydict({"sensor_id": ["S1", "S3"], "temperature": [23.1, 105.0]}, schema=schema)
    batch3 = pa.RecordBatch.from_pydict({"sensor_id": ["S2", "S3"], "temperature": [91.0, 102.5]}, schema=schema)

    print("Processing live sensor feed...")
    
    results = session.memory_stream_collect(
        "sensor_feed",
        [ks.Batch(batch1), ks.Batch(batch2), ks.Batch(batch3)],
    )

    pa_batches = [pa.record_batch(b.to_arrow()) for b in results]
    table = pa.Table.from_batches(pa_batches)
    
    anomalies = table.filter(pc.greater(table["temperature"], 90.0))
    
    print("\nIdentified Critical Anomalies (Temperature > 90.0):")
    print(anomalies)

if __name__ == "__main__":
    main()
