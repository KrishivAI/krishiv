#!/usr/bin/env python3
"""IoT Sensor metrics aggregation from local Parquet files using SQL in Python."""

import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks

def main():
    # 1. Create a temporary Parquet file with mock sensor logs
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "sensors.parquet")
        write_sensor_parquet(parquet_path)

        # 2. Build the embedded session
        session = ks.Session.embedded()

        # 3. Register the local Parquet file as a table
        session.register_parquet("sensor_logs", parquet_path)

        # 4. Run aggregate SQL query
        df = session.sql(
            "SELECT device_id, "
            "       AVG(temperature) as avg_temp, "
            "       MAX(humidity) as max_humidity, "
            "       COUNT(*) as reading_count "
            "FROM sensor_logs "
            "GROUP BY device_id "
            "ORDER BY device_id"
        )

        # 5. Collect and print formatted results
        result = df.collect()
        print(result.pretty())

def write_sensor_parquet(path):
    schema = pa.schema([
        ("device_id", pa.string()),
        ("temperature", pa.float64()),
        ("humidity", pa.float64()),
    ])
    table = pa.Table.from_pydict({
        "device_id": ["device-1", "device-2", "device-1", "device-2"],
        "temperature": [22.5, 18.0, 24.1, 19.5],
        "humidity": [55.0, 62.1, 54.2, 60.8],
    }, schema=schema)
    pq.write_table(table, path)

if __name__ == "__main__":
    main()
