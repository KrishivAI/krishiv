"""Example 5: IoT Sensor Data — Delta table with SQL aggregation.

Simulates temperature sensors writing readings in batches. Uses SQL
to compute averages, min/max, and hourly aggregations from Delta data.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "crates", "krishiv-python", "python"))

import tempfile, shutil
import krishiv as ks
import pyarrow as pa

def main():
    tmpdir = tempfile.mkdtemp(prefix="delta_iot_")
    delta_path = os.path.join(tmpdir, "sensor_readings")
    try:
        session = ks.Session()

        schema = pa.schema([
            pa.field("sensor_id", pa.string()),
            pa.field("temperature", pa.float64()),
            pa.field("humidity", pa.float64()),
            pa.field("hour", pa.int64()),
        ])

        # Batch 1: Hour 8 readings
        h8 = pa.record_batch(
            [["sensor_A", "sensor_B", "sensor_C", "sensor_A"],
             [22.5, 21.0, 23.1, 22.8],
             [45.0, 50.2, 42.1, 44.8],
             [8, 8, 8, 8]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(h8)], mode="overwrite")
        print("Hour 8: 4 readings written")

        # Batch 2: Hour 9 readings
        h9 = pa.record_batch(
            [["sensor_A", "sensor_B", "sensor_C", "sensor_A", "sensor_B"],
             [23.2, 21.5, 24.0, 23.5, 21.8],
             [44.5, 49.8, 41.5, 44.2, 49.5],
             [9, 9, 9, 9, 9]],
            schema=schema,
        )
        ks.write_delta(delta_path, [ks.Batch(h9)], mode="append")
        print("Hour 9: 5 readings appended")

        # Register Delta table for SQL queries
        arrow_table = ks.read_delta(session, delta_path).collect().to_arrow()
        session.register_record_batches("sensor_data", [
            ks.Batch(b) for b in arrow_table.to_batches()
        ])

        # Average temperature per sensor
        df_avg = session.sql("SELECT sensor_id, AVG(temperature) as avg_temp FROM sensor_data GROUP BY sensor_id")
        print("\n--- Average temperature per sensor ---")
        print(df_avg.collect_pretty())

        # Hourly temperature stats
        df_hourly = session.sql("SELECT hour, MIN(temperature) as min_temp, MAX(temperature) as max_temp, AVG(humidity) as avg_humidity FROM sensor_data GROUP BY hour ORDER BY hour")
        print("\n--- Hourly temperature stats ---")
        print(df_hourly.collect_pretty())

        # Total readings count
        df_count = session.sql("SELECT COUNT(*) as total_readings, COUNT(DISTINCT sensor_id) as unique_sensors FROM sensor_data")
        print("\n--- Summary ---")
        print(df_count.collect_pretty())

        print("\nIoT sensor aggregation example completed successfully!")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()
