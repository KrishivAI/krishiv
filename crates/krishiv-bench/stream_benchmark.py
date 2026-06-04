import time
import os
import pyarrow as pa
import pyarrow.parquet as pq
import krishiv as ks
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col
import numpy as np

NUM_ROWS = 10_000_000
DATA_FILE = "/home/code/krishiv/tpch_sf10/stream_data.parquet"

def generate_data():
    if os.path.exists(DATA_FILE):
        return
    print(f"Generating {NUM_ROWS} rows of streaming data...")
    timestamps = np.arange(0, NUM_ROWS * 10, 10, dtype=np.int64) # 10ms intervals
    device_ids = np.random.choice(["dev-1", "dev-2", "dev-3", "dev-4", "dev-5"], NUM_ROWS)
    values = np.random.rand(NUM_ROWS)

    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("device_id", pa.string()),
        ("value", pa.float64())
    ])

    table = pa.Table.from_arrays([timestamps, device_ids, values], schema=schema)
    pq.write_table(table, DATA_FILE, compression='NONE')
    print("Data generation complete.")

def run_krishiv():
    print("\n--- Running Krishiv Streaming (Tumbling Window) ---")
    session = ks.Session.from_env()
    
    table = pq.read_table(DATA_FILE)
    batches = table.to_batches(max_chunksize=100_000)
    ks_batches = [ks.Batch(b) for b in batches]
    
    stream = session.from_bounded_stream(
        "sensor_stream",
        ks_batches,
        watermark_column="timestamp",
        max_lateness_ms=1000,
    )
    
    windowed = (
        stream
        .key_by("device_id")
        .tumbling_window(1000) # 1 second window
    )
    
    start = time.time()
    results = windowed.collect()
    end = time.time()
    
    print(f"Krishiv Streaming Processed {NUM_ROWS} rows in: {end - start:.4f} seconds")
    print(f"Throughput: {NUM_ROWS / (end - start):,.0f} rows/sec")

def run_spark():
    print("\n--- Running PySpark Streaming (Batch Simulation) ---")
    spark = SparkSession.builder \
        .appName("Streaming-Benchmark") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    df = spark.read.parquet(DATA_FILE)
    df = df.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))
    
    start = time.time()
    result = df.groupBy(
        window(col("timestamp"), "1 seconds"),
        col("device_id")
    ).count().collect()
    end = time.time()
    
    print(f"PySpark Processed {NUM_ROWS} rows in: {end - start:.4f} seconds")
    print(f"Throughput: {NUM_ROWS / (end - start):,.0f} rows/sec")
    spark.stop()

if __name__ == "__main__":
    generate_data()
    run_krishiv()
    run_spark()
