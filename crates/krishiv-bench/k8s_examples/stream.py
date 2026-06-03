import time
import krishiv as ks
import pyarrow.parquet as pq

def main():
    print("\n--- Running Complex Streaming Query ---")
    session = ks.Session.from_env()
    
    table = pq.read_table("/home/code/krishiv/tpch_sf10/stream_data.parquet")
    table = table.slice(0, 1_000_000)
    batches = table.to_batches(max_chunksize=50_000)
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
        .tumbling_window(1000)
    )
    
    start = time.time()
    result = windowed.collect()
    end = time.time()
    
    print(f"Streaming Execution Time (1M rows): {end - start:.4f} seconds")

if __name__ == "__main__":
    main()
