import os
import time
import krishiv as ks
import pyarrow.parquet as pq

def run_batch(session):
    print("\n--- Running Distributed Batch TPC-H Q1 ---")
    session.register_parquet("lineitem", "/home/code/krishiv/tpch_sf10/lineitem.parquet")
    
    q1 = """
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    """
    
    start = time.time()
    result = session.sql(q1).collect()
    end = time.time()
    print(result.pretty())
    print(f"Distributed Batch Execution Time: {end - start:.4f} seconds")

def run_streaming(session):
    print("\n--- Running Distributed Streaming Tumbling Window (1M Rows) ---")
    
    # Read 1M rows to avoid giant client-to-coordinator gRPC bottlenecks
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
    print(f"Distributed Streaming Execution Time (1M rows): {end - start:.4f} seconds")

if __name__ == "__main__":
    coordinator = os.environ.get("KRISHIV_COORDINATOR_URL", "http://127.0.0.1:30080")
    session = ks.Session.connect(coordinator)
    run_batch(session)
    run_streaming(session)
