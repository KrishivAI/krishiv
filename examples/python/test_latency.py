import asyncio
import time
from krishiv import Session, Schema
import pyarrow as pa

async def main():
    session = Session()
    schema = pa.schema([("user_id", pa.int64()), ("timestamp", pa.float64())])
    session.register_unbounded("events", schema)
    
    async def event_generator():
        for i in range(1, 101):
            await asyncio.sleep(0.01)
            batch = pa.RecordBatch.from_arrays([pa.array([i]), pa.array([time.time()])], names=["user_id", "timestamp"])
            yield batch
            
    session.register_arrow_stream("events", event_generator())
    df = session.sql("SELECT user_id, timestamp FROM events")
    
    latencies = []
    try:
        async for batch in df.execute_stream_async():
            receive_time = time.time()
            ts = batch.to_pandas()["timestamp"][0]
            latencies.append((receive_time - ts) * 1000)
            if len(latencies) >= 100:
                break
    except Exception as e:
        print(f"Error: {e}")

    if latencies:
        print(f"Average latency: {sum(latencies)/len(latencies):.2f} ms")
        print(f"Min latency: {min(latencies):.2f} ms")
        print(f"Max latency: {max(latencies):.2f} ms")

asyncio.run(main())
