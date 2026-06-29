import asyncio
import pyarrow as pa
from krishiv import Session, agg, Batch
import time

session = Session()

schema = pa.schema([
    ("ts", pa.int64()), ("user_id", pa.string()), ("amount", pa.int64())
])

stream = session.memory_stream("memory:test_job", [], "ts", 0)
windowed = stream.key_by("user_id").tumbling_window(1).agg(total=agg.sum("amount"))
session.submit_stream_job("test_job", windowed)

async def pump():
    print("Pumping data...")
    for t in [1000, 2000, 3000, 4000]: # Advancing by 1 second each
        table = pa.Table.from_arrays([
            pa.array([t]), pa.array(["U1"]), pa.array([100])
        ], schema=schema)
        session.push_stream_job_input("test_job", [Batch(b) for b in table.to_batches()])
        await asyncio.sleep(0.5)

async def poll():
    for _ in range(10):
        await asyncio.sleep(0.5)
        batches = session.poll_stream_job("test_job")
        if batches:
            print("Received:", batches[0].to_arrow().to_pandas())

async def main():
    await asyncio.gather(pump(), poll())

asyncio.run(main())
