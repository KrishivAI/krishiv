import asyncio
from krishiv import Session, Schema
import pyarrow as pa

async def main():
    session = Session()
    schema = pa.schema([("user_id", pa.int64()), ("amount", pa.int64())])
    session.register_unbounded("events", schema)
    
    async def event_generator():
        for i in range(1, 4):
            await asyncio.sleep(0.5)
            batch = pa.RecordBatch.from_arrays([pa.array([i % 3]), pa.array([100])], names=["user_id", "amount"])
            yield batch
            
    session.register_arrow_stream("events", event_generator())
    
    df = session.sql("SELECT user_id, amount * 2 AS double_amount FROM events")
    
    try:
        async for batch in df.execute_stream_async():
            print(f"Received: {batch.to_pydict()}")
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(main())
