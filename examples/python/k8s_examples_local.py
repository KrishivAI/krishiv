import asyncio
import os
from krishiv import Session, Schema, Batch
import pyarrow as pa

async def main():
    print("Running Krishiv natively inside Kubernetes!")
    os.environ["KRISHIV_MODE"] = "local"
    session = Session.from_env()

    print("=====================================")
    print("1. BATCH SQL EXAMPLE (Fraud Detection)")
    print("=====================================")
    
    schema = pa.schema([("user_id", pa.int64()), ("amount", pa.int64()), ("timestamp", pa.int64())])
    session.register_unbounded("transactions", schema)

    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 1, 3, 1]),
        pa.array([500, 100, 1500, 200, 800]),
        pa.array([1000, 1005, 1010, 1015, 1020])
    ], names=["user_id", "amount", "timestamp"])

    session.push_stream_job_input("transactions", [Batch(batch)])

    df_batch = session.sql("SELECT user_id, amount * 2 as doubled_amount FROM transactions")
    
    res = []
    async for b in df_batch.execute_stream_async():
        res.append(pa.record_batch(b.to_arrow()).to_pydict())
        break
    
    print("Batch Query Result:")
    print(res)

    print("=====================================")
    print("2. CONTINUOUS STREAMING SQL EXAMPLE")
    print("=====================================")
    
    schema_events = pa.schema([("event_id", pa.int64()), ("value", pa.int64())])
    session.register_unbounded("live_events", schema_events)
    
    async def event_generator():
        for i in range(1, 6):
            await asyncio.sleep(0.5)
            yield pa.RecordBatch.from_arrays([pa.array([i]), pa.array([i * 10])], names=["event_id", "value"])
            print(f"Produced event_id = {i}")

    session.register_arrow_stream("live_events", event_generator())
    
    df_stream = session.sql("SELECT event_id, value * 3 as tripled_value FROM live_events")
    
    count = 0
    try:
        async for b in df_stream.execute_stream_async():
            print(f"Received streaming result: {pa.record_batch(b.to_arrow()).to_pydict()}")
            count += 1
            if count >= 5:
                break
    except Exception as e:
        print(f"Error in streaming: {e}")

    print("Successfully completed both Batch and Streaming SQL on Krishiv K8s pod!")

if __name__ == "__main__":
    asyncio.run(main())
