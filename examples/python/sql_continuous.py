import asyncio
import pyarrow as pa
from krishiv import Session, Schema

async def main():
    # 1. Start session
    session = Session()

    # 2. Define schema
    schema = pa.schema([
        ("user_id", pa.int64()),
        ("amount", pa.int64()),
    ])

    # 3. Register unbounded streaming source
    print("Registering unbounded source 'events'...")
    session.register_unbounded("events", schema)

    # 4. Create generator for pushing continuous data
    async def event_generator():
        for i in range(1, 10):
            await asyncio.sleep(0.5)
            batch = pa.RecordBatch.from_arrays([
                pa.array([i % 3]),
                pa.array([100]),
            ], names=["user_id", "amount"])
            print(f"Python pushed batch {i}")
            yield batch
        
        # Keep generator open to allow pipeline to drain
        await asyncio.sleep(2)

    # 5. Connect Python generator to the session stream input
    print("Starting generator feed...")
    session.register_arrow_stream("events", event_generator())

    # 6. Run SQL aggregation query
    # The session will see 'events' is registered as unbounded, so it will execute continuously.
    print("Executing SQL query...")
    df = session.sql("SELECT user_id, amount * 2 AS double_amount FROM events")
    
    # 7. Drain output asynchronously
    print("Listening to continuous output stream...")
    count = 0
    try:
        async for batch in df.execute_stream_async():
            # In pyo3-arrow, to_arrow() usually gives an arro3 object, 
            # and to convert it to pyarrow dict we might need to wrap it.
            # PyBatch has to_pandas() though.
            print(f"Received result with {batch.num_rows} rows")
            count += 1
            if count >= 5:
                break
    except Exception as e:
        print(f"Stream complete or error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
