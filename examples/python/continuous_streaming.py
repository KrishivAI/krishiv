import asyncio
import json
import time
import pyarrow as pa
from krishiv import Session, agg
from confluent_kafka import Consumer

# Initialize Krishiv session
session = Session()

# Define scenario schemas to avoid PyArrow type inference issues
fraud_schema = pa.schema([
    ("ts", pa.int64()), ("user_id", pa.string()), ("amount", pa.int64())
])

def create_job(job_name, key_col, agg_col, agg_fn):
    stream = session.memory_stream("memory:" + job_name, [], "ts", 0)
    windowed = stream.key_by(key_col).tumbling_window(1).agg(**{f"total_{agg_col}": agg_fn(agg_col)})
    session.submit_stream_job(job_name, windowed)
    print(f"Created continuous job: {job_name}")

create_job("fraud_job", "user_id", "amount", agg.sum)

async def pump_kafka():
    c = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'krishiv-continuous-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['scenarios'])
    
    print("Starting continuous single-digit latency pipeline...")
    while True:
        # Yield to event loop
        await asyncio.sleep(0.01)
        msgs = c.consume(num_messages=500, timeout=0.1)
        if not msgs:
            continue
            
        fraud_records = []
        for m in msgs:
            if m.error() is None:
                record = json.loads(m.value().decode('utf-8'))
                if record['scenario'] == 'fraud':
                    fraud_records.append(record)
                    
        if fraud_records:
            # Micro-second latency push to Rust StreamPipeline
            import pandas as pd
            table = pa.Table.from_pandas(pd.DataFrame(fraud_records), schema=fraud_schema)
            # Push directly into continuous execution graph
            from krishiv import Batch
            batches = [Batch(b) for b in table.to_batches()]
            session.push_stream_job_input("fraud_job", batches)
            
async def poll_results():
    while True:
        await asyncio.sleep(1)
        batches = session.poll_stream_job("fraud_job")
        if batches:
            print("--- CONTINUOUS FRAUD ALERTS ---")
            for b in batches:
                print(b.to_pandas())

async def main():
    await asyncio.gather(pump_kafka(), poll_results())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
