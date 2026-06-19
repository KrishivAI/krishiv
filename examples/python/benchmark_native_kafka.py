import asyncio
import time
import json
import pyarrow as pa
import statistics
import sys
from krishiv import Session
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import random

BROKER = "127.0.0.1:9092"

scenarios = {
    "fraud": (
        pa.schema([("user_id", pa.utf8()), ("amount", pa.int64()), ("ts", pa.float64())]),
        "SELECT user_id, amount, ts FROM fraud WHERE amount > 5000"
    ),
    "iot": (
        pa.schema([("device_id", pa.utf8()), ("temp", pa.int64()), ("ts", pa.float64())]),
        "SELECT device_id, temp, ts FROM iot WHERE temp > 90"
    )
}

def create_topics():
    admin = AdminClient({'bootstrap.servers': BROKER})
    topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in scenarios.keys()]
    admin.create_topics(topics)

async def produce_events():
    producer = Producer({'bootstrap.servers': BROKER})
    for _ in range(50):
        now = time.time()
        for topic in scenarios.keys():
            if topic == "fraud":
                msg = {"user_id": f"u{random.randint(1,10)}", "amount": random.randint(1000, 10000), "ts": now}
            elif topic == "iot":
                msg = {"device_id": f"d{random.randint(1,10)}", "temp": random.randint(50, 120), "ts": now}
            producer.produce(topic, value=json.dumps(msg))
        producer.poll(0)
        await asyncio.sleep(0.01)
    producer.flush()

async def consume_and_query(session, name, schema, query):
    columns = []
    for field in schema:
        sql_type = "VARCHAR"
        if field.type == pa.int64():
            sql_type = "BIGINT"
        elif field.type == pa.float64():
            sql_type = "DOUBLE"
        columns.append(f"{field.name} {sql_type}")
        
    ddl = f"""
    CREATE EXTERNAL TABLE {name} ({', '.join(columns)})
    STORED AS KAFKA
    LOCATION '{name}'
    OPTIONS ('bootstrap.servers' '{BROKER}', 'group.id' 'krishiv-sql-{name}')
    """
    print(f"[{name}] Executing DDL: {ddl}")
    
    try:
        session.sql(ddl).to_arrow()
    except Exception as e:
        print(f"[{name}] Failed to register table: {e}")
        return

    df = session.sql(query)
    latencies = []
    
    try:
        async for b in df.execute_stream_async():
            res = pa.record_batch(b.to_arrow()).to_pydict()
            now = time.time()
            for ts in res['ts']:
                latencies.append((now - ts) * 1000) # ms
                
            if len(latencies) >= 10:
                break
                
        avg_latency = statistics.mean(latencies)
        p99_latency = statistics.quantiles(latencies, n=10)[8] if len(latencies) > 1 else latencies[0]
        print(f"[{name.upper()}] Avg Latency: {avg_latency:.2f} ms, P99: {p99_latency:.2f} ms")
    except Exception as e:
        print(f"[{name}] Stream execution error: {e}")

async def main():
    try:
        create_topics()
    except Exception as e:
        pass
        
    session = Session.from_env()
    prod_task = asyncio.create_task(produce_events())
    
    tasks = []
    for i, (name, (schema, query)) in enumerate(scenarios.items()):
        tasks.append(asyncio.create_task(consume_and_query(session, name, schema, query)))
        
    await asyncio.gather(*tasks)
    await prod_task

if __name__ == "__main__":
    asyncio.run(main())
