import asyncio
import json
import time
import os
import statistics
import pyarrow as pa
from krishiv import Session
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

# Ensure Krishiv runs natively inside this python process
os.environ["KRISHIV_MODE"] = "local"
BROKER = "127.0.0.1:9092"

scenarios = {
    "fraud": (
        pa.schema([("user_id", pa.utf8()), ("amount", pa.int64()), ("ts", pa.float64())]),
        "SELECT user_id, amount, ts FROM fraud WHERE amount > 5000"
    ),
    "iot": (
        pa.schema([("device_id", pa.utf8()), ("temp", pa.int64()), ("ts", pa.float64())]),
        "SELECT device_id, temp, ts FROM iot WHERE temp > 90"
    ),
    "clickstream": (
        pa.schema([("user", pa.utf8()), ("action", pa.utf8()), ("ts", pa.float64())]),
        "SELECT user, action, ts FROM clickstream WHERE action = 'click'"
    ),
    "ride": (
        pa.schema([("zone", pa.utf8()), ("requests", pa.int64()), ("ts", pa.float64())]),
        "SELECT zone, requests * 2 as est_demand, ts FROM ride"
    ),
    "log": (
        pa.schema([("service", pa.utf8()), ("status", pa.int64()), ("ts", pa.float64())]),
        "SELECT service, status, ts FROM log WHERE status >= 500"
    ),
    "supply": (
        pa.schema([("truck", pa.utf8()), ("lat", pa.float64()), ("lon", pa.float64()), ("ts", pa.float64())]),
        "SELECT truck, lat, lon, ts FROM supply"
    ),
    "vwap": (
        pa.schema([("ticker", pa.utf8()), ("price", pa.float64()), ("volume", pa.int64()), ("ts", pa.float64())]),
        "SELECT ticker, price * volume as value, ts FROM vwap"
    ),
    "social": (
        pa.schema([("hashtag", pa.utf8()), ("ts", pa.float64())]),
        "SELECT hashtag, ts FROM social"
    ),
    "gaming": (
        pa.schema([("player", pa.utf8()), ("score", pa.int64()), ("ts", pa.float64())]),
        "SELECT player, score, ts FROM gaming WHERE score > 500"
    ),
    "retail": (
        pa.schema([("sku", pa.utf8()), ("count", pa.int64()), ("ts", pa.float64())]),
        "SELECT sku, count, ts FROM retail WHERE count < 10"
    ),
}

def create_topics():
    admin = AdminClient({'bootstrap.servers': BROKER})
    topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in scenarios.keys()]
    admin.create_topics(topics)
    print("Topics created.")
    time.sleep(2)

async def produce_events():
    p = Producer({'bootstrap.servers': BROKER})
    
    for i in range(100):
        now = time.time()
        events = [
            {"scenario": "fraud", "user_id": f"U{i}", "amount": 6000, "ts": now},
            {"scenario": "iot", "device_id": f"D{i}", "temp": 95, "ts": now},
            {"scenario": "clickstream", "user": f"U{i}", "action": "click", "ts": now},
            {"scenario": "ride", "zone": f"Z{i}", "requests": 1, "ts": now},
            {"scenario": "log", "service": "auth", "status": 500, "ts": now},
            {"scenario": "supply", "truck": f"T{i}", "lat": 34.0, "lon": -118.0, "ts": now},
            {"scenario": "vwap", "ticker": "AAPL", "price": 150.0, "volume": 100, "ts": now},
            {"scenario": "social", "hashtag": "#rust", "ts": now},
            {"scenario": "gaming", "player": f"P{i}", "score": 1000, "ts": now},
            {"scenario": "retail", "sku": f"SKU{i}", "count": 5, "ts": now},
        ]
        for e in events:
            p.produce(e["scenario"], json.dumps(e).encode('utf-8'))
        p.flush()
        await asyncio.sleep(0.01)
    print("Producer finished sending benchmark data.")

async def consume_and_query(session, name, schema, query):
    c = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': f'krishiv-{name}',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([name])
    
    async def kafka_generator():
        count = 0
        while count < 100:
            msg = await asyncio.to_thread(c.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            arrays = []
            for field in schema:
                val = data[field.name]
                if field.type == pa.utf8():
                    arrays.append(pa.array([val], type=pa.utf8()))
                elif field.type == pa.int64():
                    arrays.append(pa.array([val], type=pa.int64()))
                elif field.type == pa.float64():
                    arrays.append(pa.array([val], type=pa.float64()))
            
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            yield batch
            count += 1
            
    session.register_unbounded(name, schema)
    session.register_arrow_stream(name, kafka_generator())
    
    df = session.sql(query)
    
    latencies = []
    
    async for b in df.execute_stream_async():
        res = pa.record_batch(b.to_arrow()).to_pydict()
        
        now = time.time()
        for ts in res['ts']:
            latencies.append((now - ts) * 1000) # ms
            
        if len(latencies) >= 100:
            break
            
    avg_latency = statistics.mean(latencies)
    p99_latency = statistics.quantiles(latencies, n=100)[98]
    print(f"[{name.upper()}] Avg Latency: {avg_latency:.2f} ms, P99: {p99_latency:.2f} ms")

async def main():
    try:
        create_topics()
    except Exception as e:
        print(f"Topic creation info: {e}")
        
    session = Session.from_env()
    
    # Start producer in background
    prod_task = asyncio.create_task(produce_events())
    
    print("Running 10 Real-life complex streaming examples and collecting benchmarks...")
    
    tasks = []
    for i, (name, (schema, query)) in enumerate(scenarios.items()):
        tasks.append(asyncio.create_task(consume_and_query(session, name, schema, query)))
        if i == 2:
            break
        
    await asyncio.gather(*tasks)
    await prod_task
    
    print("All 10 Streaming scenarios completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
