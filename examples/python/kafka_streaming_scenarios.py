import json
import pyarrow as pa
import pyarrow.parquet as pq
from krishiv import Session
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'krishiv-python-group',
    'auto.offset.reset': 'earliest'
})
c.subscribe(['scenarios'])

session = Session()

scenarios_sql = {
    "Fraud": "SELECT user_id, SUM(amount) as total FROM events WHERE scenario='fraud' GROUP BY user_id",
    "IoT": "SELECT device_id, AVG(temp) as avg_temp FROM events WHERE scenario='iot' GROUP BY device_id",
    "Clickstream": "SELECT action, COUNT(*) as clicks FROM events WHERE scenario='clickstream' GROUP BY action",
    "Ride": "SELECT zone, SUM(requests) as total_requests FROM events WHERE scenario='ride' GROUP BY zone",
    "Log": "SELECT service, COUNT(*) as err_count FROM events WHERE scenario='log' AND status >= 500 GROUP BY service",
    "Supply": "SELECT truck, MAX(lat) as lat, MAX(lon) as lon FROM events WHERE scenario='supply' GROUP BY truck",
    "VWAP": "SELECT ticker, SUM(price * volume) / SUM(volume) as vwap FROM events WHERE scenario='vwap' GROUP BY ticker",
    "Social": "SELECT hashtag, COUNT(*) as mentions FROM events WHERE scenario='social' GROUP BY hashtag ORDER BY mentions DESC LIMIT 5",
    "Gaming": "SELECT player, SUM(score) as total_score FROM events WHERE scenario='gaming' GROUP BY player ORDER BY total_score DESC LIMIT 10",
    "Retail": "SELECT sku, SUM(count) as sold FROM events WHERE scenario='retail' GROUP BY sku"
}

def process_batch():
    msgs = c.consume(num_messages=100, timeout=1.0)
    if not msgs:
        return
    
    records = []
    for m in msgs:
        if m.error() is None:
            records.append(json.loads(m.value().decode('utf-8')))
    
    if not records:
        return

    # Normalize JSON to Arrow Table
    import pandas as pd
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    
    # Check if register_arrow exists, else just save to parquet and query
    try:
        session.register_arrow("events", table)
    except AttributeError:
        # Fallback for Krishiv python API
        pq.write_table(table, '/tmp/events.parquet')
        session.register_parquet("events", "/tmp/events.parquet")
    
    for name, sql in scenarios_sql.items():
        try:
            res = session.sql(sql)
            print(f"--- {name} Scenario ---")
            res.show()
        except Exception as e:
            pass # Ignore if schema mismatch for this batch

if __name__ == "__main__":
    for _ in range(5):
        process_batch()
