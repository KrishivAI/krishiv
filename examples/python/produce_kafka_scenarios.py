import json
import time
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

def delivery_report(err, msg):
    if err is not None:
        pass

# We will send JSON messages
scenarios = [
    # 1. Fraud
    {"scenario": "fraud", "user_id": "U1", "amount": 5000, "ts": int(time.time())},
    {"scenario": "fraud", "user_id": "U1", "amount": 6000, "ts": int(time.time())},
    # 2. IoT
    {"scenario": "iot", "device_id": "D1", "temp": 95, "ts": int(time.time())},
    # 3. Clickstream
    {"scenario": "clickstream", "user": "U2", "action": "click", "ts": int(time.time())},
    # 4. Ride
    {"scenario": "ride", "zone": "Z1", "requests": 1, "ts": int(time.time())},
    # 5. Log
    {"scenario": "log", "service": "auth", "status": 500, "ts": int(time.time())},
    # 6. Supply Chain
    {"scenario": "supply", "truck": "T1", "lat": 34.0, "lon": -118.0, "ts": int(time.time())},
    # 7. VWAP
    {"scenario": "vwap", "ticker": "AAPL", "price": 150.0, "volume": 100, "ts": int(time.time())},
    # 8. Social
    {"scenario": "social", "hashtag": "#rust", "ts": int(time.time())},
    # 9. Gaming
    {"scenario": "gaming", "player": "P1", "score": 1000, "ts": int(time.time())},
    # 10. Retail
    {"scenario": "retail", "sku": "SKU1", "count": 5, "ts": int(time.time())},
]

for _ in range(10):
    for event in scenarios:
        p.produce('scenarios', json.dumps(event).encode('utf-8'), callback=delivery_report)
    p.flush()
    time.sleep(0.5)
