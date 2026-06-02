import asyncio
from confluent_kafka import Consumer

async def main():
    c = Consumer({
        'bootstrap.servers': '127.0.0.1:9092',
        'group.id': 'krishiv-test-consumer',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['fraud'])
    
    count = 0
    while count < 100:
        msg = await asyncio.to_thread(c.poll, 1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        count += 1
        print(f"Received {count}")

asyncio.run(main())
