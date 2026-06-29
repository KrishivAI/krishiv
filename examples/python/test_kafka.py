from confluent_kafka import Consumer
c = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})
c.subscribe(['fraud'])
msg = c.poll(5.0)
if msg is None:
    print("No message received.")
elif msg.error():
    print(f"Error: {msg.error()}")
else:
    print(f"Received: {msg.value()}")
