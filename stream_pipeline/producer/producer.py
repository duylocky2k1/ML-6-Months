from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random

def create_producer():
    retries = 0
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                retries=5,
                acks='all'
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            retries += 1
            print(f"Kafka not ready, retry {retries}/10...")
            time.sleep(3)
            if retries > 10:
                raise

producer = create_producer()

# Dữ liệu
valid = [
    {"order_id": 1, "user_id": 101, "amount": 99.99, "ts": 1731222000},
    {"order_id": 2, "user_id": 102, "amount": 149.50, "ts": 1731222060},
]

invalid = [
    '{"order_id": 3, "user_id": "abc"}',
    '{"order_id": 4}',
    'not json at all',
    '{"order_id": 5, "user_id": 105, "amount": "xyz"}',
]

messages = [(json.dumps(m).encode('utf-8'), "valid") for m in valid] + \
           [(m.encode('utf-8'), "invalid") for m in invalid]

for msg, typ in messages:
    producer.send('orders_stream', msg)
    print(f"Sent {typ}: {msg.decode('utf-8', errors='ignore')[:60]}...")
    time.sleep(0.5)

producer.flush()
print("All messages sent! Producer exiting.")
