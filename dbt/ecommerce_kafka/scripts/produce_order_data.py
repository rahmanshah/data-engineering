from kafka import KafkaProducer
import json
import time
import uuid

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Updated to use service name
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample order data
orders = [
    {"order_id": 1001, "customer_id": 1, "order_date": "2024-05-10", "status": "completed", "amount": 125.99},
    {"order_id": 1002, "customer_id": 2, "order_date": "2024-05-15", "status": "completed", "amount": 89.50},
    {"order_id": 1003, "customer_id": 1, "order_date": "2024-06-01", "status": "completed", "amount": 245.75},
    {"order_id": 1004, "customer_id": 3, "order_date": "2024-06-10", "status": "completed", "amount": 45.25},
    {"order_id": 1005, "customer_id": 2, "order_date": "2024-06-20", "status": "returned", "amount": 120.00},
    {"order_id": 1006, "customer_id": 4, "order_date": "2024-07-05", "status": "completed", "amount": 550.00},
    {"order_id": 1007, "customer_id": 5, "order_date": "2024-07-10", "status": "pending", "amount": 1200.50},
    {"order_id": 1008, "customer_id": 1, "order_date": "2024-07-15", "status": "completed", "amount": 78.25}
]

# Send order data to Kafka
for order in orders:
    producer.send('orders', value=order)
    print(f"Sent order: {order['order_id']}")
    time.sleep(1)

producer.flush()
print("All order data sent to Kafka")