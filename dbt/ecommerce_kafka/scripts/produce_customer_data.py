from kafka import KafkaProducer
import json
import time
import uuid

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Updated to use service name
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample customer data
customers = [
    {"customer_id": 1, "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com", "created_at": "2024-01-15T10:30:00", "country": "USA"},
    {"customer_id": 2, "first_name": "Jane", "last_name": "Smith", "email": "jane.smith@example.com", "created_at": "2024-02-20T14:15:00", "country": "Canada"},
    {"customer_id": 3, "first_name": "Robert", "last_name": "Johnson", "email": "robert.j@example.com", "created_at": "2024-03-05T09:45:00", "country": "UK"},
    {"customer_id": 4, "first_name": "Maria", "last_name": "Garcia", "email": "maria.g@example.com", "created_at": "2024-03-10T16:20:00", "country": "Spain"},
    {"customer_id": 5, "first_name": "David", "last_name": "Brown", "email": "david.b@example.com", "created_at": "2024-04-25T11:10:00", "country": "USA"}
]

# Send customer data to Kafka
for customer in customers:
    producer.send('customers', value=customer)
    print(f"Sent customer: {customer['first_name']} {customer['last_name']}")
    time.sleep(1)

producer.flush()
print("All customer data sent to Kafka")