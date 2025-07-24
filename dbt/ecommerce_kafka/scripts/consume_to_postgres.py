import json
import psycopg2
from kafka import KafkaConsumer
import time

# Wait for services to be ready
print("Waiting for services to be ready...")
time.sleep(10)

# Database connection
conn = psycopg2.connect(
    host="postgres",  # Updated to use service name
    database="ecommerce_kafka",
    user="dbt_user",
    password="dbt_password"
)
cursor = conn.cursor()

# Create schemas
try:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS raw_data")
    conn.commit()
    print("Schema 'raw_data' created or already exists")
except Exception as e:
    print(f"Error creating schema 'raw_data': {e}")
    conn.rollback()

# Create tables if they don't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS raw_data.customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT UNIQUE,
    created_at TIMESTAMP,
    country TEXT
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS raw_data.orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    status TEXT,
    amount DECIMAL(10,2)
);
''')
conn.commit()

# Initialize Kafka consumers
customer_consumer = KafkaConsumer(
    'customers',
    bootstrap_servers=['kafka:9092'],  # Updated to use service name
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=30000  # Stop after 30 seconds of no messages
)

order_consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['kafka:9092'],  # Updated to use service name
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=30000  # Stop after 30 seconds of no messages
)

# Process customer messages
print("Consuming customer messages...")
for message in customer_consumer:
    customer = message.value
    try:
        cursor.execute(
            "INSERT INTO raw_data.customers VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO UPDATE SET first_name = %s, last_name = %s, email = %s, created_at = %s, country = %s",
            (
                customer['customer_id'],
                customer['first_name'],
                customer['last_name'],
                customer['email'],
                customer['created_at'],
                customer['country'],
                customer['first_name'],
                customer['last_name'],
                customer['email'],
                customer['created_at'],
                customer['country']
            )
        )
        conn.commit()
        print(f"Customer inserted/updated: {customer['customer_id']}")
    except Exception as e:
        print(f"Error inserting customer: {e}")
        conn.rollback()

# Process order messages  
print("Consuming order messages...")
for message in order_consumer:
    order = message.value
    try:
        cursor.execute(
            "INSERT INTO raw_data.orders VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO UPDATE SET customer_id = %s, order_date = %s, status = %s, amount = %s",
            (
                order['order_id'],
                order['customer_id'],
                order['order_date'],
                order['status'],
                order['amount'],
                order['customer_id'],
                order['order_date'],
                order['status'],
                order['amount']
            )
        )
        conn.commit()
        print(f"Order inserted/updated: {order['order_id']}")
    except Exception as e:
        print(f"Error inserting order: {e}")
        conn.rollback()

print("Consumer finished processing messages")
cursor.close()
conn.close()