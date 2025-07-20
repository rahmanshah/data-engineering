import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer

## Function to generate random data
def generate_order():
    countries = [
        "USA",
        "Canada",
        "UK",
        "Germany",
        "France",
        "Australia",
        "Japan",
        "Ireland",
    ]
    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 10),
        "total_price": round(random.uniform(20.0, 1000.0), 2),
        "customer_country": random.choice(countries),
        "merchant_country": random.choice(countries),
        "order_date": datetime.now().isoformat(),
    }
    return order

def main():
    config = {
        "bootstrap.servers": "localhost:9092"
    }
    producer = Producer(config)

    topic = "orders"

if __name__ == "__main__":
    main()