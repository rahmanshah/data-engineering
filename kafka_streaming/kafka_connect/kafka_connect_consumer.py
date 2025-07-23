import base64
import json
from decimal import Decimal

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}

def main():
    pass 

if __name__ == "__main__":
    main()