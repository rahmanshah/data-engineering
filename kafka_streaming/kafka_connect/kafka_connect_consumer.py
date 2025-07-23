import base64
import json
from decimal import Decimal

from confluent_kafka import Consumer, KafkaException

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}

def main():
    consumer = Consumer(consumer_config)

    topic = "postgres-.public.orders"
    consumer.subscribe([topic])

    try:
        print(f"Consuming messages from topic '{topic}'")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
    finally:
        consumer.close()

if __name__ == "__main__":
    main()