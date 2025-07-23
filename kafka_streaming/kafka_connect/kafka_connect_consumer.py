import base64
import json
from decimal import Decimal

from confluent_kafka import Consumer, KafkaException

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}

def decode_decimal(encoded_string, scale=2):
    value_bytes = base64.b64decode(encoded_string)
    unscaled_value = int.from_bytes(value_bytes, byteorder="big", signed=True)
    return Decimal(unscaled_value) / Decimal(10**scale)

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