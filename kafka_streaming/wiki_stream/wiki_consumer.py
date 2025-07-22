import json
import sys

from confluent_kafka import Consumer

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "wiki-consumer-group",
    "auto.offset.reset": "earliest",
}
kafka_topic = "wikipedia-changes"

def main():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic])

    print(f"Consuming messages from topic '{kafka_topic}'")

    try:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()