import json
import textwrap

from confluent_kafka import Producer
from sseclient import SSEClient

producer_conf = {"bootstrap.servers": "localhost:9092"}
kafka_topic = "wikipedia-changes"

def main():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"

    print(
        f"Starting to consume Wikipedia recent changes from {url} and produce to Kafka topic '{kafka_topic}'..."
    )

if __name__ == "__main__":
    main()