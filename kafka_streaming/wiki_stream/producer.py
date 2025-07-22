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

    producer = Producer(producer_conf)
    messages = SSEClient(url)

    for event in messages:
        if event.event == "message" and event.data:
            try:
                data = json.loads(event.data)
            except json.JSONDecodeError:
                continue

            id = data.get("id")

if __name__ == "__main__":
    main()