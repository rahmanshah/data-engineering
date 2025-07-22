import json
import textwrap

from confluent_kafka import Producer
from sseclient import SSEClient

producer_conf = {"bootstrap.servers": "localhost:9092"}
kafka_topic = "wikipedia-changes"

def main():
    pass 

if __name__ == "__main__":
    main()