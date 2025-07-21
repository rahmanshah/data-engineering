import argparse
import json

from confluent_kafka import Consumer


def main():
    parser = argparse.ArgumentParser(description="Test Kafka consumer")
    parser.add_argument("--group-id", "-g", help="Consumer group ID")
    parser.add_argument("--topic-name", "-t", help="Topic name ")
    parser.add_argument("--name", "-n", help="Name of this consumer")

    args = parser.parse_args()

    group_id = args.group_id
    topic_name = args.topic_name
    consumer_name = args.name

if __name__ == "__main__":
    main()