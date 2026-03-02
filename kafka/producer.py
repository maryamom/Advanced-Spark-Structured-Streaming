#!/usr/bin/env python3
"""Read data/events_dirty.json and send each line to Kafka topic 'events'."""
import os
import sys
import time

from kafka import KafkaProducer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SCRIPT_DIR)
DATA_PATH = os.path.join(ROOT, "data", "events_dirty.json")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("EVENTS_TOPIC", "events")

def main():
    if not os.path.isfile(DATA_PATH):
        print(f"File not found: {DATA_PATH}", file=sys.stderr)
        sys.exit(1)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP.split(","), value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"))
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                producer.send(TOPIC, value=line)
                time.sleep(0.02)
    producer.flush()
    producer.close()
    print("Done.")

if __name__ == "__main__":
    main()
