"""
Simple Kafka Consumer (non-Spark)
----------------------------------
Lightweight consumer for local testing and DLQ monitoring.
Reads from any topic and prints events to stdout.

Usage:
    # Monitor the raw topic
    python consumer/simple_consumer.py

    # Monitor the DLQ
    python consumer/simple_consumer.py --topic transactions.dlq

    # From beginning
    python consumer/simple_consumer.py --from-beginning
"""
import argparse
import json
import logging
import sys
import os

from confluent_kafka import Consumer, KafkaError, KafkaException

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW,
    KAFKA_TOPIC_DLQ, KAFKA_CONSUMER_GROUP,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def run(topic: str, from_beginning: bool, max_messages: int):
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           f"{KAFKA_CONSUMER_GROUP}-simple",
        "auto.offset.reset":  "earliest" if from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([topic])
    log.info("Subscribed to topic: %s", topic)

    consumed = 0
    try:
        while max_messages == 0 or consumed < max_messages:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info("Reached end of partition %d", msg.partition())
                else:
                    raise KafkaException(msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                print(json.dumps(event, indent=2))
            except json.JSONDecodeError:
                log.warning("Could not decode message: %s", msg.value())

            consumed += 1
            if consumed % 50 == 0:
                log.info("Consumed %d messages from %s", consumed, topic)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        consumer.close()
        log.info("Consumer closed. Total consumed: %d", consumed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple Kafka consumer")
    parser.add_argument("--topic",         default=KAFKA_TOPIC_RAW,
                        help="Topic to consume from")
    parser.add_argument("--from-beginning", action="store_true",
                        help="Read from earliest offset")
    parser.add_argument("--max",           type=int, default=0,
                        help="Max messages to consume; 0 = infinite")
    args = parser.parse_args()
    run(args.topic, args.from_beginning, args.max)
