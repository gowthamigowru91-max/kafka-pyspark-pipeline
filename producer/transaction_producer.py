"""
Kafka Transaction Producer
--------------------------
Simulates a high-throughput financial transaction feed.
Produces JSON events to the 'transactions.raw' topic.

Usage:
    python producer/transaction_producer.py
    python producer/transaction_producer.py --rate 10 --count 1000
"""
import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW,
    VALID_CURRENCIES, VALID_TRANSACTION_TYPES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Sample reference data ─────────────────────────────────────────────
MERCHANTS = [
    "Amazon", "Walmart", "Target", "Apple Store", "Netflix",
    "Uber", "Lyft", "Starbucks", "McDonald's", "Whole Foods",
    "Best Buy", "Home Depot", "Costco", "CVS", "Walgreens",
]
ACCOUNT_PREFIXES = ["ACC", "CHK", "SAV", "INV"]
REGIONS = ["US-EAST", "US-WEST", "EU-WEST", "APAC", "LATAM"]


def make_transaction(inject_error: bool = False) -> dict:
    """Generate a realistic transaction event."""
    amount = round(random.uniform(1.0, 5000.0), 2)
    currency = random.choice(list(VALID_CURRENCIES))
    tx_type  = random.choice(list(VALID_TRANSACTION_TYPES))

    event = {
        "transaction_id": str(uuid.uuid4()),
        "account_id":     f"{random.choice(ACCOUNT_PREFIXES)}-{random.randint(100000, 999999)}",
        "merchant":       random.choice(MERCHANTS),
        "amount":         amount if not inject_error else -amount,   # negative = DLQ candidate
        "currency":       currency if not inject_error else "INVALID",
        "transaction_type": tx_type,
        "region":         random.choice(REGIONS),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "is_flagged":     random.random() < 0.02,   # 2% flagged for review
        "metadata": {
            "source":       "transaction-producer-v1",
            "schema_version": "1.0",
        },
    }
    return event


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        log.debug("Delivered to %s [%d] @ offset %d",
                  msg.topic(), msg.partition(), msg.offset())


def run(rate_per_sec: float, total_count: int, error_rate: float):
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks":              "all",
        "retries":           5,
        "linger.ms":         10,
        "batch.size":        65536,
    })

    log.info("Starting producer → topic=%s  rate=%.1f/s  total=%d",
             KAFKA_TOPIC_RAW, rate_per_sec, total_count)

    interval = 1.0 / rate_per_sec
    produced = 0
    errors   = 0

    try:
        while total_count == 0 or produced < total_count:
            inject = random.random() < error_rate
            tx     = make_transaction(inject_error=inject)
            if inject:
                errors += 1

            producer.produce(
                topic     = KAFKA_TOPIC_RAW,
                key       = tx["account_id"].encode(),
                value     = json.dumps(tx).encode(),
                callback  = delivery_report,
            )
            producer.poll(0)
            produced += 1

            if produced % 100 == 0:
                log.info("Produced %d events (%d with errors)", produced, errors)

            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        log.info("Flushing remaining messages…")
        producer.flush()
        log.info("Done. Total produced: %d  (errors injected: %d)", produced, errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka transaction producer")
    parser.add_argument("--rate",       type=float, default=2.0,
                        help="Events per second (default: 2)")
    parser.add_argument("--count",      type=int,   default=0,
                        help="Total events to produce; 0 = run forever")
    parser.add_argument("--error-rate", type=float, default=0.05,
                        help="Fraction of events with injected errors (default: 0.05)")
    args = parser.parse_args()
    run(args.rate, args.count, args.error_rate)
