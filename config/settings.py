"""
Central configuration for the Kafka + PySpark pipeline.
Override any value via environment variables.
"""
import os

# ── Kafka ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",  "transactions.raw")
KAFKA_TOPIC_ENRICHED    = os.getenv("KAFKA_TOPIC_ENRICHED", "transactions.enriched")
KAFKA_TOPIC_DLQ         = os.getenv("KAFKA_TOPIC_DLQ",  "transactions.dlq")
KAFKA_CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "pyspark-pipeline-group")
SCHEMA_REGISTRY_URL     = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# ── Producer ──────────────────────────────────────────────────────────
PRODUCER_INTERVAL_SEC   = float(os.getenv("PRODUCER_INTERVAL_SEC", "0.5"))
PRODUCER_BATCH_SIZE     = int(os.getenv("PRODUCER_BATCH_SIZE", "100"))

# ── PySpark ───────────────────────────────────────────────────────────
SPARK_APP_NAME          = "KafkaTransactionPipeline"
SPARK_MASTER            = os.getenv("SPARK_MASTER", "local[*]")
SPARK_KAFKA_PACKAGE     = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
CHECKPOINT_DIR          = os.getenv("CHECKPOINT_DIR", "/tmp/spark_checkpoints")

# ── Output ────────────────────────────────────────────────────────────
OUTPUT_DIR              = os.getenv("OUTPUT_DIR", "./output/parquet")
OUTPUT_FORMAT           = "parquet"
OUTPUT_MODE             = "append"
TRIGGER_INTERVAL        = "30 seconds"

# ── Data Quality ──────────────────────────────────────────────────────
MIN_AMOUNT              = 0.01
MAX_AMOUNT              = 1_000_000.0
VALID_CURRENCIES        = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD"}
VALID_TRANSACTION_TYPES = {"PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT"}
