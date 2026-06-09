"""
PySpark Streaming Pipeline
--------------------------
Reads transaction events from Kafka, applies:
  1. Schema enforcement & deserialization
  2. Data quality validation  (bad records → DLQ topic)
  3. Enrichment               (risk scoring, currency normalization)
  4. Aggregations             (rolling 5-min window stats per merchant)
  5. Sink                     (Parquet output partitioned by date/region)

Usage:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        spark/pipeline.py

Or with Python directly (uses local[*] Spark):
    python spark/pipeline.py
"""
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType, MapType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW, KAFKA_TOPIC_ENRICHED,
    KAFKA_TOPIC_DLQ, KAFKA_CONSUMER_GROUP,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_KAFKA_PACKAGE,
    CHECKPOINT_DIR, OUTPUT_DIR, TRIGGER_INTERVAL,
    MIN_AMOUNT, MAX_AMOUNT, VALID_CURRENCIES, VALID_TRANSACTION_TYPES,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── Schema ────────────────────────────────────────────────────────────

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    False),
    StructField("account_id",        StringType(),    False),
    StructField("merchant",          StringType(),    True),
    StructField("amount",            DoubleType(),    False),
    StructField("currency",          StringType(),    False),
    StructField("transaction_type",  StringType(),    False),
    StructField("region",            StringType(),    True),
    StructField("timestamp",         StringType(),    False),
    StructField("is_flagged",        BooleanType(),   True),
    StructField("metadata",          MapType(StringType(), StringType()), True),
])


# ── Spark session ─────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── Read from Kafka ───────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("kafka.group.id", KAFKA_CONSUMER_GROUP)
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Parse & deserialize ───────────────────────────────────────────────

def parse_events(raw_df):
    """Deserialize Kafka value bytes → typed struct."""
    return (
        raw_df
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value",
                    "partition", "offset", "timestamp as kafka_ts")
        .withColumn("data", F.from_json(F.col("json_value"), TRANSACTION_SCHEMA))
        .select("kafka_key", "kafka_ts", "partition", "offset", "data.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("ingest_ts", F.current_timestamp())
    )


# ── Data quality ──────────────────────────────────────────────────────

VALID_CURRENCIES_LIST       = list(VALID_CURRENCIES)
VALID_TRANSACTION_TYPES_LIST = list(VALID_TRANSACTION_TYPES)

def validate(df):
    """
    Split into valid and invalid (DLQ) streams.
    Returns (valid_df, invalid_df).
    """
    validation_expr = (
        F.col("transaction_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("amount").between(MIN_AMOUNT, MAX_AMOUNT)
        & F.col("currency").isin(VALID_CURRENCIES_LIST)
        & F.col("transaction_type").isin(VALID_TRANSACTION_TYPES_LIST)
        & F.col("event_ts").isNotNull()
    )

    valid_df = df.filter(validation_expr).withColumn("dq_status", F.lit("VALID"))
    invalid_df = (
        df.filter(~validation_expr)
          .withColumn("dq_status", F.lit("INVALID"))
          .withColumn("dq_reason",
                F.when(~F.col("amount").between(MIN_AMOUNT, MAX_AMOUNT),
                       F.lit("AMOUNT_OUT_OF_RANGE"))
                .when(~F.col("currency").isin(VALID_CURRENCIES_LIST),
                       F.lit("INVALID_CURRENCY"))
                .when(~F.col("transaction_type").isin(VALID_TRANSACTION_TYPES_LIST),
                       F.lit("INVALID_TRANSACTION_TYPE"))
                .otherwise(F.lit("MISSING_REQUIRED_FIELD")))
    )
    return valid_df, invalid_df


# ── Enrichment ────────────────────────────────────────────────────────

def enrich(df):
    """Add risk score, normalized amount, processing date."""
    return (
        df
        # Simple rule-based risk score (in prod: call a feature store or ML model)
        .withColumn("risk_score",
            F.when(F.col("is_flagged"), 0.9)
             .when(F.col("amount") > 10_000, 0.7)
             .when(F.col("amount") > 1_000,  0.4)
             .otherwise(0.1)
        )
        # Normalize all amounts to USD (simplified — real impl calls FX rates API)
        .withColumn("amount_usd",
            F.when(F.col("currency") == "EUR", F.col("amount") * 1.08)
             .when(F.col("currency") == "GBP", F.col("amount") * 1.27)
             .when(F.col("currency") == "JPY", F.col("amount") * 0.0067)
             .when(F.col("currency") == "CAD", F.col("amount") * 0.74)
             .when(F.col("currency") == "AUD", F.col("amount") * 0.65)
             .otherwise(F.col("amount"))   # already USD
        )
        .withColumn("processing_date", F.to_date("event_ts"))
        .withColumn("processing_hour", F.hour("event_ts"))
    )


# ── Windowed aggregations ─────────────────────────────────────────────

def aggregate(df):
    """5-minute tumbling window: total volume and tx count per merchant + region."""
    return (
        df
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "merchant",
            "region",
            "transaction_type",
        )
        .agg(
            F.count("transaction_id").alias("tx_count"),
            F.sum("amount_usd").alias("total_volume_usd"),
            F.avg("amount_usd").alias("avg_amount_usd"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum(F.when(F.col("is_flagged"), 1).otherwise(0)).alias("flagged_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


# ── Sinks ─────────────────────────────────────────────────────────────

def sink_parquet(df, name: str, path: str, partition_cols=None):
    """Write a streaming DF to partitioned Parquet."""
    writer = (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    return writer.start()


def sink_kafka(df, topic: str, name: str):
    """Write a streaming DF back to a Kafka topic."""
    return (
        df
        .select(
            F.col("account_id").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def sink_console(df, name: str, num_rows: int = 5):
    """Debug sink — prints batches to stdout."""
    return (
        df.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", num_rows)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}_console")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session started — app: %s", SPARK_APP_NAME)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # 1. Ingest
    raw_df    = read_kafka(spark)
    parsed_df = parse_events(raw_df)

    # 2. Validate
    valid_df, invalid_df = validate(parsed_df)

    # 3. Enrich valid events
    enriched_df = enrich(valid_df)

    # 4. Aggregate
    agg_df = aggregate(enriched_df)

    # 5. Sinks
    queries = [
        # Main enriched stream → Parquet (partitioned by date + region)
        sink_parquet(enriched_df, "enriched",
                     f"{OUTPUT_DIR}/enriched",
                     partition_cols=["processing_date", "region"]),

        # Aggregations → Parquet
        sink_parquet(agg_df, "aggregations",
                     f"{OUTPUT_DIR}/aggregations",
                     partition_cols=["window_start"]),

        # DLQ → Kafka dead-letter topic
        sink_kafka(invalid_df, KAFKA_TOPIC_DLQ, "dlq"),

        # Debug console (remove in prod)
        sink_console(enriched_df, "debug", num_rows=3),
    ]

    log.info("All streaming queries started. Waiting for termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
