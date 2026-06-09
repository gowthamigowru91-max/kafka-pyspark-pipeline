# kafka-pyspark-pipeline

Real-time financial transaction pipeline built with **Apache Kafka** and **PySpark Structured Streaming**.

## Architecture

```
Transaction Events
       │
       ▼
┌─────────────────┐
│  Kafka Producer  │  Generates synthetic transaction events at configurable rate
│  (Python)        │  topics: transactions.raw
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│           PySpark Streaming Pipeline         │
│                                             │
│  1. Ingest       ← Kafka topic (raw)        │
│  2. Parse        ← JSON → typed schema      │
│  3. Validate     ← DQ checks                │
│  4. Enrich       ← risk score + FX convert  │
│  5. Aggregate    ← 5-min window by merchant │
│                                             │
└──────┬─────────────────────┬───────────────┘
       │                     │
       ▼                     ▼
  Parquet Output        Kafka DLQ Topic
  (partitioned by       (transactions.dlq)
   date + region)
```

## Stack

| Component | Technology |
|-----------|-----------|
| Message broker | Apache Kafka (Confluent 7.5) |
| Schema management | Confluent Schema Registry |
| Stream processing | PySpark 3.5 Structured Streaming |
| Output format | Parquet (partitioned) |
| Local infra | Docker Compose |
| Testing | pytest + PySpark local mode |

## Quick Start

### 1. Start Kafka infrastructure

```bash
docker-compose up -d
```

This starts:
- Kafka broker on `localhost:9092`
- Schema Registry on `localhost:8081`
- Kafka UI on `http://localhost:8080` ← browse topics and messages here

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the producer

```bash
# Produce 2 events/sec indefinitely
python producer/transaction_producer.py

# Produce 500 events at 10/sec with 10% injected errors
python producer/transaction_producer.py --rate 10 --count 500 --error-rate 0.10
```

### 4. Start the PySpark pipeline

```bash
# Option A: via spark-submit (recommended)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/pipeline.py

# Option B: Python directly (uses local[*] Spark)
python spark/pipeline.py
```

### 5. Monitor

```bash
# Watch raw events
python consumer/simple_consumer.py

# Watch DLQ (bad records)
python consumer/simple_consumer.py --topic transactions.dlq

# Browse in Kafka UI
open http://localhost:8080
```

### 6. Run tests

```bash
pytest tests/ -v
```

## Configuration

All settings are in `config/settings.py` and can be overridden via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_RAW` | `transactions.raw` | Input topic |
| `KAFKA_TOPIC_DLQ` | `transactions.dlq` | Dead-letter topic |
| `OUTPUT_DIR` | `./output/parquet` | Parquet output path |
| `CHECKPOINT_DIR` | `/tmp/spark_checkpoints` | Spark checkpoint dir |
| `TRIGGER_INTERVAL` | `30 seconds` | Micro-batch interval |

## Pipeline Details

### Data Quality Rules
Records failing any rule are routed to the DLQ topic with a `dq_reason` field:
- `amount` must be between $0.01 and $1,000,000
- `currency` must be one of: USD, EUR, GBP, JPY, CAD, AUD
- `transaction_type` must be one of: PURCHASE, REFUND, TRANSFER, WITHDRAWAL, DEPOSIT
- `transaction_id` and `account_id` must be non-null

### Enrichment
- **Risk score**: 0.1 (normal) → 0.4 (>$1K) → 0.7 (>$10K) → 0.9 (flagged)
- **Amount USD**: All currencies normalized to USD using fixed FX rates
- **Processing date/hour**: Extracted from event timestamp for partitioning

### Output Schema (Parquet)
Partitioned by `processing_date` / `region`:
```
output/parquet/enriched/
  processing_date=2024-01-15/
    region=US-EAST/
      part-00000-*.parquet
```

## Project Structure

```
kafka-pyspark-pipeline/
├── config/
│   └── settings.py          # Central config
├── producer/
│   └── transaction_producer.py
├── consumer/
│   └── simple_consumer.py   # Lightweight consumer for testing/DLQ monitoring
├── spark/
│   └── pipeline.py          # PySpark Structured Streaming job
├── tests/
│   └── test_pipeline.py     # Unit tests (validate + enrich)
├── docker-compose.yml       # Kafka + Schema Registry + UI
├── requirements.txt
└── README.md
```
