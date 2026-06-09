"""
Unit tests for the PySpark pipeline transformations.
Run with: pytest tests/
"""
import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("TestPipeline")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_data(spark):
    rows = [
        ("txn-001", "ACC-111111", "Amazon",    150.0,  "USD", "PURCHASE",   "US-EAST", "2024-01-15T10:00:00+00:00", False),
        ("txn-002", "ACC-222222", "Netflix",   15.99,  "EUR", "PURCHASE",   "EU-WEST", "2024-01-15T10:01:00+00:00", False),
        ("txn-003", "ACC-333333", "Walmart",  -50.0,   "USD", "PURCHASE",   "US-WEST", "2024-01-15T10:02:00+00:00", False),  # bad: negative
        ("txn-004", "ACC-444444", "Apple",     999.0,  "XYZ", "PURCHASE",   "US-EAST", "2024-01-15T10:03:00+00:00", False),  # bad: invalid currency
        ("txn-005", "ACC-555555", "Starbucks",  5.50,  "USD", "UNKNOWN_TX", "APAC",    "2024-01-15T10:04:00+00:00", True),   # bad: invalid type
        ("txn-006", "ACC-666666", "Target",  15000.0,  "USD", "TRANSFER",   "US-EAST", "2024-01-15T10:05:00+00:00", True),   # high risk
    ]
    schema = StructType([
        StructField("transaction_id",   StringType(),  False),
        StructField("account_id",       StringType(),  False),
        StructField("merchant",         StringType(),  True),
        StructField("amount",           DoubleType(),  False),
        StructField("currency",         StringType(),  False),
        StructField("transaction_type", StringType(),  False),
        StructField("region",           StringType(),  True),
        StructField("timestamp",        StringType(),  False),
        StructField("is_flagged",       BooleanType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema).withColumn(
        "event_ts", F.to_timestamp("timestamp")
    )


class TestValidation:
    def test_valid_records_pass(self, sample_data):
        from spark.pipeline import validate
        valid_df, _ = validate(sample_data)
        ids = {r.transaction_id for r in valid_df.collect()}
        assert "txn-001" in ids
        assert "txn-002" in ids
        assert "txn-006" in ids  # high amount but within range, valid

    def test_negative_amount_goes_to_dlq(self, sample_data):
        from spark.pipeline import validate
        _, invalid_df = validate(sample_data)
        ids = {r.transaction_id for r in invalid_df.collect()}
        assert "txn-003" in ids

    def test_invalid_currency_goes_to_dlq(self, sample_data):
        from spark.pipeline import validate
        _, invalid_df = validate(sample_data)
        ids = {r.transaction_id for r in invalid_df.collect()}
        assert "txn-004" in ids

    def test_invalid_transaction_type_goes_to_dlq(self, sample_data):
        from spark.pipeline import validate
        _, invalid_df = validate(sample_data)
        ids = {r.transaction_id for r in invalid_df.collect()}
        assert "txn-005" in ids

    def test_dlq_reason_populated(self, sample_data):
        from spark.pipeline import validate
        _, invalid_df = validate(sample_data)
        reasons = {r.transaction_id: r.dq_reason for r in invalid_df.collect()}
        assert reasons.get("txn-003") == "AMOUNT_OUT_OF_RANGE"
        assert reasons.get("txn-004") == "INVALID_CURRENCY"
        assert reasons.get("txn-005") == "INVALID_TRANSACTION_TYPE"


class TestEnrichment:
    def test_risk_score_flagged(self, sample_data):
        from spark.pipeline import validate, enrich
        valid_df, _ = validate(sample_data)
        enriched = enrich(valid_df)
        row = enriched.filter(F.col("is_flagged") == True).first()
        assert row.risk_score == 0.9

    def test_risk_score_high_amount(self, sample_data):
        from spark.pipeline import validate, enrich
        valid_df, _ = validate(sample_data)
        enriched = enrich(valid_df)
        # txn-006: amount=15000, is_flagged=True → risk 0.9 (flagged takes priority)
        row = enriched.filter(F.col("transaction_id") == "txn-006").first()
        assert row.risk_score == 0.9

    def test_usd_amount_unchanged(self, sample_data):
        from spark.pipeline import validate, enrich
        valid_df, _ = validate(sample_data)
        enriched = enrich(valid_df)
        row = enriched.filter(F.col("transaction_id") == "txn-001").first()
        assert row.amount_usd == pytest.approx(150.0)

    def test_eur_converted(self, sample_data):
        from spark.pipeline import validate, enrich
        valid_df, _ = validate(sample_data)
        enriched = enrich(valid_df)
        row = enriched.filter(F.col("transaction_id") == "txn-002").first()
        assert row.amount_usd == pytest.approx(15.99 * 1.08, rel=1e-3)

    def test_processing_date_added(self, sample_data):
        from spark.pipeline import validate, enrich
        valid_df, _ = validate(sample_data)
        enriched = enrich(valid_df)
        row = enriched.filter(F.col("transaction_id") == "txn-001").first()
        assert row.processing_date is not None
