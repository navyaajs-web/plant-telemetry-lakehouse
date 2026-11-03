"""Wire and table schemas.

Payload schemas are declared explicitly rather than inferred: inference on a
stream would silently widen types the first time a bad record shows up.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MACHINE_STATUSES = ("RUNNING", "IDLE", "DOWN")
WORK_ORDER_STATUSES = ("OPEN", "IN_PROGRESS", "ON_HOLD", "CLOSED")
CDC_OPS = ("c", "u", "d")

# from_json does not return NULL for a broken payload; it returns a struct of
# NULLs. Asking Spark to keep the raw text in this column is the reliable way
# to tell "malformed" apart from "valid JSON with missing fields".
CORRUPT_RECORD = "_corrupt_record"
JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_RECORD}

TELEMETRY_PAYLOAD = StructType(
    [
        StructField("plant_id", StringType()),
        StructField("line_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("seq", LongType()),
        StructField("status", StringType()),
        StructField("temperature_c", DoubleType()),
        StructField("vibration_mm_s", DoubleType()),
        StructField("parts_produced", IntegerType()),
        StructField("parts_rejected", IntegerType()),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)

WORK_ORDER_PAYLOAD = StructType(
    [
        StructField("op", StringType()),
        StructField("lsn", LongType()),
        StructField("change_ts", TimestampType()),
        StructField("work_order_id", StringType()),
        StructField("machine_id", StringType()),
        StructField("status", StringType()),
        StructField("priority", StringType()),
        StructField("reason", StringType()),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)

# Columns of the bronze tables, i.e. the Kafka record plus ingestion metadata.
BRONZE = StructType(
    [
        StructField("key", StringType()),
        StructField("payload", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

DIM_WORK_ORDER = StructType(
    [
        StructField("work_order_id", StringType(), nullable=False),
        StructField("machine_id", StringType()),
        StructField("status", StringType()),
        StructField("priority", StringType()),
        StructField("reason", StringType()),
        StructField("lsn", LongType(), nullable=False),
        StructField("op", StringType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
        StructField("is_deleted", BooleanType()),
    ]
)
