"""Kafka to bronze Delta, append-only.

Bronze keeps the payload as an opaque string. Parsing happens in silver so a
schema change or a malformed message can never stop ingestion, and bronze can
always be replayed into a fixed silver job.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from plant_telemetry.common.config import Settings


def read_kafka(spark: SparkSession, settings: Settings, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def with_ingest_metadata(kafka_df: DataFrame) -> DataFrame:
    """Project a Kafka source frame to the bronze schema.

    Topic, partition and offset are kept because together they identify a
    message exactly; that is what you need to trace a silver row back to its
    origin or to prove a replay did not double count.
    """
    return kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("payload"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("_ingested_at"),
    )


def start_bronze(
    spark: SparkSession, settings: Settings, topic: str, table: str, available_now: bool
) -> StreamingQuery:
    writer = (
        with_ingest_metadata(read_kafka(spark, settings, topic))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint(f"bronze_{table}"))
        .queryName(f"bronze_{table}")
    )
    writer = writer.trigger(availableNow=True) if available_now else writer
    return writer.start(settings.table("bronze", table))
