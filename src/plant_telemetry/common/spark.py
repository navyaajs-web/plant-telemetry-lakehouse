"""SparkSession construction.

Delta (and Kafka, for streaming jobs) are pulled as JVM packages that match the
pinned ``delta-spark`` wheel, so the Python and JVM sides never drift apart.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def get_spark(app_name: str, master: str = "local[2]", with_kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        # Small local data: the default 200 shuffle partitions is pure overhead.
        .config("spark.sql.shuffle.partitions", "4")
    )
    extra = [KAFKA_PACKAGE] if with_kafka else None
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
