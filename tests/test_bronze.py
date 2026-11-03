from datetime import datetime

from plant_telemetry.common.schemas import BRONZE
from plant_telemetry.pipelines.bronze import with_ingest_metadata


def test_kafka_record_is_projected_to_bronze_schema(spark):
    kafka_like = spark.createDataFrame(
        [(b"L1-M01", b'{"seq": 1}', "telemetry", 0, 42, datetime(2026, 3, 2, 8, 0), 0)],
        "key binary, value binary, topic string, partition int, offset long, "
        "timestamp timestamp, timestampType int",
    )

    bronze = with_ingest_metadata(kafka_like)

    assert bronze.columns == BRONZE.fieldNames()
    row = bronze.first()
    assert (row.key, row.payload, row.offset) == ("L1-M01", '{"seq": 1}', 42)
    assert row._ingested_at is not None
