"""Bronze telemetry to silver, with validation, watermarking and dedup.

Two streaming queries read the same bronze table: valid readings go to silver,
everything that fails a check goes to quarantine with the reasons attached.
Bad data is kept and explained instead of failing the stream or vanishing.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from plant_telemetry.common.config import Settings
from plant_telemetry.common.schemas import JSON_OPTIONS, MACHINE_STATUSES, TELEMETRY_PAYLOAD
from plant_telemetry.pipelines.quarantine import Check, quarantined, valid_rows, with_errors

DEDUP_KEY = ("machine_id", "event_ts", "seq")


def _checks() -> tuple[Check, ...]:
    # Built on demand: Column expressions need an active SparkContext.
    return (
        ("missing_machine_id", F.col("r.machine_id").isNull()),
        ("missing_event_ts", F.col("r.event_ts").isNull()),
        ("missing_seq", F.col("r.seq").isNull()),
        ("unknown_status", ~F.coalesce(F.col("r.status").isin(*MACHINE_STATUSES), F.lit(False))),
        (
            "temperature_out_of_range",
            ~F.coalesce(F.col("r.temperature_c").between(-40.0, 250.0), F.lit(True)),
        ),
        (
            "negative_count",
            F.coalesce(
                (F.col("r.parts_produced") < 0) | (F.col("r.parts_rejected") < 0), F.lit(False)
            ),
        ),
        (
            "rejected_exceeds_produced",
            F.coalesce(F.col("r.parts_rejected") > F.col("r.parts_produced"), F.lit(False)),
        ),
    )


def parse_and_validate(bronze: DataFrame) -> DataFrame:
    parsed = bronze.withColumn("r", F.from_json("payload", TELEMETRY_PAYLOAD, JSON_OPTIONS))
    return with_errors(parsed, "r", _checks())


def valid_readings(validated: DataFrame) -> DataFrame:
    return valid_rows(validated, "r", "topic", "partition", "offset", "_ingested_at")


def deduplicate_readings(readings: DataFrame, watermark_delay: str) -> DataFrame:
    """Drop redeliveries of the same reading.

    On a stream, a key is remembered until the watermark (max event time seen
    minus ``watermark_delay``) passes it, so state stays bounded. A reading
    whose event time is already behind the watermark when it arrives is
    dropped by Spark: very late readings are traded for bounded state, and the
    delay is the knob. A batch frame (backfill, tests) gets a plain dedup on
    the same key, because watermarks have no meaning there.
    """
    if not readings.isStreaming:
        return readings.dropDuplicates(list(DEDUP_KEY))
    return readings.withWatermark("event_ts", watermark_delay).dropDuplicatesWithinWatermark(
        list(DEDUP_KEY)
    )


def start_silver_telemetry(
    spark: SparkSession, settings: Settings, available_now: bool
) -> list[StreamingQuery]:
    validated = parse_and_validate(
        spark.readStream.format("delta").load(settings.table("bronze", "telemetry_raw"))
    )
    outputs = (
        (
            "silver_telemetry",
            deduplicate_readings(valid_readings(validated), settings.watermark_delay),
            settings.table("silver", "telemetry"),
        ),
        (
            "quarantine_telemetry",
            quarantined(validated, source="telemetry"),
            settings.table("silver", "quarantine"),
        ),
    )
    queries = []
    for name, frame, table in outputs:
        writer = (
            frame.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", settings.checkpoint(name))
            .queryName(name)
        )
        writer = writer.trigger(availableNow=True) if available_now else writer
        queries.append(writer.start(table))
    return queries
