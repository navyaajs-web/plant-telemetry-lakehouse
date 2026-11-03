import json
import os
from datetime import UTC, datetime, timedelta

from plant_telemetry.common.schemas import BRONZE
from plant_telemetry.generator.simulation import iso
from plant_telemetry.pipelines.quarantine import quarantined
from plant_telemetry.pipelines.silver_telemetry import (
    deduplicate_readings,
    parse_and_validate,
    valid_readings,
)

T0 = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)


def reading(minute: float, seq: int, machine: str = "L1-M01", **overrides) -> dict:
    row = {
        "plant_id": "NW-P1",
        "line_id": "L1",
        "machine_id": machine,
        "event_ts": iso(T0 + timedelta(minutes=minute)),
        "seq": seq,
        "status": "RUNNING",
        "temperature_c": 65.0,
        "vibration_mm_s": 2.4,
        "parts_produced": 2,
        "parts_rejected": 0,
    }
    return {**row, **overrides}


def bronze_row(payload: str, offset: int) -> dict:
    return {
        "key": "k",
        "payload": payload,
        "topic": "telemetry",
        "partition": 0,
        "offset": offset,
        "kafka_ts": T0.isoformat(),
        "_ingested_at": T0.isoformat(),
    }


def bronze_df(spark, payloads: list[str]):
    rows = [("k", p, "telemetry", 0, i, T0, T0) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, BRONZE)


def test_valid_readings_pass_and_bad_ones_are_quarantined_with_reasons(spark):
    payloads = [
        json.dumps(reading(0, 1)),
        '{"machine_id": "L1-M01", "event_ts": "2026-03',  # truncated
        json.dumps(reading(0, 2, status="UNKNOWN")),
        json.dumps(reading(0, 3, machine=None)),
        json.dumps(reading(0, 4, parts_produced=-1)),
        json.dumps(reading(0, 5, parts_produced=1, parts_rejected=3)),
        json.dumps(reading(0, 6, temperature_c=900.0)),
        json.dumps(reading(0, 7, temperature_c="hot")),  # wrong type
    ]
    validated = parse_and_validate(bronze_df(spark, payloads))

    good = valid_readings(validated).collect()
    bad = {r.offset: r._errors for r in quarantined(validated, "telemetry").collect()}

    assert [r.seq for r in good] == [1]
    assert bad == {
        1: ["malformed_payload"],
        2: ["unknown_status"],
        3: ["missing_machine_id"],
        4: ["negative_count", "rejected_exceeds_produced"],
        5: ["rejected_exceeds_produced"],
        6: ["temperature_out_of_range"],
        7: ["malformed_payload"],
    }


def test_valid_readings_keep_lineage_and_drop_parser_internals(spark):
    validated = parse_and_validate(bronze_df(spark, [json.dumps(reading(0, 1))]))
    columns = valid_readings(validated).columns
    assert {"machine_id", "event_ts", "seq", "topic", "partition", "offset"} <= set(columns)
    assert "_corrupt_record" not in columns


def test_dedup_drops_redeliveries_but_keeps_distinct_readings(spark):
    payloads = [
        json.dumps(reading(0, 1)),
        json.dumps(reading(0, 1)),  # redelivery
        json.dumps(reading(0, 1, temperature_c=66.0)),  # same key, retried with new value
        json.dumps(reading(0, 2)),  # same time, next sequence number
        json.dumps(reading(0, 1, machine="L1-M02")),  # same time, other machine
    ]
    readings = valid_readings(parse_and_validate(bronze_df(spark, payloads)))

    deduped = deduplicate_readings(readings, "10 minutes").collect()

    assert sorted((r.machine_id, r.seq) for r in deduped) == [
        ("L1-M01", 1),
        ("L1-M01", 2),
        ("L1-M02", 1),
    ]


def _write_microbatch(directory, name: str, payloads: list[str], order: int) -> None:
    path = directory / f"{name}.json"
    path.write_text("\n".join(json.dumps(bronze_row(p, i)) for i, p in enumerate(payloads)))
    # The file source orders new files by modification time.
    os.utime(path, (1_000_000 + order, 1_000_000 + order))


def test_stream_drops_duplicates_and_readings_behind_the_watermark(spark, tmp_path):
    source = tmp_path / "bronze"
    source.mkdir()
    _write_microbatch(
        source, "batch1", [json.dumps(reading(m, m)) for m in (0, 10, 20, 30)], order=1
    )
    _write_microbatch(
        source,
        "batch2",
        [
            json.dumps(reading(30, 30)),  # redelivery of a reading already seen
            json.dumps(reading(25, 99)),  # late, but newer than the 10:20 watermark
        ],
        order=2,
    )
    # Spark filters late rows against the watermark of the *previous* batch, so
    # the too-late reading has to arrive at least two batches after 10:30 did.
    _write_microbatch(source, "batch3", [json.dumps(reading(5, 98))], order=3)

    stream = spark.readStream.schema(BRONZE).option("maxFilesPerTrigger", 1).json(str(source))
    query = (
        deduplicate_readings(valid_readings(parse_and_validate(stream)), "10 minutes")
        .writeStream.format("memory")
        .queryName("silver_watermark_test")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    seqs = sorted(r.seq for r in spark.table("silver_watermark_test").collect())
    assert seqs == [0, 10, 20, 30, 99]
