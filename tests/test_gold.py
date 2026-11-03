from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from plant_telemetry.common.schemas import DIM_WORK_ORDER
from plant_telemetry.pipelines.gold import line_oee_daily, machine_downtime_hourly, with_durations

T0 = datetime(2026, 3, 2, 10, 0)
TELEMETRY_SCHEMA = (
    "plant_id string, line_id string, machine_id string, event_ts timestamp, seq long, "
    "status string, parts_produced int, parts_rejected int"
)


def telemetry(spark, statuses: str, machine: str = "L1-M01", step_s: int = 10, parts: int = 0):
    """One reading per character: R=RUNNING, I=IDLE, D=DOWN."""
    names = {"R": "RUNNING", "I": "IDLE", "D": "DOWN"}
    rows = [
        (
            "NW-P1",
            "L1",
            machine,
            T0 + timedelta(seconds=i * step_s),
            i,
            names[s],
            parts if s == "R" else 0,
            0,
        )
        for i, s in enumerate(statuses)
    ]
    return spark.createDataFrame(rows, TELEMETRY_SCHEMA)


def work_order(wo, status, start_s, end_s=None, deleted=False, machine="L1-M01"):
    end = None if end_s is None else T0 + timedelta(seconds=end_s)
    return (
        wo,
        machine,
        status,
        "HIGH",
        "x",
        1,
        "u",
        T0 + timedelta(seconds=start_s),
        end,
        end is None,
        deleted,
    )


def test_durations_use_next_reading_and_cap_gateway_gaps(spark):
    df = telemetry(spark, "RRR", step_s=300)
    assert [r.duration_s for r in with_durations(df).orderBy("seq").collect()] == [
        60.0,
        60.0,
        10.0,  # last reading: nominal interval
    ]


def test_downtime_minutes_with_open_work_orders(spark):
    df = telemetry(spark, "RRDDDRR")  # DOWN from 10:00:20 for 30 seconds
    dim = spark.createDataFrame(
        [
            work_order("WO-1", "IN_PROGRESS", start_s=20),
            work_order("WO-0", "CLOSED", start_s=0, end_s=None),  # closed, not open
            work_order("WO-9", "OPEN", start_s=0, deleted=True),  # deleted in source
            work_order("WO-7", "OPEN", start_s=0, machine="L2-M01"),  # other machine
        ],
        DIM_WORK_ORDER,
    )

    rows = machine_downtime_hourly(df, dim).collect()

    assert len(rows) == 1
    assert (rows[0].machine_id, rows[0].downtime_minutes, rows[0].down_readings) == (
        "L1-M01",
        0.5,
        3,
    )
    assert rows[0].work_order_ids == ["WO-1"]


def test_downtime_without_work_order_has_empty_context(spark):
    dim = spark.createDataFrame([], DIM_WORK_ORDER)
    row = machine_downtime_hourly(telemetry(spark, "D"), dim).first()
    assert row.work_order_ids == []


def test_line_oee(spark):
    # 6 running readings x 2 parts, 2 idle, 2 down, 10 s each: 100 s planned, 60 s running.
    df = telemetry(spark, "RRRRRRIIDD", parts=2).withColumn("parts_rejected", F.lit(0))
    df = df.union(
        telemetry(spark, "R", machine="L1-M02", parts=0)
        .withColumn("parts_produced", F.lit(1))
        .withColumn("parts_rejected", F.lit(1))
    )

    row = line_oee_daily(df).first()

    # 13 parts, 1 rejected; 70 s run of 110 s planned; 13 * 4 s ideal / 70 s.
    assert row.availability == pytest.approx(70 / 110, abs=1e-4)
    assert row.performance == pytest.approx(52 / 70, abs=1e-4)
    assert row.quality == pytest.approx(12 / 13, abs=1e-4)
    assert row.oee == pytest.approx((70 / 110) * (52 / 70) * (12 / 13), abs=1e-3)
