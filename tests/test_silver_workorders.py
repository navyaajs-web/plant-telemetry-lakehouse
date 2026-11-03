import json
from datetime import datetime, timedelta

import pytest

from plant_telemetry.common.config import Settings
from plant_telemetry.common.schemas import BRONZE
from plant_telemetry.pipelines.silver_workorders import (
    apply_cdc,
    make_batch_handler,
    stage_scd2,
)

T0 = datetime(2026, 3, 2, 10, 0)
CHANGE_SCHEMA = (
    "op string, lsn long, change_ts timestamp, work_order_id string, machine_id string, "
    "status string, priority string, reason string"
)
CURRENT_SCHEMA = "work_order_id string, current_lsn long"


def change(lsn: int, minute: int, status: str | None, op: str = "u", wo: str = "WO-1") -> tuple:
    machine = None if op == "d" else "L1-M01"
    priority = None if op == "d" else "HIGH"
    return (op, lsn, T0 + timedelta(minutes=minute), wo, machine, status, priority, "tool wear")


def changes_df(spark, rows):
    return spark.createDataFrame(rows, CHANGE_SCHEMA)


def current_df(spark, rows=()):
    return spark.createDataFrame(list(rows), CURRENT_SCHEMA)


def _staged(spark, rows, current=()):
    staged = stage_scd2(changes_df(spark, rows), current_df(spark, current))
    return sorted(staged.collect(), key=lambda r: (r._action, r.lsn or 0))


def test_every_change_in_a_batch_becomes_a_version(spark):
    rows = [change(1, 0, "OPEN", op="c"), change(2, 5, "IN_PROGRESS"), change(3, 9, "CLOSED")]

    staged = _staged(spark, rows)

    assert [r._action for r in staged] == ["insert"] * 3
    assert [(r.status, r.is_current) for r in staged] == [
        ("OPEN", False),
        ("IN_PROGRESS", False),
        ("CLOSED", True),
    ]
    # valid_to of each version is valid_from of the next one: no gaps, no overlaps.
    assert [r.valid_to for r in staged[:-1]] == [r.valid_from for r in staged[1:]]
    assert staged[-1].valid_to is None


def test_redelivered_events_inside_a_batch_count_once(spark):
    rows = [change(1, 0, "OPEN", op="c"), change(1, 0, "OPEN", op="c")]
    assert len(_staged(spark, rows)) == 1


def test_replaying_an_applied_batch_stages_nothing(spark):
    rows = [change(1, 0, "OPEN", op="c"), change(2, 5, "IN_PROGRESS")]
    assert _staged(spark, rows, current=[("WO-1", 2)]) == []


def test_new_changes_close_the_current_version_and_skip_stale_ones(spark):
    rows = [change(2, 5, "IN_PROGRESS"), change(3, 9, "ON_HOLD"), change(4, 12, "CLOSED")]

    staged = _staged(spark, rows, current=[("WO-1", 2)])

    close = [r for r in staged if r._action == "close"]
    inserts = [r for r in staged if r._action == "insert"]
    assert [(r._match_lsn, r.valid_to) for r in close] == [(2, T0 + timedelta(minutes=9))]
    assert [r.lsn for r in inserts] == [3, 4]


def test_delete_becomes_a_current_tombstone(spark):
    rows = [change(1, 0, "OPEN", op="c"), change(2, 1, None, op="d")]
    tombstone = _staged(spark, rows)[-1]
    assert (tombstone.is_deleted, tombstone.is_current) == (True, True)


def _dim(spark, path):
    return sorted(
        spark.read.format("delta").load(path).collect(), key=lambda r: (r.work_order_id, r.lsn)
    )


@pytest.mark.delta
def test_merge_builds_history_across_batches(delta_spark, tmp_path):
    path = str(tmp_path / "dim_work_order")
    apply_cdc(delta_spark, changes_df(delta_spark, [change(1, 0, "OPEN", op="c")]), path)
    apply_cdc(
        delta_spark,
        changes_df(
            delta_spark,
            [
                change(2, 5, "IN_PROGRESS"),
                change(3, 9, "CLOSED"),
                change(4, 1, "OPEN", "c", "WO-2"),
            ],
        ),
        path,
    )

    dim = _dim(delta_spark, path)

    assert [(r.work_order_id, r.status, r.is_current) for r in dim] == [
        ("WO-1", "OPEN", False),
        ("WO-1", "IN_PROGRESS", False),
        ("WO-1", "CLOSED", True),
        ("WO-2", "OPEN", True),
    ]
    assert dim[0].valid_to == dim[1].valid_from == T0 + timedelta(minutes=5)


@pytest.mark.delta
def test_merge_is_idempotent_on_replay(delta_spark, tmp_path):
    path = str(tmp_path / "dim_work_order")
    batch = changes_df(
        delta_spark,
        [change(1, 0, "OPEN", op="c"), change(2, 5, "IN_PROGRESS"), change(3, 7, None, op="d")],
    )
    apply_cdc(delta_spark, batch, path)
    first = _dim(delta_spark, path)

    apply_cdc(delta_spark, batch, path)
    apply_cdc(delta_spark, batch.limit(1), path)

    assert _dim(delta_spark, path) == first
    assert len(first) == 3


@pytest.mark.delta
def test_batch_handler_quarantines_bad_events_once_per_batch(delta_spark, tmp_path):
    settings = Settings(lake_root=str(tmp_path))
    handler = make_batch_handler(settings)
    good = {
        "op": "c",
        "lsn": 1,
        "change_ts": "2026-03-02T10:00:00.000Z",
        "work_order_id": "WO-1",
        "machine_id": "L1-M01",
        "status": "OPEN",
        "priority": "LOW",
        "reason": "planned",
    }
    bad = {**good, "lsn": 2, "op": "x"}
    batch = delta_spark.createDataFrame(
        [
            ("WO-1", json.dumps(p), "workorders.cdc", 0, i, T0, T0)
            for i, p in enumerate([good, bad])
        ],
        BRONZE,
    )

    handler(batch, 0)
    handler(batch, 0)  # Spark re-running the same batch after a failure

    quarantine = delta_spark.read.format("delta").load(settings.table("silver", "quarantine"))
    assert [(r.source, r._errors) for r in quarantine.collect()] == [("workorders", ["unknown_op"])]
    assert len(_dim(delta_spark, settings.table("silver", "dim_work_order"))) == 1
