"""Work-order CDC to an SCD Type 2 dimension, via ``foreachBatch`` + MERGE.

Design notes
------------
* Every CDC event becomes one version row. Collapsing a micro-batch to the
  latest change per key would be simpler, but it silently loses history when
  an order is opened and closed inside one trigger interval.
* Idempotency comes from the data, not from the checkpoint: each version row
  carries the source log sequence number (``lsn``), and only events with an
  ``lsn`` above the key's current version are applied. Replaying a batch, or
  re-reading bronze from the start, therefore changes nothing.
* Changes are ordered per key by ``lsn``. The producer keys Kafka by work
  order, so a key's events stay in order; an event older than the current
  version is treated as already applied and skipped.
* Deletes are kept as a tombstone version (``is_deleted = true``) so the
  history shows when an order disappeared from the source.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from plant_telemetry.common.config import Settings
from plant_telemetry.common.schemas import (
    CDC_OPS,
    DIM_WORK_ORDER,
    JSON_OPTIONS,
    WORK_ORDER_PAYLOAD,
)
from plant_telemetry.common.tables import ensure_table
from plant_telemetry.pipelines.quarantine import Check, quarantined, valid_rows, with_errors

DIM_COLUMNS = tuple(f.name for f in DIM_WORK_ORDER.fields)


def _checks() -> tuple[Check, ...]:
    # Built on demand: Column expressions need an active SparkContext.
    return (
        ("missing_work_order_id", F.col("r.work_order_id").isNull()),
        ("missing_lsn", F.col("r.lsn").isNull()),
        ("missing_change_ts", F.col("r.change_ts").isNull()),
        ("unknown_op", ~F.coalesce(F.col("r.op").isin(*CDC_OPS), F.lit(False))),
    )


def parse_and_validate_cdc(bronze: DataFrame) -> DataFrame:
    parsed = bronze.withColumn("r", F.from_json("payload", WORK_ORDER_PAYLOAD, JSON_OPTIONS))
    return with_errors(parsed, "r", _checks())


def current_versions(dim: DataFrame) -> DataFrame:
    """The latest applied ``lsn`` per work order (tombstones included)."""
    return dim.where("is_current").select("work_order_id", F.col("lsn").alias("current_lsn"))


def _typed_null(name: str) -> Column:
    return F.lit(None).cast(DIM_WORK_ORDER[name].dataType).alias(name)


def stage_scd2(changes: DataFrame, current: DataFrame) -> DataFrame:
    """Build the MERGE source for one batch of CDC events.

    Returns two kinds of rows, told apart by ``_action``:

    * ``insert``: one per new version. ``_match_lsn`` is the row's own lsn, so
      it can never match an existing target row and is inserted.
    * ``close``: one per work order that already has a current version.
      ``_match_lsn`` points at that version, which gets its ``valid_to`` set to
      the first new change and ``is_current`` cleared.

    Pure DataFrame logic, so replay behaviour is testable without Delta.
    """
    new = (
        changes.dropDuplicates(["work_order_id", "lsn"])
        .join(current, "work_order_id", "left")
        .where(F.col("lsn") > F.coalesce("current_lsn", F.lit(-1)))
    )
    by_lsn = Window.partitionBy("work_order_id").orderBy("lsn")
    versions = new.select(
        "*",
        F.col("change_ts").alias("valid_from"),
        F.lead("change_ts").over(by_lsn).alias("valid_to"),
        F.lead("lsn").over(by_lsn).isNull().alias("is_current"),
        (F.col("op") == "d").alias("is_deleted"),
    )
    inserts = versions.select(
        *DIM_COLUMNS, F.lit("insert").alias("_action"), F.col("lsn").alias("_match_lsn")
    )
    closes = (
        versions.where(F.col("current_lsn").isNotNull())
        .groupBy("work_order_id", "current_lsn")
        .agg(F.min("valid_from").alias("valid_to"))
        .select(
            *[
                F.col(c) if c in ("work_order_id", "valid_to") else _typed_null(c)
                for c in DIM_COLUMNS
            ],
            F.lit("close").alias("_action"),
            F.col("current_lsn").alias("_match_lsn"),
        )
    )
    return inserts.unionByName(closes)


def apply_cdc(spark: SparkSession, changes: DataFrame, path: str) -> None:
    """Apply validated CDC events to the SCD2 dimension at ``path`` in one MERGE."""
    target = ensure_table(spark, path, DIM_WORK_ORDER)
    staged = stage_scd2(changes, current_versions(target.toDF()))
    (
        target.alias("t")
        .merge(staged.alias("s"), "t.work_order_id = s.work_order_id AND t.lsn = s._match_lsn")
        .whenMatchedUpdate(
            condition="s._action = 'close' AND t.is_current",
            set={"valid_to": "s.valid_to", "is_current": "false"},
        )
        .whenNotMatchedInsert(
            condition="s._action = 'insert'",
            values={c: f"s.{c}" for c in DIM_COLUMNS},
        )
        .execute()
    )


def make_batch_handler(settings: Settings) -> Callable[[DataFrame, int], None]:
    dim_path = settings.table("silver", "dim_work_order")
    quarantine_path = settings.table("silver", "quarantine")

    def handle(batch: DataFrame, batch_id: int) -> None:
        validated = parse_and_validate_cdc(batch).persist()
        # txnAppId/txnVersion make the append idempotent if Spark re-runs this
        # batch after a failure between the write and the checkpoint commit.
        (
            quarantined(validated, source="workorders")
            .write.format("delta")
            .mode("append")
            .option("txnAppId", "silver_workorders_quarantine")
            .option("txnVersion", batch_id)
            .save(quarantine_path)
        )
        apply_cdc(batch.sparkSession, valid_rows(validated, "r"), dim_path)
        validated.unpersist()

    return handle


def start_silver_workorders(
    spark: SparkSession, settings: Settings, available_now: bool
) -> StreamingQuery:
    writer = (
        spark.readStream.format("delta")
        .load(settings.table("bronze", "workorders_cdc"))
        .writeStream.foreachBatch(make_batch_handler(settings))
        .option("checkpointLocation", settings.checkpoint("silver_workorders"))
        .queryName("silver_workorders")
    )
    writer = writer.trigger(availableNow=True) if available_now else writer
    return writer.start()
