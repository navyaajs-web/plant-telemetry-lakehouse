"""Gold aggregates: hourly machine downtime and daily line OEE.

Gold is a small batch job over silver rather than another stream. Downtime per
reading depends on the *next* reading for the machine, which a streaming
aggregate cannot see without holding state for every machine. Recomputing a
short look-back window and swapping it in with ``replaceWhere`` keeps the job
incremental without that complexity.
"""

from __future__ import annotations

from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from plant_telemetry.common.config import Settings

# Ideal seconds per part. Real plants keep this per product on the routing; a
# single constant is enough for one synthetic product.
IDEAL_CYCLE_S = 4.0
NOMINAL_INTERVAL_S = 10.0
# A gap longer than this means the gateway was offline, not that the machine
# spent the whole gap in its last reported state.
MAX_GAP_S = 60.0
_OPEN_STATUSES = ("OPEN", "IN_PROGRESS", "ON_HOLD")


def with_durations(
    telemetry: DataFrame, nominal_s: float = NOMINAL_INTERVAL_S, max_gap_s: float = MAX_GAP_S
) -> DataFrame:
    """Give each reading the seconds until the machine's next reading, capped."""
    by_time = Window.partitionBy("machine_id").orderBy("event_ts", "seq")
    gap = F.lead("event_ts").over(by_time).cast("double") - F.col("event_ts").cast("double")
    return telemetry.withColumn(
        "duration_s", F.least(F.coalesce(gap, F.lit(nominal_s)), F.lit(max_gap_s))
    )


def machine_downtime_hourly(telemetry: DataFrame, dim_work_order: DataFrame) -> DataFrame:
    """Minutes DOWN per machine and hour, with the work orders open at the time.

    Downtime and work orders are aggregated separately and then joined, because
    joining readings to overlapping work orders first would double count time.
    """
    down = (
        with_durations(telemetry)
        .where(F.col("status") == "DOWN")
        .withColumn("hour", F.date_trunc("hour", "event_ts"))
    )
    downtime = down.groupBy("plant_id", "line_id", "machine_id", "hour").agg(
        F.round(F.sum("duration_s") / 60, 2).alias("downtime_minutes"),
        F.count("*").alias("down_readings"),
    )

    open_orders = dim_work_order.where(
        ~F.col("is_deleted") & F.col("status").isin(*_OPEN_STATUSES)
    ).select("work_order_id", F.col("machine_id").alias("wo_machine_id"), "valid_from", "valid_to")
    as_of = (
        (F.col("machine_id") == F.col("wo_machine_id"))
        & (F.col("event_ts") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("event_ts") < F.col("valid_to")))
    )
    orders = (
        down.join(open_orders, as_of)
        .groupBy("machine_id", "hour")
        .agg(F.array_sort(F.collect_set("work_order_id")).alias("work_order_ids"))
    )
    return downtime.join(orders, ["machine_id", "hour"], "left").select(
        "hour",
        "plant_id",
        "line_id",
        "machine_id",
        "downtime_minutes",
        "down_readings",
        F.coalesce("work_order_ids", F.array().cast("array<string>")).alias("work_order_ids"),
    )


def _ratio(num: Column, den: Column) -> Column:
    return F.when(den > 0, num / den)


def line_oee_daily(telemetry: DataFrame, ideal_cycle_s: float = IDEAL_CYCLE_S) -> DataFrame:
    """OEE = availability x performance x quality, per line and day.

    Planned time is all observed time (RUNNING + IDLE + DOWN): the plant runs
    around the clock, so idle time is a loss, not a planned break.
    """
    daily = (
        with_durations(telemetry)
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("plant_id", "line_id", "day")
        .agg(
            F.sum("duration_s").alias("planned_s"),
            F.sum(F.when(F.col("status") == "RUNNING", F.col("duration_s")).otherwise(0.0)).alias(
                "run_s"
            ),
            F.sum("parts_produced").alias("parts_produced"),
            F.sum("parts_rejected").alias("parts_rejected"),
        )
    )
    availability = _ratio(F.col("run_s"), F.col("planned_s"))
    # least() skips NULLs, so guard the zero-run-time case explicitly; capping
    # at 1.0 absorbs a slightly optimistic ideal cycle time.
    performance = F.when(
        F.col("run_s") > 0,
        F.least(F.col("parts_produced") * F.lit(ideal_cycle_s) / F.col("run_s"), F.lit(1.0)),
    )
    quality = _ratio(F.col("parts_produced") - F.col("parts_rejected"), F.col("parts_produced"))
    return daily.select(
        "day",
        "plant_id",
        "line_id",
        F.round(availability, 4).alias("availability"),
        F.round(performance, 4).alias("performance"),
        F.round(quality, 4).alias("quality"),
        F.round(availability * performance * quality, 4).alias("oee"),
        "parts_produced",
        "parts_rejected",
    )


def _replace_since(df: DataFrame, path: str, column: str, since: date) -> None:
    """Overwrite only the rows on or after ``since``; older gold rows are final."""
    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, path):
        writer = writer.option("replaceWhere", f"{column} >= '{since.isoformat()}'")
    writer.save(path)


def run_gold(spark: SparkSession, settings: Settings, today: date, lookback_days: int = 2) -> None:
    since = today - timedelta(days=lookback_days)
    telemetry = (
        spark.read.format("delta")
        .load(settings.table("silver", "telemetry"))
        .where(F.col("event_ts") >= F.lit(since).cast("timestamp"))
    )
    dim = spark.read.format("delta").load(settings.table("silver", "dim_work_order"))
    _replace_since(
        machine_downtime_hourly(telemetry, dim),
        settings.table("gold", "machine_downtime_hourly"),
        "hour",
        since,
    )
    _replace_since(
        line_oee_daily(telemetry), settings.table("gold", "line_oee_daily"), "day", since
    )
