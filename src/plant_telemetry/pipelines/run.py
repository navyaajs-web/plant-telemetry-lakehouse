"""Run the whole pipeline.

``--mode once`` (used by ``make demo``) drains everything currently in Kafka
with ``availableNow`` triggers, layer by layer, then builds gold. ``--mode
continuous`` keeps the streams running and refreshes gold on an interval.
Both use the same checkpoints, so you can switch between them freely.
"""

from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from plant_telemetry.common.config import Settings
from plant_telemetry.common.schemas import BRONZE, DIM_WORK_ORDER
from plant_telemetry.common.spark import get_spark
from plant_telemetry.common.tables import ensure_table
from plant_telemetry.pipelines.bronze import start_bronze
from plant_telemetry.pipelines.gold import run_gold
from plant_telemetry.pipelines.silver_telemetry import start_silver_telemetry
from plant_telemetry.pipelines.silver_workorders import start_silver_workorders


def _start_bronze(spark: SparkSession, s: Settings, available_now: bool) -> list[StreamingQuery]:
    return [
        start_bronze(spark, s, s.telemetry_topic, "telemetry_raw", available_now),
        start_bronze(spark, s, s.workorder_topic, "workorders_cdc", available_now),
    ]


def _start_silver(spark: SparkSession, s: Settings, available_now: bool) -> list[StreamingQuery]:
    return [
        *start_silver_telemetry(spark, s, available_now),
        start_silver_workorders(spark, s, available_now),
    ]


def run_once(spark: SparkSession, settings: Settings) -> None:
    for start in (_start_bronze, _start_silver):
        for query in start(spark, settings, True):
            query.awaitTermination()
    run_gold(spark, settings, today=datetime.now(UTC).date())


def run_continuous(spark: SparkSession, settings: Settings, gold_every_s: int) -> None:
    queries = _start_bronze(spark, settings, False) + _start_silver(spark, settings, False)
    while all(q.isActive for q in queries):
        time.sleep(gold_every_s)
        run_gold(spark, settings, today=datetime.now(UTC).date())
    for query in queries:
        if query.exception() is not None:
            raise RuntimeError(f"stream {query.name} failed") from query.exception()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bronze, silver and gold.")
    parser.add_argument("--mode", choices=("once", "continuous"), default="once")
    parser.add_argument("--gold-every-s", type=int, default=60)
    args = parser.parse_args()

    settings = Settings.from_env()
    spark = get_spark("plant-telemetry", settings.spark_master, with_kafka=True)
    spark.sparkContext.setLogLevel("WARN")
    for name in ("telemetry_raw", "workorders_cdc"):
        ensure_table(spark, settings.table("bronze", name), BRONZE)
    ensure_table(spark, settings.table("silver", "dim_work_order"), DIM_WORK_ORDER)

    if args.mode == "once":
        run_once(spark, settings)
    else:
        run_continuous(spark, settings, args.gold_every_s)


if __name__ == "__main__":
    main()
