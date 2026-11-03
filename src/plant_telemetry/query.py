"""Print the gold tables and a short data-quality summary (``make query``)."""

from __future__ import annotations

from pyspark.sql import functions as F

from plant_telemetry.common.config import Settings
from plant_telemetry.common.spark import get_spark


def main() -> None:
    settings = Settings.from_env()
    spark = get_spark("plant-telemetry-query", settings.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    def load(layer: str, name: str):
        return spark.read.format("delta").load(settings.table(layer, name))

    print("== gold.machine_downtime_hourly ==")
    load("gold", "machine_downtime_hourly").orderBy(F.desc("downtime_minutes")).show(
        20, truncate=False
    )
    print("== gold.line_oee_daily ==")
    load("gold", "line_oee_daily").orderBy("day", "line_id").show(truncate=False)
    print("== silver.quarantine (by reason) ==")
    load("silver", "quarantine").select("source", F.explode("_errors").alias("reason")).groupBy(
        "source", "reason"
    ).count().orderBy(F.desc("count")).show(truncate=False)
    print("== silver.dim_work_order (versions per status) ==")
    load("silver", "dim_work_order").groupBy("status", "is_current", "is_deleted").count().orderBy(
        "status"
    ).show(truncate=False)
    print(
        f"bronze telemetry: {load('bronze', 'telemetry_raw').count()} rows, "
        f"silver telemetry: {load('silver', 'telemetry').count()} rows"
    )


if __name__ == "__main__":
    main()
