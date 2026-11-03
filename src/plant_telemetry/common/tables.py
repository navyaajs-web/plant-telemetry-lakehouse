"""Delta table helpers shared by the jobs."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def ensure_table(spark: SparkSession, path: str, schema: StructType) -> DeltaTable:
    """Create an empty Delta table if needed.

    A Delta streaming source cannot start on a path that does not exist yet,
    so creating tables up front lets every stream start in any order.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], schema).write.format("delta").save(path)
    return DeltaTable.forPath(spark, path)
