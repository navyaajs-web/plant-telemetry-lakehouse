"""One local SparkSession for the whole test run.

The session is created with Delta enabled. Delta's JVM package is resolved
from Maven on first use; if that is not possible (an offline machine), the
session falls back to plain Spark and only the tests marked ``delta`` are
skipped. CI sets ``REQUIRE_DELTA=1`` so a resolution problem fails loudly
there instead of skipping.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from plant_telemetry.common.spark import get_spark

# Spark returns naive datetimes in the driver's local zone; pin it so test
# expectations written in UTC hold on any laptop.
os.environ["TZ"] = "UTC"
time.tzset()

_delta_error: Exception | None = None


def _plain_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("plant-telemetry-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    global _delta_error
    try:
        session = get_spark("plant-telemetry-tests", "local[2]")
    except Exception as exc:  # Delta package could not be resolved
        if os.getenv("REQUIRE_DELTA") == "1":
            raise
        _delta_error = exc
        session = _plain_session()
    session.conf.set("spark.sql.shuffle.partitions", "2")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def delta_spark(spark: SparkSession) -> SparkSession:
    """The shared session, for tests that need real Delta tables."""
    if _delta_error is not None:
        pytest.skip(f"Delta Lake JVM package unavailable: {type(_delta_error).__name__}")
    return spark
