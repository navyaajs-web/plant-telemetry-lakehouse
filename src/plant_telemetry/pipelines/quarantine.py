"""Shared validation plumbing for silver jobs.

Both silver jobs attach an ``_errors`` array to every parsed row and send the
non-empty ones to a single quarantine table, so one place answers "what did we
reject, from where, and why".
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from plant_telemetry.common.schemas import CORRUPT_RECORD

Check = tuple[str, Column]


def with_errors(parsed: DataFrame, struct_col: str, checks: Sequence[Check]) -> DataFrame:
    """Add ``_errors``: the names of every failed check, empty when the row is valid.

    A malformed payload gets a single ``malformed_payload`` reason; running the
    field checks on a struct of NULLs would only add noise to the quarantine.
    Each predicate must be TRUE for a bad row and never NULL, otherwise a bad
    row would slip through as valid.
    """
    field_errors = F.filter(
        F.array(*[F.when(bad, F.lit(reason)) for reason, bad in checks]),
        lambda e: e.isNotNull(),
    )
    errors = F.when(
        F.col(f"{struct_col}.{CORRUPT_RECORD}").isNotNull(), F.array(F.lit("malformed_payload"))
    ).otherwise(field_errors)
    return parsed.withColumn("_errors", errors)


def valid_rows(validated: DataFrame, struct_col: str, *passthrough: str) -> DataFrame:
    return (
        validated.where(F.size("_errors") == 0)
        .select(f"{struct_col}.*", *passthrough)
        .drop(CORRUPT_RECORD)
    )


def quarantined(validated: DataFrame, source: str) -> DataFrame:
    """Rows that failed validation, in the shared quarantine schema.

    The raw payload and its Kafka coordinates are kept so a fixed parser can
    re-process exactly these messages later.
    """
    return validated.where(F.size("_errors") > 0).select(
        F.lit(source).alias("source"),
        "payload",
        "_errors",
        "topic",
        "partition",
        "offset",
        "_ingested_at",
        F.current_timestamp().alias("_quarantined_at"),
    )
