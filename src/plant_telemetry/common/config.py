"""Runtime settings, read from the environment.

Every path the pipeline touches is derived from ``LAKE_ROOT`` so the same code
runs against a laptop folder, a Docker volume or a cloud storage prefix.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "localhost:19092"
    telemetry_topic: str = "telemetry"
    workorder_topic: str = "workorders.cdc"
    lake_root: str = "./lake"
    watermark_delay: str = "10 minutes"
    spark_master: str = "local[2]"

    @classmethod
    def from_env(cls) -> Settings:
        defaults = cls()
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
            ),
            telemetry_topic=os.getenv("TELEMETRY_TOPIC", defaults.telemetry_topic),
            workorder_topic=os.getenv("WORKORDER_TOPIC", defaults.workorder_topic),
            lake_root=os.getenv("LAKE_ROOT", defaults.lake_root),
            watermark_delay=os.getenv("WATERMARK_DELAY", defaults.watermark_delay),
            spark_master=os.getenv("SPARK_MASTER", defaults.spark_master),
        )

    def table(self, layer: str, name: str) -> str:
        """Path of a Delta table, e.g. ``table("silver", "telemetry")``."""
        return f"{self.lake_root}/{layer}/{name}"

    def checkpoint(self, query_name: str) -> str:
        """One checkpoint per streaming query; sharing one corrupts offsets."""
        return f"{self.lake_root}/_checkpoints/{query_name}"
