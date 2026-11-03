"""Turn clean simulated events into what a real broker delivers.

Faults are applied after simulation so the ground truth stays available for
tests: we know exactly which records are duplicates, late or broken.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class FaultRates:
    duplicate: float = 0.02
    late: float = 0.03
    bad: float = 0.01
    # Some late records land beyond a 10 minute watermark on purpose.
    max_late_minutes: int = 15


@dataclass(frozen=True)
class Record:
    key: str
    value: str
    arrival_ts: datetime


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _corrupt(event: dict[str, Any], rng: random.Random) -> str:
    """Produce one of the failure shapes seen on real plant gateways."""
    kind = rng.choice(("truncated", "bad_status", "negative_count", "missing_key"))
    if kind == "truncated":
        return json.dumps(event)[: rng.randint(5, 40)]
    broken = dict(event)
    if kind == "bad_status":
        broken["status"] = "UNKNOWN"
    elif kind == "negative_count":
        broken["parts_produced"] = -abs(broken.get("parts_produced") or 1)
    else:
        broken["machine_id"] = None
    return json.dumps(broken)


def to_records(
    events: list[dict[str, Any]],
    key_field: str,
    time_field: str,
    rates: FaultRates,
    seed: int = 11,
) -> list[Record]:
    """Serialise events, inject faults and return them in arrival order."""
    rng = random.Random(seed)
    records: list[Record] = []
    for event in events:
        key = str(event.get(key_field))
        ts = _parse_ts(event[time_field])
        arrival = ts + timedelta(milliseconds=rng.randint(0, 500))
        if rng.random() < rates.late:
            arrival = ts + timedelta(minutes=rng.uniform(1, rates.max_late_minutes))
        value = _corrupt(event, rng) if rng.random() < rates.bad else json.dumps(event)
        records.append(Record(key, value, arrival))
        if rng.random() < rates.duplicate:
            records.append(Record(key, value, arrival + timedelta(seconds=rng.uniform(1, 30))))
    return sorted(records, key=lambda r: r.arrival_ts)
