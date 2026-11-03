"""Discrete-time simulation of a small plant.

Telemetry and work-order CDC come out of the same simulation so they agree with
each other: a breakdown produces DOWN readings *and* opens a work order, and the
repair closes it. That correlation is what makes the gold join meaningful.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

BREAKDOWN_REASONS = ("spindle overheating", "coolant leak", "tool wear", "sensor fault")


@dataclass(frozen=True)
class Machine:
    plant_id: str
    line_id: str
    machine_id: str


@dataclass(frozen=True)
class SimulationConfig:
    start: datetime
    minutes: int = 10
    interval_s: int = 10
    seed: int = 7
    p_breakdown: float = 0.01
    p_repair: float = 0.10
    p_idle: float = 0.02
    p_resume: float = 0.20
    p_planned_work_order: float = 0.3
    p_cancel_work_order: float = 0.5


@dataclass
class _MachineState:
    status: str = "RUNNING"
    seq: int = 0
    work_order_id: str | None = None
    # Attributes of the open work order, repeated on every CDC event like a
    # full-row (Debezium "after" image) change feed would.
    work_order_row: dict[str, Any] = field(default_factory=dict)


@dataclass
class SimulationOutput:
    telemetry: list[dict[str, Any]] = field(default_factory=list)
    cdc: list[dict[str, Any]] = field(default_factory=list)


def plant_layout(
    plant_id: str = "NW-P1", lines: int = 3, machines_per_line: int = 4
) -> list[Machine]:
    return [
        Machine(plant_id, f"L{line}", f"L{line}-M{m:02d}")
        for line in range(1, lines + 1)
        for m in range(1, machines_per_line + 1)
    ]


def iso(ts: datetime) -> str:
    """ISO-8601 in UTC with a ``Z`` suffix, the format Spark's JSON reader expects."""
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class _WorkOrderLog:
    """Emits CDC events with a single, strictly increasing log sequence number."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._lsn = 0
        self._next_id = 1

    def new_id(self) -> str:
        wo_id = f"WO-{self._next_id:05d}"
        self._next_id += 1
        return wo_id

    def emit(self, op: str, ts: datetime, work_order_id: str, **row: Any) -> None:
        self._lsn += 1
        self.events.append(
            {
                "op": op,
                "lsn": self._lsn,
                "change_ts": iso(ts),
                "work_order_id": work_order_id,
                **row,
            }
        )


def _reading(machine: Machine, state: _MachineState, ts: datetime, rng: random.Random) -> dict:
    running = state.status == "RUNNING"
    produced = rng.randint(1, 3) if running else 0
    rejected = sum(rng.random() < 0.03 for _ in range(produced))
    base_temp = {"RUNNING": 65.0, "IDLE": 42.0, "DOWN": 31.0}[state.status]
    return {
        "plant_id": machine.plant_id,
        "line_id": machine.line_id,
        "machine_id": machine.machine_id,
        "event_ts": iso(ts),
        "seq": state.seq,
        "status": state.status,
        "temperature_c": round(rng.gauss(base_temp, 2.5), 2),
        "vibration_mm_s": round(abs(rng.gauss(2.5 if running else 0.2, 0.4)), 3),
        "parts_produced": produced,
        "parts_rejected": rejected,
    }


def simulate(config: SimulationConfig, machines: list[Machine]) -> SimulationOutput:
    """Run the simulation and return events in event-time order (no faults yet)."""
    rng = random.Random(config.seed)
    states = {m.machine_id: _MachineState() for m in machines}
    log = _WorkOrderLog()
    out = SimulationOutput()
    ticks = config.minutes * 60 // config.interval_s

    for tick in range(ticks):
        ts = config.start + timedelta(seconds=tick * config.interval_s)
        for machine in machines:
            state = states[machine.machine_id]
            _transition(machine, state, ts, rng, config, log)
            state.seq += 1
            out.telemetry.append(_reading(machine, state, ts, rng))

    out.cdc = log.events
    return out


def _transition(
    machine: Machine,
    state: _MachineState,
    ts: datetime,
    rng: random.Random,
    config: SimulationConfig,
    log: _WorkOrderLog,
) -> None:
    """Advance one machine by one tick, emitting work-order changes as a side effect."""
    roll = rng.random()
    if state.status == "RUNNING" and roll < config.p_breakdown:
        state.status = "DOWN"
        _open_work_order(machine, state, ts, log, "HIGH", rng.choice(BREAKDOWN_REASONS))
        log.emit(
            "u",
            ts + timedelta(seconds=1),
            state.work_order_id,
            status="IN_PROGRESS",
            **state.work_order_row,
        )
    elif state.status == "RUNNING" and roll < config.p_breakdown + config.p_idle:
        state.status = "IDLE"
        if rng.random() < config.p_planned_work_order:
            _open_work_order(machine, state, ts, log, "LOW", "planned maintenance")
            if rng.random() < config.p_cancel_work_order:
                # Raised by mistake and deleted in the source system.
                log.emit("d", ts + timedelta(seconds=2), state.work_order_id)
                state.work_order_id = None
    elif state.status in ("DOWN", "IDLE"):
        resume = config.p_repair if state.status == "DOWN" else config.p_resume
        if roll < resume:
            if state.work_order_id is not None:
                log.emit("u", ts, state.work_order_id, status="CLOSED", **state.work_order_row)
                state.work_order_id = None
            state.status = "RUNNING"


def _open_work_order(
    machine: Machine,
    state: _MachineState,
    ts: datetime,
    log: _WorkOrderLog,
    priority: str,
    reason: str,
) -> None:
    state.work_order_id = log.new_id()
    state.work_order_row = {
        "machine_id": machine.machine_id,
        "priority": priority,
        "reason": reason,
    }
    log.emit("c", ts, state.work_order_id, status="OPEN", **state.work_order_row)
