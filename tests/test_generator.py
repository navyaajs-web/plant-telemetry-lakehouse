import json
from datetime import UTC, datetime

from plant_telemetry.generator.faults import FaultRates, to_records
from plant_telemetry.generator.simulation import SimulationConfig, plant_layout, simulate

START = datetime(2026, 3, 2, 8, 0, tzinfo=UTC)


def _run(seed: int = 7):
    return simulate(SimulationConfig(start=START, minutes=60, seed=seed), plant_layout())


def test_simulation_is_deterministic_for_a_seed():
    assert _run().telemetry == _run().telemetry
    assert _run().cdc == _run().cdc


def test_cdc_lsn_strictly_increases_and_breakdowns_open_work_orders():
    run = _run()
    lsns = [e["lsn"] for e in run.cdc]
    assert lsns == sorted(set(lsns))
    high_priority_creates = [e for e in run.cdc if e["op"] == "c" and e["priority"] == "HIGH"]
    down_machines = {r["machine_id"] for r in run.telemetry if r["status"] == "DOWN"}
    assert high_priority_creates, "expected at least one breakdown in an hour"
    assert {e["machine_id"] for e in high_priority_creates} == down_machines


def test_faults_add_duplicates_and_reorder_late_records():
    events = _run().telemetry
    clean = to_records(events, "machine_id", "event_ts", FaultRates(0, 0, 0))
    faulty = to_records(
        events, "machine_id", "event_ts", FaultRates(duplicate=0.1, late=0.1, bad=0.0)
    )

    assert len(clean) == len(events)
    assert len(faulty) > len(events)
    arrival = [r.arrival_ts for r in faulty]
    assert arrival == sorted(arrival)
    # A late record shows up after readings that happened well after it.
    event_ts = [json.loads(r.value)["event_ts"] for r in faulty]
    newest_so_far = [max(event_ts[: i + 1]) for i in range(len(event_ts))]
    assert any(ts < newest for ts, newest in zip(event_ts, newest_so_far, strict=True))


def test_bad_records_are_injected():
    events = _run().telemetry
    records = to_records(events, "machine_id", "event_ts", FaultRates(0, 0, bad=0.2))
    clean_values = {json.dumps(e) for e in events}
    broken = [r for r in records if r.value not in clean_values]
    assert 0.1 < len(broken) / len(records) < 0.3
