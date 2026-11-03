"""Publish a simulated run to Kafka.

Usage: ``python -m plant_telemetry.generator.producer --minutes 10``

Records are keyed by machine (telemetry) and by work order (CDC). Keying CDC
by work order keeps every change for one order on one partition, which is the
ordering guarantee the SCD2 merge relies on.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

from plant_telemetry.common.config import Settings
from plant_telemetry.generator.faults import FaultRates, Record, to_records
from plant_telemetry.generator.simulation import SimulationConfig, plant_layout, simulate


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=int, default=10, help="event-time span to simulate")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--duplicate-rate", type=float, default=FaultRates.duplicate)
    parser.add_argument("--late-rate", type=float, default=FaultRates.late)
    parser.add_argument("--bad-rate", type=float, default=FaultRates.bad)
    return parser.parse_args()


def _send(producer: KafkaProducer, topic: str, records: list[Record]) -> None:
    for record in records:
        producer.send(topic, key=record.key.encode(), value=record.value.encode())


def main() -> None:
    args = _parse_args()
    settings = Settings.from_env()
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    config = SimulationConfig(
        start=now - timedelta(minutes=args.minutes), minutes=args.minutes, seed=args.seed
    )
    run = simulate(config, plant_layout())

    telemetry_rates = FaultRates(
        duplicate=args.duplicate_rate, late=args.late_rate, bad=args.bad_rate
    )
    # The work-order system is a database log: no late or malformed rows, but the
    # connector redelivers on restart, so duplicates still happen.
    cdc_rates = FaultRates(duplicate=args.duplicate_rate, late=0.0, bad=0.0)
    telemetry = to_records(run.telemetry, "machine_id", "event_ts", telemetry_rates, args.seed)
    cdc = to_records(run.cdc, "work_order_id", "change_ts", cdc_rates, args.seed)

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, acks="all", linger_ms=20
    )
    _send(producer, settings.telemetry_topic, telemetry)
    _send(producer, settings.workorder_topic, cdc)
    producer.flush()
    producer.close()
    print(f"sent {len(telemetry)} telemetry and {len(cdc)} work-order records")


if __name__ == "__main__":
    main()
