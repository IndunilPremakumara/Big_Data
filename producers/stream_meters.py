"""Streaming data source: simulated smart meters publishing to Kafka.

Each wall-clock tick the producer sweeps every meter in the registry and emits one
reading covering the simulated interval that just elapsed. Energy is derived from
a physical model (base load x time-of-day demand curve, PV output x solar bell
curve x cloud cover) rather than uniform noise, so downstream aggregates show a
believable overnight trough, morning shoulder and evening peak. Renewable
contribution genuinely collapses to zero at simulated night, which is what the
low-renewable alert is meant to catch.

Faults are injected on purpose (duplicates, out-of-order events, sensor dropout,
implausible spikes). The cleaning, de-duplication and watermarking logic in the
speed layer exists to handle these, and the data-quality metrics exist to count
them. Rates are configurable via .env so the demo can dial them up.

Partitioning: messages are keyed by household_id. Kafka hashes the key to a
partition, so all readings for one household land on one partition and stay in
per-household order. Per-household billing in the batch layer depends on that.
"""
from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import timedelta

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, "/opt/pipeline")

from common import metrics  # noqa: E402
from common.config import settings  # noqa: E402
from common.domain import build_registry, demand_factor, solar_factor  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402
from common.simclock import SimClock  # noqa: E402

COMPONENT = "stream-producer"
STAGE = "ingestion"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

log = get_logger(COMPONENT, STAGE)
_running = True


def _stop(signum, _frame):
    global _running
    log.info("shutdown.signal", signal=signum)
    _running = False


def ensure_topics(bootstrap: str) -> None:
    """Create the readings and DLQ topics with the configured partition count.

    Done explicitly rather than relying on auto-creation, because auto-created
    topics get a single partition, which would silently serialise the whole
    stream and make the partitioning claim in the report untrue.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = [
        NewTopic(
            settings.topic_readings,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.replication_factor,
        ),
        NewTopic(
            settings.topic_dlq,
            num_partitions=1,
            replication_factor=settings.replication_factor,
        ),
    ]
    existing = admin.list_topics(timeout=20).topics
    todo = [t for t in wanted if t.topic not in existing]
    if not todo:
        log.info("kafka.topics_exist", topics=[t.topic for t in wanted])
        return
    for topic, future in admin.create_topics(todo).items():
        try:
            future.result()
            log.info("kafka.topic_created", topic=topic, partitions=settings.topic_partitions)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                log.info("kafka.topic_exists", topic=topic)
            else:
                log.error("kafka.topic_create_failed", topic=topic, error=str(exc))
                raise


def wait_for_kafka(bootstrap: str, attempts: int = 30, delay: float = 3.0) -> None:
    """Block until the broker answers metadata requests.

    Compose depends_on only waits for the container, not for the broker to finish
    KRaft startup, so the producer must tolerate a broker process that is up but
    not yet listening.
    """
    for attempt in range(1, attempts + 1):
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            log.info("kafka.ready", bootstrap=bootstrap, attempt=attempt)
            return
        except Exception as exc:
            log.warning(
                "kafka.not_ready", bootstrap=bootstrap, attempt=attempt, error=str(exc)[:160]
            )
            time.sleep(delay)
    raise RuntimeError(f"Kafka not reachable at {bootstrap} after {attempts} attempts")


def _delivery_report(err, msg):
    """Kafka delivery callback: the only place a failed send becomes visible."""
    if err is not None:
        metrics.ERRORS.labels(COMPONENT, STAGE, "delivery_failed").inc()
        log.error("kafka.delivery_failed", error=str(err))
    else:
        metrics.RECORDS.labels(COMPONENT, STAGE, "produced").inc()
        metrics.BYTES_PRODUCED.labels(COMPONENT, STAGE).inc(len(msg))
        metrics.LAST_RECORD_TS.labels(COMPONENT, STAGE).set(time.time())


def build_reading(hh, clock, cloud_by_zone, rng, sim_interval_hours):
    """Produce one physically plausible meter reading, with optional faults."""
    sim_now = clock.now()
    hour = sim_now.hour + sim_now.minute / 60.0

    # Consumption over the simulated interval that just elapsed.
    kw = hh.base_load_kw * demand_factor(hour) * rng.uniform(0.85, 1.15)
    consumption = kw * sim_interval_hours

    # PV output for the same interval, attenuated by that zone's cloud cover.
    cloud = cloud_by_zone.get(hh.grid_zone, 30.0)
    generation = hh.solar_capacity_kw * solar_factor(hour, cloud) * sim_interval_hours
    generation *= rng.uniform(0.9, 1.1)

    voltage = rng.gauss(230.0, 2.5)
    quality_flags = []

    # ---- fault injection -------------------------------------------------
    if rng.random() < settings.anomaly_rate:
        # Implausible spike: a stuck register or a genuine surge. The speed layer
        # must not let this poison the zone aggregate unchallenged.
        consumption *= rng.uniform(8.0, 20.0)
        quality_flags.append("SPIKE")
    if rng.random() < settings.null_field_rate:
        # Sensor dropout: the inverter stops reporting while the meter keeps going.
        generation = None
        quality_flags.append("NULL_SOLAR")
    if rng.random() < settings.anomaly_rate / 2:
        voltage = rng.choice([0.0, rng.uniform(180.0, 195.0), rng.uniform(260.0, 275.0)])
        quality_flags.append("VOLTAGE_OUT_OF_RANGE")

    event_ts = sim_now
    if rng.random() < settings.late_event_rate:
        # Backdated event: a meter that buffered while offline, then flushed.
        event_ts = sim_now - timedelta(seconds=rng.uniform(60, 900))
        quality_flags.append("LATE")

    return {
        "event_id": uuid.uuid4().hex,
        "meter_id": hh.meter_id,
        "household_id": hh.household_id,
        "grid_zone": hh.grid_zone,
        "power_consumption_kwh": round(max(0.0, consumption), 5),
        "solar_generation_kwh": None if generation is None else round(max(0.0, generation), 5),
        "voltage": round(voltage, 2),
        "timestamp": event_ts.isoformat(),   # simulated event time
        "sim_day": event_ts.date().isoformat(),
        "ingest_ts": time.time(),            # real wall clock, for lag measurement
        "quality_flags": quality_flags,
        "producer_run_id": log.run_id,
    }


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    metrics.serve_metrics(METRICS_PORT, log)
    clock = SimClock(settings.start_date, settings.sim_day_seconds)
    registry = build_registry(settings.num_households, settings.num_zones)
    rng = random.Random()

    log.info(
        "producer.starting",
        households=len(registry),
        zones=settings.num_zones,
        topic=settings.topic_readings,
        partitions=settings.topic_partitions,
        **clock.describe(),
    )

    wait_for_kafka(settings.kafka_bootstrap)
    ensure_topics(settings.kafka_bootstrap)

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": f"{COMPONENT}-{log.run_id}",
            # Durability over raw throughput: this is billing-relevant data, and
            # the Lambda master dataset is rebuilt from this topic.
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 50,
            "batch.size": 65536,
            "retries": 10,
            "retry.backoff.ms": 250,
        }
    )

    # Cloud cover drifts per zone and is regenerated each simulated day, seeded by
    # the day index so the daily weather batch file can reproduce the same values.
    cloud_by_zone = {}
    current_day_index = -1

    # Simulated hours covered by one wall-clock tick.
    sim_interval_hours = (settings.stream_interval_seconds * clock.speedup) / 3600.0

    emitted = 0
    duplicates = 0
    last_heartbeat = time.time()

    while _running:
        tick_started = time.perf_counter()
        day_index = clock.day_index()

        if day_index != current_day_index:
            current_day_index = day_index
            cloud_by_zone = {
                z: round(random.Random(f"{day_index}-{z}").uniform(5, 85), 1)
                for z in sorted({h.grid_zone for h in registry})
            }
            metrics.SIM_DAY_INDEX.labels(COMPONENT).set(day_index)
            log.info(
                "simday.rollover",
                day_index=day_index,
                sim_day=clock.sim_day().isoformat(),
                cloud_cover_by_zone=cloud_by_zone,
            )

        with metrics.LATENCY.labels(COMPONENT, STAGE, "sweep").time():
            for hh in registry:
                reading = build_reading(hh, clock, cloud_by_zone, rng, sim_interval_hours)
                payload = json.dumps(reading).encode()
                try:
                    producer.produce(
                        settings.topic_readings,
                        key=hh.household_id.encode(),  # key drives partition affinity
                        value=payload,
                        on_delivery=_delivery_report,
                    )
                    emitted += 1

                    # Duplicate delivery: at-least-once transport replaying a
                    # message. Same event_id, so the speed layer can collapse it.
                    if rng.random() < settings.duplicate_rate:
                        producer.produce(
                            settings.topic_readings,
                            key=hh.household_id.encode(),
                            value=payload,
                            on_delivery=_delivery_report,
                        )
                        duplicates += 1
                except BufferError:
                    # Local queue full: back-pressure from a slow or stalled broker.
                    metrics.ERRORS.labels(COMPONENT, STAGE, "local_queue_full").inc()
                    log.warning("kafka.local_queue_full", queued=len(producer))
                    producer.flush(5)
                producer.poll(0)

        if time.time() - last_heartbeat >= 15:
            producer.flush(10)
            log.info(
                "producer.heartbeat",
                emitted=emitted,
                duplicates_injected=duplicates,
                queued=len(producer),
                sim_day=clock.sim_day().isoformat(),
                sim_now=clock.now().isoformat(),
                day_index=day_index,
            )
            last_heartbeat = time.time()

        # Hold the configured wall-clock cadence regardless of sweep cost.
        sleep_for = settings.stream_interval_seconds - (time.perf_counter() - tick_started)
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            metrics.ERRORS.labels(COMPONENT, STAGE, "sweep_overrun").inc()
            log.warning("producer.sweep_overrun", overrun_s=round(-sleep_for, 3))

    log.info("producer.draining", queued=len(producer))
    producer.flush(30)
    log.info("producer.stopped", emitted=emitted, duplicates_injected=duplicates)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        log.exception("producer.fatal")
        metrics.ERRORS.labels(COMPONENT, STAGE, "fatal").inc()
        sys.exit(1)
