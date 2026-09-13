"""Daily-batch data source: tariff and weather files dropped once per simulated day.

This is the second ingestion path required by the assignment: a feed that arrives
once a day rather than continuously. At every simulated midnight it writes two
CSV objects into the MinIO landing zone:

  landing/tariffs/sim_date=<D>/tariffs.csv
      household_id, tariff_rate, billing_tier, subsidy_flag, standing_charge,
      export_rate            -- the commercial reference data billing needs.

  landing/weather/sim_date=<D>/weather.csv
      grid_zone, forecast_date, cloud_cover_pct, expected_solar_index,
      temperature_c          -- lets the batch layer score forecast vs actual solar.

A zero-byte ``_SUCCESS`` marker is written *after* both files close. Airflow waits
on the marker, never on the CSVs themselves, so a partially uploaded file can
never be picked up as a complete day. This is the object-store equivalent of an
atomic commit and matters because S3-style stores have no rename-as-commit.

The tariff file is generated with a per-day seed, so re-running the producer for
a given simulated day reproduces byte-identical reference data. That is what makes
a batch-layer recomputation deterministic, which is the core promise of Lambda.
"""
from __future__ import annotations

import csv
import io
import os
import random
import signal
import sys
import time
from datetime import date, timedelta

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

sys.path.insert(0, "/opt/pipeline")

from common import metrics  # noqa: E402
from common.config import settings  # noqa: E402
from common.domain import build_registry  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402
from common.simclock import SimClock  # noqa: E402

COMPONENT = "batch-producer"
STAGE = "ingestion"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

# Base price per kWh by billing tier, in local currency units.
TIER_BASE_RATE = {
    "DOMESTIC_LOW": 0.14,
    "DOMESTIC_STD": 0.21,
    "DOMESTIC_HIGH": 0.28,
    "COMMERCIAL": 0.33,
}
# Fixed daily connection fee by tier.
TIER_STANDING_CHARGE = {
    "DOMESTIC_LOW": 0.10,
    "DOMESTIC_STD": 0.18,
    "DOMESTIC_HIGH": 0.25,
    "COMMERCIAL": 0.60,
}

log = get_logger(COMPONENT, STAGE)
_running = True


def _stop(signum, _frame):
    global _running
    log.info("shutdown.signal", signal=signum)
    _running = False


def s3_client():
    """S3 client pointed at MinIO. Path-style addressing is required for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def wait_for_minio(client, attempts: int = 30, delay: float = 3.0) -> None:
    """Block until MinIO answers and the project bucket exists."""
    for attempt in range(1, attempts + 1):
        try:
            client.head_bucket(Bucket=settings.minio_bucket)
            log.info("minio.ready", bucket=settings.minio_bucket, attempt=attempt)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchBucket"):
                client.create_bucket(Bucket=settings.minio_bucket)
                log.info("minio.bucket_created", bucket=settings.minio_bucket)
                return
            log.warning("minio.not_ready", attempt=attempt, error=str(exc)[:160])
        except Exception as exc:
            log.warning("minio.not_ready", attempt=attempt, error=str(exc)[:160])
        time.sleep(delay)
    raise RuntimeError(f"MinIO not reachable at {settings.minio_endpoint}")


def _csv_bytes(header: list[str], rows: list[list]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue().encode()


def build_tariff_rows(sim_day: date, registry) -> list[list]:
    """Per-household commercial terms for one simulated day.

    Rates drift slightly day to day (wholesale market movement) but are seeded by
    the date, so the same day always regenerates the same file.
    """
    rng = random.Random(f"tariff-{sim_day.isoformat()}")
    # A single market factor moves every household's rate together, the way a
    # wholesale price change actually propagates.
    market_factor = rng.uniform(0.92, 1.12)

    rows = []
    for hh in registry:
        base = TIER_BASE_RATE[hh.billing_tier]
        rate = round(base * market_factor * rng.uniform(0.98, 1.02), 4)
        rows.append(
            [
                hh.household_id,
                rate,
                hh.billing_tier,
                # CSV carries booleans as strings; the batch job casts explicitly.
                "true" if hh.subsidy_flag else "false",
                TIER_STANDING_CHARGE[hh.billing_tier],
                # Export credit is deliberately below the import rate, which is why
                # a household with PV can still end the day with a positive bill.
                round(rate * 0.45, 4),
            ]
        )
    return rows


def build_weather_rows(sim_day: date, day_index: int, zones: list[str]) -> list[list]:
    """Zone-level forecast for the day.

    Cloud cover uses the same seed formula as the streaming producer, so forecast
    and actual solar output are correlated but not identical: the batch layer can
    compute a genuine forecast-error percentage rather than a trivial zero.
    """
    rows = []
    for zone in zones:
        actual_cloud = round(random.Random(f"{day_index}-{zone}").uniform(5, 85), 1)
        # The forecast is the truth plus forecasting error.
        noise = random.Random(f"fc-{sim_day}-{zone}").uniform(-12, 12)
        forecast_cloud = round(min(100.0, max(0.0, actual_cloud + noise)), 1)
        rows.append(
            [
                zone,
                sim_day.isoformat(),
                forecast_cloud,
                round(1.0 - 0.75 * (forecast_cloud / 100.0), 4),  # expected_solar_index
                round(random.Random(f"t-{sim_day}-{zone}").uniform(9.0, 34.0), 1),
            ]
        )
    return rows


def drop_day(client, sim_day: date, day_index: int, registry) -> dict:
    """Write both files plus the _SUCCESS marker for one simulated day."""
    zones = sorted({h.grid_zone for h in registry})
    tariff_rows = build_tariff_rows(sim_day, registry)
    weather_rows = build_weather_rows(sim_day, day_index, zones)

    prefix = f"landing/tariffs/sim_date={sim_day.isoformat()}"
    weather_prefix = f"landing/weather/sim_date={sim_day.isoformat()}"

    tariff_body = _csv_bytes(
        ["household_id", "tariff_rate", "billing_tier", "subsidy_flag",
         "standing_charge", "export_rate"],
        tariff_rows,
    )
    weather_body = _csv_bytes(
        ["grid_zone", "forecast_date", "cloud_cover_pct", "expected_solar_index",
         "temperature_c"],
        weather_rows,
    )

    with log.timed("batch.drop", sim_day=sim_day.isoformat(), day_index=day_index):
        client.put_object(
            Bucket=settings.minio_bucket,
            Key=f"{prefix}/tariffs.csv",
            Body=tariff_body,
            ContentType="text/csv",
        )
        client.put_object(
            Bucket=settings.minio_bucket,
            Key=f"{weather_prefix}/weather.csv",
            Body=weather_body,
            ContentType="text/csv",
        )
        # Commit marker written last: Airflow polls for this, never for the CSVs.
        client.put_object(Bucket=settings.minio_bucket, Key=f"{prefix}/_SUCCESS", Body=b"")

    metrics.RECORDS.labels(COMPONENT, STAGE, "produced").inc(len(tariff_rows) + len(weather_rows))
    metrics.BYTES_PRODUCED.labels(COMPONENT, STAGE).inc(len(tariff_body) + len(weather_body))
    metrics.LAST_RECORD_TS.labels(COMPONENT, STAGE).set(time.time())
    metrics.SIM_DAY_INDEX.labels(COMPONENT).set(day_index)

    summary = {
        "sim_day": sim_day.isoformat(),
        "day_index": day_index,
        "tariff_rows": len(tariff_rows),
        "weather_rows": len(weather_rows),
        "tariff_key": f"{prefix}/tariffs.csv",
        "weather_key": f"{weather_prefix}/weather.csv",
        "marker_key": f"{prefix}/_SUCCESS",
        "bytes": len(tariff_body) + len(weather_body),
    }
    log.info("batch.day_published", **summary)
    return summary


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    metrics.serve_metrics(METRICS_PORT, log)
    clock = SimClock(settings.start_date, settings.sim_day_seconds)
    registry = build_registry(settings.num_households, settings.num_zones)
    client = s3_client()

    log.info(
        "batch_producer.starting",
        households=len(registry),
        bucket=settings.minio_bucket,
        endpoint=settings.minio_endpoint,
        **clock.describe(),
    )
    wait_for_minio(client)

    # A real utility publishes yesterday's reference data, so day 0 has no
    # upstream file of its own. Seed the day before the simulation starts, which
    # gives the first batch reconciliation something to join against.
    drop_day(client, settings.start_date - timedelta(days=1), -1, registry)

    published: set[str] = set()
    while _running:
        sim_day = clock.sim_day()
        key = sim_day.isoformat()
        if key not in published:
            try:
                drop_day(client, sim_day, clock.day_index(), registry)
                published.add(key)
            except Exception as exc:
                metrics.ERRORS.labels(COMPONENT, STAGE, type(exc).__name__).inc()
                log.exception("batch.drop_failed", sim_day=key)
                time.sleep(5)
                continue

        # Sleep until just past the next simulated midnight rather than polling hot.
        wait = max(1.0, clock.seconds_until_next_day() + 0.5)
        log.debug("batch.sleeping", seconds=round(wait, 2), next_sim_day=key)
        slept = 0.0
        while slept < wait and _running:
            step = min(2.0, wait - slept)
            time.sleep(step)
            slept += step

    log.info("batch_producer.stopped", days_published=len(published))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        log.exception("batch_producer.fatal")
        metrics.ERRORS.labels(COMPONENT, STAGE, "fatal").inc()
        sys.exit(1)
