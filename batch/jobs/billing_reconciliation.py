"""Batch layer: authoritative daily billing reconciliation.

Orchestrated by Airflow once per simulated day. Recomputes one whole simulated
day from scratch:

    Parquet master dataset (MinIO)          <- streaming source, immutable
      x  tariffs.csv for that day (MinIO)   <- daily-batch source
      x  weather.csv for that day (MinIO)   <- daily-batch source
      x  dim_household (PostgreSQL)         <- reference dimension
      -> batch_household_bill, batch_zone_daily (PostgreSQL)

Why this job exists at all
--------------------------
The speed layer already knows roughly what each household consumed. It cannot
produce a *bill*, because:

  * billing needs the tariff file, which does not exist until end of day;
  * billing must be exact -- duplicates de-duplicated globally, not per
    micro-batch, and late events fully accounted for, not just those inside the
    watermark;
  * billing must be reproducible and auditable months later, and correctable if
    a tariff is restated.

Re-running this job for a past simulated day overwrites that day's rows with
corrected values. That recomputability is the property Lambda buys and the reason
this pipeline is not Kappa.

The job is written entirely in DataFrame/SQL operations with no Python UDFs, so
executors never need the project source on their classpath.
"""
from __future__ import annotations

import argparse
import math
import sys
import time
import uuid
from datetime import date, datetime

sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import DataFrame, SparkSession, functions as F, Window  # noqa: E402

from common import metrics  # noqa: E402
from common.billing import SUBSIDY_RATE, TIER_BLOCKS  # noqa: E402
from common.config import settings  # noqa: E402
from common.db import connect, finish_run, record_dq, start_run, upsert  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402

COMPONENT = "batch-layer"
STAGE = "batch"
JOB_NAME = "billing_reconciliation"

log = get_logger(COMPONENT, STAGE)

# Same plausibility ceiling the speed layer uses. Kept explicit rather than
# imported into a UDF so the rule is visible in the physical plan.
MAX_PLAUSIBLE_KWH = 12.0

# The tiered block schedule and subsidy rate are defined once in common.billing,
# alongside a plain-Python reference implementation of the same rules. The Spark
# expressions below are a translation of that reference; tests/test_billing.py
# asserts the two agree so this translation cannot drift.

# Integral of the half-sine solar bell over a 12-hour daylight window, per kW of
# installed capacity: 12h * 2/pi. Used to score actual output against clear-sky
# potential so the weather forecast can be evaluated.
CLEAR_SKY_KWH_PER_KW = 12.0 * 2.0 / math.pi


def build_spark(app_suffix: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"smartgrid-batch-{app_suffix}")
        .master(settings.spark_master_url)
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def read_master_dataset(spark: SparkSession, sim_day: date) -> DataFrame:
    """Read one simulated day from the immutable Parquet master dataset.

    Reads the partition directory directly rather than scanning the whole dataset
    with a filter, so partition pruning is guaranteed rather than hoped for.
    """
    path = settings.s3a("raw", "meter_readings", f"sim_date={sim_day.isoformat()}")
    df = spark.read.parquet(path)
    # sim_date is the partition column and is therefore absent from the files
    # when a single partition is read directly; reattach it.
    return df.withColumn("sim_date", F.lit(sim_day))


def read_tariffs(spark: SparkSession, sim_day: date) -> DataFrame:
    path = settings.s3a("landing", "tariffs", f"sim_date={sim_day.isoformat()}", "tariffs.csv")
    return (
        spark.read.option("header", "true").option("inferSchema", "false").csv(path)
        .select(
            F.col("household_id"),
            F.col("tariff_rate").cast("double"),
            F.col("billing_tier"),
            (F.lower(F.col("subsidy_flag")) == "true").alias("subsidy_flag"),
            F.col("standing_charge").cast("double"),
            F.col("export_rate").cast("double"),
        )
    )


def read_weather(spark: SparkSession, sim_day: date) -> DataFrame:
    path = settings.s3a("landing", "weather", f"sim_date={sim_day.isoformat()}", "weather.csv")
    return (
        spark.read.option("header", "true").csv(path)
        .select(
            F.col("grid_zone"),
            F.col("cloud_cover_pct").cast("double"),
            F.col("expected_solar_index").cast("double"),
            F.col("temperature_c").cast("double"),
        )
    )


def read_dim_household(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", "grid.dim_household")
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .select("household_id", "grid_zone", "solar_capacity_kw")
        .withColumn("solar_capacity_kw", F.col("solar_capacity_kw").cast("double"))
    )


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def clean(raw: DataFrame) -> tuple[DataFrame, dict]:
    """Global de-duplication and exact validation.

    Unlike the speed layer, this sees the entire day at once, so de-duplication on
    event_id is exhaustive and late events are simply present rather than needing
    a watermark. This is where the speed layer approximation gets corrected.
    """
    total = raw.count()

    deduped = raw.dropDuplicates(["event_id"])
    distinct = deduped.count()

    valid = deduped.filter(
        F.col("household_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("power_consumption_kwh").isNotNull()
        & (F.col("power_consumption_kwh") >= 0)
        & (F.col("power_consumption_kwh") <= MAX_PLAUSIBLE_KWH)
    ).withColumn(
        # A missing PV reading is inverter dropout, not zero sun. Filling with the
        # household median for the day is closer to the truth than zero and stops
        # dropout from understating renewable contribution in the billing report.
        "solar_kwh",
        F.coalesce(
            F.col("solar_generation_kwh"),
            F.percentile_approx("solar_generation_kwh", 0.5).over(
                Window.partitionBy("household_id")
            ),
            F.lit(0.0),
        ),
    ).withColumn("consumption_kwh", F.col("power_consumption_kwh"))

    kept = valid.count()
    stats = {
        "records_read": total,
        "duplicates_removed": total - distinct,
        "rejected": distinct - kept,
        "records_valid": kept,
    }
    return valid, stats


def household_energy(valid: DataFrame) -> DataFrame:
    """Per-household daily energy position."""
    return (
        valid.groupBy("household_id", "grid_zone")
        .agg(
            F.sum("consumption_kwh").alias("gross_consumption_kwh"),
            F.sum("solar_kwh").alias("solar_generation_kwh"),
            F.count("*").alias("reading_count"),
        )
        # Self-consumption is bounded by both what was produced and what was used;
        # only the surplus is exported and only the shortfall is imported.
        .withColumn(
            "self_consumed_kwh",
            F.least(F.col("gross_consumption_kwh"), F.col("solar_generation_kwh")),
        )
        .withColumn(
            "net_import_kwh",
            F.greatest(
                F.col("gross_consumption_kwh") - F.col("solar_generation_kwh"), F.lit(0.0)
            ),
        )
        .withColumn(
            "exported_kwh",
            F.greatest(
                F.col("solar_generation_kwh") - F.col("gross_consumption_kwh"), F.lit(0.0)
            ),
        )
        .withColumn(
            "self_sufficiency_pct",
            F.when(
                F.col("gross_consumption_kwh") > 0,
                F.col("self_consumed_kwh") / F.col("gross_consumption_kwh") * 100.0,
            ).otherwise(F.lit(0.0)),
        )
    )


def apply_tariffs(spark: SparkSession, energy: DataFrame, tariffs: DataFrame) -> DataFrame:
    """Join the daily tariff file and price each household with tiered blocks.

    This join is the whole point of the daily-batch ingestion path: consumption
    comes from the stream, price comes from the once-a-day file, and neither
    source can answer the business question alone.
    """
    blocks = spark.createDataFrame(
        TIER_BLOCKS, ["billing_tier", "block1_cap", "block2_cap", "block2_mult", "block3_mult"]
    )

    # Left join on tariffs: a household with no tariff row must be visible as a
    # reconciliation gap, not silently dropped from the bill run.
    priced = (
        energy.join(F.broadcast(tariffs), "household_id", "left")
        .join(F.broadcast(blocks), "billing_tier", "left")
    )

    net = F.col("net_import_kwh")
    b1 = F.least(net, F.col("block1_cap"))
    b2 = F.least(F.greatest(net - F.col("block1_cap"), F.lit(0.0)), F.col("block2_cap"))
    b3 = F.greatest(net - F.col("block1_cap") - F.col("block2_cap"), F.lit(0.0))

    billed_units = b1 + b2 * F.col("block2_mult") + b3 * F.col("block3_mult")

    return (
        priced.withColumn("billed_units_kwh", billed_units)
        .withColumn("energy_charge", F.col("tariff_rate") * F.col("billed_units_kwh"))
        .withColumn("export_credit", F.col("export_rate") * F.col("exported_kwh"))
        .withColumn(
            "subsidy_amount",
            F.when(F.col("subsidy_flag"), F.col("energy_charge") * F.lit(SUBSIDY_RATE))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "total_bill",
            F.col("energy_charge")
            - F.col("export_credit")
            + F.col("standing_charge")
            - F.col("subsidy_amount"),
        )
    )


def zone_rollup(
    valid: DataFrame, bills: DataFrame, weather: DataFrame, dim: DataFrame
) -> DataFrame:
    """Zone-level daily summary including solar forecast accuracy."""
    # Peak hour: the simulated hour with the highest zone load.
    hourly = (
        valid.withColumn("hour", F.hour("event_time"))
        .groupBy("grid_zone", "hour")
        .agg(F.sum("consumption_kwh").alias("hour_load"))
    )
    peak = (
        hourly.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("grid_zone").orderBy(F.col("hour_load").desc())
            ),
        )
        .filter("rn = 1")
        .select(
            "grid_zone",
            F.col("hour").alias("peak_hour"),
            F.col("hour_load").alias("peak_load_kwh"),
        )
    )

    # Clear-sky potential for the zone, from installed PV capacity.
    potential = (
        dim.groupBy("grid_zone")
        .agg(F.sum("solar_capacity_kw").alias("zone_capacity_kw"))
        .withColumn("clear_sky_kwh", F.col("zone_capacity_kw") * F.lit(CLEAR_SKY_KWH_PER_KW))
    )

    totals = bills.groupBy("grid_zone").agg(
        F.countDistinct("household_id").alias("households"),
        F.sum("gross_consumption_kwh").alias("total_consumption_kwh"),
        F.sum("solar_generation_kwh").alias("total_solar_kwh"),
        F.sum("net_import_kwh").alias("net_import_kwh"),
        F.sum("total_bill").alias("total_revenue"),
    )

    return (
        totals.join(peak, "grid_zone", "left")
        .join(F.broadcast(potential), "grid_zone", "left")
        .join(F.broadcast(weather), "grid_zone", "left")
        .withColumn(
            "renewable_contribution_pct",
            F.when(
                F.col("total_consumption_kwh") > 0,
                F.col("total_solar_kwh") / F.col("total_consumption_kwh") * 100.0,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "actual_solar_index",
            F.when(
                F.col("clear_sky_kwh") > 0, F.col("total_solar_kwh") / F.col("clear_sky_kwh")
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "solar_forecast_error_pct",
            F.when(
                F.col("actual_solar_index") > 0,
                (F.col("expected_solar_index") - F.col("actual_solar_index"))
                / F.col("actual_solar_index") * 100.0,
            ).otherwise(F.lit(None).cast("double")),
        )
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

BILL_COLUMNS = [
    "sim_day", "household_id", "grid_zone", "billing_tier", "subsidy_flag",
    "gross_consumption_kwh", "solar_generation_kwh", "self_consumed_kwh",
    "net_import_kwh", "exported_kwh", "tariff_rate", "export_rate",
    "energy_charge", "export_credit", "standing_charge", "subsidy_amount",
    "total_bill", "self_sufficiency_pct", "reading_count",
    "rejected_reading_count", "batch_run_id",
]

ZONE_COLUMNS = [
    "sim_day", "grid_zone", "households", "total_consumption_kwh",
    "total_solar_kwh", "net_import_kwh", "renewable_contribution_pct",
    "peak_hour", "peak_load_kwh", "forecast_solar_index", "actual_solar_index",
    "solar_forecast_error_pct", "total_revenue", "batch_run_id",
]


def _n(value, digits=5):
    """Round a possibly-null numeric coming back from Spark."""
    return None if value is None else round(float(value), digits)


def write_results(bills_rows, zone_rows, sim_day: date, run_id: str) -> tuple[int, int]:
    with connect() as conn:
        b = upsert(conn, "batch_household_bill", BILL_COLUMNS, bills_rows,
                   ["sim_day", "household_id"])
        z = upsert(conn, "batch_zone_daily", ZONE_COLUMNS, zone_rows,
                   ["sim_day", "grid_zone"])
    return b, z


# ---------------------------------------------------------------------------

def run(sim_day: date, run_id: str) -> dict:
    started = time.perf_counter()
    spark = build_spark(sim_day.isoformat())
    spark.sparkContext.setLogLevel("WARN")

    with connect() as conn:
        start_run(conn, run_id, "batch", JOB_NAME, sim_day)

    try:
        with log.timed("batch.extract", sim_day=sim_day.isoformat()):
            raw = read_master_dataset(spark, sim_day)
            tariffs = read_tariffs(spark, sim_day)
            weather = read_weather(spark, sim_day)
            dim = read_dim_household(spark)

        with log.timed("batch.clean", sim_day=sim_day.isoformat()):
            valid, stats = clean(raw)
            valid.cache()
            log.info("batch.clean_stats", sim_day=sim_day.isoformat(), **stats)

        if stats["records_valid"] == 0:
            raise RuntimeError(f"No valid readings for {sim_day}; refusing to write a bill run")

        with log.timed("batch.transform", sim_day=sim_day.isoformat()):
            energy = household_energy(valid)
            bills = apply_tariffs(spark, energy, tariffs).cache()

            missing_tariff = bills.filter(F.col("tariff_rate").isNull()).count()
            billed = bills.count()

            zones = zone_rollup(valid, bills, weather, dim)

            bill_rows = [
                (
                    sim_day, r["household_id"], r["grid_zone"], r["billing_tier"],
                    bool(r["subsidy_flag"]) if r["subsidy_flag"] is not None else False,
                    _n(r["gross_consumption_kwh"]), _n(r["solar_generation_kwh"]),
                    _n(r["self_consumed_kwh"]), _n(r["net_import_kwh"]),
                    _n(r["exported_kwh"]), _n(r["tariff_rate"], 4), _n(r["export_rate"], 4),
                    _n(r["energy_charge"], 4), _n(r["export_credit"], 4),
                    _n(r["standing_charge"], 4), _n(r["subsidy_amount"], 4),
                    _n(r["total_bill"], 4), _n(r["self_sufficiency_pct"], 2),
                    int(r["reading_count"] or 0), 0, run_id,
                )
                # Households with no tariff row cannot be priced; they are counted
                # as a DQ failure above and excluded from the bill run.
                for r in bills.filter(F.col("tariff_rate").isNotNull()).collect()
            ]
            zone_rows = [
                (
                    sim_day, r["grid_zone"], int(r["households"] or 0),
                    _n(r["total_consumption_kwh"]), _n(r["total_solar_kwh"]),
                    _n(r["net_import_kwh"]), _n(r["renewable_contribution_pct"], 2),
                    int(r["peak_hour"]) if r["peak_hour"] is not None else None,
                    _n(r["peak_load_kwh"]), _n(r["expected_solar_index"], 4),
                    _n(r["actual_solar_index"], 4), _n(r["solar_forecast_error_pct"], 2),
                    _n(r["total_revenue"], 4), run_id,
                )
                for r in zones.collect()
            ]

        with log.timed("batch.load", sim_day=sim_day.isoformat()):
            written_bills, written_zones = write_results(bill_rows, zone_rows, sim_day, run_id)

        duration = time.perf_counter() - started
        total_revenue = sum(r[12] or 0 for r in zone_rows)

        with connect() as conn:
            record_dq(conn, run_id, JOB_NAME, STAGE, "deduplication",
                      passed=stats["records_read"] - stats["duplicates_removed"],
                      failed=stats["duplicates_removed"], sim_day=sim_day)
            record_dq(conn, run_id, JOB_NAME, STAGE, "plausibility",
                      passed=stats["records_valid"], failed=stats["rejected"], sim_day=sim_day)
            record_dq(conn, run_id, JOB_NAME, STAGE, "tariff_coverage",
                      passed=billed - missing_tariff, failed=missing_tariff, sim_day=sim_day)
            finish_run(conn, run_id, "SUCCESS",
                       records_in=stats["records_read"], records_out=written_bills,
                       duration_s=round(duration, 3))

        metrics.BATCH_RUN.labels(JOB_NAME, "SUCCESS").inc()
        metrics.BATCH_DURATION.labels(JOB_NAME).set(duration)
        if stats["records_read"]:
            metrics.DQ_SCORE.labels(JOB_NAME, "plausibility").set(
                stats["records_valid"] / stats["records_read"]
            )

        summary = {
            "sim_day": sim_day.isoformat(),
            "run_id": run_id,
            "bills_written": written_bills,
            "zones_written": written_zones,
            "missing_tariff_households": missing_tariff,
            "total_revenue": round(total_revenue, 2),
            "duration_s": round(duration, 2),
            **stats,
        }
        log.info("batch.completed", **summary)
        return summary

    except Exception as exc:
        with connect() as conn:
            finish_run(conn, run_id, "FAILED",
                       duration_s=round(time.perf_counter() - started, 3), error=str(exc)[:2000])
        metrics.BATCH_RUN.labels(JOB_NAME, "FAILED").inc()
        log.exception("batch.failed", sim_day=sim_day.isoformat())
        raise
    finally:
        spark.stop()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Daily billing reconciliation (Lambda batch layer)")
    p.add_argument("--sim-day", required=True,
                   help="Simulated day to reconcile, YYYY-MM-DD")
    p.add_argument("--run-id", default=None, help="Override the generated run id")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    day = datetime.strptime(args.sim_day, "%Y-%m-%d").date()
    rid = args.run_id or f"batch-{day.isoformat()}-{uuid.uuid4().hex[:8]}"
    try:
        result = run(day, rid)
        print(f"BATCH_RESULT {result}")
        sys.exit(0)
    except Exception:
        log.exception("batch.fatal", sim_day=args.sim_day)
        sys.exit(1)
