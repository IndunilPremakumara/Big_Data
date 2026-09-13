"""Speed layer: Spark Structured Streaming over the Kafka meter stream.

Responsibilities, in Lambda terms
---------------------------------
1. Append every parsed reading, untouched, to the immutable master dataset on
   object storage (Parquet on MinIO, partitioned by simulated day). Rejected rows
   are kept too, tagged with why they failed. The batch layer recomputes from
   this, so nothing may be silently dropped at ingest -- that is the property
   that makes a Lambda batch recomputation able to *correct* the speed layer.
2. Maintain low-latency approximate views for the dashboard: windowed zone load
   and running per-household totals.
3. Raise business alerts on threshold breaches (low renewable contribution, zone
   overload, consumption spikes, silent meters).
4. Route unparseable payloads to a dead-letter topic.

Time model
----------
Windows are over *simulated* event time, which advances 288x wall clock. A
one-simulated-hour window therefore closes roughly every 12.5 wall seconds, which
is a sensible dashboard refresh rate and yields 24 windows per simulated day.

A deliberate design choice
--------------------------
The windowed aggregation runs in `update` output mode so the dashboard shows the
in-progress window rather than only the last closed one. Spark does not allow a
streaming de-duplication operator to be chained ahead of an aggregation in update
mode, so duplicates are collapsed per micro-batch inside foreachBatch instead.
A micro-batch spans ~2880 simulated seconds, far wider than the gap between a
duplicate pair, so this catches them in practice. Any duplicate that straddles a
micro-batch boundary survives into the speed view and is corrected by the batch
layer, which de-duplicates globally on event_id. That is precisely the
speed-layer-approximate / batch-layer-exact division Lambda is built around.
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType, DoubleType, StringType, StructField, StructType,
)

from common import metrics  # noqa: E402
from common.config import settings  # noqa: E402
from common.db import connect, insert, seed_dim_household, start_run, upsert  # noqa: E402
from common.domain import build_registry  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402

COMPONENT = "speed-layer"
STAGE = "speed"
METRICS_PORT = int(os.getenv("METRICS_PORT", "9103"))

log = get_logger(COMPONENT, STAGE)

# Physical plausibility bounds. A domestic meter reporting outside these is a
# fault, not a customer behaviour change.
MAX_PLAUSIBLE_KWH = 12.0     # per reading interval (~9.6 simulated minutes)
MIN_VOLTAGE, MAX_VOLTAGE = 200.0, 255.0

READING_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("meter_id", StringType(), False),
    StructField("household_id", StringType(), False),
    StructField("grid_zone", StringType(), False),
    StructField("power_consumption_kwh", DoubleType(), True),
    StructField("solar_generation_kwh", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sim_day", StringType(), True),
    StructField("ingest_ts", DoubleType(), True),
    StructField("quality_flags", ArrayType(StringType()), True),
    StructField("producer_run_id", StringType(), True),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    """Session configured for Kafka input and S3A (MinIO) output."""
    builder = (
        SparkSession.builder.appName("smartgrid-speed-layer")
        .master(settings.spark_master_url)
        # MinIO speaks S3 but needs path-style addressing and no AWS credential chain.
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
        # Small cluster: keep shuffle partitions low or every micro-batch pays
        # scheduling cost for 200 mostly-empty tasks.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Parse and validate
# ---------------------------------------------------------------------------

def read_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.topic_readings)
        # Fresh runs start at the earliest retained offset so a restart replays
        # the topic rather than silently skipping data. The checkpoint takes over
        # on subsequent restarts.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )


def parse(raw: DataFrame) -> DataFrame:
    """Decode JSON payloads and attach Kafka metadata plus a validation verdict."""
    parsed = (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("payload"),
        )
        .withColumn("data", F.from_json("payload", READING_SCHEMA))
    )

    flat = parsed.select(
        "kafka_partition", "kafka_offset", "kafka_ts", "payload",
        F.col("data.*"),
    ).withColumn("event_time", F.to_timestamp("timestamp"))

    # A single expression describing *why* a row is unusable, rather than a bare
    # boolean, so rejects can be counted and explained per reason.
    reason = (
        F.when(F.col("event_id").isNull(), F.lit("UNPARSEABLE"))
        .when(F.col("event_time").isNull(), F.lit("MISSING_EVENT_TIME"))
        .when(F.col("household_id").isNull(), F.lit("MISSING_HOUSEHOLD"))
        .when(F.col("power_consumption_kwh").isNull(), F.lit("MISSING_CONSUMPTION"))
        .when(F.col("power_consumption_kwh") < 0, F.lit("NEGATIVE_CONSUMPTION"))
        .when(F.col("power_consumption_kwh") > MAX_PLAUSIBLE_KWH, F.lit("IMPLAUSIBLE_CONSUMPTION"))
        .otherwise(F.lit(None).cast("string"))
    )

    return (
        flat.withColumn("reject_reason", reason)
        .withColumn("is_valid", F.col("reject_reason").isNull())
        # Voltage out of range does not invalidate the energy reading; it is a
        # separate power-quality signal, so it is flagged rather than rejected.
        .withColumn(
            "voltage_suspect",
            F.col("voltage").isNull()
            | (F.col("voltage") < MIN_VOLTAGE)
            | (F.col("voltage") > MAX_VOLTAGE),
        )
        # Sensor dropout: treat a null PV reading as zero generation for the live
        # view, but keep the flag so the batch layer can interpolate instead.
        .withColumn("solar_kwh", F.coalesce(F.col("solar_generation_kwh"), F.lit(0.0)))
        .withColumn("solar_missing", F.col("solar_generation_kwh").isNull())
        .withColumn("consumption_kwh", F.coalesce(F.col("power_consumption_kwh"), F.lit(0.0)))
        .withColumn("sim_date", F.to_date("event_time"))
        # End-to-end pipeline latency in wall-clock seconds: produced -> processed.
        .withColumn("ingest_lag_s", F.unix_timestamp(F.current_timestamp()) - F.col("ingest_ts"))
    )


# ---------------------------------------------------------------------------
# Sink 1: immutable master dataset
# ---------------------------------------------------------------------------

def start_raw_archive(events: DataFrame):
    """Append every parsed event to Parquet on MinIO, partitioned by simulated day.

    This is the Lambda master dataset. It is append-only and includes rows the
    speed layer rejected, because the batch layer may apply different (corrected)
    validation rules when it recomputes.
    """
    target = settings.s3a("raw", "meter_readings")
    checkpoint = settings.s3a("checkpoints", "raw_archive")
    return (
        events.select(
            "event_id", "meter_id", "household_id", "grid_zone",
            "power_consumption_kwh", "solar_generation_kwh", "voltage",
            "event_time", "sim_date", "ingest_ts", "quality_flags",
            "kafka_partition", "kafka_offset", "reject_reason", "is_valid",
        )
        .writeStream.format("parquet")
        .option("path", target)
        .option("checkpointLocation", checkpoint)
        .partitionBy("sim_date")
        .outputMode("append")
        .trigger(processingTime=f"{settings.stream_trigger_seconds * 3} seconds")
        .queryName("raw_archive")
        .start()
    )


# ---------------------------------------------------------------------------
# Sink 2: windowed zone aggregates + alerts
# ---------------------------------------------------------------------------

def zone_aggregate(events: DataFrame) -> DataFrame:
    valid = events.filter("is_valid")
    return (
        valid.withWatermark("event_time", f"{settings.stream_watermark_seconds} seconds")
        .groupBy(
            F.window("event_time", f"{settings.stream_window_seconds} seconds"),
            F.col("grid_zone"),
        )
        .agg(
            F.sum("consumption_kwh").alias("total_consumption_kwh"),
            F.sum("solar_kwh").alias("total_solar_kwh"),
            F.approx_count_distinct("meter_id").alias("active_meters"),
            F.avg(F.when(~F.col("voltage_suspect"), F.col("voltage"))).alias("avg_voltage"),
            F.count("*").alias("reading_count"),
            F.collect_set("event_id").alias("event_ids"),
        )
        .select(
            F.col("grid_zone"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.to_date(F.col("window.start")).alias("sim_day"),
            "total_consumption_kwh", "total_solar_kwh",
            "active_meters", "avg_voltage", "reading_count", "event_ids",
        )
    )


def _zone_rows_from_batch(pdf) -> list[tuple]:
    """Turn a collected micro-batch into rt_zone_load rows, de-duplicating first."""
    rows = []
    for r in pdf:
        # Per-micro-batch de-duplication: collect_set already collapsed identical
        # event_ids, so the distinct count is the true reading count.
        distinct_readings = len(r["event_ids"]) if r["event_ids"] else r["reading_count"]
        dup_ratio = 1.0 - (distinct_readings / r["reading_count"]) if r["reading_count"] else 0.0

        consumption = float(r["total_consumption_kwh"] or 0.0)
        solar = float(r["total_solar_kwh"] or 0.0)
        # Duplicates inflate the sums; scale them back by the observed duplicate
        # ratio so the live view stays close to the eventual batch truth.
        if dup_ratio > 0:
            consumption *= 1.0 - dup_ratio
            solar *= 1.0 - dup_ratio

        net_import = max(0.0, consumption - solar)
        renewable_pct = (solar / consumption * 100.0) if consumption > 0 else 0.0
        rows.append((
            r["grid_zone"], r["window_start"], r["window_end"], r["sim_day"],
            round(consumption, 5), round(solar, 5), round(net_import, 5),
            round(min(renewable_pct, 999.99), 2),
            int(r["active_meters"] or 0),
            float(r["avg_voltage"]) if r["avg_voltage"] is not None else None,
            distinct_readings,
        ))
    return rows


def _evaluate_alerts(rows: list[tuple]) -> list[tuple]:
    """Threshold rules over the freshly written zone windows.

    Returns rt_alerts rows. Kept as a pure function so it is unit-testable
    without Spark or a database.
    """
    alerts = []
    for (zone, w_start, _w_end, sim_day, consumption, solar, _net,
         renewable_pct, active_meters, _avg_v, _count) in rows:
        if consumption > 0 and renewable_pct < settings.renewable_low_pct:
            alerts.append((
                "LOW_RENEWABLE_CONTRIBUTION", "WARNING", "zone", zone, sim_day, w_start,
                round(renewable_pct, 2), settings.renewable_low_pct,
                f"Zone {zone} renewable contribution {renewable_pct:.1f}% is below "
                f"the {settings.renewable_low_pct:.0f}% floor",
            ))
        if consumption > settings.zone_overload_kwh:
            alerts.append((
                "ZONE_OVERLOAD", "CRITICAL", "zone", zone, sim_day, w_start,
                round(consumption, 2), settings.zone_overload_kwh,
                f"Zone {zone} load {consumption:.1f} kWh exceeds "
                f"{settings.zone_overload_kwh:.0f} kWh in one window",
            ))
        if active_meters == 0:
            alerts.append((
                "ZONE_SILENT", "CRITICAL", "zone", zone, sim_day, w_start,
                0, 1, f"Zone {zone} reported no active meters in this window",
            ))
    return alerts


ZONE_COLUMNS = [
    "grid_zone", "window_start", "window_end", "sim_day",
    "total_consumption_kwh", "total_solar_kwh", "net_import_kwh",
    "renewable_pct", "active_meters", "avg_voltage", "reading_count",
]
ALERT_COLUMNS = [
    "alert_type", "severity", "entity_type", "entity_id", "sim_day",
    "window_start", "metric_value", "threshold", "message",
]


def write_zone_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: upsert zone windows, raise alerts, update gauges."""
    started = time.perf_counter()
    try:
        collected = [row.asDict() for row in batch_df.collect()]
        if not collected:
            log.debug("zone.batch_empty", batch_id=batch_id)
            return

        rows = _zone_rows_from_batch(collected)
        alerts = _evaluate_alerts(rows)

        with connect() as conn:
            written = upsert(conn, "rt_zone_load", ZONE_COLUMNS, rows,
                             ["grid_zone", "window_start"])
            if alerts:
                insert(conn, "rt_alerts", ALERT_COLUMNS, alerts)

        # Feed the business gauges Prometheus scrapes and Grafana plots.
        for r in rows:
            zone, renewable_pct, active = r[0], float(r[7]), int(r[8])
            metrics.ZONE_LOAD_KWH.labels(zone).set(float(r[4]))
            metrics.ZONE_RENEWABLE_PCT.labels(zone).set(renewable_pct)
            metrics.ACTIVE_METERS.labels(zone).set(active)
        for a in alerts:
            metrics.ALERTS_RAISED.labels(a[0], a[1]).inc()

        metrics.RECORDS.labels(COMPONENT, STAGE, "written").inc(written)
        metrics.LAST_RECORD_TS.labels(COMPONENT, STAGE).set(time.time())
        metrics.LATENCY.labels(COMPONENT, STAGE, "zone_batch").observe(
            time.perf_counter() - started
        )
        log.info(
            "zone.batch_written",
            batch_id=batch_id, windows=written, alerts=len(alerts),
            zones=sorted({r[0] for r in rows}),
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
        )
    except Exception as exc:
        metrics.ERRORS.labels(COMPONENT, STAGE, type(exc).__name__).inc()
        log.exception("zone.batch_failed", batch_id=batch_id)
        raise


# ---------------------------------------------------------------------------
# Sink 3: running per-household daily totals
# ---------------------------------------------------------------------------

def household_aggregate(events: DataFrame) -> DataFrame:
    """Per-household totals bucketed into one-simulated-day windows.

    Grouping on a 1-day event-time window (rather than the raw sim_day string)
    lets Spark evict state once the watermark passes the day, which keeps state
    bounded across a long-running simulation.
    """
    valid = events.filter("is_valid")
    return (
        valid.withWatermark("event_time", f"{settings.stream_watermark_seconds} seconds")
        .groupBy(
            F.window("event_time", "1 day"),
            F.col("household_id"),
            F.col("grid_zone"),
        )
        .agg(
            F.sum("consumption_kwh").alias("consumption_kwh"),
            F.sum("solar_kwh").alias("solar_kwh"),
            F.count("*").alias("reading_count"),
            F.max("event_time").alias("last_event_time"),
        )
        .select(
            "household_id", "grid_zone",
            F.to_date(F.col("window.start")).alias("sim_day"),
            "consumption_kwh", "solar_kwh", "reading_count", "last_event_time",
        )
    )


HOUSEHOLD_COLUMNS = [
    "household_id", "sim_day", "grid_zone", "consumption_kwh",
    "solar_kwh", "net_import_kwh", "reading_count", "last_seen",
]


def write_household_batch(batch_df: DataFrame, batch_id: int) -> None:
    started = time.perf_counter()
    try:
        collected = batch_df.collect()
        if not collected:
            return
        rows = []
        spikes = []
        for r in collected:
            consumption = float(r["consumption_kwh"] or 0.0)
            solar = float(r["solar_kwh"] or 0.0)
            rows.append((
                r["household_id"], r["sim_day"], r["grid_zone"],
                round(consumption, 5), round(solar, 5),
                round(max(0.0, consumption - solar), 5),
                int(r["reading_count"] or 0), r["last_event_time"],
            ))
            # A household whose daily total already exceeds its plausible ceiling
            # is almost certainly a faulty meter rather than a heavy user.
            readings = int(r["reading_count"] or 0)
            if readings > 0:
                mean_per_reading = consumption / readings
                if mean_per_reading > MAX_PLAUSIBLE_KWH / settings.spike_factor:
                    spikes.append((
                        "CONSUMPTION_SPIKE", "WARNING", "household", r["household_id"],
                        r["sim_day"], r["last_event_time"],
                        round(mean_per_reading, 4),
                        round(MAX_PLAUSIBLE_KWH / settings.spike_factor, 4),
                        f"Household {r['household_id']} averaging "
                        f"{mean_per_reading:.2f} kWh per reading, well above baseline",
                    ))

        with connect() as conn:
            written = upsert(conn, "rt_household_running", HOUSEHOLD_COLUMNS, rows,
                             ["household_id", "sim_day"])
            if spikes:
                insert(conn, "rt_alerts", ALERT_COLUMNS, spikes)

        for a in spikes:
            metrics.ALERTS_RAISED.labels(a[0], a[1]).inc()
        metrics.RECORDS.labels(COMPONENT, STAGE, "written").inc(written)
        metrics.LATENCY.labels(COMPONENT, STAGE, "household_batch").observe(
            time.perf_counter() - started
        )
        log.info("household.batch_written", batch_id=batch_id,
                 households=written, spike_alerts=len(spikes))
    except Exception as exc:
        metrics.ERRORS.labels(COMPONENT, STAGE, type(exc).__name__).inc()
        log.exception("household.batch_failed", batch_id=batch_id)
        raise


# ---------------------------------------------------------------------------
# Sink 4: dead-letter queue
# ---------------------------------------------------------------------------

def start_dlq(events: DataFrame):
    """Ship rejected records to a dead-letter topic with the reason attached.

    Rejects are never dropped on the floor: they are queryable in Kafka for
    diagnosis and still present in the Parquet master dataset for recomputation.
    """
    dlq = events.filter("NOT is_valid").select(
        F.col("household_id").cast("string").alias("key"),
        F.to_json(
            F.struct("event_id", "household_id", "grid_zone", "reject_reason",
                     "payload", "kafka_partition", "kafka_offset")
        ).alias("value"),
    )
    return (
        dlq.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("topic", settings.topic_dlq)
        .option("checkpointLocation", settings.s3a("checkpoints", "dlq"))
        .outputMode("append")
        .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
        .queryName("dlq")
        .start()
    )


# ---------------------------------------------------------------------------
# Query progress -> metrics
# ---------------------------------------------------------------------------

def install_progress_listener(spark: SparkSession) -> None:
    """Publish Spark's own streaming progress as Prometheus metrics.

    Spark already computes input rate, processing rate and batch duration per
    query; exporting them means throughput and backlog are visible in the same
    Grafana view as the producer and API metrics.
    """
    from pyspark.sql.streaming import StreamingQueryListener

    class ProgressListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            log.info("spark.query_started", query=event.name, query_id=str(event.id))

        def onQueryProgress(self, event):
            p = event.progress
            name = p.name or "unnamed"
            metrics.RECORDS.labels(COMPONENT, STAGE, "consumed").inc(p.numInputRows)
            metrics.LATENCY.labels(COMPONENT, STAGE, f"{name}_batch_duration").observe(
                (p.batchDuration or 0) / 1000.0
            )
            log.info(
                "spark.query_progress",
                query=name,
                batch_id=p.batchId,
                input_rows=p.numInputRows,
                input_rows_per_sec=round(p.inputRowsPerSecond or 0.0, 2),
                processed_rows_per_sec=round(p.processedRowsPerSecond or 0.0, 2),
                batch_duration_ms=p.batchDuration,
            )

        def onQueryTerminated(self, event):
            if event.exception:
                metrics.ERRORS.labels(COMPONENT, STAGE, "query_terminated").inc()
                log.error("spark.query_terminated", query_id=str(event.id),
                          error=str(event.exception))
            else:
                log.info("spark.query_terminated", query_id=str(event.id))

    spark.streams.addListener(ProgressListener())


# ---------------------------------------------------------------------------

def main() -> int:
    metrics.serve_metrics(METRICS_PORT, log)
    log.info(
        "speed_layer.starting",
        master=settings.spark_master_url,
        topic=settings.topic_readings,
        window_sim_seconds=settings.stream_window_seconds,
        watermark_sim_seconds=settings.stream_watermark_seconds,
        trigger_wall_seconds=settings.stream_trigger_seconds,
    )

    # Load reference data before processing starts, so the API can resolve zones
    # from the first request onward.
    with connect() as conn:
        seeded = seed_dim_household(conn, build_registry(settings.num_households,
                                                         settings.num_zones))
        start_run(conn, log.run_id, "speed", "speed_layer")
    log.info("dim_household.seeded", households=seeded)

    spark = build_spark()
    install_progress_listener(spark)

    events = parse(read_stream(spark))

    queries = [
        start_raw_archive(events),
        (
            zone_aggregate(events)
            .writeStream.foreachBatch(write_zone_batch)
            .outputMode("update")
            .option("checkpointLocation", settings.s3a("checkpoints", "zone_load"))
            .trigger(processingTime=f"{settings.stream_trigger_seconds} seconds")
            .queryName("zone_load")
            .start()
        ),
        (
            household_aggregate(events)
            .writeStream.foreachBatch(write_household_batch)
            .outputMode("update")
            .option("checkpointLocation", settings.s3a("checkpoints", "household_running"))
            .trigger(processingTime=f"{settings.stream_trigger_seconds * 2} seconds")
            .queryName("household_running")
            .start()
        ),
        start_dlq(events),
    ]

    log.info("speed_layer.running", queries=[q.name for q in queries])
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        log.exception("speed_layer.fatal")
        metrics.ERRORS.labels(COMPONENT, STAGE, "fatal").inc()
        sys.exit(1)
