"""Prometheus instrumentation shared across pipeline stages.

Design note
-----------
Every long-running component (both producers, the Spark streaming driver, the
serving API) exposes a ``/metrics`` endpoint that Prometheus scrapes. The metric
*names* are deliberately identical across components and distinguished by a
``stage``/``component`` label, so one Grafana panel can plot throughput for the
whole pipeline and one alert rule ("no records for N minutes") covers every
stage instead of needing one rule per service.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --- Throughput -------------------------------------------------------------
RECORDS = Counter(
    "grid_records_total",
    "Records handled, by pipeline stage and outcome.",
    ["component", "stage", "outcome"],  # outcome: produced|consumed|written|dropped|invalid
)

BYTES_PRODUCED = Counter(
    "grid_bytes_produced_total",
    "Payload bytes handed to the sink.",
    ["component", "stage"],
)

# --- Errors -----------------------------------------------------------------
ERRORS = Counter(
    "grid_errors_total",
    "Errors, by stage and error class. Drives the error-rate alert.",
    ["component", "stage", "error_type"],
)

# --- Latency ----------------------------------------------------------------
LATENCY = Histogram(
    "grid_operation_duration_seconds",
    "Duration of a pipeline operation (publish, micro-batch, query, ...).",
    ["component", "stage", "operation"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
)

# --- Liveness / freshness ---------------------------------------------------
LAST_RECORD_TS = Gauge(
    "grid_last_record_unixtime",
    "Unix timestamp of the most recent record handled. Feeds the staleness alert.",
    ["component", "stage"],
)

# --- Business-level gauges (what the operator actually cares about) ---------
ZONE_LOAD_KWH = Gauge(
    "grid_zone_load_kwh",
    "Most recent windowed load per zone, kWh.",
    ["grid_zone"],
)

ZONE_RENEWABLE_PCT = Gauge(
    "grid_zone_renewable_pct",
    "Most recent renewable (solar) contribution per zone, percent.",
    ["grid_zone"],
)

ACTIVE_METERS = Gauge(
    "grid_active_meters",
    "Distinct meters seen in the most recent window.",
    ["grid_zone"],
)

ALERTS_RAISED = Counter(
    "grid_alerts_raised_total",
    "Business alerts raised by the speed layer.",
    ["alert_type", "severity"],
)

SIM_DAY_INDEX = Gauge(
    "grid_sim_day_index",
    "Current simulated day index (0-based). Confirms the compressed clock is advancing.",
    ["component"],
)

# --- Batch layer ------------------------------------------------------------
BATCH_RUN = Counter(
    "grid_batch_runs_total",
    "Batch (Airflow-orchestrated) job executions by status.",
    ["job", "status"],
)

BATCH_DURATION = Gauge(
    "grid_batch_last_duration_seconds",
    "Duration of the most recent successful run of a batch job.",
    ["job"],
)

DQ_SCORE = Gauge(
    "grid_data_quality_ratio",
    "Ratio of records passing a named data-quality check (1.0 = all passed).",
    ["job", "check"],
)


def serve_metrics(port: int, logger=None) -> None:
    """Start the Prometheus scrape endpoint on ``port`` in a background thread."""
    start_http_server(port)
    if logger:
        logger.info("metrics.endpoint_started", port=port, path="/metrics")
