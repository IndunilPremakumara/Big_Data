"""Airflow DAG: pipeline health checks.

The assignment requires at least one alert or health-check rule. The pipeline has
three complementary layers of them, and this DAG is the third:

  1. Prometheus alert rules (observability/alert_rules.yml) watch *process*
     health -- a scrape target down, error rate climbing, throughput at zero.
  2. The speed layer raises *business* alerts into grid.rt_alerts -- a zone whose
     renewable contribution has collapsed, a meter reporting implausible load.
  3. This DAG checks *end-to-end data* health -- is the warehouse actually
     receiving fresh data, is the backlog of unreconciled days growing, are
     rejects trending up.

Layer 3 catches the failure the other two miss: every container healthy, every
process running, and yet no data arriving because a topic was renamed or a
checkpoint went stale. That is the outage that otherwise goes unnoticed until
someone asks why yesterday has no bill.

Failures here surface as failed Airflow task instances, which is the alerting
channel a real deployment would wire to email/PagerDuty via on_failure_callback.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

sys.path.insert(0, "/opt/pipeline")

# How stale the real-time tables may get before we call it an outage. The speed
# layer writes every STREAM_TRIGGER_SECONDS, so a multi-minute gap is unambiguous.
MAX_STALENESS_SECONDS = int(os.getenv("METER_SILENCE_SECONDS", "90")) * 2

# More than this many complete-but-unreconciled days means the batch layer is
# falling behind faster than it can catch up.
MAX_BACKLOG_DAYS = 3

# Reject ratio above this suggests a schema change or a broken producer.
MAX_REJECT_RATIO = 0.25


with DAG(
    dag_id="pipeline_health_check",
    description="End-to-end freshness, backlog and data-quality health checks",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["observability", "health-check"],
) as dag:

    @task(task_id="check_stream_freshness")
    def check_stream_freshness() -> dict:
        """Alert if the speed layer has stopped writing to the serving tables."""
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-health", "orchestration")

        with connect() as conn:
            rows = query(conn, "SELECT * FROM grid.v_pipeline_freshness")

        staleness = {
            r["table_name"]: (
                float(r["staleness_seconds"]) if r["staleness_seconds"] is not None else None
            )
            for r in rows
        }
        log.info("health.freshness", threshold_s=MAX_STALENESS_SECONDS, **staleness)

        # The batch table is expected to be stale between runs, so only the two
        # real-time tables are gated on freshness.
        for table in ("rt_zone_load", "rt_household_running"):
            value = staleness.get(table)
            if value is None:
                raise ValueError(
                    f"HEALTH ALERT: {table} has never been written. "
                    "The speed layer has not produced any output."
                )
            if value > MAX_STALENESS_SECONDS:
                raise ValueError(
                    f"HEALTH ALERT: no data written to {table} for {value:.0f}s "
                    f"(threshold {MAX_STALENESS_SECONDS}s). Stream ingestion may be stalled."
                )
        return staleness

    @task(task_id="check_batch_backlog")
    def check_batch_backlog() -> dict:
        """Alert if reconciled days are falling behind the days the stream produced."""
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-health", "orchestration")

        with connect() as conn:
            stream_days = query(
                conn, "SELECT count(DISTINCT sim_day) AS n FROM grid.rt_household_running"
            )[0]["n"]
            batch_days = query(
                conn, "SELECT count(DISTINCT sim_day) AS n FROM grid.batch_household_bill"
            )[0]["n"]
            failed = query(
                conn,
                """SELECT count(*) AS n FROM grid.ops_pipeline_runs
                    WHERE status = 'FAILED' AND started_at > now() - interval '15 minutes'""",
            )[0]["n"]

        # The current simulated day is always in flight and never reconcilable,
        # so one day of lag is the healthy steady state, not a backlog.
        backlog = max(0, int(stream_days or 0) - int(batch_days or 0) - 1)
        result = {
            "stream_days": int(stream_days or 0),
            "batch_days": int(batch_days or 0),
            "backlog_days": backlog,
            "recent_failed_runs": int(failed or 0),
        }
        log.info("health.backlog", threshold=MAX_BACKLOG_DAYS, **result)

        if backlog > MAX_BACKLOG_DAYS:
            raise ValueError(
                f"HEALTH ALERT: {backlog} simulated days awaiting reconciliation "
                f"(threshold {MAX_BACKLOG_DAYS}). The batch layer is not keeping up."
            )
        if result["recent_failed_runs"] >= 3:
            raise ValueError(
                f"HEALTH ALERT: {result['recent_failed_runs']} pipeline runs failed "
                "in the last 15 minutes."
            )
        return result

    @task(task_id="check_data_quality")
    def check_data_quality() -> dict:
        """Alert if the share of rejected readings has climbed abnormally."""
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-health", "orchestration")

        with connect() as conn:
            recent = query(
                conn,
                """SELECT check_name,
                          sum(passed) AS passed,
                          sum(failed) AS failed
                     FROM grid.ops_dq_metrics
                    WHERE recorded_at > now() - interval '30 minutes'
                    GROUP BY check_name""",
            )
            alerts = query(
                conn,
                """SELECT alert_type, severity, count(*) AS n
                     FROM grid.rt_alerts
                    WHERE created_at > now() - interval '10 minutes'
                    GROUP BY alert_type, severity ORDER BY n DESC""",
            )

        summary = {}
        breaches = []
        for r in recent:
            passed, failed = int(r["passed"] or 0), int(r["failed"] or 0)
            total = passed + failed
            ratio = failed / total if total else 0.0
            summary[r["check_name"]] = round(ratio, 4)
            if ratio > MAX_REJECT_RATIO:
                breaches.append(f"{r['check_name']} reject ratio {ratio:.1%}")

        log.info(
            "health.data_quality",
            reject_ratios=summary,
            business_alerts={f"{a['alert_type']}/{a['severity']}": int(a["n"]) for a in alerts},
        )

        if breaches:
            raise ValueError(
                f"HEALTH ALERT: data quality degraded -- {'; '.join(breaches)} "
                f"(threshold {MAX_REJECT_RATIO:.0%})"
            )
        return summary

    # Independent checks, run in parallel: one failing should not mask the others.
    check_stream_freshness()
    check_batch_backlog()
    check_data_quality()
