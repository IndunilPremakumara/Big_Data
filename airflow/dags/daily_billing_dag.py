"""Airflow DAG: daily billing reconciliation (the Lambda batch layer).

Scheduling model
----------------
One simulated day is compressed into SIM_DAY_SECONDS of wall-clock time (5 min by
default), so this DAG runs every 5 wall minutes rather than daily.

It does not compute "which simulated day is it now" from the wall clock. Doing so
would require the DAG to know when the producers started and would silently skip
days after any outage. Instead the DAG *discovers* work:

    a simulated day is reconcilable when
        its tariff _SUCCESS marker exists              (reference data landed)
      AND the next day marker also exists              (the day is actually over)
      AND its Parquet partition exists                 (stream data landed)
      AND it has no rows in batch_household_bill yet   (not already done)

That makes the DAG idempotent, catch-up capable and restart-safe: kill the whole
stack for twenty minutes and it works through the backlog oldest-first, one day
per run, with no manual intervention.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/pipeline")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
    "depends_on_past": False,
}

SIM_DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", "300"))


def _next_day(d: str) -> str:
    """The calendar day after an ISO date string."""
    return (datetime.strptime(d, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()


with DAG(
    dag_id="daily_billing_reconciliation",
    description="Recompute the authoritative per-household bill for one simulated day",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Every 5 wall-clock minutes = once per simulated day at default compression.
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    catchup=False,
    # One reconciliation at a time: two concurrent runs would race on the same
    # sim_day rows and duplicate the Spark cluster load for no benefit.
    max_active_runs=1,
    tags=["lambda", "batch-layer", "billing"],
) as dag:

    @task(task_id="find_pending_sim_day")
    def find_pending_sim_day() -> str:
        """Pick the oldest complete, unreconciled simulated day. Skip if none."""
        import boto3
        from botocore.client import Config as BotoConfig

        from common.config import settings
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-billing", "orchestration")

        s3 = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_user,
            aws_secret_access_key=settings.minio_password,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

        def _partition_dates(prefix: str, marker: str | None = None) -> set[str]:
            """Collect sim_date=YYYY-MM-DD values under a prefix."""
            found: set[str] = set()
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=settings.minio_bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if marker and not key.endswith(marker):
                        continue
                    for part in key.split("/"):
                        if part.startswith("sim_date="):
                            found.add(part.split("=", 1)[1])
            return found

        tariff_days = _partition_dates("landing/tariffs/", marker="_SUCCESS")
        raw_days = _partition_dates("raw/meter_readings/")

        with connect() as conn:
            done = {
                r["sim_day"].isoformat()
                for r in query(conn, "SELECT DISTINCT sim_day FROM grid.batch_household_bill")
            }

        # A day is only over once the *next* day has been published upstream.
        complete = {d for d in tariff_days if _next_day(d) in tariff_days}
        candidates = sorted((complete & raw_days) - done)

        log.info(
            "orchestration.scan",
            tariff_days=len(tariff_days),
            raw_days=len(raw_days),
            already_reconciled=len(done),
            candidates=candidates[:5],
        )

        if not candidates:
            raise AirflowSkipException(
                "No complete, unreconciled simulated day available yet"
            )

        chosen = candidates[0]
        log.info("orchestration.selected_day", sim_day=chosen, backlog=len(candidates))
        return chosen

    pending_day = find_pending_sim_day()

    # Submit the batch job to the standalone Spark cluster. Client deploy mode
    # keeps the driver inside this container so the task logs carry the driver
    # output, which is what makes an Airflow failure diagnosable without SSH.
    run_reconciliation = BashOperator(
        task_id="run_billing_reconciliation",
        bash_command=(
            "set -euo pipefail; "
            "spark-submit "
            "--master ${SPARK_MASTER_URL} "
            "--deploy-mode client "
            "--name smartgrid-billing-{{ ti.xcom_pull(task_ids='find_pending_sim_day') }} "
            "--conf spark.driver.host=$(hostname -i) "
            "--conf spark.sql.shuffle.partitions=8 "
            "--conf spark.executor.memory=1g "
            "--conf spark.cores.max=2 "
            "/opt/pipeline/batch/jobs/billing_reconciliation.py "
            "--sim-day {{ ti.xcom_pull(task_ids='find_pending_sim_day') }} "
            "--run-id batch-{{ ti.xcom_pull(task_ids='find_pending_sim_day') }}-{{ run_id | replace(':','') | replace('+','') | truncate(12, true, '') }}"
        ),
    )

    @task(task_id="quality_gate")
    def quality_gate(sim_day: str) -> dict:
        """Fail the DAG run if the reconciliation produced suspect output.

        This is the batch-layer health rule required by the observability section:
        a run that completes but silently prices only half the estate is a worse
        outcome than a run that fails loudly.
        """
        from common.config import settings
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-billing", "orchestration")
        expected = settings.num_households

        with connect() as conn:
            billed = query(
                conn,
                """SELECT count(*) AS n,
                          sum(total_bill) AS revenue,
                          sum(gross_consumption_kwh) AS kwh
                     FROM grid.batch_household_bill WHERE sim_day = %s""",
                (sim_day,),
            )[0]
            dq = query(
                conn,
                """SELECT check_name, ratio FROM grid.ops_dq_metrics
                    WHERE sim_day = %s ORDER BY recorded_at DESC""",
                (sim_day,),
            )

        result = {
            "sim_day": sim_day,
            "households_billed": int(billed["n"] or 0),
            "expected_households": expected,
            "coverage_pct": round(100.0 * (billed["n"] or 0) / expected, 1) if expected else 0.0,
            "total_revenue": float(billed["revenue"] or 0.0),
            "total_kwh": float(billed["kwh"] or 0.0),
            "dq_checks": {r["check_name"]: float(r["ratio"] or 0) for r in dq},
        }
        log.info("orchestration.quality_gate", **result)

        # Coverage below 80% means the join lost households: a tariff file that
        # arrived truncated, or a lost stream partition. Both need a human.
        if result["coverage_pct"] < 80.0:
            raise ValueError(
                f"Billing coverage {result['coverage_pct']}% for {sim_day} "
                f"({result['households_billed']}/{expected} households) is below the 80% gate"
            )
        return result

    @task(task_id="publish_consolidated_report")
    def publish_consolidated_report(sim_day: str) -> dict:
        """Write the consolidated daily report to object storage as CSV and HTML.

        This is the final deliverable of the running system: the artefact a grid
        operator actually opens, joining streamed consumption to the daily tariff
        and weather files.
        """
        import csv
        import io

        import boto3
        from botocore.client import Config as BotoConfig

        from common.config import settings
        from common.db import connect, query
        from common.logging_setup import get_logger

        log = get_logger("airflow-billing", "orchestration")

        with connect() as conn:
            households = query(
                conn,
                """SELECT * FROM grid.v_daily_billing_report
                    WHERE sim_day = %s ORDER BY total_bill DESC""",
                (sim_day,),
            )
            zones = query(
                conn,
                "SELECT * FROM grid.batch_zone_daily WHERE sim_day = %s ORDER BY grid_zone",
                (sim_day,),
            )

        if not households:
            raise ValueError(f"No billing rows to report for {sim_day}")

        # --- CSV ---
        csv_buf = io.StringIO()
        writer = csv.DictWriter(csv_buf, fieldnames=list(households[0].keys()),
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows(households)

        # --- HTML ---
        html = _render_report_html(sim_day, households, zones)

        s3 = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_user,
            aws_secret_access_key=settings.minio_password,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )
        base = f"reports/sim_date={sim_day}"
        s3.put_object(Bucket=settings.minio_bucket, Key=f"{base}/billing_report.csv",
                      Body=csv_buf.getvalue().encode(), ContentType="text/csv")
        s3.put_object(Bucket=settings.minio_bucket, Key=f"{base}/billing_report.html",
                      Body=html.encode(), ContentType="text/html")

        summary = {
            "sim_day": sim_day,
            "households": len(households),
            "zones": len(zones),
            "total_revenue": round(sum(float(h["total_bill"] or 0) for h in households), 2),
            "csv_key": f"{base}/billing_report.csv",
            "html_key": f"{base}/billing_report.html",
        }
        log.info("orchestration.report_published", **summary)
        return summary

    def _render_report_html(sim_day, households, zones) -> str:
        """Minimal self-contained HTML report -- no template engine, no CDN."""
        def zone_row(z):
            return (
                f"<tr><td>{z['grid_zone']}</td>"
                f"<td class=n>{float(z['total_consumption_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(z['total_solar_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(z['renewable_contribution_pct'] or 0):.1f}%</td>"
                f"<td class=n>{z['peak_hour'] if z['peak_hour'] is not None else '-'}:00</td>"
                f"<td class=n>{float(z['total_revenue'] or 0):.2f}</td>"
                f"<td class=n>{float(z['solar_forecast_error_pct'] or 0):+.1f}%</td></tr>"
            )

        def hh_row(h):
            return (
                f"<tr><td>{h['household_id']}</td><td>{h['grid_zone']}</td>"
                f"<td>{h['billing_tier']}</td>"
                f"<td class=n>{float(h['gross_consumption_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(h['solar_generation_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(h['net_import_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(h['exported_kwh'] or 0):.2f}</td>"
                f"<td class=n>{float(h['self_sufficiency_pct'] or 0):.0f}%</td>"
                f"<td class=n>{float(h['tariff_rate'] or 0):.4f}</td>"
                f"<td class=n><b>{float(h['total_bill'] or 0):.2f}</b></td></tr>"
            )

        total_rev = sum(float(h["total_bill"] or 0) for h in households)
        total_kwh = sum(float(h["gross_consumption_kwh"] or 0) for h in households)
        total_solar = sum(float(h["solar_generation_kwh"] or 0) for h in households)
        renewable = (total_solar / total_kwh * 100.0) if total_kwh else 0.0

        return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Daily Billing Report {sim_day}</title>
<style>
 body{{font:14px system-ui,sans-serif;margin:2rem;color:#1a1a1a;background:#fafafa}}
 h1{{margin:0 0 .25rem}} .sub{{color:#666;margin-bottom:1.5rem}}
 .kpis{{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:2rem}}
 .kpi{{background:#fff;border:1px solid #e2e2e2;border-radius:8px;padding:1rem 1.25rem;min-width:150px}}
 .kpi .v{{font-size:1.6rem;font-weight:600}} .kpi .l{{color:#666;font-size:.8rem;text-transform:uppercase}}
 table{{border-collapse:collapse;width:100%;background:#fff;margin-bottom:2rem}}
 th,td{{border:1px solid #e2e2e2;padding:.4rem .6rem;text-align:left}}
 th{{background:#f0f0f0}} td.n{{text-align:right;font-variant-numeric:tabular-nums}}
 caption{{text-align:left;font-weight:600;padding:.5rem 0}}
</style></head><body>
<h1>Consolidated Daily Billing &amp; Solar Report</h1>
<div class=sub>Simulated day {sim_day} &middot; generated by the Lambda batch layer</div>
<div class=kpis>
  <div class=kpi><div class=l>Households billed</div><div class=v>{len(households)}</div></div>
  <div class=kpi><div class=l>Total consumption</div><div class=v>{total_kwh:.1f} kWh</div></div>
  <div class=kpi><div class=l>Solar generated</div><div class=v>{total_solar:.1f} kWh</div></div>
  <div class=kpi><div class=l>Renewable share</div><div class=v>{renewable:.1f}%</div></div>
  <div class=kpi><div class=l>Total revenue</div><div class=v>{total_rev:.2f}</div></div>
</div>
<table><caption>Zone summary</caption>
<tr><th>Zone</th><th>Consumption kWh</th><th>Solar kWh</th><th>Renewable %</th>
<th>Peak hour</th><th>Revenue</th><th>Solar forecast error</th></tr>
{''.join(zone_row(z) for z in zones)}
</table>
<table><caption>Per-household bill (highest first)</caption>
<tr><th>Household</th><th>Zone</th><th>Tier</th><th>Consumed kWh</th><th>Solar kWh</th>
<th>Net import kWh</th><th>Exported kWh</th><th>Self-sufficiency</th><th>Rate</th><th>Bill</th></tr>
{''.join(hh_row(h) for h in households)}
</table>
</body></html>"""

    gate = quality_gate(pending_day)
    report = publish_consolidated_report(pending_day)

    pending_day >> run_reconciliation >> gate >> report
