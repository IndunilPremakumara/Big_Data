"""Serving layer: FastAPI over the PostgreSQL serving tables.

This is the query side of the Lambda architecture. Endpoints fall into three
groups that map exactly onto the three layers:

  /api/v1/realtime/*   speed layer only  -- approximate, seconds old
  /api/v1/billing/*    batch layer only  -- exact, settled, recomputable
  /api/v1/households/* merged view       -- batch where available, speed elsewhere

Every merged response carries a ``source`` field ("batch" or "speed") and an
``is_final`` flag. A consumer is never left guessing whether the number it just
read is a settled bill or a live estimate, which is the single most important
contract a Lambda serving layer has to offer.

The API also exposes /metrics for Prometheus and /health for the container
health check.
"""
from __future__ import annotations

import sys
import time
from contextlib import asynccontextmanager
from datetime import date

sys.path.insert(0, "/opt/pipeline")

from fastapi import FastAPI, HTTPException, Query, Request, Response  # noqa: E402
from fastapi.responses import HTMLResponse, JSONResponse  # noqa: E402
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # noqa: E402

from common import metrics  # noqa: E402
from common.config import settings  # noqa: E402
from common.db import connect, query  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402

COMPONENT = "serving-api"
STAGE = "serving"

log = get_logger(COMPONENT, STAGE)

# How stale the real-time tables may be before /health reports degraded.
FRESHNESS_BUDGET_S = settings.meter_silence_seconds * 2


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("api.starting", port=settings.api_port, db=settings.pg_host)
    # Fail fast and loudly if the serving store is unreachable at boot, rather
    # than returning 500s to every caller afterwards.
    with connect() as conn:
        n = query(conn, "SELECT count(*) AS n FROM grid.dim_household")[0]["n"]
    log.info("api.ready", households_known=n)
    yield
    log.info("api.stopping")


app = FastAPI(
    title="Smart Grid Monitoring & Billing API",
    description=(
        "Serving layer of a Lambda-architecture smart-grid pipeline. "
        "Real-time endpoints read the Spark Structured Streaming speed layer; "
        "billing endpoints read the Airflow-orchestrated batch layer."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    """Log and time every request; feed latency and error counters."""
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        metrics.ERRORS.labels(COMPONENT, STAGE, type(exc).__name__).inc()
        log.exception("api.request_failed", path=request.url.path, method=request.method)
        raise
    duration = time.perf_counter() - started
    metrics.LATENCY.labels(COMPONENT, STAGE, "http_request").observe(duration)
    metrics.RECORDS.labels(COMPONENT, STAGE, "consumed").inc()
    if response.status_code >= 500:
        metrics.ERRORS.labels(COMPONENT, STAGE, f"http_{response.status_code}").inc()
    log.info(
        "api.request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round(duration * 1000, 2),
    )
    return response


def _rows(sql: str, params=()) -> list[dict]:
    with connect() as conn:
        return query(conn, sql, params)


def _floats(rows: list[dict], *fields: str) -> list[dict]:
    """Cast Decimal columns to float so FastAPI can serialise them as JSON numbers."""
    for r in rows:
        for f in fields:
            if r.get(f) is not None:
                r[f] = float(r[f])
    return rows


# ---------------------------------------------------------------------------
# Health & metrics
# ---------------------------------------------------------------------------

@app.get("/health", tags=["observability"])
def health() -> JSONResponse:
    """Liveness + data-freshness probe.

    Returns 200 when the pipeline is healthy, 503 when it is up but not
    receiving data. The distinction matters: a container that answers requests
    while the stream is dead is exactly the failure mode a naive health check
    misses.
    """
    try:
        rows = _rows("SELECT * FROM grid.v_pipeline_freshness")
    except Exception as exc:
        log.error("health.db_unreachable", error=str(exc))
        return JSONResponse(
            status_code=503, content={"status": "unhealthy", "reason": "database unreachable"}
        )

    freshness = {
        r["table_name"]: (
            float(r["staleness_seconds"]) if r["staleness_seconds"] is not None else None
        )
        for r in rows
    }
    stream_stale = [
        t for t in ("rt_zone_load", "rt_household_running")
        if freshness.get(t) is None or freshness[t] > FRESHNESS_BUDGET_S
    ]

    body = {
        "status": "degraded" if stream_stale else "healthy",
        "freshness_seconds": freshness,
        "freshness_budget_seconds": FRESHNESS_BUDGET_S,
        "stale_tables": stream_stale,
    }
    return JSONResponse(status_code=503 if stream_stale else 200, content=body)


@app.get("/metrics", tags=["observability"])
def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Speed layer: real-time grid state
# ---------------------------------------------------------------------------

@app.get("/api/v1/realtime/zones", tags=["speed layer"])
def realtime_zones() -> dict:
    """Current grid load and renewable mix per zone.

    Backed by the most recent window each zone has produced. This is the
    assignment's required real-time endpoint.
    """
    rows = _floats(
        _rows("SELECT * FROM grid.v_zone_latest ORDER BY grid_zone"),
        "total_consumption_kwh", "total_solar_kwh", "net_import_kwh",
        "renewable_pct", "avg_voltage",
    )
    total_load = sum(r["total_consumption_kwh"] or 0 for r in rows)
    total_solar = sum(r["total_solar_kwh"] or 0 for r in rows)
    return {
        "as_of": max((r["updated_at"] for r in rows), default=None),
        "source": "speed",
        "is_final": False,
        "grid_total": {
            "load_kwh": round(total_load, 4),
            "solar_kwh": round(total_solar, 4),
            "renewable_pct": round(total_solar / total_load * 100, 2) if total_load else 0.0,
            "active_meters": sum(r["active_meters"] or 0 for r in rows),
            "zones_reporting": len(rows),
        },
        "zones": rows,
    }


@app.get("/api/v1/realtime/zones/{grid_zone}/history", tags=["speed layer"])
def zone_history(grid_zone: str, windows: int = Query(24, ge=1, le=500)) -> dict:
    """Recent windowed history for one zone, newest first."""
    rows = _floats(
        _rows(
            """SELECT * FROM grid.rt_zone_load WHERE grid_zone = %s
                ORDER BY window_start DESC LIMIT %s""",
            (grid_zone, windows),
        ),
        "total_consumption_kwh", "total_solar_kwh", "net_import_kwh",
        "renewable_pct", "avg_voltage",
    )
    if not rows:
        raise HTTPException(404, f"No windows recorded for zone {grid_zone}")
    return {"grid_zone": grid_zone, "source": "speed", "windows": rows}


@app.get("/api/v1/realtime/alerts", tags=["speed layer"])
def realtime_alerts(
    limit: int = Query(50, ge=1, le=500),
    severity: str | None = Query(None, pattern="^(INFO|WARNING|CRITICAL)$"),
) -> dict:
    """Threshold alerts raised by the speed layer, newest first."""
    if severity:
        rows = _rows(
            """SELECT * FROM grid.rt_alerts WHERE severity = %s
                ORDER BY created_at DESC LIMIT %s""",
            (severity, limit),
        )
    else:
        rows = _rows("SELECT * FROM grid.rt_alerts ORDER BY created_at DESC LIMIT %s", (limit,))
    counts = _rows(
        """SELECT alert_type, severity, count(*) AS n FROM grid.rt_alerts
            WHERE created_at > now() - interval '15 minutes'
            GROUP BY alert_type, severity ORDER BY n DESC"""
    )
    return {
        "alerts": _floats(rows, "metric_value", "threshold"),
        "last_15_min_summary": [
            {"alert_type": c["alert_type"], "severity": c["severity"], "count": int(c["n"])}
            for c in counts
        ],
    }


# ---------------------------------------------------------------------------
# Batch layer: settled billing
# ---------------------------------------------------------------------------

@app.get("/api/v1/billing/{sim_day}", tags=["batch layer"])
def billing_for_day(sim_day: date, limit: int = Query(200, ge=1, le=1000)) -> dict:
    """The consolidated daily billing report for one simulated day."""
    households = _floats(
        _rows(
            """SELECT * FROM grid.v_daily_billing_report
                WHERE sim_day = %s ORDER BY total_bill DESC LIMIT %s""",
            (sim_day, limit),
        ),
        "gross_consumption_kwh", "solar_generation_kwh", "net_import_kwh", "exported_kwh",
        "self_sufficiency_pct", "tariff_rate", "energy_charge", "export_credit",
        "subsidy_amount", "standing_charge", "total_bill",
    )
    if not households:
        raise HTTPException(
            404,
            f"No settled bills for {sim_day}. The batch layer may not have reconciled "
            "this day yet -- check /api/v1/households for the live estimate.",
        )
    zones = _floats(
        _rows("SELECT * FROM grid.batch_zone_daily WHERE sim_day = %s ORDER BY grid_zone",
              (sim_day,)),
        "total_consumption_kwh", "total_solar_kwh", "net_import_kwh",
        "renewable_contribution_pct", "peak_load_kwh", "forecast_solar_index",
        "actual_solar_index", "solar_forecast_error_pct", "total_revenue",
    )
    return {
        "sim_day": sim_day.isoformat(),
        "source": "batch",
        "is_final": True,
        "totals": {
            "households_billed": len(households),
            "revenue": round(sum(h["total_bill"] or 0 for h in households), 2),
            "consumption_kwh": round(sum(h["gross_consumption_kwh"] or 0 for h in households), 3),
            "solar_kwh": round(sum(h["solar_generation_kwh"] or 0 for h in households), 3),
        },
        "zones": zones,
        "households": households,
    }


@app.get("/api/v1/billing/{sim_day}/report", response_class=HTMLResponse, tags=["batch layer"])
def billing_report_html(sim_day: date) -> HTMLResponse:
    """Human-readable version of the same consolidated report."""
    data = billing_for_day(sim_day, limit=1000)
    rows = "".join(
        f"<tr><td>{h['household_id']}</td><td>{h['grid_zone']}</td><td>{h['billing_tier']}</td>"
        f"<td class=n>{h['gross_consumption_kwh']:.2f}</td>"
        f"<td class=n>{h['solar_generation_kwh']:.2f}</td>"
        f"<td class=n>{h['net_import_kwh']:.2f}</td>"
        f"<td class=n>{h['self_sufficiency_pct']:.0f}%</td>"
        f"<td class=n><b>{h['total_bill']:.2f}</b></td></tr>"
        for h in data["households"]
    )
    t = data["totals"]
    return HTMLResponse(f"""<!doctype html><meta charset=utf-8>
<title>Billing report {sim_day}</title>
<style>body{{font:14px system-ui,sans-serif;margin:2rem}}
table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:.35rem .6rem}}
th{{background:#f0f0f0}}td.n{{text-align:right;font-variant-numeric:tabular-nums}}</style>
<h1>Consolidated billing report &mdash; {sim_day}</h1>
<p>{t['households_billed']} households &middot; {t['consumption_kwh']:.1f} kWh consumed &middot;
{t['solar_kwh']:.1f} kWh solar &middot; revenue {t['revenue']:.2f}</p>
<table><tr><th>Household</th><th>Zone</th><th>Tier</th><th>Consumed</th><th>Solar</th>
<th>Net import</th><th>Self-suff.</th><th>Bill</th></tr>{rows}</table>""")


@app.get("/api/v1/billing/{sim_day}/zones", tags=["batch layer"])
def billing_zones(sim_day: date) -> dict:
    """Zone rollup including how far the daily solar forecast missed."""
    zones = _floats(
        _rows("SELECT * FROM grid.batch_zone_daily WHERE sim_day = %s ORDER BY grid_zone",
              (sim_day,)),
        "total_consumption_kwh", "total_solar_kwh", "net_import_kwh",
        "renewable_contribution_pct", "peak_load_kwh", "forecast_solar_index",
        "actual_solar_index", "solar_forecast_error_pct", "total_revenue",
    )
    if not zones:
        raise HTTPException(404, f"No zone rollup for {sim_day}")
    return {"sim_day": sim_day.isoformat(), "source": "batch", "zones": zones}


# ---------------------------------------------------------------------------
# Merged view: the Lambda serving contract
# ---------------------------------------------------------------------------

@app.get("/api/v1/households", tags=["merged"])
def households(
    sim_day: date | None = None,
    grid_zone: str | None = None,
    limit: int = Query(200, ge=1, le=1000),
) -> dict:
    """Per-household daily position, batch where settled and speed elsewhere.

    This endpoint is the Lambda merge. Rows carry ``source`` and ``is_final`` so
    callers can distinguish a settled bill from a live estimate.
    """
    clauses, params = [], []
    if sim_day:
        clauses.append("sim_day = %s")
        params.append(sim_day)
    if grid_zone:
        clauses.append("grid_zone = %s")
        params.append(grid_zone)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    rows = _floats(
        _rows(
            f"""SELECT * FROM grid.v_household_daily {where}
                 ORDER BY sim_day DESC, consumption_kwh DESC LIMIT %s""",
            tuple(params),
        ),
        "consumption_kwh", "solar_kwh", "net_import_kwh", "total_bill",
    )
    by_source: dict[str, int] = {}
    for r in rows:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
    return {"count": len(rows), "by_source": by_source, "households": rows}


@app.get("/api/v1/households/{household_id}", tags=["merged"])
def household_detail(household_id: str, days: int = Query(7, ge=1, le=60)) -> dict:
    """History for one household across both layers."""
    dim = _rows("SELECT * FROM grid.dim_household WHERE household_id = %s", (household_id,))
    if not dim:
        raise HTTPException(404, f"Unknown household {household_id}")
    history = _floats(
        _rows(
            """SELECT * FROM grid.v_household_daily WHERE household_id = %s
                ORDER BY sim_day DESC LIMIT %s""",
            (household_id, days),
        ),
        "consumption_kwh", "solar_kwh", "net_import_kwh", "total_bill",
    )
    return {
        "household": _floats(dim, "base_load_kw", "solar_capacity_kw")[0],
        "history": history,
    }


# ---------------------------------------------------------------------------
# Pipeline introspection
# ---------------------------------------------------------------------------

@app.get("/api/v1/pipeline/runs", tags=["observability"])
def pipeline_runs(limit: int = Query(25, ge=1, le=200)) -> dict:
    """Recent job executions across every layer, from the run audit table."""
    return {
        "runs": _floats(
            _rows("SELECT * FROM grid.ops_pipeline_runs ORDER BY started_at DESC LIMIT %s",
                  (limit,)),
            "duration_s",
        )
    }


@app.get("/api/v1/pipeline/quality", tags=["observability"])
def pipeline_quality(limit: int = Query(50, ge=1, le=500)) -> dict:
    """Recent data-quality check results."""
    return {
        "checks": _floats(
            _rows("SELECT * FROM grid.ops_dq_metrics ORDER BY recorded_at DESC LIMIT %s",
                  (limit,)),
            "ratio",
        )
    }


@app.get("/", tags=["meta"])
def index() -> dict:
    """Entry point: what this service is and where the interesting endpoints are."""
    return {
        "service": "Smart Grid Lambda Pipeline - Serving Layer",
        "architecture": "Lambda (Kafka -> Spark Structured Streaming + Spark batch -> PostgreSQL)",
        "simulated_clock": {
            "sim_day_seconds": settings.sim_day_seconds,
            "speedup_x": round(86400 / settings.sim_day_seconds, 1),
            "start_date": settings.sim_start_date,
        },
        "endpoints": {
            "speed layer": [
                "/api/v1/realtime/zones",
                "/api/v1/realtime/zones/{grid_zone}/history",
                "/api/v1/realtime/alerts",
            ],
            "batch layer": [
                "/api/v1/billing/{sim_day}",
                "/api/v1/billing/{sim_day}/report",
                "/api/v1/billing/{sim_day}/zones",
            ],
            "merged": ["/api/v1/households", "/api/v1/households/{household_id}"],
            "observability": [
                "/health", "/metrics",
                "/api/v1/pipeline/runs", "/api/v1/pipeline/quality",
            ],
            "docs": ["/docs", "/redoc"],
        },
    }
