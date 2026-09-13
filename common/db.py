"""PostgreSQL helpers shared by the speed layer, batch layer and serving API.

Uses psycopg 3. Every write goes through ``upsert`` so that re-running any job is
idempotent: the speed layer re-emits a window it has already written whenever a
late event updates it, and the batch layer deliberately recomputes whole days.
Both must overwrite rather than duplicate.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Iterable, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from common.config import settings

SCHEMA = "grid"


@contextmanager
def connect(autocommit: bool = True, retries: int = 20, delay: float = 3.0):
    """Yield a connection with search_path set, retrying while Postgres boots."""
    last: Exception | None = None
    for _ in range(retries):
        try:
            with psycopg.connect(settings.pg_dsn, autocommit=autocommit) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {SCHEMA}, public")
                yield conn
                return
        except psycopg.OperationalError as exc:
            last = exc
            time.sleep(delay)
    raise RuntimeError(f"PostgreSQL unreachable at {settings.pg_host}: {last}")


def upsert(
    conn,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    conflict_keys: Sequence[str],
    update_columns: Sequence[str] | None = None,
) -> int:
    """Bulk INSERT ... ON CONFLICT DO UPDATE. Returns the number of rows sent.

    ``update_columns`` defaults to every non-key column, which is the behaviour
    both layers want: last write for a given key wins.
    """
    rows = list(rows)
    if not rows:
        return 0
    update_columns = update_columns or [c for c in columns if c not in conflict_keys]

    stmt = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
        "ON CONFLICT ({keys}) DO UPDATE SET {assignments}"
    ).format(
        table=sql.Identifier(SCHEMA, table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        keys=sql.SQL(", ").join(map(sql.Identifier, conflict_keys)),
        assignments=sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in update_columns
        ),
    )
    with conn.cursor() as cur:
        cur.executemany(stmt, rows)
    return len(rows)


def insert(conn, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    """Plain bulk INSERT, for append-only tables such as alerts and DQ metrics."""
    rows = list(rows)
    if not rows:
        return 0
    stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({placeholders})").format(
        table=sql.Identifier(SCHEMA, table),
        cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
    )
    with conn.cursor() as cur:
        cur.executemany(stmt, rows)
    return len(rows)


def query(conn, statement: str, params: Sequence[Any] | None = None) -> list[dict]:
    """Run a read query and return a list of dicts."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(statement, params or ())
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Run auditing: every job records what it did, in the database, so the history
# survives container restarts and log rotation.
# ---------------------------------------------------------------------------

def start_run(conn, run_id: str, layer: str, job_name: str, sim_day=None) -> None:
    upsert(
        conn,
        "ops_pipeline_runs",
        ["run_id", "layer", "job_name", "sim_day", "status", "started_at"],
        [(run_id, layer, job_name, sim_day, "RUNNING", "now()")],
        ["run_id"],
    )


def finish_run(
    conn, run_id: str, status: str, records_in: int = 0, records_out: int = 0,
    duration_s: float | None = None, error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""UPDATE {SCHEMA}.ops_pipeline_runs
                   SET status=%s, records_in=%s, records_out=%s,
                       finished_at=now(), duration_s=%s, error=%s
                 WHERE run_id=%s""",
            (status, records_in, records_out, duration_s, (error or None), run_id),
        )


def record_dq(
    conn, run_id: str, job_name: str, stage: str, check_name: str,
    passed: int, failed: int, sim_day=None,
) -> None:
    """Persist one named data-quality check result."""
    total = passed + failed
    ratio = round(passed / total, 4) if total else None
    insert(
        conn,
        "ops_dq_metrics",
        ["run_id", "job_name", "stage", "sim_day", "check_name", "passed", "failed", "ratio"],
        [(run_id, job_name, stage, sim_day, check_name, passed, failed, ratio)],
    )


def seed_dim_household(conn, households) -> int:
    """Load the deterministic household registry into the serving database.

    Called on startup by the speed layer. Keeping the dimension in sync with
    common.domain guarantees the API resolves the same zone mapping the producers
    used, without the API needing to import the simulation code.
    """
    rows = [
        (h.household_id, h.meter_id, h.grid_zone, h.base_load_kw,
         h.solar_capacity_kw, h.billing_tier, h.subsidy_flag)
        for h in households
    ]
    return upsert(
        conn,
        "dim_household",
        ["household_id", "meter_id", "grid_zone", "base_load_kw",
         "solar_capacity_kw", "billing_tier", "subsidy_flag"],
        rows,
        ["household_id"],
    )
