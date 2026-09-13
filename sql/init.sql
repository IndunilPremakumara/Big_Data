-- ===========================================================================
-- Smart Grid Lambda Pipeline: serving-layer schema (PostgreSQL)
--
-- Table naming encodes the Lambda layer that owns each table:
--   rt_*      speed layer   - written continuously by Spark Structured Streaming.
--                             Approximate, low latency, overwritten in place.
--   batch_*   batch layer   - written by the Airflow-orchestrated Spark job.
--                             Exact, authoritative, fully recomputable.
--   ops_*     cross-cutting - run audit and data-quality metrics.
--
-- Nothing writes to a table owned by another layer. That separation is what lets
-- the batch layer be re-run over a simulated day without racing the stream, and
-- it is what the serving view relies on when it decides which number to trust.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS grid;
SET search_path TO grid, public;

-- ---------------------------------------------------------------------------
-- Reference dimension. Mirrors common/domain.py so the API can resolve zone and
-- tier without joining back to the raw stream.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_household (
    household_id      TEXT PRIMARY KEY,
    meter_id          TEXT NOT NULL,
    grid_zone         TEXT NOT NULL,
    base_load_kw      NUMERIC(8, 3) NOT NULL,
    solar_capacity_kw NUMERIC(8, 2) NOT NULL DEFAULT 0,
    billing_tier      TEXT NOT NULL,
    subsidy_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dim_household_zone ON dim_household (grid_zone);

-- ===========================================================================
-- SPEED LAYER
-- ===========================================================================

-- Windowed zone load. One row per (zone, window). Upserted every micro-batch;
-- a late event that lands inside the watermark updates the row it belongs to.
CREATE TABLE IF NOT EXISTS rt_zone_load (
    grid_zone             TEXT        NOT NULL,
    window_start          TIMESTAMPTZ NOT NULL,
    window_end            TIMESTAMPTZ NOT NULL,
    sim_day               DATE        NOT NULL,
    total_consumption_kwh NUMERIC(14, 5) NOT NULL DEFAULT 0,
    total_solar_kwh       NUMERIC(14, 5) NOT NULL DEFAULT 0,
    net_import_kwh        NUMERIC(14, 5) NOT NULL DEFAULT 0,
    renewable_pct         NUMERIC(6, 2)  NOT NULL DEFAULT 0,
    active_meters         INTEGER        NOT NULL DEFAULT 0,
    avg_voltage           NUMERIC(7, 2),
    reading_count         INTEGER        NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);
CREATE INDEX IF NOT EXISTS idx_rt_zone_load_window  ON rt_zone_load (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_rt_zone_load_sim_day ON rt_zone_load (sim_day);

-- Running per-household totals for the current simulated day. This is the speed
-- layer estimate the API serves for "today", before the batch job produces the
-- authoritative bill.
CREATE TABLE IF NOT EXISTS rt_household_running (
    household_id     TEXT NOT NULL,
    sim_day          DATE NOT NULL,
    grid_zone        TEXT NOT NULL,
    consumption_kwh  NUMERIC(14, 5) NOT NULL DEFAULT 0,
    solar_kwh        NUMERIC(14, 5) NOT NULL DEFAULT 0,
    net_import_kwh   NUMERIC(14, 5) NOT NULL DEFAULT 0,
    reading_count    INTEGER        NOT NULL DEFAULT 0,
    last_seen        TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (household_id, sim_day)
);
CREATE INDEX IF NOT EXISTS idx_rt_hh_sim_day ON rt_household_running (sim_day);

-- Business alerts raised by the speed layer (threshold rules over the stream).
-- Distinct from Prometheus alerts, which watch pipeline *health*; these describe
-- the *grid*.
CREATE TABLE IF NOT EXISTS rt_alerts (
    alert_id     BIGSERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,
    severity     TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL')),
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    sim_day      DATE,
    window_start TIMESTAMPTZ,
    metric_value NUMERIC(14, 4),
    threshold    NUMERIC(14, 4),
    message      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rt_alerts_created ON rt_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rt_alerts_type    ON rt_alerts (alert_type, severity);

-- ===========================================================================
-- BATCH LAYER
-- ===========================================================================

-- The authoritative per-household daily bill. Recomputed from the immutable
-- Parquet master dataset joined to that day tariff file, so re-running the DAG
-- for a past day overwrites this row with a corrected value.
CREATE TABLE IF NOT EXISTS batch_household_bill (
    sim_day                DATE NOT NULL,
    household_id           TEXT NOT NULL,
    grid_zone              TEXT NOT NULL,
    billing_tier           TEXT NOT NULL,
    subsidy_flag           BOOLEAN NOT NULL DEFAULT FALSE,
    gross_consumption_kwh  NUMERIC(14, 5) NOT NULL DEFAULT 0,
    solar_generation_kwh   NUMERIC(14, 5) NOT NULL DEFAULT 0,
    self_consumed_kwh      NUMERIC(14, 5) NOT NULL DEFAULT 0,
    net_import_kwh         NUMERIC(14, 5) NOT NULL DEFAULT 0,
    exported_kwh           NUMERIC(14, 5) NOT NULL DEFAULT 0,
    tariff_rate            NUMERIC(10, 4) NOT NULL,
    export_rate            NUMERIC(10, 4) NOT NULL,
    energy_charge          NUMERIC(12, 4) NOT NULL DEFAULT 0,
    export_credit          NUMERIC(12, 4) NOT NULL DEFAULT 0,
    standing_charge        NUMERIC(12, 4) NOT NULL DEFAULT 0,
    subsidy_amount         NUMERIC(12, 4) NOT NULL DEFAULT 0,
    total_bill             NUMERIC(12, 4) NOT NULL DEFAULT 0,
    self_sufficiency_pct   NUMERIC(6, 2)  NOT NULL DEFAULT 0,
    reading_count          INTEGER        NOT NULL DEFAULT 0,
    rejected_reading_count INTEGER        NOT NULL DEFAULT 0,
    batch_run_id           TEXT,
    computed_at            TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, household_id)
);
CREATE INDEX IF NOT EXISTS idx_bill_sim_day ON batch_household_bill (sim_day);
CREATE INDEX IF NOT EXISTS idx_bill_zone    ON batch_household_bill (grid_zone, sim_day);

-- Authoritative zone rollup, including how far the solar forecast missed.
CREATE TABLE IF NOT EXISTS batch_zone_daily (
    sim_day                    DATE NOT NULL,
    grid_zone                  TEXT NOT NULL,
    households                 INTEGER NOT NULL DEFAULT 0,
    total_consumption_kwh      NUMERIC(14, 5) NOT NULL DEFAULT 0,
    total_solar_kwh            NUMERIC(14, 5) NOT NULL DEFAULT 0,
    net_import_kwh             NUMERIC(14, 5) NOT NULL DEFAULT 0,
    renewable_contribution_pct NUMERIC(6, 2)  NOT NULL DEFAULT 0,
    peak_hour                  INTEGER,
    peak_load_kwh              NUMERIC(14, 5),
    forecast_solar_index       NUMERIC(8, 4),
    actual_solar_index         NUMERIC(8, 4),
    solar_forecast_error_pct   NUMERIC(8, 2),
    total_revenue              NUMERIC(14, 4) NOT NULL DEFAULT 0,
    batch_run_id               TEXT,
    computed_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_day, grid_zone)
);

-- ===========================================================================
-- OPERATIONAL / OBSERVABILITY
-- ===========================================================================

-- Audit row per job execution. Every stage writes here, which gives a single
-- queryable history of what ran, over what, how long it took and whether it
-- failed, independent of container log retention.
CREATE TABLE IF NOT EXISTS ops_pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    layer        TEXT NOT NULL CHECK (layer IN ('speed', 'batch', 'ingestion', 'serving')),
    job_name     TEXT NOT NULL,
    sim_day      DATE,
    status       TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    records_in   BIGINT  DEFAULT 0,
    records_out  BIGINT  DEFAULT 0,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    duration_s   NUMERIC(12, 3),
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON ops_pipeline_runs (job_name, started_at DESC);

-- Named data-quality checks, recorded per run so quality can be trended rather
-- than only alerted on.
CREATE TABLE IF NOT EXISTS ops_dq_metrics (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT,
    job_name     TEXT NOT NULL,
    stage        TEXT NOT NULL,
    sim_day      DATE,
    check_name   TEXT NOT NULL,
    passed       BIGINT NOT NULL DEFAULT 0,
    failed       BIGINT NOT NULL DEFAULT 0,
    ratio        NUMERIC(6, 4),
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_job ON ops_dq_metrics (job_name, recorded_at DESC);

-- ===========================================================================
-- SERVING VIEWS: the Lambda merge made explicit
-- ===========================================================================

-- The defining query of a Lambda serving layer. For any simulated day it prefers
-- the batch (exact) result and falls back to the speed-layer estimate only where
-- batch has not yet produced a row. The `source` column is exposed to API clients
-- so a consumer can always tell whether it is reading a settled bill or a live
-- approximation.
CREATE OR REPLACE VIEW v_household_daily AS
SELECT
    b.sim_day,
    b.household_id,
    b.grid_zone,
    b.gross_consumption_kwh AS consumption_kwh,
    b.solar_generation_kwh  AS solar_kwh,
    b.net_import_kwh,
    b.total_bill,
    b.reading_count,
    'batch'::TEXT           AS source,
    TRUE                    AS is_final,
    b.computed_at           AS as_of
FROM batch_household_bill b
UNION ALL
SELECT
    r.sim_day,
    r.household_id,
    r.grid_zone,
    r.consumption_kwh,
    r.solar_kwh,
    r.net_import_kwh,
    NULL::NUMERIC           AS total_bill,   -- unknown until tariffs are applied
    r.reading_count,
    'speed'::TEXT           AS source,
    FALSE                   AS is_final,
    r.last_seen             AS as_of
FROM rt_household_running r
WHERE NOT EXISTS (
    SELECT 1 FROM batch_household_bill b2
    WHERE b2.sim_day = r.sim_day AND b2.household_id = r.household_id
);

-- Most recent completed window per zone: what the real-time dashboard reads.
CREATE OR REPLACE VIEW v_zone_latest AS
SELECT DISTINCT ON (grid_zone)
    grid_zone, window_start, window_end, sim_day,
    total_consumption_kwh, total_solar_kwh, net_import_kwh,
    renewable_pct, active_meters, avg_voltage, reading_count, updated_at
FROM rt_zone_load
ORDER BY grid_zone, window_start DESC;

-- Freshness probe backing the /health endpoint and the staleness alert.
CREATE OR REPLACE VIEW v_pipeline_freshness AS
SELECT
    'rt_zone_load'   AS table_name,
    max(updated_at)  AS last_write,
    EXTRACT(EPOCH FROM (now() - max(updated_at)))::NUMERIC(12,1) AS staleness_seconds
FROM rt_zone_load
UNION ALL
SELECT 'rt_household_running', max(last_seen),
       EXTRACT(EPOCH FROM (now() - max(last_seen)))::NUMERIC(12,1)
FROM rt_household_running
UNION ALL
SELECT 'batch_household_bill', max(computed_at),
       EXTRACT(EPOCH FROM (now() - max(computed_at)))::NUMERIC(12,1)
FROM batch_household_bill;

-- Least profitable / highest-import households once tariffs are applied. This is
-- the query the daily consolidated report is built from.
CREATE OR REPLACE VIEW v_daily_billing_report AS
SELECT
    b.sim_day,
    b.grid_zone,
    b.household_id,
    b.billing_tier,
    b.subsidy_flag,
    b.gross_consumption_kwh,
    b.solar_generation_kwh,
    b.net_import_kwh,
    b.exported_kwh,
    b.self_sufficiency_pct,
    b.tariff_rate,
    b.energy_charge,
    b.export_credit,
    b.subsidy_amount,
    b.standing_charge,
    b.total_bill,
    RANK() OVER (PARTITION BY b.sim_day ORDER BY b.total_bill DESC) AS bill_rank
FROM batch_household_bill b;
