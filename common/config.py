"""Central, environment-driven configuration.

Every service reads its settings from here so that a single ``.env`` file is the
one source of truth across producers, Spark jobs, Airflow and the serving API.
Values are read once at import time and exposed as a frozen dataclass.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _str(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    # --- simulated clock ---
    sim_day_seconds: int = field(default_factory=lambda: _int("SIM_DAY_SECONDS", 300))
    stream_interval_seconds: float = field(
        default_factory=lambda: _float("STREAM_INTERVAL_SECONDS", 2.0)
    )
    sim_start_date: str = field(default_factory=lambda: _str("SIM_START_DATE", "2026-01-01"))

    # --- simulation scale ---
    num_households: int = field(default_factory=lambda: _int("NUM_HOUSEHOLDS", 60))
    num_zones: int = field(default_factory=lambda: _int("NUM_ZONES", 4))

    # --- fault injection ---
    anomaly_rate: float = field(default_factory=lambda: _float("ANOMALY_RATE", 0.03))
    duplicate_rate: float = field(default_factory=lambda: _float("DUPLICATE_RATE", 0.02))
    late_event_rate: float = field(default_factory=lambda: _float("LATE_EVENT_RATE", 0.02))
    null_field_rate: float = field(default_factory=lambda: _float("NULL_FIELD_RATE", 0.02))

    # --- kafka ---
    kafka_bootstrap: str = field(default_factory=lambda: _str("KAFKA_BOOTSTRAP", "kafka:9092"))
    topic_readings: str = field(default_factory=lambda: _str("KAFKA_TOPIC_READINGS", "meter.readings"))
    topic_dlq: str = field(default_factory=lambda: _str("KAFKA_TOPIC_DLQ", "meter.readings.dlq"))
    topic_partitions: int = field(default_factory=lambda: _int("KAFKA_TOPIC_PARTITIONS", 6))
    replication_factor: int = field(default_factory=lambda: _int("KAFKA_REPLICATION_FACTOR", 1))

    # --- postgres ---
    pg_host: str = field(default_factory=lambda: _str("POSTGRES_HOST", "postgres"))
    pg_port: int = field(default_factory=lambda: _int("POSTGRES_PORT", 5432))
    pg_user: str = field(default_factory=lambda: _str("POSTGRES_USER", "grid"))
    pg_password: str = field(default_factory=lambda: _str("POSTGRES_PASSWORD", "gridpass"))
    pg_db: str = field(default_factory=lambda: _str("POSTGRES_DB", "smartgrid"))

    # --- minio / s3a ---
    minio_endpoint: str = field(default_factory=lambda: _str("MINIO_ENDPOINT", "http://minio:9000"))
    minio_user: str = field(default_factory=lambda: _str("MINIO_ROOT_USER", "minioadmin"))
    minio_password: str = field(default_factory=lambda: _str("MINIO_ROOT_PASSWORD", "minioadmin123"))
    minio_bucket: str = field(default_factory=lambda: _str("MINIO_BUCKET", "smartgrid"))

    # --- spark ---
    spark_master_url: str = field(
        default_factory=lambda: _str("SPARK_MASTER_URL", "spark://spark-master:7077")
    )
    stream_trigger_seconds: int = field(default_factory=lambda: _int("STREAM_TRIGGER_SECONDS", 10))
    stream_window_seconds: int = field(default_factory=lambda: _int("STREAM_WINDOW_SECONDS", 60))
    stream_watermark_seconds: int = field(
        default_factory=lambda: _int("STREAM_WATERMARK_SECONDS", 120)
    )

    # --- business thresholds ---
    renewable_low_pct: float = field(default_factory=lambda: _float("RENEWABLE_LOW_PCT", 15.0))
    zone_overload_kwh: float = field(default_factory=lambda: _float("ZONE_OVERLOAD_KWH", 45.0))
    meter_silence_seconds: int = field(default_factory=lambda: _int("METER_SILENCE_SECONDS", 90))
    spike_factor: float = field(default_factory=lambda: _float("CONSUMPTION_SPIKE_FACTOR", 3.0))

    # --- misc ---
    api_port: int = field(default_factory=lambda: _int("API_PORT", 8000))
    log_level: str = field(default_factory=lambda: _str("LOG_LEVEL", "INFO"))

    # ---------------- derived helpers ----------------
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )

    @property
    def start_date(self) -> date:
        return date.fromisoformat(self.sim_start_date)

    def s3a(self, *parts: str) -> str:
        """Build an s3a:// path inside the project bucket."""
        suffix = "/".join(p.strip("/") for p in parts if p)
        return f"s3a://{self.minio_bucket}/{suffix}" if suffix else f"s3a://{self.minio_bucket}"


settings = Settings()
