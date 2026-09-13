"""Structured (JSON) logging shared by every pipeline stage.

Rationale
---------
The pipeline spans six processes (two producers, a Spark streaming job, a Spark
batch job, Airflow and the API). Free-text logs from six containers cannot be
correlated. Every record therefore carries a fixed envelope:

    ts, level, service, stage, run_id, event, plus arbitrary structured fields

``stage`` is one of ingestion / speed / batch / storage / serving, so a single
``docker compose logs | jq 'select(.stage=="speed")'`` slices the whole platform
by pipeline layer. ``run_id`` ties every record of one job execution together.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any

# Pipeline layers, used as the `stage` field. Keep this list closed so that log
# queries and Grafana filters have a known, small domain.
STAGES = ("ingestion", "speed", "batch", "storage", "serving", "orchestration")


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON object."""

    def __init__(self, service: str, stage: str, run_id: str) -> None:
        super().__init__()
        self.service = service
        self.stage = stage
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": self.service,
            "stage": self.stage,
            "run_id": self.run_id,
            "logger": record.name,
            "event": record.getMessage(),
        }
        # Anything passed via `extra={"fields": {...}}` is merged in flat.
        extra = getattr(record, "fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class StructuredLogger:
    """Thin wrapper that makes ``log.info("event", key=value)`` the natural call."""

    def __init__(self, logger: logging.Logger, run_id: str) -> None:
        self._log = logger
        self.run_id = run_id

    def _emit(self, level: int, event: str, **fields: Any) -> None:
        self._log.log(level, event, extra={"fields": fields})

    def debug(self, event: str, **f: Any) -> None:
        self._emit(logging.DEBUG, event, **f)

    def info(self, event: str, **f: Any) -> None:
        self._emit(logging.INFO, event, **f)

    def warning(self, event: str, **f: Any) -> None:
        self._emit(logging.WARNING, event, **f)

    def error(self, event: str, **f: Any) -> None:
        self._emit(logging.ERROR, event, **f)

    def exception(self, event: str, **f: Any) -> None:
        self._log.exception(event, extra={"fields": f})

    @contextmanager
    def timed(self, event: str, **fields: Any):
        """Log start/finish of a unit of work with a measured duration.

        Emits ``<event>.start`` then ``<event>.ok`` (or ``<event>.failed`` with the
        traceback), always with ``duration_ms`` -- the raw material for latency
        panels and for the pipeline_runs audit table.
        """
        started = time.perf_counter()
        self.info(f"{event}.start", **fields)
        try:
            yield
        except Exception as exc:
            self.error(
                f"{event}.failed",
                duration_ms=round((time.perf_counter() - started) * 1000, 2),
                error_type=type(exc).__name__,
                error=str(exc),
                **fields,
            )
            raise
        else:
            self.info(
                f"{event}.ok",
                duration_ms=round((time.perf_counter() - started) * 1000, 2),
                **fields,
            )


def get_logger(service: str, stage: str, run_id: str | None = None) -> StructuredLogger:
    """Configure root logging for this process and return a structured logger.

    Idempotent: repeated calls replace the handler rather than stacking them,
    which matters for Spark drivers that re-import modules.
    """
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {STAGES}, got {stage!r}")

    run_id = run_id or os.getenv("RUN_ID") or uuid.uuid4().hex[:12]
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service=service, stage=stage, run_id=run_id))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Third-party libraries are far too chatty at INFO for a demo transcript.
    for noisy in ("kafka", "botocore", "boto3", "urllib3", "s3transfer", "py4j"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return StructuredLogger(logging.getLogger(service), run_id)
