"""Compressed simulated clock.

The assignment allows compressing simulated time so a multi-day pipeline can be
demonstrated in one session. One simulated day is compressed into
``SIM_DAY_SECONDS`` of wall-clock time (default 300s = 5 minutes), i.e. the
simulation runs at 86400/300 = 288x real time.

Both the streaming producer and the daily-batch producer share this class, so
"the batch file for simulated day D" always lines up with "the stream events
stamped with simulated day D" -- that alignment is what makes the daily
stream/batch join in the batch layer correct.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

SECONDS_PER_DAY = 86_400


@dataclass
class SimClock:
    start_date: date
    day_seconds: int
    _origin: float = 0.0

    def __post_init__(self) -> None:
        if self.day_seconds <= 0:
            raise ValueError("day_seconds must be positive")
        if not self._origin:
            self._origin = time.time()

    @property
    def speedup(self) -> float:
        """How many simulated seconds pass per wall-clock second."""
        return SECONDS_PER_DAY / self.day_seconds

    def elapsed_wall(self) -> float:
        return time.time() - self._origin

    def now(self) -> datetime:
        """Current simulated timestamp (UTC)."""
        sim_seconds = self.elapsed_wall() * self.speedup
        base = datetime.combine(self.start_date, datetime.min.time(), tzinfo=timezone.utc)
        return base + timedelta(seconds=sim_seconds)

    def sim_day(self) -> date:
        return self.now().date()

    def day_index(self) -> int:
        """0 for the first simulated day, 1 for the second, ..."""
        return int(self.elapsed_wall() // self.day_seconds)

    def seconds_until_next_day(self) -> float:
        """Wall-clock seconds remaining before the simulated date rolls over."""
        return self.day_seconds - (self.elapsed_wall() % self.day_seconds)

    def describe(self) -> dict:
        return {
            "sim_start_date": self.start_date.isoformat(),
            "sim_day_seconds": self.day_seconds,
            "speedup_x": round(self.speedup, 1),
            "sim_now": self.now().isoformat(),
            "sim_day": self.sim_day().isoformat(),
            "day_index": self.day_index(),
        }
