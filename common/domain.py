"""Reference data for the simulated grid.

The streaming producer (meter readings) and the daily-batch producer (tariff and
weather files) are separate processes, but the batch layer joins their output on
``household_id``. They must therefore agree exactly on which households exist and
which zone each belongs to.

Rather than sharing a database, both derive the registry from this module using a
fixed seed. Same inputs -> same registry, in any process, on any machine. That
determinism is also what makes the batch layer recomputable, which is the whole
point of the Lambda batch path.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, asdict

REGISTRY_SEED = 20260101

ZONE_NAMES = ["ZONE-NORTH", "ZONE-SOUTH", "ZONE-EAST", "ZONE-WEST", "ZONE-CENTRAL"]
BILLING_TIERS = ["DOMESTIC_LOW", "DOMESTIC_STD", "DOMESTIC_HIGH", "COMMERCIAL"]


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    grid_zone: str
    base_load_kw: float       # average continuous draw, kW
    solar_capacity_kw: float  # 0.0 for households with no PV array
    billing_tier: str
    subsidy_flag: bool

    def as_dict(self) -> dict:
        return asdict(self)


def build_registry(num_households: int, num_zones: int) -> list[Household]:
    """Deterministically construct the household registry."""
    if num_zones > len(ZONE_NAMES):
        raise ValueError(f"num_zones must be <= {len(ZONE_NAMES)}")
    rng = random.Random(REGISTRY_SEED)
    zones = ZONE_NAMES[:num_zones]

    households: list[Household] = []
    for i in range(num_households):
        hid = f"HH-{i:05d}"
        # Round-robin so every zone is populated evenly regardless of scale.
        zone = zones[i % num_zones]
        tier = rng.choice(BILLING_TIERS)
        # Commercial premises draw noticeably more than domestic ones.
        base = rng.uniform(0.9, 2.4) if tier == "COMMERCIAL" else rng.uniform(0.25, 1.1)
        # ~60% of households have rooftop PV.
        solar = round(rng.uniform(1.5, 6.0), 2) if rng.random() < 0.6 else 0.0
        households.append(
            Household(
                household_id=hid,
                meter_id=f"MTR-{i:05d}",
                grid_zone=zone,
                base_load_kw=round(base, 3),
                solar_capacity_kw=solar,
                billing_tier=tier,
                # Low-tier and subsidised customers overlap but are not identical.
                subsidy_flag=(tier == "DOMESTIC_LOW" and rng.random() < 0.7),
            )
        )
    return households


def solar_factor(hour_float: float, cloud_cover_pct: float) -> float:
    """Fraction of nameplate PV capacity produced at a given hour of day.

    A half-sine bell between sunrise (06:00) and sunset (18:00), attenuated by
    cloud cover. Returns 0.0 outside daylight hours -- which is exactly what makes
    the "renewable contribution is low" alert fire every simulated night, giving
    the alerting path something real to detect.
    """
    sunrise, sunset = 6.0, 18.0
    if not (sunrise < hour_float < sunset):
        return 0.0
    position = (hour_float - sunrise) / (sunset - sunrise)  # 0..1 across the day
    bell = math.sin(math.pi * position)
    attenuation = 1.0 - 0.75 * (max(0.0, min(100.0, cloud_cover_pct)) / 100.0)
    return round(bell * attenuation, 4)


def demand_factor(hour_float: float) -> float:
    """Household demand multiplier over the day: overnight trough, evening peak."""
    # Two peaks (morning ~07:00, evening ~19:30) on top of a night-time baseline.
    morning = math.exp(-((hour_float - 7.0) ** 2) / 2.0)
    evening = 1.4 * math.exp(-((hour_float - 19.5) ** 2) / 3.0)
    return round(0.55 + 0.6 * morning + 0.6 * evening, 4)
