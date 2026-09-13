"""Tariff rules for the batch billing layer.

The batch job expresses these rules as Spark SQL columns so that no Python UDF is
needed on executors. That translation is easy to get subtly wrong, so the rules
also live here as a plain-Python reference implementation. ``tests/test_billing.py``
runs both over the same inputs and asserts they agree, which is what stops a
refactor of the Spark expression from quietly changing everybody's bill.

Charging model
--------------
Only *net import* is charged: solar generated and consumed on site never reaches
the meter. Net import is then priced in three blocks, so heavy users pay a
premium on the top slice:

    block 1   up to block1_cap                  at  tariff_rate
    block 2   the next block2_cap kWh           at  tariff_rate * block2_mult
    block 3   everything above that             at  tariff_rate * block3_mult

Surplus solar is exported and credited at export_rate, which is deliberately set
below the import rate (as it is in most real feed-in schemes), so exporting never
turns a bill negative through arbitrage alone. Subsidised households receive a
flat percentage discount on the energy charge only -- not on the standing charge.
"""
from __future__ import annotations

from dataclasses import dataclass

# (billing_tier, block-1 cap kWh, block-2 cap kWh, block-2 multiplier, block-3 multiplier)
TIER_BLOCKS: list[tuple[str, float, float, float, float]] = [
    ("DOMESTIC_LOW", 6.0, 8.0, 1.25, 1.60),
    ("DOMESTIC_STD", 10.0, 12.0, 1.30, 1.70),
    ("DOMESTIC_HIGH", 14.0, 16.0, 1.35, 1.80),
    ("COMMERCIAL", 30.0, 40.0, 1.15, 1.30),
]

TIER_BLOCK_MAP = {t[0]: t[1:] for t in TIER_BLOCKS}

SUBSIDY_RATE = 0.20  # discount applied to the energy charge for subsidised customers


@dataclass(frozen=True)
class BillLines:
    net_import_kwh: float
    exported_kwh: float
    self_consumed_kwh: float
    billed_units_kwh: float
    energy_charge: float
    export_credit: float
    standing_charge: float
    subsidy_amount: float
    total_bill: float
    self_sufficiency_pct: float


def split_energy(consumption_kwh: float, solar_kwh: float) -> tuple[float, float, float]:
    """Split a household daily position into (self-consumed, net import, exported)."""
    self_consumed = min(consumption_kwh, solar_kwh)
    net_import = max(0.0, consumption_kwh - solar_kwh)
    exported = max(0.0, solar_kwh - consumption_kwh)
    return self_consumed, net_import, exported


def billed_units(net_import_kwh: float, billing_tier: str) -> float:
    """Apply the tiered block schedule, returning rate-multiplied units."""
    cap1, cap2, mult2, mult3 = TIER_BLOCK_MAP[billing_tier]
    b1 = min(net_import_kwh, cap1)
    b2 = min(max(net_import_kwh - cap1, 0.0), cap2)
    b3 = max(net_import_kwh - cap1 - cap2, 0.0)
    return b1 + b2 * mult2 + b3 * mult3


def compute_bill(
    consumption_kwh: float,
    solar_kwh: float,
    billing_tier: str,
    tariff_rate: float,
    export_rate: float,
    standing_charge: float,
    subsidy_flag: bool,
) -> BillLines:
    """Reference implementation of one household daily bill."""
    self_consumed, net_import, exported = split_energy(consumption_kwh, solar_kwh)
    units = billed_units(net_import, billing_tier)

    energy_charge = tariff_rate * units
    export_credit = export_rate * exported
    subsidy_amount = energy_charge * SUBSIDY_RATE if subsidy_flag else 0.0
    total = energy_charge - export_credit + standing_charge - subsidy_amount

    return BillLines(
        net_import_kwh=net_import,
        exported_kwh=exported,
        self_consumed_kwh=self_consumed,
        billed_units_kwh=units,
        energy_charge=energy_charge,
        export_credit=export_credit,
        standing_charge=standing_charge,
        subsidy_amount=subsidy_amount,
        total_bill=total,
        self_sufficiency_pct=(
            self_consumed / consumption_kwh * 100.0 if consumption_kwh > 0 else 0.0
        ),
    )
