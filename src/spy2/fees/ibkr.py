from __future__ import annotations

import dataclasses
import os
from typing import Iterable

from spy2.options.fill import FillResult, SpreadFill
from spy2.options.models import OptionLeg


@dataclasses.dataclass(frozen=True)
class IbkrFeeSchedule:
    per_contract: float
    min_per_leg: float
    regulatory_per_contract: float
    transaction_per_contract: float
    sec_fee_rate: float
    contract_multiplier: int = 100

    @classmethod
    def from_env(cls) -> "IbkrFeeSchedule":
        return cls(
            per_contract=_env_float("SPY2_IBKR_PER_CONTRACT", 0.0),
            min_per_leg=_env_float("SPY2_IBKR_MIN_PER_LEG", 0.0),
            regulatory_per_contract=_env_float("SPY2_IBKR_REG_PER_CONTRACT", 0.0),
            transaction_per_contract=_env_float("SPY2_IBKR_TRANS_PER_CONTRACT", 0.0),
            sec_fee_rate=_env_float("SPY2_IBKR_SEC_FEE_RATE", 0.0),
            contract_multiplier=int(_env_float("SPY2_IBKR_CONTRACT_MULTIPLIER", 100)),
        )


@dataclasses.dataclass(frozen=True)
class FeeBreakdown:
    commission: float
    regulatory: float
    transaction: float
    sec_fee: float
    total: float


@dataclasses.dataclass(frozen=True)
class SpreadFeeBreakdown:
    per_leg: list[FeeBreakdown]
    commission: float
    regulatory: float
    transaction: float
    sec_fee: float
    total: float


def estimate_leg_fee(
    leg: OptionLeg,
    fill: FillResult,
    schedule: IbkrFeeSchedule,
) -> FeeBreakdown:
    contracts = leg.quantity
    commission = max(schedule.per_contract * contracts, schedule.min_per_leg)
    regulatory = schedule.regulatory_per_contract * contracts
    transaction = schedule.transaction_per_contract * contracts if leg.side < 0 else 0.0
    sec_fee = 0.0
    if fill.price is not None and leg.side < 0:
        notional = fill.price * contracts * schedule.contract_multiplier
        sec_fee = schedule.sec_fee_rate * notional
    total = commission + regulatory + transaction + sec_fee
    return FeeBreakdown(
        commission=commission,
        regulatory=regulatory,
        transaction=transaction,
        sec_fee=sec_fee,
        total=total,
    )


def estimate_spread_fees(
    spread: OptionLeg | Iterable[OptionLeg] | SpreadFill,
    fill: SpreadFill | None = None,
    schedule: IbkrFeeSchedule | None = None,
) -> SpreadFeeBreakdown:
    if schedule is None:
        schedule = IbkrFeeSchedule.from_env()

    if isinstance(spread, SpreadFill):
        fills = spread.leg_fills
        legs: Iterable[OptionLeg] = [spread.spread.long_leg, spread.spread.short_leg]
    else:
        if fill is None:
            raise ValueError("fill is required when spread is not a SpreadFill.")
        legs = spread if isinstance(spread, Iterable) else [spread]
        fills = fill.leg_fills

    per_leg: list[FeeBreakdown] = []
    commission_total = 0.0
    regulatory_total = 0.0
    transaction_total = 0.0
    sec_fee_total = 0.0

    for leg, fill_result in zip(legs, fills):
        breakdown = estimate_leg_fee(leg, fill_result, schedule)
        per_leg.append(breakdown)
        commission_total += breakdown.commission
        regulatory_total += breakdown.regulatory
        transaction_total += breakdown.transaction
        sec_fee_total += breakdown.sec_fee

    total = commission_total + regulatory_total + transaction_total + sec_fee_total
    return SpreadFeeBreakdown(
        per_leg=per_leg,
        commission=commission_total,
        regulatory=regulatory_total,
        transaction=transaction_total,
        sec_fee=sec_fee_total,
        total=total,
    )


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return float(raw)
