from __future__ import annotations

import dataclasses
import datetime as dt
import math
from typing import Any

from spy2.fees.ibkr import SpreadFeeBreakdown
from spy2.options.fill import SpreadFill
from spy2.options.models import OptionLeg, VerticalSpread


@dataclasses.dataclass
class FeeLedger:
    commission: float = 0.0
    regulatory: float = 0.0
    transaction: float = 0.0
    sec_fee: float = 0.0
    total: float = 0.0

    def add(self, fees: SpreadFeeBreakdown) -> None:
        self.commission += fees.commission
        self.regulatory += fees.regulatory
        self.transaction += fees.transaction
        self.sec_fee += fees.sec_fee
        self.total += fees.total


@dataclasses.dataclass
class SpreadPosition:
    position_id: str
    spread: VerticalSpread
    opened_at: dt.datetime

    entry_fill: SpreadFill
    entry_fees: SpreadFeeBreakdown
    entry_cashflow: float

    max_profit_dollars: float | None
    max_loss_dollars: float | None
    risk_exposure: float
    margin_required: float

    closed_at: dt.datetime | None = None
    exit_fill: SpreadFill | None = None
    exit_fees: SpreadFeeBreakdown | None = None
    exit_cashflow: float | None = None
    realized_pnl: float | None = None

    def is_open(self) -> bool:
        return self.closed_at is None


@dataclasses.dataclass
class PortfolioState:
    cash: float
    reserved_margin: float = 0.0
    realized_pnl: float = 0.0
    fees: FeeLedger = dataclasses.field(default_factory=FeeLedger)
    positions: dict[str, SpreadPosition] = dataclasses.field(default_factory=dict)

    def open_position(self, position: SpreadPosition) -> None:
        if position.position_id in self.positions:
            raise ValueError(f"Position already exists: {position.position_id}")
        self.positions[position.position_id] = position
        self.cash += position.entry_cashflow
        self.reserved_margin += position.margin_required
        self.fees.add(position.entry_fees)

    def close_position(
        self,
        position_id: str,
        *,
        closed_at: dt.datetime,
        exit_fill: SpreadFill,
        exit_fees: SpreadFeeBreakdown,
        exit_cashflow: float,
    ) -> SpreadPosition:
        if position_id not in self.positions:
            raise ValueError(f"Unknown position: {position_id}")
        position = self.positions[position_id]
        if not position.is_open():
            raise ValueError(f"Position already closed: {position_id}")

        position.closed_at = closed_at
        position.exit_fill = exit_fill
        position.exit_fees = exit_fees
        position.exit_cashflow = exit_cashflow
        position.realized_pnl = position.entry_cashflow + exit_cashflow

        self.cash += exit_cashflow
        self.reserved_margin -= position.margin_required
        self.fees.add(exit_fees)
        self.realized_pnl += position.realized_pnl

        return position

    def open_positions(self) -> list[SpreadPosition]:
        return [pos for pos in self.positions.values() if pos.is_open()]


def build_close_spread(position_spread: VerticalSpread) -> VerticalSpread:
    """
    Build a vertical spread representing the *closing* transaction for a held spread:

    - sell the original long leg
    - buy back the original short leg
    """
    buy_back = OptionLeg(
        symbol=position_spread.short_leg.symbol,
        right=position_spread.short_leg.right,
        expiration=position_spread.short_leg.expiration,
        strike=position_spread.short_leg.strike,
        side=1,
        quantity=position_spread.quantity,
    )
    sell_out = OptionLeg(
        symbol=position_spread.long_leg.symbol,
        right=position_spread.long_leg.right,
        expiration=position_spread.long_leg.expiration,
        strike=position_spread.long_leg.strike,
        side=-1,
        quantity=position_spread.quantity,
    )
    return VerticalSpread.from_legs(
        buy_back, sell_out, multiplier=position_spread.multiplier
    )


def cashflow_from_fill(fill: SpreadFill, *, fees: SpreadFeeBreakdown) -> float:
    if fill.net_debit is None:
        raise ValueError("fill.net_debit is required to compute cashflow.")
    contracts = fill.spread.quantity
    return -(fill.net_debit * fill.spread.multiplier * contracts) - fees.total


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out
