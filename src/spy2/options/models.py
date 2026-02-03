from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Iterable

import pandas as pd


@dataclasses.dataclass(frozen=True)
class OptionDefinition:
    symbol: str
    underlying: str
    expiration: dt.date
    strike: float
    right: str
    multiplier: int = 100


@dataclasses.dataclass(frozen=True)
class OptionQuote:
    symbol: str
    ts_event: dt.datetime
    bid: float | None
    ask: float | None
    bid_size: int | None = None
    ask_size: int | None = None

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2.0


@dataclasses.dataclass(frozen=True)
class OptionLeg:
    symbol: str
    right: str
    expiration: dt.date
    strike: float
    side: int
    quantity: int = 1
    price: float | None = None

    def signed_price(self) -> float | None:
        if self.price is None:
            return None
        return self.side * self.price * self.quantity


@dataclasses.dataclass(frozen=True)
class VerticalSpread:
    long_leg: OptionLeg
    short_leg: OptionLeg
    multiplier: int = 100

    @classmethod
    def from_legs(
        cls, leg_a: OptionLeg, leg_b: OptionLeg, *, multiplier: int = 100
    ) -> "VerticalSpread":
        if leg_a.side == leg_b.side:
            raise ValueError("Vertical spread requires one long and one short leg.")
        long_leg = leg_a if leg_a.side > 0 else leg_b
        short_leg = leg_b if long_leg is leg_a else leg_a

        _validate_vertical_legs(long_leg, short_leg)
        return cls(long_leg=long_leg, short_leg=short_leg, multiplier=multiplier)

    @property
    def right(self) -> str:
        return self.long_leg.right

    @property
    def expiration(self) -> dt.date:
        return self.long_leg.expiration

    @property
    def width(self) -> float:
        return abs(self.short_leg.strike - self.long_leg.strike)

    @property
    def net_debit(self) -> float | None:
        if self.long_leg.price is None or self.short_leg.price is None:
            return None
        return (
            self.long_leg.price * self.long_leg.quantity
            - self.short_leg.price * self.short_leg.quantity
        )

    @property
    def net_credit(self) -> float | None:
        net_debit = self.net_debit
        return None if net_debit is None else -net_debit

    @property
    def max_profit(self) -> float | None:
        net_debit = self.net_debit
        if net_debit is None:
            return None
        long_strike = self.long_leg.strike
        short_strike = self.short_leg.strike
        width = self.width

        if self.right == "C":
            if long_strike < short_strike:
                return width - net_debit
            net_credit = -net_debit
            return net_credit
        if self.right == "P":
            if long_strike > short_strike:
                return width - net_debit
            net_credit = -net_debit
            return net_credit
        return None

    @property
    def max_loss(self) -> float | None:
        net_debit = self.net_debit
        if net_debit is None:
            return None
        long_strike = self.long_leg.strike
        short_strike = self.short_leg.strike
        width = self.width

        if self.right == "C":
            if long_strike < short_strike:
                return net_debit
            net_credit = -net_debit
            return width - net_credit
        if self.right == "P":
            if long_strike > short_strike:
                return net_debit
            net_credit = -net_debit
            return width - net_credit
        return None

    @property
    def breakeven(self) -> float | None:
        net_debit = self.net_debit
        if net_debit is None:
            return None
        long_strike = self.long_leg.strike
        short_strike = self.short_leg.strike

        if self.right == "C":
            if long_strike < short_strike:
                return long_strike + net_debit
            net_credit = -net_debit
            return short_strike + net_credit
        if self.right == "P":
            if long_strike > short_strike:
                return long_strike - net_debit
            net_credit = -net_debit
            return short_strike - net_credit
        return None

    @property
    def assignment_bounds(self) -> tuple[float, float]:
        low = min(self.long_leg.strike, self.short_leg.strike)
        high = max(self.long_leg.strike, self.short_leg.strike)
        return (low, high)


@dataclasses.dataclass(frozen=True)
class OptionChainSnapshot:
    ts_event: dt.datetime
    underlying_price: float | None
    chain: pd.DataFrame

    def iter_definitions(self) -> Iterable[OptionDefinition]:
        for row in self.chain.itertuples(index=False):
            yield OptionDefinition(
                symbol=row.symbol,
                underlying=row.underlying,
                expiration=row.expiration,
                strike=row.strike,
                right=row.right,
            )


def _validate_vertical_legs(long_leg: OptionLeg, short_leg: OptionLeg) -> None:
    if long_leg.side <= 0 or short_leg.side >= 0:
        raise ValueError("Leg sides must be long (+1) and short (-1).")
    if long_leg.right != short_leg.right:
        raise ValueError("Vertical spread legs must share the same right.")
    if long_leg.expiration != short_leg.expiration:
        raise ValueError("Vertical spread legs must share the same expiration.")
