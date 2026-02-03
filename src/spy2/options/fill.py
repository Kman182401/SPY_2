from __future__ import annotations

import dataclasses
from typing import Mapping

from spy2.options.models import OptionLeg, VerticalSpread


@dataclasses.dataclass(frozen=True)
class FillResult:
    symbol: str
    side: int
    bid: float | None
    ask: float | None
    mid: float | None
    slippage: float | None
    price: float | None


@dataclasses.dataclass(frozen=True)
class SpreadFill:
    spread: VerticalSpread
    net_debit: float | None
    leg_fills: list[FillResult]


def _mid_price(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def _fill_leg(
    leg: OptionLeg,
    bid: float | None,
    ask: float | None,
    *,
    slippage_bps: float,
) -> FillResult:
    mid = _mid_price(bid, ask)
    if leg.side > 0:
        base = ask if ask is not None else mid
    else:
        base = bid if bid is not None else mid
    if base is None:
        return FillResult(
            symbol=leg.symbol,
            side=leg.side,
            bid=bid,
            ask=ask,
            mid=mid,
            slippage=None,
            price=None,
        )
    slippage = base * (slippage_bps / 10000.0)
    price = base + slippage if leg.side > 0 else base - slippage
    return FillResult(
        symbol=leg.symbol,
        side=leg.side,
        bid=bid,
        ask=ask,
        mid=mid,
        slippage=slippage,
        price=price,
    )


def fill_vertical_spread(
    spread: VerticalSpread,
    quotes_by_symbol: Mapping[str, tuple[float | None, float | None]],
    *,
    slippage_bps: float = 0.0,
) -> SpreadFill:
    fills: list[FillResult] = []
    net_debit: float | None = 0.0
    for leg in (spread.long_leg, spread.short_leg):
        bid, ask = quotes_by_symbol.get(leg.symbol, (None, None))
        fill = _fill_leg(leg, bid, ask, slippage_bps=slippage_bps)
        fills.append(fill)
        if fill.price is None:
            net_debit = None
        elif net_debit is not None:
            net_debit += leg.side * fill.price * leg.quantity

    return SpreadFill(spread=spread, net_debit=net_debit, leg_fills=fills)


def fill_spread(
    spread: VerticalSpread,
    quotes_by_symbol: Mapping[str, tuple[float | None, float | None]],
    *,
    slippage_bps: float = 0.0,
) -> SpreadFill:
    return fill_vertical_spread(
        spread,
        quotes_by_symbol,
        slippage_bps=slippage_bps,
    )
