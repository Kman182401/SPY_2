from __future__ import annotations

import dataclasses
import math
from collections.abc import Callable
from typing import Mapping

from spy2.options.models import OptionLeg, VerticalSpread


def _finite_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


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


@dataclasses.dataclass(frozen=True)
class SpreadQuote:
    """Synthetic spread-level NBBO derived from leg NBBO."""

    long_bid: float
    long_ask: float
    short_bid: float
    short_ask: float
    net_bid: float
    net_ask: float
    net_mid: float
    net_width: float


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
    tick_size: float | None = None,
) -> FillResult:
    bid = _finite_or_none(bid)
    ask = _finite_or_none(ask)
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
    if tick_size is not None:
        if not math.isfinite(tick_size) or tick_size <= 0:
            raise ValueError("tick_size must be a finite positive number.")
        from spy2.fees.tick import round_price_for_side

        price = round_price_for_side(price, tick_size, leg.side)
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
    tick_size_fn: Callable[[str], float] | None = None,
) -> SpreadFill:
    fills: list[FillResult] = []
    net_debit: float | None = 0.0
    for leg in (spread.long_leg, spread.short_leg):
        bid, ask = quotes_by_symbol.get(leg.symbol, (None, None))
        tick_size = tick_size_fn(leg.symbol) if tick_size_fn else None
        fill = _fill_leg(
            leg,
            bid,
            ask,
            slippage_bps=slippage_bps,
            tick_size=tick_size,
        )
        fills.append(fill)
        if fill.price is None:
            net_debit = None
        elif net_debit is not None:
            net_debit += leg.side * fill.price

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


def quote_vertical_spread_nbbo(
    spread: VerticalSpread,
    quotes_by_symbol: Mapping[str, tuple[float | None, float | None]],
) -> SpreadQuote | None:
    """
    Compute a synthetic NBBO for a vertical spread (net debit price space) from leg NBBO.

    Conventions:
    - spread.net_debit = long_price - short_price
    - net_bid uses (long bid) - (short ask)
    - net_ask uses (long ask) - (short bid)
    """
    long_bid, long_ask = quotes_by_symbol.get(spread.long_leg.symbol, (None, None))
    short_bid, short_ask = quotes_by_symbol.get(spread.short_leg.symbol, (None, None))
    long_bid = _finite_or_none(long_bid)
    long_ask = _finite_or_none(long_ask)
    short_bid = _finite_or_none(short_bid)
    short_ask = _finite_or_none(short_ask)
    if long_bid is None or long_ask is None or short_bid is None or short_ask is None:
        return None
    if long_ask < long_bid or short_ask < short_bid:
        return None

    net_bid = float(long_bid) - float(short_ask)
    net_ask = float(long_ask) - float(short_bid)
    net_mid = (net_bid + net_ask) / 2.0
    net_width = net_ask - net_bid
    return SpreadQuote(
        long_bid=float(long_bid),
        long_ask=float(long_ask),
        short_bid=float(short_bid),
        short_ask=float(short_ask),
        net_bid=float(net_bid),
        net_ask=float(net_ask),
        net_mid=float(net_mid),
        net_width=float(net_width),
    )


def _round_up_to_tick(price: float, tick_size: float) -> float:
    if not math.isfinite(price):
        raise ValueError("price must be a finite number.")
    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("tick_size must be positive.")
    factor = price / tick_size
    # If already close to a valid increment, keep it stable.
    if abs(factor - round(factor)) < 1e-9:
        steps = int(round(factor))
    else:
        steps = math.ceil(factor)
    return steps * tick_size


def fill_vertical_spread_inside(
    spread: VerticalSpread,
    quotes_by_symbol: Mapping[str, tuple[float | None, float | None]],
    *,
    alpha: float,
    slippage_bps: float = 0.0,
    net_tick_size_fn: Callable[[str], float] | None = None,
    leg_tick_size_fn: Callable[[str], float] | None = None,
) -> SpreadFill:
    """
    Spread-native fill model.

    - Compute synthetic spread NBBO from leg NBBO.
    - Fill at: net_mid + alpha * (net_ask - net_mid), where alpha in [0, 1].
      alpha=0 -> mid, alpha=1 -> net ask (cross).
    - Net price is rounded up to the spread tick size (conservative, avoids half-ticks).
    - Leg prices are synthesized inside their own spreads to match the chosen net.
      By default, we do not tick-round legs (leg_tick_size_fn=None) because the
      fill is driven by the spread limit, not independent leg limits.
    """
    if not (0.0 <= alpha <= 1.0):
        raise ValueError("alpha must be between 0 and 1 inclusive.")

    quote = quote_vertical_spread_nbbo(spread, quotes_by_symbol)
    if quote is None:
        # Fall back to the leg-based fill logic; this will typically return net_debit=None
        # if either leg lacks usable quotes.
        return fill_vertical_spread(
            spread,
            quotes_by_symbol,
            slippage_bps=slippage_bps,
            tick_size_fn=leg_tick_size_fn,
        )

    long_mid = (quote.long_bid + quote.long_ask) / 2.0
    short_mid = (quote.short_bid + quote.short_ask) / 2.0
    long_hs = (quote.long_ask - quote.long_bid) / 2.0
    short_hs = (quote.short_ask - quote.short_bid) / 2.0
    total_hs = long_hs + short_hs

    target_net = quote.net_mid + alpha * (quote.net_ask - quote.net_mid)
    if net_tick_size_fn is not None:
        tick = float(net_tick_size_fn(spread.long_leg.symbol))
        target_net = _round_up_to_tick(float(target_net), tick)

    if total_hs <= 0:
        long_px = long_mid
        short_px = short_mid
    else:
        delta = target_net - quote.net_mid
        w_long = long_hs / total_hs
        w_short = short_hs / total_hs
        long_px = long_mid + w_long * delta
        short_px = short_mid - w_short * delta

    use_quotes = {
        spread.long_leg.symbol: (long_px, long_px),
        spread.short_leg.symbol: (short_px, short_px),
    }
    return fill_vertical_spread(
        spread,
        use_quotes,
        slippage_bps=slippage_bps,
        tick_size_fn=leg_tick_size_fn,
    )
