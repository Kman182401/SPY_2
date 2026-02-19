from __future__ import annotations

import dataclasses
import datetime as dt
import math
from bisect import bisect_left, bisect_right
from typing import Literal

from spy2.options.models import OptionLeg, OptionChainSnapshot, VerticalSpread
from spy2.options.liquidity import LiquidityFilterConfig, filter_liquid_chain


def select_vertical_spread(
    snapshot: OptionChainSnapshot,
    *,
    right: Literal["C", "P"] = "C",
    width: float = 1.0,
    structure: Literal["debit", "credit"] = "debit",
    allow_fallback_right: bool = True,
    liquidity: LiquidityFilterConfig | None = None,
) -> tuple[VerticalSpread, str] | None:
    """
    Pick a simple 1-lot vertical from a single snapshot.

    This is intentionally naive and deterministic: pick the earliest expiration
    with >=2 strikes, then choose strikes around spot with the requested width.
    """
    requested_right = right

    base_chain = snapshot.chain.dropna(
        subset=["symbol", "expiration", "strike", "right"]
    )
    if liquidity is not None:
        base_chain = filter_liquid_chain(base_chain, config=liquidity)
    right_chain = base_chain[base_chain["right"] == requested_right]
    used_right = requested_right
    if right_chain.empty and allow_fallback_right:
        used_right = "P" if requested_right == "C" else "C"
        right_chain = base_chain[base_chain["right"] == used_right]
    if right_chain.empty:
        return None

    expirations = sorted(set(right_chain["expiration"]))
    subset = None
    strikes: list[float] = []
    expiration = None
    for exp in expirations:
        subset_candidate = right_chain[right_chain["expiration"] == exp]
        strikes_candidate = sorted(set(subset_candidate["strike"]))
        if len(strikes_candidate) >= 2:
            subset = subset_candidate
            strikes = strikes_candidate
            expiration = exp
            break
    if subset is None or expiration is None:
        return None

    spot = snapshot.underlying_price
    if spot is None:
        spot = strikes[len(strikes) // 2]

    # Convention: long_leg.side=+1, short_leg.side=-1. "structure" controls which strike
    # is long vs short (debit vs credit).
    if used_right == "C":
        anchor_candidates = [strike for strike in strikes if strike >= spot]
        anchor = anchor_candidates[0] if anchor_candidates else strikes[-1]
        if structure == "debit":
            long_strike = anchor
            short_target = long_strike + width
            if short_target in strikes:
                short_strike = short_target
            else:
                higher = [strike for strike in strikes if strike > long_strike]
                if not higher:
                    return None
                short_strike = min(
                    higher, key=lambda strike: abs(strike - short_target)
                )
        else:
            short_strike = anchor
            long_target = short_strike + width
            if long_target in strikes:
                long_strike = long_target
            else:
                higher = [strike for strike in strikes if strike > short_strike]
                if not higher:
                    return None
                long_strike = min(higher, key=lambda strike: abs(strike - long_target))
    else:
        anchor_candidates = [strike for strike in strikes if strike <= spot]
        anchor = anchor_candidates[-1] if anchor_candidates else strikes[0]
        if structure == "debit":
            long_strike = anchor
            short_target = long_strike - width
            if short_target in strikes:
                short_strike = short_target
            else:
                lower = [strike for strike in strikes if strike < long_strike]
                if not lower:
                    return None
                short_strike = min(lower, key=lambda strike: abs(strike - short_target))
        else:
            short_strike = anchor
            long_target = short_strike - width
            if long_target in strikes:
                long_strike = long_target
            else:
                lower = [strike for strike in strikes if strike < short_strike]
                if not lower:
                    return None
                long_strike = min(lower, key=lambda strike: abs(strike - long_target))

    long_row = subset[subset["strike"] == long_strike].iloc[0]
    short_row = subset[subset["strike"] == short_strike].iloc[0]

    long_leg = OptionLeg(
        symbol=long_row.symbol,
        right=used_right,
        expiration=long_row.expiration,
        strike=float(long_row.strike),
        side=1,
        quantity=1,
    )
    short_leg = OptionLeg(
        symbol=short_row.symbol,
        right=used_right,
        expiration=short_row.expiration,
        strike=float(short_row.strike),
        side=-1,
        quantity=1,
    )
    spread = VerticalSpread.from_legs(long_leg, short_leg)
    return (spread, used_right)


@dataclasses.dataclass(frozen=True)
class VerticalSelectionConfig:
    dte_min: int = 21
    dte_max: int = 45
    otm_pct: float = 0.01
    min_credit: float = 0.20


@dataclasses.dataclass(frozen=True)
class _CreditRow:
    symbol: str
    expiration: dt.date
    strike: float
    bid: float
    ask: float


def select_vertical_spread_otm_credit(
    snapshot: OptionChainSnapshot,
    *,
    right: Literal["C", "P"],
    width: float,
    config: VerticalSelectionConfig,
    allow_fallback_right: bool = False,
    liquidity: LiquidityFilterConfig | None = None,
) -> tuple[VerticalSpread, str] | None:
    """
    Deterministic OTM credit spread selector (v1).

    - Scans expirations in the configured DTE window.
    - Chooses the closest-to-spot OTM short strike (by percent), then uses `width`
      for the long strike.
    - Requires a minimum conservative credit estimate.
    """
    spot = snapshot.underlying_price
    if spot is None:
        return None

    dte_min = int(config.dte_min)
    dte_max = int(config.dte_max)
    if dte_min < 0 or dte_max < dte_min:
        raise ValueError("Invalid DTE window.")

    otm_pct = float(config.otm_pct)
    if otm_pct < 0:
        raise ValueError("otm_pct must be >= 0.")

    min_credit = float(config.min_credit)
    if min_credit < 0:
        raise ValueError("min_credit must be >= 0.")

    liq = liquidity or LiquidityFilterConfig()
    rights_to_try = [right]
    if allow_fallback_right:
        rights_to_try.append("P" if right == "C" else "C")

    trade_date = snapshot.ts_event.date()
    width = float(width)

    def _finite_or_none(value: object) -> float | None:
        try:
            out = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        if not math.isfinite(out):
            return None
        return out

    for used_right in rights_to_try:
        # Group rows by expiration and strike with O(1) lookup by strike.
        by_exp: dict[dt.date, dict[float, _CreditRow]] = {}
        for row in snapshot.chain.itertuples(index=False):
            if getattr(row, "right", None) != used_right:
                continue
            symbol = getattr(row, "symbol", None)
            exp_val = getattr(row, "expiration", None)
            expiration: dt.date | None
            if isinstance(exp_val, dt.datetime):
                expiration = exp_val.date()
            elif isinstance(exp_val, dt.date):
                expiration = exp_val
            else:
                expiration = None
            strike = _finite_or_none(getattr(row, "strike", None))
            bid = _finite_or_none(getattr(row, "bid", None))
            ask = _finite_or_none(getattr(row, "ask", None))
            if (
                symbol is None
                or expiration is None
                or strike is None
                or bid is None
                or ask is None
            ):
                continue

            # Inline liquidity checks (same semantics as filter_liquid_chain).
            width_px = ask - bid
            mid = (bid + ask) / 2.0
            threshold = max(liq.max_abs_bid_ask, liq.max_rel_bid_ask * mid)
            if width_px > threshold:
                continue
            if liq.require_stats:
                oi = _finite_or_none(getattr(row, "open_interest", None))
                vol = _finite_or_none(getattr(row, "volume", None))
                if (oi or 0.0) < liq.min_open_interest:
                    continue
                if (vol or 0.0) < liq.min_volume:
                    continue

            rec = _CreditRow(
                symbol=str(symbol),
                expiration=expiration,
                strike=strike,
                bid=bid,
                ask=ask,
            )
            exp_map = by_exp.setdefault(expiration, {})
            # Keep first row at strike for deterministic behavior.
            exp_map.setdefault(round(strike, 6), rec)

        if not by_exp:
            continue

        expirations = sorted(by_exp.keys())
        for exp in expirations:
            dte = (exp - trade_date).days
            if dte < dte_min or dte > dte_max:
                continue
            strike_map = by_exp[exp]
            strikes = sorted(strike_map.keys())
            if len(strikes) < 2:
                continue

            if used_right == "P":
                short_max = float(spot) * (1.0 - otm_pct)
                i = bisect_right(strikes, short_max)
                short_candidates = list(reversed(strikes[:i]))  # closest OTM first

                def _long_for_short(short_strike: float) -> float:
                    return round(short_strike - width, 6)
            else:
                short_min = float(spot) * (1.0 + otm_pct)
                i = bisect_left(strikes, short_min)
                short_candidates = strikes[i:]  # closest OTM first

                def _long_for_short(short_strike: float) -> float:
                    return round(short_strike + width, 6)

            for short_strike in short_candidates:
                long_strike = _long_for_short(short_strike)
                short_row = strike_map.get(short_strike)
                long_row = strike_map.get(long_strike)
                if short_row is None or long_row is None:
                    continue
                credit = short_row.bid - long_row.ask
                if credit < min_credit:
                    continue

                long_leg = OptionLeg(
                    symbol=long_row.symbol,
                    right=used_right,
                    expiration=long_row.expiration,
                    strike=float(long_row.strike),
                    side=1,
                    quantity=1,
                )
                short_leg = OptionLeg(
                    symbol=short_row.symbol,
                    right=used_right,
                    expiration=short_row.expiration,
                    strike=float(short_row.strike),
                    side=-1,
                    quantity=1,
                )
                spread = VerticalSpread.from_legs(long_leg, short_leg)
                return (spread, used_right)

    return None


def priced_spread_from_fill(
    spread: VerticalSpread, *, leg_prices: dict[str, float | None]
) -> VerticalSpread:
    priced_long = dataclasses.replace(
        spread.long_leg, price=leg_prices.get(spread.long_leg.symbol)
    )
    priced_short = dataclasses.replace(
        spread.short_leg, price=leg_prices.get(spread.short_leg.symbol)
    )
    return VerticalSpread.from_legs(
        priced_long, priced_short, multiplier=spread.multiplier
    )
