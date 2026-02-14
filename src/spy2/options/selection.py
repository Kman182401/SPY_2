from __future__ import annotations

import dataclasses
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
