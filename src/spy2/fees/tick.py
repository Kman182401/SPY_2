from __future__ import annotations

import math
import os


def tick_size_for_symbol(symbol: str) -> float:
    env_spy = os.getenv("SPY2_TICK_SIZE_SPY")
    if symbol.upper().startswith("SPY") and env_spy:
        return float(env_spy)
    env_default = os.getenv("SPY2_TICK_SIZE_DEFAULT")
    if env_default:
        return float(env_default)
    if symbol.upper().startswith("SPY"):
        return 0.01
    return 0.01


def round_price_for_side(price: float, tick_size: float, side: int) -> float:
    if not math.isfinite(price):
        raise ValueError("price must be a finite number.")
    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("tick_size must be positive.")
    factor = price / tick_size
    # Exchange behavior for invalid increments: offers round up, bids round down.
    if side > 0:  # buy (bid)
        return math.floor(factor) * tick_size
    if side < 0:  # sell (offer)
        return math.ceil(factor) * tick_size
    raise ValueError("side must be non-zero.")
