from __future__ import annotations

import math
import os


def tick_size_for_symbol(symbol: str) -> float:
    env_default = os.getenv("SPY2_TICK_SIZE_DEFAULT")
    if env_default:
        return float(env_default)
    env_spy = os.getenv("SPY2_TICK_SIZE_SPY")
    if symbol.upper().startswith("SPY") and env_spy:
        return float(env_spy)
    if symbol.upper().startswith("SPY"):
        return 0.01
    return 0.01


def round_price_for_side(price: float, tick_size: float, side: int) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive.")
    factor = price / tick_size
    if side > 0:
        return math.ceil(factor) * tick_size
    return math.floor(factor) * tick_size
