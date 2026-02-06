from __future__ import annotations

import dataclasses
import os

import pandas as pd


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default


@dataclasses.dataclass(frozen=True)
class LiquidityFilterConfig:
    enabled: bool = True
    require_stats: bool = True

    min_open_interest: int = 500
    min_volume: int = 50

    # Reject legs with extremely wide markets relative to mid.
    max_abs_bid_ask: float = 0.05
    max_rel_bid_ask: float = 0.30

    @classmethod
    def from_env(cls) -> "LiquidityFilterConfig":
        return cls(
            enabled=_env_bool("SPY2_LIQ_ENABLED", True),
            require_stats=_env_bool("SPY2_LIQ_REQUIRE_STATS", True),
            min_open_interest=_env_int("SPY2_LIQ_MIN_OI", 500),
            min_volume=_env_int("SPY2_LIQ_MIN_VOLUME", 50),
            max_abs_bid_ask=_env_float("SPY2_LIQ_MAX_ABS_BID_ASK", 0.05),
            max_rel_bid_ask=_env_float("SPY2_LIQ_MAX_REL_BID_ASK", 0.30),
        )


def filter_liquid_chain(
    chain: pd.DataFrame,
    *,
    config: LiquidityFilterConfig,
) -> pd.DataFrame:
    if not config.enabled:
        return chain

    required_cols = {"symbol", "expiration", "strike", "right", "bid", "ask"}
    if not required_cols.issubset(chain.columns):
        return chain.iloc[0:0].copy()

    df = chain.copy()
    df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
    df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
    df = df.dropna(subset=["bid", "ask"])

    mid = (df["bid"] + df["ask"]) / 2.0
    width = df["ask"] - df["bid"]

    threshold = config.max_abs_bid_ask
    if config.max_rel_bid_ask > 0:
        threshold = (config.max_rel_bid_ask * mid).where(mid.notna(), other=0.0)
        threshold = threshold.clip(lower=config.max_abs_bid_ask)

    df = df[width <= threshold]

    if config.require_stats:
        if "open_interest" not in df.columns or "volume" not in df.columns:
            return df.iloc[0:0].copy()
        df["open_interest"] = pd.to_numeric(
            df["open_interest"], errors="coerce"
        ).fillna(0)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        df = df[(df["open_interest"] >= config.min_open_interest)]
        df = df[(df["volume"] >= config.min_volume)]

    return df
