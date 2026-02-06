import datetime as dt

import pandas as pd

from spy2.options.liquidity import LiquidityFilterConfig, filter_liquid_chain
from spy2.options.models import OptionChainSnapshot
from spy2.options.selection import select_vertical_spread


def test_filter_liquid_chain_requires_stats_and_narrow_markets():
    expiration = dt.date(2026, 2, 19)
    chain = pd.DataFrame(
        [
            {
                "symbol": "A",
                "expiration": expiration,
                "strike": 100.0,
                "right": "P",
                "bid": 1.00,
                "ask": 2.00,
                "open_interest": 10_000,
                "volume": 10_000,
            },  # too wide
            {
                "symbol": "B",
                "expiration": expiration,
                "strike": 99.0,
                "right": "P",
                "bid": 1.00,
                "ask": 1.04,
                "open_interest": 100,
                "volume": 10_000,
            },  # low OI
            {
                "symbol": "C",
                "expiration": expiration,
                "strike": 98.0,
                "right": "P",
                "bid": 1.00,
                "ask": 1.04,
                "open_interest": 10_000,
                "volume": 10,
            },  # low volume
            {
                "symbol": "D",
                "expiration": expiration,
                "strike": 97.0,
                "right": "P",
                "bid": 1.00,
                "ask": 1.03,
                "open_interest": 10_000,
                "volume": 10_000,
            },  # passes
        ]
    )
    cfg = LiquidityFilterConfig(
        enabled=True,
        require_stats=True,
        min_open_interest=500,
        min_volume=50,
        max_abs_bid_ask=0.05,
        max_rel_bid_ask=0.30,
    )
    filtered = filter_liquid_chain(chain, config=cfg)
    assert list(filtered["symbol"]) == ["D"]


def test_select_vertical_spread_returns_none_when_liquidity_filters_leave_too_few_strikes():
    expiration = dt.date(2026, 2, 19)
    chain = pd.DataFrame(
        [
            {
                "symbol": "P100",
                "expiration": expiration,
                "strike": 100.0,
                "right": "P",
                "bid": 1.00,
                "ask": 1.03,
                "open_interest": 10_000,
                "volume": 10_000,
            },
            {
                "symbol": "P99",
                "expiration": expiration,
                "strike": 99.0,
                "right": "P",
                "bid": 1.00,
                "ask": 1.03,
                "open_interest": 10,
                "volume": 10_000,
            },  # filtered out
        ]
    )
    snap = OptionChainSnapshot(
        ts_event=dt.datetime(2026, 2, 2, 14, 30, tzinfo=dt.timezone.utc),
        underlying_price=100.0,
        chain=chain,
    )
    cfg = LiquidityFilterConfig(
        enabled=True,
        require_stats=True,
        min_open_interest=500,
        min_volume=50,
        max_abs_bid_ask=0.05,
        max_rel_bid_ask=0.30,
    )
    sel = select_vertical_spread(
        snap,
        right="P",
        width=1.0,
        allow_fallback_right=False,
        liquidity=cfg,
    )
    assert sel is None
