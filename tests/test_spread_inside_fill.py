import datetime as dt

import pytest

from spy2.options.fill import (
    fill_vertical_spread,
    fill_vertical_spread_inside,
    quote_vertical_spread_nbbo,
)
from spy2.options.models import OptionLeg, VerticalSpread


def _tick(_symbol: str) -> float:
    return 0.01


def test_quote_vertical_spread_nbbo_math():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {
        "SPY_LONG": (1.00, 1.01),
        "SPY_SHORT": (1.50, 1.51),
    }
    q = quote_vertical_spread_nbbo(spread, quotes)
    assert q is not None
    assert q.net_bid == pytest.approx(1.00 - 1.51)
    assert q.net_ask == pytest.approx(1.01 - 1.50)
    assert q.net_mid == pytest.approx(-0.50)
    assert q.net_width == pytest.approx(0.02)


def test_fill_vertical_spread_inside_rounds_net_up_to_tick_and_matches():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {
        "SPY_LONG": (1.00, 1.01),
        "SPY_SHORT": (1.50, 1.51),
    }

    # net_mid=-0.50, net_ask=-0.49 -> alpha=0.5 target is -0.495, which rounds up to -0.49.
    fill = fill_vertical_spread_inside(
        spread,
        quotes,
        alpha=0.5,
        slippage_bps=0.0,
        net_tick_size_fn=_tick,
        leg_tick_size_fn=None,
    )
    assert fill.net_debit == pytest.approx(-0.49)


def test_fill_vertical_spread_inside_alpha_bounds():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {"SPY_LONG": (1.00, 1.01), "SPY_SHORT": (1.50, 1.51)}
    with pytest.raises(ValueError, match="alpha must be between 0 and 1"):
        fill_vertical_spread_inside(spread, quotes, alpha=-0.1)
    with pytest.raises(ValueError, match="alpha must be between 0 and 1"):
        fill_vertical_spread_inside(spread, quotes, alpha=1.1)


def test_quote_vertical_spread_nbbo_rejects_nan_quotes():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {
        "SPY_LONG": (1.00, float("nan")),
        "SPY_SHORT": (1.50, 1.51),
    }
    assert quote_vertical_spread_nbbo(spread, quotes) is None


def test_fill_vertical_spread_treats_nan_as_missing_and_does_not_raise():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {
        "SPY_LONG": (1.00, float("nan")),
        "SPY_SHORT": (1.50, 1.51),
    }
    fill = fill_vertical_spread(
        spread,
        quotes,
        slippage_bps=0.0,
        tick_size_fn=_tick,
    )
    assert fill.net_debit is None


def test_fill_vertical_spread_inside_treats_nan_as_no_quote_and_does_not_raise():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="SPY_LONG",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=400.0,
            side=1,
        ),
        OptionLeg(
            symbol="SPY_SHORT",
            right="P",
            expiration=dt.date(2026, 2, 19),
            strike=401.0,
            side=-1,
        ),
    )
    quotes = {
        "SPY_LONG": (1.00, float("nan")),
        "SPY_SHORT": (1.50, 1.51),
    }
    fill = fill_vertical_spread_inside(
        spread,
        quotes,
        alpha=0.5,
        slippage_bps=0.0,
        net_tick_size_fn=_tick,
        leg_tick_size_fn=None,
    )
    assert fill.net_debit is None
