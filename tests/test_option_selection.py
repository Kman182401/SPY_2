import datetime as dt

import pandas as pd

from spy2.options.models import OptionChainSnapshot
from spy2.options.selection import select_vertical_spread


def test_select_vertical_put_picks_closest_lower_strike_when_target_missing():
    expiration = dt.date(2026, 2, 19)
    chain = pd.DataFrame(
        [
            {"symbol": "P100", "expiration": expiration, "strike": 100.0, "right": "P"},
            {"symbol": "P98", "expiration": expiration, "strike": 98.0, "right": "P"},
            {"symbol": "P97", "expiration": expiration, "strike": 97.0, "right": "P"},
            {"symbol": "P90", "expiration": expiration, "strike": 90.0, "right": "P"},
        ]
    )
    snap = OptionChainSnapshot(
        ts_event=dt.datetime(2026, 2, 2, 14, 30, tzinfo=dt.timezone.utc),
        underlying_price=100.0,
        chain=chain,
    )

    sel = select_vertical_spread(
        snap,
        right="P",
        width=1.0,
        allow_fallback_right=False,
    )
    assert sel is not None
    spread, used_right = sel
    assert used_right == "P"
    assert spread.long_leg.strike == 100.0
    assert spread.short_leg.strike == 98.0
