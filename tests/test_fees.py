import datetime as dt

from spy2.fees.ibkr import IbkrFeeSchedule, estimate_spread_fees
from spy2.fees.tick import round_price_for_side
from spy2.options.fill import FillResult, SpreadFill
from spy2.options.models import OptionLeg, VerticalSpread


def test_round_price_for_side():
    assert round_price_for_side(1.02, 0.05, side=1) == 1.05
    assert round_price_for_side(1.02, 0.05, side=-1) == 1.0


def test_estimate_spread_fees_min_per_leg():
    expiration = dt.date(2026, 2, 19)
    long_leg = OptionLeg(
        symbol="SPY   260219C00400000",
        right="C",
        expiration=expiration,
        strike=400.0,
        side=1,
        quantity=1,
    )
    short_leg = OptionLeg(
        symbol="SPY   260219C00401000",
        right="C",
        expiration=expiration,
        strike=401.0,
        side=-1,
        quantity=1,
    )
    spread = VerticalSpread.from_legs(long_leg, short_leg)
    fills = [
        FillResult(
            symbol=long_leg.symbol,
            side=long_leg.side,
            bid=1.0,
            ask=1.1,
            mid=1.05,
            slippage=0.0,
            price=1.1,
        ),
        FillResult(
            symbol=short_leg.symbol,
            side=short_leg.side,
            bid=0.5,
            ask=0.6,
            mid=0.55,
            slippage=0.0,
            price=0.5,
        ),
    ]
    spread_fill = SpreadFill(spread=spread, net_debit=0.6, leg_fills=fills)
    schedule = IbkrFeeSchedule(
        per_contract=0.5,
        min_per_leg=1.0,
        regulatory_per_contract=0.1,
        transaction_per_contract=0.2,
        sec_fee_rate=0.001,
        contract_multiplier=100,
    )
    fees = estimate_spread_fees(spread_fill, schedule=schedule)
    assert fees.commission == 2.0
    assert fees.regulatory == 0.2
    assert fees.transaction == 0.2
    assert abs(fees.sec_fee - 0.05) < 1e-9
    assert abs(fees.total - 2.45) < 1e-9
