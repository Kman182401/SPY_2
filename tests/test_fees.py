import datetime as dt

from spy2.fees.ibkr import IbkrFeeSchedule, estimate_spread_fees
from spy2.fees.tick import round_price_for_side
from spy2.options.fill import FillResult, SpreadFill
from spy2.options.models import OptionLeg, VerticalSpread


def test_round_price_for_side():
    assert round_price_for_side(1.02, 0.05, side=1) == 1.0
    assert round_price_for_side(1.02, 0.05, side=-1) == 1.05


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


def test_ibkr_commission_per_contract_tiered():
    schedule = IbkrFeeSchedule(
        per_contract=0.99,
        min_per_leg=0.0,
        regulatory_per_contract=0.0,
        transaction_per_contract=0.0,
        sec_fee_rate=0.0,
        contract_multiplier=100,
        commission_cutoff_1=0.05,
        commission_rate_1=0.25,
        commission_cutoff_2=0.10,
        commission_rate_2=0.50,
        commission_rate_3=0.65,
    )
    assert schedule.commission_per_contract(0.04) == 0.25
    assert schedule.commission_per_contract(0.05) == 0.50
    assert schedule.commission_per_contract(0.099) == 0.50
    assert schedule.commission_per_contract(0.10) == 0.65


def test_estimate_spread_fees_uses_tiered_commission_by_leg_premium():
    expiration = dt.date(2026, 2, 19)
    long_leg = OptionLeg(
        symbol="SPY   260219C00400000",
        right="C",
        expiration=expiration,
        strike=400.0,
        side=1,
        quantity=10,
    )
    short_leg = OptionLeg(
        symbol="SPY   260219C00401000",
        right="C",
        expiration=expiration,
        strike=401.0,
        side=-1,
        quantity=10,
    )
    spread = VerticalSpread.from_legs(long_leg, short_leg)
    fills = [
        FillResult(
            symbol=long_leg.symbol,
            side=long_leg.side,
            bid=0.03,
            ask=0.04,
            mid=0.035,
            slippage=0.0,
            price=0.04,
        ),
        FillResult(
            symbol=short_leg.symbol,
            side=short_leg.side,
            bid=0.12,
            ask=0.13,
            mid=0.125,
            slippage=0.0,
            price=0.12,
        ),
    ]
    spread_fill = SpreadFill(spread=spread, net_debit=-0.08, leg_fills=fills)
    schedule = IbkrFeeSchedule(
        per_contract=0.99,
        min_per_leg=0.0,
        regulatory_per_contract=0.0,
        transaction_per_contract=0.0,
        sec_fee_rate=0.0,
        contract_multiplier=100,
        commission_cutoff_1=0.05,
        commission_rate_1=0.25,
        commission_cutoff_2=0.10,
        commission_rate_2=0.50,
        commission_rate_3=0.65,
    )
    fees = estimate_spread_fees(spread_fill, schedule=schedule)
    assert fees.per_leg[0].commission == 2.5
    assert fees.per_leg[1].commission == 6.5
    assert fees.commission == 9.0
