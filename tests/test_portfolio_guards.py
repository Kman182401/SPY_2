import datetime as dt

import pandas as pd

from spy2.corpactions.ops import import_dividends_csv
from spy2.corpactions.dividends import load_dividend_calendar
from spy2.options.models import OptionChainSnapshot, OptionLeg, VerticalSpread
from spy2.portfolio.guards import (
    DividendGuardConfig,
    PdtGuardConfig,
    evaluate_ex_dividend_guard,
    evaluate_pdt_open_guard,
)


def test_dividend_guard_closes_short_itm_call_when_extrinsic_lt_dividend():
    spread = VerticalSpread.from_legs(
        OptionLeg(
            symbol="CALL_LONG",
            right="C",
            expiration=dt.date(2026, 2, 19),
            strike=101.0,
            side=1,
        ),
        OptionLeg(
            symbol="CALL_SHORT",
            right="C",
            expiration=dt.date(2026, 2, 19),
            strike=100.0,
            side=-1,
        ),
    )
    snapshot = OptionChainSnapshot(
        ts_event=dt.datetime(2026, 2, 2, 20, 59, tzinfo=dt.timezone.utc),
        underlying_price=105.0,
        chain=pd.DataFrame(
            [
                {"symbol": "CALL_SHORT", "bid": 5.10, "ask": 5.20},
            ]
        ),
    )
    eval = evaluate_ex_dividend_guard(
        spread=spread,
        snapshot=snapshot,
        dividend_amount=0.20,
        config=DividendGuardConfig(enabled=True),
    )
    assert eval.should_close
    assert eval.reason == "DIV_GUARD_EXTRINSIC_LT_DIV"


def test_pdt_guard_blocks_fourth_open_transaction_in_window():
    sessions = [
        dt.date(2026, 2, 2),
        dt.date(2026, 2, 3),
        dt.date(2026, 2, 4),
        dt.date(2026, 2, 5),
        dt.date(2026, 2, 6),
    ]
    eval = evaluate_pdt_open_guard(
        session_date=sessions[-1],
        sessions=sessions,
        open_transactions_by_date={
            sessions[1]: 1,
            sessions[2]: 1,
            sessions[3]: 1,
        },
        day_trades_by_date={},
        account_equity=1000.0,
        config=PdtGuardConfig(),
    )
    assert not eval.allowed
    assert eval.reason == "PDT_GUARD_MAX_OPEN_TRANSACTIONS"


def test_dividends_import_csv_round_trip(tmp_path):
    csv_path = tmp_path / "divs.csv"
    csv_path.write_text("ex_date,gross_dividend\n2026-02-03,0.25\n2026-02-03,0.05\n")
    output_path, _manifest_path = import_dividends_csv(
        symbol="SPY",
        csv_path=csv_path,
        root=tmp_path,
    )
    assert output_path.exists()

    cal = load_dividend_calendar(symbol="SPY", root=tmp_path)
    assert cal is not None
    assert cal.amount_on(dt.date(2026, 2, 3)) == 0.30
