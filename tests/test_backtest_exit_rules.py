import datetime as dt

import pandas as pd

from spy2.backtest.runner import run_backtest_range
from spy2.portfolio.exits import ExitRuleConfig


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _build_root_for_profit_take_credit_put(tmp_path) -> tuple[dt.date, dt.date]:
    day1 = dt.date(2026, 2, 3)
    day2 = dt.date(2026, 2, 4)
    base = tmp_path / "data" / "raw"

    exp = pd.Timestamp("2026-03-20T00:00:00Z")
    # Put credit spread under our selection rules for puts:
    # structure=credit -> short=anchor<=spot, long=short-width.
    short_sym = "SPY   260320P00400000"
    long_sym = "SPY   260320P00399000"

    for trade_date in (day1, day2):
        date_str = trade_date.isoformat()
        underlying = pd.DataFrame(
            {
                "ts_event": [
                    pd.Timestamp(f"{date_str}T14:30:00Z"),
                    pd.Timestamp(f"{date_str}T20:58:00Z"),
                ],
                "close": [400.0, 400.0],
                "symbol": ["SPY", "SPY"],
            }
        )
        _write_parquet(
            underlying,
            base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
        )

    defs = pd.DataFrame(
        {
            "symbol": [short_sym, long_sym],
            "underlying": ["SPY", "SPY"],
            "strike_price": [400.0, 399.0],
            "expiration": [exp, exp],
        }
    )
    _write_parquet(
        defs,
        base
        / "OPRA.PILLAR"
        / "definition"
        / f"date={day1.isoformat()}"
        / "part-0000.parquet",
    )
    _write_parquet(
        defs,
        base
        / "OPRA.PILLAR"
        / "definition"
        / f"date={day2.isoformat()}"
        / "part-0000.parquet",
    )

    entry_ts1 = pd.Timestamp("2026-02-03T14:30:20Z")
    close_ts1 = pd.Timestamp("2026-02-03T20:50:00Z")
    close_ts2 = pd.Timestamp("2026-02-04T20:50:00Z")

    # Day 1 entry: credit ~0.40 (sell short rich, buy long cheap).
    quotes_day1 = pd.DataFrame(
        {
            "ts_event": [entry_ts1, entry_ts1, close_ts1, close_ts1],
            "symbol": [short_sym, long_sym, short_sym, long_sym],
            "bid_px_00": [1.10, 0.60, 1.05, 0.58],
            "ask_px_00": [1.20, 0.70, 1.15, 0.68],
            "bid_sz_00": [10, 10, 10, 10],
            "ask_sz_00": [10, 10, 10, 10],
        }
    )
    _write_parquet(
        quotes_day1,
        base
        / "OPRA.PILLAR"
        / "cbbo-1m"
        / f"date={day1.isoformat()}"
        / "part-0000.parquet",
    )

    # Day 2 close: spread value tightens, unrealized PnL hits profit target.
    quotes_day2 = pd.DataFrame(
        {
            "ts_event": [close_ts2, close_ts2],
            "symbol": [short_sym, long_sym],
            "bid_px_00": [0.50, 0.40],
            "ask_px_00": [0.60, 0.50],
            "bid_sz_00": [10, 10],
            "ask_sz_00": [10, 10],
        }
    )
    _write_parquet(
        quotes_day2,
        base
        / "OPRA.PILLAR"
        / "cbbo-1m"
        / f"date={day2.isoformat()}"
        / "part-0000.parquet",
    )

    stats_ts1 = pd.Timestamp("2026-02-03T20:59:00Z")
    stats_ts2 = pd.Timestamp("2026-02-04T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [
                stats_ts1,
                stats_ts1,
                stats_ts1,
                stats_ts1,
                stats_ts2,
                stats_ts2,
                stats_ts2,
                stats_ts2,
            ],
            "symbol": [
                short_sym,
                short_sym,
                long_sym,
                long_sym,
                short_sym,
                short_sym,
                long_sym,
                long_sym,
            ],
            "stat_type": [6, 9, 6, 9, 6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000, 1_000, 10_000, 1_000, 10_000],
        }
    )
    _write_parquet(
        statistics,
        base
        / "OPRA.PILLAR"
        / "statistics"
        / f"date={day1.isoformat()}"
        / "part-0000.parquet",
    )
    _write_parquet(
        statistics,
        base
        / "OPRA.PILLAR"
        / "statistics"
        / f"date={day2.isoformat()}"
        / "part-0000.parquet",
    )

    return day1, day2


def test_profit_take_exit_closes_position(tmp_path):
    day1, day2 = _build_root_for_profit_take_credit_put(tmp_path)
    outputs = run_backtest_range(
        start=day1,
        end=day2,
        root=tmp_path,
        strategy="demo_vertical",
        right="P",
        width=1.0,
        structure="credit",
        force_close_dte=0,
        fill_model="conservative",
        fill_sensitivity=False,
        exit_rules=ExitRuleConfig(enabled=True, profit_take_frac=0.5),
    )
    trades = pd.read_parquet(outputs.trades_path)
    closes = trades[trades["stage"] == "CLOSE"]
    assert not closes.empty
    assert "PROFIT_TAKE" in set(closes["reason"].dropna())
