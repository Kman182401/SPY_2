import datetime as dt

import pandas as pd

from spy2.backtest.runner import run_backtest_range
from spy2.options.selection import VerticalSelectionConfig


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _build_root(tmp_path) -> tuple[dt.date, dt.date]:
    day1 = dt.date(2026, 2, 3)
    day2 = dt.date(2026, 2, 4)
    base = tmp_path / "data" / "raw"

    exp = pd.Timestamp("2026-03-20T00:00:00Z")
    # spot=400; OTM puts at 1% => <=396. We include 396 and 395 to build 1-wide.
    short_sym = "SPY   260320P00396000"
    long_sym = "SPY   260320P00395000"

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
                "strike_price": [396.0, 395.0],
                "expiration": [exp, exp],
            }
        )
        _write_parquet(
            defs,
            base
            / "OPRA.PILLAR"
            / "definition"
            / f"date={date_str}"
            / "part-0000.parquet",
        )

        stats_ts = pd.Timestamp(f"{date_str}T20:59:00Z")
        statistics = pd.DataFrame(
            {
                "ts_event": [stats_ts, stats_ts, stats_ts, stats_ts],
                "symbol": [short_sym, short_sym, long_sym, long_sym],
                "stat_type": [6, 9, 6, 9],
                "quantity": [1_000, 10_000, 1_000, 10_000],
            }
        )
        _write_parquet(
            statistics,
            base
            / "OPRA.PILLAR"
            / "statistics"
            / f"date={date_str}"
            / "part-0000.parquet",
        )

    entry_ts1 = pd.Timestamp("2026-02-03T14:30:20Z")
    close_ts1 = pd.Timestamp("2026-02-03T20:50:00Z")
    close_ts2 = pd.Timestamp("2026-02-04T20:50:00Z")

    # Day1 entry quote (enough credit): short bid 0.55, long ask 0.30 => credit 0.25
    quotes_day1 = pd.DataFrame(
        {
            "ts_event": [entry_ts1, entry_ts1, close_ts1, close_ts1],
            "symbol": [short_sym, long_sym, short_sym, long_sym],
            "bid_px_00": [0.55, 0.25, 0.50, 0.23],
            "ask_px_00": [0.60, 0.30, 0.55, 0.28],
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

    # Day2 close quote (any valid quote; runner should close by DTE at worst).
    quotes_day2 = pd.DataFrame(
        {
            "ts_event": [close_ts2, close_ts2],
            "symbol": [short_sym, long_sym],
            "bid_px_00": [0.40, 0.20],
            "ask_px_00": [0.45, 0.25],
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

    return day1, day2


def test_baseline_otm_credit_opens_and_closes(tmp_path):
    day1, day2 = _build_root(tmp_path)
    outputs = run_backtest_range(
        start=day1,
        end=day2,
        root=tmp_path,
        strategy="baseline_otm_credit",
        right="P",
        width=1.0,
        structure="credit",
        selection=VerticalSelectionConfig(
            dte_min=1, dte_max=90, otm_pct=0.01, min_credit=0.2
        ),
        fill_model="conservative",
        fill_sensitivity=False,
        force_close_dte=1,
    )
    trades = pd.read_parquet(outputs.trades_path)
    opens = int((trades["stage"] == "OPEN").sum())
    closes = int((trades["stage"] == "CLOSE").sum())
    assert opens == 1
    assert closes == 1
