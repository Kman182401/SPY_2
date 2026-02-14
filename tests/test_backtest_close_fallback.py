import datetime as dt

import pandas as pd

from spy2.backtest.runner import run_backtest_range


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _build_root_with_missing_close_legs(tmp_path) -> dt.date:
    trade_date = dt.date(2026, 2, 2)
    date_str = trade_date.isoformat()
    base = tmp_path / "data" / "raw"

    underlying = pd.DataFrame(
        {
            "ts_event": [
                pd.Timestamp("2026-02-02T14:30:00Z"),
                pd.Timestamp("2026-02-02T20:58:00Z"),
            ],
            "close": [400.0, 401.0],
            "symbol": ["SPY", "SPY"],
        }
    )
    _write_parquet(
        underlying,
        base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    expiration = pd.Timestamp("2026-02-19T00:00:00Z")
    leg_symbols = ["SPY   260219C00400000", "SPY   260219C00401000"]
    other_symbol = "SPY   260219C00402000"
    definitions = pd.DataFrame(
        {
            "symbol": [*leg_symbols, other_symbol],
            "underlying": ["SPY", "SPY", "SPY"],
            "strike_price": [400.0, 401.0, 402.0],
            "expiration": [expiration, expiration, expiration],
        }
    )
    _write_parquet(
        definitions,
        base / "OPRA.PILLAR" / "definition" / f"date={date_str}" / "part-0000.parquet",
    )

    entry_ts = pd.Timestamp("2026-02-02T14:30:20Z")
    close_leg_ts = pd.Timestamp("2026-02-02T20:50:00Z")
    close_other_ts = pd.Timestamp("2026-02-02T20:58:59Z")
    quotes = pd.DataFrame(
        {
            "ts_event": [
                entry_ts,
                entry_ts,
                close_leg_ts,
                close_leg_ts,
                close_other_ts,
            ],
            "symbol": [
                leg_symbols[0],
                leg_symbols[1],
                leg_symbols[0],
                leg_symbols[1],
                other_symbol,
            ],
            "bid_px_00": [1.00, 0.60, 0.90, 0.55, 0.10],
            "ask_px_00": [1.10, 0.70, 1.00, 0.65, 0.20],
            "bid_sz_00": [10, 15, 10, 15, 10],
            "ask_sz_00": [12, 20, 12, 20, 12],
        }
    )
    _write_parquet(
        quotes,
        base / "OPRA.PILLAR" / "cbbo-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    stats_ts = pd.Timestamp("2026-02-02T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
            ],
            "symbol": [
                leg_symbols[0],
                leg_symbols[0],
                leg_symbols[1],
                leg_symbols[1],
                other_symbol,
                other_symbol,
            ],
            "stat_type": [6, 9, 6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000, 1_000, 10_000],
        }
    )
    _write_parquet(
        statistics,
        base / "OPRA.PILLAR" / "statistics" / f"date={date_str}" / "part-0000.parquet",
    )

    return trade_date


def _build_root_with_nan_in_last_quote(tmp_path) -> dt.date:
    trade_date = dt.date(2026, 2, 2)
    date_str = trade_date.isoformat()
    base = tmp_path / "data" / "raw"

    underlying = pd.DataFrame(
        {
            "ts_event": [
                pd.Timestamp("2026-02-02T14:30:00Z"),
                pd.Timestamp("2026-02-02T20:58:00Z"),
            ],
            "close": [400.0, 401.0],
            "symbol": ["SPY", "SPY"],
        }
    )
    _write_parquet(
        underlying,
        base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    expiration = pd.Timestamp("2026-02-19T00:00:00Z")
    leg_symbols = ["SPY   260219C00400000", "SPY   260219C00401000"]
    other_symbol = "SPY   260219C00402000"
    definitions = pd.DataFrame(
        {
            "symbol": [*leg_symbols, other_symbol],
            "underlying": ["SPY", "SPY", "SPY"],
            "strike_price": [400.0, 401.0, 402.0],
            "expiration": [expiration, expiration, expiration],
        }
    )
    _write_parquet(
        definitions,
        base / "OPRA.PILLAR" / "definition" / f"date={date_str}" / "part-0000.parquet",
    )

    entry_ts = pd.Timestamp("2026-02-02T14:30:20Z")
    close_leg_good_ts = pd.Timestamp("2026-02-02T20:49:00Z")
    close_leg_bad_ts = pd.Timestamp("2026-02-02T20:50:00Z")
    close_other_ts = pd.Timestamp("2026-02-02T20:58:59Z")
    quotes = pd.DataFrame(
        {
            "ts_event": [
                entry_ts,
                entry_ts,
                close_leg_good_ts,
                close_leg_good_ts,
                close_leg_bad_ts,
                close_leg_bad_ts,
                close_other_ts,
            ],
            "symbol": [
                leg_symbols[0],
                leg_symbols[1],
                leg_symbols[0],
                leg_symbols[1],
                leg_symbols[0],
                leg_symbols[1],
                other_symbol,
            ],
            # Last quote for each leg has a NaN ask; close fallback should use the last
            # finite ask from the earlier close_leg_good_ts quote.
            "bid_px_00": [1.00, 0.60, 0.90, 0.55, 0.90, 0.55, 0.10],
            "ask_px_00": [
                1.10,
                0.70,
                1.00,
                0.65,
                float("nan"),
                float("nan"),
                0.20,
            ],
            "bid_sz_00": [10, 15, 10, 15, 10, 15, 10],
            "ask_sz_00": [12, 20, 12, 20, 12, 20, 12],
        }
    )
    _write_parquet(
        quotes,
        base / "OPRA.PILLAR" / "cbbo-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    stats_ts = pd.Timestamp("2026-02-02T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
                stats_ts,
            ],
            "symbol": [
                leg_symbols[0],
                leg_symbols[0],
                leg_symbols[1],
                leg_symbols[1],
                other_symbol,
                other_symbol,
            ],
            "stat_type": [6, 9, 6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000, 1_000, 10_000],
        }
    )
    _write_parquet(
        statistics,
        base / "OPRA.PILLAR" / "statistics" / f"date={date_str}" / "part-0000.parquet",
    )

    return trade_date


def _build_root_with_missing_chain_on_close_day(tmp_path) -> tuple[dt.date, dt.date]:
    # Day 1 has a valid chain snapshot (definitions + quotes) so we can open.
    # Day 2 is missing chain-building inputs (definitions), but still has leg quotes.
    # The backtest must still be able to force-close using as-of leg quotes.
    day1 = dt.date(2026, 2, 3)
    day2 = dt.date(2026, 2, 4)
    base = tmp_path / "data" / "raw"

    exp = pd.Timestamp("2026-02-04T00:00:00Z")
    leg_symbols = ["SPY   260204P00400000", "SPY   260204P00399000"]

    for trade_date in (day1, day2):
        date_str = trade_date.isoformat()
        underlying = pd.DataFrame(
            {
                "ts_event": [
                    pd.Timestamp(f"{date_str}T14:30:00Z"),
                    pd.Timestamp(f"{date_str}T20:58:00Z"),
                ],
                "close": [400.0, 401.0],
                "symbol": ["SPY", "SPY"],
            }
        )
        _write_parquet(
            underlying,
            base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
        )

    # Definitions only for day1 (day2 intentionally missing).
    defs = pd.DataFrame(
        {
            "symbol": leg_symbols,
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

    entry_ts = pd.Timestamp("2026-02-03T14:30:20Z")
    close1_ts = pd.Timestamp("2026-02-03T20:50:00Z")
    close2_ts = pd.Timestamp("2026-02-04T20:50:00Z")
    quotes_day1 = pd.DataFrame(
        {
            "ts_event": [entry_ts, entry_ts, close1_ts, close1_ts],
            "symbol": [leg_symbols[0], leg_symbols[1], leg_symbols[0], leg_symbols[1]],
            "bid_px_00": [1.00, 0.60, 0.90, 0.55],
            "ask_px_00": [1.10, 0.70, 1.00, 0.65],
            "bid_sz_00": [10, 15, 10, 15],
            "ask_sz_00": [12, 20, 12, 20],
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
    quotes_day2 = pd.DataFrame(
        {
            "ts_event": [close2_ts, close2_ts],
            "symbol": [leg_symbols[0], leg_symbols[1]],
            "bid_px_00": [0.80, 0.45],
            "ask_px_00": [0.90, 0.55],
            "bid_sz_00": [10, 15],
            "ask_sz_00": [12, 20],
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

    # Liquidity stats for day1 so candidate selection isn't filtered out.
    stats_ts = pd.Timestamp("2026-02-03T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [stats_ts, stats_ts, stats_ts, stats_ts],
            "symbol": [leg_symbols[0], leg_symbols[0], leg_symbols[1], leg_symbols[1]],
            "stat_type": [6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000],
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

    return day1, day2


def test_force_close_still_runs_when_chain_missing_on_close_day(tmp_path):
    day1, day2 = _build_root_with_missing_chain_on_close_day(tmp_path)
    outputs = run_backtest_range(
        start=day1,
        end=day2,
        root=tmp_path,
        strategy="demo_vertical",
        right="P",
        width=1.0,
        fill_model="conservative",
        fill_sensitivity=False,
    )
    trades = pd.read_parquet(outputs.trades_path)
    opens = int((trades["stage"] == "OPEN").sum())
    closes = int((trades["stage"] == "CLOSE").sum())
    assert opens == closes


def _build_root_with_expiry_settlement(tmp_path) -> tuple[dt.date, dt.date]:
    # Open on day1, expiration is day2. Day2 lacks option chain quotes, so the
    # runner must settle intrinsically using the underlying close on expiration.
    day1 = dt.date(2026, 2, 3)
    day2 = dt.date(2026, 2, 4)
    base = tmp_path / "data" / "raw"

    exp = pd.Timestamp("2026-02-04T00:00:00Z")
    leg_symbols = ["SPY   260204P00400000", "SPY   260204P00399000"]

    for trade_date in (day1, day2):
        date_str = trade_date.isoformat()
        underlying = pd.DataFrame(
            {
                "ts_event": [
                    pd.Timestamp(f"{date_str}T14:30:00Z"),
                    pd.Timestamp(f"{date_str}T20:58:00Z"),
                ],
                "close": [400.0, 401.0],
                "symbol": ["SPY", "SPY"],
            }
        )
        _write_parquet(
            underlying,
            base / "EQUS.MINI" / "ohlcv-1m" / f"date={date_str}" / "part-0000.parquet",
        )

    defs = pd.DataFrame(
        {
            "symbol": leg_symbols,
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

    entry_ts = pd.Timestamp("2026-02-03T14:30:20Z")
    close1_ts = pd.Timestamp("2026-02-03T20:50:00Z")
    quotes_day1 = pd.DataFrame(
        {
            "ts_event": [entry_ts, entry_ts, close1_ts, close1_ts],
            "symbol": [leg_symbols[0], leg_symbols[1], leg_symbols[0], leg_symbols[1]],
            "bid_px_00": [1.00, 0.60, 0.90, 0.55],
            "ask_px_00": [1.10, 0.70, 1.00, 0.65],
            "bid_sz_00": [10, 15, 10, 15],
            "ask_sz_00": [12, 20, 12, 20],
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

    stats_ts = pd.Timestamp("2026-02-03T20:59:00Z")
    statistics = pd.DataFrame(
        {
            "ts_event": [stats_ts, stats_ts, stats_ts, stats_ts],
            "symbol": [leg_symbols[0], leg_symbols[0], leg_symbols[1], leg_symbols[1]],
            "stat_type": [6, 9, 6, 9],
            "quantity": [1_000, 10_000, 1_000, 10_000],
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

    return day1, day2


def test_expiry_settlement_closes_when_quotes_missing(tmp_path):
    day1, day2 = _build_root_with_expiry_settlement(tmp_path)
    outputs = run_backtest_range(
        start=day1,
        end=day2,
        root=tmp_path,
        strategy="demo_vertical",
        right="P",
        width=1.0,
        fill_model="conservative",
        fill_sensitivity=False,
    )
    trades = pd.read_parquet(outputs.trades_path)
    opens = int((trades["stage"] == "OPEN").sum())
    closes = int((trades["stage"] == "CLOSE").sum())
    assert opens == closes
    assert "EXPIRED_SETTLEMENT" in set(
        trades.loc[trades["stage"] == "CLOSE", "reason"].dropna()
    )


def test_backtest_closes_when_close_snapshot_missing_leg_quotes(tmp_path):
    trade_date = _build_root_with_missing_close_legs(tmp_path)
    outputs = run_backtest_range(
        start=trade_date,
        end=trade_date,
        root=tmp_path,
        strategy="demo_vertical",
        right="P",
        width=1.0,
        quotes_schema="cbbo-1m",
        fill_model="conservative",
    )
    trades = pd.read_parquet(outputs.trades_path)
    assert set(trades["stage"]) >= {"OPEN", "CLOSE"}


def test_backtest_close_fallback_skips_nan_and_uses_last_finite_quote(tmp_path):
    trade_date = _build_root_with_nan_in_last_quote(tmp_path)
    outputs = run_backtest_range(
        start=trade_date,
        end=trade_date,
        root=tmp_path,
        strategy="demo_vertical",
        right="P",
        width=1.0,
        quotes_schema="cbbo-1m",
        fill_model="conservative",
    )
    trades = pd.read_parquet(outputs.trades_path)
    assert set(trades["stage"]) >= {"OPEN", "CLOSE"}
