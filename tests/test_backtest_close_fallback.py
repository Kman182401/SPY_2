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

    return trade_date


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
