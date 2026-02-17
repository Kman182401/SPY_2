import datetime as dt

import pandas as pd

from spy2.options.chain import load_chain_frame


def _write_parquet(df: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_load_chain_frame_writes_and_reuses_cache(tmp_path):
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
    defs = pd.DataFrame(
        {
            "symbol": ["SPY   260219P00400000", "QQQ   260219P00400000"],
            "underlying": ["SPY", "QQQ"],
            "strike_price": [400.0, 400.0],
            "expiration": [expiration, expiration],
        }
    )
    _write_parquet(
        defs,
        base / "OPRA.PILLAR" / "definition" / f"date={date_str}" / "part-0000.parquet",
    )

    quotes = pd.DataFrame(
        {
            "ts_event": [
                pd.Timestamp("2026-02-02T14:30:20Z"),
                pd.Timestamp("2026-02-02T14:30:20Z"),
            ],
            "symbol": ["SPY   260219P00400000", "QQQ   260219P00400000"],
            "bid_px_00": [1.0, 2.0],
            "ask_px_00": [1.1, 2.1],
            "bid_sz_00": [10, 10],
            "ask_sz_00": [10, 10],
        }
    )
    _write_parquet(
        quotes,
        base / "OPRA.PILLAR" / "cbbo-1m" / f"date={date_str}" / "part-0000.parquet",
    )

    stats = pd.DataFrame(
        {
            "ts_event": [
                pd.Timestamp("2026-02-02T20:59:00Z"),
                pd.Timestamp("2026-02-02T20:59:00Z"),
            ],
            "symbol": ["SPY   260219P00400000", "SPY   260219P00400000"],
            "stat_type": [6, 9],
            "quantity": [1000, 10000],
        }
    )
    _write_parquet(
        stats,
        base / "OPRA.PILLAR" / "statistics" / f"date={date_str}" / "part-0000.parquet",
    )

    first = load_chain_frame(
        trade_date, root=tmp_path, quotes_schema="cbbo-1m", underlying_symbol="SPY"
    )
    assert not first.empty
    assert set(first["symbol"]) == {"SPY   260219P00400000"}

    cache_path = (
        tmp_path
        / "artifacts"
        / "cache"
        / "chain_frames"
        / "schema=cbbo-1m"
        / "underlying=SPY"
        / "asof_tol_s=60"
        / f"date={date_str}"
        / "part-0000.parquet"
    )
    assert cache_path.exists()

    second = load_chain_frame(
        trade_date, root=tmp_path, quotes_schema="cbbo-1m", underlying_symbol="SPY"
    )
    pd.testing.assert_frame_equal(
        first.reset_index(drop=True), second.reset_index(drop=True)
    )
